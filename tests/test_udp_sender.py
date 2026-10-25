import threading
import time

import pytest

from pb import sources_pb2
from sources.udp_sender import PeriodicPublisher, UdpSender
from tests.builders import make_reading
from tests.fakes import FailingDatagramSocket, FakeClock, FakeDatagramSocket

GATEWAY = ("127.0.0.1", 6000)


def decode(payload: bytes) -> sources_pb2.SensorReading:
    message = sources_pb2.SourceMessage.FromString(payload)
    assert message.WhichOneof("payload") == "reading"
    return message.reading


# ----- UdpSender ------------------------------------------------------------------

def test_sends_reading_inside_the_envelope_to_the_gateway() -> None:
    sock = FakeDatagramSocket()
    assert UdpSender(lambda: GATEWAY, sock).send(make_reading("s1", value=12.5)) is True
    payload, destination = sock.sent[0]
    assert destination == GATEWAY
    reading = decode(payload)
    assert (reading.source_id, reading.measurements[0].value, reading.is_event) == ("s1", 12.5, False)


def test_nothing_is_sent_before_discovery() -> None:
    sock = FakeDatagramSocket()
    assert UdpSender(lambda: None, sock).send(make_reading()) is False
    assert sock.sent == []


def test_event_flag_and_timestamp_are_filled_in() -> None:
    sock = FakeDatagramSocket()
    reading = make_reading()
    reading.timestamp_ms = 0
    UdpSender(lambda: GATEWAY, sock, clock=lambda: 1_700_000_000.5).send(reading, is_event=True)
    sent = decode(sock.sent[0][0])
    assert sent.is_event and sent.timestamp_ms == 1_700_000_000_500


def test_gateway_change_is_followed() -> None:
    sock, gateway = FakeDatagramSocket(), [GATEWAY]
    sender = UdpSender(lambda: gateway[0], sock)
    sender.send(make_reading())
    gateway[0] = ("127.0.0.1", 6001)  # Gateway reiniciou em outra porta
    sender.send(make_reading())
    assert [destination for _, destination in sock.sent] == [GATEWAY, ("127.0.0.1", 6001)]


def test_network_error_does_not_raise() -> None:
    assert UdpSender(lambda: GATEWAY, FailingDatagramSocket()).send(make_reading()) is False


# ----- PeriodicPublisher (relógio falso, sem threads) --------------------------------

class CountingSampler:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> sources_pb2.SensorReading:
        self.calls += 1
        return make_reading(value=float(self.calls))


def make_publisher(interval_s: float = 15.0) -> tuple[PeriodicPublisher, FakeClock, FakeDatagramSocket, CountingSampler]:
    clock, sock, sampler = FakeClock(), FakeDatagramSocket(), CountingSampler()
    publisher = PeriodicPublisher(sampler, interval_s, UdpSender(lambda: GATEWAY, sock), clock=clock)
    return publisher, clock, sock, sampler


def test_publishes_immediately_then_every_interval() -> None:
    publisher, clock, sock, _ = make_publisher(15.0)
    assert publisher.run_pending()
    clock.advance(14.9)
    assert not publisher.run_pending()
    clock.advance(0.1)
    assert publisher.run_pending()
    assert [decode(p).measurements[0].value for p, _ in sock.sent] == [1.0, 2.0]


def test_shorter_interval_applies_to_the_next_send() -> None:
    publisher, clock, _, sampler = make_publisher(15.0)
    publisher.run_pending()
    clock.advance(6.0)
    publisher.set_interval(5.0)  # 15 s → 5 s: já passou 6 s desde o último envio
    assert publisher.seconds_until_due() == 0.0
    assert publisher.run_pending()
    clock.advance(5.0)
    assert publisher.run_pending()
    assert sampler.calls == 3


def test_longer_interval_postpones_the_next_send() -> None:
    publisher, clock, _, _ = make_publisher(5.0)
    publisher.run_pending()
    publisher.set_interval(20.0)
    clock.advance(19.0)
    assert not publisher.run_pending()
    clock.advance(1.0)
    assert publisher.run_pending()


def test_pause_stops_sending_and_resume_sends_right_away() -> None:
    publisher, clock, _, sampler = make_publisher(5.0)
    publisher.run_pending()
    publisher.pause()
    clock.advance(60.0)
    assert not publisher.run_pending() and publisher.paused
    clock.advance(1.0)
    publisher.resume()
    assert publisher.run_pending()
    assert sampler.calls == 2


def test_publish_now_with_delay() -> None:
    publisher, clock, _, _ = make_publisher(15.0)
    publisher.run_pending()
    publisher.publish_now(delay_s=0.2)
    assert not publisher.run_pending()
    clock.advance(0.2)
    assert publisher.run_pending()


def test_sample_returning_none_skips_the_cycle() -> None:
    sock = FakeDatagramSocket()
    publisher = PeriodicPublisher(lambda: None, 1.0, UdpSender(lambda: GATEWAY, sock), clock=FakeClock())
    assert publisher.run_pending()
    assert sock.sent == []


@pytest.mark.parametrize("interval", [0.0, -1.0, float("nan"), float("inf")])
def test_invalid_interval_is_rejected(interval: float) -> None:
    publisher, *_ = make_publisher()
    with pytest.raises(ValueError):
        publisher.set_interval(interval)
    with pytest.raises(ValueError):
        PeriodicPublisher(CountingSampler(), interval, UdpSender(lambda: GATEWAY, FakeDatagramSocket()))


# ----- PeriodicPublisher (thread de verdade) -------------------------------------------

def test_background_loop_follows_interval_changes_in_real_time() -> None:
    sent, event = [], threading.Event()

    class RecordingSender:
        def send(self, reading, is_event=False):
            sent.append(time.monotonic())
            event.set()
            return True

    publisher = PeriodicPublisher(lambda: make_reading(), 30.0, RecordingSender())
    publisher.start()
    try:
        assert event.wait(1.0)  # primeira leitura logo de cara
        publisher.set_interval(0.1)  # sem esperar os 30 s
        deadline = time.monotonic() + 2.0
        while len(sent) < 4 and time.monotonic() < deadline:
            time.sleep(0.02)
        assert len(sent) >= 4
        publisher.pause()
        time.sleep(0.15)
        count = len(sent)
        time.sleep(0.4)
        assert len(sent) == count  # pausado não envia
    finally:
        publisher.stop()


def test_errors_in_sample_function_do_not_kill_the_loop() -> None:
    calls = []

    def flaky() -> sources_pb2.SensorReading:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("sensor travou")
        return make_reading()

    sock = FakeDatagramSocket()
    publisher = PeriodicPublisher(flaky, 0.05, UdpSender(lambda: GATEWAY, sock))
    publisher.start()
    try:
        deadline = time.monotonic() + 2.0
        while not sock.sent and time.monotonic() < deadline:
            time.sleep(0.02)
        assert sock.sent
    finally:
        publisher.stop()
