"""Fontes controláveis (#5, #6, #8) sem rede: comandos chamados direto na fonte, sockets falsos."""

import random
import time

import grpc
import pytest

from common.net import open_udp_socket
from pb import sources_pb2
from sources.config import SourceConfig
from sources.control import FAULTY_VALUE, CommandRejected
from sources.greenhouse import GreenhouseSensor, base_temperature
from sources.valve import IrrigationValve
from tests.fakes import ClosableFakeSocket, FakeClock

GATEWAY = ("127.0.0.1", 6000)


def config(source_id: str = "valvula-1", grpc_port: int = 50051, interval_s: float = 5.0) -> SourceConfig:
    return SourceConfig(source_id, "TESTE", interval_s, grpc_port)


@pytest.fixture
def sockets():
    listener, udp = open_udp_socket(0), ClosableFakeSocket()
    yield listener, udp
    listener.close()


def make_valve(sockets, **kwargs) -> IrrigationValve:
    listener, udp = sockets
    valve = IrrigationValve(config(), rng=random.Random(1), listener_sock=listener, udp_sock=udp, **kwargs)
    valve.locator.update(GATEWAY)
    return valve


def make_sensor(sockets, **kwargs) -> GreenhouseSensor:
    listener, udp = sockets
    kwargs.setdefault("threshold", 30.0)
    kwargs.setdefault("ventilation_pct", 50.0)
    sensor = GreenhouseSensor(config("estufa-1", 50052), rng=random.Random(1), listener_sock=listener, udp_sock=udp,
                              **kwargs)
    sensor._temperature.noise = sensor._temperature.drift = 0.0  # valores exatos (= base)
    sensor.locator.update(GATEWAY)
    return sensor


def sent_readings(udp: ClosableFakeSocket) -> list[sources_pb2.SensorReading]:
    return [sources_pb2.SourceMessage.FromString(payload).reading for payload, _ in udp.sent]


# ----- estado e comandos ----------------------------------------------------------

def test_initial_status_is_reported_in_the_announcement(sockets) -> None:
    valve = make_valve(sockets, target_flow_lpm=12.0)
    announcement = valve.build_announcement("10.0.0.5", request_id=3)
    assert (announcement.controllable, announcement.grpc_port) == (True, 50051)
    state = announcement.initial_state
    assert (state.powered_on, state.send_interval_s, dict(state.parameters)) == (True, 5.0, {"vazao_alvo": 12.0})
    assert not state.HasField("threshold")


def test_power_off_pauses_the_publisher_and_on_resumes(sockets) -> None:
    valve = make_valve(sockets)
    assert valve.set_power(False).powered_on is False
    assert valve.publisher.paused and valve.build_reading() is None
    assert valve.set_power(True).powered_on is True
    assert not valve.publisher.paused and valve.build_reading() is not None


def test_send_interval_changes_the_publisher(sockets) -> None:
    valve = make_valve(sockets)
    assert valve.set_send_interval(1.5).send_interval_s == 1.5
    assert valve.publisher.interval_s == 1.5


@pytest.mark.parametrize("interval", [0.0, -1.0, float("nan"), 99_999.0])
def test_invalid_interval_is_rejected(sockets, interval: float) -> None:
    with pytest.raises(CommandRejected) as error:
        make_valve(sockets).set_send_interval(interval)
    assert error.value.code == grpc.StatusCode.INVALID_ARGUMENT


def test_parameter_changes_the_flow_sent(sockets) -> None:
    valve = make_valve(sockets, target_flow_lpm=10.0)
    assert valve.set_parameter("vazao_alvo", 60.0).parameters["vazao_alvo"] == 60.0
    flow = [m.value for m in valve.build_reading().measurements if m.metric == "vazao"][0]
    assert flow == pytest.approx(60.0, abs=3.0)


def test_valve_with_zero_target_flow_releases_no_water(sockets) -> None:
    valve = make_valve(sockets)
    valve.set_parameter("vazao_alvo", 0.0)
    values = {m.metric: m.value for m in valve.build_reading().measurements}
    assert values == {"vazao": 0.0, "volume": 0.0}


@pytest.mark.parametrize("name, value", [("vazao_alvo", -1.0), ("vazao_alvo", 1000.0), ("pressao", 1.0)])
def test_invalid_parameter_is_rejected(sockets, name: str, value: float) -> None:
    with pytest.raises(CommandRejected) as error:
        make_valve(sockets).set_parameter(name, value)
    assert error.value.code == grpc.StatusCode.INVALID_ARGUMENT


def test_threshold_on_a_source_without_threshold_is_rejected(sockets) -> None:
    with pytest.raises(CommandRejected) as error:
        make_valve(sockets).set_threshold(10.0)
    assert error.value.code == grpc.StatusCode.FAILED_PRECONDITION


def test_controllable_source_requires_grpc_port(sockets) -> None:
    listener, udp = sockets
    with pytest.raises(ValueError, match="grpc-port"):
        IrrigationValve(config(grpc_port=0), listener_sock=listener, udp_sock=udp)


# ----- falhas simuladas (#8) ------------------------------------------------------------

def test_silent_failure_stops_readings_and_discovery_until_it_expires(sockets) -> None:
    listener, udp = sockets
    clock = FakeClock(1_000.0)
    valve = IrrigationValve(config(), listener_sock=listener, udp_sock=udp, clock=clock)
    status = valve.simulate_failure(sources_pb2.FAILURE_SILENT, duration_s=30)
    assert status.failure == sources_pb2.FAILURE_SILENT and status.failure_until_ms == 1_030_000
    assert valve.build_reading() is None and valve.answers_discovery() is False
    clock.advance(31)
    assert valve.build_reading() is not None and valve.answers_discovery() is True
    assert valve.current_status().failure == sources_pb2.FAILURE_NONE


def test_out_of_range_failure_sends_faulty_values_until_cleared(sockets) -> None:
    valve = make_valve(sockets)
    valve.simulate_failure(sources_pb2.FAILURE_OUT_OF_RANGE, duration_s=0)
    assert all(m.value == FAULTY_VALUE for m in valve.build_reading().measurements)
    assert valve.simulate_failure(sources_pb2.FAILURE_NONE, 0).failure == sources_pb2.FAILURE_NONE
    assert all(m.value != FAULTY_VALUE for m in valve.build_reading().measurements)


def test_crash_failure_exits_the_process_after_answering(sockets, monkeypatch) -> None:
    monkeypatch.setattr("sources.control.CRASH_DELAY_S", 0.0)
    exits = []
    valve = make_valve(sockets, exit_process=exits.append)
    assert valve.simulate_failure(sources_pb2.FAILURE_CRASH, 0).failure == sources_pb2.FAILURE_CRASH
    deadline = time.monotonic() + 2
    while not exits and time.monotonic() < deadline:
        time.sleep(0.01)
    assert exits == [1]


def test_unknown_failure_mode_is_rejected(sockets) -> None:
    with pytest.raises(CommandRejected):
        make_valve(sockets).simulate_failure(42, 0)


# ----- sensor com limiar e evento imediato (#6) ------------------------------------------

def test_event_is_sent_once_when_temperature_crosses_the_threshold(sockets) -> None:
    _, udp = sockets
    sensor = make_sensor(sockets, threshold=30.0, ventilation_pct=50.0)  # base 26 °C
    assert sensor.sample_once() is False and udp.sent == []

    sensor.set_parameter("ventilacao", 0.0)  # fecha a ventilação: base 32 °C
    assert sensor.sample_once() is True
    assert sensor.sample_once() is False  # continua acima: sem repetir o alarme

    events = sent_readings(udp)
    assert len(events) == 1 and events[0].is_event
    assert events[0].measurements[0].value == pytest.approx(base_temperature(0.0))


def test_alarm_rearms_after_dropping_below_the_hysteresis(sockets) -> None:
    sensor = make_sensor(sockets, ventilation_pct=0.0)
    assert sensor.sample_once() is True
    sensor.set_parameter("ventilacao", 100.0)
    assert sensor.sample_once() is False  # 20 °C: rearma
    sensor.set_parameter("ventilacao", 0.0)
    assert sensor.sample_once() is True


def test_lowering_the_threshold_fires_an_event(sockets) -> None:
    sensor = make_sensor(sockets, threshold=30.0, ventilation_pct=50.0)
    assert sensor.sample_once() is False
    assert sensor.set_threshold(20.0).threshold == 20.0
    assert sensor.sample_once() is True


def test_no_event_while_powered_off_or_failing(sockets) -> None:
    _, udp = sockets
    sensor = make_sensor(sockets, ventilation_pct=0.0)
    sensor.set_power(False)
    assert sensor.sample_once() is False
    sensor.set_power(True)
    sensor.simulate_failure(sources_pb2.FAILURE_OUT_OF_RANGE, 0)
    assert sensor.sample_once() is False
    assert udp.sent == []


def test_periodic_reading_is_not_an_event(sockets) -> None:
    sensor = make_sensor(sockets, ventilation_pct=50.0)
    sensor.sample_once()
    reading = sensor.build_reading()
    assert not reading.is_event
    assert reading.measurements[0].metric == "temperatura_estufa"


def test_invalid_threshold_is_rejected(sockets) -> None:
    with pytest.raises(CommandRejected) as error:
        make_sensor(sockets).set_threshold(500.0)
    assert error.value.code == grpc.StatusCode.INVALID_ARGUMENT
