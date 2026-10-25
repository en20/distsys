"""Integração das fontes com a rede: descoberta multicast, fluxo UDP e comandos gRPC."""

import socket
import time

import grpc
import pytest

from sources.pb import pb, pb_grpc
from sources.reservoir import ReservoirLevelSensor
from sources.soil_moisture import SoilMoistureSensor
from sources.valve import IrrigationValve
from sources.weather_station import WeatherStation
from tests.conftest import FakeGateway, requires_multicast

pytestmark = requires_multicast


@pytest.fixture
def stub_for():
    channels = []

    def factory(source):
        channel = grpc.insecure_channel(f"127.0.0.1:{source.grpc_port}")
        channels.append(channel)
        return pb_grpc.SourceControlStub(channel)

    yield factory
    for channel in channels:
        channel.close()


def readings_with(gw, source_id, predicate):
    return [r for r in gw.readings(source_id) if predicate(r)]


# ---------------------------------------------------------------------------
# Descoberta (requisitos 2b, 4a, 4b)
# ---------------------------------------------------------------------------

def test_sources_answer_discovery_with_type_address_and_initial_state(gateway, make_source):
    valve = make_source(IrrigationValve, source_id="valvula-1")
    soil = make_source(SoilMoistureSensor, source_id="umidade-1")

    gateway.discover(request_id=42)
    gateway.wait_for(lambda gw: len(gw.announcements()) >= 2)

    a_valve = gateway.announcements("valvula-1")[0]
    assert a_valve.info.type == pb.IRRIGATION_VALVE and a_valve.info.controllable
    assert a_valve.ip == "127.0.0.1"
    assert a_valve.grpc_port == valve.grpc_port != 0
    assert a_valve.request_id == 42
    assert a_valve.initial_state.powered_on and a_valve.initial_state.send_interval_s == 0.5
    assert list(a_valve.measurements) == ["vazao", "volume"]

    a_soil = gateway.announcements("umidade-1")[0]
    assert a_soil.info.type == pb.SOIL_MOISTURE and not a_soil.info.controllable
    assert a_soil.grpc_port == 0
    assert soil.gateway == ("127.0.0.1", gateway.port)


def test_no_data_is_sent_before_discovery(gateway, make_source):
    make_source(SoilMoistureSensor)
    time.sleep(1.0)
    assert gateway.messages == []

    gateway.discover()
    gateway.wait_for(lambda gw: gw.readings())


def test_several_instances_on_the_same_machine_are_all_discovered(gateway, make_source):
    ids = [f"umidade-{i}" for i in range(1, 4)] + ["estacao-1", "reservatorio-1"]
    for source_id in ids[:3]:
        make_source(SoilMoistureSensor, source_id=source_id)
    make_source(WeatherStation, source_id="estacao-1")
    make_source(ReservoirLevelSensor, source_id="reservatorio-1")

    gateway.discover()
    gateway.wait_for(lambda gw: {a.info.source_id for a in gw.announcements()} == set(ids))


def test_rediscovery_by_a_new_gateway_redirects_the_data(gateway, make_source, mcast_port):
    make_source(SoilMoistureSensor, source_id="umidade-1")
    gateway.discover(request_id=1)
    gateway.wait_for(lambda gw: gw.readings())

    new_gateway = FakeGateway(mcast_port)  # ex.: o Gateway reiniciou em outra porta
    try:
        new_gateway.discover(request_id=2)
        new_gateway.wait_for(lambda gw: gw.announcements() and gw.readings())
        assert new_gateway.announcements()[0].request_id == 2
    finally:
        new_gateway.close()


def test_same_gateway_rediscovery_just_reannounces(gateway, make_source):
    make_source(SoilMoistureSensor, source_id="umidade-1")
    gateway.discover(request_id=1)
    gateway.discover(request_id=2)
    gateway.wait_for(lambda gw: [a.request_id for a in gw.announcements()] == [1, 2])


def test_empty_gateway_ip_falls_back_to_sender_address(make_source):
    soil = make_source(SoilMoistureSensor, start=False)
    soil._on_discovery(pb.DiscoveryRequest(gateway_udp_port=6123, request_id=1), ("10.1.2.3", 40000))
    assert soil.gateway == ("10.1.2.3", 6123)


def test_invalid_multicast_messages_are_ignored(gateway, make_source, mcast_port):
    make_source(SoilMoistureSensor)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.sendto(b"\xff\xff\xff lixo", ("224.1.1.1", mcast_port))
        s.sendto(pb.DiscoveryRequest(gateway_ip="127.0.0.1").SerializeToString(), ("224.1.1.1", mcast_port))
    time.sleep(0.5)
    assert gateway.messages == []

    gateway.discover()
    gateway.wait_for(lambda gw: gw.announcements())


# ---------------------------------------------------------------------------
# Fluxo de dados UDP (requisitos 1b.iii, 2a.ii)
# ---------------------------------------------------------------------------

def test_periodic_readings_with_sequence_numbers(gateway, make_source):
    make_source(WeatherStation, source_id="estacao-1", interval=0.5)
    gateway.discover()
    gateway.wait_for(lambda gw: len(gw.readings()) >= 4, timeout=4)

    readings = gateway.readings("estacao-1")
    assert [r.seq for r in readings] == list(range(1, len(readings) + 1))
    gaps = [(b.timestamp_ms - a.timestamp_ms) / 1000 for a, b in zip(readings, readings[1:])]
    assert all(0.4 <= gap <= 0.7 for gap in gaps), gaps
    first = readings[0]
    assert first.powered_on and not first.is_event
    assert {m.name for m in first.measurements} == {"temperatura", "umidade_ar", "chuva", "vento"}
    assert {m.unit for m in first.measurements} == {"°C", "%", "mm/h", "km/h"}


def test_unreachable_gateway_does_not_stop_the_source(make_source, stub_for):
    valve = make_source(IrrigationValve)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("127.0.0.1", 0))
        dead_port = s.getsockname()[1]
    valve._on_discovery(pb.DiscoveryRequest(gateway_ip="127.0.0.1", gateway_udp_port=dead_port), ("127.0.0.1", 0))
    time.sleep(1.2)  # vários envios para uma porta fechada
    assert stub_for(valve).GetStatus(pb.StatusRequest(), timeout=2).powered_on


# ---------------------------------------------------------------------------
# Comandos gRPC (requisitos 1b.ii, 2a.i, 2c)
# ---------------------------------------------------------------------------

def test_power_off_turns_readings_into_heartbeats(gateway, make_source, stub_for):
    valve = make_source(IrrigationValve, source_id="valvula-1")
    stub = stub_for(valve)
    gateway.discover()
    gateway.wait_for(lambda gw: gw.readings())

    assert not stub.SetPower(pb.PowerRequest(on=False), timeout=2).powered_on
    gateway.clear()
    off = gateway.wait_for(lambda gw: gw.readings())[0]
    assert not off.powered_on and len(off.measurements) == 0

    assert stub.SetPower(pb.PowerRequest(on=True), timeout=2).powered_on
    gateway.wait_for(lambda gw: readings_with(gw, "valvula-1", lambda r: r.powered_on and r.measurements))


def test_interval_change_applies_immediately(gateway, make_source, stub_for):
    valve = make_source(IrrigationValve, interval=30)
    gateway.discover()
    gateway.wait_for(lambda gw: len(gw.readings()) == 1)  # primeira leitura sai logo após a descoberta

    status = stub_for(valve).SetSendInterval(pb.IntervalRequest(send_interval_s=0.5), timeout=2)
    assert status.send_interval_s == 0.5
    gateway.wait_for(lambda gw: len(gw.readings()) >= 3, timeout=2.5)  # sem esperar os 30 s


def test_parameter_change_is_reflected_in_readings(gateway, make_source, stub_for):
    valve = make_source(IrrigationValve)
    gateway.discover()
    status = stub_for(valve).SetParameter(pb.ParameterRequest(name="vazao_alvo_lpm", value=50), timeout=2)
    assert status.parameters["vazao_alvo_lpm"] == 50

    def high_flow(gw):
        return [r for r in gw.readings() for m in r.measurements if m.name == "vazao" and m.value > 45]

    gateway.wait_for(high_flow, timeout=8)


def test_threshold_crossing_sends_event_immediately(gateway, make_source, stub_for):
    res = make_source(ReservoirLevelSensor, source_id="reservatorio-1", interval=30)
    gateway.discover()
    gateway.wait_for(lambda gw: gw.readings())  # leitura inicial

    sent_at = time.monotonic()
    assert stub_for(res).SetThreshold(pb.ThresholdRequest(threshold=99), timeout=2).threshold == 99
    event = gateway.wait_for(lambda gw: [r for r in gw.readings() if r.is_event], timeout=2)[0]

    assert time.monotonic() - sent_at < 1.0  # não esperou o intervalo de 30 s
    assert "abaixo do limiar" in event.event_message
    assert event.measurements[0].name == "nivel" and event.measurements[0].value < 99


def test_grpc_errors_have_proper_status_codes(make_source, stub_for):
    valve = make_source(IrrigationValve)
    stub = stub_for(valve)
    with pytest.raises(grpc.RpcError) as err:
        stub.SetSendInterval(pb.IntervalRequest(send_interval_s=0), timeout=2)
    assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    with pytest.raises(grpc.RpcError) as err:
        stub.SetThreshold(pb.ThresholdRequest(threshold=10), timeout=2)
    assert err.value.code() == grpc.StatusCode.UNIMPLEMENTED
    with pytest.raises(grpc.RpcError) as err:
        stub.SetParameter(pb.ParameterRequest(name="nao_existe", value=1), timeout=2)
    assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT
    with pytest.raises(grpc.RpcError) as err:
        stub.SimulateFailure(pb.FailureRequest(mode=42), timeout=2)
    assert err.value.code() == grpc.StatusCode.INVALID_ARGUMENT


# ---------------------------------------------------------------------------
# Falhas simuladas
# ---------------------------------------------------------------------------

def test_silent_failure_stops_udp_and_recovers_after_duration(gateway, make_source, stub_for):
    valve = make_source(IrrigationValve, source_id="valvula-1")
    stub = stub_for(valve)
    gateway.discover()
    gateway.wait_for(lambda gw: gw.readings())

    stub.SimulateFailure(pb.FailureRequest(mode=pb.FAILURE_SILENT, duration_s=2), timeout=2)
    time.sleep(0.1)
    gateway.clear()
    gateway.discover(request_id=2)  # também não responde à descoberta enquanto "morta"
    time.sleep(1.5)
    assert gateway.messages == []
    assert stub.GetStatus(pb.StatusRequest(), timeout=2).failure_mode == pb.FAILURE_SILENT

    gateway.wait_for(lambda gw: gw.readings(), timeout=2)  # volta sozinha
    assert stub.GetStatus(pb.StatusRequest(), timeout=2).failure_mode == pb.FAILURE_NONE


def test_failure_can_be_cleared_on_demand(gateway, make_source, stub_for):
    valve = make_source(IrrigationValve)
    stub = stub_for(valve)
    gateway.discover()
    stub.SimulateFailure(pb.FailureRequest(mode=pb.FAILURE_SILENT), timeout=2)
    time.sleep(0.2)
    gateway.clear()
    time.sleep(0.8)
    assert gateway.readings() == []
    stub.SimulateFailure(pb.FailureRequest(mode=pb.FAILURE_NONE), timeout=2)
    gateway.wait_for(lambda gw: gw.readings(), timeout=1.5)


def test_out_of_range_failure_sends_impossible_values(gateway, make_source, stub_for):
    res = make_source(ReservoirLevelSensor)
    gateway.discover()
    stub_for(res).SimulateFailure(pb.FailureRequest(mode=pb.FAILURE_OUT_OF_RANGE), timeout=2)
    gateway.wait_for(lambda gw: [r for r in gw.readings()
                                 if not r.is_event and r.measurements and r.measurements[0].value > 100])


def test_crash_failure_answers_then_terminates(gateway, make_source, stub_for):
    exited = []
    valve = make_source(IrrigationValve, exit_callback=lambda: exited.append(True))
    stub = stub_for(valve)
    gateway.discover()

    status = stub.SimulateFailure(pb.FailureRequest(mode=pb.FAILURE_CRASH), timeout=2)
    assert status.failure_mode == pb.FAILURE_CRASH  # a resposta chega antes de cair
    deadline = time.monotonic() + 3
    while not exited and time.monotonic() < deadline:
        time.sleep(0.05)
    assert exited
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:  # o servidor gRPC cai logo depois
        try:
            stub.GetStatus(pb.StatusRequest(), timeout=0.5)
        except grpc.RpcError as err:
            assert err.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED)
            break
        time.sleep(0.05)
    else:
        pytest.fail("fonte continuou respondendo após o crash")
