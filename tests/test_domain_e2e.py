"""Fontes do domínio com o Gateway real na loopback: descoberta, fluxo UDP, comandos gRPC e falhas.

Portado dos testes de rede de `enzo/fontes-de-dados`, trocando o FakeGateway pelo Gateway (#10–#14) e o
stub gRPC direto pelo `gateway.commands` (#13).
"""

import time

import grpc
import pytest

from gateway.commands import Command, CommandError, CommandErrorKind
from gateway.queries import QueryFilter
from gateway.registry import SourceState
from pb import sources_pb2 as pb
from pb import sources_pb2_grpc as pb_grpc
from sources.reservoir import ReservoirLevelSensor
from sources.soil_moisture import SoilMoistureSensor
from sources.valve import IrrigationValve
from sources.weather_station import WeatherStation


def wait_until(condition, timeout_s: float = 5.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        value = condition()
        if value:
            return value
        time.sleep(0.05)
    pytest.fail(f"condição não atingida em {timeout_s}s")


def readings(gateway, source_id: str):
    return gateway.store.readings(source_id=source_id)


def test_sources_are_discovered_with_type_address_and_initial_state(gateway, make_source):
    valve = make_source(IrrigationValve, source_id="valvula-1")
    make_source(SoilMoistureSensor, source_id="umidade-1")
    wait_until(lambda: gateway.registry.get("valvula-1") and gateway.registry.get("umidade-1"))

    record = gateway.registry.get("valvula-1")
    assert (record.source_type, record.controllable, record.grpc_port) == \
        ("IRRIGATION_VALVE", True, valve.config.grpc_port)
    assert record.status.parameters["vazao_alvo_lpm"] == 12
    assert not gateway.registry.get("umidade-1").controllable


def test_several_instances_stream_readings_and_answer_queries(gateway, make_source):
    sensors = [make_source(SoilMoistureSensor, source_id=f"umidade-{i}", interval=0.5) for i in range(3)]
    make_source(WeatherStation, source_id="estacao-1", interval=0.5)
    wait_until(lambda: all(len(readings(gateway, s.config.source_id)) >= 2 for s in sensors))

    station = readings(gateway, "estacao-1")[-1]
    assert {m.metric for m in station.measurements} == {"temperatura", "umidade_ar", "chuva", "vento"}
    driest = gateway.queries.run("menor_media", QueryFilter(metric="umidade_solo"), window_s=60)
    assert driest.source_id in {s.config.source_id for s in sensors}  # "talhão mais seco"


def test_interval_and_parameter_commands_change_the_stream(gateway, make_source):
    make_source(IrrigationValve, source_id="valvula-1", interval=0.5)
    wait_until(lambda: len(readings(gateway, "valvula-1")) >= 2)

    gateway.commands.execute("valvula-1", Command.parameter("vazao_alvo_lpm", 80))
    wait_until(lambda: readings(gateway, "valvula-1")[-1].measurements[0].value > 60, timeout_s=8)

    status = gateway.commands.execute("valvula-1", Command.interval(3.0))
    assert status.send_interval_s == 3.0
    count = len(readings(gateway, "valvula-1"))
    time.sleep(1.0)
    assert len(readings(gateway, "valvula-1")) <= count + 1


def test_power_off_stops_readings_and_gateway_shows_powered_off(gateway, make_source):
    make_source(IrrigationValve, source_id="valvula-1", interval=0.5)
    wait_until(lambda: len(readings(gateway, "valvula-1")) >= 2)

    gateway.commands.execute("valvula-1", Command.power(False))
    time.sleep(0.3)
    count = len(readings(gateway, "valvula-1"))
    time.sleep(0.8)
    assert len(readings(gateway, "valvula-1")) == count
    assert gateway.registry.get("valvula-1").state == SourceState.POWERED_OFF  # não expira por silêncio

    gateway.commands.execute("valvula-1", Command.power(True))
    wait_until(lambda: len(readings(gateway, "valvula-1")) > count)


def test_threshold_crossing_sends_event_immediately(gateway, make_source):
    reservoir = make_source(ReservoirLevelSensor, source_id="reservatorio-1", interval=60)
    wait_until(lambda: gateway.registry.get("reservatorio-1"))
    assert gateway.receiver.alarm_counts() == {}

    gateway.commands.execute("reservatorio-1", Command.threshold(reservoir.level + 10))  # nível fica abaixo
    wait_until(lambda: gateway.receiver.alarm_counts().get("reservatorio-1") == 1)
    assert readings(gateway, "reservatorio-1")[-1].is_event


def test_invalid_commands_are_rejected_with_grpc_codes(gateway, make_source):
    valve = make_source(IrrigationValve, source_id="valvula-1")
    make_source(SoilMoistureSensor, source_id="umidade-1")
    wait_until(lambda: gateway.registry.get("valvula-1") and gateway.registry.get("umidade-1"))

    with grpc.insecure_channel(f"127.0.0.1:{valve.config.grpc_port}") as channel:
        stub = pb_grpc.SourceControlStub(channel)
        with pytest.raises(grpc.RpcError) as error:
            stub.SetSendInterval(pb.IntervalRequest(interval_s=0.1), timeout=2)
        assert error.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        with pytest.raises(grpc.RpcError) as error:
            stub.SetThreshold(pb.ThresholdRequest(threshold=10), timeout=2)
        assert error.value.code() == grpc.StatusCode.UNIMPLEMENTED

    for command in (Command.interval(0.1), Command.threshold(10)):
        with pytest.raises(CommandError) as error:
            gateway.commands.execute("valvula-1", command)
        assert error.value.kind == CommandErrorKind.REJECTED
    with pytest.raises(CommandError) as error:
        gateway.commands.execute("umidade-1", Command.power(False))
    assert error.value.kind == CommandErrorKind.NOT_CONTROLLABLE


def test_silent_failure_takes_the_source_offline_and_it_recovers(gateway, make_source):
    make_source(IrrigationValve, source_id="valvula-1", interval=0.5)
    wait_until(lambda: len(readings(gateway, "valvula-1")) >= 2)

    gateway.commands.execute("valvula-1", Command.failure(pb.FAILURE_SILENT, duration_s=4))
    wait_until(lambda: gateway.registry.get("valvula-1").state == SourceState.OFFLINE)
    wait_until(lambda: gateway.registry.get("valvula-1").state == SourceState.ONLINE, timeout_s=8)


def test_out_of_range_failure_sends_impossible_values(gateway, make_source):
    make_source(IrrigationValve, source_id="valvula-1", interval=0.5)
    wait_until(lambda: len(readings(gateway, "valvula-1")) >= 1)
    gateway.commands.execute("valvula-1", Command.failure(pb.FAILURE_OUT_OF_RANGE))
    wait_until(lambda: readings(gateway, "valvula-1")[-1].measurements[0].value > IrrigationValve.MEASUREMENTS["vazao"].max)


def test_crash_failure_answers_then_terminates(gateway, make_source):
    crashed = []
    make_source(IrrigationValve, source_id="valvula-1", exit_callback=lambda: crashed.append(True))
    wait_until(lambda: gateway.registry.get("valvula-1"))

    status = gateway.commands.execute("valvula-1", Command.failure(pb.FAILURE_CRASH))
    assert status.failure == pb.FAILURE_CRASH  # a resposta chega antes do processo cair
    assert gateway.registry.get("valvula-1").state == SourceState.OFFLINE
    wait_until(lambda: crashed)


def test_stopped_source_is_reported_unavailable(gateway, make_source):
    valve = make_source(IrrigationValve, source_id="valvula-1")
    wait_until(lambda: gateway.registry.get("valvula-1"))
    valve.stop()  # como um Ctrl+C na fonte
    with pytest.raises(CommandError) as error:
        gateway.commands.execute("valvula-1", Command.status())
    assert error.value.kind == CommandErrorKind.UNAVAILABLE
    assert gateway.registry.get("valvula-1").state == SourceState.OFFLINE

