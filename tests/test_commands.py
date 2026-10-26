"""Handler de comandos do Gateway (#13) contra uma fonte real com servidor gRPC na loopback."""

import random
import socket
import time

import pytest

from common.net import open_udp_socket
from gateway.commands import Command, CommandError, CommandErrorKind, CommandService, describe_status
from gateway.registry import SourceRegistry, SourceState
from pb import sources_pb2
from sources.config import SourceConfig
from sources.greenhouse import GreenhouseSensor
from sources.valve import IrrigationValve
from tests.builders import make_announcement


def free_tcp_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def announcement_for(source_id: str, port: int, interval_s: float = 5.0) -> sources_pb2.SourceAnnouncement:
    announcement = make_announcement(source_id, interval_s=interval_s, controllable=True, ip="127.0.0.1")
    announcement.grpc_port = port
    return announcement


class FakeStorage:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, str]] = []

    def record_event(self, source_id: str, kind: str, detail: str) -> None:
        self.events.append((source_id, kind, detail))


@pytest.fixture
def valve():
    port = free_tcp_port()
    source = IrrigationValve(SourceConfig("valvula-1", "VALVULA_IRRIGACAO", 5.0, port), rng=random.Random(1),
                             listener_sock=open_udp_socket(0))
    source.start()
    yield source
    source.stop()


@pytest.fixture
def registry(valve) -> SourceRegistry:
    registry = SourceRegistry()
    registry.upsert(announcement_for("valvula-1", valve.config.grpc_port))
    registry.upsert(make_announcement("umidade-1"))  # contínua
    return registry


@pytest.fixture
def storage() -> FakeStorage:
    return FakeStorage()


@pytest.fixture
def commands(registry, storage):
    service = CommandService(registry, timeout_s=1.0, storage=storage)
    yield service
    service.close()


def test_command_reaches_the_source_and_status_comes_back(commands, registry, valve, storage) -> None:
    status = commands.execute("valvula-1", Command.interval(2.5))
    assert status.send_interval_s == 2.5 and valve.publisher.interval_s == 2.5
    assert registry.get("valvula-1").status.send_interval_s == 2.5
    assert storage.events == [("valvula-1", "comando", "SetSendInterval(2.5s)")]


def test_power_off_updates_the_registry_state(commands, registry, valve) -> None:
    assert commands.execute("valvula-1", Command.power(False)).powered_on is False
    assert valve.powered_on is False
    assert registry.get("valvula-1").state == SourceState.POWERED_OFF
    commands.execute("valvula-1", Command.power(True))
    assert registry.get("valvula-1").state == SourceState.ONLINE


def test_parameter_and_get_status(commands, valve) -> None:
    commands.execute("valvula-1", Command.parameter("vazao_alvo", 35.0))
    assert commands.get_status("valvula-1").parameters["vazao_alvo"] == 35.0
    assert valve.parameter("vazao_alvo") == 35.0


def test_rejected_command_keeps_the_source_online(commands, registry) -> None:
    with pytest.raises(CommandError) as error:
        commands.execute("valvula-1", Command.interval(-1))
    assert error.value.kind == CommandErrorKind.REJECTED and "intervalo" in error.value.message
    with pytest.raises(CommandError) as error:
        commands.execute("valvula-1", Command.threshold(10))  # válvula não tem limiar
    assert error.value.kind == CommandErrorKind.REJECTED
    assert registry.get("valvula-1").state == SourceState.ONLINE


def test_unknown_and_non_controllable_sources(commands) -> None:
    with pytest.raises(CommandError) as error:
        commands.execute("nao-existe", Command.status())
    assert error.value.kind == CommandErrorKind.UNKNOWN_SOURCE
    with pytest.raises(CommandError) as error:
        commands.execute("umidade-1", Command.power(False))
    assert error.value.kind == CommandErrorKind.NOT_CONTROLLABLE


def test_unreachable_source_fails_fast_and_is_marked_offline(registry, storage) -> None:
    registry.upsert(announcement_for("valvula-morta", free_tcp_port()))  # ninguém escutando
    service = CommandService(registry, timeout_s=0.5, storage=storage)
    try:
        started = time.monotonic()
        with pytest.raises(CommandError) as error:
            service.execute("valvula-morta", Command.status())
        assert time.monotonic() - started < 3.0
        assert error.value.kind == CommandErrorKind.UNAVAILABLE
        assert registry.get("valvula-morta").state == SourceState.OFFLINE
    finally:
        service.close()


def test_simulated_crash_marks_the_source_offline(commands, registry, valve) -> None:
    exits = []
    valve._exit_process = exits.append
    status = commands.execute("valvula-1", Command.failure(sources_pb2.FAILURE_CRASH))
    assert status.failure == sources_pb2.FAILURE_CRASH
    assert registry.get("valvula-1").state == SourceState.OFFLINE


def test_sync_status_runs_in_background(commands, registry, valve) -> None:
    valve.set_parameter("vazao_alvo", 42.0)  # mudou depois do anúncio
    commands.sync_status_async("valvula-1").result(timeout=3)
    assert registry.get("valvula-1").status.parameters["vazao_alvo"] == 42.0


def test_threshold_command_on_the_greenhouse_sensor(registry, storage) -> None:
    port = free_tcp_port()
    sensor = GreenhouseSensor(SourceConfig("estufa-1", "SENSOR_TEMPERATURA_ESTUFA", 5.0, port),
                              listener_sock=open_udp_socket(0))
    sensor.start()
    registry.upsert(announcement_for("estufa-1", port))
    service = CommandService(registry, timeout_s=1.0, storage=storage)
    try:
        status = service.execute("estufa-1", Command.threshold(22.5))
        assert status.HasField("threshold") and status.threshold == 22.5
        assert "limiar 22.5" in describe_status(status)
    finally:
        service.close()
        sensor.stop()


# ----- Command.parse (texto da linha de comando) ------------------------------------------

@pytest.mark.parametrize("text, expected", [
    ("status", Command.status()),
    ("off", Command.power(False)),
    ("ON", Command.power(True)),
    ("interval 5", Command.interval(5.0)),
    ("threshold 30.5", Command.threshold(30.5)),
    ("param vazao_alvo 12", Command.parameter("vazao_alvo", 12.0)),
    ("fail silent 20", Command.failure(sources_pb2.FAILURE_SILENT, 20)),
    ("fail crash", Command.failure(sources_pb2.FAILURE_CRASH, 0)),
])
def test_parse(text: str, expected: Command) -> None:
    assert Command.parse(text.split()) == expected


@pytest.mark.parametrize("text", ["", "interval", "interval abc", "fail explode", "param x", "off now"])
def test_parse_rejects_invalid_text(text: str) -> None:
    with pytest.raises(ValueError, match="use:"):
        Command.parse(text.split())
