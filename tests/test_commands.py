"""Handler de comandos do Gateway (#13) contra um servidor gRPC SourceControl falso na loopback."""

import socket
import time
from concurrent import futures

import grpc
import pytest

from common.net import configure_multicast_sender, open_udp_socket
from gateway.commands import Command, CommandError, CommandErrorKind, CommandService, describe_status
from gateway.main import Gateway
from gateway.readings import ReadingStore
from gateway.registry import SourceRegistry, SourceState
from pb import sources_pb2, sources_pb2_grpc
from sources.base import SourceProcess
from sources.config import SourceConfig
from tests.builders import make_announcement
from tests.conftest import random_udp_port

LOOPBACK = "127.0.0.1"


class FakeSourceControl(sources_pb2_grpc.SourceControlServicer):
    """Fonte controlável mínima: guarda o estado e recusa valores inválidos como uma fonte real."""

    def __init__(self, source_id: str = "valvula-1") -> None:
        self.status = sources_pb2.SourceStatus(source_id=source_id, powered_on=True, send_interval_s=5.0,
                                               parameters={"vazao_alvo": 20.0})
        self.calls: list[str] = []
        self.delay_s = 0.0

    def GetStatus(self, request, context):
        return self._reply("GetStatus", context)

    def SetPower(self, request, context):
        self.status.powered_on = request.on
        return self._reply("SetPower", context)

    def SetSendInterval(self, request, context):
        if request.interval_s <= 0:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "intervalo deve ser > 0")
        self.status.send_interval_s = request.interval_s
        return self._reply("SetSendInterval", context)

    def SetThreshold(self, request, context):
        context.abort(grpc.StatusCode.UNIMPLEMENTED, "a fonte não tem limiar")

    def SetParameter(self, request, context):
        self.status.parameters[request.name] = request.value
        return self._reply("SetParameter", context)

    def SimulateFailure(self, request, context):
        self.status.failure = request.mode
        return self._reply("SimulateFailure", context)

    def _reply(self, name: str, context) -> sources_pb2.SourceStatus:
        self.calls.append(name)
        time.sleep(self.delay_s)
        return self.status


def free_tcp_port() -> int:
    with socket.socket() as probe:
        probe.bind((LOOPBACK, 0))
        return probe.getsockname()[1]


def announcement_for(source_id: str, port: int) -> sources_pb2.SourceAnnouncement:
    announcement = make_announcement(source_id, interval_s=5.0, controllable=True, ip=LOOPBACK)
    announcement.grpc_port = port
    return announcement


class FakeStorage:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, str]] = []

    def record_event(self, source_id: str, kind: str, detail: str) -> None:
        self.events.append((source_id, kind, detail))


@pytest.fixture
def fake_source():
    servicer, port = FakeSourceControl(), free_tcp_port()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
    sources_pb2_grpc.add_SourceControlServicer_to_server(servicer, server)
    server.add_insecure_port(f"{LOOPBACK}:{port}")
    server.start()
    servicer.port = port
    yield servicer
    server.stop(grace=None)


@pytest.fixture
def registry(fake_source) -> SourceRegistry:
    registry = SourceRegistry()
    registry.upsert(announcement_for("valvula-1", fake_source.port))
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


def test_command_reaches_the_source_and_status_comes_back(commands, registry, fake_source, storage) -> None:
    status = commands.execute("valvula-1", Command.interval(2.5))
    assert status.send_interval_s == 2.5 and fake_source.calls == ["SetSendInterval"]
    assert registry.get("valvula-1").status.send_interval_s == 2.5
    assert storage.events == [("valvula-1", "comando", "SetSendInterval(2.5s)")]


def test_power_off_updates_the_registry_state(commands, registry) -> None:
    assert commands.execute("valvula-1", Command.power(False)).powered_on is False
    assert registry.get("valvula-1").state == SourceState.POWERED_OFF
    commands.execute("valvula-1", Command.power(True))
    assert registry.get("valvula-1").state == SourceState.ONLINE


def test_parameter_and_get_status(commands, storage) -> None:
    commands.execute("valvula-1", Command.parameter("vazao_alvo", 35.0))
    assert commands.get_status("valvula-1").parameters["vazao_alvo"] == 35.0
    assert [detail for _, _, detail in storage.events] == ["SetParameter(vazao_alvo=35)"]  # GetStatus não entra


def test_channel_is_reused_between_commands(registry, fake_source) -> None:
    created = []

    def factory(target: str) -> grpc.Channel:
        created.append(target)
        return grpc.insecure_channel(target)

    service = CommandService(registry, timeout_s=1.0, channel_factory=factory)
    try:
        service.get_status("valvula-1")
        service.get_status("valvula-1")
        assert created == [f"{LOOPBACK}:{fake_source.port}"]
    finally:
        service.close()


def test_rejected_command_keeps_the_source_online(commands, registry) -> None:
    with pytest.raises(CommandError) as error:
        commands.execute("valvula-1", Command.interval(-1))
    assert error.value.kind == CommandErrorKind.REJECTED and "intervalo" in error.value.message
    with pytest.raises(CommandError) as error:
        commands.execute("valvula-1", Command.threshold(10))
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


def test_slow_source_hits_the_deadline(registry, fake_source) -> None:
    fake_source.delay_s = 1.0
    service = CommandService(registry, timeout_s=0.2)
    try:
        with pytest.raises(CommandError) as error:
            service.get_status("valvula-1")
        assert error.value.kind == CommandErrorKind.UNAVAILABLE
        assert registry.get("valvula-1").state == SourceState.OFFLINE
    finally:
        service.close()


def test_simulated_crash_marks_the_source_offline(commands, registry) -> None:
    status = commands.execute("valvula-1", Command.failure(sources_pb2.FAILURE_CRASH))
    assert status.failure == sources_pb2.FAILURE_CRASH
    assert registry.get("valvula-1").state == SourceState.OFFLINE


def test_sync_status_runs_in_background(commands, registry, fake_source) -> None:
    fake_source.status.parameters["vazao_alvo"] = 42.0  # mudou depois do anúncio
    commands.sync_status_async("valvula-1").result(timeout=3)
    assert registry.get("valvula-1").status.parameters["vazao_alvo"] == 42.0


def test_describe_status() -> None:
    status = sources_pb2.SourceStatus(powered_on=False, send_interval_s=5.0, threshold=30.0,
                                      parameters={"vazao_alvo": 12.0}, failure=sources_pb2.FAILURE_SILENT)
    assert describe_status(status) == "desligada, intervalo 5s, limiar 30, vazao_alvo=12, FALHA FAILURE_SILENT"


# ----- integração com o Gateway: descoberta → GetStatus inicial → comando ---------------------

def test_gateway_syncs_a_discovered_controllable_source(fake_source) -> None:
    mcast_port = random_udp_port(40_001, 50_000)
    sock = open_udp_socket(0)
    configure_multicast_sender(sock, ttl=1, interface_ip=LOOPBACK)
    gateway = Gateway(sock, ("224.1.1.1", mcast_port), (LOOPBACK, sock.getsockname()[1]), rediscovery_s=5.0,
                      registry=SourceRegistry(), store=ReadingStore())
    # Fonte que só faz descoberta (base do Marcelo) anunciando a porta do servidor gRPC falso.
    source = SourceProcess(SourceConfig("valvula-1", "VALVULA_IRRIGACAO", 5.0, fake_source.port,
                                        mcast_port=mcast_port, mcast_interface=LOOPBACK))
    source.start()  # já inscrita no grupo antes do primeiro DiscoveryRequest
    gateway.start()  # redescoberta lenta: nenhum anúncio novo sobrescreve o estado durante o teste
    try:
        deadline = time.monotonic() + 5
        while "GetStatus" not in fake_source.calls and time.monotonic() < deadline:
            time.sleep(0.05)
        assert fake_source.calls[:1] == ["GetStatus"]  # sincronizou ao descobrir
        gateway.commands.execute("valvula-1", Command.power(False))
        assert gateway.registry.get("valvula-1").state == SourceState.POWERED_OFF
    finally:
        source.stop()
        gateway.stop()


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
