"""Controle de ponta a ponta na loopback: descoberta → GetStatus inicial → comandos gRPC → leituras UDP."""

import random
import socket
import time

import pytest

from common.net import configure_multicast_sender, open_udp_socket
from gateway.commands import Command, CommandError, CommandErrorKind
from gateway.main import Gateway
from gateway.queries import QueryFilter
from gateway.readings import ReadingStore
from gateway.registry import SourceRegistry, SourceState
from pb import sources_pb2
from sources.config import SourceConfig
from sources.greenhouse import GreenhouseSensor
from sources.soil_moisture import soil_moisture_source
from sources.valve import IrrigationValve
from sources.weather_station import weather_station_source

LOOPBACK = "127.0.0.1"


def wait_until(condition, timeout_s: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return False


def free_tcp_port() -> int:
    with socket.socket() as probe:
        probe.bind((LOOPBACK, 0))
        return probe.getsockname()[1]


@pytest.fixture
def mcast_port() -> int:
    return random.randint(40_001, 50_000)  # isola de outras execuções e da porta real 5007


@pytest.fixture
def gateway(mcast_port: int):
    sock = open_udp_socket(0)
    configure_multicast_sender(sock, ttl=1, interface_ip=LOOPBACK)
    gw = Gateway(sock, ("224.1.1.1", mcast_port), (LOOPBACK, sock.getsockname()[1]), rediscovery_s=0.3,
                 registry=SourceRegistry(min_timeout_s=0.5), store=ReadingStore())
    gw.start()
    yield gw
    gw.stop()


def source_config(source_id: str, mcast_port: int, grpc_port: int = 0, interval_s: float = 0.2) -> SourceConfig:
    return SourceConfig(source_id, source_id.split("-")[0].upper(), interval_s, grpc_port,
                        mcast_port=mcast_port, mcast_interface=LOOPBACK)


def readings_of(gateway: Gateway, source_id: str):
    return gateway.store.readings(source_id=source_id)


def test_commands_from_the_gateway_change_what_the_valve_sends(gateway: Gateway, mcast_port: int) -> None:
    valve = IrrigationValve(source_config("valvula-1", mcast_port, free_tcp_port()), target_flow_lpm=10.0,
                            rng=random.Random(1))
    valve.start()
    try:
        assert wait_until(lambda: len(readings_of(gateway, "valvula-1")) >= 2)
        assert gateway.registry.get("valvula-1").controllable

        gateway.commands.execute("valvula-1", Command.parameter("vazao_alvo", 80.0))
        assert wait_until(lambda: readings_of(gateway, "valvula-1")[-1].measurements[0].value > 70.0)

        gateway.commands.execute("valvula-1", Command.power(False))
        assert gateway.registry.get("valvula-1").state == SourceState.POWERED_OFF
        time.sleep(0.3)
        count = len(readings_of(gateway, "valvula-1"))
        time.sleep(0.6)
        assert len(readings_of(gateway, "valvula-1")) == count  # desligada não envia
        assert gateway.registry.get("valvula-1").state == SourceState.POWERED_OFF  # e não expira

        gateway.commands.execute("valvula-1", Command.power(True))
        assert wait_until(lambda: len(readings_of(gateway, "valvula-1")) > count)
    finally:
        valve.stop()


def test_lowering_the_threshold_delivers_an_event_to_the_gateway(gateway: Gateway, mcast_port: int) -> None:
    sensor = GreenhouseSensor(source_config("estufa-1", mcast_port, free_tcp_port(), interval_s=5.0),
                              threshold=40.0, ventilation_pct=50.0, sample_period_s=0.1, rng=random.Random(1))
    sensor.start()
    try:
        assert wait_until(lambda: gateway.registry.get("estufa-1") is not None)
        assert gateway.receiver.alarm_counts() == {}

        gateway.commands.execute("estufa-1", Command.threshold(10.0))
        assert wait_until(lambda: gateway.receiver.alarm_counts().get("estufa-1") == 1)
        assert any(r.is_event for r in readings_of(gateway, "estufa-1"))
    finally:
        sensor.stop()


def test_silent_failure_takes_the_source_offline_and_it_comes_back(gateway: Gateway, mcast_port: int) -> None:
    valve = IrrigationValve(source_config("valvula-1", mcast_port, free_tcp_port(), interval_s=0.1))
    valve.start()
    try:
        assert wait_until(lambda: len(readings_of(gateway, "valvula-1")) >= 2)
        gateway.commands.execute("valvula-1", Command.failure(sources_pb2.FAILURE_SILENT, duration_s=2))
        assert wait_until(lambda: gateway.registry.get("valvula-1").state == SourceState.OFFLINE)
        assert wait_until(lambda: gateway.registry.get("valvula-1").state == SourceState.ONLINE, timeout_s=5.0)
    finally:
        valve.stop()


def test_stopped_source_is_reported_unavailable(gateway: Gateway, mcast_port: int) -> None:
    valve = IrrigationValve(source_config("valvula-1", mcast_port, free_tcp_port()))
    valve.start()
    assert wait_until(lambda: gateway.registry.get("valvula-1") is not None)
    valve.stop()  # como um Ctrl+C na fonte
    with pytest.raises(CommandError) as error:
        gateway.commands.execute("valvula-1", Command.status())
    assert error.value.kind == CommandErrorKind.UNAVAILABLE
    assert gateway.registry.get("valvula-1").state == SourceState.OFFLINE


def test_many_continuous_sources_are_discovered_and_send(gateway: Gateway, mcast_port: int) -> None:
    sources = [soil_moisture_source(source_config(f"umidade-{i}", mcast_port)) for i in range(3)]
    sources.append(weather_station_source(source_config("estacao-1", mcast_port)))
    for source in sources:
        source.start()
    try:
        ids = [s.config.source_id for s in sources]
        assert wait_until(lambda: all(len(readings_of(gateway, i)) >= 2 for i in ids))
        assert not any(gateway.registry.get(i).controllable for i in ids)
        with pytest.raises(CommandError) as error:
            gateway.commands.execute("umidade-0", Command.power(False))
        assert error.value.kind == CommandErrorKind.NOT_CONTROLLABLE
        driest = gateway.queries.run("menor_media", QueryFilter(metric="umidade_solo"), window_s=60)
        bases = {s.config.source_id: s.metrics[0].generator.base for s in sources[:3]}
        assert driest.source_id == min(bases, key=bases.get)  # talhão mais seco
    finally:
        for source in sources:
            source.stop()
