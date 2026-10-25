"""Descoberta de ponta a ponta com sockets reais na loopback (multicast via 127.0.0.1)."""

import random
import time

import pytest

from common.net import configure_multicast_sender, open_udp_socket
from gateway.main import Gateway
from gateway.registry import SourceRegistry, SourceState
from sources.base import SourceProcess
from sources.config import SourceConfig

LOOPBACK = "127.0.0.1"


def wait_until(condition, timeout_s: float = 3.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return False


@pytest.fixture
def mcast_port() -> int:
    return random.randint(20_000, 30_000)  # isola de outras execuções e da porta real 5007


def start_gateway(mcast_port: int, registry: SourceRegistry | None = None) -> Gateway:
    sock = open_udp_socket(0)
    configure_multicast_sender(sock, ttl=1, interface_ip=LOOPBACK)
    advertise = (LOOPBACK, sock.getsockname()[1])
    gateway = Gateway(sock, ("224.1.1.1", mcast_port), advertise, rediscovery_s=0.3, registry=registry)
    gateway.start()
    return gateway


def start_source(source_id: str, mcast_port: int, grpc_port: int = 0) -> SourceProcess:
    config = SourceConfig(source_id, "PROBE", 1.0, grpc_port, mcast_port=mcast_port, mcast_interface=LOOPBACK)
    source = SourceProcess(config)
    source.start()
    return source


def test_sources_started_before_and_after_gateway_are_discovered(mcast_port: int) -> None:
    early = start_source("early", mcast_port, grpc_port=50999)
    gateway = start_gateway(mcast_port)
    late = start_source("late", mcast_port)
    try:
        assert wait_until(lambda: len(gateway.registry.list()) == 2)
        record = gateway.registry.get("early")
        assert (record.ip, record.grpc_port, record.controllable) == (LOOPBACK, 50999, True)
        assert late.gateway_address() == (LOOPBACK, gateway.sock.getsockname()[1])
    finally:
        for process in (early, late):
            process.stop()
        gateway.stop()


def test_killed_source_goes_offline_and_returns_online_when_restarted(mcast_port: int) -> None:
    # timeout = max(3 × intervalo de 1 s, 0.5) = 3 s, em vez do mínimo padrão de 5 s
    gateway = start_gateway(mcast_port, SourceRegistry(min_timeout_s=0.5))
    source = start_source("s1", mcast_port)
    try:
        assert wait_until(lambda: gateway.registry.get("s1") is not None)
        source.stop()
        assert wait_until(lambda: gateway.registry.get("s1").state == SourceState.OFFLINE, timeout_s=5.0)
        source = start_source("s1", mcast_port)
        assert wait_until(lambda: gateway.registry.get("s1").state == SourceState.ONLINE)
    finally:
        source.stop()
        gateway.stop()
