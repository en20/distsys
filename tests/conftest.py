"""Fixtures das fontes do domínio (portadas de `enzo/fontes-de-dados`)."""

import random
import socket

import pytest

from common.net import configure_multicast_sender, open_udp_socket
from gateway.main import Gateway
from gateway.readings import ReadingStore
from gateway.registry import SourceRegistry
from sources.config import SourceConfig

LOOPBACK = "127.0.0.1"


def free_tcp_port() -> int:
    with socket.socket() as probe:
        probe.bind((LOOPBACK, 0))
        return probe.getsockname()[1]


@pytest.fixture
def mcast_port() -> int:
    return random.randint(50_001, 60_000)  # porta exclusiva por teste: fontes de testes diferentes não se enxergam


@pytest.fixture
def make_source(mcast_port):
    """Cria (e opcionalmente inicia) fontes com config rápida para testes; encerra todas no fim."""
    created = []

    def factory(cls, *, start: bool = True, source_id: str = "teste-1", interval: float = 0.5,
                exit_callback=None, grpc_port: int | None = None):
        if grpc_port is None:
            grpc_port = free_tcp_port() if cls.CONTROLLABLE else 0
        config = SourceConfig(source_id, cls.TYPE, interval, grpc_port, mcast_port=mcast_port,
                              mcast_interface=LOOPBACK)
        source = cls(config, seed=42, tick_s=0.05, exit_callback=exit_callback or (lambda: None))
        created.append(source)
        if start:
            source.start()
        return source

    yield factory
    for source in created:
        source.stop()


@pytest.fixture
def gateway(mcast_port):
    """Gateway real (descoberta, registro, receptor UDP, consultas) na loopback, sem SQLite."""
    sock = open_udp_socket(0)
    configure_multicast_sender(sock, ttl=1, interface_ip=LOOPBACK)
    gw = Gateway(sock, ("224.1.1.1", mcast_port), (LOOPBACK, sock.getsockname()[1]), rediscovery_s=0.3,
                 registry=SourceRegistry(min_timeout_s=0.5), store=ReadingStore())
    gw.start()
    yield gw
    gw.stop()
