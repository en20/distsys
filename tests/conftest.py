"""Configuração compartilhada dos testes (rode `pytest` na raiz do repositório)."""

from __future__ import annotations

import socket
import struct
import sys
import threading
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _generate_protos_if_needed() -> None:
    protos = list((ROOT / "proto").glob("*.proto"))
    stale = any(not p.with_name(p.stem + "_pb2.py").exists()
                or p.stat().st_mtime > p.with_name(p.stem + "_pb2.py").stat().st_mtime for p in protos)
    if stale:
        sys.path.insert(0, str(ROOT / "scripts"))
        import gen_proto
        assert gen_proto.main() == 0, "falha ao gerar o código protobuf"


_generate_protos_if_needed()


def free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        s.bind(("", 0))
        return s.getsockname()[1]


def free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _multicast_works() -> bool:
    group, port = "224.1.1.1", free_udp_port()
    rx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    tx = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        rx.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        rx.bind(("", port))
        rx.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP,
                      struct.pack("4s4s", socket.inet_aton(group), socket.inet_aton("0.0.0.0")))
        rx.settimeout(1)
        tx.sendto(b"ping", (group, port))
        return rx.recvfrom(16)[0] == b"ping"
    except OSError:
        return False
    finally:
        rx.close()
        tx.close()


MULTICAST_OK = _multicast_works()
requires_multicast = pytest.mark.skipif(not MULTICAST_OK, reason="multicast indisponível neste ambiente")


class FakeGateway:
    """Gateway de mentira: envia DiscoveryRequest e coleta o que as fontes mandam por UDP."""

    def __init__(self, mcast_port: int):
        from sources.pb import pb
        self.pb = pb
        self.mcast_port = mcast_port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", 0))
        self.sock.settimeout(0.1)
        self.port = self.sock.getsockname()[1]
        self.messages: list = []
        self._cond = threading.Condition()
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                data, _ = self.sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                return
            msg = self.pb.SourceMessage()
            msg.ParseFromString(data)
            with self._cond:
                self.messages.append(msg)
                self._cond.notify_all()

    def discover(self, request_id: int = 1, gateway_ip: str = "127.0.0.1", group: str = "224.1.1.1") -> None:
        req = self.pb.DiscoveryRequest(gateway_ip=gateway_ip, gateway_udp_port=self.port, request_id=request_id)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
            s.sendto(req.SerializeToString(), (group, self.mcast_port))

    def announcements(self, source_id: str | None = None) -> list:
        with self._cond:
            return [m.announcement for m in self.messages if m.WhichOneof("payload") == "announcement"
                    and (source_id is None or m.announcement.info.source_id == source_id)]

    def readings(self, source_id: str | None = None) -> list:
        with self._cond:
            return [m.reading for m in self.messages if m.WhichOneof("payload") == "reading"
                    and (source_id is None or m.reading.info.source_id == source_id)]

    def wait_for(self, predicate, timeout: float = 5.0):
        """Espera até `predicate(self)` ser verdadeiro; devolve o valor ou falha o teste."""
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                self._cond.release()
                try:
                    value = predicate(self)
                finally:
                    self._cond.acquire()
                if value:
                    return value
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    pytest.fail(f"condição não atingida em {timeout}s; mensagens: {len(self.messages)}")
                self._cond.wait(min(remaining, 0.1))

    def clear(self) -> None:
        with self._cond:
            self.messages.clear()

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=1)
        self.sock.close()


@pytest.fixture
def mcast_port() -> int:
    return free_udp_port()  # porta exclusiva por teste: fontes de testes diferentes não se enxergam


@pytest.fixture
def gateway(mcast_port):
    gw = FakeGateway(mcast_port)
    yield gw
    gw.close()


@pytest.fixture
def make_source(mcast_port):
    """Cria (e opcionalmente inicia) fontes com config rápida para testes; encerra todas no fim."""
    from sources.base import SourceConfig

    created = []

    def factory(cls, *, start: bool = True, source_id: str = "teste-1", location: str = "talhao-1",
                interval: float = 0.5, exit_callback=None, **overrides):
        config = SourceConfig(source_id=source_id, location=location, send_interval_s=interval,
                              multicast_port=mcast_port, grpc_host="127.0.0.1", grpc_port=0,
                              tick_s=0.05, seed=42, **overrides)
        source = cls(config, exit_callback=exit_callback or (lambda: None))
        created.append(source)
        if start:
            source.start()
        return source

    yield factory
    for source in created:
        source.stop()
