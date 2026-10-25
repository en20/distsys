"""Rede: escuta da descoberta via UDP multicast e utilitários de endereço."""

from __future__ import annotations

import logging
import socket
import struct
import threading
from typing import Callable

from google.protobuf.message import DecodeError

from sources.pb import pb

DEFAULT_MULTICAST_GROUP = "224.1.1.1"
DEFAULT_MULTICAST_PORT = 5007

DiscoveryHandler = Callable[["pb.DiscoveryRequest", tuple[str, int]], None]


def local_ip_for(dest_ip: str) -> str:
    """IP local da interface usada para alcançar `dest_ip` (não envia nada pela rede)."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        try:
            s.connect((dest_ip, 9))
            return s.getsockname()[0]
        except OSError:
            return "127.0.0.1"


class MulticastListener:
    """Entra no grupo multicast e repassa cada DiscoveryRequest recebido ao `handler`.

    Várias fontes na mesma máquina podem escutar o mesmo grupo/porta ao mesmo
    tempo (SO_REUSEADDR/SO_REUSEPORT); todas recebem cada mensagem.
    """

    def __init__(self, group: str, port: int, handler: DiscoveryHandler,
                 iface: str = "0.0.0.0", log: logging.Logger | None = None):
        self.group = group
        self.port = port
        self.iface = iface
        self._handler = handler
        self._log = log or logging.getLogger(__name__)
        self._sock: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    def start(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):  # macOS/BSD precisam dele para compartilhar a porta
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass
        sock.bind(("", self.port))
        mreq = struct.pack("4s4s", socket.inet_aton(self.group), socket.inet_aton(self.iface))
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        sock.settimeout(0.5)
        self._sock = sock
        self._thread = threading.Thread(target=self._loop, name="multicast", daemon=True)
        self._thread.start()
        self._log.info("aguardando descoberta no grupo multicast %s:%d", self.group, self.port)

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)
        if self._sock:
            self._sock.close()

    def _loop(self) -> None:
        assert self._sock is not None
        while not self._stop.is_set():
            try:
                data, sender = self._sock.recvfrom(65535)
            except socket.timeout:
                continue
            except OSError:
                break
            request = pb.DiscoveryRequest()
            try:
                request.ParseFromString(data)
            except DecodeError:
                self._log.warning("mensagem multicast inválida de %s:%d ignorada", *sender)
                continue
            if request.gateway_udp_port == 0:
                self._log.warning("DiscoveryRequest sem porta UDP do gateway (de %s:%d) ignorado", *sender)
                continue
            try:
                self._handler(request, sender)
            except Exception:  # noqa: BLE001 - não derrubar a escuta por erro no handler
                self._log.exception("erro tratando DiscoveryRequest")
