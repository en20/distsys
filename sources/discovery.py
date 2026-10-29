"""Lado da fonte na descoberta multicast (#4, requisitos 2b e 4b)."""

import logging
import threading
from collections.abc import Callable

from common.net import Address, DatagramSender, resolve_local_ip
from pb import sources_pb2

log = logging.getLogger(__name__)

AnnouncementBuilder = Callable[[str, int], sources_pb2.SourceAnnouncement]  # (ip local, request_id)


class GatewayLocator:
    """Endereço UDP do Gateway, conhecido só depois da descoberta. Thread-safe.

    Ex.: addr = locator.get()  # None até o primeiro DiscoveryRequest
    """

    def __init__(self) -> None:
        self._address: Address | None = None
        self._lock = threading.Lock()
        self._known = threading.Event()

    def get(self) -> Address | None:
        with self._lock:
            return self._address

    def update(self, address: Address) -> bool:
        """Grava o endereço; True se mudou (primeira descoberta ou Gateway reiniciado em outro lugar)."""
        with self._lock:
            changed = address != self._address
            self._address = address
        self._known.set()
        return changed

    def wait(self, timeout_s: float | None = None) -> Address | None:
        self._known.wait(timeout_s)
        return self.get()


class DiscoveryResponder:
    """Responde cada DiscoveryRequest com SourceMessage{announcement} por UDP unicast.

    Ex.: responder.handle_datagram(sock.recv(65535))
    """

    def __init__(
        self,
        locator: GatewayLocator,
        reply_sock: DatagramSender,
        build_announcement: AnnouncementBuilder,
        on_gateway_changed: Callable[[Address], None] = lambda _addr: None,
    ) -> None:
        self._locator = locator
        self._reply_sock = reply_sock
        self._build_announcement = build_announcement
        self._on_gateway_changed = on_gateway_changed

    def handle_datagram(self, payload: bytes) -> None:
        request = _parse_discovery_request(payload)
        if request is None:
            return
        gateway = (request.gateway_ip, request.gateway_udp_port)
        if self._locator.update(gateway):
            log.info("Gateway descoberto em %s:%d", *gateway)
            self._on_gateway_changed(gateway)
        self.announce(gateway, request.request_id)

    def announce(self, gateway: Address, request_id: int) -> None:
        announcement = self._build_announcement(resolve_local_ip(gateway[0]), request_id)
        message = sources_pb2.SourceMessage(announcement=announcement)
        self._reply_sock.sendto(message.SerializeToString(), gateway)
        log.info("Anúncio enviado ao Gateway (request_id=%d)", request_id)


def _parse_discovery_request(payload: bytes) -> sources_pb2.DiscoveryRequest | None:
    request = sources_pb2.DiscoveryRequest()
    try:
        request.ParseFromString(payload)
    except Exception:
        log.warning("Pacote multicast inválido ignorado (%d bytes)", len(payload))
        return None
    if not request.gateway_ip or not request.gateway_udp_port:
        log.warning("DiscoveryRequest sem endereço do Gateway ignorado: %s", request)
        return None
    return request
