"""Lado do Gateway na descoberta multicast (#10, requisitos 4a e 2b)."""

import itertools
import logging
from collections.abc import Callable

from common.net import Address, DatagramSender
from common.runtime import Clock, epoch_ms
from gateway.registry import SourceRegistry
from pb import sources_pb2

log = logging.getLogger(__name__)


class DiscoveryBroadcaster:
    """Monta e envia DiscoveryRequest ao grupo multicast.

    O Gateway chama `send_request()` ao iniciar e periodicamente (via PeriodicTask),
    para achar fontes que subirem depois ou reiniciarem.
    Ex.: DiscoveryBroadcaster(sock, ("224.1.1.1", 5007), ("192.168.0.5", 6000)).send_request()
    """

    def __init__(self, sock: DatagramSender, group: Address, advertise: Address, clock: Clock | None = None) -> None:
        self._sock = sock
        self._group = group
        self._advertise = advertise
        self._clock = clock
        self._request_ids = itertools.count(1)

    def build_request(self) -> sources_pb2.DiscoveryRequest:
        timestamp = epoch_ms(self._clock) if self._clock else epoch_ms()
        return sources_pb2.DiscoveryRequest(
            gateway_ip=self._advertise[0],
            gateway_udp_port=self._advertise[1],
            request_id=next(self._request_ids),
            timestamp_ms=timestamp,
        )

    def send_request(self) -> int:
        request = self.build_request()
        self._sock.sendto(request.SerializeToString(), self._group)
        log.info("DiscoveryRequest #%d enviado para %s:%d", request.request_id, *self._group)
        return request.request_id


class AnnouncementHandler:
    """Registra cada SourceAnnouncement recebido; anúncios repetidos só atualizam o registro.

    `on_controllable_discovered(source_id)` é chamado para fontes controláveis novas —
    o main liga isso ao GetStatus via gRPC (Jon, #13) para sincronizar o estado.
    Ex.: handler(announcement, ("192.168.0.7", 40123))
    """

    def __init__(
        self,
        registry: SourceRegistry,
        on_controllable_discovered: Callable[[str], None] = lambda _source_id: None,
    ) -> None:
        self._registry = registry
        self._on_controllable_discovered = on_controllable_discovered

    def __call__(self, announcement: sources_pb2.SourceAnnouncement, sender: Address) -> None:
        if not announcement.source_id:
            log.warning("Anúncio sem source_id ignorado (de %s:%d)", *sender)
            return
        if not announcement.ip:
            announcement.ip = sender[0]  # fonte não soube o próprio IP: usa o de origem do pacote
        is_new = self._registry.upsert(announcement)
        if not is_new:
            return
        _log_discovered(announcement)
        if announcement.controllable:
            self._on_controllable_discovered(announcement.source_id)


def _log_discovered(announcement: sources_pb2.SourceAnnouncement) -> None:
    kind = f"controlável, gRPC {announcement.ip}:{announcement.grpc_port}" if announcement.controllable else "contínua"
    log.info(
        "Fonte descoberta: %s (%s, %s), intervalo %.1fs",
        announcement.source_id, announcement.type, kind, announcement.initial_state.send_interval_s,
    )
