"""Classe base de todas as fontes de dados (#4).

Cada fonte roda como processo separado (requisito 2a), escuta o grupo multicast
de descoberta e responde ao Gateway com seu tipo, IP, porta gRPC e estado inicial.
O envio periódico de leituras fica em `sources/udp_sender.py` (Enzo, #26).

Rodar uma fonte de teste (só descoberta, sem leituras):
    python -m sources.base --id probe-1 --type PROBE
"""

import logging
import socket

from common.loops import BackgroundLoop
from common.net import RECV_BUFFER_BYTES, Address, open_multicast_listener, open_udp_socket
from common.runtime import configure_logging, wait_for_shutdown_signal
from pb import sources_pb2
from sources.config import SourceConfig, build_arg_parser, config_from_args
from sources.discovery import DiscoveryResponder, GatewayLocator

log = logging.getLogger(__name__)


class SourceProcess:
    """Base de uma fonte: descoberta multicast + endereço do Gateway + ciclo de vida.

    Subclasses sobrescrevem `current_status()` (estado reportado na descoberta) e,
    se quiserem reagir à descoberta, `on_gateway_discovered()`. Leituras só devem ser
    enviadas quando `gateway_address()` não for None.

    Ex.:
        class Valvula(SourceProcess):
            def current_status(self) -> sources_pb2.SourceStatus: ...
        Valvula(config).run_forever()
    """

    def __init__(
        self,
        config: SourceConfig,
        listener_sock: socket.socket | None = None,
        udp_sock: socket.socket | None = None,
    ) -> None:
        self.config = config
        self.locator = GatewayLocator()
        self.udp_sock = udp_sock or open_udp_socket()
        self._listener_sock = listener_sock or self._open_listener()
        self._responder = DiscoveryResponder(
            self.locator, self.udp_sock, self.build_announcement, self.on_gateway_discovered
        )
        self._discovery_loop = BackgroundLoop(f"discovery-{config.source_id}", self._receive_discovery)

    # ----- pontos de extensão -------------------------------------------------

    def current_status(self) -> sources_pb2.SourceStatus:
        """Estado operacional atual; a versão base só reporta ligada + intervalo."""
        return sources_pb2.SourceStatus(
            source_id=self.config.source_id,
            powered_on=True,
            send_interval_s=self.config.interval_s,
        )

    def on_gateway_discovered(self, gateway: Address) -> None:
        """Chamado quando o Gateway é descoberto ou muda de endereço."""

    def answers_discovery(self) -> bool:
        """False faz a fonte ignorar DiscoveryRequest (ex.: falha simulada silenciosa, #8)."""
        return True

    # ----- API para subclasses ------------------------------------------------

    def gateway_address(self) -> Address | None:
        return self.locator.get()

    def build_announcement(self, local_ip: str, request_id: int) -> sources_pb2.SourceAnnouncement:
        return sources_pb2.SourceAnnouncement(
            source_id=self.config.source_id,
            type=self.config.source_type,
            ip=local_ip,
            grpc_port=self.config.grpc_port,
            controllable=self.config.controllable,
            initial_state=self.current_status(),
            request_id=request_id,
        )

    # ----- ciclo de vida -------------------------------------------------------

    def start(self) -> None:
        self._discovery_loop.start()
        log.info(
            "Fonte %s (%s) aguardando descoberta em %s:%d",
            self.config.source_id, self.config.source_type, self.config.mcast_group, self.config.mcast_port,
        )

    def stop(self) -> None:
        self._discovery_loop.stop()
        self._listener_sock.close()
        self.udp_sock.close()
        log.info("Fonte %s encerrada", self.config.source_id)

    def run_forever(self) -> None:
        """Sobe a fonte e bloqueia até Ctrl+C/SIGTERM, encerrando de forma limpa."""
        self.start()
        try:
            wait_for_shutdown_signal()
        finally:
            self.stop()

    # ----- internos -------------------------------------------------------------

    def _open_listener(self) -> socket.socket:
        sock = open_multicast_listener(self.config.mcast_group, self.config.mcast_port, self.config.mcast_interface)
        sock.settimeout(0.5)
        return sock

    def _receive_discovery(self) -> None:
        payload = self._listener_sock.recv(RECV_BUFFER_BYTES)
        if self.answers_discovery():
            self._responder.handle_datagram(payload)


def main() -> None:
    args = build_arg_parser("Fonte de teste (só descoberta)", default_type="PROBE").parse_args()
    config = config_from_args(args)
    configure_logging(f"source:{config.source_id}")
    SourceProcess(config).run_forever()


if __name__ == "__main__":
    main()
