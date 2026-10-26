"""Ponto de entrada do Gateway (#27): liga descoberta, registro, UDP e detecção de falhas.

    python -m gateway.main [--udp-port 6000] [--tcp-port 7000] [--rediscovery 10] [--advertise-ip 192.168.0.5]

Pontos de encaixe dos outros fluxos (injetados aqui, sem import global nos módulos):
    - receptor/armazenamento de leituras (Enzo, #12) → substitui RegistryReadingSink em on_reading
    - consultas (Enzo, #14) e persistência (Enzo, #16)
    - cliente gRPC de comandos (Jon, #13) → on_controllable_discovered do AnnouncementHandler
    - servidor TCP do Cliente (LG, #15)
"""

import argparse
import logging
import os
import socket
from collections.abc import Callable

from common.loops import PeriodicTask
from common.net import (
    DEFAULT_GATEWAY_UDP_PORT,
    DEFAULT_MCAST_GROUP,
    DEFAULT_MCAST_PORT,
    Address,
    configure_multicast_sender,
    open_udp_socket,
    resolve_local_ip,
)
from common.runtime import configure_logging, wait_for_shutdown_signal
from gateway.client_dispatcher import ClientRequestDispatcher
from gateway.discovery import AnnouncementHandler, DiscoveryBroadcaster
from gateway.registry import SourceRegistry
from gateway.tcp_server import DEFAULT_CLIENT_TCP_PORT, ClientTcpServer
from gateway.udp_endpoint import GatewayUdpEndpoint
from pb import sources_pb2

log = logging.getLogger(__name__)

FAILURE_CHECK_INTERVAL_S = 1.0


class RegistryReadingSink:
    """Destino provisório das leituras: só mantém o registro vivo (last_seen, último valor).

    Leitura de fonte desconhecida (ex.: Gateway reiniciou) antecipa a redescoberta.
    Ex.: RegistryReadingSink(registry, rediscovery.trigger)(reading, sender)
    """

    def __init__(self, registry: SourceRegistry, request_rediscovery: Callable[[], None]) -> None:
        self._registry = registry
        self._request_rediscovery = request_rediscovery
        self._unknown_sources: set[str] = set()

    def __call__(self, reading: sources_pb2.SensorReading, sender: Address) -> None:
        if self._registry.touch(reading.source_id, reading):
            self._unknown_sources.discard(reading.source_id)
            return
        if reading.source_id not in self._unknown_sources:
            log.warning("Leitura de fonte desconhecida %s (%s:%d); pedindo redescoberta", reading.source_id, *sender)
            self._unknown_sources.add(reading.source_id)
        self._request_rediscovery()


class Gateway:
    """Componentes do Gateway já ligados entre si, com start/stop em ordem."""

    def __init__(
        self,
        sock: socket.socket,
        group: Address,
        advertise: Address,
        rediscovery_s: float,
        registry: SourceRegistry | None = None,
        client_server: ClientTcpServer | None = None,
    ) -> None:
        self.sock = sock
        self.registry = registry or SourceRegistry()
        self.broadcaster = DiscoveryBroadcaster(sock, group, advertise)
        self.rediscovery = PeriodicTask("rediscovery", rediscovery_s, self.broadcaster.send_request)
        self.failure_monitor = PeriodicTask("failure-monitor", FAILURE_CHECK_INTERVAL_S, self.registry.sweep)
        self.udp = GatewayUdpEndpoint(
            sock,
            on_announcement=AnnouncementHandler(self.registry),
            on_reading=RegistryReadingSink(self.registry, self.rediscovery.trigger),
        )
        self.client_server = client_server

    def start(self) -> None:
        # UDP primeiro: os anúncios podem chegar logo após o primeiro DiscoveryRequest.
        self.udp.start()
        self.failure_monitor.start()
        self.rediscovery.start()
        if self.client_server:
            self.client_server.start()

    def stop(self) -> None:
        if self.client_server:
            self.client_server.stop()
        self.rediscovery.stop()
        self.failure_monitor.stop()
        self.udp.stop()
        self.sock.close()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gateway do sistema de monitoramento")
    parser.add_argument("--udp-port", type=int, default=DEFAULT_GATEWAY_UDP_PORT, help="porta UDP (anúncios e leituras)")
    parser.add_argument("--tcp-port", type=int, default=DEFAULT_CLIENT_TCP_PORT, help="porta TCP dos Clientes")
    parser.add_argument("--mcast-group", default=os.environ.get("DISTSYS_MCAST_GROUP", DEFAULT_MCAST_GROUP))
    parser.add_argument("--mcast-port", type=int, default=int(os.environ.get("DISTSYS_MCAST_PORT", DEFAULT_MCAST_PORT)))
    parser.add_argument("--mcast-if", default=os.environ.get("DISTSYS_MCAST_IF"), help="IP da interface multicast")
    parser.add_argument("--mcast-ttl", type=int, default=1, help="TTL multicast (1 = só rede local)")
    parser.add_argument("--advertise-ip", help="IP que as fontes usam para falar com o Gateway (padrão: autodetectar)")
    parser.add_argument("--rediscovery", type=float, default=10.0, help="intervalo de redescoberta em segundos")
    return parser


def build_gateway(args: argparse.Namespace) -> Gateway:
    if args.rediscovery <= 0:
        raise ValueError(f"--rediscovery deve ser > 0 segundos, recebido {args.rediscovery!r}")
    sock = open_udp_socket(args.udp_port)
    configure_multicast_sender(sock, args.mcast_ttl, args.mcast_if)
    advertise_ip = args.advertise_ip or args.mcast_if or resolve_local_ip(args.mcast_group)
    advertise = (advertise_ip, sock.getsockname()[1])
    log.info("Gateway em %s:%d (UDP), descoberta em %s:%d", *advertise, args.mcast_group, args.mcast_port)
    registry = SourceRegistry()
    # commands/queries/storage entram aqui quando os fluxos do Jon (#13) e do Enzo (#14, #16) existirem;
    # enquanto isso o servidor responde à listagem e devolve ERROR_UNSUPPORTED nos outros pedidos.
    client_server = ClientTcpServer(ClientRequestDispatcher(registry), port=args.tcp_port)
    return Gateway(sock, (args.mcast_group, args.mcast_port), advertise, args.rediscovery, registry, client_server)


def main() -> None:
    configure_logging("gateway")
    gateway = build_gateway(build_arg_parser().parse_args())
    gateway.start()
    try:
        wait_for_shutdown_signal()
    finally:
        gateway.stop()
        log.info("Gateway encerrado")


if __name__ == "__main__":
    main()
