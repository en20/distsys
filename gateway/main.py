"""Ponto de entrada do Gateway (#27): liga descoberta, registro, UDP e detecção de falhas.

    python -m gateway.main [--udp-port 6000] [--tcp-port 7000] [--rediscovery 10] [--advertise-ip 192.168.0.5]
                           [--db data/gateway.db]

Pontos de encaixe dos outros fluxos (injetados aqui, sem import global nos módulos):
    - receptor/armazenamento de leituras (Enzo, #12) → ReadingReceiver em on_reading (usa o RegistryReadingSink)
    - consultas (Enzo, #14) → gateway.queries; persistência (Enzo, #16) → gateway.storage
    - cliente gRPC de comandos (Jon, #13) → gateway.commands (CommandService), GetStatus inicial
      ligado ao on_controllable_discovered do AnnouncementHandler
    - servidor TCP do Cliente (LG, #15) → gateway.client_server, com os módulos acima adaptados ao
      contrato do Cliente por gateway.client_adapters
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
from common.runtime import configure_logging, epoch_ms, wait_for_shutdown_signal
from gateway.client_adapters import ClientCommands, ClientHistory, ClientQueries
from gateway.client_dispatcher import ClientRequestDispatcher
from gateway.commands import CommandService
from gateway.discovery import AnnouncementHandler, DiscoveryBroadcaster
from gateway.queries import QueryEngine
from gateway.readings import DEFAULT_RETENTION_S, ReadingReceiver, ReadingStore
from gateway.registry import SourceRegistry
from gateway.storage import SqliteStorage
from gateway.tcp_server import DEFAULT_CLIENT_TCP_PORT, ClientTcpServer
from gateway.udp_endpoint import GatewayUdpEndpoint
from pb import sources_pb2

log = logging.getLogger(__name__)

FAILURE_CHECK_INTERVAL_S = 1.0
DEFAULT_DB_PATH = "data/gateway.db"


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
        store: ReadingStore | None = None,
        storage: SqliteStorage | None = None,
        client_port: int | None = None,
    ) -> None:
        self.sock = sock
        self.registry = registry or SourceRegistry()
        self.store = store if store is not None else ReadingStore()  # store vazio é falsy (__len__)
        self.storage = storage
        self.broadcaster = DiscoveryBroadcaster(sock, group, advertise)
        self.rediscovery = PeriodicTask("rediscovery", rediscovery_s, self.broadcaster.send_request)
        self.failure_monitor = PeriodicTask("failure-monitor", FAILURE_CHECK_INTERVAL_S, self.registry.sweep)
        self.receiver = ReadingReceiver(
            self.store, RegistryReadingSink(self.registry, self.rediscovery.trigger), storage
        )
        self.queries = QueryEngine(self.store, history=storage)
        self.commands = CommandService(self.registry, storage=storage)
        self.udp = GatewayUdpEndpoint(
            sock,
            on_announcement=AnnouncementHandler(self.registry, self.commands.sync_status_async),
            on_reading=self.receiver,
        )
        # Servidor TCP dos Clientes (LG, #15); None = sem Clientes (testes que só exercitam as fontes).
        self.client_server = None
        if client_port is not None:
            dispatcher = ClientRequestDispatcher(
                self.registry,
                commands=ClientCommands(self.commands),
                queries=ClientQueries(self.queries),
                storage=ClientHistory(storage) if storage is not None else None,
            )
            self.client_server = ClientTcpServer(dispatcher, port=client_port)

    def start(self) -> None:
        if self.storage is not None:
            self.storage.start()
            since_ms = epoch_ms() - int(self.store.retention_s * 1000)
            loaded = self.store.load(self.storage.readings(since_ms=since_ms))
            log.info("Histórico recarregado: %d leituras das últimas %.0f h", loaded, self.store.retention_s / 3600)
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
        self.commands.close()
        if self.storage is not None:
            self.storage.stop()
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
    parser.add_argument("--db", default=os.environ.get("DISTSYS_DB", DEFAULT_DB_PATH),
                        help=f"arquivo SQLite do histórico (padrão: {DEFAULT_DB_PATH}; vazio = sem persistência)")
    parser.add_argument("--retention-h", type=float, default=DEFAULT_RETENTION_S / 3600,
                        help="horas de leituras mantidas em memória para as consultas")
    return parser


def build_gateway(args: argparse.Namespace) -> Gateway:
    if args.rediscovery <= 0:
        raise ValueError(f"--rediscovery deve ser > 0 segundos, recebido {args.rediscovery!r}")
    if args.retention_h <= 0:
        raise ValueError(f"--retention-h deve ser > 0 horas, recebido {args.retention_h!r}")
    sock = open_udp_socket(args.udp_port)
    configure_multicast_sender(sock, args.mcast_ttl, args.mcast_if)
    advertise_ip = args.advertise_ip or args.mcast_if or resolve_local_ip(args.mcast_group)
    advertise = (advertise_ip, sock.getsockname()[1])
    log.info("Gateway em %s:%d (UDP), descoberta em %s:%d", *advertise, args.mcast_group, args.mcast_port)
    store = ReadingStore(retention_s=args.retention_h * 3600)
    storage = SqliteStorage(args.db) if args.db else None
    return Gateway(sock, (args.mcast_group, args.mcast_port), advertise, args.rediscovery, store=store, storage=storage,
                   client_port=args.tcp_port)


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
