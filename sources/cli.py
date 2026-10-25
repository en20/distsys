"""Linha de comando comum a todas as fontes."""

from __future__ import annotations

import argparse
import logging
import signal
import sys
from typing import Sequence

from sources.base import DataSource, InvalidArgument, SourceConfig
from sources.net import DEFAULT_MULTICAST_GROUP, DEFAULT_MULTICAST_PORT


def run(source_cls: type[DataSource], argv: Sequence[str] | None = None, *, default_id: str,
        default_location: str, default_interval: float = 15.0, default_grpc_port: int = 0) -> int:
    parser = argparse.ArgumentParser(prog=f"python -m {source_cls.__module__}",
                                     description=(source_cls.__doc__ or "").strip().splitlines()[0])
    parser.add_argument("--id", default=default_id, help=f"identificador único da fonte (padrão: {default_id})")
    parser.add_argument("--location", default=default_location, help=f"talhão/local (padrão: {default_location})")
    parser.add_argument("--interval", type=float, default=default_interval,
                        help=f"intervalo de envio em segundos (padrão: {default_interval:g})")
    if source_cls.CONTROLLABLE:
        parser.add_argument("--grpc-port", type=int, default=default_grpc_port,
                            help=f"porta do servidor gRPC (padrão: {default_grpc_port}; 0 = porta livre)")
        parser.add_argument("--grpc-host", default="0.0.0.0", help="interface do servidor gRPC (padrão: 0.0.0.0)")
    parser.add_argument("--mcast-group", default=DEFAULT_MULTICAST_GROUP, help="grupo multicast de descoberta")
    parser.add_argument("--mcast-port", type=int, default=DEFAULT_MULTICAST_PORT, help="porta multicast de descoberta")
    parser.add_argument("--mcast-iface", default="0.0.0.0", help="IP da interface para o multicast")
    parser.add_argument("--advertise-ip", help="IP anunciado ao Gateway (padrão: detectado automaticamente)")
    parser.add_argument("--seed", type=int, help="semente da simulação (resultados reproduzíveis)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level, format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
                        datefmt="%H:%M:%S")
    config = SourceConfig(
        source_id=args.id, location=args.location, send_interval_s=args.interval,
        multicast_group=args.mcast_group, multicast_port=args.mcast_port, multicast_iface=args.mcast_iface,
        advertise_ip=args.advertise_ip, seed=args.seed,
        grpc_host=getattr(args, "grpc_host", "0.0.0.0"), grpc_port=getattr(args, "grpc_port", 0),
    )
    try:
        source = source_cls(config)
        source.start()
    except (InvalidArgument, OSError) as exc:
        parser.exit(2, f"erro: {exc}\n")

    def shutdown(signum, frame):  # noqa: ARG001
        source.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    source.wait()
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit("use: python -m sources.<fonte> (valve, reservoir, soil_moisture, weather_station)")
