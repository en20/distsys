"""Configuração de uma fonte via linha de comando/variáveis de ambiente (#4)."""

import argparse
import os
from dataclasses import dataclass

from common.net import DEFAULT_MCAST_GROUP, DEFAULT_MCAST_PORT


@dataclass(frozen=True)
class SourceConfig:
    source_id: str
    source_type: str
    interval_s: float
    grpc_port: int  # 0 = fonte não controlável
    mcast_group: str = DEFAULT_MCAST_GROUP
    mcast_port: int = DEFAULT_MCAST_PORT
    mcast_interface: str | None = None

    @property
    def controllable(self) -> bool:
        return self.grpc_port > 0


def build_arg_parser(description: str, default_type: str, default_interval_s: float = 15.0) -> argparse.ArgumentParser:
    """Parser com os argumentos comuns; subclasses podem adicionar os seus.

    Ex.: parser = build_arg_parser("Válvula", "VALVULA_IRRIGACAO"); parser.add_argument("--vazao")
    """
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--id", required=True, help="identificador único da fonte")
    parser.add_argument("--type", default=default_type, help="tipo da fonte (anunciado na descoberta)")
    parser.add_argument("--interval", type=float, default=default_interval_s, help="intervalo de envio em segundos")
    parser.add_argument("--grpc-port", type=int, default=0, help="porta do servidor gRPC (0 = não controlável)")
    parser.add_argument("--mcast-group", default=os.environ.get("DISTSYS_MCAST_GROUP", DEFAULT_MCAST_GROUP))
    parser.add_argument("--mcast-port", type=int, default=int(os.environ.get("DISTSYS_MCAST_PORT", DEFAULT_MCAST_PORT)))
    parser.add_argument("--mcast-if", default=os.environ.get("DISTSYS_MCAST_IF"), help="IP da interface multicast")
    return parser


def config_from_args(args: argparse.Namespace) -> SourceConfig:
    """Valida e converte os argumentos. Lança ValueError com o valor inválido."""
    if args.interval <= 0:
        raise ValueError(f"--interval deve ser > 0 segundos, recebido {args.interval!r}")
    if not 0 <= args.grpc_port <= 65535:
        raise ValueError(f"--grpc-port deve estar em [0, 65535], recebido {args.grpc_port!r}")
    if not args.id.strip():
        raise ValueError(f"--id não pode ser vazio, recebido {args.id!r}")
    return SourceConfig(
        source_id=args.id,
        source_type=args.type,
        interval_s=args.interval,
        grpc_port=args.grpc_port,
        mcast_group=args.mcast_group,
        mcast_port=args.mcast_port,
        mcast_interface=args.mcast_if,
    )
