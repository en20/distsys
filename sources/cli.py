"""Linha de comando comum às fontes do domínio (sobre os argumentos comuns de `sources.config`, #4)."""

from __future__ import annotations

import sys
from typing import Sequence

from common.runtime import configure_logging
from sources.config import build_arg_parser, config_from_args
from sources.datasource import DataSource


def run(source_cls: type[DataSource], argv: Sequence[str] | None = None, *,
        default_interval: float = 15.0, default_grpc_port: int = 0) -> int:
    description = (source_cls.__doc__ or "").strip().splitlines()[0]
    parser = build_arg_parser(description, default_type=source_cls.TYPE, default_interval_s=default_interval)
    parser.prog = f"python -m {source_cls.__module__}"
    parser.set_defaults(grpc_port=default_grpc_port)
    parser.add_argument("--seed", type=int, help="semente da simulação (resultados reproduzíveis)")
    args = parser.parse_args(argv)

    try:
        config = config_from_args(args)
        configure_logging(f"source:{config.source_id}")
        source = source_cls(config, seed=args.seed)
    except (ValueError, OSError) as exc:  # InvalidArgument é ValueError
        parser.exit(2, f"erro: {exc}\n")
    try:
        source.run_forever()  # Ctrl+C/SIGTERM → encerramento limpo
    except OSError as exc:  # ex.: porta gRPC ocupada
        parser.exit(2, f"erro: {exc}\n")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit("use: python -m sources.<fonte> (valve, reservoir, soil_moisture, weather_station)")
