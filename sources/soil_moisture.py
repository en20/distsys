"""Sensor de umidade do solo de um talhão (fonte contínua, #7).

Cada instância tem média e variabilidade próprias, derivadas do `--id` (ou dadas por
`--base`/`--noise`), para que "talhão mais seco" e "fonte com maior variação" (#14)
tenham respostas claras com vários sensores rodando.

    python -m sources.soil_moisture --id umidade-talhao-1 --interval 15
    python -m sources.soil_moisture --id umidade-talhao-2 --base 40 --noise 1.5
"""

import random

from common.runtime import configure_logging
from sources.config import SourceConfig, build_arg_parser, config_from_args
from sources.continuous import ContinuousSource, MetricSpec
from sources.generator import ValueGenerator


def soil_moisture_source(
    config: SourceConfig,
    base: float | None = None,
    noise: float | None = None,
    seed: int | None = None,
    **kwargs,
) -> ContinuousSource:
    """Monta a fonte; sem `base`/`noise`, os valores são escolhidos a partir do id (estáveis entre execuções)."""
    profile = random.Random(config.source_id)  # mesmo id → mesmo perfil de talhão
    base = round(profile.uniform(18.0, 45.0), 1) if base is None else base
    noise = round(profile.uniform(0.3, 1.5), 2) if noise is None else noise
    generator = ValueGenerator(base=base, noise=noise, drift=noise / 2, minimum=0.0, maximum=100.0,
                               rng=random.Random(seed))
    return ContinuousSource(config, [MetricSpec("umidade_solo", "%", generator)], **kwargs)


def main() -> None:
    parser = build_arg_parser("Sensor de umidade do solo (fonte contínua)", default_type="SENSOR_UMIDADE_SOLO")
    parser.add_argument("--base", type=float, help="umidade média em %% (padrão: derivada do --id)")
    parser.add_argument("--noise", type=float, help="variação entre leituras (padrão: derivada do --id)")
    parser.add_argument("--seed", type=int, help="semente (valores reproduzíveis)")
    args = parser.parse_args()
    try:
        source = soil_moisture_source(config_from_args(args), base=args.base, noise=args.noise, seed=args.seed)
    except ValueError as exc:
        parser.error(str(exc))
    configure_logging(f"source:{source.config.source_id}")
    source.run_forever()


if __name__ == "__main__":
    main()
