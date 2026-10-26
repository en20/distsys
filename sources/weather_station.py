"""Estação meteorológica (fonte contínua com várias grandezas, #7).

Cada leitura leva temperatura do ar (°C), umidade do ar (%) e velocidade do vento (km/h).

    python -m sources.weather_station --id estacao-1 --interval 15
"""

import random

from common.runtime import configure_logging
from sources.config import SourceConfig, build_arg_parser, config_from_args
from sources.continuous import ContinuousSource, MetricSpec
from sources.generator import ValueGenerator


def weather_station_source(config: SourceConfig, temperature_c: float = 27.0, seed: int | None = None,
                           **kwargs) -> ContinuousSource:
    rng = random.Random(seed)
    metrics = [
        MetricSpec("temperatura", "°C",
                   ValueGenerator(base=temperature_c, noise=0.2, drift=0.15, minimum=-10.0, maximum=50.0, rng=rng)),
        MetricSpec("umidade_ar", "%",
                   ValueGenerator(base=65.0, noise=1.0, drift=0.4, minimum=0.0, maximum=100.0, rng=rng)),
        MetricSpec("vento", "km/h",
                   ValueGenerator(base=10.0, noise=2.0, drift=0.5, minimum=0.0, maximum=120.0, rng=rng)),
    ]
    return ContinuousSource(config, metrics, **kwargs)


def main() -> None:
    parser = build_arg_parser("Estação meteorológica (fonte contínua)", default_type="ESTACAO_METEOROLOGICA")
    parser.add_argument("--temperatura", type=float, default=27.0, help="temperatura média em °C")
    parser.add_argument("--seed", type=int, help="semente (valores reproduzíveis)")
    args = parser.parse_args()
    try:
        source = weather_station_source(config_from_args(args), temperature_c=args.temperatura, seed=args.seed)
    except ValueError as exc:
        parser.error(str(exc))
    configure_logging(f"source:{source.config.source_id}")
    source.run_forever()


if __name__ == "__main__":
    main()
