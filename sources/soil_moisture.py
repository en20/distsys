"""Sensor de umidade do solo: fonte contínua (não controlável), só envia via UDP.

Cada instância representa um talhão com umidade média e variabilidade
próprias (sorteadas a partir do id), o que torna consultas como "talhão mais
seco" e "fonte com maior variação" interessantes. Suba várias instâncias:

    python -m sources.soil_moisture --id umidade-1 --location talhao-1
    python -m sources.soil_moisture --id umidade-2 --location talhao-2
"""

from __future__ import annotations

import math
import random
import sys
import zlib

from sources.base import DataSource, MeasurementSpec
from sources.cli import run
from sources.pb import pb

REVERSION = 0.01  # velocidade com que a umidade volta à média do talhão (1/s)


class SoilMoistureSensor(DataSource):
    """Sensor de umidade do solo (contínuo)."""

    TYPE = pb.SOIL_MOISTURE
    MEASUREMENTS = {"umidade_solo": MeasurementSpec("%", 0.0, 100.0)}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        profile = random.Random(zlib.crc32(self.config.source_id.encode()))  # perfil estável por id
        self.mean = profile.uniform(18.0, 45.0)
        self.volatility = profile.uniform(0.15, 0.8)
        self.moisture = self.mean + self.rng.gauss(0, 2)

    def update(self, dt: float) -> None:
        # Processo de Ornstein-Uhlenbeck: oscila em torno da média do talhão.
        noise = self.volatility * math.sqrt(dt) * self.rng.gauss(0, 1)
        self.moisture += REVERSION * (self.mean - self.moisture) * dt + noise
        self.moisture = min(100.0, max(0.0, self.moisture))

    def measure(self) -> dict[str, float]:
        return {"umidade_solo": self.moisture}


def main(argv=None) -> int:
    return run(SoilMoistureSensor, argv, default_id="umidade-1", default_location="talhao-1")


if __name__ == "__main__":
    sys.exit(main())
