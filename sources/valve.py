"""Válvula de irrigação: fonte controlável (atuador) com servidor gRPC.

Comandos: ligar/desligar (abrir/fechar), vazão alvo (`vazao_alvo_lpm`),
intervalo de envio e simulação de falha.

Leituras: `vazao` (L/min) e `volume` (litros consumidos desde a leitura
anterior; somando os volumes obtém-se o consumo de água do talhão).
"""

from __future__ import annotations

import math
import sys

from sources.datasource import DataSource, MeasurementSpec, ParameterSpec
from sources.cli import run

RESPONSE_TIME_S = 3.0  # constante de tempo da abertura/fechamento da válvula


class IrrigationValve(DataSource):
    """Válvula de irrigação (controlável)."""

    TYPE = "IRRIGATION_VALVE"
    CONTROLLABLE = True
    MEASUREMENTS = {
        "vazao": MeasurementSpec("L/min", 0.0, 100.0),
        "volume": MeasurementSpec("L", 0.0, 100_000.0),
    }
    PARAMETERS = {"vazao_alvo_lpm": ParameterSpec(12.0, 0.0, 100.0, "vazão com a válvula aberta")}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.flow_lpm = self.parameter("vazao_alvo_lpm")
        self.volume_l = 0.0

    def update(self, dt: float) -> None:
        target = self.parameter("vazao_alvo_lpm") if self.powered_on else 0.0
        target *= 1 + self.rng.gauss(0, 0.02)  # oscilação da pressão da rede
        alpha = 1 - math.exp(-dt / RESPONSE_TIME_S)
        self.flow_lpm = max(0.0, self.flow_lpm + (target - self.flow_lpm) * alpha)
        self.volume_l += self.flow_lpm * dt / 60

    def measure(self) -> dict[str, float]:
        return {"vazao": self.flow_lpm, "volume": self.volume_l}

    def after_send(self) -> None:
        self.volume_l = 0.0


def main(argv=None) -> int:
    return run(IrrigationValve, argv, default_grpc_port=50051)


if __name__ == "__main__":
    sys.exit(main())
