"""Sensor de nível do reservatório: fonte controlável com limiar e evento imediato.

O nível é amostrado a cada tick (1 s). Além do envio periódico, quando o nível
cai abaixo do limiar a leitura é enviada IMEDIATAMENTE com `is_event = true`
(requisito 5c); quando volta ao normal, outro evento é enviado.

Comandos: ligar/desligar, limiar de alerta (%), intervalo de envio e falha.
"""

from __future__ import annotations

import sys

from sources.datasource import DataSource, MeasurementSpec, ParameterSpec
from sources.cli import run

DRAIN_PCT_S = 0.08      # consumo da irrigação (%/s)
REFILL_PCT_S = 0.40     # enchimento pela bomba (%/s)
PUMP_ON_BELOW = 20.0    # bomba liga abaixo deste nível
PUMP_OFF_ABOVE = 85.0   # e desliga acima deste
HYSTERESIS = 1.0        # evita alarmes repetidos com o ruído em torno do limiar


class ReservoirLevelSensor(DataSource):
    """Sensor de nível do reservatório (controlável, limiar de alerta)."""

    TYPE = "RESERVOIR_LEVEL"
    CONTROLLABLE = True
    MEASUREMENTS = {"nivel": MeasurementSpec("%", 0.0, 100.0)}
    THRESHOLD = ParameterSpec(30.0, 0.0, 100.0, "alerta quando o nível fica abaixo do limiar")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.level = self.rng.uniform(55.0, 75.0)
        self.pump_on = False
        self.in_alarm = False

    def update(self, dt: float) -> None:
        if self.level < PUMP_ON_BELOW:
            self.pump_on = True
        elif self.level > PUMP_OFF_ABOVE:
            self.pump_on = False
        rate = REFILL_PCT_S if self.pump_on else -DRAIN_PCT_S
        self.level = min(100.0, max(0.0, self.level + rate * dt + self.rng.gauss(0, 0.05)))

    def measure(self) -> dict[str, float]:
        return {"nivel": self.level}

    def check_event(self) -> str | None:
        threshold = self.threshold
        if not self.in_alarm and self.level < threshold:
            self.in_alarm = True
            return f"nível {self.level:.1f}% abaixo do limiar {threshold:g}%"
        if self.in_alarm and self.level > threshold + HYSTERESIS:
            self.in_alarm = False
            return f"nível normalizado: {self.level:.1f}% (limiar {threshold:g}%)"
        return None

    def on_power_changed(self, on: bool) -> None:
        self.in_alarm = False  # ao religar, reavalia e alerta de novo se continuar abaixo


def main(argv=None) -> int:
    return run(ReservoirLevelSensor, argv, default_grpc_port=50052)


if __name__ == "__main__":
    sys.exit(main())
