"""Estação meteorológica: fonte contínua (não controlável) com várias grandezas.

Leituras: temperatura (ciclo diário pelo relógio local), umidade do ar
(inversa à temperatura), chuva (episódios aleatórios) e vento.
"""

from __future__ import annotations

import math
import sys
import time

from sources.base import DataSource, MeasurementSpec
from sources.cli import run
from sources.pb import pb

RAIN_START_PROB_S = 1 / 900   # chance por segundo de começar a chover
RAIN_STOP_PROB_S = 1 / 300    # chance por segundo de parar


def diurnal_temperature(hour: float) -> float:
    """Temperatura típica (°C) para a hora do dia: mínima ~6h, máxima ~15h."""
    return 25.0 + 6.0 * math.sin(2 * math.pi * (hour - 9.0) / 24.0)


class WeatherStation(DataSource):
    """Estação meteorológica (contínua)."""

    TYPE = pb.WEATHER_STATION
    MEASUREMENTS = {
        "temperatura": MeasurementSpec("°C", -10.0, 50.0),
        "umidade_ar": MeasurementSpec("%", 0.0, 100.0),
        "chuva": MeasurementSpec("mm/h", 0.0, 200.0),
        "vento": MeasurementSpec("km/h", 0.0, 150.0),
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.temp_offset = 0.0
        self.wind = self.rng.uniform(5.0, 15.0)
        self.rain = 0.0
        self.temperature = diurnal_temperature(self._hour())
        self.humidity = 60.0

    @staticmethod
    def _hour() -> float:
        t = time.localtime()
        return t.tm_hour + t.tm_min / 60 + t.tm_sec / 3600

    def update(self, dt: float) -> None:
        g = self.rng.gauss
        self.temp_offset += -0.01 * self.temp_offset * dt + 0.05 * math.sqrt(dt) * g(0, 1)
        if self.rain == 0.0 and self.rng.random() < RAIN_START_PROB_S * dt:
            self.rain = self.rng.uniform(1.0, 15.0)
        elif self.rain > 0.0:
            if self.rng.random() < RAIN_STOP_PROB_S * dt:
                self.rain = 0.0
            else:
                self.rain = max(0.1, self.rain + 0.3 * math.sqrt(dt) * g(0, 1))
        cooling = 3.0 if self.rain else 0.0
        self.temperature = diurnal_temperature(self._hour()) + self.temp_offset - cooling
        target_humidity = 60.0 - 2.0 * (self.temperature - 25.0) + (30.0 if self.rain else 0.0)
        self.humidity += 0.1 * (target_humidity - self.humidity) * min(dt, 10.0) + 0.2 * math.sqrt(dt) * g(0, 1)
        self.humidity = min(100.0, max(5.0, self.humidity))
        self.wind += 0.02 * (10.0 - self.wind) * dt + 0.8 * math.sqrt(dt) * g(0, 1)
        self.wind = min(150.0, max(0.0, self.wind))

    def measure(self) -> dict[str, float]:
        return {"temperatura": self.temperature, "umidade_ar": self.humidity,
                "chuva": self.rain, "vento": self.wind}


def main(argv=None) -> int:
    return run(WeatherStation, argv, default_id="estacao-1", default_location="sede")


if __name__ == "__main__":
    sys.exit(main())
