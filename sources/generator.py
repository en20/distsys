"""Gerador de valores simulados das fontes (#26).

Cada amostra = base + tendência + deriva lenta + ruído, limitada à faixa física.
A deriva é um passeio aleatório que volta devagar para a base: os valores
oscilam de forma contínua (sem saltos), o que deixa média, desvio padrão e
"fonte com maior variação" interessantes nas consultas agregadas.
"""

import math
import random
from dataclasses import dataclass, field


@dataclass
class ValueGenerator:
    """Gera a próxima leitura de uma grandeza simulada.

    Ex.:
        umidade = ValueGenerator(base=35.0, noise=0.5, drift=0.3, minimum=0.0, maximum=100.0)
        umidade.next(dt_s=15.0)  # -> 34.71
    """

    base: float
    noise: float = 0.5            # desvio padrão do ruído de cada amostra
    drift: float = 0.1            # intensidade da deriva lenta (por √s)
    trend_per_s: float = 0.0      # tendência linear da base (ex.: reservatório esvaziando)
    minimum: float = -math.inf
    maximum: float = math.inf
    reversion_per_s: float = 0.01  # rapidez com que a deriva volta para a base
    rng: random.Random = field(default_factory=random.Random, repr=False)
    _offset: float = field(default=0.0, init=False, repr=False)
    _elapsed_s: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.minimum > self.maximum:
            raise ValueError(f"faixa inválida: mínimo {self.minimum} > máximo {self.maximum}")
        for name in ("noise", "drift", "reversion_per_s"):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} não pode ser negativo, recebido {getattr(self, name)!r}")

    def next(self, dt_s: float = 1.0) -> float:
        """Avança `dt_s` segundos de simulação e devolve a nova amostra."""
        if dt_s < 0:
            raise ValueError(f"dt_s não pode ser negativo, recebido {dt_s!r}")
        self._elapsed_s += dt_s
        pull_back = min(self.reversion_per_s * dt_s, 1.0) * self._offset
        self._offset += -pull_back + self.drift * math.sqrt(dt_s) * self.rng.gauss(0.0, 1.0)
        value = self.center + self.noise * self.rng.gauss(0.0, 1.0)
        return self._clamp(value)

    def spike(self, amount: float) -> None:
        """Desloca o valor de repente (ex.: forçar um evento de limiar na demo); some aos poucos."""
        self._offset += amount

    @property
    def center(self) -> float:
        """Valor sem ruído no instante atual (base + tendência + deriva)."""
        return self.base + self.trend_per_s * self._elapsed_s + self._offset

    def _clamp(self, value: float) -> float:
        return min(self.maximum, max(self.minimum, value))
