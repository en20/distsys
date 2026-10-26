"""Fonte controlável 2 — sensor de temperatura da estufa com limiar e evento imediato (#6, requisito 5c).

A temperatura é amostrada internamente a cada `--sample-period` s (padrão 1 s), mais rápido
que o envio periódico (`--interval`, padrão 15 s). Quando a temperatura passa de abaixo para
acima do limiar, a fonte envia na hora uma leitura com `is_event = true`, sem esperar o próximo
ciclo. Para não repetir o alarme, ele só volta a disparar depois que a temperatura cai
`HYSTERESIS_C` abaixo do limiar.

Para forçar o evento na demo:
    - baixar o limiar (SetThreshold 20) → dispara na próxima amostra;
    - fechar a ventilação (SetParameter ventilacao 0) → a estufa esquenta ~6 °C;
    - `--spike-chance` > 0 → picos de calor aleatórios ocasionais.

    python -m sources.greenhouse --id estufa-1 --grpc-port 50052 --interval 15 --threshold 30
"""

import logging
import random
import threading

from common.loops import BackgroundLoop
from common.runtime import configure_logging
from pb import sources_pb2
from sources.config import SourceConfig, build_arg_parser, config_from_args
from sources.control import ControllableSource, ParameterSpec
from sources.generator import ValueGenerator

log = logging.getLogger(__name__)

DEFAULT_GRPC_PORT = 50052
METRIC, UNIT = "temperatura_estufa", "°C"
VENTILATION_PARAMETER = "ventilacao"
HYSTERESIS_C = 0.5
CLOSED_TEMPERATURE_C = 32.0     # estufa com a ventilação fechada
COOLING_PER_PERCENT_C = 0.12    # 100% de ventilação esfria 12 °C


def base_temperature(ventilation_pct: float) -> float:
    """Temperatura média da estufa para a abertura da ventilação. Ex.: base_temperature(50) -> 26.0"""
    return CLOSED_TEMPERATURE_C - COOLING_PER_PERCENT_C * ventilation_pct


class GreenhouseSensor(ControllableSource):
    """Sensor de temperatura com limiar ajustável e envio imediato ao ultrapassá-lo.

    Ex.: GreenhouseSensor(config, threshold=30.0, ventilation_pct=50.0).run_forever()
    """

    PARAMETERS = {VENTILATION_PARAMETER: ParameterSpec(0.0, 100.0, "%")}
    THRESHOLD_RANGE = (-20.0, 80.0)

    def __init__(
        self,
        config: SourceConfig,
        threshold: float = 30.0,
        ventilation_pct: float = 50.0,
        sample_period_s: float = 1.0,
        spike_chance: float = 0.0,
        spike_size_c: float = 8.0,
        rng: random.Random | None = None,
        **kwargs,
    ) -> None:
        if not 0 < sample_period_s <= 1.0:  # o BackgroundLoop precisa voltar em ~1 s para parar rápido
            raise ValueError(f"--sample-period deve estar em (0, 1] segundos, recebido {sample_period_s!r}")
        if not 0.0 <= spike_chance <= 1.0:
            raise ValueError(f"--spike-chance deve estar entre 0 e 1, recebido {spike_chance!r}")
        super().__init__(config, parameters={VENTILATION_PARAMETER: ventilation_pct}, threshold=threshold, **kwargs)
        self._rng = rng or random.Random()
        self._temperature = ValueGenerator(base=base_temperature(ventilation_pct), noise=0.2, drift=0.1,
                                           minimum=-20.0, maximum=80.0, reversion_per_s=0.02, rng=self._rng)
        self._sample_period_s = sample_period_s
        self._spike_chance = spike_chance
        self._spike_size_c = spike_size_c
        self._latest = self._temperature.next(0.0)
        self._above = False  # alarme já disparado e ainda não rearmado
        self._sample_lock = threading.Lock()
        self._wake = threading.Event()
        self._sampler = BackgroundLoop(f"sampler-{config.source_id}", self._sample_step)

    @property
    def latest_temperature(self) -> float:
        with self._sample_lock:
            return self._latest

    def measure(self) -> list[sources_pb2.Measurement]:
        return [sources_pb2.Measurement(metric=METRIC, value=round(self.latest_temperature, 2), unit=UNIT)]

    def sample_once(self) -> bool:
        """Amostra a temperatura e envia evento se ela acabou de passar do limiar; True se enviou evento."""
        with self._sample_lock:
            if self._spike_chance and self._rng.random() < self._spike_chance:
                log.info("Pico de calor simulado (+%.1f °C)", self._spike_size_c)
                self._temperature.spike(self._spike_size_c)
            self._latest = value = self._temperature.next(self._sample_period_s)
            crossed = self._update_alarm(value)
        if not crossed:
            return False
        reading = self.build_reading()
        if reading is None or self.failure() != sources_pb2.FAILURE_NONE:
            return False  # desligada ou em falha simulada: sem alarme
        log.warning("Temperatura %.2f °C acima do limiar %g °C: enviando evento imediato", value, self.threshold)
        return self.sender.send(reading, is_event=True)

    def on_parameter_changed(self, name: str, value: float) -> None:
        with self._sample_lock:
            self._temperature.base = base_temperature(value)
        self._wake.set()

    def on_threshold_changed(self, threshold: float) -> None:
        with self._sample_lock:
            self._above = False  # novo limiar: avalia de novo, mesmo que já estivesse acima
        self._wake.set()  # amostra já, sem esperar o período

    def on_power_changed(self, on: bool) -> None:
        if on:
            with self._sample_lock:
                self._above = False

    def start(self) -> None:
        super().start()
        self._sampler.start()

    def stop(self) -> None:
        self._wake.set()
        self._sampler.stop()
        super().stop()

    def _update_alarm(self, value: float) -> bool:
        """Histerese: dispara só na transição abaixo → acima. Chamar com `_sample_lock`."""
        threshold = self.threshold
        if threshold is None:
            return False
        if self._above:
            if value < threshold - HYSTERESIS_C:
                self._above = False
            return False
        if value > threshold:
            self._above = True
            return True
        return False

    def _sample_step(self) -> None:
        if self._wake.wait(self._sample_period_s):
            self._wake.clear()
        if not self._sampler.stopping:
            self.sample_once()


def main() -> None:
    parser = build_arg_parser("Sensor de temperatura da estufa (limiar + evento imediato)",
                              default_type="SENSOR_TEMPERATURA_ESTUFA")
    parser.set_defaults(grpc_port=DEFAULT_GRPC_PORT)
    parser.add_argument("--threshold", type=float, default=30.0, help="limiar de alarme em °C")
    parser.add_argument("--ventilacao", type=float, default=50.0, help="abertura inicial da ventilação em %%")
    parser.add_argument("--sample-period", type=float, default=1.0, help="amostragem interna em segundos (≤ 1)")
    parser.add_argument("--spike-chance", type=float, default=0.0, help="probabilidade de pico de calor por amostra")
    parser.add_argument("--seed", type=int, help="semente (valores reproduzíveis)")
    args = parser.parse_args()
    try:
        config = config_from_args(args)
        sensor = GreenhouseSensor(config, threshold=args.threshold, ventilation_pct=args.ventilacao,
                                  sample_period_s=args.sample_period, spike_chance=args.spike_chance,
                                  rng=random.Random(args.seed))
    except ValueError as exc:
        parser.error(str(exc))
    configure_logging(f"source:{config.source_id}")
    sensor.run_forever()


if __name__ == "__main__":
    main()
