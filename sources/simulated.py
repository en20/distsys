"""Fonte contínua genérica: descoberta (#4) + envio periódico de valores simulados (#26).

Serve para testar o fluxo de dados de ponta a ponta antes das fontes do domínio
(Jon, #5–#7) e como exemplo de como ligar o `PeriodicPublisher` a um `SourceProcess`.

    python -m sources.simulated --id temp-1 --type SENSOR_TEMPERATURA --metric temperatura \
        --unit °C --base 25 --noise 0.3 --drift 0.2 --min -10 --max 50 --interval 5
"""

import logging
import random
import socket
import time

from common.net import Address
from common.runtime import configure_logging
from pb import sources_pb2
from sources.base import SourceProcess
from sources.config import SourceConfig, build_arg_parser, config_from_args
from sources.generator import ValueGenerator
from sources.udp_sender import PeriodicPublisher, UdpSender

log = logging.getLogger(__name__)


class SimulatedSource(SourceProcess):
    """Fonte não controlável que envia uma grandeza simulada a cada `interval_s`.

    Ex.: SimulatedSource(config, "temperatura", "°C", ValueGenerator(base=25.0)).run_forever()
    """

    def __init__(
        self,
        config: SourceConfig,
        metric: str,
        unit: str,
        generator: ValueGenerator,
        listener_sock: socket.socket | None = None,
        udp_sock: socket.socket | None = None,
    ) -> None:
        super().__init__(config, listener_sock, udp_sock)
        self.metric = metric
        self.unit = unit
        self.generator = generator
        self.sender = UdpSender(self.gateway_address, self.udp_sock)
        self.publisher = PeriodicPublisher(self.sample, config.interval_s, self.sender, f"publisher-{config.source_id}")
        self._last_sample = time.monotonic()

    def sample(self) -> sources_pb2.SensorReading:
        now = time.monotonic()
        value = self.generator.next(now - self._last_sample)
        self._last_sample = now
        return sources_pb2.SensorReading(
            source_id=self.config.source_id,
            type=self.config.source_type,
            measurements=[sources_pb2.Measurement(metric=self.metric, value=round(value, 3), unit=self.unit)],
        )

    def on_gateway_discovered(self, gateway: Address) -> None:
        # Primeira leitura logo após a descoberta; o atraso deixa o anúncio chegar antes.
        self.publisher.publish_now(delay_s=0.2)

    def start(self) -> None:
        super().start()
        self.publisher.start()

    def stop(self) -> None:
        self.publisher.stop()  # antes do super().stop(), que fecha o socket UDP
        super().stop()


def main() -> None:
    parser = build_arg_parser("Fonte contínua simulada (fluxo de dados)", default_type="SENSOR_SIMULADO")
    parser.add_argument("--metric", default="valor", help="nome da grandeza (ex.: temperatura)")
    parser.add_argument("--unit", default="", help="unidade (ex.: °C)")
    parser.add_argument("--base", type=float, default=50.0, help="valor médio")
    parser.add_argument("--noise", type=float, default=0.5, help="ruído de cada amostra")
    parser.add_argument("--drift", type=float, default=0.2, help="intensidade da deriva lenta")
    parser.add_argument("--trend", type=float, default=0.0, help="tendência linear por segundo")
    parser.add_argument("--min", type=float, default=float("-inf"), dest="minimum", help="valor mínimo físico")
    parser.add_argument("--max", type=float, default=float("inf"), dest="maximum", help="valor máximo físico")
    parser.add_argument("--seed", type=int, help="semente (valores reproduzíveis)")
    args = parser.parse_args()
    try:
        config = config_from_args(args)
        generator = ValueGenerator(base=args.base, noise=args.noise, drift=args.drift, trend_per_s=args.trend,
                                   minimum=args.minimum, maximum=args.maximum, rng=random.Random(args.seed))
    except ValueError as exc:
        parser.error(str(exc))
    configure_logging(f"source:{config.source_id}")
    SimulatedSource(config, args.metric, args.unit, generator).run_forever()


if __name__ == "__main__":
    main()
