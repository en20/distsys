"""Fontes não controláveis (contínuas): só descoberta + envio periódico via UDP (#7, requisito 2a.ii).

Não têm servidor gRPC: anunciam `controllable = false` e `grpc_port = 0`. Cada fonte envia
uma ou mais grandezas por leitura (ex.: estação meteorológica com temperatura + umidade + vento).

Os tipos concretos ficam em `sources/soil_moisture.py` e `sources/weather_station.py`.
Várias instâncias do mesmo tipo podem rodar juntas, cada uma com seu `--id`.
"""

import socket
import time
from collections.abc import Sequence
from dataclasses import dataclass

from common.net import Address
from pb import sources_pb2
from sources.base import SourceProcess
from sources.config import SourceConfig
from sources.generator import ValueGenerator
from sources.udp_sender import PeriodicPublisher, UdpSender


@dataclass(frozen=True)
class MetricSpec:
    """Uma grandeza enviada pela fonte e o gerador dos seus valores."""

    metric: str
    unit: str
    generator: ValueGenerator
    decimals: int = 2


class ContinuousSource(SourceProcess):
    """Fonte contínua com uma ou mais grandezas simuladas, enviadas a cada `interval_s`.

    Ex.:
        ContinuousSource(config, [MetricSpec("umidade_solo", "%", ValueGenerator(base=30.0))]).run_forever()
    """

    def __init__(
        self,
        config: SourceConfig,
        metrics: Sequence[MetricSpec],
        listener_sock: socket.socket | None = None,
        udp_sock: socket.socket | None = None,
    ) -> None:
        if config.controllable:
            raise ValueError(f"fonte contínua não é controlável: use --grpc-port 0, recebido {config.grpc_port!r}")
        if not metrics:
            raise ValueError("a fonte precisa de pelo menos uma grandeza")
        super().__init__(config, listener_sock, udp_sock)
        self.metrics = list(metrics)
        self.sender = UdpSender(self.gateway_address, self.udp_sock)
        self.publisher = PeriodicPublisher(self.sample, config.interval_s, self.sender, f"publisher-{config.source_id}")
        self._last_sample = time.monotonic()

    def sample(self) -> sources_pb2.SensorReading:
        now = time.monotonic()
        elapsed_s, self._last_sample = now - self._last_sample, now
        return sources_pb2.SensorReading(
            source_id=self.config.source_id,
            type=self.config.source_type,
            measurements=[
                sources_pb2.Measurement(metric=m.metric, value=round(m.generator.next(elapsed_s), m.decimals),
                                        unit=m.unit)
                for m in self.metrics
            ],
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
