"""Fonte controlável 1 — válvula de irrigação (atuador com servidor gRPC, #5).

Envia a vazão atual (L/min) e o volume de água liberado desde a leitura anterior (L).
A soma de `volume` numa janela dá o consumo de água do talhão (consulta `soma`, #14).

Comandos gRPC: SetPower (abrir/fechar = ligar/desligar), SetSendInterval,
SetParameter("vazao_alvo", L/min) e SimulateFailure. Não tem limiar.

    python -m sources.valve --id valvula-1 --grpc-port 50051 --interval 5 --vazao-alvo 20
"""

import random
import time

from common.runtime import configure_logging
from pb import sources_pb2
from sources.config import SourceConfig, build_arg_parser, config_from_args
from sources.control import ControllableSource, ParameterSpec
from sources.generator import ValueGenerator

DEFAULT_GRPC_PORT = 50051
FLOW_PARAMETER = "vazao_alvo"
MAX_FLOW_LPM = 100.0


class IrrigationValve(ControllableSource):
    """Válvula com vazão regulável; a vazão real oscila um pouco em torno da vazão-alvo.

    Ex.: IrrigationValve(config, target_flow_lpm=20.0).run_forever()
    """

    PARAMETERS = {FLOW_PARAMETER: ParameterSpec(0.0, MAX_FLOW_LPM, "L/min")}

    def __init__(self, config: SourceConfig, target_flow_lpm: float = 20.0, rng: random.Random | None = None,
                 **kwargs) -> None:
        super().__init__(config, parameters={FLOW_PARAMETER: target_flow_lpm}, **kwargs)
        self._flow = ValueGenerator(base=target_flow_lpm, noise=0.2, drift=0.05, minimum=0.0, maximum=MAX_FLOW_LPM,
                                    reversion_per_s=0.05, rng=rng or random.Random())
        self._last_measure = time.monotonic()

    def measure(self) -> list[sources_pb2.Measurement]:
        now = time.monotonic()
        elapsed_s, self._last_measure = now - self._last_measure, now
        flow = self._flow.next(elapsed_s) if self.parameter(FLOW_PARAMETER) > 0 else 0.0
        return [
            sources_pb2.Measurement(metric="vazao", value=round(flow, 2), unit="L/min"),
            sources_pb2.Measurement(metric="volume", value=round(flow * elapsed_s / 60.0, 3), unit="L"),
        ]

    def on_parameter_changed(self, name: str, value: float) -> None:
        self._flow.base = value

    def on_power_changed(self, on: bool) -> None:
        if on:  # fechada não libera água: o volume volta a contar a partir de agora
            self._last_measure = time.monotonic()


def main() -> None:
    parser = build_arg_parser("Válvula de irrigação (fonte controlável via gRPC)", default_type="VALVULA_IRRIGACAO")
    parser.set_defaults(grpc_port=DEFAULT_GRPC_PORT)
    parser.add_argument("--vazao-alvo", type=float, default=20.0, help="vazão inicial em L/min")
    parser.add_argument("--seed", type=int, help="semente (valores reproduzíveis)")
    args = parser.parse_args()
    try:
        config = config_from_args(args)
        valve = IrrigationValve(config, target_flow_lpm=args.vazao_alvo, rng=random.Random(args.seed))
    except ValueError as exc:
        parser.error(str(exc))
    configure_logging(f"source:{config.source_id}")
    valve.run_forever()


if __name__ == "__main__":
    main()
