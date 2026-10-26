"""Fontes controláveis: estado operacional + servidor gRPC `SourceControl` (#5, #6, #8).

Uma fonte controlável estende `ControllableSource` e implementa `measure()`. A base cuida de:
    - servidor gRPC na porta anunciada na descoberta (requisitos 2a.i, 1b.ii);
    - envio periódico via `PeriodicPublisher` (#26), com `SetSendInterval` valendo na hora
      e `SetPower(off)` pausando o envio (o registro do Gateway mostra a fonte como DESLIGADA);
    - validação dos comandos (INVALID_ARGUMENT / FAILED_PRECONDITION);
    - falhas simuladas para a demo (`SimulateFailure`, #8).

Todo o estado fica atrás de um lock: as threads do gRPC, do publisher e da descoberta o acessam juntas.
"""

import logging
import math
import os
import threading
import time
from collections.abc import Callable
from concurrent import futures
from dataclasses import dataclass

import grpc

from common.runtime import Clock, epoch_ms
from pb import sources_pb2, sources_pb2_grpc
from sources.base import SourceProcess
from sources.config import SourceConfig
from sources.udp_sender import PeriodicPublisher, UdpSender

log = logging.getLogger(__name__)

MIN_INTERVAL_S = 0.1
MAX_INTERVAL_S = 3600.0
CRASH_DELAY_S = 0.5      # tempo para a resposta do SimulateFailure(CRASH) chegar ao Gateway
FAULTY_VALUE = -9999.0   # valor sentinela de sensor com defeito (FAILURE_OUT_OF_RANGE)
GRPC_WORKERS = 4

FAILURE_NAMES = {
    sources_pb2.FAILURE_NONE: "nenhuma",
    sources_pb2.FAILURE_SILENT: "silêncio",
    sources_pb2.FAILURE_OUT_OF_RANGE: "valores fora da faixa",
    sources_pb2.FAILURE_CRASH: "queda do processo",
}


class CommandRejected(ValueError):
    """Comando inválido para esta fonte; o servidor gRPC devolve `code` com a mensagem."""

    def __init__(self, code: grpc.StatusCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class ParameterSpec:
    """Faixa aceita por um parâmetro do domínio (SetParameter)."""

    minimum: float
    maximum: float
    unit: str = ""


class ControllableSource(SourceProcess):
    """Base das fontes controláveis. Subclasses definem `PARAMETERS` e implementam `measure()`.

    Ex.:
        class Valvula(ControllableSource):
            PARAMETERS = {"vazao_alvo": ParameterSpec(0, 100, "L/min")}
            def measure(self): return [sources_pb2.Measurement(metric="vazao", value=..., unit="L/min")]
        Valvula(config, parameters={"vazao_alvo": 20.0}).run_forever()
    """

    PARAMETERS: dict[str, ParameterSpec] = {}
    THRESHOLD_RANGE: tuple[float, float] = (-math.inf, math.inf)

    def __init__(
        self,
        config: SourceConfig,
        parameters: dict[str, float] | None = None,
        threshold: float | None = None,
        listener_sock=None,
        udp_sock=None,
        clock: Clock = time.time,
        exit_process: Callable[[int], None] = os._exit,
    ) -> None:
        if not config.controllable:
            raise ValueError(f"fonte controlável precisa de --grpc-port > 0, recebido {config.grpc_port!r}")
        super().__init__(config, listener_sock, udp_sock)
        self._clock = clock
        self._exit_process = exit_process
        self._state_lock = threading.RLock()
        self._powered_on = True
        low, high = self.THRESHOLD_RANGE
        if threshold is not None and not (math.isfinite(threshold) and low <= threshold <= high):
            raise ValueError(f"limiar deve estar entre {low:g} e {high:g}, recebido {threshold!r}")
        self._threshold = threshold
        self._parameters = {name: self._validated_parameter(name, value) for name, value in (parameters or {}).items()}
        missing = sorted(set(self.PARAMETERS) - set(self._parameters))
        if missing:
            raise ValueError(f"valor inicial faltando para os parâmetros: {', '.join(missing)}")
        self._failure = sources_pb2.FAILURE_NONE
        self._failure_until_ms = 0
        self.sender = UdpSender(self.gateway_address, self.udp_sock)
        self.publisher = PeriodicPublisher(self.build_reading, config.interval_s, self.sender,
                                           f"publisher-{config.source_id}")
        self._grpc_server: grpc.Server | None = None

    # ----- pontos de extensão -------------------------------------------------

    def measure(self) -> list[sources_pb2.Measurement]:
        """Grandezas atuais da fonte (sem falha aplicada)."""
        raise NotImplementedError

    def on_parameter_changed(self, name: str, value: float) -> None:
        """Chamado depois que `SetParameter` gravou o novo valor."""

    def on_power_changed(self, on: bool) -> None:
        """Chamado depois que `SetPower` mudou o estado."""

    def on_threshold_changed(self, threshold: float) -> None:
        """Chamado depois que `SetThreshold` gravou o novo limiar."""

    # ----- estado ---------------------------------------------------------------

    def current_status(self) -> sources_pb2.SourceStatus:
        with self._state_lock:
            self._expire_failure()
            status = sources_pb2.SourceStatus(
                source_id=self.config.source_id,
                powered_on=self._powered_on,
                send_interval_s=self.publisher.interval_s,
                parameters=self._parameters,
                failure=self._failure,
                failure_until_ms=self._failure_until_ms,
            )
            if self._threshold is not None:
                status.threshold = self._threshold
            return status

    @property
    def powered_on(self) -> bool:
        with self._state_lock:
            return self._powered_on

    @property
    def threshold(self) -> float | None:
        with self._state_lock:
            return self._threshold

    def parameter(self, name: str) -> float:
        with self._state_lock:
            return self._parameters[name]

    def failure(self) -> int:
        """Falha simulada ativa (`FAILURE_NONE` quando operando normalmente)."""
        with self._state_lock:
            self._expire_failure()
            return self._failure

    def answers_discovery(self) -> bool:
        # Em silêncio a fonte também não responde à redescoberta; senão o Gateway nunca a marcaria OFFLINE.
        return self.failure() != sources_pb2.FAILURE_SILENT

    # ----- comandos (chamados pelo servidor gRPC) -------------------------------

    def set_power(self, on: bool) -> sources_pb2.SourceStatus:
        with self._state_lock:
            changed = on != self._powered_on
            self._powered_on = on
        if changed:
            if on:
                self.publisher.resume()
            else:
                self.publisher.pause()
            log.info("Fonte %s %s", self.config.source_id, "LIGADA" if on else "DESLIGADA")
            self.on_power_changed(on)
        return self.current_status()

    def set_send_interval(self, interval_s: float) -> sources_pb2.SourceStatus:
        if not (math.isfinite(interval_s) and MIN_INTERVAL_S <= interval_s <= MAX_INTERVAL_S):
            raise CommandRejected(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"intervalo deve estar entre {MIN_INTERVAL_S:g} e {MAX_INTERVAL_S:g} s, recebido {interval_s!r}",
            )
        self.publisher.set_interval(interval_s)
        return self.current_status()

    def set_threshold(self, threshold: float) -> sources_pb2.SourceStatus:
        low, high = self.THRESHOLD_RANGE
        with self._state_lock:
            if self._threshold is None:
                raise CommandRejected(grpc.StatusCode.FAILED_PRECONDITION,
                                      f"a fonte {self.config.source_id} não tem limiar")
            if not (math.isfinite(threshold) and low <= threshold <= high):
                raise CommandRejected(grpc.StatusCode.INVALID_ARGUMENT,
                                      f"limiar deve estar entre {low:g} e {high:g}, recebido {threshold!r}")
            self._threshold = threshold
        log.info("Limiar alterado para %g", threshold)
        self.on_threshold_changed(threshold)
        return self.current_status()

    def set_parameter(self, name: str, value: float) -> sources_pb2.SourceStatus:
        value = self._validated_parameter(name, value)
        with self._state_lock:
            self._parameters[name] = value
        spec = self.PARAMETERS[name]
        log.info("Parâmetro %s alterado para %g%s", name, value, spec.unit)
        self.on_parameter_changed(name, value)
        return self.current_status()

    def simulate_failure(self, mode: int, duration_s: int) -> sources_pb2.SourceStatus:
        if mode not in FAILURE_NAMES:
            raise CommandRejected(grpc.StatusCode.INVALID_ARGUMENT, f"modo de falha desconhecido: {mode!r}")
        with self._state_lock:
            self._failure = mode
            no_deadline = mode in (sources_pb2.FAILURE_NONE, sources_pb2.FAILURE_CRASH) or duration_s == 0
            self._failure_until_ms = 0 if no_deadline else epoch_ms(self._clock) + duration_s * 1000
        if mode == sources_pb2.FAILURE_NONE:
            log.warning("Falha simulada encerrada: fonte %s voltou ao normal", self.config.source_id)
        elif mode == sources_pb2.FAILURE_CRASH:
            log.critical("FALHA SIMULADA: processo da fonte %s será encerrado em %.1fs",
                         self.config.source_id, CRASH_DELAY_S)
            threading.Timer(CRASH_DELAY_S, self._exit_process, args=(1,)).start()
        else:
            until = f"por {duration_s}s" if duration_s else "até novo comando"
            log.warning("FALHA SIMULADA (%s) %s", FAILURE_NAMES[mode], until)
        return self.current_status()

    # ----- leituras -------------------------------------------------------------

    def build_reading(self) -> sources_pb2.SensorReading | None:
        """Leitura atual com a falha simulada aplicada; None quando a fonte não deve enviar."""
        with self._state_lock:
            self._expire_failure()
            if not self._powered_on or self._failure == sources_pb2.FAILURE_SILENT:
                return None
            out_of_range = self._failure == sources_pb2.FAILURE_OUT_OF_RANGE
        measurements = self.measure()
        if out_of_range:
            for measurement in measurements:
                measurement.value = FAULTY_VALUE
        return sources_pb2.SensorReading(
            source_id=self.config.source_id,
            type=self.config.source_type,
            measurements=measurements,
        )

    def on_gateway_discovered(self, gateway) -> None:
        # Primeira leitura logo após a descoberta; o atraso deixa o anúncio chegar antes.
        self.publisher.publish_now(delay_s=0.2)

    # ----- ciclo de vida -------------------------------------------------------

    def start(self) -> None:
        self._grpc_server = self._start_grpc_server()  # antes da descoberta: a porta anunciada já atende
        super().start()
        self.publisher.start()

    def stop(self) -> None:
        if self._grpc_server is not None:
            self._grpc_server.stop(grace=0.5).wait()
        self.publisher.stop()  # antes do super().stop(), que fecha o socket UDP
        super().stop()

    # ----- internos ---------------------------------------------------------------

    def _start_grpc_server(self) -> grpc.Server:
        # so_reuseport=0: duas fontes na mesma porta devem falhar, não dividir as chamadas.
        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=GRPC_WORKERS, thread_name_prefix=f"grpc-{self.config.source_id}"),
            options=[("grpc.so_reuseport", 0)],
        )
        sources_pb2_grpc.add_SourceControlServicer_to_server(SourceControlServicer(self), server)
        address = f"0.0.0.0:{self.config.grpc_port}"
        try:
            bound = server.add_insecure_port(address)
        except RuntimeError as exc:
            raise OSError(f"não foi possível abrir o servidor gRPC em {address}: {exc}") from exc
        if bound == 0:
            raise OSError(f"não foi possível abrir o servidor gRPC em {address}")
        server.start()
        log.info("Servidor gRPC SourceControl em %s", address)
        return server

    def _validated_parameter(self, name: str, value: float) -> float:
        spec = self.PARAMETERS.get(name)
        if spec is None:
            accepted = ", ".join(sorted(self.PARAMETERS)) or "nenhum"
            raise CommandRejected(grpc.StatusCode.INVALID_ARGUMENT,
                                  f"parâmetro desconhecido {name!r}; aceitos: {accepted}")
        if not (math.isfinite(value) and spec.minimum <= value <= spec.maximum):
            raise CommandRejected(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"{name} deve estar entre {spec.minimum:g} e {spec.maximum:g}{spec.unit}, recebido {value!r}",
            )
        return value

    def _expire_failure(self) -> None:
        """Encerra a falha com prazo vencido. Chamar com `_state_lock`."""
        if self._failure_until_ms and epoch_ms(self._clock) >= self._failure_until_ms:
            log.warning("Fim da falha simulada (%s): fonte %s voltou ao normal",
                        FAILURE_NAMES[self._failure], self.config.source_id)
            self._failure = sources_pb2.FAILURE_NONE
            self._failure_until_ms = 0


class SourceControlServicer(sources_pb2_grpc.SourceControlServicer):
    """Traduz cada RPC para o comando da fonte; `CommandRejected` vira status gRPC de erro."""

    def __init__(self, source: ControllableSource) -> None:
        self._source = source

    def GetStatus(self, request, context):
        return self._run(context, "GetStatus", self._source.current_status)

    def SetPower(self, request, context):
        return self._run(context, f"SetPower({'on' if request.on else 'off'})", self._source.set_power, request.on)

    def SetSendInterval(self, request, context):
        return self._run(context, f"SetSendInterval({request.interval_s:g}s)",
                         self._source.set_send_interval, request.interval_s)

    def SetThreshold(self, request, context):
        return self._run(context, f"SetThreshold({request.threshold:g})", self._source.set_threshold, request.threshold)

    def SetParameter(self, request, context):
        return self._run(context, f"SetParameter({request.name}={request.value:g})",
                         self._source.set_parameter, request.name, request.value)

    def SimulateFailure(self, request, context):
        mode_name = sources_pb2.FailureMode.Name(request.mode) if request.mode in FAILURE_NAMES else str(request.mode)
        return self._run(context, f"SimulateFailure({mode_name}, {request.duration_s}s)",
                         self._source.simulate_failure, request.mode, request.duration_s)

    def _run(self, context: grpc.ServicerContext, description: str, action, *args) -> sources_pb2.SourceStatus:
        if description != "GetStatus":
            log.info("gRPC %s recebido", description)
        try:
            return action(*args)
        except CommandRejected as exc:
            log.warning("gRPC %s rejeitado: %s", description, exc.message)
            context.abort(exc.code, exc.message)
