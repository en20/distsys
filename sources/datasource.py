"""Base comum das fontes do domínio (Agricultura de precisão).

Código do Enzo (branch `enzo/fontes-de-dados`) adaptado ao contrato do Marcelo
(`proto/sources.proto`, #3) e à base `SourceProcess` (#4). Uma fonte:
  1. responde cada DiscoveryRequest do Gateway com um SourceAnnouncement
     (tipo, IP, porta gRPC, estado inicial) — feito pelo `SourceProcess`;
  2. a partir daí envia SensorReading periodicamente via UDP ao Gateway (`UdpSender`, #26);
  3. se for controlável, expõe o serviço gRPC SourceControl (ver control.py).

O comportamento físico de cada tipo fica nas subclasses (`update`, `measure`,
`check_event`). A lógica de agendamento e comandos fica aqui.
"""

from __future__ import annotations

import logging
import math
import os
import random
import socket
import threading
import time
from dataclasses import dataclass
from typing import Callable, ClassVar

from common.loops import BackgroundLoop
from common.net import Address
from common.runtime import epoch_ms
from pb import sources_pb2 as pb
from sources.base import SourceProcess
from sources.config import SourceConfig
from sources.udp_sender import UdpSender

MIN_INTERVAL_S = 0.5
MAX_INTERVAL_S = 3600.0
FIRST_READING_DELAY_S = 0.2  # deixa o anúncio chegar ao Gateway antes da primeira leitura


class InvalidArgument(ValueError):
    """Valor de comando inválido (vira INVALID_ARGUMENT no gRPC)."""


class Unsupported(Exception):
    """Comando não suportado por este tipo de fonte (vira UNIMPLEMENTED no gRPC)."""


@dataclass(frozen=True)
class MeasurementSpec:
    unit: str
    min: float
    max: float


@dataclass(frozen=True)
class ParameterSpec:
    default: float
    min: float
    max: float
    description: str = ""


def now_ms() -> int:
    return epoch_ms()


class DataSource(SourceProcess):
    """Fonte de dados genérica. Subclasses definem os atributos de classe e os hooks."""

    TYPE: ClassVar[str] = ""  # tipo anunciado por padrão (o --type da linha de comando sobrescreve)
    CONTROLLABLE: ClassVar[bool] = False
    MEASUREMENTS: ClassVar[dict[str, MeasurementSpec]] = {}
    PARAMETERS: ClassVar[dict[str, ParameterSpec]] = {}
    THRESHOLD: ClassVar[ParameterSpec | None] = None  # None = fonte sem limiar

    def __init__(
        self,
        config: SourceConfig,
        *,
        seed: int | None = None,
        tick_s: float = 1.0,
        exit_callback: Callable[[], None] | None = None,
        listener_sock: socket.socket | None = None,
        udp_sock: socket.socket | None = None,
    ):
        self._validate_interval(config.interval_s)
        if config.controllable != self.CONTROLLABLE:
            needed = "uma porta gRPC (--grpc-port > 0)" if self.CONTROLLABLE else "--grpc-port 0 (não é controlável)"
            raise InvalidArgument(f"{type(self).__name__} precisa de {needed}")
        super().__init__(config, listener_sock, udp_sock)
        self.log = logging.getLogger(config.source_id)
        self.rng = random.Random(seed)
        self.tick_s = tick_s
        self._exit_callback = exit_callback or (lambda: os._exit(1))

        self._lock = threading.RLock()
        self._powered_on = True
        self._interval = config.interval_s
        self._threshold = self.THRESHOLD.default if self.THRESHOLD else None
        self._parameters = {name: spec.default for name, spec in self.PARAMETERS.items()}
        self._failure_mode = pb.FAILURE_NONE
        self._failure_until_ms = 0

        self._next_send = math.inf  # nada antes da descoberta
        self._last_send = 0.0

        self.sender = UdpSender(self.gateway_address, self.udp_sock)
        self._wake = threading.Event()
        self._loop = BackgroundLoop(f"{config.source_id}-loop", self._tick)
        self._last_tick = time.monotonic()
        self._grpc_server = None

    # ------------------------------------------------------------------
    # Hooks das subclasses (chamados com o lock adquirido)
    # ------------------------------------------------------------------

    def update(self, dt: float) -> None:
        """Avança a simulação física em `dt` segundos (chamado a cada tick)."""

    def measure(self) -> dict[str, float]:
        """Valores atuais de cada grandeza em MEASUREMENTS."""
        raise NotImplementedError

    def check_event(self) -> str | None:
        """Mensagem de evento se algo relevante acabou de acontecer (envio imediato)."""
        return None

    def after_send(self) -> None:
        """Chamado depois que uma leitura com medições foi enviada."""

    def on_power_changed(self, on: bool) -> None:
        """Chamado quando a fonte é ligada/desligada."""

    # ------------------------------------------------------------------
    # Ciclo de vida
    # ------------------------------------------------------------------

    def start(self) -> None:
        if self.CONTROLLABLE:  # antes da descoberta: a porta anunciada já atende
            from sources.control import start_server
            self._grpc_server = start_server(self, "0.0.0.0", self.config.grpc_port)
            self.log.info("servidor gRPC SourceControl ouvindo na porta %d", self.config.grpc_port)
        super().start()
        self._loop.start()
        self.log.info("fonte %s iniciada (intervalo=%.1fs)", self.config.source_type, self._interval)

    def stop(self) -> None:
        self._wake.set()
        self._loop.stop()
        if self._grpc_server is not None:
            self._grpc_server.stop(grace=0.5).wait()
        super().stop()  # fecha os sockets por último

    # ------------------------------------------------------------------
    # Descoberta (a resposta em si é do SourceProcess)
    # ------------------------------------------------------------------

    def current_status(self) -> pb.SourceStatus:
        return self.status()

    def answers_discovery(self) -> bool:
        with self._lock:
            self._expire_failure()
            if self._failure_mode == pb.FAILURE_SILENT:
                self.log.info("DiscoveryRequest ignorado (falha simulada: silenciosa)")
                return False
            return True

    def on_gateway_discovered(self, gateway: Address) -> None:
        with self._lock:
            self._next_send = time.monotonic() + FIRST_READING_DELAY_S  # primeira leitura logo após a descoberta
        self._wake.set()

    # ------------------------------------------------------------------
    # Envio de dados
    # ------------------------------------------------------------------

    def _tick(self) -> None:
        self._wake.wait(self.tick_s)
        self._wake.clear()
        if self._loop.stopping:
            return
        now = time.monotonic()
        dt, self._last_tick = now - self._last_tick, now
        with self._lock:
            self._expire_failure()
            self.update(dt)
            event = self.check_event() if self._powered_on else None
            due = now >= self._next_send
        if event:
            self.log.warning("EVENTO: %s — envio imediato", event)
            self._send_reading(is_event=True)
        if due:
            self._send_reading()

    def _send_reading(self, is_event: bool = False) -> None:
        with self._lock:
            if not is_event:
                self._last_send = time.monotonic()
                self._next_send = self._last_send + self._interval
            # Desligada não envia: o Gateway mostra DESLIGADA (estado vindo do gRPC), que não expira.
            if not self._powered_on or self._failure_mode == pb.FAILURE_SILENT:
                return
            values = self.measure()
            if self._failure_mode == pb.FAILURE_OUT_OF_RANGE:
                values = {name: self._out_of_range(name) for name in values}
            reading = pb.SensorReading(source_id=self.config.source_id, type=self.config.source_type)
            for name, value in values.items():
                reading.measurements.add(metric=name, value=round(value, 3), unit=self.MEASUREMENTS[name].unit)
            if self.sender.send(reading, is_event=is_event):
                self.after_send()

    def _out_of_range(self, name: str) -> float:
        spec = self.MEASUREMENTS[name]
        return spec.max + (spec.max - spec.min) * 0.5

    # ------------------------------------------------------------------
    # Comandos (usados pelo serviço gRPC). Todos retornam o novo estado.
    # ------------------------------------------------------------------

    def status(self) -> pb.SourceStatus:
        with self._lock:
            self._expire_failure()
            return self._status_locked()

    def set_power(self, on: bool) -> pb.SourceStatus:
        with self._lock:
            if on != self._powered_on:
                self._powered_on = on
                self.on_power_changed(on)
                self.log.info("fonte %s", "LIGADA" if on else "DESLIGADA")
                if on:
                    self._next_send = time.monotonic()  # religada: envia já
            status = self._status_locked()
        self._wake.set()
        return status

    def set_send_interval(self, seconds: float) -> pb.SourceStatus:
        self._validate_interval(seconds)
        with self._lock:
            self._interval = seconds
            if self._next_send != math.inf:  # antes da descoberta continua esperando o Gateway
                self._next_send = self._last_send + seconds  # vale já para o próximo envio
            self.log.info("intervalo de envio alterado para %.1fs", seconds)
            status = self._status_locked()
        self._wake.set()
        return status

    def set_threshold(self, value: float) -> pb.SourceStatus:
        spec = self.THRESHOLD
        if spec is None:
            raise Unsupported(f"{self.config.source_type} não possui limiar")
        self._check_range("limiar", value, spec)
        with self._lock:
            self._threshold = value
            self.log.info("limiar alterado para %g", value)
            status = self._status_locked()
        self._wake.set()  # reavalia o limiar no próximo tick
        return status

    def set_parameter(self, name: str, value: float) -> pb.SourceStatus:
        if not self.PARAMETERS:
            raise Unsupported(f"{self.config.source_type} não possui parâmetros ajustáveis")
        spec = self.PARAMETERS.get(name)
        if spec is None:
            raise InvalidArgument(f"parâmetro desconhecido '{name}'; disponíveis: {', '.join(self.PARAMETERS)}")
        self._check_range(name, value, spec)
        with self._lock:
            self._parameters[name] = value
            self.log.info("parâmetro %s alterado para %g", name, value)
            return self._status_locked()

    def simulate_failure(self, mode: int, duration_s: int = 0) -> pb.SourceStatus:
        if mode not in pb.FailureMode.values():
            raise InvalidArgument(f"modo de falha desconhecido: {mode}")
        with self._lock:
            self._failure_mode = mode
            self._failure_until_ms = now_ms() + duration_s * 1000 if duration_s and mode != pb.FAILURE_NONE else 0
            status = self._status_locked()
        if mode == pb.FAILURE_NONE:
            self.log.info("falha simulada encerrada")
        elif mode == pb.FAILURE_CRASH:
            self.log.error("FALHA SIMULADA: o processo será encerrado")
            threading.Timer(0.3, self._crash).start()  # deixa a resposta gRPC sair antes
        else:
            self.log.error("FALHA SIMULADA: %s por %s", pb.FailureMode.Name(mode),
                           f"{duration_s}s" if duration_s else "tempo indeterminado")
        self._wake.set()
        return status

    @property
    def threshold(self) -> float | None:
        return self._threshold

    @property
    def powered_on(self) -> bool:
        return self._powered_on

    def parameter(self, name: str) -> float:
        return self._parameters[name]

    # ------------------------------------------------------------------
    # Auxiliares
    # ------------------------------------------------------------------

    def _status_locked(self) -> pb.SourceStatus:
        status = pb.SourceStatus(source_id=self.config.source_id, powered_on=self._powered_on,
                                 send_interval_s=self._interval, parameters=self._parameters,
                                 failure=self._failure_mode, failure_until_ms=self._failure_until_ms)
        if self._threshold is not None:
            status.threshold = self._threshold
        return status

    def _expire_failure(self) -> None:
        if self._failure_until_ms and now_ms() >= self._failure_until_ms:
            self.log.info("falha simulada %s expirou", pb.FailureMode.Name(self._failure_mode))
            self._failure_mode = pb.FAILURE_NONE
            self._failure_until_ms = 0

    def _crash(self) -> None:
        # Sai antes de qualquer limpeza: um encerramento "limpo" faria o processo terminar com código 0.
        self._exit_callback()

    @staticmethod
    def _validate_interval(seconds: float) -> None:
        if not MIN_INTERVAL_S <= seconds <= MAX_INTERVAL_S:
            raise InvalidArgument(f"intervalo deve estar entre {MIN_INTERVAL_S:g}s e {MAX_INTERVAL_S:g}s")

    @staticmethod
    def _check_range(name: str, value: float, spec: ParameterSpec) -> None:
        if not spec.min <= value <= spec.max:
            raise InvalidArgument(f"{name} deve estar entre {spec.min:g} e {spec.max:g}")
