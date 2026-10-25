"""Base comum de todas as fontes de dados.

Uma fonte:
  1. escuta o grupo multicast e responde cada DiscoveryRequest do Gateway com
     um SourceAnnouncement (tipo, IP, porta gRPC, estado inicial);
  2. a partir daí envia SensorReading periodicamente via UDP ao Gateway;
  3. se for controlável, expõe o serviço gRPC SourceControl (ver control.py).

O comportamento físico de cada tipo fica nas subclasses (`update`, `measure`,
`check_event`). Toda a lógica de rede, agendamento e comandos fica aqui.
"""

from __future__ import annotations

import logging
import os
import random
import socket
import threading
import time
from dataclasses import dataclass
from typing import Callable, ClassVar

from sources.net import DEFAULT_MULTICAST_GROUP, DEFAULT_MULTICAST_PORT, MulticastListener, local_ip_for
from sources.pb import pb

MIN_INTERVAL_S = 0.5
MAX_INTERVAL_S = 3600.0


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


@dataclass
class SourceConfig:
    source_id: str
    location: str
    send_interval_s: float = 15.0
    multicast_group: str = DEFAULT_MULTICAST_GROUP
    multicast_port: int = DEFAULT_MULTICAST_PORT
    multicast_iface: str = "0.0.0.0"
    advertise_ip: str | None = None   # IP anunciado; padrão: IP da interface que alcança o Gateway
    grpc_host: str = "0.0.0.0"
    grpc_port: int = 0                # 0 = porta livre escolhida pelo sistema
    tick_s: float = 1.0               # passo da simulação e da checagem de eventos
    seed: int | None = None


def now_ms() -> int:
    return int(time.time() * 1000)


class DataSource:
    """Fonte de dados genérica. Subclasses definem os atributos de classe e os hooks."""

    TYPE: ClassVar[int] = pb.SOURCE_TYPE_UNSPECIFIED
    CONTROLLABLE: ClassVar[bool] = False
    MEASUREMENTS: ClassVar[dict[str, MeasurementSpec]] = {}
    PARAMETERS: ClassVar[dict[str, ParameterSpec]] = {}
    THRESHOLD: ClassVar[ParameterSpec | None] = None  # None = fonte sem limiar

    def __init__(self, config: SourceConfig, *, exit_callback: Callable[[], None] | None = None):
        self._validate_interval(config.send_interval_s)
        self.config = config
        self.log = logging.getLogger(config.source_id)
        self.rng = random.Random(config.seed)
        self._exit_callback = exit_callback or (lambda: os._exit(1))

        self._lock = threading.RLock()
        self._powered_on = True
        self._interval = config.send_interval_s
        self._threshold = self.THRESHOLD.default if self.THRESHOLD else None
        self._parameters = {name: spec.default for name, spec in self.PARAMETERS.items()}
        self._failure_mode = pb.FAILURE_NONE
        self._failure_until_ms = 0

        self._gateway: tuple[str, int] | None = None
        self._seq = 0
        self._next_send = 0.0
        self._last_send = 0.0

        self._udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._listener = MulticastListener(config.multicast_group, config.multicast_port,
                                           self._on_discovery, config.multicast_iface, self.log)
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._grpc_server = None
        self.grpc_port = 0

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

    @property
    def info(self) -> "pb.SourceInfo":
        return pb.SourceInfo(source_id=self.config.source_id, type=self.TYPE,
                             location=self.config.location, controllable=self.CONTROLLABLE)

    @property
    def gateway(self) -> tuple[str, int] | None:
        with self._lock:
            return self._gateway

    def start(self) -> None:
        if self.CONTROLLABLE:
            from sources.control import start_server
            self._grpc_server, self.grpc_port = start_server(self, self.config.grpc_host, self.config.grpc_port)
            self.log.info("servidor gRPC SourceControl ouvindo em %s:%d", self.config.grpc_host, self.grpc_port)
        self._listener.start()
        self._thread = threading.Thread(target=self._run, name=f"{self.config.source_id}-loop", daemon=True)
        self._thread.start()
        self.log.info("fonte %s iniciada (local=%s, intervalo=%.1fs)",
                      pb.SourceType.Name(self.TYPE), self.config.location, self._interval)

    def stop(self) -> None:
        if self._stop.is_set():
            return
        self._stop.set()
        self._wake.set()
        self._listener.stop()
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(timeout=2)
        if self._grpc_server is not None:
            self._grpc_server.stop(grace=0.5).wait()
        self._udp.close()
        self.log.info("fonte encerrada")

    def wait(self) -> None:
        """Bloqueia até a fonte ser encerrada."""
        while not self._stop.wait(0.5):
            pass

    # ------------------------------------------------------------------
    # Descoberta
    # ------------------------------------------------------------------

    def _on_discovery(self, request: "pb.DiscoveryRequest", sender: tuple[str, int]) -> None:
        with self._lock:
            self._expire_failure()
            if self._failure_mode == pb.FAILURE_SILENT:
                self.log.info("DiscoveryRequest ignorado (falha simulada: silenciosa)")
                return
            gateway = (request.gateway_ip or sender[0], request.gateway_udp_port)
            changed = gateway != self._gateway
            self._gateway = gateway
            announcement = pb.SourceAnnouncement(
                info=self.info,
                ip=self.config.advertise_ip or local_ip_for(gateway[0]),
                grpc_port=self.grpc_port if self.CONTROLLABLE else 0,
                initial_state=self._status_locked(),
                measurements=list(self.MEASUREMENTS),
                request_id=request.request_id,
            )
            if changed:
                self._next_send = time.monotonic()  # primeira leitura logo após a descoberta
        self._send(pb.SourceMessage(announcement=announcement))
        if changed:
            self.log.info("descoberta pelo gateway %s:%d (request_id=%d); anúncio enviado",
                          gateway[0], gateway[1], request.request_id)
            self._wake.set()
        else:
            self.log.debug("anúncio reenviado ao gateway %s:%d", *gateway)

    # ------------------------------------------------------------------
    # Envio de dados
    # ------------------------------------------------------------------

    def _run(self) -> None:
        last = time.monotonic()
        while not self._stop.is_set():
            self._wake.wait(self.config.tick_s)
            self._wake.clear()
            if self._stop.is_set():
                break
            now = time.monotonic()
            dt, last = now - last, now
            with self._lock:
                self._expire_failure()
                self.update(dt)
                event = self.check_event() if self._powered_on else None
                due = now >= self._next_send
            if event:
                self.log.warning("EVENTO: %s — envio imediato", event)
                self._send_reading(is_event=True, event_message=event)
            if due:
                self._send_reading()

    def _send_reading(self, is_event: bool = False, event_message: str = "") -> None:
        with self._lock:
            if not is_event:
                self._last_send = time.monotonic()
                self._next_send = self._last_send + self._interval
            if self._gateway is None or self._failure_mode == pb.FAILURE_SILENT:
                return
            self._seq += 1
            reading = pb.SensorReading(info=self.info, seq=self._seq, timestamp_ms=now_ms(),
                                       powered_on=self._powered_on, is_event=is_event,
                                       event_message=event_message)
            if self._powered_on:
                values = self.measure()
                if self._failure_mode == pb.FAILURE_OUT_OF_RANGE:
                    values = {name: self._out_of_range(name) for name in values}
                for name, value in values.items():
                    reading.measurements.add(name=name, value=round(value, 3), unit=self.MEASUREMENTS[name].unit)
                self.after_send()
            gateway = self._gateway
        self._send(pb.SourceMessage(reading=reading))
        summary = ", ".join(f"{m.name}={m.value:g}{m.unit}" for m in reading.measurements) or "desligada"
        self.log.info("leitura #%d -> %s:%d: %s", reading.seq, *gateway, summary)

    def _out_of_range(self, name: str) -> float:
        spec = self.MEASUREMENTS[name]
        return spec.max + (spec.max - spec.min) * 0.5

    def _send(self, message: "pb.SourceMessage") -> None:
        gateway = self.gateway
        if gateway is None:
            return
        try:
            self._udp.sendto(message.SerializeToString(), gateway)
        except OSError as exc:  # UDP: gateway fora do ar não pode travar a fonte
            self.log.warning("falha ao enviar para %s:%d: %s", *gateway, exc)

    # ------------------------------------------------------------------
    # Comandos (usados pelo serviço gRPC). Todos retornam o novo estado.
    # ------------------------------------------------------------------

    def status(self) -> "pb.SourceStatus":
        with self._lock:
            self._expire_failure()
            return self._status_locked()

    def set_power(self, on: bool) -> "pb.SourceStatus":
        with self._lock:
            if on != self._powered_on:
                self._powered_on = on
                self.on_power_changed(on)
                self.log.info("fonte %s", "LIGADA" if on else "DESLIGADA")
            status = self._status_locked()
        self._wake.set()
        return status

    def set_send_interval(self, seconds: float) -> "pb.SourceStatus":
        self._validate_interval(seconds)
        with self._lock:
            self._interval = seconds
            self._next_send = self._last_send + seconds  # vale já para o próximo envio
            self.log.info("intervalo de envio alterado para %.1fs", seconds)
            status = self._status_locked()
        self._wake.set()
        return status

    def set_threshold(self, value: float) -> "pb.SourceStatus":
        spec = self.THRESHOLD
        if spec is None:
            raise Unsupported(f"{pb.SourceType.Name(self.TYPE)} não possui limiar")
        self._check_range("limiar", value, spec)
        with self._lock:
            self._threshold = value
            self.log.info("limiar alterado para %g", value)
            status = self._status_locked()
        self._wake.set()  # reavalia o limiar no próximo tick
        return status

    def set_parameter(self, name: str, value: float) -> "pb.SourceStatus":
        if not self.PARAMETERS:
            raise Unsupported(f"{pb.SourceType.Name(self.TYPE)} não possui parâmetros ajustáveis")
        spec = self.PARAMETERS.get(name)
        if spec is None:
            raise InvalidArgument(f"parâmetro desconhecido '{name}'; disponíveis: {', '.join(self.PARAMETERS)}")
        self._check_range(name, value, spec)
        with self._lock:
            self._parameters[name] = value
            self.log.info("parâmetro %s alterado para %g", name, value)
            return self._status_locked()

    def simulate_failure(self, mode: int, duration_s: int = 0) -> "pb.SourceStatus":
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

    def _status_locked(self) -> "pb.SourceStatus":
        status = pb.SourceStatus(info=self.info, powered_on=self._powered_on, send_interval_s=self._interval,
                                 parameters=self._parameters, failure_mode=self._failure_mode,
                                 failure_until_ms=self._failure_until_ms)
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
        self.stop()

    @staticmethod
    def _validate_interval(seconds: float) -> None:
        if not MIN_INTERVAL_S <= seconds <= MAX_INTERVAL_S:
            raise InvalidArgument(f"intervalo deve estar entre {MIN_INTERVAL_S:g}s e {MAX_INTERVAL_S:g}s")

    @staticmethod
    def _check_range(name: str, value: float, spec: ParameterSpec) -> None:
        if not spec.min <= value <= spec.max:
            raise InvalidArgument(f"{name} deve estar entre {spec.min:g} e {spec.max:g}")
