"""Envio de leituras ao Gateway via UDP (#26, requisitos 1b.iii e 2a.ii).

    sender = UdpSender(source.gateway_address, source.udp_sock)
    publisher = PeriodicPublisher(source.sample, config.interval_s, sender)
    publisher.start()

- `UdpSender.send(reading, is_event=True)` manda na hora (evento imediato, Jon #6).
- `PeriodicPublisher.set_interval()` vale já para o próximo envio (gRPC SetSendInterval).
- `PeriodicPublisher.pause()/resume()` para o SetPower.
Nada é enviado antes da descoberta: sem endereço do Gateway, `send` só descarta.
"""

import logging
import math
import threading
import time
from collections.abc import Callable

from common.loops import BackgroundLoop
from common.net import Address, DatagramSender
from common.runtime import Clock, epoch_ms
from pb import sources_pb2

log = logging.getLogger(__name__)

GatewayAddressProvider = Callable[[], Address | None]
SampleFn = Callable[[], sources_pb2.SensorReading | None]

MAX_WAIT_S = 0.5  # o loop acorda pelo menos assim para checar parada e mudanças


class UdpSender:
    """Serializa SourceMessage{reading} e envia ao Gateway atual. Thread-safe.

    Ex.: UdpSender(source.gateway_address, source.udp_sock).send(reading)  # -> True se enviou
    """

    def __init__(self, gateway_address: GatewayAddressProvider, sock: DatagramSender, clock: Clock = time.time) -> None:
        self._gateway_address = gateway_address
        self._sock = sock
        self._clock = clock
        self._lock = threading.Lock()

    def send(self, reading: sources_pb2.SensorReading, is_event: bool = False) -> bool:
        """Envia a leitura; False se o Gateway ainda não foi descoberto ou o envio falhou."""
        gateway = self._gateway_address()
        if gateway is None:
            log.debug("Leitura de %s descartada: Gateway ainda não descoberto", reading.source_id)
            return False
        if is_event:
            reading.is_event = True
        if not reading.timestamp_ms:
            reading.timestamp_ms = epoch_ms(self._clock)
        payload = sources_pb2.SourceMessage(reading=reading).SerializeToString()
        try:
            with self._lock:
                self._sock.sendto(payload, gateway)
        except OSError as exc:  # UDP só envia: Gateway fora do ar não pode travar a fonte
            log.warning("Falha ao enviar leitura para %s:%d: %s", gateway[0], gateway[1], exc)
            return False
        _log_sent(reading, gateway)
        return True


class PeriodicPublisher:
    """Chama `sample_fn` a cada `interval_s` e envia o resultado pelo `UdpSender`.

    `sample_fn` devolve a leitura atual (ou None para pular o ciclo).
    Ex.:
        publisher = PeriodicPublisher(sensor.sample, 15.0, sender)
        publisher.start(); publisher.set_interval(5.0); publisher.pause()
    """

    def __init__(
        self,
        sample_fn: SampleFn,
        interval_s: float,
        sender: UdpSender,
        name: str = "publisher",
        clock: Clock = time.monotonic,
    ) -> None:
        _validate_interval(interval_s)
        self._sample_fn = sample_fn
        self._sender = sender
        self._clock = clock
        self._interval_s = interval_s
        self._paused = False
        self._next_due = clock()  # primeira leitura assim que possível
        self._last_run = self._next_due
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._loop = BackgroundLoop(name, self._step)

    # ----- controle (chamado pelos handlers gRPC) -------------------------------

    @property
    def interval_s(self) -> float:
        return self._interval_s

    @property
    def paused(self) -> bool:
        return self._paused

    def set_interval(self, interval_s: float) -> None:
        """Novo intervalo, contado a partir do último envio (vale já para o próximo)."""
        _validate_interval(interval_s)
        with self._lock:
            self._interval_s = interval_s
            self._next_due = self._last_run + interval_s
        log.info("Intervalo de envio alterado para %.1fs", interval_s)
        self._wake.set()

    def pause(self) -> None:
        with self._lock:
            self._paused = True
        log.info("Envio periódico pausado")

    def resume(self) -> None:
        """Retoma e envia uma leitura logo em seguida."""
        with self._lock:
            self._paused = False
            self._next_due = self._clock()
        log.info("Envio periódico retomado")
        self._wake.set()

    def publish_now(self, delay_s: float = 0.0) -> None:
        """Antecipa o próximo envio para daqui a `delay_s` (ex.: logo após a descoberta do Gateway)."""
        with self._lock:
            self._next_due = self._clock() + delay_s
        self._wake.set()

    # ----- ciclo de vida ---------------------------------------------------------

    def start(self) -> None:
        self._loop.start()

    def stop(self) -> None:
        self._wake.set()
        self._loop.stop()

    def run_pending(self) -> bool:
        """Envia se já deu a hora (e não está pausado); True se chamou `sample_fn`."""
        with self._lock:
            now = self._clock()
            if self._paused or now < self._next_due:
                return False
            self._last_run = now
            self._next_due = now + self._interval_s
        reading = self._sample_fn()
        if reading is not None:
            self._sender.send(reading)
        return True

    def seconds_until_due(self) -> float:
        with self._lock:
            return max(0.0, self._next_due - self._clock())

    def _step(self) -> None:
        timeout = MAX_WAIT_S if self._paused else min(self.seconds_until_due(), MAX_WAIT_S)
        if self._wake.wait(timeout):
            self._wake.clear()
        if not self._loop.stopping:
            self.run_pending()


def _validate_interval(interval_s: float) -> None:
    if not (math.isfinite(interval_s) and interval_s > 0):
        raise ValueError(f"intervalo deve ser > 0 segundos, recebido {interval_s!r}")


def _log_sent(reading: sources_pb2.SensorReading, gateway: Address) -> None:
    values = ", ".join(f"{m.metric}={m.value:g}{m.unit}" for m in reading.measurements) or "(sem medições)"
    if reading.is_event:
        log.warning("EVENTO enviado ao Gateway %s:%d: %s", gateway[0], gateway[1], values)
    else:
        log.info("Leitura enviada ao Gateway %s:%d: %s", gateway[0], gateway[1], values)
