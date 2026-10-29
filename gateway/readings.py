"""Receptor do fluxo de dados e armazenamento das leituras em memória (#12).

O `GatewayUdpEndpoint` decodifica o envelope e chama `ReadingReceiver` para cada
`SensorReading`. O receptor:
    1. mantém o registro vivo (`registry.touch`, via `RegistryReadingSink`);
    2. guarda a leitura na série temporal em memória (`ReadingStore`);
    3. persiste no SQLite (`storage.save`, #16), se configurado;
    4. trata `is_event = true` como alarme: log em destaque + contagem.

Interface para os outros:
    store.readings(since_ms, until_ms, source_id, source_type)  # consultas (#14), servidor TCP (#15)
    store.latest(source_id)                                     # última leitura de uma fonte
    receiver.alarm_counts()                                     # alarmes por fonte desde o início
"""

import logging
import threading
import time
from collections import Counter, deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Protocol

from common.net import Address
from common.runtime import Clock, epoch_ms
from pb import sources_pb2

log = logging.getLogger(__name__)

DEFAULT_RETENTION_S = 25 * 3600  # cobre consultas de "últimas 24 h" com folga
DEFAULT_MAX_PER_SOURCE = 200_000  # limite de memória por fonte (≈ 1 leitura/0,5 s por 24 h)


@dataclass(frozen=True)
class MetricValue:
    metric: str
    value: float
    unit: str


@dataclass(frozen=True)
class StoredReading:
    """Leitura como guardada pelo Gateway (imutável, segura para compartilhar entre threads).

    `received_ms` é o relógio do Gateway na chegada — é ele que define as janelas das consultas,
    para não depender do relógio de cada fonte. `source_timestamp_ms` é o que a fonte informou.
    """

    source_id: str
    source_type: str
    received_ms: int
    source_timestamp_ms: int
    is_event: bool
    measurements: tuple[MetricValue, ...]

    @classmethod
    def from_proto(cls, reading: sources_pb2.SensorReading, received_ms: int) -> "StoredReading":
        return cls(
            source_id=reading.source_id,
            source_type=reading.type,
            received_ms=received_ms,
            source_timestamp_ms=reading.timestamp_ms,
            is_event=reading.is_event,
            measurements=tuple(MetricValue(m.metric, m.value, m.unit) for m in reading.measurements),
        )

    def value_of(self, metric: str) -> MetricValue | None:
        return next((m for m in self.measurements if m.metric == metric), None)


class ReadingSource(Protocol):
    """O que as consultas precisam ler (implementado por ReadingStore e SqliteStorage)."""

    def readings(
        self,
        since_ms: int | None = None,
        until_ms: int | None = None,
        source_id: str | None = None,
        source_type: str | None = None,
    ) -> list[StoredReading]: ...


class ReadingSink(Protocol):
    def save(self, reading: StoredReading) -> None: ...


class ReadingStore:
    """Séries temporais por fonte, em memória, com retenção por tempo. Thread-safe.

    Ex.:
        store = ReadingStore()
        store.add(StoredReading.from_proto(reading, epoch_ms()))
        store.readings(since_ms=epoch_ms() - 3_600_000, source_type="SENSOR_UMIDADE")
    """

    def __init__(
        self,
        retention_s: float = DEFAULT_RETENTION_S,
        clock: Clock = time.time,
        max_per_source: int = DEFAULT_MAX_PER_SOURCE,
    ) -> None:
        if retention_s <= 0:
            raise ValueError(f"retenção deve ser > 0 segundos, recebido {retention_s!r}")
        self.retention_s = retention_s
        self._clock = clock
        self._max_per_source = max_per_source
        self._series: dict[str, deque[StoredReading]] = {}
        self._lock = threading.Lock()

    def add(self, reading: StoredReading) -> None:
        with self._lock:
            series = self._series.setdefault(reading.source_id, deque(maxlen=self._max_per_source))
            series.append(reading)
            self._prune(series)

    def load(self, readings: Iterable[StoredReading]) -> int:
        """Carrega leituras antigas no store ainda vazio (ex.: do SQLite ao reiniciar).

        Devolve quantas leituras ficaram em memória (as fora da retenção são descartadas).
        """
        with self._lock:
            for reading in sorted(readings, key=lambda r: r.received_ms):
                self._series.setdefault(reading.source_id, deque(maxlen=self._max_per_source)).append(reading)
            for series in self._series.values():
                self._prune(series)
            return sum(len(series) for series in self._series.values())

    def readings(
        self,
        since_ms: int | None = None,
        until_ms: int | None = None,
        source_id: str | None = None,
        source_type: str | None = None,
    ) -> list[StoredReading]:
        """Leituras no intervalo [since_ms, until_ms], em ordem de chegada."""
        with self._lock:
            if source_id is not None:
                candidates = list(self._series.get(source_id, ()))
            else:
                candidates = [r for series in self._series.values() for r in series]
        selected = [
            r for r in candidates
            if (since_ms is None or r.received_ms >= since_ms)
            and (until_ms is None or r.received_ms <= until_ms)
            and (source_type is None or r.source_type == source_type)
        ]
        if source_id is None:
            selected.sort(key=lambda r: r.received_ms)
        return selected

    def latest(self, source_id: str) -> StoredReading | None:
        with self._lock:
            series = self._series.get(source_id)
            return series[-1] if series else None

    def source_ids(self) -> list[str]:
        with self._lock:
            return sorted(source for source, series in self._series.items() if series)

    def __len__(self) -> int:
        with self._lock:
            return sum(len(series) for series in self._series.values())

    def _prune(self, series: deque[StoredReading]) -> None:
        oldest_allowed = epoch_ms(self._clock) - int(self.retention_s * 1000)
        while series and series[0].received_ms < oldest_allowed:
            series.popleft()


class ReadingReceiver:
    """Callback `on_reading` do GatewayUdpEndpoint: registro + memória + persistência + alarmes.

    `registry_sink` é o `RegistryReadingSink` do main (touch no registro e redescoberta de
    fontes desconhecidas). Leituras de fontes desconhecidas também são guardadas: os dados
    não se perdem enquanto a redescoberta não termina.
    Ex.: GatewayUdpEndpoint(sock, handler, ReadingReceiver(store, RegistryReadingSink(...), storage))
    """

    def __init__(
        self,
        store: ReadingStore,
        registry_sink: Callable[[sources_pb2.SensorReading, Address], None] = lambda _r, _s: None,
        storage: ReadingSink | None = None,
        clock: Clock = time.time,
    ) -> None:
        self._store = store
        self._registry_sink = registry_sink
        self._storage = storage
        self._clock = clock
        self._alarms: Counter[str] = Counter()
        self._lock = threading.Lock()

    def __call__(self, reading: sources_pb2.SensorReading, sender: Address) -> None:
        if not reading.source_id:
            log.warning("Leitura sem source_id ignorada (de %s:%d)", *sender)
            return
        self._registry_sink(reading, sender)
        stored = StoredReading.from_proto(reading, epoch_ms(self._clock))
        self._store.add(stored)
        if self._storage is not None:
            self._storage.save(stored)
        if stored.is_event:
            self._record_alarm(stored)
        else:
            log.info("Leitura de %s: %s", stored.source_id, _format_values(stored))

    def alarm_counts(self) -> dict[str, int]:
        """Alarmes (leituras com is_event) recebidos por fonte desde que o Gateway subiu."""
        with self._lock:
            return dict(self._alarms)

    def _record_alarm(self, reading: StoredReading) -> None:
        with self._lock:
            self._alarms[reading.source_id] += 1
            total = self._alarms[reading.source_id]
        log.warning("ALARME de %s (%s): %s [%dº alarme desta fonte]",
                    reading.source_id, reading.source_type, _format_values(reading), total)


def _format_values(reading: StoredReading) -> str:
    return ", ".join(f"{m.metric}={m.value:g}{m.unit}" for m in reading.measurements) or "(sem medições)"
