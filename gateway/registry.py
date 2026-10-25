"""Registro das fontes conhecidas pelo Gateway e detecção de falhas (#11).

Interface usada pelos outros módulos:
    registry.upsert(announcement)            # descoberta (#10)
    registry.touch(source_id, reading)       # receptor UDP (Enzo, #12)
    registry.update_status(source_id, st)    # comandos gRPC (Jon, #13)
    registry.mark_offline(source_id, motivo) # gRPC UNAVAILABLE (Jon, #13)
    registry.get(source_id) / registry.list()  # servidor TCP (LG, #15)
"""

import logging
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from enum import Enum
from typing import TypeVar

from common.runtime import Clock
from pb import sources_pb2

log = logging.getLogger(__name__)

# TypeVar (e não `def f[M]`) para manter compatibilidade com Python 3.11.
ProtoT = TypeVar("ProtoT", sources_pb2.SourceStatus, sources_pb2.SensorReading)


class SourceState(str, Enum):
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"
    POWERED_OFF = "DESLIGADA"


@dataclass
class SourceRecord:
    source_id: str
    source_type: str
    ip: str
    grpc_port: int
    controllable: bool
    status: sources_pb2.SourceStatus
    state: SourceState
    last_seen: float  # epoch s
    last_reading: sources_pb2.SensorReading | None = None


StateListener = Callable[[SourceRecord, SourceState], None]  # (registro já atualizado, estado anterior)


class SourceRegistry:
    """Mapa thread-safe source_id → SourceRecord. Leituras devolvem cópias.

    Uma fonte ONLINE vira OFFLINE quando fica `offline_factor` × intervalo de envio sem
    mandar nada (mínimo `min_timeout_s`). Fontes desligadas por comando não expiram.
    Ex.: registry = SourceRegistry(); registry.upsert(ann); registry.sweep()
    """

    def __init__(
        self,
        clock: Clock = time.time,
        offline_factor: float = 3.0,
        min_timeout_s: float = 5.0,
        listeners: Iterable[StateListener] = (),
    ) -> None:
        self._clock = clock
        self._offline_factor = offline_factor
        self._min_timeout_s = min_timeout_s
        self._listeners = list(listeners) or [_log_state_change]
        self._records: dict[str, SourceRecord] = {}
        self._lock = threading.Lock()

    def upsert(self, announcement: sources_pb2.SourceAnnouncement) -> bool:
        """Registra ou atualiza a fonte anunciada; True se ela é nova."""
        with self._lock:
            previous = self._records.get(announcement.source_id)
            record = _record_from_announcement(announcement, self._clock())
            if previous is not None:
                record.last_reading = previous.last_reading
            self._records[record.source_id] = record
            snapshot = _snapshot(record)
        self._notify_if_changed(snapshot, previous.state if previous else None)
        return previous is None

    def touch(self, source_id: str, reading: sources_pb2.SensorReading) -> bool:
        """Marca a fonte como viva e guarda a última leitura; False se ela é desconhecida."""
        with self._lock:
            record = self._records.get(source_id)
            if record is None:
                return False
            old_state = record.state
            record.last_seen = self._clock()
            record.last_reading = _copy_proto(reading)
            record.state = _state_for(record.status)
            snapshot = _snapshot(record)
        self._notify_if_changed(snapshot, old_state)
        return True

    def update_status(self, source_id: str, status: sources_pb2.SourceStatus) -> bool:
        """Grava o estado devolvido pela fonte via gRPC; False se ela é desconhecida."""
        with self._lock:
            record = self._records.get(source_id)
            if record is None:
                return False
            old_state = record.state
            record.status = _copy_proto(status)
            record.last_seen = self._clock()
            record.state = _state_for(record.status)
            snapshot = _snapshot(record)
        self._notify_if_changed(snapshot, old_state)
        return True

    def mark_offline(self, source_id: str, reason: str) -> bool:
        """Força OFFLINE (ex.: gRPC UNAVAILABLE); False se a fonte é desconhecida."""
        with self._lock:
            record = self._records.get(source_id)
            if record is None:
                return False
            old_state, record.state = record.state, SourceState.OFFLINE
            snapshot = _snapshot(record)
        if old_state != SourceState.OFFLINE:
            log.warning("Fonte %s marcada OFFLINE: %s", source_id, reason)
        self._notify_if_changed(snapshot, old_state)
        return True

    def sweep(self) -> list[str]:
        """Marca OFFLINE as fontes ONLINE silenciosas há tempo demais; devolve os ids afetados."""
        now = self._clock()
        with self._lock:
            expired = [r for r in self._records.values() if self._is_expired(r, now)]
            for record in expired:
                record.state = SourceState.OFFLINE
            snapshots = [_snapshot(r) for r in expired]
        for snapshot in snapshots:
            self._notify_if_changed(snapshot, SourceState.ONLINE)
        return [s.source_id for s in snapshots]

    def get(self, source_id: str) -> SourceRecord | None:
        with self._lock:
            record = self._records.get(source_id)
            return _snapshot(record) if record else None

    def list(self) -> list[SourceRecord]:
        with self._lock:
            return [_snapshot(r) for r in sorted(self._records.values(), key=lambda r: r.source_id)]

    def offline_timeout_s(self, record: SourceRecord) -> float:
        return max(self._offline_factor * record.status.send_interval_s, self._min_timeout_s)

    def _is_expired(self, record: SourceRecord, now: float) -> bool:
        return record.state == SourceState.ONLINE and now - record.last_seen > self.offline_timeout_s(record)

    def _notify_if_changed(self, snapshot: SourceRecord, old_state: SourceState | None) -> None:
        if snapshot.state == old_state:
            return
        for listener in self._listeners:
            listener(snapshot, old_state)


def _state_for(status: sources_pb2.SourceStatus) -> SourceState:
    return SourceState.ONLINE if status.powered_on else SourceState.POWERED_OFF


def _record_from_announcement(announcement: sources_pb2.SourceAnnouncement, now: float) -> SourceRecord:
    status = _copy_proto(announcement.initial_state)
    status.source_id = announcement.source_id
    return SourceRecord(
        source_id=announcement.source_id,
        source_type=announcement.type,
        ip=announcement.ip,
        grpc_port=announcement.grpc_port,
        controllable=announcement.controllable,
        status=status,
        state=_state_for(status),
        last_seen=now,
    )


def _copy_proto(message: ProtoT) -> ProtoT:
    duplicate = type(message)()
    duplicate.CopyFrom(message)
    return duplicate


def _snapshot(record: SourceRecord) -> SourceRecord:
    reading = _copy_proto(record.last_reading) if record.last_reading else None
    return replace(record, status=_copy_proto(record.status), last_reading=reading)


def _log_state_change(record: SourceRecord, old_state: SourceState | None) -> None:
    if old_state is None:
        return  # fonte nova: a descoberta já loga
    log.info("Fonte %s: %s → %s", record.source_id, old_state.value, record.state.value)
