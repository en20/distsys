"""Persistência das leituras em SQLite e consultas sobre o histórico (#16, pontuação extra).

Interface para os outros:
    storage.save(reading)                             # receptor UDP (#12) — não bloqueia
    storage.history(source_id, inicio_ms, fim_ms)     # servidor TCP / HistoryRequest (LG, #15)
    storage.readings(since_ms, until_ms, ...)         # consultas sobre janelas longas (#14)
    storage.record_event(source_id, "comando", "...") # comandos/mudanças de estado (Jon #13, Marcelo #11)

As escritas vão para uma fila e uma thread grava em lote (uma transação por lote):
o receptor UDP nunca espera o disco.
"""

import logging
import queue
import sqlite3
import threading
import time
from dataclasses import dataclass, replace
from pathlib import Path

from common.loops import BackgroundLoop
from common.runtime import Clock, epoch_ms
from gateway.readings import MetricValue, StoredReading

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS readings (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id           TEXT    NOT NULL,
    source_type         TEXT    NOT NULL,
    received_ms         INTEGER NOT NULL,
    source_timestamp_ms INTEGER NOT NULL,
    is_event            INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS readings_by_source_time ON readings (source_id, received_ms);
CREATE INDEX IF NOT EXISTS readings_by_time ON readings (received_ms);

CREATE TABLE IF NOT EXISTS measurements (
    reading_id INTEGER NOT NULL REFERENCES readings(id) ON DELETE CASCADE,
    metric     TEXT    NOT NULL,
    value      REAL    NOT NULL,
    unit       TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS measurements_by_reading ON measurements (reading_id);

CREATE TABLE IF NOT EXISTS source_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_ms INTEGER NOT NULL,
    source_id    TEXT    NOT NULL,
    kind         TEXT    NOT NULL,
    detail       TEXT    NOT NULL
);
"""

BATCH_SIZE = 500


@dataclass(frozen=True)
class SourceEvent:
    """Fato registrado sobre uma fonte (comando enviado, mudança de estado...)."""

    timestamp_ms: int
    source_id: str
    kind: str     # ex.: "comando", "estado"
    detail: str   # ex.: "SetSendInterval(5s)", "ONLINE → OFFLINE"


class SqliteStorage:
    """Banco SQLite do Gateway. `start()` sobe a thread de escrita; `stop()` grava o que falta.

    Ex.:
        storage = SqliteStorage("data/gateway.db"); storage.start()
        storage.save(stored_reading)
        storage.history("umidade-1", inicio_ms, fim_ms, metric="umidade")
    """

    def __init__(self, path: str | Path, clock: Clock = time.time) -> None:
        self.path = Path(path)
        self._clock = clock
        if str(path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(SCHEMA)
        self._db_lock = threading.Lock()
        self._queue: queue.Queue[StoredReading | SourceEvent] = queue.Queue()
        self._writer = BackgroundLoop("sqlite-writer", self._write_batch)
        self._closed = False

    # ----- escrita (não bloqueia quem chama) -------------------------------------

    def save(self, reading: StoredReading) -> None:
        self._enqueue(reading)

    def record_event(self, source_id: str, kind: str, detail: str, timestamp_ms: int | None = None) -> None:
        self._enqueue(SourceEvent(timestamp_ms or epoch_ms(self._clock), source_id, kind, detail))

    def flush(self) -> None:
        """Bloqueia até tudo que foi enfileirado estar gravado (usado no stop e nos testes)."""
        self._queue.join()

    # ----- ciclo de vida ---------------------------------------------------------

    def start(self) -> None:
        self._writer.start()
        log.info("Persistência em %s (%d leituras gravadas)", self.path, self.count())

    def stop(self) -> None:
        if self._closed:
            return
        self._closed = True  # novos save() são descartados com aviso
        self._writer.stop()
        self._write_pending()  # o que chegou depois da última volta do writer
        with self._db_lock:
            self._conn.close()

    # ----- leitura -----------------------------------------------------------------

    def readings(
        self,
        since_ms: int | None = None,
        until_ms: int | None = None,
        source_id: str | None = None,
        source_type: str | None = None,
    ) -> list[StoredReading]:
        """Leituras persistidas no intervalo, em ordem de chegada (mesma interface do ReadingStore)."""
        return self._select(since_ms, until_ms, source_id, source_type)

    def history(
        self,
        source_id: str,
        start_ms: int | None = None,
        end_ms: int | None = None,
        metric: str | None = None,
        limit: int | None = None,
    ) -> list[StoredReading]:
        """Série temporal de uma fonte; com `metric`, só as leituras que têm essa grandeza.

        Com `limit`, devolve as `limit` leituras mais recentes do intervalo (em ordem cronológica).
        """
        rows = self._select(start_ms, end_ms, source_id, None, metric=metric, limit=limit)
        if metric:
            rows = [_only_metric(r, metric) for r in rows]
        return rows

    def source_events(self, source_id: str | None = None, since_ms: int | None = None) -> list[SourceEvent]:
        sql, params = "SELECT timestamp_ms, source_id, kind, detail FROM source_events WHERE 1=1", []
        if source_id is not None:
            sql, params = sql + " AND source_id = ?", params + [source_id]
        if since_ms is not None:
            sql, params = sql + " AND timestamp_ms >= ?", params + [since_ms]
        with self._db_lock:
            rows = self._conn.execute(sql + " ORDER BY timestamp_ms, id", params).fetchall()
        return [SourceEvent(*row) for row in rows]

    def count(self) -> int:
        with self._db_lock:
            return self._conn.execute("SELECT COUNT(*) FROM readings").fetchone()[0]

    # ----- internos ----------------------------------------------------------------

    def _enqueue(self, item: "StoredReading | SourceEvent") -> None:
        if self._closed:
            log.warning("Persistência já encerrada; item descartado: %s", item)
            return
        self._queue.put(item)

    def _write_batch(self) -> None:
        try:
            first = self._queue.get(timeout=0.5)
        except queue.Empty:
            return
        batch = [first]
        while len(batch) < BATCH_SIZE:
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
        self._commit(batch)

    def _write_pending(self) -> None:
        batch = []
        while True:
            try:
                batch.append(self._queue.get_nowait())
            except queue.Empty:
                break
        if batch:
            self._commit(batch)

    def _commit(self, batch: list["StoredReading | SourceEvent"]) -> None:
        try:
            with self._db_lock, self._conn:
                for item in batch:
                    if isinstance(item, StoredReading):
                        self._insert_reading(item)
                    else:
                        self._conn.execute(
                            "INSERT INTO source_events (timestamp_ms, source_id, kind, detail) VALUES (?, ?, ?, ?)",
                            (item.timestamp_ms, item.source_id, item.kind, item.detail),
                        )
        except sqlite3.Error:
            log.exception("Falha ao gravar %d itens no SQLite; lote descartado", len(batch))
        finally:
            for _ in batch:
                self._queue.task_done()

    def _insert_reading(self, reading: StoredReading) -> None:
        cursor = self._conn.execute(
            "INSERT INTO readings (source_id, source_type, received_ms, source_timestamp_ms, is_event)"
            " VALUES (?, ?, ?, ?, ?)",
            (reading.source_id, reading.source_type, reading.received_ms, reading.source_timestamp_ms,
             int(reading.is_event)),
        )
        self._conn.executemany(
            "INSERT INTO measurements (reading_id, metric, value, unit) VALUES (?, ?, ?, ?)",
            [(cursor.lastrowid, m.metric, m.value, m.unit) for m in reading.measurements],
        )

    def _select(
        self,
        since_ms: int | None,
        until_ms: int | None,
        source_id: str | None,
        source_type: str | None,
        metric: str | None = None,
        limit: int | None = None,
    ) -> list[StoredReading]:
        where, params = ["1=1"], []
        for clause, value in (("received_ms >= ?", since_ms), ("received_ms <= ?", until_ms),
                              ("source_id = ?", source_id), ("source_type = ?", source_type)):
            if value is not None:
                where.append(clause)
                params.append(value)
        if metric is not None:
            where.append("id IN (SELECT reading_id FROM measurements WHERE metric = ?)")
            params.append(metric)
        sql = (f"SELECT id, source_id, source_type, received_ms, source_timestamp_ms, is_event"
               f" FROM readings WHERE {' AND '.join(where)} ORDER BY received_ms DESC, id DESC")
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with self._db_lock:
            rows = self._conn.execute(sql, params).fetchall()
            measurements = self._measurements_for([row[0] for row in rows])
        rows.reverse()  # cronológica
        return [
            StoredReading(source_id=sid, source_type=stype, received_ms=received, source_timestamp_ms=source_ts,
                          is_event=bool(event), measurements=measurements.get(rid, ()))
            for rid, sid, stype, received, source_ts, event in rows
        ]

    def _measurements_for(self, reading_ids: list[int]) -> dict[int, tuple[MetricValue, ...]]:
        grouped: dict[int, list[MetricValue]] = {}
        for start in range(0, len(reading_ids), 900):  # limite de parâmetros do SQLite
            chunk = reading_ids[start:start + 900]
            placeholders = ",".join("?" * len(chunk))
            for rid, metric, value, unit in self._conn.execute(
                f"SELECT reading_id, metric, value, unit FROM measurements WHERE reading_id IN ({placeholders})"
                " ORDER BY rowid", chunk,
            ):
                grouped.setdefault(rid, []).append(MetricValue(metric, value, unit))
        return {rid: tuple(values) for rid, values in grouped.items()}


def _only_metric(reading: StoredReading, metric: str) -> StoredReading:
    return replace(reading, measurements=tuple(m for m in reading.measurements if m.metric == metric))
