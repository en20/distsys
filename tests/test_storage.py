import threading
import time
from pathlib import Path

import pytest

from gateway.readings import ReadingStore
from gateway.storage import SqliteStorage
from tests.builders import make_stored
from tests.fakes import FakeClock


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "dados" / "gateway.db"


@pytest.fixture
def storage(db_path: Path):
    storage = SqliteStorage(db_path)
    storage.start()
    yield storage
    storage.stop()


def test_saved_readings_come_back_from_history(storage: SqliteStorage) -> None:
    storage.save(make_stored("s1", 1_000, umidade=30.0, temperatura=20.0))
    storage.save(make_stored("s1", 2_000, is_event=True, umidade=10.0))
    storage.save(make_stored("s2", 1_500, umidade=50.0))
    storage.flush()

    history = storage.history("s1")
    assert [(r.received_ms, r.is_event) for r in history] == [(1_000, False), (2_000, True)]
    assert {m.metric: m.value for m in history[0].measurements} == {"umidade": 30.0, "temperatura": 20.0}
    assert storage.count() == 3


def test_history_by_interval_metric_and_limit(storage: SqliteStorage) -> None:
    for i in range(10):
        metrics = {"umidade": float(i)} if i % 2 == 0 else {"temperatura": float(i)}
        storage.save(make_stored("s1", 1_000 * i, **metrics))
    storage.flush()

    assert [r.received_ms for r in storage.history("s1", 2_000, 5_000)] == [2_000, 3_000, 4_000, 5_000]
    only_humidity = storage.history("s1", metric="umidade")
    assert [r.measurements[0].value for r in only_humidity] == [0.0, 2.0, 4.0, 6.0, 8.0]
    assert all(len(r.measurements) == 1 for r in only_humidity)
    assert [r.received_ms for r in storage.history("s1", limit=3)] == [7_000, 8_000, 9_000]


def test_readings_interface_matches_the_memory_store(storage: SqliteStorage) -> None:
    memory = ReadingStore(clock=FakeClock(start=10.0))
    for reading in [make_stored("a", 1_000, source_type="T1"), make_stored("b", 2_000, source_type="T2"),
                    make_stored("a", 3_000, source_type="T1", umidade=1.0)]:
        storage.save(reading)
        memory.add(reading)
    storage.flush()
    for kwargs in [{}, {"source_id": "a"}, {"source_type": "T2"}, {"since_ms": 1_500, "until_ms": 3_000}]:
        assert storage.readings(**kwargs) == memory.readings(**kwargs), kwargs


def test_history_survives_gateway_restart(db_path: Path) -> None:
    first = SqliteStorage(db_path)
    first.start()
    first.save(make_stored("s1", 1_000, umidade=12.0))
    first.record_event("s1", "estado", "ONLINE → OFFLINE", timestamp_ms=1_500)
    first.stop()  # grava o que estava na fila

    second = SqliteStorage(db_path)
    second.start()
    try:
        assert [r.measurements[0].value for r in second.history("s1")] == [12.0]
        events = second.source_events("s1")
        assert [(e.kind, e.detail) for e in events] == [("estado", "ONLINE → OFFLINE")]
    finally:
        second.stop()


def test_source_events_filters(storage: SqliteStorage) -> None:
    storage.record_event("s1", "comando", "SetPower(off)", timestamp_ms=1_000)
    storage.record_event("s2", "comando", "SetSendInterval(5s)", timestamp_ms=2_000)
    storage.flush()
    assert [e.source_id for e in storage.source_events()] == ["s1", "s2"]
    assert [e.detail for e in storage.source_events(since_ms=1_500)] == ["SetSendInterval(5s)"]


def test_save_never_blocks_the_udp_receiver(storage: SqliteStorage) -> None:
    started = time.perf_counter()
    for i in range(2_000):
        storage.save(make_stored("s1", i, umidade=float(i)))
    assert time.perf_counter() - started < 0.5  # só enfileira
    storage.flush()
    assert storage.count() == 2_000


def test_concurrent_writes_and_reads(storage: SqliteStorage) -> None:
    def writer(source_id: str) -> None:
        for i in range(300):
            storage.save(make_stored(source_id, i, umidade=1.0))

    threads = [threading.Thread(target=writer, args=(f"s{n}",)) for n in range(4)]
    for t in threads:
        t.start()
    while any(t.is_alive() for t in threads):
        storage.readings(source_id="s0")  # leitura enquanto grava
    for t in threads:
        t.join()
    storage.flush()
    assert storage.count() == 1_200


def test_save_after_stop_is_dropped_with_warning(db_path: Path, caplog) -> None:
    storage = SqliteStorage(db_path)
    storage.start()
    storage.stop()
    storage.save(make_stored())
    storage.flush()  # não trava
    assert "já encerrada" in caplog.text


def test_stop_writes_what_is_still_queued(db_path: Path) -> None:
    storage = SqliteStorage(db_path)  # writer não iniciado: tudo fica na fila, como numa corrida no Ctrl+C
    for i in range(5):
        storage.save(make_stored("s1", i))
    storage.stop()
    reopened = SqliteStorage(db_path)
    try:
        assert reopened.count() == 5
    finally:
        reopened.stop()
