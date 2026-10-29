import logging

from gateway.main import RegistryReadingSink
from gateway.readings import ReadingReceiver, ReadingStore, StoredReading
from gateway.registry import SourceRegistry
from tests.builders import make_announcement, make_reading, make_stored
from tests.fakes import FakeClock

SENDER = ("10.0.0.7", 40000)
HOUR_MS = 3_600_000


class RecordingStorage:
    def __init__(self) -> None:
        self.saved: list[StoredReading] = []

    def save(self, reading: StoredReading) -> None:
        self.saved.append(reading)


# ----- ReadingStore -----------------------------------------------------------------

def test_store_filters_by_window_source_and_type() -> None:
    clock = FakeClock(start=10_000.0)
    store = ReadingStore(clock=clock)
    store.add(make_stored("a", 1_000_000, source_type="T1"))
    store.add(make_stored("b", 2_000_000, source_type="T2"))
    store.add(make_stored("a", 3_000_000, source_type="T1"))

    assert [r.received_ms for r in store.readings()] == [1_000_000, 2_000_000, 3_000_000]
    assert [r.received_ms for r in store.readings(source_id="a")] == [1_000_000, 3_000_000]
    assert [r.source_id for r in store.readings(source_type="T2")] == ["b"]
    assert [r.received_ms for r in store.readings(since_ms=1_500_000, until_ms=3_000_000)] == [2_000_000, 3_000_000]
    assert store.readings(source_id="ninguem") == []
    assert store.latest("a").received_ms == 3_000_000 and store.latest("x") is None
    assert store.source_ids() == ["a", "b"] and len(store) == 3


def test_old_readings_are_dropped_after_retention() -> None:
    clock = FakeClock(start=100_000.0)
    store = ReadingStore(retention_s=3600, clock=clock)
    now_ms = int(clock() * 1000)
    store.add(make_stored("a", now_ms - HOUR_MS - 1))
    store.add(make_stored("a", now_ms))
    assert [r.received_ms for r in store.readings()] == [now_ms]


def test_retention_covers_24_hours_by_default() -> None:
    assert ReadingStore().retention_s >= 24 * 3600


def test_per_source_cap_bounds_memory() -> None:
    store = ReadingStore(clock=FakeClock(start=10_000.0), max_per_source=3)
    for i in range(5):
        store.add(make_stored("a", 9_000_000 + i))
    assert [r.received_ms for r in store.readings()] == [9_000_002, 9_000_003, 9_000_004]


def test_load_restores_history_in_order_and_skips_expired() -> None:
    clock = FakeClock(start=100_000.0)
    store = ReadingStore(retention_s=3600, clock=clock)
    now_ms = int(clock() * 1000)
    loaded = store.load([make_stored("a", now_ms), make_stored("a", now_ms - 2 * HOUR_MS), make_stored("b", now_ms - 10)])
    assert loaded == 2
    assert [(r.source_id, r.received_ms) for r in store.readings()] == [("b", now_ms - 10), ("a", now_ms)]


def test_stored_reading_from_proto_keeps_all_measurements() -> None:
    reading = make_reading("s1", value=7.0, is_event=True)
    reading.measurements.add(metric="temperatura", value=21.5, unit="°C")
    stored = StoredReading.from_proto(reading, received_ms=99)
    assert (stored.source_id, stored.source_type, stored.received_ms, stored.is_event) == ("s1", "SENSOR_TESTE", 99, True)
    assert stored.value_of("temperatura").value == 21.5 and stored.value_of("pressao") is None


# ----- ReadingReceiver ----------------------------------------------------------------

def make_receiver(registry: SourceRegistry | None = None):
    registry = registry or SourceRegistry()
    rediscoveries: list[int] = []
    store, storage = ReadingStore(clock=FakeClock(start=50_000.0)), RecordingStorage()
    sink = RegistryReadingSink(registry, lambda: rediscoveries.append(1))
    receiver = ReadingReceiver(store, sink, storage, clock=FakeClock(start=50_000.0))
    return receiver, registry, store, storage, rediscoveries


def test_reading_is_stored_persisted_and_keeps_source_alive() -> None:
    registry = SourceRegistry()
    registry.upsert(make_announcement("s1"))
    receiver, _, store, storage, rediscoveries = make_receiver(registry)

    receiver(make_reading("s1", value=33.0), SENDER)

    stored = store.latest("s1")
    assert stored.received_ms == 50_000_000  # relógio do Gateway, não o da fonte
    assert stored.value_of("umidade").value == 33.0
    assert storage.saved == [stored]
    assert registry.get("s1").last_reading.measurements[0].value == 33.0
    assert rediscoveries == []


def test_unknown_source_is_stored_and_triggers_rediscovery() -> None:
    receiver, _, store, _, rediscoveries = make_receiver()
    receiver(make_reading("fantasma"), SENDER)
    assert store.latest("fantasma") is not None
    assert rediscoveries == [1]


def test_events_are_counted_and_logged_as_alarms(caplog) -> None:
    receiver, *_ = make_receiver()
    with caplog.at_level(logging.WARNING, logger="gateway.readings"):
        receiver(make_reading("s1", is_event=True), SENDER)
        receiver(make_reading("s1", is_event=True), SENDER)
        receiver(make_reading("s2"), SENDER)
    assert receiver.alarm_counts() == {"s1": 2}
    alarms = [r.getMessage() for r in caplog.records if "ALARME" in r.getMessage()]
    assert len(alarms) == 2 and "s1" in alarms[0] and "umidade=42%" in alarms[0]


def test_reading_without_source_id_is_ignored() -> None:
    receiver, _, store, storage, _ = make_receiver()
    receiver(make_reading(""), SENDER)
    assert len(store) == 0 and storage.saved == []
