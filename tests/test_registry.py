from gateway.registry import SourceRecord, SourceRegistry, SourceState
from pb import sources_pb2
from tests.builders import make_announcement, make_reading
from tests.fakes import FakeClock


class RecordingListener:
    def __init__(self) -> None:
        self.changes: list[tuple[str, SourceState | None, SourceState]] = []

    def __call__(self, record: SourceRecord, old: SourceState | None) -> None:
        self.changes.append((record.source_id, old, record.state))


def make_registry() -> tuple[SourceRegistry, FakeClock, RecordingListener]:
    clock, listener = FakeClock(), RecordingListener()
    return SourceRegistry(clock=clock, offline_factor=3.0, min_timeout_s=1.0, listeners=[listener]), clock, listener


def test_upsert_reports_new_only_the_first_time() -> None:
    registry, _, _ = make_registry()
    assert registry.upsert(make_announcement("s1")) is True
    assert registry.upsert(make_announcement("s1")) is False
    assert [r.source_id for r in registry.list()] == ["s1"]


def test_silent_source_goes_offline_after_three_intervals() -> None:
    registry, clock, listener = make_registry()
    registry.upsert(make_announcement("s1", interval_s=2.0))
    clock.advance(6.0)
    assert registry.sweep() == []
    clock.advance(0.1)
    assert registry.sweep() == ["s1"]
    assert listener.changes[-1] == ("s1", SourceState.ONLINE, SourceState.OFFLINE)


def test_reading_brings_offline_source_back_and_keeps_last_value() -> None:
    registry, clock, _ = make_registry()
    registry.upsert(make_announcement("s1"))
    registry.mark_offline("s1", "gRPC UNAVAILABLE")
    clock.advance(1.0)
    assert registry.touch("s1", make_reading("s1", value=12.5))
    record = registry.get("s1")
    assert record.state == SourceState.ONLINE
    assert record.last_seen == clock.now
    assert record.last_reading.measurements[0].value == 12.5


def test_powered_off_source_never_times_out() -> None:
    registry, clock, _ = make_registry()
    registry.upsert(make_announcement("s1", powered_on=False))
    clock.advance(1_000.0)
    assert registry.sweep() == []
    assert registry.get("s1").state == SourceState.POWERED_OFF


def test_status_update_from_grpc_changes_state_and_timeout() -> None:
    registry, _, listener = make_registry()
    registry.upsert(make_announcement("s1", interval_s=15.0))
    registry.update_status("s1", sources_pb2.SourceStatus(powered_on=False, send_interval_s=5.0))
    record = registry.get("s1")
    assert record.state == SourceState.POWERED_OFF
    assert registry.offline_timeout_s(record) == 15.0
    assert listener.changes[-1] == ("s1", SourceState.ONLINE, SourceState.POWERED_OFF)


def test_unknown_source_operations_return_false() -> None:
    registry, _, _ = make_registry()
    assert not registry.touch("ghost", make_reading("ghost"))
    assert not registry.update_status("ghost", sources_pb2.SourceStatus())
    assert not registry.mark_offline("ghost", "teste")
    assert registry.get("ghost") is None


def test_returned_records_are_isolated_copies() -> None:
    registry, _, _ = make_registry()
    registry.upsert(make_announcement("s1"))
    registry.get("s1").status.powered_on = False
    assert registry.get("s1").status.powered_on is True
