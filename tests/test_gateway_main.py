from gateway.main import RegistryReadingSink
from gateway.registry import SourceRegistry
from tests.builders import make_announcement, make_reading

SENDER = ("10.0.0.7", 40000)


class RediscoveryCounter:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self) -> None:
        self.calls += 1


def test_known_source_reading_updates_registry_without_rediscovery() -> None:
    registry, rediscovery = SourceRegistry(), RediscoveryCounter()
    registry.upsert(make_announcement("s1"))
    RegistryReadingSink(registry, rediscovery)(make_reading("s1", value=3.0), SENDER)
    assert registry.get("s1").last_reading.measurements[0].value == 3.0
    assert rediscovery.calls == 0


def test_unknown_source_reading_requests_rediscovery() -> None:
    rediscovery = RediscoveryCounter()
    sink = RegistryReadingSink(SourceRegistry(), rediscovery)
    sink(make_reading("ghost"), SENDER)
    sink(make_reading("ghost"), SENDER)
    assert rediscovery.calls == 2
