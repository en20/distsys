"""Fontes contínuas (#7): grandezas enviadas, não controláveis, várias instâncias."""

import pytest

from common.net import open_udp_socket
from sources.config import SourceConfig
from sources.soil_moisture import soil_moisture_source
from sources.weather_station import weather_station_source


@pytest.fixture
def listener():
    sock = open_udp_socket(0)
    yield sock
    sock.close()


def config(source_id: str, grpc_port: int = 0) -> SourceConfig:
    return SourceConfig(source_id, "TESTE", 5.0, grpc_port)


def test_weather_station_sends_several_measurements_in_one_reading(listener) -> None:
    source = weather_station_source(config("estacao-1"), seed=1, listener_sock=listener)
    try:
        reading = source.sample()
        assert [(m.metric, m.unit) for m in reading.measurements] == [
            ("temperatura", "°C"), ("umidade_ar", "%"), ("vento", "km/h")]
    finally:
        source.stop()


def test_continuous_source_is_announced_as_not_controllable(listener) -> None:
    source = soil_moisture_source(config("umidade-1"), seed=1, listener_sock=listener)
    try:
        announcement = source.build_announcement("10.0.0.5", 1)
        assert (announcement.controllable, announcement.grpc_port) == (False, 0)
    finally:
        source.stop()


def test_continuous_source_rejects_grpc_port(listener) -> None:
    with pytest.raises(ValueError, match="não é controlável"):
        soil_moisture_source(config("umidade-1", grpc_port=50051), listener_sock=listener)


def test_soil_moisture_profile_depends_on_the_id(listener) -> None:
    sources = [soil_moisture_source(config(f"umidade-{i}"), listener_sock=open_udp_socket(0)) for i in range(3)]
    try:
        bases = [s.metrics[0].generator.base for s in sources]
        assert len(set(bases)) == 3  # cada talhão com sua umidade média
        again = soil_moisture_source(config("umidade-0"), listener_sock=listener)
        assert again.metrics[0].generator.base == bases[0]  # estável entre execuções
        again.stop()
    finally:
        for source in sources:
            source.stop()


def test_soil_moisture_values_stay_in_the_physical_range(listener) -> None:
    source = soil_moisture_source(config("umidade-1"), base=1.0, noise=5.0, seed=3, listener_sock=listener)
    try:
        values = [source.sample().measurements[0].value for _ in range(200)]
        assert all(0.0 <= v <= 100.0 for v in values)
    finally:
        source.stop()
