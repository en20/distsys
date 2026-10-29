"""Fontes do domínio sem rede: simulação física e validação dos comandos (portado de `enzo/fontes-de-dados`)."""

import math

import pytest

from pb import sources_pb2 as pb
from sources import datasource
from sources.config import SourceConfig
from sources.datasource import InvalidArgument, Unsupported
from sources.reservoir import HYSTERESIS, PUMP_OFF_ABOVE, PUMP_ON_BELOW, ReservoirLevelSensor
from sources.soil_moisture import SoilMoistureSensor
from sources.valve import IrrigationValve
from sources.weather_station import WeatherStation

ALL_SOURCES = [IrrigationValve, ReservoirLevelSensor, SoilMoistureSensor, WeatherStation]


def simulate(source, seconds: float, dt: float = 1.0):
    for _ in range(int(seconds / dt)):
        source.update(dt)


# ----- modelos físicos -----------------------------------------------------------------

@pytest.mark.parametrize("cls", ALL_SOURCES)
def test_measurements_match_declared_specs_and_stay_in_range(make_source, cls):
    source = make_source(cls, start=False)
    for _ in range(3000):
        source.update(1.0)
        values = source.measure()
        assert set(values) == set(cls.MEASUREMENTS)
        for name, value in values.items():
            spec = cls.MEASUREMENTS[name]
            assert spec.min <= value <= spec.max, f"{name}={value} fora de [{spec.min}, {spec.max}]"


def test_at_least_three_types_with_controllable_and_continuous():
    assert len({cls.TYPE for cls in ALL_SOURCES}) >= 3
    assert any(cls.CONTROLLABLE for cls in ALL_SOURCES)
    assert any(not cls.CONTROLLABLE for cls in ALL_SOURCES)


class TestValve:
    def test_flow_converges_to_target_and_accumulates_volume(self, make_source):
        valve = make_source(IrrigationValve, start=False)
        valve.set_parameter("vazao_alvo_lpm", 30)
        simulate(valve, 30)
        assert valve.measure()["vazao"] == pytest.approx(30, rel=0.1)

        valve.after_send()
        simulate(valve, 60)
        assert valve.measure()["volume"] == pytest.approx(30, rel=0.1)  # 30 L/min durante 1 min

    def test_closing_valve_stops_flow(self, make_source):
        valve = make_source(IrrigationValve, start=False)
        simulate(valve, 10)
        valve.set_power(False)
        simulate(valve, 30)
        assert valve.measure()["vazao"] < 0.01

    def test_volume_resets_after_each_reading(self, make_source):
        valve = make_source(IrrigationValve, start=False)
        simulate(valve, 10)
        assert valve.measure()["volume"] > 0
        valve.after_send()
        assert valve.measure()["volume"] == 0


class TestReservoir:
    def test_level_drains_and_pump_refills(self, make_source):
        res = make_source(ReservoirLevelSensor, start=False)
        start = res.level
        simulate(res, 60)
        assert res.level < start  # bomba desligada: nível cai

        res.level = PUMP_ON_BELOW - 1
        simulate(res, 5)
        assert res.pump_on and res.level > PUMP_ON_BELOW - 1
        simulate(res, 400)
        assert not res.pump_on and res.level <= PUMP_OFF_ABOVE + 5

    def test_event_on_crossing_threshold_only_once(self, make_source):
        res = make_source(ReservoirLevelSensor, start=False)
        res.level = 50
        assert res.check_event() is None

        res.set_threshold(60)
        event = res.check_event()
        assert event and "abaixo do limiar" in event
        assert res.check_event() is None  # sem repetir enquanto continua abaixo

    def test_hysteresis_and_normalization_event(self, make_source):
        res = make_source(ReservoirLevelSensor, start=False)
        res.set_threshold(40)
        res.level = 39.5
        assert res.check_event()
        res.level = 40 + HYSTERESIS / 2  # ruído logo acima do limiar não normaliza
        assert res.check_event() is None
        res.level = 40 + HYSTERESIS + 0.1
        event = res.check_event()
        assert event and "normalizado" in event

    def test_turning_back_on_reevaluates_alarm(self, make_source):
        res = make_source(ReservoirLevelSensor, start=False)
        res.set_threshold(80)
        res.level = 50
        assert res.check_event()
        res.set_power(False)
        res.set_power(True)
        assert res.check_event()


class TestSoilMoisture:
    def test_each_plot_has_its_own_profile(self, make_source):
        a = make_source(SoilMoistureSensor, start=False, source_id="umidade-1")
        b = make_source(SoilMoistureSensor, start=False, source_id="umidade-2")
        assert (a.mean, a.volatility) != (b.mean, b.volatility)

    def test_profile_is_stable_for_the_same_id(self, make_source):
        a = make_source(SoilMoistureSensor, start=False, source_id="umidade-1")
        b = make_source(SoilMoistureSensor, start=False, source_id="umidade-1")
        assert (a.mean, a.volatility) == (b.mean, b.volatility)

    def test_moisture_reverts_to_plot_mean(self, make_source):
        sensor = make_source(SoilMoistureSensor, start=False)
        sensor.moisture = 90
        simulate(sensor, 1000)
        assert abs(sensor.moisture - sensor.mean) < 15


class TestWeatherStation:
    def test_rain_episodes_happen_and_raise_humidity(self, make_source):
        station = make_source(WeatherStation, start=False)
        rainy, dry = [], []
        for _ in range(20000):
            station.update(1.0)
            (rainy if station.rain > 0 else dry).append(station.humidity)
        assert rainy and dry
        assert sum(rainy) / len(rainy) > sum(dry) / len(dry)


# ----- comandos (chamados diretamente, sem gRPC) -------------------------------------------

@pytest.mark.parametrize("seconds", [0.0, 0.4, 3601, math.nan, -5])
def test_invalid_send_interval_is_rejected(make_source, seconds):
    valve = make_source(IrrigationValve, start=False)
    with pytest.raises(InvalidArgument):
        valve.set_send_interval(seconds)
    assert valve.status().send_interval_s == 0.5


def test_valid_send_interval_updates_status(make_source):
    valve = make_source(IrrigationValve, start=False)
    assert valve.set_send_interval(5).send_interval_s == 5


def test_invalid_interval_in_config_is_rejected():
    with pytest.raises(InvalidArgument):
        IrrigationValve(SourceConfig("x", "IRRIGATION_VALVE", 0.1, 50051))


def test_controllable_flag_must_match_grpc_port():
    with pytest.raises(InvalidArgument, match="porta gRPC"):
        IrrigationValve(SourceConfig("x", "IRRIGATION_VALVE", 5.0, 0))
    with pytest.raises(InvalidArgument, match="não é controlável"):
        SoilMoistureSensor(SourceConfig("x", "SOIL_MOISTURE", 5.0, 50051))


def test_power_is_reported_in_status(make_source):
    valve = make_source(IrrigationValve, start=False)
    assert valve.status().powered_on
    assert not valve.set_power(False).powered_on
    assert valve.set_power(True).powered_on


def test_threshold_only_on_sources_that_have_it(make_source):
    valve = make_source(IrrigationValve, start=False)
    res = make_source(ReservoirLevelSensor, start=False)
    with pytest.raises(Unsupported):
        valve.set_threshold(10)
    assert not valve.status().HasField("threshold")

    assert res.status().threshold == 30
    assert res.set_threshold(45).threshold == 45
    for bad in (-1, 101, math.nan):
        with pytest.raises(InvalidArgument):
            res.set_threshold(bad)


def test_parameters(make_source):
    valve = make_source(IrrigationValve, start=False)
    assert valve.status().parameters["vazao_alvo_lpm"] == 12
    assert valve.set_parameter("vazao_alvo_lpm", 25).parameters["vazao_alvo_lpm"] == 25
    with pytest.raises(InvalidArgument, match="desconhecido"):
        valve.set_parameter("pressao", 1)
    with pytest.raises(InvalidArgument):
        valve.set_parameter("vazao_alvo_lpm", 1000)

    res = make_source(ReservoirLevelSensor, start=False)
    with pytest.raises(Unsupported):
        res.set_parameter("qualquer", 1)


def test_announcement_identifies_the_source(make_source):
    soil = make_source(SoilMoistureSensor, start=False, source_id="umidade-9")
    announcement = soil.build_announcement("10.0.0.5", request_id=1)
    assert (announcement.source_id, announcement.type, announcement.controllable, announcement.grpc_port) == \
        ("umidade-9", "SOIL_MOISTURE", False, 0)
    assert announcement.initial_state.source_id == "umidade-9"


def test_failure_with_duration_expires(make_source, monkeypatch):
    clock = [1_000_000]
    monkeypatch.setattr(datasource, "now_ms", lambda: clock[0])
    valve = make_source(IrrigationValve, start=False)

    status = valve.simulate_failure(pb.FAILURE_SILENT, 10)
    assert status.failure == pb.FAILURE_SILENT and status.failure_until_ms == 1_010_000
    assert valve.answers_discovery() is False

    clock[0] += 9_999
    assert valve.status().failure == pb.FAILURE_SILENT
    clock[0] += 1
    status = valve.status()
    assert status.failure == pb.FAILURE_NONE and status.failure_until_ms == 0
    assert valve.answers_discovery() is True


def test_failure_without_duration_lasts_until_cleared(make_source):
    valve = make_source(IrrigationValve, start=False)
    assert valve.simulate_failure(pb.FAILURE_OUT_OF_RANGE).failure_until_ms == 0
    assert valve.status().failure == pb.FAILURE_OUT_OF_RANGE
    assert valve.simulate_failure(pb.FAILURE_NONE).failure == pb.FAILURE_NONE


def test_unknown_failure_mode_is_rejected(make_source):
    valve = make_source(IrrigationValve, start=False)
    with pytest.raises(InvalidArgument):
        valve.simulate_failure(99)
