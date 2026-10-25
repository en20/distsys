"""Simulação física de cada tipo de fonte (sem rede)."""

import pytest

from sources.reservoir import HYSTERESIS, PUMP_OFF_ABOVE, PUMP_ON_BELOW, ReservoirLevelSensor
from sources.soil_moisture import SoilMoistureSensor
from sources.valve import IrrigationValve
from sources.weather_station import WeatherStation

ALL_SOURCES = [IrrigationValve, ReservoirLevelSensor, SoilMoistureSensor, WeatherStation]


def simulate(source, seconds: float, dt: float = 1.0):
    for _ in range(int(seconds / dt)):
        source.update(dt)


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
