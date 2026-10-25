"""Validação dos comandos das fontes (chamados diretamente, sem gRPC)."""

import math

import pytest

from sources import base
from sources.base import InvalidArgument, SourceConfig, Unsupported
from sources.pb import pb
from sources.reservoir import ReservoirLevelSensor
from sources.soil_moisture import SoilMoistureSensor
from sources.valve import IrrigationValve


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
        IrrigationValve(SourceConfig(source_id="x", location="y", send_interval_s=0.1))


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


def test_status_identifies_the_source(make_source):
    soil = make_source(SoilMoistureSensor, start=False, source_id="umidade-9", location="talhao-9")
    info = soil.status().info
    assert (info.source_id, info.type, info.location, info.controllable) == \
        ("umidade-9", pb.SOIL_MOISTURE, "talhao-9", False)


def test_failure_with_duration_expires(make_source, monkeypatch):
    clock = [1_000_000]
    monkeypatch.setattr(base, "now_ms", lambda: clock[0])
    valve = make_source(IrrigationValve, start=False)

    status = valve.simulate_failure(pb.FAILURE_SILENT, 10)
    assert status.failure_mode == pb.FAILURE_SILENT and status.failure_until_ms == 1_010_000

    clock[0] += 9_999
    assert valve.status().failure_mode == pb.FAILURE_SILENT
    clock[0] += 1
    status = valve.status()
    assert status.failure_mode == pb.FAILURE_NONE and status.failure_until_ms == 0


def test_failure_without_duration_lasts_until_cleared(make_source):
    valve = make_source(IrrigationValve, start=False)
    assert valve.simulate_failure(pb.FAILURE_OUT_OF_RANGE).failure_until_ms == 0
    assert valve.status().failure_mode == pb.FAILURE_OUT_OF_RANGE
    assert valve.simulate_failure(pb.FAILURE_NONE).failure_mode == pb.FAILURE_NONE


def test_unknown_failure_mode_is_rejected(make_source):
    valve = make_source(IrrigationValve, start=False)
    with pytest.raises(InvalidArgument):
        valve.simulate_failure(99)
