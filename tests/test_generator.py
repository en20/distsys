import random
import statistics

import pytest

from sources.generator import ValueGenerator


def make_generator(**overrides) -> ValueGenerator:
    params = {"base": 50.0, "noise": 1.0, "drift": 0.5, "rng": random.Random(7)} | overrides
    return ValueGenerator(**params)


def test_values_stay_inside_the_physical_range() -> None:
    generator = make_generator(noise=20.0, drift=5.0, minimum=0.0, maximum=100.0)
    values = [generator.next(1.0) for _ in range(5_000)]
    assert min(values) >= 0.0 and max(values) <= 100.0


def test_values_oscillate_around_the_base() -> None:
    generator = make_generator()
    values = [generator.next(1.0) for _ in range(20_000)]
    assert statistics.fmean(values) == pytest.approx(50.0, abs=2.0)
    assert statistics.stdev(values) > 0.5  # varia de verdade


def test_more_noise_means_more_variation() -> None:
    calm_gen, wild_gen = make_generator(noise=0.2, drift=0.0), make_generator(noise=5.0, drift=0.0)
    calm = [calm_gen.next() for _ in range(2_000)]
    wild = [wild_gen.next() for _ in range(2_000)]
    assert statistics.stdev(wild) > 10 * statistics.stdev(calm)


def test_trend_moves_the_center_linearly() -> None:
    generator = make_generator(noise=0.0, drift=0.0, trend_per_s=-0.5)
    generator.next(10.0)
    assert generator.center == pytest.approx(45.0)
    generator.next(10.0)
    assert generator.center == pytest.approx(40.0)


def test_drift_is_continuous_without_jumps() -> None:
    generator = make_generator(noise=0.0, drift=0.3)
    values = [generator.next(1.0) for _ in range(1_000)]
    biggest_step = max(abs(b - a) for a, b in zip(values, values[1:]))
    assert biggest_step < 2.0


def test_spike_shifts_value_and_decays_back() -> None:
    generator = make_generator(noise=0.0, drift=0.0, reversion_per_s=0.1)
    generator.spike(30.0)
    assert generator.next(0.0) == pytest.approx(80.0)
    for _ in range(100):
        generator.next(1.0)
    assert generator.center == pytest.approx(50.0, abs=0.1)


def test_same_seed_gives_same_series() -> None:
    a, b = make_generator(rng=random.Random(1)), make_generator(rng=random.Random(1))
    assert [a.next() for _ in range(50)] == [b.next() for _ in range(50)]


@pytest.mark.parametrize("bad", [{"minimum": 10.0, "maximum": 0.0}, {"noise": -1.0}, {"drift": -0.1}])
def test_invalid_configuration_is_rejected(bad: dict) -> None:
    with pytest.raises(ValueError):
        make_generator(**bad)


def test_negative_time_step_is_rejected() -> None:
    with pytest.raises(ValueError):
        make_generator().next(-1.0)
