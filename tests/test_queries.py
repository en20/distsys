import math
import statistics

import pytest

from gateway.queries import QueryEngine, QueryError, QueryFilter, QueryKind, describe_window
from gateway.readings import ReadingStore
from tests.builders import make_stored
from tests.fakes import FakeClock

NOW_S = 100_000.0
NOW_MS = int(NOW_S * 1000)
MIN_MS = 60_000

# Três talhões com umidades conhecidas nos últimos minutos.
DATA = {
    "umidade-1": [30.0, 32.0, 34.0],      # média 32, desvio 2
    "umidade-2": [20.0, 20.0, 20.0, 20.0],  # média 20, desvio 0  -> mais seco, menor variação
    "umidade-3": [40.0, 50.0, 60.0],      # média 50, desvio 10 -> maior variação
}


def make_engine(history=None) -> tuple[QueryEngine, ReadingStore]:
    clock = FakeClock(start=NOW_S)
    store = ReadingStore(clock=clock)
    for source_id, values in DATA.items():
        for i, value in enumerate(values):
            store.add(make_stored(source_id, NOW_MS - (i + 1) * MIN_MS, source_type="SENSOR_UMIDADE", umidade=value))
    store.add(make_stored("estacao-1", NOW_MS - MIN_MS, source_type="ESTACAO", temperatura=25.0, umidade=70.0))
    return QueryEngine(store, history=history, clock=clock), store


SOIL = QueryFilter(metric="umidade", source_type="SENSOR_UMIDADE")
ALL_SOIL_VALUES = [v for values in DATA.values() for v in values]


def test_average() -> None:
    engine, _ = make_engine()
    result = engine.run("media", SOIL, window_s=3600)
    assert result.value == pytest.approx(statistics.fmean(ALL_SOIL_VALUES))
    assert result.sample_count == 10 and result.unit == "%"
    assert [s.source_id for s in result.per_source] == ["umidade-3", "umidade-1", "umidade-2"]


def test_stddev() -> None:
    engine, _ = make_engine()
    result = engine.run(QueryKind.STDDEV, SOIL, window_s=3600)
    assert result.value == pytest.approx(statistics.stdev(ALL_SOIL_VALUES))
    by_source = {s.source_id: s.stddev for s in result.per_source}
    assert by_source == pytest.approx({"umidade-1": 2.0, "umidade-2": 0.0, "umidade-3": 10.0})


def test_min_and_max_report_the_source() -> None:
    engine, _ = make_engine()
    low, high = engine.run("minimo", SOIL), engine.run("maximo", SOIL)
    assert (low.value, low.source_id) == (20.0, "umidade-2")
    assert (high.value, high.source_id) == (60.0, "umidade-3")


def test_sum() -> None:
    engine, _ = make_engine()
    result = engine.run("soma", QueryFilter(metric="umidade", source_id="umidade-1"))
    assert result.value == pytest.approx(96.0) and result.sample_count == 3


def test_lowest_mean_is_the_driest_plot() -> None:
    engine, _ = make_engine()
    result = engine.run("menor_media", SOIL)
    assert (result.source_id, result.value) == ("umidade-2", 20.0)
    assert "umidade-2" in result.description and "20.00 %" in result.description


def test_highest_mean() -> None:
    engine, _ = make_engine()
    result = engine.run("maior_media", QueryFilter(metric="umidade"))  # inclui a estação (70 %)
    assert (result.source_id, result.value) == ("estacao-1", 70.0)


def test_source_with_largest_variation() -> None:
    engine, _ = make_engine()
    result = engine.run("maior_variacao", SOIL, window_s=24 * 3600)
    assert result.source_id == "umidade-3" and result.value == pytest.approx(10.0)
    assert [s.source_id for s in result.per_source] == ["umidade-3", "umidade-1", "umidade-2"]


def test_event_count_per_source() -> None:
    engine, store = make_engine()
    for i in range(3):
        store.add(make_stored("bomba-1", NOW_MS - i * 1000, is_event=True, source_type="BOMBA", pressao=9.0))
    store.add(make_stored("umidade-1", NOW_MS, is_event=True, source_type="SENSOR_UMIDADE", umidade=10.0))
    store.add(make_stored("bomba-1", NOW_MS - 2 * 3600 * 1000, is_event=True, pressao=9.0))  # fora da janela
    result = engine.run("contagem_eventos", window_s=3600)
    assert result.value == 4.0 and result.source_id == "bomba-1"
    assert {s.source_id: s.events for s in result.per_source}["bomba-1"] == 3
    assert result.description == "Alarmes na última 1 h: 4; mais alarmes: bomba-1"


def test_event_count_without_events() -> None:
    engine, _ = make_engine()
    result = engine.run("contagem_eventos", SOIL)
    assert result.value == 0.0 and result.source_id is None


def test_window_excludes_older_readings() -> None:
    engine, _ = make_engine()
    result = engine.run("media", QueryFilter(metric="umidade", source_id="umidade-3"), window_s=150)
    assert result.sample_count == 2 and result.value == pytest.approx(45.0)  # só 40 e 50 (1 e 2 min atrás)


def test_empty_window_returns_no_value_without_error() -> None:
    engine, _ = make_engine()
    result = engine.run("media", QueryFilter(metric="umidade", source_id="nao-existe"))
    assert result.empty and result.value is None and result.source_id is None
    assert result.description == "Média de umidade da fonte nao-existe na última 1 h: sem dados"
    assert engine.run("maior_variacao", QueryFilter(metric="co2")).value is None


def test_metric_is_inferred_when_unambiguous() -> None:
    engine, _ = make_engine()
    assert engine.run("media", QueryFilter(source_type="SENSOR_UMIDADE")).metric == "umidade"


def test_ambiguous_metric_is_an_error_listing_options() -> None:
    engine, _ = make_engine()
    with pytest.raises(QueryError, match="temperatura, umidade"):
        engine.run("media", QueryFilter(source_id="estacao-1"))


@pytest.mark.parametrize("window", [0, -10, math.inf, math.nan, 40 * 86400])
def test_invalid_window_is_rejected(window: float) -> None:
    engine, _ = make_engine()
    with pytest.raises(QueryError):
        engine.run("media", SOIL, window_s=window)


def test_unknown_kind_lists_the_options() -> None:
    engine, _ = make_engine()
    with pytest.raises(QueryError, match="media, desvio_padrao"):
        engine.run("mediana", SOIL)


def test_at_least_three_distinct_queries_exist() -> None:
    assert len(QueryKind) >= 3


def test_long_windows_use_the_persisted_history() -> None:
    class History:
        def __init__(self) -> None:
            self.calls = 0

        def readings(self, since_ms=None, until_ms=None, source_id=None, source_type=None):
            self.calls += 1
            return [make_stored("antigo", since_ms + 1, umidade=5.0)]

    history = History()
    engine, store = make_engine(history)
    engine.run("media", SOIL, window_s=3600)
    assert history.calls == 0  # janela curta: memória
    result = engine.run("media", QueryFilter(metric="umidade"), window_s=store.retention_s + 3600)
    assert history.calls == 1 and result.source_id is None and result.value == 5.0


def test_descriptions_are_ready_for_the_client() -> None:
    engine, _ = make_engine()
    assert engine.run("media", SOIL).description == (
        "Média de umidade das fontes SENSOR_UMIDADE na última 1 h: 32.60 % (10 leituras de 3 fontes)")
    assert engine.run("maximo", SOIL, 86400).description == (
        "Máximo de umidade das fontes SENSOR_UMIDADE nas últimas 24 h: 60.00 % em umidade-3 (10 leituras de 3 fontes)")


@pytest.mark.parametrize("seconds, text", [(3600, "na última 1 h"), (86400, "nas últimas 24 h"),
                                           (900, "nos últimos 15 min"), (60, "no último 1 min"), (90, "nos últimos 90 s")])
def test_describe_window(seconds: float, text: str) -> None:
    assert describe_window(seconds) == text
