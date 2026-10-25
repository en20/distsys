"""Consultas analíticas agregadas sobre as leituras recebidas (#14, requisito 5d).

Genéricas por grandeza (`metric`), então servem para qualquer domínio:

| Tipo               | Resultado principal                              | Exemplo no domínio                    |
|--------------------|--------------------------------------------------|---------------------------------------|
| `media`            | média da grandeza na janela                      | média de temperatura na última hora   |
| `desvio_padrao`    | desvio padrão (amostral) da grandeza             | desvio padrão de CO2 nas últimas 24 h |
| `minimo`/`maximo`  | menor/maior leitura e a fonte que a mediu        | pico de consumo nas últimas 24 h      |
| `soma`             | soma das leituras                                | consumo de água por talhão em 24 h    |
| `menor_media`      | fonte com a menor média (ranking por fonte)      | talhão mais seco                      |
| `maior_media`      | fonte com a maior média                          | rack mais quente                      |
| `maior_variacao`   | fonte com maior desvio padrão                    | fonte com maior variação nas leituras |
| `contagem_eventos` | alarmes (is_event) na janela, por fonte          | leito com mais alarmes na última hora |

Uso pelo servidor TCP (LG, #15):
    engine = QueryEngine(store, history=storage)
    result = engine.run("media", QueryFilter(metric="temperatura", source_type="SENSOR_TEMP"), window_s=3600)
    result.value, result.source_id, result.per_source, result.description
"""

import math
import statistics
import time
from collections import defaultdict
from dataclasses import dataclass, replace
from enum import Enum

from common.runtime import Clock, epoch_ms
from gateway.readings import MetricValue, ReadingSource, StoredReading

Values = list[tuple[StoredReading, MetricValue]]

MAX_WINDOW_S = 30 * 24 * 3600


class QueryError(ValueError):
    """Consulta inválida (tipo desconhecido, grandeza ambígua, janela inválida)."""


class QueryKind(str, Enum):
    AVERAGE = "media"
    STDDEV = "desvio_padrao"
    MIN = "minimo"
    MAX = "maximo"
    SUM = "soma"
    LOWEST_MEAN = "menor_media"
    HIGHEST_MEAN = "maior_media"
    MAX_VARIATION = "maior_variacao"
    EVENT_COUNT = "contagem_eventos"

    @classmethod
    def parse(cls, kind: "str | QueryKind") -> "QueryKind":
        try:
            return cls(kind)
        except ValueError:
            options = ", ".join(k.value for k in cls)
            raise QueryError(f"tipo de consulta desconhecido {kind!r}; disponíveis: {options}") from None


LABELS = {
    QueryKind.AVERAGE: "Média",
    QueryKind.STDDEV: "Desvio padrão",
    QueryKind.MIN: "Mínimo",
    QueryKind.MAX: "Máximo",
    QueryKind.SUM: "Soma",
    QueryKind.LOWEST_MEAN: "Fonte com menor média",
    QueryKind.HIGHEST_MEAN: "Fonte com maior média",
    QueryKind.MAX_VARIATION: "Fonte com maior variação",
    QueryKind.EVENT_COUNT: "Alarmes",
}


@dataclass(frozen=True)
class QueryFilter:
    """Quais leituras entram na consulta; campos None não filtram."""

    metric: str | None = None
    source_id: str | None = None
    source_type: str | None = None


@dataclass(frozen=True)
class SourceStats:
    source_id: str
    source_type: str
    count: int          # leituras com a grandeza (ou leituras no total, em contagem_eventos)
    events: int
    mean: float | None = None
    stddev: float | None = None
    minimum: float | None = None
    maximum: float | None = None
    total: float | None = None


@dataclass(frozen=True)
class QueryResult:
    kind: QueryKind
    metric: str | None
    unit: str
    window_s: float
    since_ms: int
    until_ms: int
    value: float | None              # resultado principal; None = sem dados na janela
    source_id: str | None            # fonte ligada ao resultado (mín/máx/rankings/eventos)
    sample_count: int
    per_source: tuple[SourceStats, ...]  # ordenado pelo critério da consulta
    description: str                 # frase pronta para o Cliente exibir

    @property
    def empty(self) -> bool:
        return self.sample_count == 0


class QueryEngine:
    """Executa as consultas sobre as leituras em memória (e no SQLite para janelas longas).

    Janelas maiores que a retenção da memória usam `history` (persistência #16), se houver.
    Ex.: QueryEngine(store).run("maior_variacao", QueryFilter(metric="umidade"), 24 * 3600)
    """

    def __init__(self, recent: ReadingSource, history: ReadingSource | None = None, clock: Clock = time.time) -> None:
        self._recent = recent
        self._history = history
        self._clock = clock

    def run(self, kind: "str | QueryKind", query_filter: QueryFilter | None = None, window_s: float = 3600) -> QueryResult:
        kind = QueryKind.parse(kind)
        query_filter = query_filter or QueryFilter()
        if not (math.isfinite(window_s) and 0 < window_s <= MAX_WINDOW_S):
            raise QueryError(f"janela deve estar entre 0 e {MAX_WINDOW_S // 86400} dias, recebido {window_s!r}s")

        until_ms = epoch_ms(self._clock)
        since_ms = until_ms - int(window_s * 1000)
        readings = self._source_for(window_s).readings(
            since_ms=since_ms, until_ms=until_ms,
            source_id=query_filter.source_id, source_type=query_filter.source_type,
        )
        if kind == QueryKind.EVENT_COUNT:
            return self._event_count(readings, query_filter, window_s, since_ms, until_ms)

        metric = _resolve_metric(readings, query_filter.metric)
        values = [(r, m) for r in readings if (m := r.value_of(metric)) is not None]
        per_source = _stats_by_source(values)
        unit = values[0][1].unit if values else ""
        value, source_id, ordered = _aggregate(kind, values, per_source)
        result = QueryResult(kind, metric, unit, window_s, since_ms, until_ms, value, source_id,
                             len(values), ordered, "")
        return _with_description(result, query_filter)

    def _source_for(self, window_s: float) -> ReadingSource:
        retention_s = getattr(self._recent, "retention_s", math.inf)
        if self._history is not None and window_s > retention_s:
            return self._history
        return self._recent

    def _event_count(self, readings: list[StoredReading], query_filter: QueryFilter, window_s: float,
                     since_ms: int, until_ms: int) -> QueryResult:
        if query_filter.metric:
            readings = [r for r in readings if r.value_of(query_filter.metric) is not None]
        by_source: dict[str, list[StoredReading]] = defaultdict(list)
        for reading in readings:
            by_source[reading.source_id].append(reading)
        stats = [
            SourceStats(source_id, items[0].source_type, len(items), sum(r.is_event for r in items))
            for source_id, items in by_source.items()
        ]
        ordered = tuple(sorted(stats, key=lambda s: (-s.events, s.source_id)))
        total = sum(s.events for s in stats)
        top = ordered[0].source_id if ordered and ordered[0].events else None
        result = QueryResult(QueryKind.EVENT_COUNT, query_filter.metric, "", window_s, since_ms, until_ms,
                             float(total), top, len(readings), ordered, "")
        return _with_description(result, query_filter)


def _resolve_metric(readings: list[StoredReading], metric: str | None) -> str:
    """Grandeza pedida; se omitida, só é aceita quando os dados filtrados têm uma única."""
    if metric:
        return metric
    available = sorted({m.metric for r in readings for m in r.measurements})
    if len(available) == 1:
        return available[0]
    if not available:
        raise QueryError("informe a grandeza (metric): não há leituras na janela para deduzi-la")
    raise QueryError(f"informe a grandeza (metric); disponíveis: {', '.join(available)}")


def _stats_by_source(values: Values) -> list[SourceStats]:
    grouped: dict[str, list[float]] = defaultdict(list)
    types: dict[str, str] = {}
    events: dict[str, int] = defaultdict(int)
    for reading, measurement in values:
        grouped[reading.source_id].append(measurement.value)
        types[reading.source_id] = reading.source_type
        events[reading.source_id] += reading.is_event
    return [
        SourceStats(
            source_id=source_id, source_type=types[source_id], count=len(series), events=events[source_id],
            mean=statistics.fmean(series), stddev=_stddev(series),
            minimum=min(series), maximum=max(series), total=math.fsum(series),
        )
        for source_id, series in grouped.items()
    ]


def _aggregate(
    kind: QueryKind, values: Values, per_source: list[SourceStats]
) -> tuple[float | None, str | None, tuple[SourceStats, ...]]:
    """(valor principal, fonte associada, ranking por fonte) para cada tipo de consulta."""
    if not values:
        return None, None, tuple(sorted(per_source, key=lambda s: s.source_id))
    numbers = [m.value for _, m in values]
    if kind == QueryKind.AVERAGE:
        return statistics.fmean(numbers), None, tuple(sorted(per_source, key=lambda s: (-s.mean, s.source_id)))
    if kind == QueryKind.STDDEV:
        return _stddev(numbers), None, tuple(sorted(per_source, key=lambda s: (-s.stddev, s.source_id)))
    if kind == QueryKind.SUM:
        return math.fsum(numbers), None, tuple(sorted(per_source, key=lambda s: (-s.total, s.source_id)))
    if kind == QueryKind.MIN:
        reading, measurement = min(values, key=lambda rm: (rm[1].value, rm[0].received_ms))
        return measurement.value, reading.source_id, tuple(sorted(per_source, key=lambda s: (s.minimum, s.source_id)))
    if kind == QueryKind.MAX:
        reading, measurement = max(values, key=lambda rm: (rm[1].value, -rm[0].received_ms))
        return measurement.value, reading.source_id, tuple(sorted(per_source, key=lambda s: (-s.maximum, s.source_id)))
    if kind == QueryKind.LOWEST_MEAN:
        ordered = tuple(sorted(per_source, key=lambda s: (s.mean, s.source_id)))
        return ordered[0].mean, ordered[0].source_id, ordered
    if kind == QueryKind.HIGHEST_MEAN:
        ordered = tuple(sorted(per_source, key=lambda s: (-s.mean, s.source_id)))
        return ordered[0].mean, ordered[0].source_id, ordered
    if kind == QueryKind.MAX_VARIATION:
        ordered = tuple(sorted(per_source, key=lambda s: (-s.stddev, -(s.maximum - s.minimum), s.source_id)))
        return ordered[0].stddev, ordered[0].source_id, ordered
    raise QueryError(f"tipo de consulta sem implementação: {kind.value}")  # pragma: no cover


def _stddev(series: list[float]) -> float:
    return statistics.stdev(series) if len(series) >= 2 else 0.0


def _with_description(result: QueryResult, query_filter: QueryFilter) -> QueryResult:
    return replace(result, description=describe(result, query_filter))


def describe(result: QueryResult, query_filter: QueryFilter | None = None) -> str:
    """Frase legível, ex.: 'Média de temperatura na última 1 h: 24.31 °C (120 leituras de 3 fontes)'."""
    query_filter = query_filter or QueryFilter()
    scope = ""
    if query_filter.source_id:
        scope = f" da fonte {query_filter.source_id}"
    elif query_filter.source_type:
        scope = f" das fontes {query_filter.source_type}"
    window = describe_window(result.window_s)
    label = LABELS[result.kind]
    if result.kind == QueryKind.EVENT_COUNT:
        if not result.per_source:
            return f"{label}{scope} {window}: nenhuma leitura"
        top = f"; mais alarmes: {result.source_id}" if result.source_id else ""
        return f"{label}{scope} {window}: {result.value:g}{top}"
    what = f"{label} de {result.metric}{scope} {window}"
    if result.empty:
        return f"{what}: sem dados"
    unit = f" {result.unit}" if result.unit else ""
    sources = len(result.per_source)
    detail = f"({result.sample_count} leituras de {sources} fonte{'s' if sources != 1 else ''})"
    origin = f" em {result.source_id}" if result.source_id else ""
    if result.kind in (QueryKind.LOWEST_MEAN, QueryKind.HIGHEST_MEAN):
        return f"{what}: {result.source_id} (média {result.value:.2f}{unit}) {detail}"
    if result.kind == QueryKind.MAX_VARIATION:
        return f"{what}: {result.source_id} (desvio padrão {result.value:.2f}{unit}) {detail}"
    return f"{what}: {result.value:.2f}{unit}{origin} {detail}"


def describe_window(window_s: float) -> str:
    """3600 → 'na última 1 h'; 86400 → 'nas últimas 24 h'; 900 → 'nos últimos 15 min'."""
    if window_s % 3600 == 0:
        hours = int(window_s // 3600)
        return "na última 1 h" if hours == 1 else f"nas últimas {hours} h"
    if window_s % 60 == 0:
        minutes = int(window_s // 60)
        return "no último 1 min" if minutes == 1 else f"nos últimos {minutes} min"
    return f"nos últimos {window_s:g} s"
