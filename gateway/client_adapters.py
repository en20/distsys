"""Adapta os módulos do Gateway às interfaces que o dispatcher do Cliente espera (#15 ↔ #13, #14, #16).

O dispatcher (LG) fala o contrato do Cliente (`client_pb2`, strings vazias = sem filtro, 0 = sem limite);
os módulos internos usam os próprios tipos. A conversão fica aqui para nenhum dos lados depender do outro:

    ClientCommands(CommandService)  client_pb2.CommandRequest → Command; CommandError → ClientError
    ClientQueries(QueryEngine)      filtros ""→None, janela 0 → MAX_WINDOW_S; QueryResult → o do dispatcher
    ClientHistory(SqliteStorage)    StoredReading → sources_pb2.SensorReading; 0 → sem limite

Ex.: ClientRequestDispatcher(registry, commands=ClientCommands(service), queries=ClientQueries(engine))
"""

from gateway import queries as engine_queries
from gateway.client_dispatcher import ClientError, QueryFilter, QueryResult, SourceAggregate
from gateway.commands import Command, CommandError, CommandErrorKind, CommandService
from gateway.readings import StoredReading
from gateway.storage import SqliteStorage
from pb import client_pb2, sources_pb2

_COMMAND_ERRORS = {
    CommandErrorKind.UNKNOWN_SOURCE: client_pb2.ERROR_NOT_FOUND,
    CommandErrorKind.NOT_CONTROLLABLE: client_pb2.ERROR_NOT_CONTROLLABLE,
    CommandErrorKind.REJECTED: client_pb2.ERROR_INVALID_REQUEST,
    CommandErrorKind.UNAVAILABLE: client_pb2.ERROR_SOURCE_UNAVAILABLE,
}

# Valor de cada fonte nos detalhes da resposta, conforme a consulta.
_PER_SOURCE_VALUE = {
    engine_queries.QueryKind.AVERAGE: "mean",
    engine_queries.QueryKind.LOWEST_MEAN: "mean",
    engine_queries.QueryKind.HIGHEST_MEAN: "mean",
    engine_queries.QueryKind.STDDEV: "stddev",
    engine_queries.QueryKind.MAX_VARIATION: "stddev",
    engine_queries.QueryKind.MIN: "minimum",
    engine_queries.QueryKind.MAX: "maximum",
    engine_queries.QueryKind.SUM: "total",
    engine_queries.QueryKind.EVENT_COUNT: "events",
}


def to_command(request: client_pb2.CommandRequest) -> Command:
    kind = request.WhichOneof("command")
    if kind == "power":
        return Command.power(request.power.on)
    if kind == "interval":
        return Command.interval(request.interval.interval_s)
    if kind == "threshold":
        return Command.threshold(request.threshold.threshold)
    if kind == "parameter":
        return Command.parameter(request.parameter.name, request.parameter.value)
    if kind == "failure":
        # Os números dos enums FailureMode de client.proto e sources.proto coincidem.
        return Command.failure(request.failure.mode, request.failure.duration_s)
    raise ClientError(client_pb2.ERROR_INVALID_REQUEST, "nenhum comando informado")


class ClientCommands:
    def __init__(self, service: CommandService) -> None:
        self._service = service

    def execute(self, source_id: str, command: client_pb2.CommandRequest) -> sources_pb2.SourceStatus:
        try:
            return self._service.execute(source_id, to_command(command))
        except CommandError as error:
            raise ClientError(_COMMAND_ERRORS[error.kind], error.message) from error


class ClientQueries:
    def __init__(self, engine: engine_queries.QueryEngine) -> None:
        self._engine = engine

    def run(self, query_type: str, query_filter: QueryFilter, window_s: float) -> QueryResult:
        engine_filter = engine_queries.QueryFilter(
            metric=query_filter.metric or None,
            source_id=query_filter.source_id or None,
            source_type=query_filter.source_type or None,
        )
        result = self._engine.run(query_type, engine_filter, window_s or engine_queries.MAX_WINDOW_S)
        field = _PER_SOURCE_VALUE[result.kind]
        details = [
            SourceAggregate(s.source_id, s.source_type, result.metric or "", float(getattr(s, field) or 0.0), s.count)
            for s in result.per_source
        ]
        value = result.value if result.value is not None else 0.0  # sem dados: samples = 0 indica o vazio
        return QueryResult(value=value, samples=result.sample_count, details=details)


class ClientHistory:
    def __init__(self, storage: SqliteStorage) -> None:
        self._storage = storage

    def history(self, source_id: str, start_ms: int, end_ms: int) -> list[sources_pb2.SensorReading]:
        rows = self._storage.history(source_id, start_ms or None, end_ms or None)
        return [_to_proto(r) for r in rows]


def _to_proto(reading: StoredReading) -> sources_pb2.SensorReading:
    # received_ms (relógio do Gateway) é o mesmo eixo usado no filtro from_ms/to_ms.
    proto = sources_pb2.SensorReading(source_id=reading.source_id, type=reading.source_type,
                                      timestamp_ms=reading.received_ms, is_event=reading.is_event)
    proto.measurements.extend(sources_pb2.Measurement(metric=m.metric, value=m.value, unit=m.unit)
                              for m in reading.measurements)
    return proto
