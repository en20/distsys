"""Despacho dos pedidos do Cliente para os módulos do Gateway (#15).

O dispatcher não conhece sockets: recebe um ClientRequest e devolve sempre um GatewayResponse
(erros viram ErrorResponse). Os módulos dos outros fluxos entram por injeção, como Protocols:

    registry.list() / registry.get(id)                        # Marcelo, #11
    commands.execute(source_id, command_request) -> SourceStatus   # Jon, #13
    queries.run(query_type, query_filter, window_s) -> QueryResult # Enzo, #14
    storage.history(source_id, start_ms, end_ms) -> leituras       # Enzo, #16 (extra)

Módulo ausente (None) vira ERROR_UNSUPPORTED, então o Gateway já responde à listagem
enquanto os outros fluxos não ficam prontos. Exceções dos módulos são traduzidas assim:
    LookupError → NOT_FOUND            ValueError → INVALID_REQUEST
    NotImplementedError → UNSUPPORTED  ConnectionError/TimeoutError/OSError → SOURCE_UNAVAILABLE
    qualquer outra → INTERNAL (logada com traceback, o cliente só vê a mensagem genérica)

Ex.: ClientRequestDispatcher(registry, commands=executor).handle(request)
"""

import logging
import math
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Protocol

from common.runtime import Clock, epoch_ms
from gateway.registry import SourceRecord, SourceRegistry, SourceState
from pb import client_pb2, sources_pb2

log = logging.getLogger(__name__)

_STATE_TO_PROTO = {
    SourceState.ONLINE: client_pb2.SOURCE_ONLINE,
    SourceState.OFFLINE: client_pb2.SOURCE_OFFLINE,
    SourceState.POWERED_OFF: client_pb2.SOURCE_POWERED_OFF,
}


class ClientError(Exception):
    """Erro com código do contrato; o dispatcher o converte em ErrorResponse."""

    def __init__(self, code: client_pb2.ErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


# ---------------------------------------------------------------------------
# Interfaces dos módulos injetados
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class QueryFilter:
    source_id: str = ""      # vazio = qualquer fonte
    source_type: str = ""    # vazio = qualquer tipo
    metric: str = ""         # vazio = todas as grandezas


@dataclass(frozen=True)
class SourceAggregate:
    source_id: str
    source_type: str
    metric: str
    value: float
    samples: int


@dataclass
class QueryResult:
    value: float
    samples: int
    details: list[SourceAggregate] = field(default_factory=list)


class CommandExecutor(Protocol):
    def execute(self, source_id: str, command: client_pb2.CommandRequest) -> sources_pb2.SourceStatus:
        """Aplica o comando via gRPC e devolve o estado atualizado da fonte."""
        ...


class QueryEngine(Protocol):
    def run(self, query_type: str, query_filter: QueryFilter, window_s: float) -> QueryResult:
        """`window_s` = 0 significa todo o histórico."""
        ...


class HistoryStore(Protocol):
    def history(self, source_id: str, start_ms: int, end_ms: int) -> Iterable[sources_pb2.SensorReading]:
        """`start_ms`/`end_ms` = 0 significa sem limite naquele lado."""
        ...


# ---------------------------------------------------------------------------
# Dispatcher
# ---------------------------------------------------------------------------


class ClientRequestDispatcher:
    def __init__(
        self,
        registry: SourceRegistry,
        commands: CommandExecutor | None = None,
        queries: QueryEngine | None = None,
        storage: HistoryStore | None = None,
        clock: Clock | None = None,
    ) -> None:
        self._registry = registry
        self._commands = commands
        self._queries = queries
        self._storage = storage
        self._now_ms = (lambda: epoch_ms(clock)) if clock else epoch_ms

    def handle(self, request: client_pb2.ClientRequest) -> client_pb2.GatewayResponse:
        """Nunca levanta: o que der errado vira ErrorResponse com o mesmo request_id."""
        response = client_pb2.GatewayResponse(request_id=request.request_id)
        try:
            kind = request.WhichOneof("request")
            if kind == "list_sources":
                response.list_sources.CopyFrom(self._list_sources())
            elif kind == "command":
                response.command.CopyFrom(self._command(request.command))
            elif kind == "query":
                response.query.CopyFrom(self._query(request.query))
            elif kind == "history":
                response.history.CopyFrom(self._history(request.history))
            else:
                raise ClientError(client_pb2.ERROR_INVALID_REQUEST, "pedido vazio")
        except Exception as error:
            response.Clear()
            response.request_id = request.request_id
            response.error.CopyFrom(_to_error_response(error))
        return response

    def _list_sources(self) -> client_pb2.ListSourcesResponse:
        return client_pb2.ListSourcesResponse(sources=[source_info(r) for r in self._registry.list()])

    def _command(self, request: client_pb2.CommandRequest) -> client_pb2.CommandResponse:
        if self._commands is None:
            raise ClientError(client_pb2.ERROR_UNSUPPORTED, "módulo de comandos indisponível no Gateway")
        record = self._require_controllable(request.source_id)
        kind = _validate_command(request)
        status = self._commands.execute(request.source_id, request)
        # Sobrepõe o estado devolvido pela fonte: não depende de quem chamou ter atualizado o registro.
        updated = self._registry.get(request.source_id) or record
        return client_pb2.CommandResponse(
            success=True,
            message=f"comando '{kind}' aplicado na fonte {request.source_id}",
            source=source_info(updated, status=status),
        )

    def _query(self, request: client_pb2.QueryRequest) -> client_pb2.QueryResponse:
        if self._queries is None:
            raise ClientError(client_pb2.ERROR_UNSUPPORTED, "módulo de consultas indisponível no Gateway")
        if not request.query_type:
            raise ClientError(client_pb2.ERROR_INVALID_REQUEST, "query_type é obrigatório")
        if request.source_id and self._registry.get(request.source_id) is None:
            raise ClientError(client_pb2.ERROR_NOT_FOUND, f"fonte '{request.source_id}' não encontrada")
        query_filter = QueryFilter(request.source_id, request.source_type, request.metric)
        result = self._queries.run(request.query_type, query_filter, float(request.window_s))
        return client_pb2.QueryResponse(
            query_type=request.query_type,
            value=result.value,
            samples=result.samples,
            details=[
                client_pb2.SourceResult(
                    source_id=d.source_id, type=d.source_type, metric=d.metric, value=d.value, samples=d.samples
                )
                for d in result.details
            ],
        )

    def _history(self, request: client_pb2.HistoryRequest) -> client_pb2.HistoryResponse:
        if self._storage is None:
            raise ClientError(client_pb2.ERROR_UNSUPPORTED, "histórico indisponível: persistência não ativada")
        if not request.source_id:
            raise ClientError(client_pb2.ERROR_INVALID_REQUEST, "source_id é obrigatório")
        if self._registry.get(request.source_id) is None:
            raise ClientError(client_pb2.ERROR_NOT_FOUND, f"fonte '{request.source_id}' não encontrada")
        if request.from_ms and request.to_ms and request.from_ms > request.to_ms:
            raise ClientError(client_pb2.ERROR_INVALID_REQUEST, "from_ms maior que to_ms")
        readings = self._storage.history(request.source_id, request.from_ms, request.to_ms)
        points = [_history_point(r, request.metric) for r in readings]
        return client_pb2.HistoryResponse(source_id=request.source_id, points=[p for p in points if p.measurements])

    def _require_controllable(self, source_id: str) -> SourceRecord:
        if not source_id:
            raise ClientError(client_pb2.ERROR_INVALID_REQUEST, "source_id é obrigatório")
        record = self._registry.get(source_id)
        if record is None:
            raise ClientError(client_pb2.ERROR_NOT_FOUND, f"fonte '{source_id}' não encontrada")
        if not record.controllable:
            raise ClientError(client_pb2.ERROR_NOT_CONTROLLABLE, f"fonte '{source_id}' não aceita comandos")
        return record


# ---------------------------------------------------------------------------
# Conversões e validação
# ---------------------------------------------------------------------------


def source_info(record: SourceRecord, status: sources_pb2.SourceStatus | None = None) -> client_pb2.SourceInfo:
    """SourceRecord → SourceInfo; `status` (ex.: resposta de um comando) substitui o do registro."""
    status = status if status is not None else record.status
    info = client_pb2.SourceInfo(
        source_id=record.source_id,
        type=record.source_type,
        ip=record.ip,
        grpc_port=record.grpc_port,
        controllable=record.controllable,
        state=_STATE_TO_PROTO[record.state],
        last_seen_ms=int(record.last_seen * 1000),
        powered_on=status.powered_on,
        send_interval_s=status.send_interval_s,
        parameters=dict(status.parameters),
        failure=status.failure,  # os números dos dois enums FailureMode coincidem
        failure_until_ms=status.failure_until_ms,
    )
    if status.HasField("threshold"):
        info.threshold = status.threshold
    if record.last_reading is not None:
        info.last_values.extend(_measurement(m) for m in record.last_reading.measurements)
        info.last_reading_ms = record.last_reading.timestamp_ms
        info.last_reading_is_event = record.last_reading.is_event
    return info


def _measurement(measurement: sources_pb2.Measurement) -> client_pb2.Measurement:
    return client_pb2.Measurement(metric=measurement.metric, value=measurement.value, unit=measurement.unit)


def _history_point(reading: sources_pb2.SensorReading, metric: str) -> client_pb2.HistoryPoint:
    return client_pb2.HistoryPoint(
        timestamp_ms=reading.timestamp_ms,
        measurements=[_measurement(m) for m in reading.measurements if not metric or m.metric == metric],
        is_event=reading.is_event,
    )


def _validate_command(request: client_pb2.CommandRequest) -> str:
    """Confere o oneof e os valores; devolve o nome do comando."""
    kind = request.WhichOneof("command")
    if kind is None:
        raise ClientError(client_pb2.ERROR_INVALID_REQUEST, "nenhum comando informado")
    if kind == "interval" and not (math.isfinite(request.interval.interval_s) and request.interval.interval_s > 0):
        raise ClientError(client_pb2.ERROR_INVALID_REQUEST, "o intervalo de envio deve ser maior que zero")
    if kind == "threshold" and not math.isfinite(request.threshold.threshold):
        raise ClientError(client_pb2.ERROR_INVALID_REQUEST, "limiar inválido")
    if kind == "parameter":
        if not request.parameter.name:
            raise ClientError(client_pb2.ERROR_INVALID_REQUEST, "nome do parâmetro é obrigatório")
        if not math.isfinite(request.parameter.value):
            raise ClientError(client_pb2.ERROR_INVALID_REQUEST, "valor do parâmetro inválido")
    return kind


def _to_error_response(error: Exception) -> client_pb2.ErrorResponse:
    if isinstance(error, ClientError):
        return client_pb2.ErrorResponse(code=error.code, message=error.message)
    if isinstance(error, LookupError):
        return client_pb2.ErrorResponse(code=client_pb2.ERROR_NOT_FOUND, message=str(error))
    if isinstance(error, ValueError):
        return client_pb2.ErrorResponse(code=client_pb2.ERROR_INVALID_REQUEST, message=str(error))
    if isinstance(error, NotImplementedError):
        return client_pb2.ErrorResponse(code=client_pb2.ERROR_UNSUPPORTED, message=str(error) or "não suportado")
    if isinstance(error, OSError):  # inclui ConnectionError e TimeoutError
        return client_pb2.ErrorResponse(code=client_pb2.ERROR_SOURCE_UNAVAILABLE, message=str(error) or "fonte indisponível")
    log.exception("Erro inesperado ao tratar pedido do cliente", exc_info=error)
    return client_pb2.ErrorResponse(code=client_pb2.ERROR_INTERNAL, message="erro interno no Gateway")
