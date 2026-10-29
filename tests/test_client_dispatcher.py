import pytest

from gateway.client_dispatcher import ClientError, ClientRequestDispatcher, QueryFilter, QueryResult, SourceAggregate
from gateway.registry import SourceRegistry
from pb import client_pb2, sources_pb2
from tests.builders import make_announcement, make_reading


class FakeCommands:
    def __init__(self, error: Exception | None = None) -> None:
        self.calls: list[tuple[str, client_pb2.CommandRequest]] = []
        self.error = error

    def execute(self, source_id: str, command: client_pb2.CommandRequest) -> sources_pb2.SourceStatus:
        self.calls.append((source_id, command))
        if self.error:
            raise self.error
        return sources_pb2.SourceStatus(source_id=source_id, powered_on=False, send_interval_s=9.0, threshold=3.5)


class FakeQueries:
    def __init__(self) -> None:
        self.calls: list[tuple[str, QueryFilter, float]] = []

    def run(self, query_type: str, query_filter: QueryFilter, window_s: float) -> QueryResult:
        self.calls.append((query_type, query_filter, window_s))
        if query_type == "desconhecida":
            raise ValueError("consulta desconhecida: desconhecida")
        return QueryResult(21.5, 4, [SourceAggregate("s1", "SENSOR_TESTE", "umidade", 21.5, 4)])


class FakeStorage:
    def history(self, source_id: str, start_ms: int, end_ms: int):
        yield make_reading(source_id, 1.0)
        yield make_reading(source_id, 2.0, is_event=True)


@pytest.fixture
def registry() -> SourceRegistry:
    registry = SourceRegistry()
    registry.upsert(make_announcement("sensor", interval_s=2.0))
    registry.upsert(make_announcement("valvula", controllable=True))
    return registry


def command_request(source_id: str, **command) -> client_pb2.ClientRequest:
    return client_pb2.ClientRequest(request_id=5, command=client_pb2.CommandRequest(source_id=source_id, **command))


def error_code(response: client_pb2.GatewayResponse) -> int:
    assert response.WhichOneof("response") == "error", response
    return response.error.code


def test_list_sources_returns_registry_content(registry: SourceRegistry) -> None:
    registry.touch("sensor", make_reading("sensor", 55.0))
    response = ClientRequestDispatcher(registry).handle(
        client_pb2.ClientRequest(request_id=11, list_sources=client_pb2.ListSourcesRequest())
    )
    assert response.request_id == 11
    sources = {s.source_id: s for s in response.list_sources.sources}
    assert set(sources) == {"sensor", "valvula"}
    assert sources["sensor"].state == client_pb2.SOURCE_ONLINE
    assert sources["sensor"].last_values[0].value == 55.0
    assert sources["valvula"].controllable and sources["valvula"].grpc_port == 50051
    assert not sources["sensor"].HasField("threshold")


def test_empty_request_is_invalid(registry: SourceRegistry) -> None:
    assert error_code(ClientRequestDispatcher(registry).handle(client_pb2.ClientRequest(request_id=1))) == client_pb2.ERROR_INVALID_REQUEST


def test_command_is_forwarded_and_response_carries_updated_status(registry: SourceRegistry) -> None:
    commands = FakeCommands()
    request = command_request("valvula", power=client_pb2.PowerCommand(on=False))
    response = ClientRequestDispatcher(registry, commands=commands).handle(request)
    assert response.request_id == 5
    assert commands.calls[0][0] == "valvula"
    assert commands.calls[0][1].power.on is False
    assert response.command.success
    source = response.command.source
    assert (source.powered_on, source.send_interval_s, source.threshold) == (False, 9.0, 3.5)


@pytest.mark.parametrize(
    ("source_id", "command", "expected"),
    [
        ("fantasma", {"power": client_pb2.PowerCommand(on=True)}, client_pb2.ERROR_NOT_FOUND),
        ("sensor", {"power": client_pb2.PowerCommand(on=True)}, client_pb2.ERROR_NOT_CONTROLLABLE),
        ("", {"power": client_pb2.PowerCommand(on=True)}, client_pb2.ERROR_INVALID_REQUEST),
        ("valvula", {}, client_pb2.ERROR_INVALID_REQUEST),
        ("valvula", {"interval": client_pb2.IntervalCommand(interval_s=0)}, client_pb2.ERROR_INVALID_REQUEST),
        ("valvula", {"interval": client_pb2.IntervalCommand(interval_s=float("nan"))}, client_pb2.ERROR_INVALID_REQUEST),
        ("valvula", {"threshold": client_pb2.ThresholdCommand(threshold=float("inf"))}, client_pb2.ERROR_INVALID_REQUEST),
        ("valvula", {"parameter": client_pb2.ParameterCommand(name="", value=1)}, client_pb2.ERROR_INVALID_REQUEST),
    ],
)
def test_invalid_commands_are_rejected_without_reaching_the_executor(registry, source_id, command, expected) -> None:
    commands = FakeCommands()
    response = ClientRequestDispatcher(registry, commands=commands).handle(command_request(source_id, **command))
    assert error_code(response) == expected
    assert commands.calls == []


def test_command_without_executor_is_unsupported(registry: SourceRegistry) -> None:
    response = ClientRequestDispatcher(registry).handle(command_request("valvula", power=client_pb2.PowerCommand(on=True)))
    assert error_code(response) == client_pb2.ERROR_UNSUPPORTED


@pytest.mark.parametrize(
    ("raised", "expected"),
    [
        (ConnectionError("gRPC UNAVAILABLE"), client_pb2.ERROR_SOURCE_UNAVAILABLE),
        (TimeoutError("sem resposta"), client_pb2.ERROR_SOURCE_UNAVAILABLE),
        (KeyError("valvula"), client_pb2.ERROR_NOT_FOUND),
        (ValueError("fora da faixa"), client_pb2.ERROR_INVALID_REQUEST),
        (NotImplementedError("sem suporte"), client_pb2.ERROR_UNSUPPORTED),
        (ClientError(client_pb2.ERROR_SOURCE_UNAVAILABLE, "offline"), client_pb2.ERROR_SOURCE_UNAVAILABLE),
        (RuntimeError("bug"), client_pb2.ERROR_INTERNAL),
    ],
)
def test_executor_exceptions_become_structured_errors(registry, raised, expected) -> None:
    dispatcher = ClientRequestDispatcher(registry, commands=FakeCommands(error=raised))
    response = dispatcher.handle(command_request("valvula", power=client_pb2.PowerCommand(on=True)))
    assert error_code(response) == expected
    assert response.request_id == 5


def test_internal_error_does_not_leak_exception_text(registry: SourceRegistry) -> None:
    dispatcher = ClientRequestDispatcher(registry, commands=FakeCommands(error=RuntimeError("senha=123")))
    response = dispatcher.handle(command_request("valvula", power=client_pb2.PowerCommand(on=True)))
    assert "senha" not in response.error.message


def test_query_is_forwarded_with_filter_and_window(registry: SourceRegistry) -> None:
    queries = FakeQueries()
    request = client_pb2.ClientRequest(
        request_id=2,
        query=client_pb2.QueryRequest(query_type="media", source_type="SENSOR_TESTE", metric="umidade", window_s=60),
    )
    response = ClientRequestDispatcher(registry, queries=queries).handle(request)
    assert queries.calls == [("media", QueryFilter("", "SENSOR_TESTE", "umidade"), 60.0)]
    assert response.query.value == 21.5 and response.query.samples == 4
    assert response.query.details[0].source_id == "s1"


def test_query_validation_and_missing_module(registry: SourceRegistry) -> None:
    dispatcher = ClientRequestDispatcher(registry, queries=FakeQueries())
    empty = client_pb2.ClientRequest(query=client_pb2.QueryRequest())
    unknown_source = client_pb2.ClientRequest(query=client_pb2.QueryRequest(query_type="media", source_id="fantasma"))
    unknown_query = client_pb2.ClientRequest(query=client_pb2.QueryRequest(query_type="desconhecida"))
    assert error_code(dispatcher.handle(empty)) == client_pb2.ERROR_INVALID_REQUEST
    assert error_code(dispatcher.handle(unknown_source)) == client_pb2.ERROR_NOT_FOUND
    assert error_code(dispatcher.handle(unknown_query)) == client_pb2.ERROR_INVALID_REQUEST
    no_module = ClientRequestDispatcher(registry).handle(client_pb2.ClientRequest(query=client_pb2.QueryRequest(query_type="media")))
    assert error_code(no_module) == client_pb2.ERROR_UNSUPPORTED


def test_history_converts_readings_and_filters_metric(registry: SourceRegistry) -> None:
    dispatcher = ClientRequestDispatcher(registry, storage=FakeStorage())
    response = dispatcher.handle(client_pb2.ClientRequest(history=client_pb2.HistoryRequest(source_id="sensor")))
    points = response.history.points
    assert [p.measurements[0].value for p in points] == [1.0, 2.0]
    assert [p.is_event for p in points] == [False, True]
    other_metric = dispatcher.handle(
        client_pb2.ClientRequest(history=client_pb2.HistoryRequest(source_id="sensor", metric="temperatura"))
    )
    assert len(other_metric.history.points) == 0


def test_history_errors(registry: SourceRegistry) -> None:
    dispatcher = ClientRequestDispatcher(registry, storage=FakeStorage())
    unknown = client_pb2.ClientRequest(history=client_pb2.HistoryRequest(source_id="fantasma"))
    backwards = client_pb2.ClientRequest(history=client_pb2.HistoryRequest(source_id="sensor", from_ms=10, to_ms=5))
    assert error_code(dispatcher.handle(unknown)) == client_pb2.ERROR_NOT_FOUND
    assert error_code(dispatcher.handle(backwards)) == client_pb2.ERROR_INVALID_REQUEST
    no_storage = ClientRequestDispatcher(registry).handle(
        client_pb2.ClientRequest(history=client_pb2.HistoryRequest(source_id="sensor"))
    )
    assert error_code(no_storage) == client_pb2.ERROR_UNSUPPORTED
