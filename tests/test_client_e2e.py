"""Cliente → TCP → Gateway real → fontes do domínio (gRPC, consultas e histórico SQLite) na loopback."""

import socket
import time

import pytest

from common.net import configure_multicast_sender, open_udp_socket
from gateway.framing import recv_msg, send_msg
from gateway.main import Gateway
from gateway.readings import ReadingStore
from gateway.registry import SourceRegistry
from gateway.storage import SqliteStorage
from pb import client_pb2
from sources.reservoir import ReservoirLevelSensor
from sources.soil_moisture import SoilMoistureSensor
from sources.valve import IrrigationValve
from tests.conftest import LOOPBACK


def wait_until(condition, timeout_s: float = 8.0):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        value = condition()
        if value:
            return value
        time.sleep(0.05)
    pytest.fail(f"condição não atingida em {timeout_s}s")


@pytest.fixture
def full_gateway(mcast_port, tmp_path):
    sock = open_udp_socket(0)
    configure_multicast_sender(sock, ttl=1, interface_ip=LOOPBACK)
    gw = Gateway(sock, ("224.1.1.1", mcast_port), (LOOPBACK, sock.getsockname()[1]), rediscovery_s=0.3,
                 registry=SourceRegistry(min_timeout_s=0.5), store=ReadingStore(),
                 storage=SqliteStorage(tmp_path / "g.db"), client_port=0)
    gw.start()
    yield gw
    gw.stop()


@pytest.fixture
def ask(full_gateway):
    conn = socket.create_connection((LOOPBACK, full_gateway.client_server.port), timeout=5)
    next_id = iter(range(1, 1000))

    def request(**kwargs) -> client_pb2.GatewayResponse:
        req = client_pb2.ClientRequest(request_id=next(next_id), **kwargs)
        send_msg(conn, req)
        resp = recv_msg(conn, client_pb2.GatewayResponse)
        assert resp.request_id == req.request_id
        return resp

    yield request
    conn.close()


def command(source_id, **kwargs):
    return {"command": client_pb2.CommandRequest(source_id=source_id, **kwargs)}


def test_list_command_query_and_history_through_tcp(full_gateway, make_source, ask):
    make_source(IrrigationValve, source_id="valvula-1", interval=0.5)
    for i in range(2):
        make_source(SoilMoistureSensor, source_id=f"umidade-{i}", interval=0.5)
    wait_until(lambda: all(len(full_gateway.store.readings(source_id=s)) >= 2
                           for s in ("valvula-1", "umidade-0", "umidade-1")))

    listed = ask(list_sources=client_pb2.ListSourcesRequest()).list_sources.sources
    valve = next(s for s in listed if s.source_id == "valvula-1")
    assert {s.source_id for s in listed} == {"valvula-1", "umidade-0", "umidade-1"}
    assert valve.controllable and valve.state == client_pb2.SOURCE_ONLINE and valve.last_values

    resp = ask(**command("valvula-1", parameter=client_pb2.ParameterCommand(name="vazao_alvo_lpm", value=40)))
    assert resp.command.success and resp.command.source.parameters["vazao_alvo_lpm"] == 40

    resp = ask(**command("valvula-1", power=client_pb2.PowerCommand(on=False)))
    assert not resp.command.source.powered_on and resp.command.source.state == client_pb2.SOURCE_POWERED_OFF

    driest = ask(query=client_pb2.QueryRequest(query_type="menor_media", metric="umidade_solo", window_s=60)).query
    assert driest.samples >= 4 and {d.source_id for d in driest.details} == {"umidade-0", "umidade-1"}
    assert driest.value == min(d.value for d in driest.details)

    everything = ask(query=client_pb2.QueryRequest(query_type="media", metric="umidade_solo")).query  # 0 = tudo
    assert everything.samples >= driest.samples

    full_gateway.storage.flush()
    history = ask(history=client_pb2.HistoryRequest(source_id="umidade-0", metric="umidade_solo")).history
    assert len(history.points) >= 2
    assert all(p.measurements[0].metric == "umidade_solo" for p in history.points)


def test_threshold_event_is_counted_by_the_query(full_gateway, make_source, ask):
    make_source(ReservoirLevelSensor, source_id="reservatorio-1", interval=60)
    wait_until(lambda: full_gateway.registry.get("reservatorio-1"))
    ask(**command("reservatorio-1", threshold=client_pb2.ThresholdCommand(threshold=99)))  # nível fica abaixo

    def alarms():
        return ask(query=client_pb2.QueryRequest(query_type="contagem_eventos", window_s=60)).query.value
    wait_until(lambda: alarms() == 1)


def test_errors_are_translated_to_the_client_contract(full_gateway, make_source, ask):
    valve = make_source(IrrigationValve, source_id="valvula-1")
    make_source(SoilMoistureSensor, source_id="umidade-1")
    wait_until(lambda: full_gateway.registry.get("valvula-1") and full_gateway.registry.get("umidade-1"))

    def error_of(**kwargs):
        resp = ask(**kwargs)
        assert resp.WhichOneof("response") == "error"
        return resp.error.code

    assert error_of(**command("nao-existe", power=client_pb2.PowerCommand(on=True))) == client_pb2.ERROR_NOT_FOUND
    assert error_of(**command("umidade-1", power=client_pb2.PowerCommand(on=False))) == \
        client_pb2.ERROR_NOT_CONTROLLABLE
    # A válvula não tem limiar: a fonte responde UNIMPLEMENTED, o Gateway devolve pedido inválido.
    assert error_of(**command("valvula-1", threshold=client_pb2.ThresholdCommand(threshold=10))) == \
        client_pb2.ERROR_INVALID_REQUEST
    assert error_of(query=client_pb2.QueryRequest(query_type="inexistente")) == client_pb2.ERROR_INVALID_REQUEST

    valve.stop()  # como um Ctrl+C na fonte
    assert error_of(**command("valvula-1", power=client_pb2.PowerCommand(on=True))) == \
        client_pb2.ERROR_SOURCE_UNAVAILABLE
    listed = ask(list_sources=client_pb2.ListSourcesRequest()).list_sources.sources
    assert next(s for s in listed if s.source_id == "valvula-1").state == client_pb2.SOURCE_OFFLINE
