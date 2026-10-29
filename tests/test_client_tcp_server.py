import socket
import struct
import time

import pytest

from gateway.client_dispatcher import ClientRequestDispatcher
from gateway.framing import recv_msg, send_msg
from gateway.registry import SourceRegistry
from gateway.tcp_server import ClientTcpServer
from pb import client_pb2
from tests.builders import make_announcement


@pytest.fixture
def server():
    registry = SourceRegistry()
    registry.upsert(make_announcement("sensor"))
    server = ClientTcpServer(ClientRequestDispatcher(registry), host="127.0.0.1", port=0)
    server.start()
    yield server
    server.stop()


def connect(server: ClientTcpServer) -> socket.socket:
    sock = socket.create_connection(("127.0.0.1", server.port), timeout=5)
    return sock


def list_request(request_id: int) -> client_pb2.ClientRequest:
    return client_pb2.ClientRequest(request_id=request_id, list_sources=client_pb2.ListSourcesRequest())


def ask(sock: socket.socket, request: client_pb2.ClientRequest) -> client_pb2.GatewayResponse:
    send_msg(sock, request)
    return recv_msg(sock, client_pb2.GatewayResponse)


def test_client_lists_sources_over_tcp(server: ClientTcpServer) -> None:
    with connect(server) as sock:
        response = ask(sock, list_request(1))
    assert response.request_id == 1
    assert [s.source_id for s in response.list_sources.sources] == ["sensor"]


def test_pipelined_requests_get_ordered_responses(server: ClientTcpServer) -> None:
    with connect(server) as sock:
        for i in range(1, 4):
            send_msg(sock, list_request(i))
        assert [recv_msg(sock, client_pb2.GatewayResponse).request_id for _ in range(3)] == [1, 2, 3]


def test_two_clients_are_served_simultaneously(server: ClientTcpServer) -> None:
    with connect(server) as first, connect(server) as second:
        assert ask(first, list_request(1)).request_id == 1
        assert ask(second, list_request(2)).request_id == 2
        assert ask(first, list_request(3)).request_id == 3


def test_unsupported_module_is_reported_as_error_and_connection_stays_open(server: ClientTcpServer) -> None:
    query = client_pb2.ClientRequest(request_id=4, query=client_pb2.QueryRequest(query_type="media"))
    with connect(server) as sock:
        assert ask(sock, query).error.code == client_pb2.ERROR_UNSUPPORTED
        assert ask(sock, list_request(5)).request_id == 5


def test_corrupted_payload_gets_error_and_next_request_still_works(server: ClientTcpServer) -> None:
    garbage = b"\xff\xff\xff\xff\xff"
    with connect(server) as sock:
        sock.sendall(struct.pack(">I", len(garbage)) + garbage)
        assert recv_msg(sock, client_pb2.GatewayResponse).error.code == client_pb2.ERROR_INVALID_REQUEST
        assert ask(sock, list_request(6)).request_id == 6


def test_client_dropping_mid_frame_does_not_affect_others(server: ClientTcpServer) -> None:
    dropper = connect(server)
    dropper.sendall(struct.pack(">I", 1000) + b"parcial")
    dropper.close()
    time.sleep(0.2)
    with connect(server) as sock:
        assert ask(sock, list_request(7)).request_id == 7


def test_absurd_frame_length_closes_only_that_connection(server: ClientTcpServer) -> None:
    with connect(server) as bad, connect(server) as good:
        bad.sendall(struct.pack(">I", 0xFFFFFFFF))
        assert bad.recv(10) == b""  # o Gateway fechou a conexão
        assert ask(good, list_request(8)).request_id == 8


def test_stop_closes_open_connections() -> None:
    server = ClientTcpServer(ClientRequestDispatcher(SourceRegistry()), host="127.0.0.1", port=0)
    server.start()
    sock = connect(server)
    assert ask(sock, list_request(1)).request_id == 1
    server.stop()
    assert sock.recv(10) == b""
    sock.close()
