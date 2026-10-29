from pb import sources_pb2
from sources.discovery import DiscoveryResponder, GatewayLocator
from tests.builders import make_announcement
from tests.fakes import FakeDatagramSocket


def make_responder() -> tuple[DiscoveryResponder, GatewayLocator, FakeDatagramSocket, list]:
    locator, sock, changes = GatewayLocator(), FakeDatagramSocket(), []
    responder = DiscoveryResponder(locator, sock, lambda _ip, rid: make_announcement(), changes.append)
    return responder, locator, sock, changes


def discovery_request(ip: str = "127.0.0.1", port: int = 6000, request_id: int = 7) -> bytes:
    return sources_pb2.DiscoveryRequest(gateway_ip=ip, gateway_udp_port=port, request_id=request_id).SerializeToString()


def test_request_stores_gateway_and_replies_with_announcement() -> None:
    responder, locator, sock, _ = make_responder()
    responder.handle_datagram(discovery_request())
    assert locator.get() == ("127.0.0.1", 6000)
    payload, destination = sock.sent[0]
    assert destination == ("127.0.0.1", 6000)
    assert sources_pb2.SourceMessage.FromString(payload).WhichOneof("payload") == "announcement"


def test_gateway_change_is_reported_once_per_new_address() -> None:
    responder, _, sock, changes = make_responder()
    responder.handle_datagram(discovery_request(request_id=1))
    responder.handle_datagram(discovery_request(request_id=2))
    responder.handle_datagram(discovery_request(port=6001, request_id=3))
    assert changes == [("127.0.0.1", 6000), ("127.0.0.1", 6001)]
    assert len(sock.sent) == 3  # responde a toda redescoberta


def test_garbage_and_incomplete_requests_are_ignored() -> None:
    responder, locator, sock, _ = make_responder()
    responder.handle_datagram(b"\xff\xff\xff")
    responder.handle_datagram(sources_pb2.DiscoveryRequest(gateway_ip="1.2.3.4").SerializeToString())
    assert locator.get() is None
    assert sock.sent == []
