from gateway.discovery import AnnouncementHandler, DiscoveryBroadcaster
from gateway.registry import SourceRegistry
from pb import sources_pb2
from tests.builders import make_announcement
from tests.fakes import FakeClock, FakeDatagramSocket

GROUP = ("224.1.1.1", 5007)


def test_broadcaster_sends_request_with_gateway_address_to_group() -> None:
    sock = FakeDatagramSocket()
    broadcaster = DiscoveryBroadcaster(sock, GROUP, ("192.168.0.5", 6000), clock=FakeClock(start=2.5))
    assert broadcaster.send_request() == 1
    assert broadcaster.send_request() == 2
    payload, destination = sock.sent[0]
    request = sources_pb2.DiscoveryRequest.FromString(payload)
    assert destination == GROUP
    assert (request.gateway_ip, request.gateway_udp_port, request.timestamp_ms) == ("192.168.0.5", 6000, 2500)


def test_handler_notifies_only_new_controllable_sources() -> None:
    discovered: list[str] = []
    handler = AnnouncementHandler(SourceRegistry(), discovered.append)
    handler(make_announcement("valvula", controllable=True), ("10.0.0.7", 1))
    handler(make_announcement("valvula", controllable=True), ("10.0.0.7", 1))
    handler(make_announcement("umidade"), ("10.0.0.8", 1))
    assert discovered == ["valvula"]


def test_handler_falls_back_to_sender_ip_and_rejects_missing_id() -> None:
    registry = SourceRegistry()
    handler = AnnouncementHandler(registry)
    handler(make_announcement("s1", ip=""), ("10.0.0.9", 1))
    handler(make_announcement(""), ("10.0.0.9", 1))
    assert [(r.source_id, r.ip) for r in registry.list()] == [("s1", "10.0.0.9")]
