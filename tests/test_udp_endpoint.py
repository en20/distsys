import socket

from gateway.udp_endpoint import GatewayUdpEndpoint
from pb import sources_pb2
from tests.builders import make_announcement, make_reading

SENDER = ("10.0.0.7", 40000)


def make_endpoint() -> tuple[GatewayUdpEndpoint, list, list]:
    announcements, readings = [], []
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # não usado por dispatch()
    endpoint = GatewayUdpEndpoint(sock, lambda a, s: announcements.append(a), lambda r, s: readings.append(r))
    sock.close()
    return endpoint, announcements, readings


def test_dispatch_routes_by_oneof() -> None:
    endpoint, announcements, readings = make_endpoint()
    endpoint.dispatch(sources_pb2.SourceMessage(announcement=make_announcement("a")).SerializeToString(), SENDER)
    endpoint.dispatch(sources_pb2.SourceMessage(reading=make_reading("b")).SerializeToString(), SENDER)
    assert [a.source_id for a in announcements] == ["a"]
    assert [r.source_id for r in readings] == ["b"]


def test_garbage_and_empty_envelopes_are_dropped() -> None:
    endpoint, announcements, readings = make_endpoint()
    endpoint.dispatch(b"\xff\xff\xff", SENDER)
    endpoint.dispatch(b"", SENDER)
    assert announcements == [] and readings == []
