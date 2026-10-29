import socket
import struct
import threading

import pytest

from gateway.framing import MAX_FRAME_BYTES, FramingError, InvalidPayloadError, recv_msg, send_msg
from pb import client_pb2


@pytest.fixture
def pair():
    a, b = socket.socketpair()
    yield a, b
    a.close()
    b.close()


def make_request(request_id: int = 1, source_id: str = "s1") -> client_pb2.ClientRequest:
    return client_pb2.ClientRequest(request_id=request_id, command=client_pb2.CommandRequest(source_id=source_id))


def test_roundtrip(pair) -> None:
    a, b = pair
    send_msg(a, make_request(7, "valvula-1"))
    received = recv_msg(b, client_pb2.ClientRequest)
    assert received.request_id == 7
    assert received.command.source_id == "valvula-1"


def test_header_is_four_bytes_big_endian(pair) -> None:
    a, b = pair
    message = make_request()
    send_msg(a, message)
    raw = b.recv(1024)
    assert struct.unpack(">I", raw[:4])[0] == len(raw) - 4 == message.ByteSize()


def test_several_messages_in_a_row_keep_their_boundaries(pair) -> None:
    a, b = pair
    for i in range(1, 6):
        send_msg(a, make_request(i))
    assert [recv_msg(b, client_pb2.ClientRequest).request_id for _ in range(5)] == [1, 2, 3, 4, 5]


def test_large_message_is_read_completely(pair) -> None:
    a, b = pair
    big = make_request(source_id="x" * 3_000_000)
    sender = threading.Thread(target=send_msg, args=(a, big))  # bloqueia até o outro lado ler
    sender.start()
    received = recv_msg(b, client_pb2.ClientRequest)
    sender.join()
    assert len(received.command.source_id) == 3_000_000


def test_message_arriving_byte_by_byte(pair) -> None:
    a, b = pair
    message = make_request(9)
    wire = struct.pack(">I", message.ByteSize()) + message.SerializeToString()

    def trickle() -> None:
        for i in range(len(wire)):
            a.sendall(wire[i : i + 1])

    sender = threading.Thread(target=trickle)
    sender.start()
    assert recv_msg(b, client_pb2.ClientRequest).request_id == 9
    sender.join()


def test_empty_message_is_valid(pair) -> None:
    a, b = pair
    send_msg(a, client_pb2.ClientRequest())
    assert recv_msg(b, client_pb2.ClientRequest).WhichOneof("request") is None


def test_clean_close_between_messages_returns_none(pair) -> None:
    a, b = pair
    a.close()
    assert recv_msg(b, client_pb2.ClientRequest) is None


def test_close_in_the_middle_of_a_frame_raises(pair) -> None:
    a, b = pair
    a.sendall(struct.pack(">I", 100) + b"abc")
    a.close()
    with pytest.raises(FramingError):
        recv_msg(b, client_pb2.ClientRequest)


def test_close_in_the_middle_of_the_header_raises(pair) -> None:
    a, b = pair
    a.sendall(b"\x00\x00")
    a.close()
    with pytest.raises(FramingError):
        recv_msg(b, client_pb2.ClientRequest)


def test_oversized_length_is_rejected_before_reading_payload(pair) -> None:
    a, b = pair
    a.sendall(struct.pack(">I", MAX_FRAME_BYTES + 1))
    with pytest.raises(FramingError):
        recv_msg(b, client_pb2.ClientRequest)


def test_corrupted_protobuf_raises_invalid_payload_and_stream_stays_usable(pair) -> None:
    a, b = pair
    garbage = b"\xff\xff\xff\xff\xff"
    a.sendall(struct.pack(">I", len(garbage)) + garbage)
    send_msg(a, make_request(3))
    with pytest.raises(InvalidPayloadError):
        recv_msg(b, client_pb2.ClientRequest)
    assert recv_msg(b, client_pb2.ClientRequest).request_id == 3
