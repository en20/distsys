"""Framing das mensagens protobuf sobre TCP (#9): 4 bytes big-endian com o tamanho + payload.

TCP é um fluxo de bytes sem fronteiras; o prefixo de tamanho diz onde cada mensagem termina.
Contraparte em Node: client/framing.js.

    send_msg(sock, client_pb2.GatewayResponse(...))
    request = recv_msg(sock, client_pb2.ClientRequest)   # None quando o par fechou a conexão
"""

import socket
import struct
from typing import TypeVar

from google.protobuf.message import Message

MessageT = TypeVar("MessageT", bound=Message)

HEADER = struct.Struct(">I")
MAX_FRAME_BYTES = 16 * 1024 * 1024  # recusa tamanhos absurdos antes de alocar memória


class FramingError(Exception):
    """Conexão fechada no meio de uma mensagem ou tamanho inválido: o fluxo não é mais confiável."""


class InvalidPayloadError(FramingError):
    """Moldura íntegra com protobuf corrompido: dá para responder com erro e seguir lendo."""


def send_msg(sock: socket.socket, message: Message) -> None:
    """Envia `message` com o prefixo de tamanho (sendall: nada de escrita parcial)."""
    payload = message.SerializeToString()
    if len(payload) > MAX_FRAME_BYTES:
        raise FramingError(f"mensagem de {len(payload)} bytes excede o limite de {MAX_FRAME_BYTES}")
    sock.sendall(HEADER.pack(len(payload)) + payload)


def recv_msg(sock: socket.socket, message_type: type[MessageT]) -> MessageT | None:
    """Lê uma mensagem completa; None se o par fechou a conexão entre duas mensagens."""
    header = _recv_exact(sock, HEADER.size, eof_ok=True)
    if header is None:
        return None
    (length,) = HEADER.unpack(header)
    if length > MAX_FRAME_BYTES:
        raise FramingError(f"tamanho de mensagem inválido: {length} bytes")
    payload = _recv_exact(sock, length, eof_ok=False) if length else b""
    message = message_type()
    try:
        message.ParseFromString(payload)
    except Exception as error:
        raise InvalidPayloadError(f"protobuf inválido ({length} bytes): {error}") from error
    return message


def _recv_exact(sock: socket.socket, size: int, eof_ok: bool) -> bytes | None:
    """Loop até ler `size` bytes: um recv pode devolver menos do que o pedido."""
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = sock.recv(min(remaining, 65536))
        if not chunk:
            if eof_ok and remaining == size:
                return None
            raise FramingError(f"conexão fechada com {remaining} de {size} bytes por ler")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)
