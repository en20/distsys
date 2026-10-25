"""Socket UDP único do Gateway: envia a descoberta e recebe anúncios + leituras.

Anúncios e leituras chegam na mesma porta (6000) dentro do envelope SourceMessage;
este módulo só decodifica e despacha pelo `oneof`. O que fazer com cada leitura
(armazenar, contar alarmes, persistir) é do receptor do Enzo (#12), plugado em `on_reading`.
"""

import logging
import socket
from collections.abc import Callable

from common.loops import BackgroundLoop
from common.net import RECV_BUFFER_BYTES, Address
from pb import sources_pb2

log = logging.getLogger(__name__)

AnnouncementCallback = Callable[[sources_pb2.SourceAnnouncement, Address], None]
ReadingCallback = Callable[[sources_pb2.SensorReading, Address], None]


class GatewayUdpEndpoint:
    """Loop de recepção que despacha SourceMessage para os callbacks.

    Ex.: GatewayUdpEndpoint(sock, handler, receiver.on_reading).start()
    """

    def __init__(self, sock: socket.socket, on_announcement: AnnouncementCallback, on_reading: ReadingCallback) -> None:
        self._sock = sock
        self._on_announcement = on_announcement
        self._on_reading = on_reading
        self._loop = BackgroundLoop("gateway-udp", self._receive_one)

    def start(self) -> None:
        self._loop.start()

    def stop(self) -> None:
        self._loop.stop()

    def dispatch(self, payload: bytes, sender: Address) -> None:
        message = _parse_source_message(payload, sender)
        if message is None:
            return
        kind = message.WhichOneof("payload")
        if kind == "announcement":
            self._on_announcement(message.announcement, sender)
        elif kind == "reading":
            self._on_reading(message.reading, sender)
        else:
            log.warning("SourceMessage vazio de %s:%d ignorado", *sender)

    def _receive_one(self) -> None:
        payload, sender = self._sock.recvfrom(RECV_BUFFER_BYTES)
        self.dispatch(payload, sender)


def _parse_source_message(payload: bytes, sender: Address) -> sources_pb2.SourceMessage | None:
    message = sources_pb2.SourceMessage()
    try:
        message.ParseFromString(payload)
    except Exception:
        log.warning("Pacote UDP inválido de %s:%d ignorado (%d bytes)", sender[0], sender[1], len(payload))
        return None
    return message
