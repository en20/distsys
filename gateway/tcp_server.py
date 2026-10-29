"""Servidor TCP do Gateway para os Clientes (#15): vários clientes simultâneos, uma thread por conexão.

Cada conexão lê ClientRequest (framing de gateway/framing.py), delega ao dispatcher e escreve o
GatewayResponse. Um cliente que cai ou manda lixo só encerra a própria conexão.

Ex.: server = ClientTcpServer(dispatcher, port=7000); server.start() ... server.stop()
"""

import logging
import socket
import threading

from common.loops import BackgroundLoop
from common.net import Address
from gateway.client_dispatcher import ClientRequestDispatcher
from gateway.framing import FramingError, InvalidPayloadError, recv_msg, send_msg
from pb import client_pb2

log = logging.getLogger(__name__)

DEFAULT_CLIENT_TCP_PORT = 7000
ACCEPT_TIMEOUT_S = 0.5


class ClientTcpServer:
    def __init__(self, dispatcher: ClientRequestDispatcher, host: str = "0.0.0.0", port: int = DEFAULT_CLIENT_TCP_PORT) -> None:
        self._dispatcher = dispatcher
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind((host, port))
        self._listener.listen()
        self._listener.settimeout(ACCEPT_TIMEOUT_S)
        self._accept_loop = BackgroundLoop("client-tcp-accept", self._accept_one)
        self._connections: set[socket.socket] = set()
        self._lock = threading.Lock()

    @property
    def port(self) -> int:
        """Porta efetiva (útil com port=0 nos testes)."""
        return self._listener.getsockname()[1]

    def start(self) -> None:
        log.info("Servidor TCP do cliente escutando na porta %d", self.port)
        self._accept_loop.start()

    def stop(self) -> None:
        self._accept_loop.stop()
        self._listener.close()
        with self._lock:
            connections = list(self._connections)
        for conn in connections:
            _close_quietly(conn)  # destrava o recv das threads de conexão

    def _accept_one(self) -> None:
        conn, peer = self._listener.accept()  # TimeoutError é tratado pelo BackgroundLoop
        conn.settimeout(None)
        with self._lock:
            self._connections.add(conn)
        threading.Thread(target=self._serve, args=(conn, peer), name=f"client-{peer[0]}:{peer[1]}", daemon=True).start()

    def _serve(self, conn: socket.socket, peer: Address) -> None:
        log.info("Cliente conectado: %s:%d", *peer)
        try:
            while True:
                try:
                    request = recv_msg(conn, client_pb2.ClientRequest)
                except InvalidPayloadError as error:
                    log.warning("Pedido inválido de %s:%d: %s", *peer, error)
                    send_msg(conn, _invalid_payload_response(error))
                    continue
                if request is None:
                    break
                send_msg(conn, self._dispatcher.handle(request))
        except (FramingError, OSError) as error:
            if not self._accept_loop.stopping:
                log.warning("Conexão com %s:%d encerrada: %s", *peer, error)
        finally:
            with self._lock:
                self._connections.discard(conn)
            _close_quietly(conn)
            log.info("Cliente desconectado: %s:%d", *peer)


def _invalid_payload_response(error: InvalidPayloadError) -> client_pb2.GatewayResponse:
    return client_pb2.GatewayResponse(error=client_pb2.ErrorResponse(code=client_pb2.ERROR_INVALID_REQUEST, message=str(error)))


def _close_quietly(sock: socket.socket) -> None:
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    sock.close()
