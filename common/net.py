"""Helpers de socket compartilhados pelas fontes e pelo Gateway."""

import socket
import struct
from typing import Protocol

Address = tuple[str, int]

DEFAULT_MCAST_GROUP = "224.1.1.1"
DEFAULT_MCAST_PORT = 5007
DEFAULT_GATEWAY_UDP_PORT = 6000
RECV_BUFFER_BYTES = 65535


class DatagramSender(Protocol):
    """O que precisamos de um socket UDP para enviar (permite fakes nos testes)."""

    def sendto(self, data: bytes, address: Address, /) -> int: ...


def resolve_local_ip(target_ip: str) -> str:
    """IP local que o SO usaria para alcançar `target_ip`.

    UDP connect não envia nada; só escolhe a rota/interface.
    Ex.: resolve_local_ip("192.168.0.10") -> "192.168.0.42"
    """
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        try:
            probe.connect((target_ip, 9))
            return probe.getsockname()[0]
        except OSError:
            return "127.0.0.1"


def open_multicast_listener(group: str, port: int, interface_ip: str | None = None) -> socket.socket:
    """Socket UDP inscrito no grupo multicast; várias fontes podem dividir a porta.

    Ex.: sock = open_multicast_listener("224.1.1.1", 5007)
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if hasattr(socket, "SO_REUSEPORT"):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    sock.bind(("", port))
    membership = struct.pack("4s4s", socket.inet_aton(group), socket.inet_aton(interface_ip or "0.0.0.0"))
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membership)
    return sock


def configure_multicast_sender(sock: socket.socket, ttl: int, interface_ip: str | None = None) -> None:
    """Prepara `sock` para enviar ao grupo: TTL, loopback (fontes na mesma máquina) e interface."""
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
    if interface_ip:
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(interface_ip))


def open_udp_socket(bind_port: int = 0, recv_timeout_s: float = 0.5) -> socket.socket:
    """Socket UDP comum; o timeout deixa loops de recepção checarem o pedido de parada."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.bind(("0.0.0.0", bind_port))
    sock.settimeout(recv_timeout_s)
    return sock
