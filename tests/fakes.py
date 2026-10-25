"""Fakes nomeados para I/O externo (sockets, relógio) usados nos testes."""

from common.net import Address


class FakeDatagramSocket:
    """Guarda cada sendto em vez de mandar pela rede."""

    def __init__(self) -> None:
        self.sent: list[tuple[bytes, Address]] = []

    def sendto(self, data: bytes, address: Address, /) -> int:
        self.sent.append((data, address))
        return len(data)


class FakeClock:
    """Relógio controlado manualmente: testes de timeout sem sleep."""

    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FailingDatagramSocket:
    """sendto sempre falha, como um socket sem rota para o Gateway."""

    def sendto(self, data: bytes, address: Address, /) -> int:
        raise OSError("Network is unreachable")
