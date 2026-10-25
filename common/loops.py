"""Threads de fundo com parada limpa (Ctrl+C sem traceback)."""

import logging
import threading
from collections.abc import Callable

log = logging.getLogger(__name__)


class BackgroundLoop:
    """Executa `step` repetidamente numa thread daemon até `stop()`.

    `step` deve retornar em ~1 s (ex.: recv com timeout) para a parada ser rápida.
    Exceções em `step` são logadas e o loop continua — um pacote ruim não derruba o processo.
    Ex.: BackgroundLoop("udp-rx", lambda: handle(sock.recvfrom(65535))).start()
    """

    def __init__(self, name: str, step: Callable[[], None]) -> None:
        self.name = name
        self._step = step
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self, timeout_s: float = 2.0) -> None:
        self._stop_event.set()
        if self._thread.is_alive():
            self._thread.join(timeout_s)

    @property
    def stopping(self) -> bool:
        return self._stop_event.is_set()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._step()
            except TimeoutError:
                continue
            except Exception:
                if not self._stop_event.is_set():
                    log.exception("Erro no loop %s", self.name)


class PeriodicTask(BackgroundLoop):
    """Executa `action` a cada `interval_s` segundos; `trigger()` antecipa a próxima execução.

    Ex.: PeriodicTask("rediscovery", 10.0, broadcaster.send_request).start()
    """

    def __init__(self, name: str, interval_s: float, action: Callable[[], object]) -> None:
        super().__init__(name, self._tick)
        self._action = action
        self._interval_s = interval_s
        self._wake_event = threading.Event()

    def trigger(self) -> None:
        self._wake_event.set()

    def stop(self, timeout_s: float = 2.0) -> None:
        self._stop_event.set()
        self._wake_event.set()
        super().stop(timeout_s)

    def _tick(self) -> None:
        self._action()
        self._wake_event.wait(self._interval_s)
        self._wake_event.clear()
