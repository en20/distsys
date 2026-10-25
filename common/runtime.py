"""Logging e ciclo de vida dos processos (fontes e Gateway)."""

import logging
import signal
import threading
import time
from collections.abc import Callable

Clock = Callable[[], float]


def epoch_ms(clock: Clock = time.time) -> int:
    """Timestamp em milissegundos desde a epoch. Ex.: epoch_ms() -> 1761400000000"""
    return int(clock() * 1000)


def configure_logging(component: str, level: int = logging.INFO) -> None:
    """Logs em texto legível no terminal — são mostrados no vídeo da demo (#22)."""
    logging.basicConfig(
        level=level,
        format=f"%(asctime)s [{component}] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )


def wait_for_shutdown_signal() -> None:
    """Bloqueia até SIGINT (Ctrl+C) ou SIGTERM (stop_all.sh / kill)."""
    shutdown = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: shutdown.set())
    # wait com timeout: garante que o handler de sinal rode no main thread.
    while not shutdown.wait(0.5):
        pass
