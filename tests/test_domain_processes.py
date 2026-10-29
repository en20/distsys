"""Cada fonte roda como processo separado (requisito 2a) — portado de `enzo/fontes-de-dados`."""

import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.conftest import free_tcp_port

ROOT = Path(__file__).resolve().parent.parent


def spawn(module, *args):
    env = dict(os.environ, PYTHONPATH=str(ROOT), PYTHONIOENCODING="utf-8", DISTSYS_MCAST_IF="127.0.0.1")
    return subprocess.Popen([sys.executable, "-m", module, *args], cwd=ROOT, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="utf-8", errors="replace")


@pytest.fixture
def processes():
    procs = []
    yield procs
    for p in procs:
        if p.poll() is None:
            p.kill()
        p.wait(timeout=5)


def wait_exit(proc, timeout=10):
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.fail("processo não terminou")


@pytest.mark.parametrize("module", ["sources.valve", "sources.reservoir", "sources.soil_moisture",
                                    "sources.weather_station"])
def test_source_process_is_discovered_and_streams(gateway, mcast_port, processes, module):
    args = ["--id", "proc-1", "--interval", "0.5", "--mcast-port", str(mcast_port)]
    if module in ("sources.valve", "sources.reservoir"):
        args += ["--grpc-port", str(free_tcp_port())]
    processes.append(spawn(module, *args))

    deadline = time.monotonic() + 15
    while not gateway.store.readings(source_id="proc-1"):
        assert time.monotonic() < deadline, "o Gateway não recebeu leituras do processo"
        time.sleep(0.1)


def test_invalid_arguments_exit_with_error(processes):
    proc = spawn("sources.soil_moisture", "--id", "x", "--interval", "0.1")
    processes.append(proc)
    assert wait_exit(proc) == 2
    assert "intervalo deve estar entre" in proc.stdout.read()


def test_continuous_source_with_grpc_port_exits_with_error(processes):
    proc = spawn("sources.soil_moisture", "--id", "x", "--grpc-port", "50051")
    processes.append(proc)
    assert wait_exit(proc) == 2
    assert "não é controlável" in proc.stdout.read()


def test_second_source_on_the_same_grpc_port_exits_with_error(processes, mcast_port):
    port = free_tcp_port()
    first = spawn("sources.valve", "--id", "v1", "--grpc-port", str(port), "--mcast-port", str(mcast_port))
    processes.append(first)
    deadline = time.monotonic() + 15
    while "ouvindo na porta" not in first.stdout.readline():
        assert time.monotonic() < deadline and first.poll() is None, "primeira fonte não subiu"

    second = spawn("sources.valve", "--id", "v2", "--grpc-port", str(port), "--mcast-port", str(mcast_port))
    processes.append(second)
    assert wait_exit(second) == 2
    assert "porta gRPC" in second.stdout.read()


def test_command_tool_and_crash_failure_kill_the_process(processes, mcast_port):
    port = free_tcp_port()
    proc = spawn("sources.valve", "--id", "valvula-1", "--grpc-port", str(port), "--mcast-port", str(mcast_port))
    processes.append(proc)
    target = f"127.0.0.1:{port}"

    def command(*words):
        return subprocess.run([sys.executable, "-m", "gateway.commands", target, *words], cwd=ROOT,
                              capture_output=True, encoding="utf-8", errors="replace",
                              env=dict(os.environ, PYTHONIOENCODING="utf-8"))

    deadline = time.monotonic() + 15
    while command("status").returncode != 0:
        assert time.monotonic() < deadline, "servidor gRPC não subiu"
        time.sleep(0.2)

    assert "vazao_alvo_lpm=20" in command("param", "vazao_alvo_lpm", "20").stdout
    rejected = command("threshold", "10")
    assert rejected.returncode != 0 and "UNIMPLEMENTED" in rejected.stderr

    assert command("fail", "crash").returncode == 0
    assert wait_exit(proc) == 1  # a falha simulada derruba o processo de verdade
