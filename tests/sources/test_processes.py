"""Cada fonte roda como processo separado (requisito 2a) e as ferramentas de linha de comando."""

import os
import signal
import subprocess
import sys
import time

import pytest

from sources.pb import pb
from sources.tools import control, gateway_stub
from tests.conftest import ROOT, free_tcp_port, requires_multicast


def spawn(module, *args):
    env = dict(os.environ, PYTHONPATH=str(ROOT))
    return subprocess.Popen([sys.executable, "-m", module, *args], cwd=ROOT, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="utf-8",
                            errors="replace")


@pytest.fixture
def processes():
    procs = []
    yield procs
    for p in procs:
        if p.poll() is None:
            p.kill()
        p.wait(timeout=5)


def wait_exit(proc, timeout=5):
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        pytest.fail("processo não terminou")


@requires_multicast
@pytest.mark.parametrize("module", ["sources.valve", "sources.reservoir", "sources.soil_moisture",
                                    "sources.weather_station"])
def test_source_process_is_discovered_streams_and_stops_cleanly(gateway, mcast_port, processes, module):
    args = ["--id", "proc-1", "--interval", "0.5", "--mcast-port", str(mcast_port)]
    if module in ("sources.valve", "sources.reservoir"):
        args += ["--grpc-port", str(free_tcp_port())]
    proc = spawn(module, *args)
    processes.append(proc)

    def discovered(gw):
        if not gw.announcements():
            gw.discover()  # o processo pode ainda estar subindo
        return gw.readings("proc-1")

    gateway.wait_for(discovered, timeout=10)
    proc.send_signal(signal.SIGTERM)
    assert wait_exit(proc) == 0
    output = proc.stdout.read()
    assert "descoberta pelo gateway" in output and "fonte encerrada" in output


def test_invalid_arguments_exit_with_error(processes):
    proc = spawn("sources.soil_moisture", "--interval", "0.1")
    processes.append(proc)
    assert wait_exit(proc) == 2
    assert "intervalo deve estar entre" in proc.stdout.read()


def test_busy_grpc_port_exits_with_error(processes):
    import socket
    with socket.socket() as busy:
        busy.bind(("0.0.0.0", 0))
        busy.listen()
        proc = spawn("sources.valve", "--grpc-port", str(busy.getsockname()[1]), "--mcast-port", "0")
        processes.append(proc)
        assert wait_exit(proc) == 2
    assert "porta gRPC" in proc.stdout.read()


def test_control_tool_and_crash_failure_kill_the_process(processes, capsys, mcast_port):
    port = free_tcp_port()
    proc = spawn("sources.valve", "--grpc-port", str(port), "--mcast-port", str(mcast_port))
    processes.append(proc)
    target = f"127.0.0.1:{port}"

    deadline = time.monotonic() + 10
    while control.main([target, "status"]) != 0:
        assert time.monotonic() < deadline, "servidor gRPC não subiu"
        time.sleep(0.2)
    assert "valvula-1" in capsys.readouterr().out

    assert control.main([target, "param", "vazao_alvo_lpm", "20"]) == 0
    assert "vazao_alvo_lpm = 20" in capsys.readouterr().out
    assert control.main([target, "threshold", "10"]) == 1
    assert "UNIMPLEMENTED" in capsys.readouterr().err

    assert control.main([target, "fail", "crash"]) == 0
    assert wait_exit(proc) == 1  # a falha simulada derruba o processo de verdade


def test_gateway_stub_describes_messages():
    status = pb.SourceStatus(powered_on=True, send_interval_s=15, threshold=30)
    ann = pb.SourceMessage(announcement=pb.SourceAnnouncement(
        info=pb.SourceInfo(source_id="reservatorio-1", type=pb.RESERVOIR_LEVEL, controllable=True),
        ip="10.0.0.2", grpc_port=50052, initial_state=status, measurements=["nivel"]))
    text = gateway_stub.describe(ann)
    assert "ANÚNCIO" in text and "10.0.0.2:50052" in text and "limiar=30" in text

    reading = pb.SensorReading(info=pb.SourceInfo(source_id="reservatorio-1"), seq=5, is_event=True,
                               event_message="nível baixo")
    reading.measurements.add(name="nivel", value=12.5, unit="%")
    text = gateway_stub.describe(pb.SourceMessage(reading=reading))
    assert text.startswith("EVENTO") and "nível baixo" in text and "nivel=12.5%" in text
