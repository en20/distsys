"""Fluxo de dados de ponta a ponta na loopback: fonte → UDP → Gateway → memória/SQLite → consultas."""

import argparse
import random
import time
from pathlib import Path

import pytest

from common.net import configure_multicast_sender, open_udp_socket
from gateway.main import Gateway, build_arg_parser, build_gateway
from gateway.readings import ReadingStore
from gateway.registry import SourceRegistry
from gateway.storage import SqliteStorage
from pb import sources_pb2
from sources.config import SourceConfig
from sources.generator import ValueGenerator
from sources.simulated import SimulatedSource
from tests.conftest import random_udp_port

LOOPBACK = "127.0.0.1"


def wait_until(condition, timeout_s: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(0.05)
    return False


@pytest.fixture
def mcast_port() -> int:
    return random_udp_port(30_001, 40_000)  # isola de outras execuções e da porta real 5007


def start_gateway(mcast_port: int, db_path: Path | None = None) -> Gateway:
    sock = open_udp_socket(0)
    configure_multicast_sender(sock, ttl=1, interface_ip=LOOPBACK)
    advertise = (LOOPBACK, sock.getsockname()[1])
    storage = SqliteStorage(db_path) if db_path else None
    gateway = Gateway(sock, ("224.1.1.1", mcast_port), advertise, rediscovery_s=0.3,
                      registry=SourceRegistry(), store=ReadingStore(), storage=storage)
    gateway.start()
    return gateway


def start_source(source_id: str, mcast_port: int, base: float, interval_s: float = 0.2) -> SimulatedSource:
    config = SourceConfig(source_id, "SENSOR_UMIDADE", interval_s, 0, mcast_port=mcast_port, mcast_interface=LOOPBACK)
    generator = ValueGenerator(base=base, noise=0.5, drift=0.1, minimum=0.0, maximum=100.0, rng=random.Random(1))
    source = SimulatedSource(config, "umidade", "%", generator)
    source.start()
    return source


def test_readings_reach_the_gateway_and_answer_queries(mcast_port: int, tmp_path: Path) -> None:
    gateway = start_gateway(mcast_port, tmp_path / "gateway.db")
    dry, wet = start_source("umidade-1", mcast_port, base=15.0), start_source("umidade-2", mcast_port, base=45.0)
    try:
        assert wait_until(lambda: all(len(gateway.store.readings(source_id=s)) >= 5 for s in ("umidade-1", "umidade-2")))

        record = gateway.registry.get("umidade-1")
        assert record.last_reading.measurements[0].metric == "umidade"  # registry.touch() continua funcionando

        driest = gateway.queries.run("menor_media", window_s=60)
        assert driest.metric == "umidade" and driest.source_id == "umidade-1"
        assert gateway.queries.run("media", window_s=60).value == pytest.approx(30.0, abs=5.0)

        dry.sender.send(dry.sample(), is_event=True)  # evento imediato (o que o Jon faz no #6)
        assert wait_until(lambda: gateway.receiver.alarm_counts() == {"umidade-1": 1})
        assert gateway.queries.run("contagem_eventos", window_s=60).source_id == "umidade-1"
    finally:
        dry.stop()
        wet.stop()
        gateway.stop()


def test_history_survives_gateway_restart(mcast_port: int, tmp_path: Path) -> None:
    db_path = tmp_path / "gateway.db"
    gateway = start_gateway(mcast_port, db_path)
    source = start_source("umidade-1", mcast_port, base=30.0)
    try:
        assert wait_until(lambda: len(gateway.store.readings()) >= 5)
    finally:
        source.stop()
        gateway.stop()
    received = len(SqliteStorage(db_path).readings())
    assert received >= 5

    restarted = start_gateway(mcast_port, db_path)
    try:
        assert len(restarted.store.readings()) == received  # recarregado do SQLite, sem nenhuma fonte no ar
        assert restarted.queries.run("media", window_s=600).sample_count == received
        assert len(restarted.storage.history("umidade-1")) == received
    finally:
        restarted.stop()


def test_paused_source_stops_sending_and_resumes(mcast_port: int) -> None:
    gateway = start_gateway(mcast_port)
    source = start_source("umidade-1", mcast_port, base=30.0, interval_s=0.1)
    try:
        assert wait_until(lambda: len(gateway.store.readings()) >= 3)
        source.publisher.pause()
        time.sleep(0.3)
        count = len(gateway.store.readings())
        time.sleep(0.5)
        assert len(gateway.store.readings()) == count
        source.publisher.resume()
        assert wait_until(lambda: len(gateway.store.readings()) > count)
    finally:
        source.stop()
        gateway.stop()


def test_simulated_source_builds_readings_from_the_generator() -> None:
    config = SourceConfig("temp-1", "SENSOR_TEMPERATURA", 5.0, 0)
    generator = ValueGenerator(base=25.0, noise=0.0, drift=0.0)
    source = SimulatedSource(config, "temperatura", "°C", generator, listener_sock=open_udp_socket(0))
    try:
        reading = source.sample()
        assert (reading.source_id, reading.type) == ("temp-1", "SENSOR_TEMPERATURA")
        assert list(reading.measurements) == [sources_pb2.Measurement(metric="temperatura", value=25.0, unit="°C")]
        assert source.gateway_address() is None and source.sender.send(reading) is False  # sem descoberta
    finally:
        source.stop()


def gateway_args(*argv: str) -> argparse.Namespace:
    return build_arg_parser().parse_args(["--udp-port", "0", "--tcp-port", "0", *argv])


def test_main_wires_persistence_from_arguments(tmp_path: Path) -> None:
    gateway = build_gateway(gateway_args("--db", str(tmp_path / "x" / "g.db"), "--retention-h", "2"))
    try:
        assert gateway.storage is not None and gateway.storage.path == tmp_path / "x" / "g.db"
        assert gateway.store.retention_s == 7200
    finally:
        gateway.storage.stop()
        gateway.sock.close()

    without_db = build_gateway(gateway_args("--db", ""))
    try:
        assert without_db.storage is None
    finally:
        without_db.sock.close()


def test_main_rejects_invalid_retention() -> None:
    with pytest.raises(ValueError, match="retention"):
        build_gateway(gateway_args("--retention-h", "0"))
