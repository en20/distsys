"""Cliente Node.js de verdade conversando com o servidor TCP Python (framing + protobuf dos dois lados)."""

import json
import shutil
import subprocess
from pathlib import Path

import pytest

from gateway.client_dispatcher import ClientRequestDispatcher
from gateway.registry import SourceRegistry
from gateway.tcp_server import ClientTcpServer
from pb import client_pb2
from tests.builders import make_announcement, make_reading

CLIENT_DIR = Path(__file__).resolve().parent.parent / "client"
NODE = shutil.which("node")

pytestmark = pytest.mark.skipif(
    NODE is None or not (CLIENT_DIR / "node_modules" / "protobufjs").exists(),
    reason="precisa de Node.js e de `npm install` em client/",
)


class UnreachableCommands:
    """O pedido grande aponta para uma fonte inexistente, então o Gateway nunca chega a executar."""

    def execute(self, source_id, command):
        raise AssertionError("comando não deveria chegar ao executor")


def test_node_client_exchanges_large_and_pipelined_messages_with_python_gateway() -> None:
    registry = SourceRegistry()
    registry.upsert(make_announcement("sensor-1", interval_s=2.0))
    registry.touch("sensor-1", make_reading("sensor-1", 61.5))
    server = ClientTcpServer(ClientRequestDispatcher(registry, commands=UnreachableCommands()), host="127.0.0.1", port=0)
    server.start()
    try:
        completed = subprocess.run(
            [NODE, str(CLIENT_DIR / "testing" / "interop-client.js"), "127.0.0.1", str(server.port)],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    finally:
        server.stop()

    assert completed.returncode == 0, completed.stderr
    result = json.loads(completed.stdout)
    assert result["pipelinedCounts"] == [1] * 25
    assert result["sourceIds"] == ["sensor-1"]
    assert result["firstSource"]["state"] == "SOURCE_ONLINE"
    assert result["firstSource"]["lastValues"][0]["value"] == 61.5
    assert result["bigErrorCode"] == "ERROR_NOT_FOUND"
    assert result["bigErrorLength"] > 3_000_000
    assert result["afterBig"] == 1
    assert client_pb2.ERROR_NOT_FOUND == 2  # o nome usado pelo Node corresponde ao valor do contrato
