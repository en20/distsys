#!/usr/bin/env bash
# Gera o código Python dos .proto em pb/ (importável como `from pb import sources_pb2`).
# O mapeamento -Ipb=proto faz o código gerado importar `pb.sources_pb2`
# em vez de `sources_pb2` solto, que só funcionaria com hack de sys.path.
# O Node (client/) carrega os .proto direto com protobufjs; não precisa gerar.
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHON="${PYTHON:-.venv/bin/python}"
[ -x "$PYTHON" ] || PYTHON=python3

mkdir -p pb
"$PYTHON" -m grpc_tools.protoc \
  -Ipb=proto \
  --python_out=. --pyi_out=. --grpc_python_out=. \
  proto/*.proto
echo "Código protobuf gerado em pb/"
