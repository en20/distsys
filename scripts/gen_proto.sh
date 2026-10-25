#!/usr/bin/env sh
# Gera o código Python dos arquivos proto/*.proto (ver scripts/gen_proto.py).
set -e
cd "$(dirname "$0")/.."
exec python scripts/gen_proto.py "$@"
