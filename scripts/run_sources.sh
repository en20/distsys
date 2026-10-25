#!/usr/bin/env bash
# Sobe todas as fontes de dados (cada uma em um processo separado).
# Logs em logs/<id>.log. Ctrl+C encerra todas.
#
#   scripts/run_sources.sh            # intervalo padrão de 15 s
#   INTERVAL=5 scripts/run_sources.sh # envio a cada 5 s
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-python}"
[ -x .venv/bin/python ] && PYTHON=.venv/bin/python
INTERVAL="${INTERVAL:-15}"
mkdir -p logs

[ -f proto/sources_pb2.py ] || "$PYTHON" scripts/gen_proto.py

pids=()
start() {  # start <log> <módulo> [args...]
  local log="logs/$1.log"; shift
  "$PYTHON" -m "$@" --interval "$INTERVAL" >"$log" 2>&1 &
  pids+=($!)
  echo "  $* (pid $!) -> $log"
}

echo "subindo fontes (intervalo ${INTERVAL}s):"
start valvula-1      sources.valve           --id valvula-1      --location talhao-1 --grpc-port 50051
start valvula-2      sources.valve           --id valvula-2      --location talhao-2 --grpc-port 50053
start reservatorio-1 sources.reservoir       --id reservatorio-1 --grpc-port 50052
start umidade-1      sources.soil_moisture   --id umidade-1      --location talhao-1
start umidade-2      sources.soil_moisture   --id umidade-2      --location talhao-2
start umidade-3      sources.soil_moisture   --id umidade-3      --location talhao-3
start estacao-1      sources.weather_station --id estacao-1      --location sede

trap 'echo; echo "encerrando fontes..."; kill "${pids[@]}" 2>/dev/null; wait' INT TERM
echo "fontes no ar. Acompanhe com: tail -f logs/*.log   (Ctrl+C para encerrar)"
wait
