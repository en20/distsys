#!/usr/bin/env bash
# Sobe o Gateway e as fontes, cada um em processo separado (requisito 2a).
# Logs em logs/<nome>.log; PIDs em logs/pids para o stop_all.sh.
#
# Por padrão o multicast usa a loopback (tudo na mesma máquina, imune a firewall).
# Para rodar em várias máquinas: DISTSYS_MCAST_IF=<ip-da-placa> ./scripts/run_all.sh
#
# Falhas para a demo: ./scripts/simulate_failure.sh (#8).
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHON="${PYTHON:-.venv/bin/python}"
[ -x "$PYTHON" ] || [ ! -x .venv/Scripts/python ] || PYTHON=.venv/Scripts/python  # venv no Windows
export DISTSYS_MCAST_IF="${DISTSYS_MCAST_IF:-127.0.0.1}"
mkdir -p logs
: > logs/pids

[ -d pb ] || ./scripts/gen_proto.sh

start() {  # start <nome> <módulo python> [args...]
  local name=$1; shift
  "$PYTHON" -m "$@" > "logs/$name.log" 2>&1 &
  echo "$! $name" >> logs/pids
  echo "  $name (pid $!) → logs/$name.log"
}

echo "Subindo processos (multicast via $DISTSYS_MCAST_IF):"
start gateway gateway.main --advertise-ip "$DISTSYS_MCAST_IF"
# Fontes do domínio (agricultura de precisão), as mesmas do run_sources.sh do Enzo:
start valvula-1 sources.valve --id valvula-1 --grpc-port 50051 --interval 5
start valvula-2 sources.valve --id valvula-2 --grpc-port 50053 --interval 5
start reservatorio-1 sources.reservoir --id reservatorio-1 --grpc-port 50052 --interval 5
start umidade-1 sources.soil_moisture --id umidade-1 --interval 5
start umidade-2 sources.soil_moisture --id umidade-2 --interval 5
start umidade-3 sources.soil_moisture --id umidade-3 --interval 5
start estacao-1 sources.weather_station --id estacao-1 --interval 5

echo "Cliente: cd client && node cli/index.js   |   dashboard: node web/server.js (http://127.0.0.1:8080)"
echo "Acompanhe com: tail -f logs/*.log   |   Pare com: ./scripts/stop_all.sh"
