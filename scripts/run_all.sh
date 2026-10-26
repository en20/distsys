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
# Fontes controláveis (gRPC + UDP, #5 e #6):
start valvula-1 sources.valve --id valvula-1 --grpc-port 50051 --interval 5 --vazao-alvo 20
start estufa-1 sources.greenhouse --id estufa-1 --grpc-port 50052 --interval 5 --threshold 30 --spike-chance 0.01
# Fontes contínuas (só UDP, #7) — vários talhões alimentam "talhão mais seco" e "maior variação":
start umidade-talhao-1 sources.soil_moisture --id umidade-talhao-1 --interval 5
start umidade-talhao-2 sources.soil_moisture --id umidade-talhao-2 --interval 5
start umidade-talhao-3 sources.soil_moisture --id umidade-talhao-3 --interval 5
start estacao-1 sources.weather_station --id estacao-1 --interval 5

echo "Acompanhe com: tail -f logs/*.log   |   Pare com: ./scripts/stop_all.sh"
