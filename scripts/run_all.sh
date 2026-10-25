#!/usr/bin/env bash
# Sobe o Gateway e as fontes, cada um em processo separado (requisito 2a).
# Logs em logs/<nome>.log; PIDs em logs/pids para o stop_all.sh.
#
# Por padrão o multicast usa a loopback (tudo na mesma máquina, imune a firewall).
# Para rodar em várias máquinas: DISTSYS_MCAST_IF=<ip-da-placa> ./scripts/run_all.sh
#
# Cada fluxo acrescenta suas fontes reais aqui quando ficarem prontas (Jon: #5, #6, #7).
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
# Fontes de teste (só descoberta) até as fontes reais existirem:
start probe-1 sources.base --id probe-1 --type PROBE --interval 5
start probe-2 sources.base --id probe-2 --type PROBE --interval 5
start probe-ctrl sources.base --id probe-ctrl --type PROBE_CTRL --grpc-port 50051
# Fontes simuladas com envio periódico (fluxo de dados, #26) — alimentam as consultas e o histórico:
start sim-umidade-1 sources.simulated --id sim-umidade-1 --type SENSOR_SIMULADO --metric umidade --unit % \
  --base 25 --noise 0.3 --drift 0.2 --min 0 --max 100 --interval 5
start sim-umidade-2 sources.simulated --id sim-umidade-2 --type SENSOR_SIMULADO --metric umidade --unit % \
  --base 40 --noise 1.0 --drift 0.5 --min 0 --max 100 --interval 5

echo "Acompanhe com: tail -f logs/*.log   |   Pare com: ./scripts/stop_all.sh"
