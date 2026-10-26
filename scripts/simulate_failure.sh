#!/usr/bin/env bash
# Cenários de falha para a demo (#8). Rodar com o sistema no ar (./scripts/run_all.sh).
#
#   ./scripts/simulate_failure.sh event        sobe o limiar do reservatório → evento imediato (ALARME no Gateway)
#   ./scripts/simulate_failure.sh silent [s]   valvula-1 para de enviar e de responder à descoberta → OFFLINE, volta sozinha
#   ./scripts/simulate_failure.sh garbage [s]  valvula-1 envia valores fora da faixa física
#   ./scripts/simulate_failure.sh kill <nome>  mata o processo de uma fonte (como queda de energia), ex.: umidade-2
#   ./scripts/simulate_failure.sh crash        processo da valvula-1 cai (SimulateFailure CRASH) → OFFLINE
#   ./scripts/simulate_failure.sh restart      sobe a valvula-1 de novo → redescoberta → ONLINE
#   ./scripts/simulate_failure.sh recover      encerra a falha simulada e restaura o limiar
set -euo pipefail
cd "$(dirname "$0")/.."
PYTHON="${PYTHON:-.venv/bin/python}"
[ -x "$PYTHON" ] || PYTHON=python3
export DISTSYS_MCAST_IF="${DISTSYS_MCAST_IF:-127.0.0.1}"
VALVE=127.0.0.1:50051
RESERVOIR=127.0.0.1:50052

send_command() { "$PYTHON" -m gateway.commands "$@"; }

case "${1:-}" in
  event)   send_command "$RESERVOIR" threshold 90 ;;
  silent)  send_command "$VALVE" fail silent "${2:-30}" ;;
  garbage) send_command "$VALVE" fail out_of_range "${2:-30}" ;;
  crash)   send_command "$VALVE" fail crash ;;
  kill)
    name="${2:?informe a fonte, ex.: umidade-2}"
    pid=$(awk -v n="$name" '$2 == n { print $1 }' logs/pids | tail -1)
    [ -n "$pid" ] || { echo "fonte $name não encontrada em logs/pids"; exit 1; }
    kill "$pid" && echo "processo $name (pid $pid) encerrado"
    ;;
  restart)
    "$PYTHON" -m sources.valve --id valvula-1 --grpc-port 50051 --interval 5 >> logs/valvula-1.log 2>&1 &
    echo "$! valvula-1" >> logs/pids
    echo "valvula-1 reiniciada (pid $!); aparece ONLINE na próxima redescoberta"
    ;;
  recover)
    send_command "$VALVE" fail none
    send_command "$RESERVOIR" threshold 30
    ;;
  *) sed -n '2,10p' "$0"; exit 1 ;;
esac
