#!/usr/bin/env bash
# Encerra (SIGTERM → encerramento limpo) os processos iniciados pelo run_all.sh.
set -uo pipefail
cd "$(dirname "$0")/.."
[ -f logs/pids ] || { echo "Nada rodando (logs/pids não existe)"; exit 0; }
while read -r pid name; do
  kill "$pid" 2>/dev/null && echo "  parado: $name (pid $pid)"
done < logs/pids
rm -f logs/pids
