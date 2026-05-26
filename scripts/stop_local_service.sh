#!/bin/zsh

set -euo pipefail

PIDS=("${(@f)$(pgrep -f 'uvicorn app.main:app --host 127.0.0.1 --port 8000' || true)}")

if [[ ${#PIDS[@]} -eq 0 || -z "${PIDS[1]:-}" ]]; then
  echo "No matching local service process found."
  exit 0
fi

echo "Stopping local service PID(s): ${PIDS[*]}"
kill "${PIDS[@]}"
