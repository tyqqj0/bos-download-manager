#!/bin/bash
# scripts/start-temporal-worker.sh
# Usage: DLM_SERVER_KEY=w1 bash scripts/start-temporal-worker.sh
set -euo pipefail

SERVER_KEY="${DLM_SERVER_KEY:?Must set DLM_SERVER_KEY (e.g. w1)}"
TEMPORAL_HOST="${TEMPORAL_HOST:-154.85.43.52:7233}"

cd /root/code/bos-download-manager

# Load environment
set -a
[ -f /root/.env ] && source /root/.env
[ -f .env ] && source .env
set +a

export TEMPORAL_HOST="$TEMPORAL_HOST"

echo "[$(date)] Starting Temporal worker: $SERVER_KEY -> $TEMPORAL_HOST"
exec python3 -m dlm.temporal --server-key "$SERVER_KEY" --temporal-host "$TEMPORAL_HOST"
