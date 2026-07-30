#!/bin/bash
# scripts/start-temporal-worker.sh
# Usage: DLM_SERVER_KEY=w1 [DLM_TASK_QUEUE=download-w1] bash scripts/start-temporal-worker.sh
set -euo pipefail

SERVER_KEY="${DLM_SERVER_KEY:?Must set DLM_SERVER_KEY (e.g. w1)}"
TASK_QUEUE="${DLM_TASK_QUEUE:-}"
TEMPORAL_REMOTE="${TEMPORAL_REMOTE:-154.85.43.52}"
TEMPORAL_PORT="${TEMPORAL_PORT:-7233}"

cd /root/code/bos-download-manager

# Load environment
set -a
[ -f /root/.env ] && source /root/.env
[ -f .env ] && source .env
set +a

# HF high-performance download (XET protocol)
export HF_XET_HIGH_PERFORMANCE=1
export HF_HUB_CACHE=/tmp/hf_cache

# Prefer a direct connection; fall back to an SSH tunnel if 7233 is blocked
if timeout 3 bash -c "echo > /dev/tcp/$TEMPORAL_REMOTE/$TEMPORAL_PORT" 2>/dev/null; then
    export TEMPORAL_HOST="$TEMPORAL_REMOTE:$TEMPORAL_PORT"
    echo "[$(date)] Direct connection to $TEMPORAL_HOST"
else
    echo "[$(date)] Direct connection blocked — setting up SSH tunnel..."
    pkill -f "ssh.*-L.*$TEMPORAL_PORT:localhost:$TEMPORAL_PORT" 2>/dev/null || true
    sleep 1
    ssh -f -N -o ServerAliveInterval=60 -o ServerAliveCountMax=3 -o ExitOnForwardFailure=yes \
        -L "$TEMPORAL_PORT:localhost:$TEMPORAL_PORT" "root@$TEMPORAL_REMOTE"
    sleep 2
    if ! timeout 3 bash -c "echo > /dev/tcp/localhost/$TEMPORAL_PORT" 2>/dev/null; then
        echo "ERROR: SSH tunnel failed — cannot reach localhost:$TEMPORAL_PORT"
        exit 1
    fi
    export TEMPORAL_HOST="localhost:$TEMPORAL_PORT"
    echo "[$(date)] SSH tunnel established."
fi

EXTRA_ARGS=()
if [ -n "$TASK_QUEUE" ]; then
    EXTRA_ARGS+=(--task-queue "$TASK_QUEUE")
fi

echo "[$(date)] Starting Temporal worker: $SERVER_KEY -> $TEMPORAL_HOST queue=${TASK_QUEUE:-default}"
exec python3 -m dlm.temporal --server-key "$SERVER_KEY" --temporal-host "$TEMPORAL_HOST" "${EXTRA_ARGS[@]}"
