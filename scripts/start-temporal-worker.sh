#!/bin/bash
# scripts/start-temporal-worker.sh
# Usage: DLM_SERVER_KEY=w1 bash scripts/start-temporal-worker.sh
set -euo pipefail

SERVER_KEY="${DLM_SERVER_KEY:?Must set DLM_SERVER_KEY (e.g. w1)}"
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

# Establish SSH tunnel to Temporal Server (port 7233 is blocked by security group)
echo "[$(date)] Setting up SSH tunnel to $TEMPORAL_REMOTE:$TEMPORAL_PORT..."
pkill -f "ssh.*-L.*$TEMPORAL_PORT:localhost:$TEMPORAL_PORT" 2>/dev/null || true
sleep 1
ssh -f -N -o ServerAliveInterval=60 -o ServerAliveCountMax=3 -o ExitOnForwardFailure=yes \
    -L "$TEMPORAL_PORT:localhost:$TEMPORAL_PORT" "root@$TEMPORAL_REMOTE"
sleep 2

# Verify tunnel is up
if ! timeout 3 bash -c "echo > /dev/tcp/localhost/$TEMPORAL_PORT" 2>/dev/null; then
    echo "ERROR: SSH tunnel failed — cannot reach localhost:$TEMPORAL_PORT"
    exit 1
fi
echo "[$(date)] SSH tunnel established."

export TEMPORAL_HOST="localhost:$TEMPORAL_PORT"

echo "[$(date)] Starting Temporal worker: $SERVER_KEY -> $TEMPORAL_HOST"
exec python3 -m dlm.temporal --server-key "$SERVER_KEY" --temporal-host "$TEMPORAL_HOST"
