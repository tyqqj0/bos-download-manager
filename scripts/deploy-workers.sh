#!/bin/bash
# scripts/deploy-workers.sh
# Sync code to all workers and restart temporal worker daemons.
# Run from S1 (154.85.43.52).
set -euo pipefail

REPO_DIR="/root/code/bos-download-manager"
REMOTE_DIR="/root/code/bos-download-manager"

declare -A WORKERS=(
    [w1]="156.240.120.209"
    [w2]="154.85.53.152"
    [w3]="154.85.49.95"
    [w4]="154.85.40.244"
    [w5]="154.85.54.251"
    [w6]="154.85.50.210"
    [w7]="156.240.121.60"
)

# Parse args
RESTART=true
TARGETS=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-restart) RESTART=false; shift ;;
        --worker) TARGETS="$TARGETS $2"; shift 2 ;;
        *) echo "Usage: $0 [--no-restart] [--worker w1] [--worker w2] ..."; exit 1 ;;
    esac
done

# Default: all workers
if [ -z "$TARGETS" ]; then
    TARGETS="${!WORKERS[*]}"
fi

echo "[$(date)] Deploying to: $TARGETS"

for key in $TARGETS; do
    ip="${WORKERS[$key]}"
    if [ -z "$ip" ]; then
        echo "  ERROR: Unknown worker '$key'"
        continue
    fi

    echo "  [$key] $ip — syncing code..."
    rsync -az --delete \
        --exclude '.git' \
        --exclude '__pycache__' \
        --exclude '*.pyc' \
        --exclude '.env' \
        --exclude 'node_modules' \
        "$REPO_DIR/" "root@$ip:$REMOTE_DIR/"

    if [ "$RESTART" = true ]; then
        echo "  [$key] $ip — restarting worker..."
        ssh "root@$ip" bash -s "$key" <<'REMOTE_SCRIPT'
            SERVER_KEY="$1"
            pkill -f "dlm.temporal" 2>/dev/null || true
            sleep 2
            cd /root/code/bos-download-manager
            export DLM_SERVER_KEY="$SERVER_KEY"
            nohup bash scripts/start-temporal-worker.sh > /var/log/dlm-worker.log 2>&1 &
            sleep 3
            if pgrep -f "dlm.temporal" > /dev/null; then
                echo "      Worker $SERVER_KEY started successfully"
            else
                echo "      ERROR: Worker $SERVER_KEY failed to start"
                tail -5 /var/log/dlm-worker.log
            fi
REMOTE_SCRIPT
    fi
done

echo "[$(date)] Deploy complete."
