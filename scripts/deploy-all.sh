#!/bin/bash
# LEGACY — Use scripts/safe-deploy.sh instead.
# This script does hard-kill without graceful workflow cancellation.
# Kept for reference only.
set -euo pipefail

WORKERS=(
    "w1:156.240.120.209"
    "w2:154.85.53.152"
    "w3:154.85.49.95"
    "w4:154.85.40.244"
    "w5:154.85.54.251"
    "w6:154.85.50.210"
    "w7:156.240.121.60"
)

echo "=== Syncing code to workers ==="
for entry in "${WORKERS[@]}"; do
    key="${entry%%:*}"
    ip="${entry##*:}"
    echo "  → $key ($ip)"
    rsync -az --delete \
        --exclude='.git' --exclude='__pycache__' --exclude='.env' \
        /root/code/bos-download-manager/ \
        root@$ip:/root/code/bos-download-manager/ &
done
wait
echo "  Sync complete."

echo ""
echo "=== Installing deps on workers ==="
for entry in "${WORKERS[@]}"; do
    key="${entry%%:*}"
    ip="${entry##*:}"
    echo "  → $key ($ip)"
    ssh root@$ip "bash /root/code/bos-download-manager/scripts/install-deps.sh" 2>&1 | tail -2 &
done
wait
echo "  Install complete."

echo ""
echo "=== Starting workers ==="
for entry in "${WORKERS[@]}"; do
    key="${entry%%:*}"
    ip="${entry##*:}"
    echo "  → $key ($ip)"
    ssh root@$ip "
        # Kill old celery/worker processes
        pkill -f 'celery' 2>/dev/null || true
        pkill -f 'dlm.temporal' 2>/dev/null || true
        pkill -f 'dlm.worker' 2>/dev/null || true
        sleep 2

        # Start new Temporal worker in tmux
        tmux kill-session -t dlm-worker 2>/dev/null || true
        export DLM_SERVER_KEY=$key
        tmux new-session -d -s dlm-worker \
            'DLM_SERVER_KEY=$key bash /root/code/bos-download-manager/scripts/start-temporal-worker.sh'
    "
    echo "    $key started"
done

echo ""
echo "=== All workers deployed ==="
echo "Check Temporal UI: http://154.85.43.52:8233"
echo "Check Dashboard:   http://154.85.43.52:8080"
