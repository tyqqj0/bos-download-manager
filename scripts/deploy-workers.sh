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
    [bj1]="120.48.57.202"
    [bj2]="180.76.182.215"
    [bj3]="120.48.21.57"
    [bj4]="180.76.228.120"
    [bj5]="120.48.56.197"
    [bj6]="120.48.174.216"
    [bj7]="120.48.79.251"
    [bj8]="120.48.142.8"
    [bj9]="106.12.159.208"
)

# BJ workers poll only their personal queue (source isolation: ModelScope);
# w* workers additionally serve the shared coordinator queue (empty = default).
declare -A QUEUES=(
    [bj1]="download-bj1"
    [bj2]="download-bj2"
    [bj3]="download-bj3"
    [bj4]="download-bj4"
    [bj5]="download-bj5"
    [bj6]="download-bj6"
    [bj7]="download-bj7"
    [bj8]="download-bj8"
    [bj9]="download-bj9"
)

# Parse args
RESTART=true
TARGETS=""
CUSTOM_IP=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-restart) RESTART=false; shift ;;
        --worker) TARGETS="$TARGETS $2"; shift 2 ;;
        --ip) CUSTOM_IP="$2"; shift 2 ;;
        *) echo "Usage: $0 [--no-restart] [--worker w1] [--worker bjN --ip X.X.X.X] ..."; exit 1 ;;
    esac
done

# Default: all workers
if [ -z "$TARGETS" ]; then
    TARGETS="${!WORKERS[*]}"
fi

echo "[$(date)] Deploying to: $TARGETS"

for key in $TARGETS; do
    ip="${WORKERS[$key]:-$CUSTOM_IP}"
    if [ -z "$ip" ]; then
        echo "  ERROR: Unknown worker '$key' (use --ip for dynamic hosts)"
        continue
    fi
    queue="${QUEUES[$key]:-}"
    # Any bjN not in the static map still needs its personal queue
    if [ -z "$queue" ] && [[ "$key" == bj* ]]; then
        queue="download-$key"
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
        echo "  [$key] $ip — restarting worker (queue=${queue:-default})..."
        ssh "root@$ip" bash -s "$key" "$queue" <<'REMOTE_SCRIPT'
            SERVER_KEY="$1"
            TASK_QUEUE="${2:-}"
            pkill -f "dlm.temporal" 2>/dev/null || true
            sleep 2
            cd /root/code/bos-download-manager
            export DLM_SERVER_KEY="$SERVER_KEY"
            export DLM_TASK_QUEUE="$TASK_QUEUE"
            nohup bash scripts/start-temporal-worker.sh > /var/log/dlm-worker.log 2>&1 &
            sleep 3
            if ps aux | grep -q "[p]ython3 -m dlm.temporal"; then
                echo "      Worker $SERVER_KEY started successfully"
            else
                echo "      ERROR: Worker $SERVER_KEY failed to start"
                tail -5 /var/log/dlm-worker.log
            fi

            # Sidecar watchdog: independent of the worker, so it still reports
            # when the worker hangs or dies silently. enable = survives reboot.
            if [ -f /etc/systemd/system/dlm-sidecar@.service ]; then
                systemctl enable --now "dlm-sidecar@$SERVER_KEY" 2>/dev/null
                systemctl restart "dlm-sidecar@$SERVER_KEY" 2>/dev/null
                if systemctl is-active --quiet "dlm-sidecar@$SERVER_KEY"; then
                    echo "      Sidecar $SERVER_KEY active"
                else
                    echo "      WARN: sidecar $SERVER_KEY not active"
                fi
            fi
REMOTE_SCRIPT
    fi
done

# Version manifest: md5 of the files that matter, per worker vs S1
echo ""
echo "[$(date)] Version manifest (md5 of key files):"
MANIFEST_FILES="dlm/temporal/activities.py dlm/temporal/workflows.py dlm/web/reconciler.py dlm/web/routes/queue.py"
local_md5=$(cd "$REPO_DIR" && cat $MANIFEST_FILES | md5sum | cut -d' ' -f1)
echo "  S1 (reference): $local_md5"
for key in $TARGETS; do
    ip="${WORKERS[$key]:-$CUSTOM_IP}"
    [ -z "$ip" ] && continue
    remote_md5=$(ssh "root@$ip" "cd $REMOTE_DIR && cat $MANIFEST_FILES | md5sum | cut -d' ' -f1" 2>/dev/null || echo "UNREACHABLE")
    status="OK"
    [ "$remote_md5" != "$local_md5" ] && status="MISMATCH"
    echo "  $key: $remote_md5 [$status]"
done

echo "[$(date)] Deploy complete."
