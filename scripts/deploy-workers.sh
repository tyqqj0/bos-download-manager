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

# One flaky host must never abort the fleet loop: on 2026-08-03 a single
# transient kex_exchange_identification reset (BJ sshd MaxStartups
# throttling) killed the run under `set -e`, leaving 7 hosts unverified —
# the exact "silent partial deploy" this script exists to prevent. Every
# per-host step retries, a host that still fails is recorded and skipped,
# and the script exits nonzero at the end if any host is not clean.
FAILED_HOSTS=""

for key in $TARGETS; do
    ip="${WORKERS[$key]:-$CUSTOM_IP}"
    if [ -z "$ip" ]; then
        echo "  ERROR: Unknown worker '$key' (use --ip for dynamic hosts)"
        FAILED_HOSTS="$FAILED_HOSTS $key"
        continue
    fi
    queue="${QUEUES[$key]:-}"
    # Any bjN not in the static map still needs its personal queue
    if [ -z "$queue" ] && [[ "$key" == bj* ]]; then
        queue="download-$key"
    fi

    echo "  [$key] $ip — syncing code..."
    synced=false
    for attempt in 1 2 3; do
        if rsync -az --delete \
            --exclude '.git' \
            --exclude '__pycache__' \
            --exclude '*.pyc' \
            --exclude '.env' \
            --exclude 'node_modules' \
            "$REPO_DIR/" "root@$ip:$REMOTE_DIR/"; then
            synced=true
            break
        fi
        echo "  [$key] rsync attempt $attempt failed — retrying in 10s..."
        sleep 10
    done
    if [ "$synced" = false ]; then
        echo "  [$key] ERROR: rsync failed after 3 attempts — skipping host"
        FAILED_HOSTS="$FAILED_HOSTS $key"
        continue
    fi

    # Sidecar unit: installed/refreshed on EVERY deploy, not only restarts.
    # The old `if [ -f ...service ]` check silently skipped hosts that never
    # had the unit — which is exactly how bj1-bj9 ran without stall telemetry
    # for a month. rsync above already put the unit at deploy/, so install
    # from there. enable --now starts a missing sidecar but leaves a running
    # one alone (code reload happens in the restart branch, same as workers).
    ssh "root@$ip" bash -s "$key" "$REMOTE_DIR" <<'SIDECAR_SCRIPT' || { echo "  [$key] ERROR: sidecar install ssh failed — skipping host"; FAILED_HOSTS="$FAILED_HOSTS $key"; continue; }
        SERVER_KEY="$1"
        REMOTE_DIR="$2"
        # The unit hardcodes /usr/bin/python3; a host where that interpreter
        # lacks `requests` (bj5-9 were provisioned separately) crash-loops
        # every 5s while briefly reading `active` — hence the warning AND the
        # sleep before is-active below.
        /usr/bin/python3 -c "import requests" 2>/dev/null \
            || echo "      WARN: $SERVER_KEY /usr/bin/python3 lacks requests — sidecar will crash-loop"
        install -m644 "$REMOTE_DIR/deploy/dlm-sidecar@.service" \
            /etc/systemd/system/dlm-sidecar@.service
        systemctl daemon-reload
        systemctl enable --now "dlm-sidecar@$SERVER_KEY" 2>/dev/null || true
        sleep 3
        if ! systemctl is-active --quiet "dlm-sidecar@$SERVER_KEY"; then
            echo "      WARN: sidecar $SERVER_KEY not active after install"
            systemctl status "dlm-sidecar@$SERVER_KEY" --no-pager 2>/dev/null | tail -3
        fi
SIDECAR_SCRIPT

    if [ "$RESTART" = true ]; then
        echo "  [$key] $ip — restarting worker (queue=${queue:-default})..."
        ssh "root@$ip" bash -s "$key" "$queue" <<'REMOTE_SCRIPT' || { echo "  [$key] ERROR: restart ssh failed"; FAILED_HOSTS="$FAILED_HOSTS $key"; continue; }
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

            # Sidecar watchdog: unit install/enable already happened in the
            # sync phase; a restart deploy also reloads its code. sleep before
            # is-active — checked immediately, a unit dying 0.2s later still
            # reads as active.
            systemctl restart "dlm-sidecar@$SERVER_KEY" 2>/dev/null
            sleep 3
            if systemctl is-active --quiet "dlm-sidecar@$SERVER_KEY"; then
                echo "      Sidecar $SERVER_KEY active"
            else
                echo "      WARN: sidecar $SERVER_KEY not active"
                systemctl status "dlm-sidecar@$SERVER_KEY" --no-pager 2>/dev/null | tail -3
            fi
REMOTE_SCRIPT
    fi
done

# Version manifest: md5 of the files that matter, per worker vs S1
echo ""
echo "[$(date)] Version manifest (md5 of key files):"
# Derived, not hand-written. The old list named seven files, so the gate said
# OK while a worker ran a stale dlm/core/bos.py — the entire upload path,
# including the multipart driver whose silent failure credits files that never
# landed on BOS. Any file rsync can carry belongs in the hash, so enumerate
# them: rsync --delete makes the trees identical on success, and LC_ALL=C
# fixes the order across hosts.
MANIFEST_CMD='find dlm -name "*.py" -not -path "*/__pycache__/*" -print0 | LC_ALL=C sort -z | xargs -0 cat | md5sum | cut -d" " -f1'
local_md5=$(cd "$REPO_DIR" && eval "$MANIFEST_CMD")
echo "  S1 (reference): $local_md5"
for key in $TARGETS; do
    ip="${WORKERS[$key]:-$CUSTOM_IP}"
    [ -z "$ip" ] && continue
    # Retried: a manifest probe losing the ssh race must not report a host
    # as MISMATCH when the deploy itself succeeded (bj1, 2026-08-03).
    # NB: the retry only works because `set -o pipefail` is in force — without
    # it, `cut` exiting 0 would make every attempt "succeed" and reinstate the
    # false-MISMATCH bug this loop exists to fix.
    remote_md5="UNREACHABLE"
    for attempt in 1 2 3; do
        if m=$(ssh -o ConnectTimeout=10 "root@$ip" "cd $REMOTE_DIR && $MANIFEST_CMD" 2>/dev/null); then
            remote_md5="$m"
            break
        fi
        sleep 5
    done
    # A host can have matching code on disk and still be running the old
    # process — it failed the version gate or its restart, so rsync landed but
    # the worker never came back on it. Reporting a bare OK there is the exact
    # "looks clean, is mixed-version" signal this manifest exists to prevent.
    already_failed=""
    case " $FAILED_HOSTS " in
        *" $key "*) already_failed=1 ;;
    esac
    if [ "$remote_md5" != "$local_md5" ]; then
        status="MISMATCH"
        [ -n "$already_failed" ] || FAILED_HOSTS="$FAILED_HOSTS $key"
    elif [ -n "$already_failed" ]; then
        status="DISK-OK/PROCESS-STALE"
    else
        status="OK"
    fi
    echo "  $key: $remote_md5 [$status]"
done

if [ -n "$FAILED_HOSTS" ]; then
    echo "[$(date)] Deploy INCOMPLETE — failed hosts:$FAILED_HOSTS"
    echo "  Re-run: bash scripts/deploy-workers.sh$(for k in $FAILED_HOSTS; do printf ' --worker %s' "$k"; done)"
    exit 1
fi
echo "[$(date)] Deploy complete."
