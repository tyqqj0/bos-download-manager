#!/bin/bash
# scripts/safe-deploy.sh — Safe deployment: cancel workflows → sync → restart → re-dispatch
#
# Usage: bash scripts/safe-deploy.sh [--skip-cancel] [--worker w1,w2]
#
# This script replaces deploy-all.sh for production use.
# It ensures running downloads are gracefully cancelled before restart,
# and automatically re-dispatched after workers come back online.
set -euo pipefail

COORDINATOR="http://154.85.43.52:8080"
# The FULL fleet. Phase 1's cancel is fleet-wide, so a no-args run of this
# script must also touch every host it cancelled work for — a w-only list
# here once meant bj downloads were cancelled by a script that never
# restarted bj workers.
WORKERS=(
    "w1:156.240.120.209"
    "w2:154.85.53.152"
    "w3:154.85.49.95"
    "w4:154.85.40.244"
    "w5:154.85.54.251"
    "w6:154.85.50.210"
    "w7:156.240.121.60"
    "bj1:120.48.57.202"
    "bj2:180.76.182.215"
    "bj3:120.48.21.57"
    "bj4:180.76.228.120"
    "bj5:120.48.56.197"
    "bj6:120.48.174.216"
    "bj7:120.48.79.251"
    "bj8:120.48.142.8"
    "bj9:106.12.159.208"
)

SKIP_CANCEL=false
TARGET_WORKERS=()

# Parse args
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-cancel) SKIP_CANCEL=true; shift ;;
        --worker) IFS=',' read -ra TARGET_WORKERS <<< "$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

# Filter workers if --worker specified
if [ ${#TARGET_WORKERS[@]} -gt 0 ]; then
    FILTERED=()
    for entry in "${WORKERS[@]}"; do
        key="${entry%%:*}"
        for target in "${TARGET_WORKERS[@]}"; do
            if [ "$key" == "$target" ]; then
                FILTERED+=("$entry")
            fi
        done
    done
    WORKERS=("${FILTERED[@]}")
fi

echo "=== DLM Safe Deploy ==="
echo "  Workers: ${WORKERS[*]}"
echo ""

# Phase 1: Cancel running workflows.
# The endpoint is FLEET-WIDE (it cancels bj downloads too), so it only runs
# when this script's scope is also the whole fleet. A --worker subset deploy
# must not take down every other worker's downloads as a side effect.
if [ "$SKIP_CANCEL" = false ] && [ ${#TARGET_WORKERS[@]} -gt 0 ]; then
    echo "=== Phase 1 SKIPPED: --worker subset given, but workflow cancel is"
    echo "    fleet-wide. Use a full-fleet run (no --worker) to cancel, or"
    echo "    accept in-flight batches being interrupted by the restart."
    SKIP_CANCEL=true
fi
if [ "$SKIP_CANCEL" = false ]; then
    echo "=== Phase 1: Cancel running workflows ==="
    # No -f: a 4xx must be loud, not masked into the {"count":0} fallback —
    # that exact masking is how this phase reported success while cancelling
    # nothing for a month.
    RESULT=$(curl -s -X POST "$COORDINATOR/api/cancel-all-workflows" \
        -H 'Content-Type: application/json' -d '{"confirm": true}')
    if ! echo "$RESULT" | python3 -c "
import sys, json
d = json.load(sys.stdin)
assert d.get('dry_run') is False, f'expected an armed cancel, got: {d}'
assert 'error' not in d, d['error']
assert not d.get('errors'), f\"partial cancel failures: {d['errors']}\"
"; then
        echo "  ERROR: cancel-all-workflows did not execute: $RESULT"
        exit 1
    fi
    CANCELLED=$(echo "$RESULT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('count',0))")
    echo "  Cancelled $CANCELLED workflows"
    if [ "$CANCELLED" -gt 0 ]; then
        echo "  Waiting 30s for graceful shutdown..."
        sleep 30
    fi
    echo ""
fi

# Phase 2+3: Sync + restart — delegated to deploy-workers.sh, the single
# owner of worker lifecycle. It knows each bj host's personal task queue
# (a worker restarted without DLM_TASK_QUEUE polls the wrong queues and
# silently takes no work), installs/checks the sidecar, and prints the md5
# version manifest. The tmux-based restart that used to live here is the
# ad-hoc launch pattern this repo has already banned once.
echo "=== Phase 2+3: Sync + restart (deploy-workers.sh) ==="
if [ ${#TARGET_WORKERS[@]} -gt 0 ]; then
    DEPLOY_ARGS=()
    for target in "${TARGET_WORKERS[@]}"; do
        DEPLOY_ARGS+=(--worker "$target")
    done
    bash /root/code/bos-download-manager/scripts/deploy-workers.sh "${DEPLOY_ARGS[@]}"
else
    bash /root/code/bos-download-manager/scripts/deploy-workers.sh
fi
echo ""

# Phase 4: Wait for workers to register, then re-dispatch
echo "=== Phase 4: Re-dispatch ==="
echo "  Waiting 15s for workers to come online..."
sleep 15

# The reconciler will auto-dispatch orphaned tasks within 5 minutes.
# Trigger it manually for immediate effect:
curl -sf "$COORDINATOR/api/doctor" -X POST \
    -H 'Content-Type: application/json' \
    -d '{"actions":["redispatch_orphaned"]}' > /dev/null 2>&1 || true
echo "  Triggered re-dispatch of orphaned tasks."
echo ""

echo "=== Deploy complete ==="
echo "  Dashboard: http://154.85.43.52:8080"
echo "  Temporal:  http://154.85.43.52:8233"
echo ""
echo "  Auto-dispatch will assign pending tasks within 5 minutes."
echo "  Monitor: curl -s http://154.85.43.52:8080/api/doctor | python3 -m json.tool"
