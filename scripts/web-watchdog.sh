#!/bin/bash
# HTTP-liveness watchdog for dlm-web. Run by dlm-web-watchdog.timer every 30s.
#
# Catches the failure Restart=always cannot: a process that is alive, holds
# the port, and never answers (2026-07-31: event loop wedged 24h unnoticed).
#
# Trigger discipline — only counts what a restart can actually fix:
#   curl exit 7  (connection refused)  \  the wedged-loop / dead-process
#   curl exit 28 (timeout)             /  shapes; these count toward restart
#   any HTTP response (200/5xx/...)    →  the loop answered, so it is not
#      wedged: resets the consecutive counter. 5xx is logged; a restart does
#      not fix SQLite lock contention.
set -u

URL="http://127.0.0.1:8080/api/dashboard"
COUNT_FILE="/run/dlm-web-watchdog.count"        # tmpfs: reboot resets, correctly
COOLDOWN_FILE="/run/dlm-web-watchdog.cooldown"  # last restart epoch
HOURLY_FILE="/run/dlm-web-watchdog.restarts"    # one epoch per line
HEARTBEAT_FILE="/run/dlm-web-watchdog.heartbeat"
LOG="/var/log/dlm-web-watchdog.log"

FAIL_THRESHOLD=3
COOLDOWN_S=120        # after a restart, give startup + cache warm-up a grace period
MAX_RESTARTS_HOUR=3   # beyond this something is really wrong — stop and shout
HEARTBEAT_EVERY_S=3600
SCHED_STALE_S=300     # dashboard updated_at older than this = scheduler wedged

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" >> "$LOG"; }
now=$(date +%s)

# Respect operator intent: `systemctl stop dlm-web` must stay stopped. A
# crashed process is Restart=always's job; the wedge this script exists for
# always leaves the unit `active`. `failed` should be unreachable with
# StartLimitIntervalSec=0, but if it happens, recover rather than stand by.
state=$(systemctl is-active dlm-web 2>/dev/null || true)
case "$state" in
    active|activating|reloading) ;;
    failed)
        log "unit is 'failed' — reset-failed + restart"
        systemctl reset-failed dlm-web 2>/dev/null || true
        systemctl restart --no-block dlm-web \
            || log "ERROR: restart of failed unit did not enqueue (rc=$?)"
        echo "$now" > "$COOLDOWN_FILE"
        exit 0
        ;;
    *) exit 0 ;;  # inactive/deactivating = deliberately stopped
esac

# Cooldown: a probe right after our own restart is measuring startup, not health.
if [ -f "$COOLDOWN_FILE" ]; then
    last=$(cat "$COOLDOWN_FILE" 2>/dev/null || echo 0)
    if [ $((now - last)) -lt "$COOLDOWN_S" ]; then
        exit 0
    fi
fi

body=$(curl -s -m 10 "$URL")
curl_rc=$?

if [ "$curl_rc" -eq 0 ] && [ -n "$body" ]; then
    echo 0 > "$COUNT_FILE"
    # The loop answering does not prove the scheduler loop is advancing —
    # the cache serves stale data forever. Log-only signal (a restart is not
    # known to be the right fix for that failure, a human is).
    stale=$(echo "$body" | python3 -c "
import json, sys, time
try:
    d = json.load(sys.stdin)
    ts = d.get('updated_at') or 0
    print(int(time.time() - ts))
except Exception:
    print(-1)
" 2>/dev/null || echo -1)
    if [ "$stale" -gt "$SCHED_STALE_S" ] 2>/dev/null; then
        log "WARNING: HTTP alive but dashboard is ${stale}s stale — scheduler may be wedged (not auto-restarting)"
    fi
    last_hb=$(cat "$HEARTBEAT_FILE" 2>/dev/null || echo 0)
    if [ $((now - last_hb)) -ge "$HEARTBEAT_EVERY_S" ]; then
        # A watchdog that only logs failures is indistinguishable from one
        # that is not running.
        log "ok (hourly heartbeat)"
        echo "$now" > "$HEARTBEAT_FILE"
    fi
    exit 0
fi

if [ "$curl_rc" -ne 7 ] && [ "$curl_rc" -ne 28 ]; then
    # Reachable but unhappy (5xx handled above as body; here: odd curl rc).
    # The server responded in some form, so the wedge chain is broken.
    echo 0 > "$COUNT_FILE"
    log "probe: curl_rc=$curl_rc — logged only, counter reset"
    exit 0
fi

fails=$(cat "$COUNT_FILE" 2>/dev/null || echo 0)
fails=$((fails + 1))
echo "$fails" > "$COUNT_FILE"
log "probe FAILED (curl_rc=$curl_rc, consecutive=$fails/$FAIL_THRESHOLD)"

[ "$fails" -lt "$FAIL_THRESHOLD" ] && exit 0

# Rate limit: more than MAX_RESTARTS_HOUR/h means restarting is not helping.
touch "$HOURLY_FILE"
recent=$(awk -v cutoff=$((now - 3600)) '$1 > cutoff' "$HOURLY_FILE" | wc -l)
if [ "$recent" -ge "$MAX_RESTARTS_HOUR" ]; then
    log "REFUSING restart: already $recent in the last hour — needs a human"
    exit 0
fi

log "RESTARTING dlm-web ($fails consecutive connect failures)"
awk -v cutoff=$((now - 3600)) '$1 > cutoff' "$HOURLY_FILE" > "$HOURLY_FILE.tmp" \
    && mv "$HOURLY_FILE.tmp" "$HOURLY_FILE"
echo "$now" >> "$HOURLY_FILE"
echo 0 > "$COUNT_FILE"
echo "$now" > "$COOLDOWN_FILE"
systemctl reset-failed dlm-web 2>/dev/null || true
# --no-block: a oneshot blocking on another unit's restart job is a
# job-ordering hazard, and it keeps this probe's own runtime bounded.
if ! systemctl restart --no-block dlm-web; then
    log "ERROR: systemctl restart dlm-web FAILED (rc=$?) — unit state needs a human"
fi
