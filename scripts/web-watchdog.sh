#!/bin/bash
# HTTP-liveness watchdog for dlm-web. Run by dlm-web-watchdog.timer every 30s.
#
# Catches the failure Restart=always cannot: a process that is alive, holds
# the port, and never answers (2026-07-31: event loop wedged 24h unnoticed).
#
# Trigger discipline — only counts what a restart can actually fix:
#   curl exit 7  (connection refused)  \  the wedged-loop / dead-process
#   curl exit 28 (timeout)             /  shapes; these count toward restart
#   HTTP 5xx                           →  logged, NOT counted. A restart does
#      not fix SQLite lock contention, and counting 500s turns a busy DB into
#      a restart loop.
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

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') $*" >> "$LOG"; }
now=$(date +%s)

# Cooldown: a probe right after our own restart is measuring startup, not health.
if [ -f "$COOLDOWN_FILE" ]; then
    last=$(cat "$COOLDOWN_FILE" 2>/dev/null || echo 0)
    if [ $((now - last)) -lt "$COOLDOWN_S" ]; then
        exit 0
    fi
fi

code=$(curl -s -o /dev/null -m 10 -w '%{http_code}' "$URL")
curl_rc=$?

if [ "$curl_rc" -eq 0 ] && [ "$code" = "200" ]; then
    echo 0 > "$COUNT_FILE"
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
    # Reachable but unhappy (5xx, empty reply...) — not the wedge shape.
    log "probe: http=$code curl_rc=$curl_rc — logged only, not counted"
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
# --no-block: a oneshot blocking on another unit's restart job is a
# job-ordering hazard, and it keeps this probe's own runtime bounded.
systemctl restart --no-block dlm-web
