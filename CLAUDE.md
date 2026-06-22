# CLAUDE.md — DLM (Dataset Download Manager)

## Project Overview

Multi-server dataset download manager. Coordinates 7 worker nodes downloading datasets from HuggingFace/ModelScope and uploading to Baidu BOS.

- **Repo**: https://github.com/tyqqj0/bos-download-manager.git
- **Web UI**: http://154.85.43.52:8080
- **State**: BOS bucket `westlake-autolab-databuilder-data`, prefix `downloads/state.json`

## Architecture

```
BOS state.json (single source of truth)
    ↕ poll every 10s
[w1] [w2] [w3] [w4] [w5] [w6] [w7]  ← worker daemons
    ↕ heartbeat every 60s
[S1: 154.85.43.52] ← web dashboard + jump host
```

- Workers claim "dispatched" tasks, transition to "downloading", then "done" on completion
- Downloads land in `/data/staging/{name}/` then upload to BOS
- Heartbeat: alive_at, current_task, disk_free_gb, speed_mbps. Threshold: 180s = dead.

## Server Topology

| Key | IP | Role |
|-----|-----|------|
| S1 | 154.85.43.52 | Jump host, web server |
| w1 | 156.240.120.209 | Worker |
| w2 | 154.85.53.152 | Worker |
| w3 | 154.85.49.95 | Worker |
| w4 | 154.85.40.244 | Worker |
| w5 | 154.85.54.251 | Worker |
| w6 | 154.85.50.210 | Worker |
| w7 | 156.240.121.60 | Worker |

SSH from S1 to w1-w5 works with pubkey. w6/w7 may need `ssh-copy-id`.

## Common Commands

```bash
# Start web server
cd /root/code/bos-download-manager
nohup python3 -m dlm web --port 8080 > /tmp/dlm-web.log 2>&1 &

# Start worker daemon (run ON the worker node)
tmux new-session -d -s dlm-worker \
  'set -a && source /root/.env && source .env 2>/dev/null && set +a && python3 -m dlm.worker.daemon --server-key w1'

# Health check
curl -s http://localhost:8080/api/doctor

# Auto-fix stuck/dead/zombie
curl -s -X POST http://localhost:8080/api/doctor \
  -H 'Content-Type: application/json' \
  -d '{"actions":["reset_stuck","restart_dead","skip_zombie"]}'

# Cleanup staging on a server
curl -s -X POST http://localhost:8080/api/servers/w7/cleanup
```

## Monitoring Duties

When running as a monitoring agent, check every hour:

1. `GET /api/doctor` — if issues found:
   - stuck_downloads → auto-fix (reset to dispatched)
   - dead_workers → auto-restart via SSH
   - zombie_tasks → skip (only if retry >= 99 or invalid repo)
   - disk_full → WARN only, do NOT auto-clean without user approval
2. `GET /api/dashboard` — check aggregate_speed_mbps > 0, active_downloads count
3. If web server is down → restart it
4. If a worker has been dead > 1 hour → SSH restart daemon

## Hard Constraints

- **NEVER delete downloaded data on BOS** — only clean /data/staging/ from done/skipped/failed tasks
- Only touch state.json through the API or StateManager (never raw BOS writes)
- Workers only pick up `status == "dispatched"` — reset stuck tasks to this status
- ModelScope downloads are unreliable for large repos — skip if stuck

## Code Structure

```
dlm/
├── cli.py              # Click CLI entry point
├── constants.py        # BOS bucket names, paths
├── commands/           # CLI subcommands (add, ls, sync, doctor, web...)
├── core/
│   ├── state.py        # StateManager — BOS state.json read/write
│   ├── models.py       # Pydantic models (DownloadTask, DownloadState)
│   ├── servers.py      # Server config loader (servers.yaml)
│   ├── ssh.py          # SSH utilities (ssh_exec, ssh_server)
│   ├── bos.py          # BOS client wrapper
│   ├── parser.py       # URL/repo ID parser
│   └── config.py       # .env / config loading
├── web/
│   ├── app.py          # FastAPI app factory
│   ├── scheduler.py    # Background size refresh
│   ├── cache.py        # In-memory state cache
│   ├── static/         # Frontend (index.html, app.js)
│   └── routes/         # API routes (dashboard, tasks, servers, doctor, actions)
└── worker/
    ├── daemon.py       # Main daemon loop (poll → claim → download → upload)
    ├── heartbeat.py    # BOS heartbeat reporter
    ├── disk.py         # Disk space monitoring
    ├── task_runner.py  # Task execution coordinator
    ├── handlers/       # Download handlers (hf, modelscope, wget)
    └── movers/         # Upload movers (rsync_fuse, bos_sdk)
```

## Environment Variables

Required in `.env` on each server:
```
BAIDU_AK=...
BAIDU_SK=...
BOS_ENDPOINT=https://bj.bcebos.com
HF_TOKEN=...              # For gated repos
HF_HUB_CACHE=/root/.cache/huggingface
```
