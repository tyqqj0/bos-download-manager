# CLAUDE.md — DLM (Dataset Download Manager)

## Project Overview

Multi-server dataset download manager. Coordinates 16 worker nodes downloading datasets from HuggingFace/ModelScope and uploading to Baidu BOS, orchestrated by Temporal workflows.

- **Repo**: https://github.com/tyqqj0/bos-download-manager.git
- **Web UI**: http://154.85.43.52:8080
- **State**: SQLite on S1 (`dlm/queue/snapshot.py`) — the ONLY state source. Legacy BOS state.json is dead (kept only in one-shot migrate scripts).
- **Data buckets**: `auwomo-data` (datasets, keys = `{category}/{name}/{repo_path}`), `auwomo-model-open` (models, keys = `{name}/{repo_path}`)
- **搬运（BOS→地瓜云）**: 自动，下载 `done` 即排队。用法/排障看
  **[`docs/runbooks/transfer.md`](docs/runbooks/transfer.md)** — 状态机、`/api/transfer` 三个按钮、
  手动脚本、额度常量、验证到底验了什么，都在那一页。只有 `blocked`/`short`/`failed` 需要人。

## Architecture (Temporal, since 2026-07)

```
S1: Temporal Server (:7233, docker) + FastAPI web (:8080) + SQLite
        │ workflows / activities            ↑ HTTP: heartbeat, shard-progress, task-progress
[w1-w7]  HF tasks   — poll "download-workers" (coordinators) + "download-wN"
[bj1-9]  MS tasks   — poll "download-bjN" only
```

### Pool mode (default since 2026-08-09)

`dispatch_mode` on a task is `pool` (default) or `sharded` (legacy, still supported).

- Pool: `PoolDownloadWorkflow` lists → BOS resume filter → `chunk_filelist` cuts the filtered list into fixed-size batches (max `POOL_MAX_BATCHES`=1500, batch lists on BOS under `download-manager/batchlists/{name}/`) → a window of batches is dispatched to whichever workers are free, widening as batches complete. Batch rows live in the `shards` table.
- The window starts at 1 and only widens after the first batch reports, so a fresh task looks single-worker for its first batch. That is the designed ramp, not a stall.
- Pool tasks do **NOT** self-heal: the reconciler deliberately skips orphan re-dispatch for them (a dead coordinator would otherwise be inferred as `done`). `pool_orphaned` / `pool_starved` are the signals; a stuck pool task needs a human.
- A batch is forgiven up to `POOL_BATCH_FAIL_MAX`=5 permanently-failed files; a task reports `done` with a WARNING if missing files are within `max(10, 0.5% of listed)`, else `failed`. Missing files are queryable, never silent.
- Restart is lossless at any shard/batch count — the BOS filter re-skips whatever is already uploaded.

- Sharded (legacy): 1 task = N shards = N workers. `ShardedDownloadWorkflow` (coordinator) → lists repo files → **BOS resume filter** (drops files already at target prefix with matching key+size) → greedy partition → `ShardWorkerWorkflow` children on per-worker queues.
- The coordinator + `list_repo_files`/`filter_filelist_against_bos`/`partition_files_greedy` are pinned to the listing worker's personal queue (the filelist lives on its local disk).
- Shards download to `/data/staging/{task}/shard-N/` but upload to the task's FLAT BOS prefix (never `shard-N/` keys).
- Batch resume markers (`.progress.json`) are md5-guarded against the shard filelist — a re-partition invalidates them; the BOS filter makes any restart lossless.
- `auto_dispatch_pending()` runs every 30s: claims a pending task (status only, `server=NULL`), starts one coordinator per source per cycle. Source routing: `modelscope → bj*`, `hf → w*`. Guard: a source with a coordinator still in listing phase (downloading task, zero shard rows, fresh `claimed_at`) is skipped.

- Speed metrics measure download activity (staging growth + uploads), not uploads alone.
- Progress reports can NOT resurrect paused/preempted/revoked/done tasks (guard in `/api/task-progress`).

## Server Topology

| Key | IP | Role |
|-----|-----|------|
| S1 | 154.85.43.52 | Temporal server, web, SQLite, jump host |
| w1-w7 | 156.240.120.209, 154.85.53.152, 154.85.49.95, 154.85.40.244, 154.85.54.251, 154.85.50.210, 156.240.121.60 | HF workers (HK) |
| bj1-bj4 | 120.48.57.202, 180.76.182.215, 120.48.21.57, 180.76.228.120 | ModelScope workers (BJ) |
| bj5-bj9 | 120.48.56.197, 120.48.174.216, 120.48.79.251, 120.48.142.8, 106.12.159.208 | ModelScope workers (BJ, BCC) |

SSH from S1 to all workers with pubkey. From dev machine, use S1 as jump host; `dlm.core.ssh.ssh_exec(host, "root", cmd)` runs on S1 handles fan-out.

## Common Commands

```bash
# Tests (696 as of 2026-08-09). No venv is checked in and neither the dev box
# nor S1 has pytest installed; build a throwaway one:
python3.12 -m venv /tmp/dlm-test-venv
/tmp/dlm-test-venv/bin/pip install pytest fastapi temporalio bce-python-sdk \
    python-dotenv huggingface_hub requests pycryptodome
/tmp/dlm-test-venv/bin/python -m pytest tests/ -q   # run from the repo root
```

```bash
# Deploy code + restart workers (from S1; prints md5 version manifest — must all match)
bash scripts/deploy-workers.sh                 # all 16
bash scripts/deploy-workers.sh --worker bj1    # subset
bash scripts/deploy-workers.sh --no-restart    # sync only

# Web server (on S1): systemd-managed since 2026-08-02 — never launch by hand
systemctl restart dlm-web         # (re)start; logs still at /var/log/dlm-web.log
systemctl status dlm-web dlm-web-watchdog.timer
# The watchdog timer probes /api/dashboard every 30s and auto-restarts a
# wedged (alive-but-unresponsive) web; its log: /var/log/dlm-web-watchdog.log

# Worker restart: ALWAYS via deploy-workers.sh, never ad-hoc ssh setsid (dies with the session)

# Create a task. Under pool mode (the default) the batch count is automatic —
# shard_count only applies to dispatch_mode=sharded.
curl -X POST http://154.85.43.52:8080/api/queue/add -H 'Content-Type: application/json' \
  -d '{"repo_id":"org/name","name":"X","category":"manipulation","source":"modelscope","priority":0,"shard_count":6}'

# Change shard count (sharded only; lossless restart — BOS filter skips uploaded files)
curl -X POST http://154.85.43.52:8080/api/queue/reshard -H 'Content-Type: application/json' \
  -d '{"task_id":"t-...","shard_count":8}'

# Re-dispatch a failed/revoked/paused task (reshard REFUSES failed). Terminates
# workflows first, drops stale batch rows, keeps name/category + dispatch_mode.
curl -X POST .../api/queue/retry -d '{"task_id":"t-..."}'

# Pause (resumable) / resume / revoke
curl -X POST .../api/queue/pause  -d '{"task_id":"t-..."}'
curl -X POST .../api/queue/resume -d '{"task_id":"t-..."}'
curl -X POST .../api/tasks/{id}/skip        # terminate workflows, THEN revoke (refuses 502 if they don't close;
                                            # ?force=true revokes anyway. Same for DELETE /api/tasks/{id})

# Inspect
curl .../api/dashboard ; curl .../api/tasks/{id}/shards ; curl .../api/doctor

# 搬运（BOS→地瓜云）。全部用法见 docs/runbooks/transfer.md
curl .../api/transfer                                  # 每行的 transfer_status + summary + paused
curl -X POST .../api/transfer/{task_id}/retry           # 重排一个 blocked/short/failed
curl -X POST .../api/transfer/pause -d '{"paused":true}'   # 停发新的（在飞的照常跑完并验证）
python3 scripts/transfer_import.py --execute --only NAME   # 手动车道（干跑是默认）
```

## Hard Constraints

- **NEVER delete downloaded data on BOS** without explicit user approval — staging cleanup only for done/skipped/failed tasks.
- **System-level ops only**: everything through the API / Temporal. If the framework can't do it, upgrade the framework — never hand-launch download processes.
- **No mixed code versions**: before any batch operation, verify all workers match S1 (deploy script's md5 manifest). SQLite is the single state source.
- Task `name` + `category` determine the BOS prefix — a resume task MUST reuse the exact original name/category or the resume filter matches nothing.
- Workflow code (`workflows.py`) must stay deterministic; restarting workers is only replay-safe if workflow definitions are unchanged since the running workflows started.
- **Every activity call must pass every declared parameter** — never rely on an activity's default from a workflow. temporalio drops ALL argument type hints when the parameter count differs from the payload count, so one omitted optional argument delivers a `TaskInput` dataclass as a raw dict and the activity dies on `.name`. This broke the first pool dispatch (`chunk_filelist`). Enforced by `tests/test_activity_arity.py`; replay stubs must match the real signatures (a 2-vs-3 stub kept 33 replay tests green through it).

## Code Structure

```
dlm/
├── cli.py              # Click CLI (add, ls, status, server, migrate, init, web)
├── constants.py        # BOS bucket names, paths
├── core/               # bos.py, ssh.py, parser.py, config.py, servers.py, models.py
│                       # (state.py = legacy, only migrate scripts import it)
├── queue/
│   ├── snapshot.py     # SQLite state — tasks/shards/workers tables (THE state source)
│   └── app.py          # Celery app (transfer tasks only; downloads are Temporal)
├── temporal/
│   ├── __main__.py     # Worker entry: python3 -m dlm.temporal --server-key X [--task-queue Q]
│   ├── workflows.py    # DownloadDataset (legacy, unreachable) / Sharded (coordinator) / ShardWorker
│   ├── activities.py   # list_repo_files, filter_filelist_against_bos, partition, run_pipeline_batch...
│   ├── pipeline.py     # PipelineEngine — parallel download+upload, disk backpressure, speed reporter
│   └── models.py       # TaskInput (shard_count), ShardInput (filelist_md5), ...
├── web/
│   ├── app.py          # FastAPI factory
│   ├── scheduler.py    # 30s dispatch loop + 300s reconcile loop
│   ├── reconciler.py   # auto_dispatch_pending, orphan re-dispatch, stale-speed zeroing
│   ├── temporal_client.py  # start_sharded_download, cancel/terminate (task_id substring sweep)
│   └── routes/         # queue (add/pause/resume/reshard/shards/*), tasks, dashboard, servers, storage, doctor
└── worker/             # disk.py, errors.py, handlers/, movers/ (daemon deleted — Temporal only)
scripts/
├── deploy-workers.sh   # rsync + restart + md5 version manifest (all 16 hosts + queues)
├── start-temporal-worker.sh  # direct Temporal connect w/ tunnel fallback, DLM_TASK_QUEUE
└── merge_beta_to_bj.py # one-shot BOS merge (server-side multipart copy, MS size gate)
```

## OpenClaw Integration

DLM skill lives in `skills/dlm/` (SKILL.md + references/) and is deployed to S1 at `/root/.openclaw/workspace/skills/dlm/`. Update flow: edit in repo → `scp -r skills/dlm root@154.85.43.52:/root/.openclaw/workspace/skills/`.

## Environment Variables

Required in `.env` on each server:
```
BAIDU_AK=... BAIDU_SK=...
BOS_ENDPOINT=https://bj.bcebos.com
HF_TOKEN=...                    # gated repos
MODELSCOPE_API_TOKEN=...        # or MS_TOKEN
TEMPORAL_HOST=154.85.43.52:7233 # workers default to this
DLM_COORDINATOR=http://154.85.43.52:8080
```
