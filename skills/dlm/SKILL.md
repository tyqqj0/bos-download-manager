---
name: dlm
description: |
  Manage DLM download tasks — add datasets from HuggingFace/ModelScope, check cluster status, run health checks, handle stuck tasks, monitor workers. Use when the user mentions: downloading datasets, DLM tasks, download progress, worker status, cluster health, disk space, adding a HuggingFace/ModelScope repo, data pipeline.
---

# DLM — Dataset Download Manager

Operate the DLM cluster: 7+ worker nodes downloading TB-scale datasets from HuggingFace/ModelScope and uploading to Baidu BOS. All operations go through the S1 web API.

## Coordination

- **API base**: `http://154.85.43.52:8080`
- **Source routing**: HuggingFace repos → `w*` workers (Hong Kong), ModelScope repos → `bj*` workers (Beijing). Never cross-assign.
- All state lives in S1's SQLite, exposed via the API. Never write state directly.

## Branches

Six branches, each with a distinct trigger. Pick the matching branch and follow its steps.

---

### Branch: add

**Trigger**: user wants to download a dataset or model ("download org/repo", "add this to the queue", a HuggingFace or ModelScope URL).

**Hard rules**:
- A link or repo_id (org/name) is mandatory. If missing, ask for it — never guess.
- Only HuggingFace (`huggingface.co`, `hf.co`) and ModelScope (`modelscope.cn`) are accepted.
- Category is required. Valid: `embodiment`, `reasoning`, `multimodal`, `language`, `video`, `other`.

**Steps**:

1. Extract the repo identifier from the user's input. Normalize: strip URL prefix to get `org/name`, detect source (`hf` or `modelscope`).

2. Validate the repo by running `{baseDir}/references/validate-repo.sh <url_or_repo> [source]`. Check the output:
   - `EXISTS=true` → proceed
   - `EXISTS=false` → report "repo not found" and stop
   - `TYPE=dataset|model` → auto-fill the type field
   - `GATED=true` → warn the user that HF_TOKEN is required

3. If the user didn't specify category, ask for it.

4. Create the task:
   ```bash
   curl -s -X POST http://154.85.43.52:8080/api/tasks \
     -H 'Content-Type: application/json' \
     -d '{"url_or_repo":"<repo>","category":"<cat>","type":"<dataset|model>","priority":"P1"}'
   ```

5. Report the result: task ID, name, status. Confirm "auto_dispatch will assign it to an idle worker".

6. If creation fails, report the error clearly.

---

### Branch: status

**Trigger**: user asks about download progress, speed, active tasks ("how's it going", "download status", "what's running").

**Steps**:

1. Fetch dashboard:
   ```bash
   curl -s http://154.85.43.52:8080/api/dashboard
   ```

2. Present a concise summary:
   - Total speed (aggregate_download_speed_mbps)
   - Active downloads: name, server(s), speed, progress %, shard info
   - Worker status: online/offline, disk free
   - Any alerts

---

### Branch: list

**Trigger**: user wants to see the task list or filter tasks ("show all tasks", "what's pending", "failed tasks").

**Steps**:

1. Fetch tasks:
   ```bash
   curl -s "http://154.85.43.52:8080/api/tasks?status=<filter>"
   ```

2. Present as a table: name, status, server, size, speed, shards.

---

### Branch: doctor

**Trigger**: user asks for health check or mentions problems ("run doctor", "anything broken", "health check").

**Steps**:

1. Fetch health report:
   ```bash
   curl -s http://154.85.43.52:8080/api/doctor
   ```

2. **Verify before reporting.** `/api/doctor` reads a cached snapshot refreshed
   every 5 minutes, and a task's `speed_mbps` is a 15-second sample. Never
   report "stuck" or "speed 0" from a single reading — confirm against
   shard-level truth first:
   ```bash
   curl -s http://154.85.43.52:8080/api/tasks/<task_id>/shards
   ```
   A task is genuinely progressing when its shards' `done_bytes` grow and
   `updated_at` is fresh (<60s), even if `speed_mbps` momentarily reads 0.
   Sharded tasks legitimately have `server: null` at the task level — the
   servers are on the shards.

3. Analyze issues. For each category:
   - **stuck_tasks**: no `updated_at` change for >30 min. Confirm via shards
     before reporting; suggest reset only if shards are also frozen.
   - **offline_workers**: no heartbeat for >3 min. Confirm the worker process
     is really gone before suggesting a restart.
   - **disk_full**: <10GB free. Warn, never auto-clean.
   - **idle_workers**: reported ONLY when pending work exists for that source.
     Workers listed under `idle_workers_no_pending_work` are resting normally
     — not an issue, do not report them as one.
   - **failed_repeat**: retry_count >= 5. Suggest skip.

4. If the user approves fixes, apply:
   ```bash
   curl -s -X POST http://154.85.43.52:8080/api/doctor \
     -H 'Content-Type: application/json' \
     -d '{"actions":["reset_stuck","restart_dead","skip_zombie"]}'
   ```

5. Never auto-clean disk without explicit user approval.

---

### Branch: manage

**Trigger**: user wants to perform a specific action ("reset task X", "skip this task", "restart w3", "clean staging on w7").

**Steps**:

1. Identify the action and target from the user's message.

2. Available actions:
   - Pause task (resumable): `POST /api/queue/pause` body `{"task_id": "<id>"}`
   - Resume task: `POST /api/queue/resume` body `{"task_id": "<id>"}`
   - Change shard count (lossless restart): `POST /api/queue/reshard` body
     `{"task_id": "<id>", "shard_count": N}`
   - Jump the queue: `POST /api/queue/jump` body `{"task_id": "<id>"}`
   - Reset task: `POST /api/tasks/<id>/reset`
   - Revoke task (also terminates its workflows): `POST /api/tasks/<id>/skip`
   - Retry task: `POST /api/tasks/<id>/retry`
   - Cancel + delete: `DELETE /api/queue/<id>`
   - Cleanup staging: `POST /api/servers/<key>/cleanup`
   - Restart a worker: the ONLY supported path is the deploy script on S1.
     Never hand-launch a worker over SSH — a `setsid`/`nohup` one-liner dies
     with the SSH session, and `python3 -m dlm.worker.daemon` no longer exists
     (the daemon architecture was removed; workers run `python3 -m dlm.temporal`).
     ```bash
     ssh root@154.85.43.52 'cd /root/code/bos-download-manager && bash scripts/deploy-workers.sh --worker <key>'
     ```
     Verify with `ps aux | grep "python3 -m dlm.temporal" | grep -v grep`
     (a bare `pgrep -f dlm.temporal` matches the check command itself).
     Restarting a worker costs its in-flight batch a ~10 min heartbeat-timeout
     retry — never restart during a healthy download without a reason.

3. Confirm before executing destructive actions (skip, cancel, cleanup, restart).

---

### Branch: heartbeat

**Trigger**: automatic — runs on OpenClaw's heartbeat cycle (every 60 min). Not user-invoked.

**Steps**:

1. Fetch doctor and dashboard:
   ```bash
   curl -s http://154.85.43.52:8080/api/doctor
   curl -s http://154.85.43.52:8080/api/dashboard
   ```

2. Check for issues, applying the verification rule from the doctor branch —
   a single 0-speed sample is not evidence of a problem:
   - Stuck tasks: `updated_at` frozen >30 min AND shards also frozen.
   - Offline workers: no heartbeat >3 min (doctor already de-duplicates
     multiple heartbeat rows per worker; trust its `offline_workers`).
   - Disk: `disk_full` entries (<10GB free).
   - Idle workers: only those in `idle_workers` (pending work exists for them),
     never those in `idle_workers_no_pending_work`.

3. If all clear: brief one-line "DLM healthy: N tasks active, X Mbps total".

4. If issues found: report each with severity, the evidence you verified, and
   a suggested action. Do not propose commands you have not read in this skill.

---

## Notification

When operating from a Feishu group chat, the agent's reply is the notification. For programmatic alerts from DLM itself (outside agent context), use:

```bash
# Fire-and-forget alert (no reasoning)
openclaw message send --channel feishu --target "<chat_id>" -m "DLM Alert: <message>"

# Agent-reasoned alert (with analysis)
openclaw agent -m "DLM alert: <details>. Check /api/doctor and report." \
  --deliver --reply-channel feishu --reply-to "<chat_id>"
```

## Hard constraints

- **Never delete data on BOS** — only clean `/data/staging/` from done/skipped/failed tasks.
- **Never auto-clean disk** without explicit user approval.
- **System-level operations only** — everything goes through the API or the
  deploy script. If the framework cannot do something, say so; never work
  around it with hand-run processes or direct DB/BOS writes.
- **Verify before alarming** — cross-check task-level metrics against
  `/api/tasks/<id>/shards` before calling anything stuck or dead. Cached
  snapshots and 15-second speed samples produce false negatives routinely.
- Pending tasks are picked up automatically by auto_dispatch every 30s; a task
  sitting in `pending` briefly is normal, not stuck.
- ModelScope downloads are unreliable for large repos — suggest skip if genuinely stuck >2h.
