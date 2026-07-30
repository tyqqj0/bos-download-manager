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

2. Analyze issues. For each category:
   - **stuck_downloads**: tasks downloading but 0 speed for >30 min. Suggest reset.
   - **dead_workers**: workers not seen for >3 min. Suggest SSH restart.
   - **disk_warnings**: workers with <50GB free. Warn, never auto-clean.
   - **zombie_tasks**: tasks stuck with high retry count. Suggest skip.

3. If the user approves fixes, apply:
   ```bash
   curl -s -X POST http://154.85.43.52:8080/api/doctor \
     -H 'Content-Type: application/json' \
     -d '{"actions":["reset_stuck","restart_dead","skip_zombie"]}'
   ```

4. Never auto-clean disk without explicit user approval.

---

### Branch: manage

**Trigger**: user wants to perform a specific action ("reset task X", "skip this task", "restart w3", "clean staging on w7").

**Steps**:

1. Identify the action and target from the user's message.

2. Available actions:
   - Reset task: `POST /api/tasks/<id>/reset`
   - Skip task: `POST /api/tasks/<id>/skip`
   - Retry task: `POST /api/tasks/<id>/retry`
   - Cancel task: `DELETE /api/queue/<id>`
   - Cleanup staging: `POST /api/servers/<key>/cleanup`
   - Restart worker: user must SSH manually — provide the command:
     ```
     ssh <server_ip> 'tmux kill-session -t dlm-worker 2>/dev/null; tmux new-session -d -s dlm-worker "set -a && source /root/.env && set +a && cd /root/code/bos-download-manager && python3 -m dlm.worker.daemon --server-key <key>"'
     ```

3. Confirm before executing destructive actions (skip, cancel, cleanup).

---

### Branch: heartbeat

**Trigger**: automatic — runs on OpenClaw's heartbeat cycle (every 60 min). Not user-invoked.

**Steps**:

1. Fetch doctor and dashboard:
   ```bash
   curl -s http://154.85.43.52:8080/api/doctor
   curl -s http://154.85.43.52:8080/api/dashboard
   ```

2. Check for issues:
   - Any stuck downloads (0 speed >30 min)?
   - Any dead workers (offline >3 min)?
   - Any disk warnings (<50GB free)?
   - Aggregate speed = 0 but tasks are "downloading"?

3. If all clear: brief one-line "DLM healthy: N tasks active, X MB/s total".

4. If issues found: report each issue with severity and suggested action.

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
- Workers only pick up `status == "dispatched"` — reset stuck tasks to this status.
- ModelScope downloads are unreliable for large repos — suggest skip if stuck >2h.
