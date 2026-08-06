# DLM API Reference

Base URL: `http://154.85.43.52:8080`

## Dashboard

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/dashboard` | Cluster summary: active downloads, workers, speeds, alerts |

Response fields: `total_tasks`, `by_status`, `aggregate_download_speed_mbps`, `aggregate_upload_speed_mbps`, `active_downloads[]`, `workers[]`, `alerts[]`.

Each active download: `id`, `name`, `server`, `speed_mbps`, `progress_pct`, `downloaded_gb`, `size_gb`, `total_shards`, `done_shards`, `shard_servers[]`.

## Tasks

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/tasks` | List all tasks. Params: `status`, `server`, `category`, `sort`, `reverse` |
| POST | `/api/tasks` | Create task. Body: `url_or_repo`, `category`, `type`, `priority`, `name?`, `size_gb?`, `no_dispatch?` |
| POST | `/api/queue/add` | Create task (queue-native). Body: `repo_id`, `name?`, `type`, `category`, `source`, `priority` (0=jump queue), `shard_count` (0=auto) |
| GET | `/api/tasks/{id}` | Get single task (incl. `resume_skipped_files/gb` — BOS resume filter evidence) |
| POST | `/api/tasks/{id}/skip` | Terminate workflows, then revoke. 502 (state unchanged) if they don't close; `?force=true` revokes anyway |
| POST | `/api/tasks/{id}/retry` | Requeue a failed/revoked/paused/pending task |
| POST | `/api/queue/pause` | Pause (durable — progress reports cannot resurrect). Body: `task_id` |
| POST | `/api/queue/resume` | Requeue a paused task. Body: `task_id` |
| POST | `/api/queue/reshard` | Change shard count via lossless restart — terminates workflows first, refuses if they don't close. Body: `task_id`, `shard_count` |
| POST | `/api/queue/preempt` | Pause a running task, start an urgent one. Body: `urgent_task_id`, `victim_task_id?` |
| DELETE | `/api/queue/{id}` | Terminate workflows, THEN delete task (refuses 502 and deletes nothing if they don't close; `?force=true` deletes anyway) |

## Shards

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/tasks/{id}/shards` | List shards for a task |
| GET | `/api/shards/idle-workers?source=hf&exclude_task=X` | Idle workers by source |
| POST | `/api/shards/aggregate` | Force re-aggregate shard progress to task |

## Health

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/doctor` | Health check report |
| POST | `/api/doctor` | Auto-fix. Body: `{"actions":[...]}` — only `redispatch_orphaned`, `reset_stuck`, `skip_zombie` are supported; others come back as `unsupported_actions` |

## Servers

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/servers` | Server list with status |
| POST | `/api/servers/{key}/cleanup` | Clean staging on a server |

## Categories

`embodiment`, `reasoning`, `multimodal`, `language`, `video`, `other`

## Priorities

`P0` (urgent), `P1` (normal), `P2` (low), `P3` (background)

## Source routing

- `hf` → `w*` workers (Hong Kong nodes)
- `modelscope` → `bj*` workers (Beijing nodes)
