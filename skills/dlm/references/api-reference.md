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
| GET | `/api/tasks/{id}` | Get single task |
| POST | `/api/tasks/{id}/reset` | Reset stuck task to dispatched |
| POST | `/api/tasks/{id}/skip` | Skip task |
| POST | `/api/tasks/{id}/retry` | Retry failed task |
| DELETE | `/api/queue/{id}` | Cancel workflow and delete task |

## Shards

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/shards?task_id=X` | List shards for a task |
| GET | `/api/shards/idle-workers?source=hf` | Idle workers by source |
| POST | `/api/shards/aggregate` | Force re-aggregate shard progress to task |

## Health

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/doctor` | Health check report |
| POST | `/api/doctor` | Auto-fix. Body: `{"actions":["reset_stuck","restart_dead","skip_zombie"]}` |

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
