# 控制面加固 + AgiBotWorld-Alpha 整理 — 实施计划（v2，已吸收 Plan 审阅意见）

> 状态：Plan 审阅完成（approve with changes，全部采纳）。流程：需求 → 验收标准 → Plan → ✅Agent 审 Plan → 实施 → Agent 审代码 → 部署跑数据 → 最终验收审查。

## 背景（一句话）

2026-07-31 S1 web 事件循环被 fork 死锁卡死 24 小时无人察觉；审计暴露三处结构性缺口；同时 Beta-BJ 已完成，Alpha 对账发现**数据已全部在 BOS**（分裂两前缀），需整理而非下载。

## 需求

- **R1 — web 进程受管**：systemd（`Restart=always`）+ 独立 HTTP 探活 watchdog（探的是"能不能应答"，不是"进程在不在"——本次故障进程一直"在"）。
- **R2 — 全舰队 sidecar**：bj1–bj9 安装启用 `dlm-sidecar@.service`（unit 已在仓库 `deploy/`）；`deploy-workers.sh` **每次部署都**保证 unit 装好并 enable（不再依赖 restart 分支、不再静默跳过）。
- **R3 — safe-deploy 诚实化**：`cancel-all-workflows` 改为取消**父类型** workflow（`DownloadDataset/SplitDownload/ShardedDownload`，不含 `ShardWorker`——子 workflow 由 `ParentClosePolicy=TERMINATE` 连带终止，避免触发子侧 failure handler 把任务打成终态 `failed`）。无 `confirm:true` 时 dry-run 预览。safe-deploy 只在**全舰队模式**下传 confirm，`--worker` 子集模式跳过 Phase 1 并大声提示（取消是全局的，blast radius 必须匹配脚本范围）。
- **R4 — 全集群代码一致**：全 16 台重启到当前代码。**前置门**：所有 running workflow 的 `start_time` 与最后一次动 `workflows.py` 的 commit 时间比对，早于者一律 terminate + 重新 dispatch（BOS filter 保证无损），**不做跨版本 replay**。
- **R5 — Alpha 完整可用**：对账已完成（258 文件 / 9 000.8 GiB，两前缀并集缺失 = 0）。canonical = `manipulation/AgiBotWorld-Alpha/`，差 2 个对象（45.2 GB tar 纯新增；20 KB `.gitattributes` 覆盖旧版，先做 `.bak` 服务端备份）。paused 任务 `t-20260729-cde1dc` **先 revoke 再 copy**。`other/` 下 5 567 GB 冗余副本去留由用户单独决定（默认保留）。**绝不删除 BOS 数据。**

## 验收标准（全部要实测证据）

| # | 标准 | 验证方法 |
|---|------|---------|
| A1 | `kill -9` web → ≤30s 自动拉起 200；unit 不落入 `failed`（StartLimit 已放开） | 计时 + `systemctl status` |
| A2 | `SIGSTOP` 冻结 → watchdog 在 **≤4 min** 内恢复（OnUnitActiveSec=30、3 次连续失败≈100s 检出 + TimeoutStopSec=20 兜底杀 + 启动 ≈10s）。演练前**预布防**独立解冻（`setsid sh -c 'sleep 300; systemctl kill -s SIGCONT dlm-web; systemctl restart dlm-web' &`），watchdog 失灵也不复刻 24h 事故。演练前记录 5 个在途任务分片 `done_bytes` 基线，演练后复查（进度上报是累计值，天然自愈）。watchdog 日志需含**正常周期**的心跳条目（只在失败时写日志的 watchdog 无法与没在跑的 watchdog 区分） | 演练 + 日志 + 分片字节对比 |
| A3 | 16/16 sidecar：(a) 9×bj `systemctl is-active dlm-sidecar@bjN` 全 active；(b) SQLite 中 `bjN@sidecar` 行 `last_seen` 新鲜 / `health_verify_report`（merge_workers 视图）无 `sidecar_missing`。**不用** `/api/servers`（last-wins 丢 sidecar 列）也不看 `/api/doctor`（不输出该字段） | systemctl + SQLite/缓存 |
| A4 | 全量重启后 16/16 md5 OK、worker+sidecar active、doctor healthy；Temporal UI 无 WorkflowTaskFailed（replay 失败不一定上 worker 日志） | 部署输出 + Temporal UI |
| A5 | dry-run 预览与 `/api/workflows` 一致；**并对 1 个一次性小任务实测 `confirm:true`**：父 workflow 取消、子被连带终止、任务行状态可被 orphan 重派回收（不落 `failed` 终态卡死） | dry-run 对照 + 单任务实弹 |
| A6 | copy 后复跑对账：`manipulation/` 258/258 精确匹配、MISSING=0、mismatch=0；paused 任务已 revoke；BOS 零删除 | 对账脚本 + 任务状态 |
| A7 | 对账报告归档 docs/（数据见 R5） | 文件存在 |

## 任务

### T1 — systemd 化 web + watchdog
- `deploy/dlm-web.service`：`ExecStart=/usr/bin/python3 -m dlm web --port 8080`、`WorkingDirectory=/root/code/bos-download-manager`（**load-bearing**：dotenv 从 cwd 向上找）、`EnvironmentFile=-/root/code/bos-download-manager/.env`、`Restart=always`、`RestartSec=5`、`TimeoutStopSec=20`、`StartLimitIntervalSec=0`、`StandardOutput/Error=append:/var/log/dlm-web.log`（保留运维肌肉记忆，与 dlm-worker@ 同约定）。
- `scripts/web-watchdog.sh`：`curl -m 10` 探 `/api/dashboard`；**只有 curl 退出码 7/28（连接拒绝/超时——正是事件循环卡死的形态）计失败**，HTTP 5xx 仅记日志不计数（防 DB 锁竞争误杀）；连续 ≥3 → `systemctl restart --no-block dlm-web`，计数清零 + 120s 冷却；**每小时 ≤3 次重启**，超限停手并大声记日志；正常周期每小时写一条心跳。状态文件固定路径 `/run/dlm-web-watchdog.count`（**不用** `RuntimeDirectory=`——oneshot 停止即删目录，计数永远到不了 3）。
- `deploy/dlm-web-watchdog.timer`：`OnBootSec=60` + `OnUnitActiveSec=30` + `AccuracySec=1s`（缺 OnBootSec 的纯 OnUnitActiveSec timer 永远不会首次触发）；enable 的是 **.timer**。
- 上线序：安装 units → daemon-reload → 杀 nohup 进程 → `enable --now dlm-web dlm-web-watchdog.timer` → `systemctl list-timers` 确认 NEXT 有值 → 200。
- 文档：CLAUDE.md + README.md（:90/:93 两处旧启动命令）同步改。检查首启 journal 有无 EnvironmentFile 解析告警（`.env` 若带 BOM 会跳行）；注意 `DCLOUD_*` 注入会让 transfer 轮询从休眠变活跃（预期内，勿当回归）。

### T2 — sidecar 铺满
- unit 已在 `deploy/`（与 w1 线上一致，已核对）。
- `deploy-workers.sh`：**restart 分支外**、每次部署都执行 `install -m644 $REMOTE_DIR/deploy/dlm-sidecar@.service /etc/systemd/system/`（rsync 已把文件带到位，无需 scp）+ `daemon-reload` + `enable --now`；restart 分支内保留 `systemctl restart`；`is-active` 检查前加 `sleep 3`（当前是重启后立查，垂死 unit 也显示 active）。
- 预检：9×bj `/usr/bin/python3 -c "import requests"`（unit 硬编码该解释器；bj5–9 为 BCC 单独装机，PATH python 可能不同 → 否则 crash-loop）。

### T3 — cancel-all-workflows 诚实化
- `routes/workflows.py::cancel_all`：类型 = `PARENT_WORKFLOW_TYPES`（`temporal_client.py` 定义，= WORKFLOW_TYPES 去掉 ShardWorker）；body 可缺省（`Body(default=None)`——必填 body 会让老 curl 得 422 又被 `-sf` 吞掉，恰好复刻要修的"假 0"）；无 confirm → `{"dry_run": true, "would_cancel": [...], "count": N}`。
- `safe-deploy.sh`：Phase 1 加 `-H 'Content-Type: application/json' -d '{"confirm": true}'`，去掉 `-f` 静默，校验响应含 `"dry_run": false`；`--worker` 子集模式**跳过 Phase 1** 并打印原因。
- 测试钉住：dry-run 语义、缺省 body、类型列表来自单一定义。

### T4 — 对账 ✅（结果在 R5/A7）

### T5 — 全集群重启（等 Kitchen-Demos 完成；**A1/A2 演练必须先于 T5 完成**，冻结控制面撞上滚动重启会让 A4 所有信号不可读）
- 预检门（IMPORTANT 9）：`/api/workflows` 取 start_time ↔ `git log` 最后动 `workflows.py` 的 commit 时间；早于者 terminate + 重派（无损）。
- `bash scripts/deploy-workers.sh` 全量 → A3/A4。

### T6 — Alpha 整理（用户确认后）
- 新一次性脚本 `scripts/consolidate_alpha.py`（**不改** merge_beta_to_bj.py——它带 Beta 硬编码；沿用其 `--execute` 门 + 1GB 分段 `upload_part_copy` + abort 清理，45.2 GB → 46 段，默认 dry-run）。
- 顺序：revoke `t-20260729-cbe1dc`（写错，`t-20260729-cde1dc`）→ `.gitattributes` 先备份为 `.gitattributes.bak-20260802` → copy 2 对象 → 复跑对账 → A6。

### T7 — 终审（Opus，逐条对 A1–A7 要证据）

## 数据源事实（已定稿 2026-08-02）

- ModelScope `agibot_world/AgiBotWorld-Alpha`（下划线）：存在、无 gating，直接列出 258 文件 / 9 000.8 GiB（页面自报 9.66 TB 即此数的十进制换算）。
- HF `agibot-world/AgiBotWorld-Alpha`：gated="auto"（401 实测）。无需下载故无关。
- GitHub 官方 8.5 TB 为扩容前旧数。paused 任务 repo_id 已是正确下划线 org。

## 风险（v2 修订）

- **SIGSTOP ≠ 普通重启**（审阅纠正）：SIGSTOP 下内核仍完成 TCP 握手进 backlog，客户端等满整个超时（worker 心跳 `requests.post(timeout=5)` 直接跑在 worker 事件循环上，期间近乎循环阻塞）；且 `run_pipeline_batch` 的 10 min heartbeat_timeout 是硬上限——**演练冻结严格 ≤5 min**，由预布防 SIGCONT 强制兜底。24h 事故已证明此形态不丢下载数据；短暂 ActivityTaskTimedOut 突发是预期现象，勿当回归。
- 全集群重启打断在途批次（各 ~10 min 心跳超时重试）——已知成本。
- copy 总量仅 45.2 GB 服务端流量,风险极低;`.gitattributes` 覆盖有 .bak 兜底。
