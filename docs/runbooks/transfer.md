# 搬运运行手册（BOS → 地瓜云）

一句话：下载任务报 `done` → 系统自己 arm → web 的 scheduler 自己发 import →
落地后自己验证 → `done` 或 `short`。**平时不需要人动手。** 需要人的只有一种情况：
`blocked` / `short` / `failed`，它们不会自愈。

远端 import 本身的监控由地瓜云那边负责。这里只做我们这一侧的账。

---

## 状态机（`tasks.transfer_status`）

```
NULL ──arm──> ready ──dispatch──> transferring ──远端成功──> verifying ──验证──> done
                                                                            └──> short
  │                                        └──远端失败──> failed
  └──闸门拒绝──> blocked
```

| 状态 | 含义 | 要不要管 |
|------|------|----------|
| `NULL` | 还没排队（任务没 done，或自动搬运关了） | 不用 |
| `ready` | 已排队，等额度 | 不用，最多等一分钟 |
| `transferring` | 远端 import 在跑 | 不用 |
| `verifying` | 远端说完了，我们在数字节 | 不用，几分钟 |
| `done` | **数过了**：地瓜云字节 ≥ 派发时 BOS 字节，且没多出目录 | 不用 |
| `short` | 远端说成功，但字节不够 / 多出目录 | **要**，见下 |
| `failed` | 远端 import 自己失败了 | **要**，见下 |
| `blocked` | 闸门认为这个 `done` 不可信，一个字节都没发 | **要**，先查为什么 |

任务自己的 `status` 永远不被搬运代码改写。下载状态和搬运状态是两件事。

一个例外要知道：远端任务列表只返回最新 500 条，而那边已有 672 条，所以**跑得久的
import 会从列表里掉出去**。这时我们不猜，改去量目的端：量够了就 `done`，不够就**留在
`transferring`** 并把差额写进 `transfer_error`（写成 `remote record not found; ...`）。
所以看到一个 `transferring` 带 `remote record not found` 是正常的——那是"还在拷或者
已经死了，我们只知道现在还不够"。它每轮都会重新量。真的死了会一直停在那，
`/api/transfer` 上一眼能看出来。**不会**因为找不到记录就判 `short`：那会诱导人去对
一个还在写的目录再发一个 import。

## 看现在什么情况

```bash
# 全量：每行一个任务 + summary（ready/transferring/short/blocked/failed 计数）+ paused
curl -s http://154.85.43.52:8080/api/transfer | python3 -m json.tool | head -40

# 只看要人管的
curl -s http://154.85.43.52:8080/api/transfer \
  | python3 -c 'import json,sys; [print(t["transfer_status"], t["name"], t["transfer_error"]) \
      for t in json.load(sys.stdin)["tasks"] \
      if t["transfer_status"] in ("blocked","short","failed")]'

# 告警（dashboard 里也有）：transfer_blocked = CRITICAL，
# transfer_short / transfer_failed / transfer_stalled = WARNING
curl -s http://154.85.43.52:8080/api/dashboard | python3 -c \
  'import json,sys; [print(a["severity"], a["type"], a["message"]) for a in json.load(sys.stdin)["alerts"]]'

# 上一轮 poll / dispatch 的原始报告（scheduler 缓存的）
grep -a "Transfer dispatch\|Transfer poll\|transfer " /var/log/dlm-web.log | tail -20
```

`transfer_error` 里写着具体原因。**先读它再动手**——blocked 的五种原因要做的事完全不同：
改名漂移（派发后被改了 name/category）、0 个 shard 行、shard 行没全 done、
BOS 前缀是空的、BOS 字节只有派发字节的一半以下。

## 要人管的三种情况

```bash
# 重新排队一个任务（failed / short / blocked 都能重排；已在飞的会被拒）
curl -X POST http://154.85.43.52:8080/api/transfer/t-xxxx/retry

# 把所有没排过队的 done 任务排上（补账用）
curl -X POST http://154.85.43.52:8080/api/transfer/trigger -H 'Content-Type: application/json' -d '{}'

# 只排指定的几个（对已排过的行是"故意重排"）
curl -X POST .../api/transfer/trigger -H 'Content-Type: application/json' -d '{"task_ids":["t-a","t-b"]}'
```

`retry` 不能强推：改名漂移和 shard 完整性两道闸门对手动同样生效。闸门说不行，
就是数据本身有问题，去查数据，不要绕闸门。

- **`short`** — 远端确实搬了东西但不够。字节还在 BOS 上，`retry` 会重发一次
  import；远端是 overwrite/skip-same 语义，补差是安全的（2026-08-03 rh20t 实验证过）。
- **`failed`** — 远端自己失败。`transfer_error` 是远端给的原话。先看是不是数据本身
  的问题（RoboDojo 的 0 字节 depth 文件那类），再决定重发。
- **`blocked`** — 一个字节都没发出去。`0 shard rows` 意味着这个 `done` 是
  reconciler 推断出来的，不是 workflow 报的，**先确认 BOS 上到底有没有数据**再说。

还有一个不是状态、是告警的情况：**`transfer_stalled`**。它说的不是某条数据有问题，
而是**我们自己的派发器不派发了**——某行 `ready` 排了 30 分钟以上，同时额度有空、
`pause` 也没开。真实原因通常是 web 挂了、搬运那一段每轮都超 600s 被放弃，或者远端
连拒我们两次触发了 `CONSECUTIVE_FAIL_LIMIT`。先看 `systemctl status dlm-web`，
再 `grep -a "transfer dispatch" /var/log/dlm-web.log | tail`。数据都还在 BOS 上，
原因一消失它自己就发出去了。

## 停 / 开

```bash
curl -X POST .../api/transfer/pause -H 'Content-Type: application/json' -d '{"paused":true}'   # 停
curl -X POST .../api/transfer/pause -H 'Content-Type: application/json' -d '{"paused":false}'  # 开
```

暂停是持久化的（存 SQLite settings），web 重启不会偷偷恢复。暂停时：不 arm、不派发；
已经在飞的 import 继续跑完并正常验证（我们停的是"发新的"，不是掐远端）。

彻底关掉自动搬运：**S1** 的 `.env` 里 `DLM_AUTO_TRANSFER=off`，`systemctl restart dlm-web`。
只有 S1 需要改——arm 和派发都只在 web 进程里跑，worker 不参与搬运。
`pause` 是临时闸，环境变量是长期开关。

## 额度和节奏（不要随手改）

| 常量 | 值 | 在哪 | 为什么 |
|------|-----|------|--------|
| `MAX_IN_FLIGHT` | 16 | `dlm/transfer/dispatch.py` | 用户 2026-08-10 定的。那边带宽几十 Gb/s，且搬运是他们在拷，我们的上限是礼貌不是吞吐 |
| `MAX_PER_CYCLE` | 4 | 同上 | 每发一个要全量扫一遍 BOS 前缀（3.4 TB ≈ 50 页 list）。一轮发 4 个，一分钟就能爬到 16 |
| `CONSECUTIVE_FAIL_LIMIT` | 2 | 同上 | 连续两次失败就是那边（或我们的凭据）在拒我们，再试 14 次只是刷日志 |
| `UNKNOWN_VERIFY_PER_CYCLE` | 2 | 同上 | 远端记录找不到的行，一轮最多量 2 个（每个都要走完两端）。计数不设上限，只限做的活 |
| `TRANSFER_STALL_SECONDS` | 1800 | `dlm/web/alerts.py` | `ready` 排了这么久还没发出去、且额度有空、且没暂停 → 是我们自己的派发器死了，报 `transfer_stalled` |
| `TRANSFER_INTERVAL` | 60s | `dlm/web/scheduler.py` | 一轮 poll+dispatch 的间隔；一轮没跑完不会叠第二轮 |
| `TRANSFER_STAGE_TIMEOUT` | 600s | 同上 | 量多 TB 前缀是几千次 list，60s 会让每一轮都被放弃、永远发不出去 |
| `RATIO_READY` / `RATIO_MIN` | 0.95 / 0.50 | `dlm/transfer/arm.py` | `DLM_TRANSFER_RATIO_*` 可覆盖。0.50 以下拒发 |

## 手动搬运（脚本，依然可用）

自动派发只处理 SQLite 里有 `done` 任务行的东西。**不在任务表里的**、
一次性的、或者闸门拒了而你已经人工核实过数据没问题的，走脚本。

```bash
cd /root/code/bos-download-manager   # S1

# 干跑（默认就是干跑，会打印每一项的两端地址和字节）
python3 scripts/transfer_import.py

# 真跑内置清单里的一项
python3 scripts/transfer_import.py --execute --only DL3DV-ALL-4K

# 自己的清单。type 缺省 dataset；src 缺省从 (type, category, name) 推导，
# 只有第一代前缀（DL3DV 在 datasets/ 下而 category 是 multimodal）才需要显式写
cat > /tmp/my.json <<'EOF'
[{"name": "Qwen3-VL-30B-A3B-Thinking", "category": "multimodal", "type": "model"},
 {"name": "DL3DV-ALL-4K", "category": "multimodal", "src": "datasets/DL3DV-ALL-4K/"}]
EOF
python3 scripts/transfer_import.py --manifest /tmp/my.json --execute

# 往一个已存在的、更小的目标上补差（默认会中止）
python3 scripts/transfer_import.py --execute --only X --allow-topup

# 第二条并行车道必须给自己的 state 文件（save_state 整体重写 JSON，共用会互相吞记录）
python3 scripts/transfer_import.py --execute --state /tmp/lane2.json --manifest /tmp/lane2-list.json
```

脚本和自动派发用**同一套**目的端映射（`dlm/transfer/targets.py`）和**同一套**测量
原语（`dlm/transfer/measure.py`），所以两边对同一个目录的判断不会打架。脚本自己也
会查远端在跑的任务，不会往已经在跑的 import 上再发一个。

## 桶和目的端布局（唯一判据）

`dlm/core/bos.py:bos_target()` 是桶的唯一判据，`dlm/transfer/targets.py` 是目的端的。
**永远不要读 `tasks.bos_path`**——实测（2026-08-10）molmobot-data 那一列存的是地瓜云
的目的路径，不是 BOS 源前缀。

| | BOS 源 | 地瓜云 目的 |
|---|---|---|
| 数据集 | `auwomo-data/{category}/{name}/` | `/727a2f92-30c/auwomo-datasets/raw-data/{category}/{name}` |
| 模型 | `auwomo-model-open/{name}/` | `/727a2f92-30c/auwomo-model/{category}/{name}` |

## 验证到底验了什么

`done` 不是"远端说成功"，是三项只读检查都过了（`dlm/transfer/verify.py`）：

1. **大小** — 地瓜云目录字节 ≥ **派发时**测到的 BOS 前缀字节。分母不是
   `SUM(shards.total_bytes)`：RoboDojo 派发 715.7 GB 而前缀上实有 6189.9 GB，
   用派发字节当分母会给只搬了十分之一的传输发 `done`。
2. **范围** — 目的端顶层子目录 ⊆ 源前缀顶层。大小检查对"搬多了"一律放行，
   只有这一项能抓前缀溢出（`RDT-1B` 不带斜杠会连 `RDT-1B-repair/` 一起拖走）。
3. **BOS 未被改动** — 源前缀字节/对象数和派发时一致。我们只读 BOS，所以不一致
   意味着别人在写。记录并写进 `transfer_error`，但**不**因此判失败。

## 需要的环境变量（S1 的 `.env`）

```
DCLOUD_USER=... DCLOUD_PASS=...   # 缺了：web 正常服务，搬运整个跳过并在报告里说
BAIDU_AK=... BAIDU_SK=...         # 远端 import 要用它去读 BOS
DLM_AUTO_TRANSFER=on              # off = 自动 arm 关闭（手动按钮仍可用）
```

## 相关代码

| 文件 | 干什么 |
|------|--------|
| `dlm/transfer/arm.py` | 纯 DB 的排队决策 + 四道闸门 + 比例带。跑在 `/api/task-progress` 里，**一个字节的外部 I/O 都不能有** |
| `dlm/transfer/dispatch.py` | scheduler 的两段：`dispatch_ready_transfers()` / `poll_transfers()` |
| `dlm/transfer/verify.py` | 上面那三项检查 |
| `dlm/transfer/measure.py` | 四个只读测量原语，脚本和自动派发共用 |
| `dlm/transfer/targets.py` | 两端地址的唯一来源 |
| `dlm/transfer/inflight.py` | 远端任务列表分类 + 重接判断（防止两个 importer 写一个目录） |
| `dlm/web/routes/transfer.py` | `GET /api/transfer`、`trigger`、`{id}/retry`、`pause` |
| `tests/test_transfer_dispatch.py` | 额度、FIFO、验证、告警的回归 |

设计与验收标准：`docs/superpowers/specs/2026-08-10-auto-transfer-design.md`
