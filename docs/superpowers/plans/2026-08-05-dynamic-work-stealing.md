# 动态均衡(work-stealing)下载调度 — 设计方案 v2

状态:**已过三视角评审并修订**(需求覆盖 / Temporal 正确性 / 运维迁移,各一名 Opus 评审)
日期:2026-08-05(v1 → v2 同日修订)
评审结论:方向与"批次=shards 行 + 共享池队列 + 窗口限流"骨架被三方确认可行;
v1 有 5 组 BLOCKER 级漏洞,已全部在本版闭合(见 §6 修订记录)。

环境事实(评审实测,非假设):temporalio SDK 1.30.0,Temporal Server 1.29.1。
`workflow.wait`/`workflow.as_completed`/`Priority(priority_key)` 可用;
`workflow.Semaphore` 不存在。pyproject 需钉 `temporalio>=1.9`。

## 0. 背景与动机

现状:任务派发时静态切成 N 个 shard,每 shard 绑死一台机器。后果:多任务重叠
机器不均衡;单机挂掉整 shard 卡死到 1800s 孤儿重派发全量重来;收尾阶段快机闲置
慢机拖尾;(注:web 添加任务默认也会自动分片,>10GB 即多 shard —— v1 里
"默认 1 shard"的说法不准确,真正缺陷是 idle_workers 在认领时刻一次性采样。)

目标:静态分区 → 拉取式共享队列,均衡由拉取行为涌现。

## 1. 需求(验收标尺,与 v1 一致)

- **R1** 建任务不选分片数;派发即自动利用池内全部可用机器
- **R2** 同池多任务自动均衡;任务收尾释放的机器立即被其他任务利用
- **R3** 优先级有效;pause/resume/skip/preempt 语义完整保留
- **R4** 弹性容错:单机故障只损失在跑批次;机器恢复自动参与
- **R5** 无损断点:BOS filter 语义不变;批次可安全重试不重传
- **R6** 源路由不变(HF→w*,MS→bj*)
- **R7** 磁盘背压保留;每机并发批次硬上限
- **R8** SQLite 唯一状态源;TERMINAL 守卫;BOS 前缀规则不动
- **R9** workflows.py 确定性;新旧并存灰度;回滚可行
- **R10** 可观测性不降级;orphan 恢复在新形态有等价物

## 1.5 验收规则(A1-A10,与 R 一一对应,全部可执行)

- **A1**(R1)web 新建任务不填分片 → 派发后 5 分钟内,池内全部存活机器各有
  1 个 running 批行(SQL 可查);dashboard 显示全员活跃
- **A2**(R2)双任务实测:A 独占池后启动同源 B → 10 分钟内 B 获得 ≥1 批;
  A 完成后 5 分钟内其机器全部被 B 接管;稳态时两任务 running 批行数之和=池大小
- **A3**(R3)P0+P1 并行:稳态 P0 running 批数 > P1(窗口比生效);
  pause 后 60s 内该任务 0 running 批行、无新批、进度报文停;resume 无损
  (BOS filter 跳过已传,对象数不涨即无重复);skip 后 temporal describe
  确认协调器 closed
- **A4**(R4)试跑中 kill 一台 worker 进程 → 批次经心跳超时+退避被他机接走
  (批行 server 变更为证),任务最终 done;机器恢复后下一窗口重算周期重新领批
- **A5**(R5)故意 terminate 再 resume 一次 → 终态后 BOS 对象数+字节与源清单
  一致(RoboDojo 式全量核对),对象数不超(无重复上传)
- **A6**(R6)describe_task_queue:pool-hf 轮询者仅 w*,pool-ms 仅 bj*
- **A7**(R7)任一时刻单机 running 池批行 ≤1(SQL);低盘机器不接批
  (重试退避日志为证);背压日志正常出现
- **A8**(R8)单测+实测:终态任务收到迟到 assign/status/progress → 状态不变
- **A9**(R9)CI replay 测试通过(存量 history 回放);两阶段部署清单 16/16
  全绿;回滚演练:半程 pool 任务 reshard 回 sharded 并成功续跑
- **A10**(R10)试跑期 doctor/alerts 零误报;批次聚合视图正常;
  停掉全池 worker → 15 分钟内池巡检告警触发
- **A0(硬约束)**:实施与灰度全过程,存量下载任务(尤其 Egocentric-100K)
  不被终止;所有部署走无损重启;验收失败即回滚,不带病切默认

## 2. 核心设计(v2)

### 2.1 骨架(不变)

每任务一个 `PoolDownloadWorkflow` 协调器:list → BOS filter →切批(BOS 中转)
→ 批次=shards 表一行 → 滑动窗口把 `run_pool_batch` 活动投进共享池队列
`pool-hf` / `pool-ms`;每机池并发=1。旧 ShardedDownloadWorkflow /
ShardWorkerWorkflow **字节不动**(replay 安全充分条件)。

### 2.2 队列与 Worker(修订)

- 新增第三个 Worker 对象轮询 `pool-{source_for_worker(key)}`,
  **`workflows=[]`**、`activities=[run_pool_batch]`、`max_concurrent_activities=1`
  (防协调器落进池队列吃掉唯一槽位)。
- 池队列名由 worker 进程内部从 `fleet.source_for_worker()` 推导,deploy 脚本
  不加参数;**代价:`dlm/temporal/__main__.py` 必须进 md5 清单**(见 §3)。
- 共存期一台机器可能同时跑:1 池批次 + 旧 shard 活动(私有队列 cap=2)。
  §3 的灰度顺序以此为约束:**旧任务清空前不把默认模式切到 pool**,
  且 run_pool_batch 磁盘预检把此期间的并发算进余量。

### 2.3 协调器:确定性窗口循环(重写,评审给出的骨架)

关键规则(每条都是评审验证过的确定性/正确性要求):

1. **`workflow.start_activity()`(不 await)拿 handle**;`await` 会串行化整个窗口。
2. **等待用 `workflow.wait(list, FIRST_COMPLETED)`**,严禁 `asyncio.wait`
   (返回真 set,迭代序随机 → 每次部署后 replay 必炸 NonDeterminismError)。
3. 处理完成 handle 时**按批号排序遍历**,对每个 done 的 handle 调
   `h.result()` 并 try/except 收集 —— 未捕获的 ActivityError 会炸掉工作流
   并连带取消所有在飞批次("收集后继续"必须显式)。
4. 活动超时三件套:**`schedule_to_close=48h`**(唯一覆盖排队+全部重试的
   计时器,防空池无限静默)、`start_to_close=12h`(单次尝试上界)、
   `heartbeat=10min`;不设 schedule_to_start(重试只会回到同一条空队列)。
5. `cancellation_type=WAIT_CANCELLATION_COMPLETED`(pause 后 reshard 重派发
   不与残余上传者重叠)。
6. **取消清理必须 `asyncio.shield`**:`except asyncio.CancelledError:` →
   shielded 活动把在飞批行重置为 `pending, server=NULL, speed=0` → re-raise。
7. 批次结果记账与窗口重算合并为**每醒一次调一个活动**
   `record_batches_and_window(task_id, results) -> window`:
   - 写 done/failed 批行状态(终态只由协调器写,见 2.4-c);
   - 重算窗口:`window_i = max(1, floor(P × W_i / Σ W_active))`,
     P = **该 source 存活机器数**(新活动 `pool_alive_workers`,
     alive ∩ worker_serves,**不看 busy/磁盘** —— v1 若误用
     query_idle_workers,忙池返回 [] → window=0 → 死锁);
     W_P0=1.5, W_P1=1.0,Σ 按当前活跃任务算。
   - 动态窗口顺带修复三件事:v1 公平性算术错误(窗口都 ≥P 时永不约束,
     实际份额 50/50 而非 W 之比);窗口冻结导致恢复机器加不进在跑任务;
     多任务准入后各自满窗的失控(Σ≈P 有界)。
   - 事件预算:此合并让每批 history 成本 ~12 → ~6 事件,
     1,500 批 < 10k 警戒线;**批数上限从 400 放宽到 1,500,
     32GiB 单批体积上限为主约束不放宽**(Beta 22.4TB = 717 批,可容纳;
     v1 的 400 批上限会被迫把单批撑到 57GiB,破坏 K2 的浪费上界)。
8. 任务终态:窗口循环末尾对 failed 批次**整体补投一轮**(防毒机连吃
   3 次重试杀任务);补投后仍失败 → 任务 failed(部分完成字节照记)。
9. 优先级:`start_activity(priority=Priority(priority_key=…))` 作为队列内
   免费 tie-break(server 1.29.1 属预发布特性,**上线前用双任务实测,
   无效也无害**);真正的公平机制是窗口,不依赖它。

### 2.4 run_pool_batch(修订)

- **(a) 进场顺序固定**:`rm -rf` 自己的 staging 批目录(残留 GC)→
  `assign_shard_server` + `update_shard_status(running)`(**先记名再干活**,
  这是 busy_servers / 看板 / 巡检的唯一信号源,v1 漏了 status 导致全链失明)
  → 磁盘预检 → 从 BOS 拉批清单 → PipelineEngine.run。
- **(b) HEAD-skip 无条件启用**(≤500 个 HEAD,秒级):v1 的 "attempt≥2 才开"
  在 resume/reshard/重派发时 attempt 归 1,保护失效;无条件后
  "浪费上界 = 未上传的在飞部分",比 v1 的"一批"更强。
- **(c) 异常不写 failed**:失败仅 raise 交给 Temporal 重试;终态 failed 由
  协调器在最终尝试后写(否则 reconciler 的 all-done+any-failed 自动完结
  会在 Temporal 还在重试时把任务打成终态 failed)。
- **(d) 毒机缓解**:重试 `initial_interval=5min`(给别的机器腾槽位窗口,
  Temporal 无反亲和,坏机最空最先抢);磁盘不足按可重试错误处理(等退避,
  不是秒败烧尝试);失败消息带 server_key 供 doctor 识别连环坏机。
- **(e) 心跳**:PipelineEngine 的 speed_reporter(15s)是唯一心跳源,
  循环体加 try/except 防其自身异常导致假心跳超时。
- staging 路径沿用 `{task}/shard-{i}` 命名约定(naming/进度上报/前缀剥离
  三处耦合零改动;“batch-”仅是展示叫法)。

### 2.5 记账与控制面(修订,闭合可见性盲区)

批次=shards 表一行的复用决策保留,但以下修改为**硬前提**:

- **注册表三处同步更新**(评审 #1 优先级最高的 BLOCKER):
  `WORKFLOW_TYPES` += PoolDownloadWorkflow;`has_live_workflow` += `pool-{task_id}`
  模式;**加测试:每个 `@workflow.defn` 必须出现在 WORKFLOW_TYPES**
  (该清单历史上已经漂移过一次)。否则 pause/skip/reshard/preempt 全部
  "返回成功但什么都没停",reconciler 把活着的池任务当孤儿重派发,
  第二个协调器 create_shards 先删行,300/400 的账全丢。
- `get_running_shards()` JOIN tasks 排除 TERMINAL 父任务(连旧模式的
  "取消后 running 行永久钉死 busy"存量 bug 一起修)。
- `/api/shards/assign`、`/api/shards/status` 补 TERMINAL 守卫
  (与 /api/shard-progress 对齐;孤儿活动的迟到写入不得复活/污染)。
- `/api/shards/create`:单事务 executemany + 幂等(重试不先删成功批)+
  客户端超时 120s(v1 的逐行 commit 在 700 行时必超时,重试还会删账)。
- `_aggregate_task` 改 SQL SUM + 每任务 5s 去抖(O(700)/次的 Python 求和
  在池模式报文量下压垮 S1 SQLite)。
- **准入控制**:listing guard 的 `NOT EXISTS(shards)` 判据在池模式失效
  (批行早建/重派发时旧行残留),改为显式 `tasks.coordinator_phase` 列
  (workflow 经活动写 listing/filtering/chunking/dispatching);
  每 source 并发 downloading 池任务数上限(默认 4,配置可调)。
- **pause/resume 行清理**:pause 时 web 侧直接
  `UPDATE shards SET status='pending', server=NULL WHERE task_id=? AND status='running'`
  (兜底,与协调器 shielded 清理双保险);resume→pending 时删除旧批行
  (对齐 reshard 行为,消灭"陈行钉死 busy/骗 guard/喂假完结"三连)。
- **池巡检(orphan 等价物,R10)**:reconciler 对 dispatch_mode=pool 且
  协调器活着的任务,`handle.describe()` 看 pending_activities:
  全部 SCHEDULED >15min(空池/无人轮询)或 attempt 爬升无完成 → 告警;
  这替代 1800s 孤儿逻辑成为池模式的停摆探测器。
- doctor / detect_idle_workers / health_verifier / task_stuck 告警全部
  改以"running 批行"为忙信号;`download-{key}` 私有队列工作流探测对
  池模式跳过;"批次在排队"(0 running 行 + 协调器健康)不算 stalled。
- dashboard:`shard_servers` 在后端**按 server 聚合**(count+速度和)再出;
  `dl["server"]` 去重;前端 shards 弹窗按状态聚合展示(400 行直接渲染
  会崩,这是必改不是可选)。

### 2.6 API 与回滚(修订)

- `dispatch_mode` 列(default 'sharded')进 snapshot.init_db 迁移清单。
- 四处派发调用点(auto_dispatch / reconcile 孤儿 / doctor.fix / preempt)
  **全部收口** `start_download(task)` 按 mode 分流(v1 说三处,实为四处;
  漏一处 = 池任务头上再起 sharded 协调器,双写同一 BOS 前缀)。
- `/queue/reshard` 三处修:接受并写回 `dispatch_mode`(回滚的实现载体);
  pool 任务跳过逐 shard 句柄展开(无子工作流,只终止父;v1 会造 700 个
  幻影句柄把 uvicorn 事件循环卡到 watchdog 重启 web);describe 轮询
  加每轮预算。`cancel_workflow` 同理跳过 per-shard 句柄。
- preempt 的 `victim["server"]` NULL 崩溃是存量 bug,顺手修
  (池模式 cancel 即释放槽位,不需要 victim server)。

## 3. 灰度与回滚(修订:加了硬门禁)

1. 代码共存,旧工作流字节不动;`temporalio>=1.9` 钉版。
2. **部署门禁**:md5 清单文件列表补 `dlm/temporal/__main__.py` + 新池模块
   (或干脆全 `dlm/**/*.py` 聚合 md5);部署改**两阶段**:先全 16 台
   `--no-restart` 同步并核清单,清单全绿再统一重启(消灭"半新半旧"窗口)。
3. **模式门禁**:web 在 `DLM_DEFAULT_DISPATCH_MODE=pool` 或手动置 pool 时,
   先查池队列轮询者数 ≥ 池预期,不满足拒绝派发(唯一权威信号 —— worker
   心跳不带队列信息)。实现细节:raw gRPC `DescribeTaskQueueRequest`,
   **`task_queue_type=TASK_QUEUE_TYPE_ACTIVITY`**(池 Worker 不注册
   workflow,查 WORKFLOW 类型恒为 0)。
4. 上线顺序:部署+门禁 → 单个小任务手动 pool 试跑(双任务优先级实测一并做)
   → 观察一轮 → 默认切 pool → 旧任务自然跑完 → 稳定 ≥1 周删旧路径。
5. 回滚:默认切回 sharded;在跑池任务 `/queue/reshard {dispatch_mode:'sharded'}`
   (v2 的 reshard 已能正确终止池协调器并写回模式);BOS filter 兜底无损。

## 4. 风险清单(v2)

| # | 风险 | 缓解 | 残余 |
|---|------|------|------|
| K1 | FIFO 无严格优先级 | 动态窗口(Σ≈P)+ priority_key tie-break + preempt(已修) | 近似公平;P0 首批仍要等 list+filter(分钟~几十分钟级,如实声明) |
| K2 | 批次跨机重试重下 | 无条件 HEAD-skip → 浪费=在飞未上传部分 | 单批多次尝试的下行带宽 |
| K3 | 共存期同机多管线磁盘压力 | 预检算并发余量;灰度期不切默认;背压兜底 | 一个灰度周期 |
| K4 | 毒机连吃重试 | 5min 退避 + 末轮补投 + doctor 连环失败识别 | 无反亲和,极端仍可拖慢 |
| K5 | 窗口循环非确定性 | 全部用 workflow.wait/start_activity/排序遍历,骨架代码入库 | 靠 CI replay 测试守住 |
| K6 | 空池静默停摆 | schedule_to_close=48h + 池巡检(pending_activities) | 告警到人,不自动跨池 |
| K7 | history 超限 | 记账合并 ~6 事件/批;1500 批 < 10k | 超巨任务需拆任务 |
| K8 | 迟到写入污染 | TERMINAL 守卫补齐 assign/status 两洞 | 无 |
| K9 | 注册表漂移复发 | defn↔WORKFLOW_TYPES 一致性测试 | 无 |
| K10 | staging 残留 | 进场自清 + 周期 GC 清非活任务目录 | 无 |

## 5. 明确不做(YAGNI,不变)

跨池借调 / 批内断点(.progress.json 池模式退役,由无条件 HEAD-skip 顶替)/
严格公平调度器 / legacy DownloadDatasetWorkflow / 传输链路 —— 均不动。
web add 表单的 shard_count 输入框也**不做**:R1 下分片数由池自动涌现;
灰度期需手控时走 `/api/queue/add` 的 `shard_count` 或 `/queue/reshard`
(注:UI 实际调用的 `/api/tasks` 从未支持过 shard_count,这才是最初
"网页没法选分片"抱怨的根因;池模式直接消灭该需求)。

## 6. v1 → v2 修订记录(评审发现摘要)

三评审共 5 组 BLOCKER,全部闭合:
① 工作流类型/ID 注册表缺失(停控失效+假孤儿+删账)→ §2.5 三处同步+一致性测试;
② 批行 status/server 生命周期缺失(全控制面失明+无准入)→ §2.4-a/§2.5;
③ 窗口来源与公平算术错误(死锁/50-50/冻结)→ §2.3-7 动态窗口;
④ 空池无计时器静默停摆 → §2.3-4 + 池巡检;
⑤ 部署/回滚门禁缺失(清单不含 __main__.py、reshard 三重失效)→ §3/§2.6。
另修 IMPORTANT 级 14 项(全列,均有对应小节与实施任务):
① HEAD-skip 无条件化(§2.4-b)② 取消清理 shielded(§2.3-6)
③ 批次建行幂等化(§2.5,实施时为池专用新端点)④ 聚合去抖+SQL 化(§2.5)
⑤ dashboard 按 server 聚合(§2.5)⑥ preempt server=NULL 存量 bug(§2.6)
⑦ assign/status 两端点补 TERMINAL 守卫(§2.5)⑧ staging GC(§2.4-a/§2.5)
⑨ 批次终态只由协调器写(§2.4-c)⑩ listing guard 改 coordinator_phase(§2.5)
⑪ 池巡检替代 1800s 孤儿重派发(§2.5)⑫ task_stuck 豁免排队态(§2.5)
⑬ cancel/reshard 跳过逐 shard 句柄+轮询预算(§2.6)
⑭ get_running_shards JOIN 排除终态父任务(§2.5)。
