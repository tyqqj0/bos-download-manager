# 接口收敛设计 (项目一)

**日期**: 2026-08-17
**分支**: `feat/architecture-upgrade`
**状态**: 已评审(sonnet fact-check),已按结论修订
**后续项目**: 飞书登录(项目二)——本项目是它的前置条件

---

## 1. 问题

DLM 的 HTTP 面有 **61 条生效路由**,其中 **41 条是写操作**。这些写操作没有任何
组织原则:

- 同一个任务的操作散落在两个 router 里。暂停走 `POST /api/queue/pause`,
  重试走 `POST /api/tasks/{id}/retry`,撤销走 `POST /api/tasks/{id}/skip`。
  三个动作、三种路径风格,作用于同一个资源。
- **两个创建入口**。`POST /api/queue/add` 和 `POST /api/tasks` 都能建任务,
  实现分别在 `queue.py:65` 和 `tasks.py:297`,约 8 个逻辑块重复,且已经漂移
  (见 §5.2)。`queue.py:88` 的注释亲口承认了这件事:

  > Same two gates as POST /api/tasks, in the same order and for the same
  > reasons (see that route's docstring). Both add paths must agree: an
  > operator who reaches for curl instead of the modal should not be the one
  > who gets a gated repo chunked into 696 doomed batches.

  "both add paths must agree" 是一句祈祷,不是一个机制。
- **人机不分**。16 台 worker 回调的 14 个写接口,和运维点按钮的 27 个写接口,
  混在同一批文件里、同一批 `@router.post` 里,没有任何标记区分。其中 3 条**两边
  都在调**,而这件事今天没有任何地方写下来(见 §3.1.1)。
- **死接口留着**。`GET /api/queue`、`/api/queue/pending`、`/api/queue/active`、
  `POST /api/queue/reorder`、`POST /api/queue/jump` 零调用者。

### 为什么这是登录的前置条件,而不是"顺手清理"

项目二要求"所有写操作必须登录"。**无法给一个数不清的面加门禁**——门禁的正确性
等于"是否每一条写路由都被覆盖",而当前状态下这个问题没有可机械验证的答案。
如果先做登录,就是在 29 条分散路由上挂 29 个装饰器,漏一个就是一个静默的后门,
而且下一个人新增路由时不会有任何东西提醒他挂上。

收敛之后,门禁是"人面 router 上一个 `Depends`",漏不掉。

---

## 2. 不可动的边界

这些是硬约束,任何实现都不得违反:

| 边界 | 理由 |
|---|---|
| **机器面 18 条(14 写 + 4 读)的路径、HTTP 方法、请求体、响应体一字不改** | 16 台 worker 跑的是旧代码。改任何一处都要全机群部署,违反 CLAUDE.md 的"No mixed code versions",而且当前有 3 个 pool 任务在下载。其中 3 条是双面路由,见 §3.1.1 |
| `workflows.py` 保持确定性 | 正在运行的 workflow 靠 replay 恢复 |
| activity 调用传全部声明参数 | 少传一个 → temporalio 丢弃全部类型标注 → dataclass 变 dict → 死在 `.name`。已由 `tests/test_activity_arity.py` 把关 |
| SQLite 是唯一状态源 | `dlm/queue/snapshot.py` |
| **978 个既有测试全绿** | 回归底线。CLAUDE.md 里写的 "696 tests" 是过期数字 |
| 不改任何下载/上传/搬运业务逻辑 | 本项目只动 HTTP 层的形状 |

**本项目不需要部署 worker。** 这是一条验收标准,不是巧合:如果某个实现需要动
worker,那个实现就是错的。

---

## 3. 平面划分(权威表)

61 条路由分三类。权威快照:`/tmp/dlm-api-converge/routes-live.txt`
(生成方式见 §6.1)。

### 3.1 机器面(18 条,冻结)

调用方在 `dlm/temporal/activities.py`、`dlm/temporal/__main__.py`、
`dlm/temporal/event_buffer.py`、`dlm/sidecar/monitor.py`、
`dlm/web/health_verifier.py`。

写(12):

```
POST   /api/task-progress          activities.py:389,498
POST   /api/worker-heartbeat       temporal/__main__.py:212, sidecar/monitor.py:169
POST   /api/shard-progress         activities.py:377,791,1360,1459,1565
POST   /api/shards/create          activities.py:721
POST   /api/shards/status          activities.py:778,1385,1576,1672
POST   /api/shards/assign          activities.py:858,1377
POST   /api/shards/resume-info     activities.py:825
POST   /api/shards/aggregate       activities.py:846
POST   /api/pool/batches/create    activities.py:1619
POST   /api/pool/batches/release   activities.py:1703
POST   /api/missing-files          activities.py:677
POST   /api/events                 temporal/event_buffer.py:187,212
```

读(3):

```
GET    /api/shards/idle-workers    activities.py:808
GET    /api/pool/alive-workers     activities.py:884
GET    /api/pool/window            activities.py:1684
```

### 3.1.1 双面路由(3 条)——本 spec 第一版漏掉的东西

以下 3 条**同时**有 worker 调用和人类调用。第一版把它们划成纯人面,是错的:

```
POST   /api/tasks/{id}/missing-limit    activities.py:1766   (+ 无人类调用者)
GET    /api/tasks/{id}/missing-files    activities.py:1786   (+ 运维 curl 查缺件)
DELETE /api/tasks/{id}/missing-files    activities.py:1875   (+ 运维手工清理)
```

`activities.py:1783` 的注释亲口说明了双面性:"The endpoint returns everything
when no limit is given — that is for operators."
`docs/design/pool-cutover-plan.md:96` 也写着 `GET .../missing-files` 是"人和仪表盘查"。

**所以机器面实际是 18 条(14 写 + 4 读),不是 15 条。**

这 3 条本项目**不改名**(§3.2 里它们是"不变"),所以项目一不会打断机群。但三个后果
必须写清楚:

1. **P0 的机器面冻结常量必须是 18,不是 15。** 按 15 写,这 3 条就不受任何机械保护,
   下一个人改名它们时测试全绿。
2. **§4.1 的"'谁可以调'变成文件的属性"对这 3 条不成立。** 它们留在 `tasks.py`
   而 worker 在调,是 `internal.py` 这个抽象的真实例外,不是可以忽略的边角。
3. **这是项目二的一个真问题,不是本项目的。** 项目二若在人面挂一个统一的
   `Depends(require_user)`,会把 `POST missing-limit` 和 `DELETE missing-files`
   一起挡掉,**16 台 worker 的缺件复核 activity 会开始 401**。项目二必须对这 3 条
   做双向鉴权(人类会话 **或** worker 凭证),或者给 worker 另开一组 `/api/internal/...`
   路径——但后者要改 worker 调用点,需要全机群部署,所以本项目不做。

把这 3 条精确地命名出来,就是本项目能为项目二做的最有价值的事:把一个会在生产
上炸的惊喜,变成一个写在纸上的、有测试钉住的已知问题。

### 3.2 人面写操作(现状 → 目标)

| 现状 | 目标 | 处置 |
|---|---|---|
| `POST /api/queue/add` | — | **删**(见 §5.1) |
| `POST /api/tasks` | `POST /api/tasks` | 唯一创建入口 |
| `POST /api/queue/pause` | `POST /api/tasks/{id}/pause` | 改名 + 别名 |
| `POST /api/queue/resume` | `POST /api/tasks/{id}/resume` | 改名 + 别名 |
| `POST /api/queue/retry` | `POST /api/tasks/{id}/retry` | 已存在且已是委派,删旧 + 别名 |
| `POST /api/queue/preempt` | `POST /api/tasks/{id}/priority` | 改名 + 别名 |
| `POST /api/queue/reshard` | `POST /api/tasks/{id}/reshard` | 改名 + 别名 |
| `POST /api/tasks/{id}/skip` | `POST /api/tasks/{id}/revoke` | 改名 + 别名(skill 文档里本来就叫 Revoke) |
| `DELETE /api/queue/{task_id}` | `DELETE /api/tasks/{task_id}` | 已存在,删旧 + 别名 |
| `POST /api/queue/reorder` | — | **删**(零调用者) |
| `POST /api/queue/jump` | — | **删**(零调用者) |
| `POST /api/tasks/batch` | `POST /api/tasks/bulk` | 改名 + 别名。"batch" 在本仓库指 pool 批次,这里指"批量操作任务",撞词 |
| `POST /api/tasks/{id}/missing-limit` | 不变 | **双面**,见 §3.1.1 |
| `DELETE /api/tasks/{id}/missing-files` | 不变 | **双面**,见 §3.1.1 |
| `POST /api/parse` | 不变 | 无副作用的解析 |
| `POST /api/sync` | 不变 | |
| `POST /api/storage/register` | 不变 | 第二个创建入口,但语义不同(登记 BOS 已有数据,`status=done`) |
| `POST /api/storage/juicefs/move` | 不变 | |
| `POST /api/doctor` | 不变 | |
| `POST /api/servers/{key}/ping` / `/cleanup` | 不变 | |
| `POST /api/transfer/*`(4 条) | 不变 | 搬运是独立子系统,本项目不动 |
| `POST /api/cancel-all-workflows` | 不变 | |
| `DELETE /api/workflows/{workflow_id}` | 不变 | |

### 3.3 人面读操作

| 现状 | 处置 |
|---|---|
| `GET /api/queue` | **删**(零调用者) |
| `GET /api/queue/pending` | **删**(零调用者) |
| `GET /api/queue/active` | **删**(零调用者) |
| `GET /api/tasks`、`/api/tasks/{id}`、`/api/tasks/{id}/shards` | 不变 |
| `GET /api/tasks/{id}/missing-files` | 不变(**双面**,见 §3.1.1) |
| `GET /api/dashboard`、`/api/doctor`、`/api/doctor/staging-gc`、`/api/servers*`、`/api/storage/*`、`/api/transfer`、`/api/workflows` | 不变 |

**路由注册顺序**:`dlm/web/app.py:48-49` 先 include `queue_router` 再 include
`tasks_router`,而 `GET /tasks/{task_id}/shards` 定义在 `queue.py:619`。也就是说
`/api/tasks/*` 的解析当前横跨两个 router。

这里不存在遮挡风险(`{task_id}` 只匹配单个 `[^/]+` 段,`/tasks/{task_id}` 不可能
吃掉 `/tasks/{task_id}/shards`)——本 spec 第一版说"顺序是有载荷的"是夸大了。搬它的
理由是归属清晰:收敛后 `/api/tasks/*` 应该只有一个 router 负责,否则项目二挂门禁时
要在两个文件里找齐。

---

## 4. 目标形状

一个资源族,一种风格:

```
POST   /api/tasks                  创建(唯一入口)
GET    /api/tasks
GET    /api/tasks/{id}
DELETE /api/tasks/{id}
POST   /api/tasks/bulk             批量改状态
POST   /api/tasks/{id}/pause
POST   /api/tasks/{id}/resume
POST   /api/tasks/{id}/retry
POST   /api/tasks/{id}/revoke
POST   /api/tasks/{id}/priority
POST   /api/tasks/{id}/reshard
POST   /api/tasks/{id}/missing-limit
GET    /api/tasks/{id}/shards
GET    /api/tasks/{id}/missing-files
DELETE /api/tasks/{id}/missing-files
```

### 4.1 文件划分

| 文件 | 职责 |
|---|---|
| `dlm/web/routes/internal.py` | **新增**。**只有**机器调的 15 条(§3.1 的 12 写 + 3 读),原样搬入。"谁可以调这个接口"变成文件的属性 |
| `dlm/web/routes/tasks.py` | 人面任务操作,全部;外加 §3.1.1 的 3 条双面路由 |
| `dlm/web/routes/queue.py` | **保留但只剩别名层**(P3 之后)。见 §4.2 |
| 其余 router | 不动 |

对 §3.1 的 15 条来说,"谁可以调"从一个需要 grep 全仓库才能回答的问题,变成一个看
文件名就能回答的问题。

**但这个抽象有 3 个已知例外**——§3.1.1 的双面路由留在 `tasks.py`,因为把它们搬进
`internal.py` 并不能改变"人类也在调"这个事实,而改路径要动 worker、要全机群部署。
所以 `internal.py` 的准确含义是"**只有**机器调的接口",不是"机器调的全部接口"。
项目二不能把"人面 = 除 internal.py 之外的一切"当作门禁规则,必须显式处理这 3 条。

### 4.2 别名层是必须的,不是保守

CLAUDE.md、`docs/runbooks/`、`skills/dlm/SKILL.md` 里有大量 curl 命令,用户的
肌肉记忆里也有。旧路径出现次数(完整表见 `/tmp/dlm-api-converge/inventory.md`):

| 旧路径 | 文档 | 测试 | skill | 前端 |
|---|---|---|---|---|
| `/api/queue/add` | 12 | 9 | 1 | 0 |
| `/api/queue/reshard` | 13 | 0 | 2 | 0 |
| `/api/queue/pause` | 6 | 0 | 2 | 2 |
| `/api/queue/preempt` | 3 | 2 | 1 | 1 |
| `/api/queue/resume` | 3 | 1 | 2 | 1 |
| `/api/queue/retry` | 2 | 3 | 0 | 0 |
| `/api/tasks/{id}/skip` | 3 | 1 | 2 | 1 |

#### 别名的实现方式(不是随便委派)

**朴素做法有个会毁掉项目二的坑:** 如果旧 handler 直接调用新 handler 这个*被装饰的
函数*,FastAPI 注入的东西全部绕过——`Request`、以及**项目二要加的
`Depends(require_user)`**。那样别名就成了一条通往同一段业务逻辑的**免鉴权通道**。
这不是理论风险:项目一的产物如果长这样,项目二挂上门禁那天就有一个现成的后门。

**规定的做法:**

1. 业务逻辑提取成一个**普通 async 函数**(不带任何 `@router` 装饰),放在
   `tasks.py`,签名用明确的 Python 参数,不接受 FastAPI 注入对象。
2. 新路径和旧别名**各自都是注册在人面 router 上的真路由**,各自声明自己的参数,
   两者都调用第 1 步那个普通函数。
3. 因此两条路由都挂在同一个 router 上,项目二给这个 router 加依赖时,**别名和正路
   一起被覆盖**,不存在绕过。
4. 旧别名的 handler 体内多做一件事:`logger.warning("deprecated path %s, use %s")`。

#### 签名与响应形状

- 旧 handler 大多从 `body: dict` 里取 `task_id`(如 `body["task_id"]`),新路径用
  路径参数 `task_id: str`。别名必须自己做这个适配,这是机械但必须写出来的代码。
- **别名返回新形状,不保留旧形状。** 明确记下一处已知的字段丢失:
  `queue.retry_task` 现在返回 `{ok, task_id, status, cleared_shard_rows, note}`,
  而 `tasks.retry_task` 返回 `{status, message, ...}`。走别名的 `/api/queue/retry`
  调用者会拿不到 `cleared_shard_rows` 和 `note`。可以接受,因为 `/queue/retry` 的
  前端调用者是 0;但"薄别名 ⇒ 行为天然一致"这句话对响应体不成立,所以在这里说清。
- **归属方向要理清:** 今天是 `tasks.py:445` 的 `/tasks/{id}/retry` 委派给
  `queue.retry_task`。P3 之后必须反过来——逻辑搬到 `tasks.py` 的普通函数里,
  `queue.py` 只剩别名。**先搬逻辑,再挂别名**,不要留下互相委派。

别名的移除是 **P5,本项目不做**,留给用户在确认没有脚本还在打旧路径之后决定。

---

## 5. 两处偏离"已批准决定"的记录

### 5.1 `dispatch_now` 不存在,`/queue/add` 直接删

用户批准的原话是"就给它改成同一个入口的不同参数",前提是我告诉他
`/queue/add` 会立即起 workflow、而 `/api/tasks` 不会,所以合并时需要一个
`dispatch_now` 参数保住这个能力。

**这个前提是错的。** 取证:

- `queue.py:67` 的 docstring 说它会派发。**这个 docstring 是过期的。**
- handler 的最后一个动作是 `await _run_blocking(do_save)`(`queue.py:175`),
  之后直接 return。没有任何 Temporal 调用。
- 实际派发发生在 `auto_dispatch_pending()`(`dlm/web/reconciler.py`),
  每 30 秒一轮,对两个入口一视同仁。

所以 `dispatch_now` 没有任何能力需要保留,加上它就是加一个死参数。而"先建行、
先别派发"这个真实需求,`/api/tasks` 已经有 `no_dispatch` 覆盖了
(`tasks.py:283,413`)。

**处置:直接删 `/queue/add`,不加参数。**

安全性论证:`/queue/add` 有 **零个调用者**(前端 0、CLI 0、脚本 0)。它与
`/api/tasks` 的全部行为差异都是潜伏的,不是活的——删掉它不可能让任何人现在
经历的行为发生退化。CLI 的 `dlm add` 走的是 `/api/tasks`
(`dlm/commands/add.py:41`)。

**这条偏离必须在最终报告里向用户明确说明。**

### 5.2 创建入口是 3 个,不是 4 个

`POST /api/tasks/batch`(`tasks.py:582`)**不创建任何行**。它把已有任务改成
`revoked` 或 `pending`(`tasks.py:602,610-614`),外加 best-effort 取消
(`:621-626`)。真正的创建入口是 `/queue/add`、`/api/tasks`、
`/storage/register`。删掉第一个之后剩 2 个,且语义不重叠:前者建待下载任务,
后者登记 BOS 上已存在的数据(`status=done`, `source=bos`,
`repo_id=bos://bucket/prefix`,`storage.py:184-186`)。

### 5.3 合并两个 handler 时的行为分歧

以 `/api/tasks` 的行为为准(它是所有人实际在用的那个,因此对用户是 no-op)——
**但有一处例外,`/api/tasks` 才是错的那一边,必须修,见 5.3.1。**

| 分歧点 | `/queue/add` | `/api/tasks` | 取 |
|---|---|---|---|
| 重复检查是否排除 `done` | 排除(`queue.py:159`) | 不排除(`tasks.py:371`) | `/api/tasks`。重跑一个 done 任务的正路是 `POST /api/tasks/{id}/retry` |
| `no_dispatch` | 无 | 有(`:283`/`:413`) | 有 |
| `category` 默认值 | `""`(`queue.py:104`) | `"other"`(`tasks.py:271`) | `"other"` |
| `size_gb` | 硬编码 0(`queue.py:146`) | 取自请求体(`tasks.py:415`) | 取自请求体 |
| `type` 推断 | 尊重推断(`queue.py:103`) | **总是覆盖成 `"dataset"`** | **修 `/api/tasks`**,见 5.3.1 |
| `priority` 入参 | int 0-9 夹紧(`queue.py:105`) | 字符串 `"P0".."P3"` 过 `PRIORITY_TO_INT`(`tasks.py:378`) | `/api/tasks`。**但这影响文档,见 §5.5** |
| `max_workers` | 总是写(0 = auto,`queue.py:150`) | 仅当 `shard_count > 0` 才写,否则留 NULL(`tasks.py:425-426`) | `/api/tasks` |
| 成功响应体 | `{ok, task_id}`(`queue.py:184`) | `{task: ...}`(`tasks.py:437`) | `/api/tasks` |

#### 5.3.1 `/api/tasks` 会把推断出来的 `type` 冲掉(要修)

`AddTaskRequest.type` 的默认值是 `"dataset"`(`tasks.py:271`),而合并逻辑是
`if req.type: parsed["type"] = req.type`(`tasks.py:330-331`)。`"dataset"` 恒为真,
所以 **`parse_repo` 推断出来的 type 永远被覆盖**。`/queue/add` 写的是
`body.get("type", parsed.get("type", "dataset"))`(`queue.py:103`),尊重推断。

为什么这不是小事:`bos_target`(`dlm/core/bos.py:15-30`)用 `type == "model"` 决定
桶——model 进 `auwomo-model-open` 且前缀是 `{name}/`,否则进 `auwomo-data` 且前缀是
`{category}/{name}/`。一个被误判成 dataset 的模型会**上传到数据集桶的错误前缀下**。

现在没炸,是因为两个真实调用方都显式传了 `type`:前端 `app.js:285`、
CLI `dlm/commands/add.py` 的 body。**但 P4 恰好会造出触发它的路径**——CLAUDE.md 的
curl 例子不带 `type`,把它从 `/queue/add` 改指到 `/api/tasks` 之后,照着例子加一个
无前缀的 HF 模型 URL(`parser.py` 的 `_parse_hf_url` 推断为 `"model"`),就会静默
落到数据集桶里。

**处置:把 `AddTaskRequest.type` 的默认值改成 `None`**(含义:没说就用推断)。前端和
CLI 都显式传值,所以对它们零影响。这必须在 P4 改文档**之前或同时**落地,否则文档
改完的那一刻就埋下一个误投桶的坑。

删掉 `/queue/add` 会移除当前唯一正确处理 type 推断的路径,所以这个修复不是可选的。

### 5.4 错误形状统一(小范围)

`/queue/add` 和 `/storage/register` 在出错时返回 `{"error": ...}` 且 HTTP 200;
`/api/tasks` 抛 `HTTPException(400)`。删掉 `/queue/add` 顺手消掉一半。
`/storage/register` 改成 400 属于行为变更,**本项目不做**,记为待办。

### 5.5 `priority` 的入参语义变了,文档必须跟着改

CLAUDE.md 里的建任务例子是
`curl -X POST .../api/queue/add -d '{..., "priority":0, "shard_count":6}'`——
`priority` 是 int。`/api/tasks` 收的是字符串 `"P0".."P3"`
(`PRIORITY_TO_INT = {"P0":0,"P1":2,"P2":5,"P3":7}`,`tasks.py:19`)。

所以 P4 改文档时,不能只把路径从 `/api/queue/add` 换成 `/api/tasks`,还必须把
`"priority":0` 换成 `"priority":"P0"`。一个只换路径的机械替换会产出一条**看起来
对、实际把优先级设错**的文档命令。P4 的验收要显式检查这一点。


---

## 6. 分阶段

每一阶段独立可测、独立提交。阶段之间不得跳序:P0 的清单测试是后面每一步的安全网。

### P0 — 冻结与清单测试(零行为变更)

新增 `tests/test_route_inventory.py`,钉住 `(method, path, plane)` 三元组:

- 用 `create_app().openapi()['paths']` 枚举(**不用 `app.routes`**,见 §6.1)
- 一份期望清单常量,包含全部 61 条及其平面归属
- 断言 1:生效路由集合 == 期望集合(新增/改名/挪动不同步更新即红)
- 断言 2:机器面 **18** 条的 `(method, path)` 逐条存在——这是"不用部署 worker"
  这条验收标准的机械化形式。这份清单必须**手写**,不能从期望清单推导,否则把一条
  机器面路由的 plane 悄悄改成 `human` 再改名就能全绿放行
- 断言 3:同一 `(method, path)` 不得注册两次。**注意 `openapi()['paths']` 查不出
  重复**(它是以 path 为键的 dict,第二次注册被静默吞掉),查重必须走
  `<module>.router.routes`

**这一阶段不改任何 `dlm/` 下的代码。**

### P0.1 — 前端/文档路径引用测试

`tests/test_servers_view.py:324` 已有先例:枚举 `openapi()['paths']`,再把
`app.js` 里所有 `/api/...` 字面量交叉核对。扩展它覆盖 `dlm/commands/`,
让"前端或 CLI 打了一条不存在的路径"变成红灯。

### P1 — 删死接口 + 合并创建入口

删:`GET /api/queue`、`/api/queue/pending`、`/api/queue/active`、
`POST /api/queue/reorder`、`POST /api/queue/jump`、`POST /api/queue/add`。
按 §5.3 确认 `/api/tasks` 保持行为,并按 §5.3.1 修掉 `type` 覆盖
(这是删 `/queue/add` 的附带义务,不是可选项)。更新 P0 的期望清单。

### P2 — 拆 router

**只有**机器调的 15 条搬入 `dlm/web/routes/internal.py`,**路径/方法/请求体/响应体
一字不改**。§3.1.1 的 3 条双面路由留在 `tasks.py`。`app.py` 注册新 router。
P0 的断言 2 保证这一步没动到机器面(18 条全部,包括留在 `tasks.py` 的那 3 条)。

`GET /tasks/{task_id}/shards` 从 `queue.py:619` 搬到 `tasks.py`,消掉
`/api/tasks/*` 横跨两个 router 的状况。

### P3 — 改名 + 薄别名

按 §3.2 建立新路径,旧路径降级为记 warning 的薄委派。**别名的实现必须按 §4.2 的
规定做**(业务逻辑提取成普通函数,新路径与别名各自是注册在同一个人面 router 上的
真路由)——直接调用被装饰的 handler 会绕过 FastAPI 注入,那样别名就是项目二的一个
免鉴权后门。每条新路径都要有测试证明它和旧路径行为一致。

### P4 — 客户端/文档/skill 切换

`dlm/web/static/app.js`、`dlm/commands/`、`CLAUDE.md`、`docs/runbooks/`、
`skills/dlm/SKILL.md` 全部改用新路径。P0.1 的测试保证没漏。
`skills/dlm/` 改完需要 scp 到 S1(用户操作,不在本项目自动化范围)。

### P5 — 移除别名(本项目不做)

留给用户在确认无人再打旧路径后决定。

---

## 7. 验收标准

1. **路由清单测试存在**,钉住全部 61 条的 `(method, path, plane)`。任何新增、
   改名、挪动不同步更新清单即测试失败。
2. **机器面一字未改**,由测试断言 2 机械证明。**本项目不需要部署 worker**。
3. **创建任务只有一个入口** `POST /api/tasks`(`/storage/register` 是语义不同的
   登记接口,不算创建下载任务)。
4. **死接口已删**:`GET /api/queue`、`/queue/pending`、`/queue/active`、
   `POST /queue/reorder`、`/queue/jump`、`/queue/add` 均不再存在。
5. **人面路径统一到 `/api/tasks` 资源族**,旧路径以薄委派 + warning 存在。
6. **前端与 CLI 全部走新路径**,仓库内 grep 不到它们引用旧路径。
7. **CLAUDE.md、`docs/runbooks/`、`skills/dlm/SKILL.md` 的 curl 已更新。**
8. **978 个既有测试全绿**,新增覆盖:清单测试、路径引用测试、每条改名路径的
   行为一致性测试。
9. **人面写操作的门禁点数量收敛到"每个人面 router 一个 `Depends`"**,项目二不需要
   逐条路由挂装饰器。

   这一条第一版写的是"一个 `Depends`",**那是夸大的**。人面写操作分布在 7 个 router
   里:`tasks.py`(任务操作 + `/parse`)、`queue.py`(`POST /sync`)、`servers.py`(ping /
   cleanup)、`storage.py`(register / juicefs move)、`doctor.py`(`POST /doctor`)、
   `transfer.py`(4 条)、`workflows.py`(cancel-all / delete)。§4.1 只把机器面搬进
   `internal.py` 并把任务操作归拢到 `tasks.py`,storage / doctor / servers / transfer /
   workflows 明确"不动",所以一个 `Depends` 覆盖不到它们。

   **本条的准确含义(两种实现都算通过,项目二选一即可):**
   - **每个人面 router 挂一个 router 级 `Depends`(约 6 个挂点)**,或者
   - **一个 app 级全局依赖 + 一份显式的机器面豁免清单**(清单即 §3.1 的 18 条)。

   验收时按这个来验:**每一条人面写路由,都必须能被"它所在的 router"或"全局依赖 +
   豁免清单"这两种机制之一无遗漏地覆盖**;不存在任何一条写路由需要单独挂装饰器
   才能被门禁到。P0 的清单测试提供枚举依据。

   **已知例外必须显式处理:** §3.1.1 的 3 条双面路由(`POST /api/tasks/{id}/missing-limit`、
   `GET`/`DELETE /api/tasks/{id}/missing-files`)住在人面文件里但 worker 在调,套上
   纯人类门禁会让 16 台 worker 401。本项目只负责把它们命名出来并用测试钉住;
   怎么鉴权是项目二的事。

10. **`AddTaskRequest.type` 不再覆盖 `parse_repo` 的推断**(§5.3.1),且有测试证明
    一个不带 `type` 的模型 URL 会被存成 `type="model"`。这一条是删掉 `/queue/add` 的
    附带义务:那是当前唯一正确处理推断的路径。


---

## 8. 不在范围内

- 任何登录/鉴权代码(项目二)
- `owner_open_id` / `owner_name` 列(项目二)
- **§3.1.1 那 3 条双面路由的鉴权方案**(项目二)。本项目只负责把它们命名出来、
  用测试钉住、写清楚后果
- 搬运子系统的接口(`/api/transfer/*`)
- `/storage/register` 的错误形状改造(记为待办)
- 移除别名(P5)
- `~/.dlm/servers.yaml` 只有 12 条、缺 bj5-bj9 的漂移(独立 bug,单独开)
- 任何下载/上传/搬运业务逻辑

**注意这几条虽然是"清理",但在范围内**,因为它们是本项目动作的直接后果,
不做就会留下一个比现在更糟的状态:

- `AddTaskRequest.type` 的覆盖 bug(§5.3.1)——删 `/queue/add` 会移除唯一正确的路径
- CLAUDE.md 建任务 curl 的 `priority` 从 int 改成 `"P0"`(§5.5)——只换路径会产出
  一条静默设错优先级的文档命令

---

## 附录 6.1 — 枚举生效路由的正确方式

fastapi 0.141.1 / starlette 1.6.0 下,`app.routes` **不能用**:

```
len(app.routes) == 13
Counter({'_IncludedRouter': 8, 'Route': 4, 'Mount': 1})
```

被 include 的 router 保持为 `_IncludedRouter` 对象,既没有 `.methods` 也没有
`.routes`(只有 `effective_candidates` / `original_router` 等内部属性),所以朴素
遍历只数出 4 条 docs 路由(`/openapi.json`、`/docs`、`/docs/oauth2-redirect`、
`/redoc`)。

正确方式:

```python
paths = create_app().openapi()["paths"]
for path, ops in paths.items():
    for method in ops:
        if method.upper() in ("GET", "POST", "PUT", "PATCH", "DELETE"):
            ...
```

先例:`tests/test_servers_view.py:324` 已经这么做,并留了注释说明为什么不用
`app.routes`(它会错处理 `/api` 前缀)。
