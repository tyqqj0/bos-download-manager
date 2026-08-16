# 接口收敛设计 (项目一)

**日期**: 2026-08-17
**分支**: `feat/architecture-upgrade`
**状态**: 待评审
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
- **人机不分**。16 台 worker 回调的 12 个写接口,和运维点按钮的 29 个写接口,
  混在同一批文件里、同一批 `@router.post` 里,没有任何标记区分。
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
| **机器面 12 个写接口 + 3 个读接口的路径、HTTP 方法、请求体、响应体一字不改** | 16 台 worker 跑的是旧代码。改任何一处都要全机群部署,违反 CLAUDE.md 的"No mixed code versions",而且当前有 3 个 pool 任务在下载 |
| `workflows.py` 保持确定性 | 正在运行的 workflow 靠 replay 恢复 |
| activity 调用传全部声明参数 | 少传一个 → temporalio 丢弃全部类型标注 → dataclass 变 dict → 死在 `.name`。已由 `tests/test_activity_arity.py` 把关 |
| SQLite 是唯一状态源 | `dlm/queue/snapshot.py` |
| 696 个既有测试全绿 | 回归底线 |
| 不改任何下载/上传/搬运业务逻辑 | 本项目只动 HTTP 层的形状 |

**本项目不需要部署 worker。** 这是一条验收标准,不是巧合:如果某个实现需要动
worker,那个实现就是错的。

---

## 3. 平面划分(权威表)

61 条路由分三类。权威快照:`/tmp/dlm-api-converge/routes-live.txt`
(生成方式见 §6.1)。

### 3.1 机器面(15 条,冻结)

调用方在 `dlm/temporal/activities.py`、`dlm/temporal/__main__.py`、
`dlm/temporal/event_buffer.py`、`dlm/sidecar/monitor.py`、
`dlm/web/health_verifier.py`。

写(12):

```
POST   /api/task-progress
POST   /api/worker-heartbeat
POST   /api/shard-progress
POST   /api/shards/create
POST   /api/shards/status
POST   /api/shards/assign
POST   /api/shards/resume-info
POST   /api/shards/aggregate
POST   /api/pool/batches/create
POST   /api/pool/batches/release
POST   /api/missing-files
POST   /api/events
```

读(3):

```
GET    /api/shards/idle-workers
GET    /api/pool/alive-workers
GET    /api/pool/window
```

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
| `POST /api/tasks/{id}/missing-limit` | 不变 | |
| `DELETE /api/tasks/{id}/missing-files` | 不变 | |
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
| `GET /api/tasks`、`/api/tasks/{id}`、`/api/tasks/{id}/shards`、`/api/tasks/{id}/missing-files` | 不变 |
| `GET /api/dashboard`、`/api/doctor`、`/api/doctor/staging-gc`、`/api/servers*`、`/api/storage/*`、`/api/transfer`、`/api/workflows` | 不变 |

**注意路由注册顺序是有载荷的**:`dlm/web/app.py:48-55` 先 include `queue_router`
再 include `tasks_router`,而 `GET /tasks/{task_id}/shards` 定义在
`queue.py:619`。也就是说 `/api/tasks/*` 的解析当前横跨两个 router。收敛后
`/tasks/{id}/shards` 必须搬到 tasks router,让 `/api/tasks/*` 只有一个归属。

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
| `dlm/web/routes/internal.py` | **新增**。机器面 15 条,原样搬入。"谁可以调这个接口"变成文件的属性,不再是需要逐条推断的事实 |
| `dlm/web/routes/tasks.py` | 人面任务操作,全部 |
| `dlm/web/routes/queue.py` | **保留但只剩别名层**(P3 之后)。见 §4.2 |
| 其余 router | 不动 |

"谁可以调"从一个需要 grep 全仓库才能回答的问题,变成一个看文件名就能回答的问题。
这正是项目二挂门禁的地方。

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

别名的形式:**薄委派**——旧路径的 handler 只做一件事,记一条
`logger.warning("deprecated path %s, use %s")` 然后调用新 handler 的函数体。
不复制业务逻辑。旧路径的行为因此天然与新路径一致。

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

以 `/api/tasks` 的行为为准(它是所有人实际在用的那个,因此对用户是 no-op):

| 分歧点 | `/queue/add` | `/api/tasks` | 取 |
|---|---|---|---|
| 重复检查是否排除 `done` | 排除(`queue.py:159`) | 不排除(`tasks.py:371`) | `/api/tasks`。重跑一个 done 任务的正路是 `POST /api/tasks/{id}/retry` |
| `no_dispatch` | 无 | 有 | 有 |
| `category` 默认值 | `""` | `"other"` | `"other"` |
| `size_gb` | 0 | 取自请求体 | 取自请求体 |

### 5.4 错误形状统一(小范围)

`/queue/add` 和 `/storage/register` 在出错时返回 `{"error": ...}` 且 HTTP 200;
`/api/tasks` 抛 `HTTPException(400)`。删掉 `/queue/add` 顺手消掉一半。
`/storage/register` 改成 400 属于行为变更,**本项目不做**,记为待办。

---

## 6. 分阶段

每一阶段独立可测、独立提交。阶段之间不得跳序:P0 的清单测试是后面每一步的安全网。

### P0 — 冻结与清单测试(零行为变更)

新增 `tests/test_route_inventory.py`,钉住 `(method, path, plane)` 三元组:

- 用 `create_app().openapi()['paths']` 枚举(**不用 `app.routes`**,见 §6.1)
- 一份期望清单常量,包含全部 61 条及其平面归属
- 断言 1:生效路由集合 == 期望集合(新增/改名/挪动不同步更新即红)
- 断言 2:机器面 15 条的 `(method, path)` 逐条存在——这是"不用部署 worker"
  这条验收标准的机械化形式
- 断言 3:同一 `(method, path)` 不得注册两次

**这一阶段不改任何 `dlm/` 下的代码。**

### P0.1 — 前端/文档路径引用测试

`tests/test_servers_view.py:324` 已有先例:枚举 `openapi()['paths']`,再把
`app.js` 里所有 `/api/...` 字面量交叉核对。扩展它覆盖 `dlm/commands/`,
让"前端或 CLI 打了一条不存在的路径"变成红灯。

### P1 — 删死接口 + 合并创建入口

删:`GET /api/queue`、`/api/queue/pending`、`/api/queue/active`、
`POST /api/queue/reorder`、`POST /api/queue/jump`、`POST /api/queue/add`。
按 §5.3 确认 `/api/tasks` 保持行为。更新 P0 的期望清单。

### P2 — 拆 router

机器面 15 条搬入 `dlm/web/routes/internal.py`,**路径/方法/请求体/响应体
一字不改**。`app.py` 注册新 router。P0 的断言 2 保证这一步没动到机器面。

`GET /tasks/{task_id}/shards` 从 `queue.py:619` 搬到 `tasks.py`,消掉
`/api/tasks/*` 横跨两个 router 的状况。

### P3 — 改名 + 薄别名

按 §3.2 建立新路径,旧路径降级为记 warning 的薄委派(§4.2)。每条新路径都要有
测试证明它和旧路径行为一致。

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
8. **696 个既有测试全绿**,新增覆盖:清单测试、路径引用测试、每条改名路径的
   行为一致性测试。
9. **人面写操作可以用一个 `Depends` 覆盖**——即项目二不需要再逐条挂装饰器。
   这一条由 §4.1 的文件划分满足,评审时按"人面写路由是否全部在 tasks router
   或明确列出的其他人面 router 上"来验。

---

## 8. 不在范围内

- 任何登录/鉴权代码(项目二)
- `owner_open_id` / `owner_name` 列(项目二)
- 搬运子系统的接口(`/api/transfer/*`)
- `/storage/register` 的错误形状改造(记为待办)
- 移除别名(P5)
- `~/.dlm/servers.yaml` 只有 12 条、缺 bj5-bj9 的漂移(独立 bug,单独开)
- 任何下载/上传/搬运业务逻辑

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
