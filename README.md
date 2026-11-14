# 裂纹事件流契约排程与日报发布 API

两类能力，同进程、同 SQLite：

1. **契约排程**：轨道检测裂纹事件流滚动升级的契约兼容性排程服务。提交采集端
   （发布端）与质检、日报（两个独立部署的订阅端）新旧两版的扁平 JSON 字段定义，
   自动计算三方从全旧版经混跑到全新版的安全升级顺序；不存在安全顺序时，指出受阻的
   版本组合、字段级违规以及发布端可发出即被拒收的示例消息。
2. **批次补传与日报发布**：采集端可整批补传裂纹事件（全局事件号跨批去重、同号
   异内容整批拒绝），并按巡测日发布可复核的裂纹日报（按车号、缺陷等级计数并列出
   纳入的事件号）。发布请求号幂等、版本不可变、失败不留半成品，并与并发补传安全
   交错。

## 1. 背景与语义约定

滚动升级期间每一方都经历三个阶段：`old → mixed → new`，混合阶段新旧实例同时运行：

| 角色 | mixed 阶段含义 |
| --- | --- |
| 发布端 `producer`（采集端 collector） | 新旧实例都在发消息，因此 **old 与 new 两类消息都可能出现** |
| 订阅端 `subscriber`（质检 qc / 日报 daily_report） | 新旧实例都在收消息，因此 **两类消息都必须能被接收** |

字段契约规则（与需求一一对应）：

1. 扁平 JSON 字段，类型仅允许 `integer`、`string`、`enum`（字符串枚举）；
   `enum` 必须声明非空且唯一的 `enum_values`。
2. 每个字段标明 `required`：发布端必填字段保证发出；**可选字段可能整体缺失**。
3. 枚举字段实际可能取声明值中的**任意一个**。
4. **订阅端忽略未知字段**：发布端多发、订阅端未声明的字段不构成问题。
5. 订阅端 `required` 字段若发布端未声明或声明为可选，则存在“消息缺字段”的可能，
   组合判为不安全（`required_missing`）。
6. 同名字段类型不一致判 `type_mismatch`；订阅端用 `enum` 收窄取值域时，发布端能
   发出的某个值不在订阅端枚举内判 `enum_violation`（无界 `string` 对 `enum` 必违规）。

**安全状态**：任一在运行的发布端版本可能发出的每条消息，都能被两个订阅端的所有
在运行版本接收。升级的每一步（共 6 次单阶段推进）后都必须满足该条件。

## 2. 求解算法

三方 × 三阶段共 27 个状态，从 `(old, old, old)` 到 `(new, new, new)` 做 BFS：

- 每个节点只允许一方沿 `old→mixed→new` 推进一步；
- 新状态通过“全部在运行 (发布端版本 × 订阅端版本) 组合均兼容”才入队；
- 8 个版本组合（2 发布端版本 × 2 订阅端 × 2 订阅端版本）的兼容性结果带缓存；
- BFS 天然给出步数最少的安全顺序（每方两步、共 6 步）；
- 走不到终点即无解：汇总所有不兼容版本组合、字段级违规、拒收反例消息，以及每个
  组合挡住了哪些升级动作；若初始状态本身不安全会单独标明 `initial_state_safe=false`。

反例消息是一条**发布端按自己契约确实可以发出**的扁平 JSON：必填字段齐全，
缺失类字段刻意不出现，枚举/类型冲突字段采用触发违规的具体值。

## 3. 目录结构

```
.
├── app/
│   ├── main.py          # FastAPI 入口、生命周期（打开 SQLite）、Swagger 示例挂载
│   ├── models.py        # 契约排程的 Pydantic 模型与入参校验
│   ├── compat.py        # 版本对兼容性判定与反例消息构造
│   ├── planner.py       # 27 状态空间 BFS 排程
│   ├── report_models.py # 批次/日报的 Pydantic 模型（字段别名、规范化）
│   ├── db.py            # SQLite 存储：整批原子入库、事件幂等、日报快照
│   └── report_api.py    # 批次接收/复核、日报发布/查询接口
├── samples/
│   ├── safe_subscriber_first.json    # 排程样例一：有安全顺序（先订阅端后发布端）
│   ├── infeasible_circular.json      # 排程样例二：循环依赖，无解
│   ├── backfill_batch.json           # 首次补传 3 条事件
│   ├── backfill_later.json           # 后到的一批事件
│   ├── reingest_conflict.json        # 同号异内容，整批 409
│   ├── publish_retry.json            # 首次发布 / 同请求号重试
│   └── republish_after_backfill.json # 补传后换新请求号重发
├── tests/               # pytest：兼容规则 / 求解器 / HTTP / 批次与发布语义（46 个用例）
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── run_samples.sh       # 一键用样例打排程与日报 API
```

## 4. 启动方式

### Docker Compose（推荐，本机无需 Python）

```bash
docker compose up --build
# API:      http://localhost:8000
# Swagger:  http://localhost:8000/docs   （/ 会自动跳转）
# 健康检查:  http://localhost:8000/health
```

镜像内带 `HEALTHCHECK`，compose 同步配置了健康检查；以非 root 用户运行。
SQLite 落在 `/app/data/cracks.db`（compose 用命名卷 `crack-data` 持久化），
可用环境变量 `CRACK_DB_PATH` 覆盖。

### 本地直接运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 一键演示

```bash
./run_samples.sh          # BASE_URL 可改指向其他环境，DAY 可改巡测日
```

脚本先跑两个排程样例，再演示：补传首批 → 重投同批（只计一次）→ 发布日报 →
同请求号重试返回原版 → 补传后到批次 → 新请求号重发版本 2 → 同号异内容整批 409。

## 5. API 概览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/plan` | 提交三方契约，返回安全升级顺序或无解报告 |
| POST | `/api/v1/compatibility` | 返回全部 8 个版本组合的兼容性明细（含违规与反例） |
| POST | `/api/v1/events/batches` | 整批补传裂纹事件（原子入库，跨批按事件号幂等） |
| GET | `/api/v1/events/batches/{batch_id}` | 按批次号复核入库明细 |
| POST | `/api/v1/reports/publish` | 按巡测日发布一版日报（请求号幂等） |
| GET | `/api/v1/reports/{survey_day}` | 列出该巡测日全部日报版本（旧版可查） |
| GET | `/api/v1/reports/{survey_day}/{version}` | 查询指定版本日报快照 |
| GET | `/health` | 健康检查 |
| GET | `/docs` | Swagger UI，请求体已内置样例可一键填充 |

请求体结构（节选）：

```json
{
  "parties": [
    {
      "id": "collector",
      "role": "producer",
      "old_schema": [
        {"name": "defect_level", "type": "enum", "required": true,
         "enum_values": ["L1", "L2"]}
      ],
      "new_schema": [
        {"name": "defect_level", "type": "enum", "required": true,
         "enum_values": ["L1", "L2", "L3"]}
      ]
    }
  ]
}
```

`parties` 必须恰好包含 1 个 `producer` 和 2 个 `subscriber`，三方 `id` 互不相同；
同一份 schema 内字段名不得重复；非法入参返回 422 并带字段级错误说明。

## 6. 批次补传与日报发布语义

### 6.1 事件与批次

```json
POST /api/v1/events/batches
{
  "events": [
    {"event_id": "EV-001", "survey_day": "2026-11-01",
     "car_id": "CAR-001", "defect_level": "L2"}
  ]
}
```

- 事件字段：`event_id`（全局事件号）、`survey_day`（巡测日）、`car_id`（车号）、
  `defect_level`（缺陷等级）。事件号与车号接受字符串或整数，入库前规范化为字符串
  （`1001` 与 `"1001"` 视为同一事件、同一车号）；兼容别名 `event_no` /
  `inspection_day` / `car_number` / `level`。
- **一个请求 = 一个批次 = 一个 SQLite 事务**。批内任一条同号异内容，或与已入库
  事件同号异内容，整批返回 `409`，批记录、事件、批明细全部回滚，不留部分记录。
- 跨批同号**同内容**只计一次：首次入库记为 `inserted`，后续批次记为 `duplicate`，
  响应分别列出 `inserted_event_ids` / `duplicate_event_ids`。
- 重投完全相同的批次（例如采集端超时重发）会生成新的 `batch_id`，但事件总数不变；
  `GET /api/v1/events/batches/{batch_id}` 可复核该批逐条结论
  （`inserted` / `duplicate`）。

### 6.2 日报发布与版本

```json
POST /api/v1/reports/publish
{"publish_request_id": "pub-20261101-001", "survey_day": "2026-11-01"}
```

- 日报按**车号 × 缺陷等级**计数（`counts`），并列出纳入的 `event_ids`；
  同时给出 `batch_ids`，可据此复核“只取到完整提交的批次”。
- 发布在 `BEGIN IMMEDIATE` 写事务中读取并写入：与并发补传串行化，快照只能看到
  完整提交的批次；任何异常整体回滚，不会留下可查询的半成品，也不占用版本号。
- `publish_request_id` 是幂等键：同号重试**返回原版快照**，响应头
  `X-Idempotent-Replay: true`，即使期间补齐了新批次也不改原版；请求号不能跨巡测日
  复用（`409`）。
- **新请求号**发布新版本（同一巡测日内 `version` 从 1 递增），纳入发布时刻已提交的
  全部事件；**旧版本始终可查且内容不变**：
  `GET /api/v1/reports/{survey_day}` 列出全部版本，
  `GET /api/v1/reports/{survey_day}/{version}` 查询指定版本。

### 6.3 并发与持久化

- 存储为单文件 SQLite（WAL + `synchronous=FULL` + `BEGIN IMMEDIATE`）。进程内
  用一把 `RLock` 串行化写操作；跨进程/多 worker 由 SQLite 写锁与 `busy_timeout`
  保证互斥。
- 建表使用 `CREATE TABLE IF NOT EXISTS`，重启后历史批次与日报版本继续可查；
  compose 以 `crack-data` 卷持久化 `/app/data/cracks.db`。

## 7. 两个内置样例解读

### 样例一 `safe_subscriber_first.json` —— 先升级订阅端才能新增等级

- 采集端新版把枚举 `defect_level` 从 `[L1, L2]` 扩展为 `[L1, L2, L3]`；
- 质检、日报的旧版枚举都只认 `[L1, L2]`，收到 `L3` 即 `enum_violation`。

排程结论（6 步）：**qc 旧→混→新、daily_report 旧→混→新，最后 collector 旧→混→新**。
发布端在两个订阅端全部完成升级前不能进入混合阶段——一旦进入就可能发出带 `L3`
的新消息，而仍在运行的旧订阅端会拒收。`/api/v1/compatibility` 可看到
`collector:new -> qc:old` 与 `collector:new -> daily_report:old` 两个组合不兼容，
正是它们把发布端的升级动作挡在后面。

### 样例二 `infeasible_circular.json` —— 循环依赖，无解

- 采集端新版同样新增枚举值 `L3`，旧质检枚举不认（`collector:new → qc:old` 违规，
  反例 `{"defect_level": "L3", ...}`）→ 发布端升级被旧质检挡住；
- 质检新版同时新增必填字段 `inspector_id`，而旧采集端永远不发该字段
  （`collector:old → qc:new` 违规，反例消息里没有 `inspector_id`）→ 质检升级被
  旧发布端挡住。

两边互相等待，BFS 无法到达全新状态，`feasible=false`，响应的 `blocked_pairs`
给出每个受阻组合的违规字段、触发值/缺失说明、拒收反例以及被它挡住的具体升级
动作。要打破死锁，需先在契约层面解耦（例如把 `inspector_id` 改为可选、或让
旧质检先放宽枚举域再分两轮升级）。

## 8. 测试

```bash
pip install pytest httpx
python -m pytest -q
```

覆盖：未知字段忽略、可选字段缺失、必填缺失、整数/字符串类型冲突、枚举越界、
无界 string 对 enum、反例消息完整性、可解顺序的订阅端优先约束、无解死锁报告、
起点不安全、入参校验，两个排程样例的 HTTP 端到端行为；以及批次整批原子入库、
跨批同号同内容去重、同号异内容（批内/跨批）整批 409 且无残留、重投幂等、
标识规范化、日报按车号×等级计数与事件号清单、请求号幂等返回原版、新请求号纳入
后到批次、旧版不可变、发布故障回滚不留半成品，以及补传/发布真并发下快照只含
完整批次、同一请求号并发重试只生成一版、重启后历史批次与版本仍可查。
