# 实验室仪器数据接收 API

基于 **FastAPI + SQLite** 的仪器批量测量包接收服务。仪器按设备上报带序号的测量包，服务提供：

- **幂等去重**：每个包携带幂等键，重试 / 重发不会产生重复数据或重复告警事件；
- **乱序缓冲**：序号不连续的包先暂存，缺口补齐后**按序连续提交**（告警也按该提交顺序评估）；
- **可观测接口**：查询每台设备的缺口、已提交序号与重复包记录；
- **窗口聚合**：按事件时间（`sent_at`）把已提交测量分入半开窗口，输出 count / min / max / mean，并以水位机制隔离迟到样本；
- **持久化阈值告警**：按设备 + 指标配置上限 / 下限与连续触发、恢复次数，带连续计数的迟滞状态机防抖动，规则状态与告警 / 恢复事件持久化在 SQLite；
- **只读归档导出**：按设备 + seq 范围创建一次性导出任务，返回分页 NDJSON 游标与 manifest（包数、首尾序号、规范化内容 SHA-256），快照不可变、续传稳定，并提供摘要校验接口。

## 快速开始

### Docker Compose（推荐，单命令）

```bash
docker compose up --build
```

这一条命令完成全部初始化：构建镜像 → 容器内安装依赖（`docker-entrypoint.sh`）→
**显式执行 `db.init_db()` 初始化 SQLite 数据库与全部表结构**（含告警规则、规则状态、告警事件以及归档导出快照表，幂等可重复执行）
→ 应用启动钩子再次自动建表（双保险）→ 启动服务。
数据库文件持久化在命名卷 `lab-data`（容器内 `/data/lab.db`），重启不丢规则、事件与导出快照。

服务地址：http://localhost:8000 ，交互式文档：http://localhost:8000/docs

### 本地开发（单命令初始化 + 起服务）

```bash
pip install -r requirements-dev.txt   # 初始化依赖（含运行时与 pytest）
uvicorn app.main:app --reload         # 数据库默认创建在 ./lab.db，可用 DB_PATH 覆盖
```

另起终端运行测试：

```bash
pytest tests/
```

## 数据模型

仪器上报的测量包：

```json
{
  "device_id": "ph-meter-01",
  "seq": 12,
  "idempotency_key": "ph-meter-01-2026-09-22-0012",
  "sent_at": "2026-09-22T08:00:00Z",
  "measurements": [
    {"metric": "ph", "value": 7.01, "unit": "pH", "ts": "2026-09-22T07:59:58Z"},
    {"metric": "temperature", "value": 21.4, "unit": "C"}
  ]
}
```

- `seq`：包序号，**按设备**从 1 起单调递增、连续；
- `idempotency_key`：幂等键（也可通过 `Idempotency-Key` 请求头传入，二者至少提供一个）。

## 提交规则

每台设备独立维护 `committed_upto`（已连续提交的最大序号）：

1. **首包自举**：设备第一个到达的包立即提交，`committed_upto` 初始化为 `seq - 1`；
2. 之后到达的包：
   - `seq <= committed_upto` → 重复（`already_committed`）；
   - 已在缓冲区 → 重复（`already_buffered`）；
   - 否则放入缓冲区，并从 `committed_upto + 1` 起连续"排水"提交；
3. 同一幂等键重放 → 返回首次处理结果（`idempotent_replay`）；
4. 同一幂等键携带**不同载荷** → `409 Conflict`。

所有摄入操作在单事务内完成，保证并发下状态一致。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 |
| POST | `/api/v1/packages` | 上报测量包（幂等、乱序缓冲、按序提交） |
| GET | `/api/v1/devices` | 设备列表及状态汇总 |
| GET | `/api/v1/devices/{id}/gaps` | 缺口：缓冲区序号与缺失序号 |
| GET | `/api/v1/devices/{id}/committed` | 已提交序号（支持 `from_seq`/`to_seq`/`limit`/`offset`） |
| GET | `/api/v1/devices/{id}/duplicates` | 重复包记录（可按 `reason` 过滤） |
| GET | `/api/v1/devices/{id}/metrics/rollup` | 按事件时间的窗口聚合（count/min/max/mean + 水位迟到统计） |
| POST | `/api/v1/rules` | 创建阈值告警规则（按 设备+指标 唯一） |
| GET | `/api/v1/rules` | 规则列表（可按 `device_id`/`metric` 过滤） |
| GET | `/api/v1/rules/{rule_id}` | 规则详情 |
| PUT | `/api/v1/rules/{rule_id}` | 全量更新规则（上下限、连续次数、启用状态） |
| DELETE | `/api/v1/rules/{rule_id}` | 删除规则（历史事件保留） |
| GET | `/api/v1/alerts` | 当前仍处于告警中的规则（可按 `device_id`/`metric` 过滤） |
| GET | `/api/v1/devices/{id}/alerts/events` | 按设备、可选指标分页查询触发/恢复事件 |
| POST | `/api/v1/exports` | 创建只读归档导出任务（按设备 + seq 范围，返回 manifest 与首页游标） |
| GET | `/api/v1/exports/{id}/manifest` | 获取导出 manifest（包数、首尾序号、内容 SHA-256） |
| GET | `/api/v1/exports/{id}/pages` | 游标分页下载 NDJSON（`cursor`/`limit`） |
| POST | `/api/v1/exports/{id}/verify` | 提交下载内容的 SHA-256 摘要校验完整性 |

## 窗口聚合（Rollup）

`GET /api/v1/devices/{device_id}/metrics/rollup` 按**事件时间**（包的 `sent_at`）聚合**已提交**的测量：

| 参数 | 必填 | 说明 |
|---|---|---|
| `metric` | 是 | 指标名，只统计该指标的测量值 |
| `from` / `to` | 是 | 聚合范围 `[from, to)`，ISO 8601（不带时区按 UTC 处理） |
| `window_seconds` | 是 | 窗口宽度（秒），必须为正 |
| `allowed_lateness` | 否 | 允许的迟到时长（秒），默认 `0` |

语义：

- 窗口是以 `from` 为锚点的**半开区间** `[from + k·w, from + (k+1)·w)`，覆盖 `[from, to)`；窗口按 `start` 升序稳定返回，空窗口 `count=0` 且统计值为 `null`；
- **水位** `watermark = 设备已提交包的最大 sent_at - allowed_lateness`（设备级，与查询的指标、时间范围无关）；
- 迟到判定发生在**包提交时刻**：按提交顺序重放，若某包的 `sent_at` 早于当时水位，其样本为迟到样本——计入 `late_count`，**不改写已关闭窗口**的统计；
- 缓冲区中尚未提交的包不参与聚合；无 `sent_at` 的包无法分窗，跳过；
- 单次查询窗口数上限 10000，超出返回 `422`（请增大 `window_seconds` 或缩小范围）。

示例：

```bash
curl 'localhost:8000/api/v1/devices/ph-meter-01/metrics/rollup?metric=temperature&from=2026-09-22T08:00:00Z&to=2026-09-22T09:00:00Z&window_seconds=300&allowed_lateness=60'
# => {"device_id":"ph-meter-01","metric":"temperature","from":"...","to":"...",
#     "window_seconds":300.0,"allowed_lateness":60.0,
#     "watermark":"2026-09-22T08:59:00Z","late_count":0,
#     "windows":[{"start":"...","end":"...","count":3,"min":21.0,"max":21.6,"mean":21.3}, ...]}
```

## 阈值告警（Threshold Alerts）

按 **设备 + 指标** 配置规则（同一组合仅一条）：

| 字段 | 说明 |
|---|---|
| `upper_limit` / `lower_limit` | 上限 / 下限，**至少提供一个**；严格比较：`value > 上限` 或 `value < 下限` 才算越界，等于阈值不越界 |
| `trigger_count` | 连续越界多少次后触发告警（默认 1） |
| `recovery_count` | 告警后连续多少次不越界才恢复（默认 1），与触发阈值相互独立，形成迟滞 |
| `enabled` | 是否启用；禁用期间不评估，重新启用不回溯历史包 |

### 迟滞状态机（防抖动）

规则状态持久化在 `alert_rule_states`，每条规则一个状态机：

- **normal**：维护连续越界计数 `breach_streak`，越界 +1、不越界清零；达到 `trigger_count` 时翻转为 **triggered** 并写入一条 `triggered` 事件；
- **triggered**：维护连续恢复计数 `recovery_streak`，不越界 +1、再次越界清零（抖动被吸收，不会反复告警）；达到 `recovery_count` 时翻回 **normal** 并写入一条 `recovered` 事件；
- 包内没有该指标的测量值时该规则**跳过、计数不变**（缺数不等于恢复）；一个包内含同一指标多个测量值时，按 measurements 的提交顺序逐个推进状态机。

### 评估时机：只认提交顺序

- 评估只发生在包**按序排水提交**时：缓冲中的包不评估，重复包（幂等重放 / `already_committed` / `already_buffered`）直接短路，**绝不重复产生事件**；
- 乱序缺口被补齐时，缓冲包按 seq 升序一次性排水，每个包**只按这一次提交顺序**评估；
- 规则在建规则之前已提交的历史包**不回溯**；
- 规则配置被更新时，保留当前是否告警的状态，但清零两个连续计数，避免沿用旧阈值的计数误判；
- 删除规则会级联删除其状态（当前告警消失），历史事件保留、`rule_id` 置空。

事件的**时间与序号**：

- 事件序号 `id` 为全表单递增自增，即按实际提交处理顺序编号；`seq` 为触发 / 恢复时处理的包序号（同包内多个测量值可触发+恢复，序号相同、`id` 不同）；
- 事件时间优先取测量值 `ts`，其次取包的 `sent_at`，均无则用提交时刻（UTC，`Z` 结尾）。

### 接口示例

创建规则（温度上限 30：连续 2 次越界触发，连续 2 次回限内恢复）：

```bash
curl -X POST localhost:8000/api/v1/rules -H 'Content-Type: application/json' -d '{
  "device_id": "ph-meter-01", "metric": "temperature",
  "upper_limit": 30, "trigger_count": 2, "recovery_count": 2
}'
# => 201 {"id":1,"device_id":"ph-meter-01","metric":"temperature",
#        "upper_limit":30.0,"lower_limit":null,"trigger_count":2,
#        "recovery_count":2,"enabled":true,"created_at":"...","updated_at":"..."}
```

更新（全量替换）/ 删除：

```bash
curl -X PUT localhost:8000/api/v1/rules/1 -H 'Content-Type: application/json' -d '{
  "upper_limit": 40, "lower_limit": 0, "trigger_count": 3,
  "recovery_count": 2, "enabled": true
}'
curl -X DELETE localhost:8000/api/v1/rules/1      # 204；历史事件保留
```

上报测量包（接口与前述完全一致，提交时自动评估）：

```bash
curl -X POST localhost:8000/api/v1/packages -H 'Content-Type: application/json' -d '{
  "device_id": "ph-meter-01", "seq": 1, "idempotency_key": "k-1",
  "measurements": [{"metric": "temperature", "value": 31.2, "ts": "2026-09-22T08:00:00Z"}]
}'
```

查询当前告警（触发后、恢复前）：

```bash
curl 'localhost:8000/api/v1/alerts?device_id=ph-meter-01'
# => {"count":1,"alerts":[{"rule_id":1,"device_id":"ph-meter-01","metric":"temperature",
#        "upper_limit":30.0,"lower_limit":null,"trigger_count":2,"recovery_count":2,
#        "triggered_seq":4,"triggered_event_id":7,"triggered_at":"2026-09-22T08:00:04Z",
#        "last_value":33.0,"last_seq":4,"recovery_streak":0}]}
```

按设备、指标分页查询事件（`limit`/`offset`，可按 `event_type=triggered|recovered` 过滤）：

```bash
curl 'localhost:8000/api/v1/devices/ph-meter-01/alerts/events?metric=temperature&limit=50&offset=0'
# => {"count":2,"limit":50,"offset":0,"events":[
#       {"id":7,"rule_id":1,"device_id":"ph-meter-01","metric":"temperature","seq":4,
#        "event_type":"triggered","value":33.0,"event_time":"2026-09-22T08:00:04Z","created_at":"..."},
#       {"id":8,"rule_id":1,"device_id":"ph-meter-01","metric":"temperature","seq":8,
#        "event_type":"recovered","value":27.0,"event_time":"2026-09-22T08:00:08Z","created_at":"..."}]}
```

### 示例

上报乱序包（seq 2 先到，进入缓冲）：

```bash
curl -X POST localhost:8000/api/v1/packages -H 'Content-Type: application/json' -d '{
  "device_id": "ph-meter-01", "seq": 2, "idempotency_key": "k-2",
  "measurements": [{"metric": "ph", "value": 7.01}]
}'
# => {"status":"buffered","committed_upto":0,"committed_seqs":[],...}
```

查询缺口：

```bash
curl localhost:8000/api/v1/devices/ph-meter-01/gaps
# => {"committed_upto":0,"next_expected":1,"buffered_seqs":[2],"gaps":[1]}
```

补齐缺口后按序提交：

```bash
curl -X POST localhost:8000/api/v1/packages -H 'Content-Type: application/json' -d '{
  "device_id": "ph-meter-01", "seq": 1, "idempotency_key": "k-1",
  "measurements": [{"metric": "ph", "value": 6.98}]
}'
# => {"status":"committed","committed_seqs":[1,2],"committed_upto":2,...}
```

重复重放与查询：

```bash
curl -X POST localhost:8000/api/v1/packages -H 'Content-Type: application/json' -d '<同上请求体>'
# => {"status":"duplicate","duplicate_reason":"idempotent_replay",...}

curl localhost:8000/api/v1/devices/ph-meter-01/committed    # 已提交序号
curl localhost:8000/api/v1/devices/ph-meter-01/duplicates   # 重复包记录
```

## 只读归档导出（Archive Export）

对一台设备的**已提交（committed）**测量包按 seq 闭区间创建一次性导出任务。导出是
**只读 + 快照**操作：任务创建时把范围内的包从 `committed` 复制到导出专用快照表
（`export_items`），此后无论缓冲区如何排水、`committed` 如何增长、是否继续产生重复包
或告警事件，**同一任务用游标续传得到的页面字节与 manifest 摘要永远不变**；导出全程
不写 `devices` / `committed` / `buffer` / `duplicates` / `alert_*` 中的任何数据，
也不驱动告警评估。

> 只能导出创建时刻**已连续提交**的序号（即设备当前已提交区间之内）；缓冲区中的包不导出。
> 乱序自举设备（首包 seq=10）的已提交区间就是 `[10, …]`，请求越界返回 `422`。

### 1）创建任务

```bash
curl -X POST localhost:8000/api/v1/exports -H 'Content-Type: application/json' -d '{
  "device_id": "ph-meter-01",
  "from_seq": 1,
  "to_seq": 500,
  "page_size": 100
}'
# => 201
# {
#   "manifest": {
#     "export_id": 1,
#     "device_id": "ph-meter-01",
#     "from_seq": 1, "to_seq": 500,
#     "page_size": 100,
#     "package_count": 500,
#     "first_seq": 1, "last_seq": 500,
#     "content_sha256": "9f2d…（64 位十六进制）",
#     "content_bytes": 182540,
#     "created_at": "2026-09-23T03:10:00.000Z"
#   },
#   "first_cursor": "eyJlIjoxLCJvIjowfQ"
# }
```

| 字段 | 说明 |
|---|---|
| `from_seq` / `to_seq` | 导出序号闭区间，需 `1 <= from_seq <= to_seq`，且完全落在设备已提交区间内 |
| `page_size` | 默认每页包数（1–1000，默认 100）；取页时可用 `limit` 临时覆盖，不改变内容 |
| `package_count` / `first_seq` / `last_seq` | manifest：包数、首尾序号 |
| `content_sha256` | manifest：**全部页面 NDJSON 字节按序拼接**后的 SHA-256 |
| `first_cursor` | 首页游标（等价于从 offset 0 开始） |

错误：设备不存在 → `404`；范围非法或越出已提交区间 → `422`（detail 中给出当前合法范围）。

### 2）游标分页下载 NDJSON

```bash
curl -i 'localhost:8000/api/v1/exports/1/pages?cursor=eyJlIjoxLCJvIjowfQ'
# HTTP/1.1 200 OK
# content-type: application/x-ndjson; charset=utf-8
# x-export-id: 1
# x-offset: 0
# x-page-size: 100
# x-package-count: 100
# x-has-more: true
# x-next-cursor: eyJlIjoxLCJvIjoxMDB9
# x-content-sha256: 9f2d…
#
# {"device_id":"ph-meter-01","measurements":[…],"seq":1,"sent_at":"…"}
# {"device_id":"ph-meter-01","measurements":[…],"seq":2,"sent_at":"…"}
# …
```

- 每行一个测量包的**规范化 JSON**：键排序（`sort_keys`）、非 ASCII 原样保留
  （`ensure_ascii=False`），与摄入时的规范化规则一致；行以 `\n` 分隔，**每个响应都以
  `\n` 结尾**，因此各页响应体直接拼接即为完整归档；
- 首页可省略 `cursor`；之后把上一页响应头 `X-Next-Cursor` 的值原样传回即可续传；
  末页 `X-Has-More: false` 且不带 `X-Next-Cursor`；
- 同一游标**任意次重复取、或换 `limit` 重新分页**：页边界可以不同，但所有页面按序拼接
  后的字节流与 `content_sha256` 必须一致——下载中断后直接重取当前游标即可；
- `limit` 范围 1–1000；游标无法解析、属于其他任务、offset 越过末页 → `422`，
  导出任务不存在 → `404`（任务存在性先于游标检查）。

### 3）校验完整性

客户端逐页保存响应体并按下载顺序拼接，本地计算 SHA-256 后提交比对：

```bash
# 按 X-Next-Cursor 循环取页并按序拼接为 export.ndjson
sha256sum export.ndjson     # => 9f2d…  export.ndjson

curl -X POST localhost:8000/api/v1/exports/1/verify \
  -H 'Content-Type: application/json' \
  -d '{"content_sha256": "9f2d…"}'
# => 200
# {"export_id":1,"valid":true,
#  "submitted_sha256":"9f2d…","expected_sha256":"9f2d…",
#  "package_count":500,"content_bytes":182540}
```

提交值接受大写与前后空白（服务端 strip 后统一转小写比对）；摘要不匹配时返回
`200` 且 `valid: false`（语义为"服务可达但内容不一致"，由客户端决定如何处置）；
摘要不是 64 位十六进制 → `422`，任务不存在 → `404`。

## 项目结构

```
app/
  main.py     # FastAPI 路由
  logic.py    # 摄入核心：幂等判定、缓冲、排水提交（提交时驱动告警评估）
  alerts.py   # 阈值告警：规则 CRUD、带连续计数的迟滞状态机、按提交顺序评估
  rollup.py   # 窗口聚合：半开窗口分桶、水位迟到判定
  exports.py  # 只读归档导出：快照固化、游标编解码、NDJSON 分页、manifest 摘要校验
  db.py       # SQLite 连接与建表（启动时 / 容器入口自动执行）
  schemas.py  # Pydantic 模型
tests/
  conftest.py     # 测试数据库隔离（临时文件）
  test_api.py     # 摄入 / 查询接口测试
  test_rollup.py  # 窗口聚合测试
  test_alerts.py  # 阈值告警测试
  test_exports.py # 只读归档导出测试（快照稳定性、游标越界、摘要校验、只读性）
Dockerfile / docker-entrypoint.sh / docker-compose.yml
requirements.txt / requirements-dev.txt
```

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `DB_PATH` | 容器内 `/data/lab.db`，本地 `./lab.db` | SQLite 数据库文件路径 |
| `PORT` | `8000` | 服务监听端口（entrypoint 使用） |
