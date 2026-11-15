# 断面水质逐时氨氮采集平台

县站分页接口的模拟服务 + FastAPI 采集网页（SQLite 落库），并附带"采集中途县站更正记录"的完整样例。

## 要解决的问题

断面水质数据员要按 **断面 + UTC 日期** 从县站分页接口拉取某日逐时氨氮（NH₃-N）观测，交给污染复核组。县站可能在采集途中更正记录。平台保证：

1. **逐页核对修订号**：首页取得的 `revision` 被钉住，后续任何一页修订号不一致即中止。
2. **游标随修订失效**：县站更正后修订号递增，旧游标再用返回 `409 stale_cursor`，杜绝把两个修订版本的页拼成"弗兰肯数据"。
3. **当日且时刻唯一**：每行必须是该 UTC 日期的整点时刻，跨页出现重复时刻即中止。
4. **整日发布（all-or-nothing）**：收齐 `total_rows` 且收尾再次确认源端修订号未变，才在一个 SQLite 事务里替换整天；失败时旧版（若有）原样保留，不完整数据永远查不到。
5. **超时重试当前页**：网络错误、超时、5xx 只重试当前页（游标不前进），最多 3 次；`409` 属永久性错误不重试。
6. **双 Web 进程并发安全**：同一断面+日期同时只允许一个活动采集任务（SQLite 部分唯一索引，跨进程生效）；两个进程共享同一 SQLite 卷。
7. **迟到采集不能盖掉较新修订**：发布事务里比对已发布修订号，旧修订号写入直接拒绝（`StaleRevisionError`）。
8. **复核交接档不可复活**：县站更正后本地日档会被替换，因此交给外部实验室的 CSV 必须以**冻结档案**形式保存（`(断面,日期,修订号)` 唯一），登记修订号、行数和 SHA-256；重复归档返回原档，下载时校验大小与摘要，文件缺失/摘要不符直接报错，绝不从当前日档重建。

## 架构

```
docker compose
├── station  县站模拟接口（可切换/递增修订号，分页 + 游标）   :8001
├── web1     采集 Web 进程 A（FastAPI + SQLite WAL）          :8011
└── web2     采集 Web 进程 B（同一份代码，同一 SQLite 卷）    :8012

卷 quality-data:/data  -> 两个 web 进程共享 quality.db 与 handovers/ CSV 档案
```

### 县站接口（`station/`，本地模拟，带管理端用于演示更正）

| 方法/路径 | 说明 |
|---|---|
| `GET /metadata?section=&date=` | 当前修订号、总行数、页大小 |
| `GET /observations?section=&date=&cursor=` | 分页返回 `revision`、`total_rows`、`page`、`next_cursor` 和行（含 `source_page`） |
| `POST /admin/correct` | 更正某时刻数值：修订号 +1，旧游标立即失效 |
| `POST /admin/reset` | 该日数据重置回修订号 1 |
| `POST /admin/fail-next` | 下一次分页读取返回 500（一次性，演示重试） |
| `POST /admin/latency` | 调整每页模拟延迟（演示并发用） |

游标是携带 `(section, date, revision, page)` 的不透明 token；服务端发现 token 里的修订号与当前不符即返回 409。

### Web 接口（`web/`）

| 方法/路径 | 说明 |
|---|---|
| `GET /` | 采集网页：选断面、UTC 日期，触发采集，查看任务与已发布版本 |
| `GET /api/sections` | 县站支持的断面 |
| `POST /api/collect` | 启动后台采集（同断面+日期已有活动任务时返回 409） |
| `GET /api/runs/{id}` | 单个任务状态（running/success/failed/interrupted、修订号、行数、页尝试次数、错误） |
| `GET /api/runs?section=&date=` | 该日全部采集任务（含两个 worker 的记录） |
| `GET /api/published?section=&date=` | 已发布整日版本：时刻、氨氮值、来源页、修订号；无完整版本时 404 |

#### 复核交接档（`web/app/archive.py`）

| 方法/路径 | 说明 |
|---|---|
| `POST /api/handovers` | 冻结某已提交修订为交接 CSV：`{section, date, revision?}`；`revision` 省略取当前已发布修订。新建返回 `201`，同一 `(断面,日期,修订号)` 已归档则返回 `200` + 原档（`created=false`），ID/行数/SHA-256 与原档一致；请求未提交的修订号返回 404，不从当前日档重建。 |
| `GET /api/handovers?section=&date=` | 该断面/日期的全部档案（按修订号倒序）：修订号、行数、SHA-256、大小、归档时间/者、下载地址 |
| `GET /api/handovers/{id}` | 单个档案元数据 |
| `GET /api/handovers/{id}/download` | 下载冻结的 CSV（`Content-Disposition: attachment`，响应头带 `X-Handover-SHA256` / `X-Handover-Revision` / `X-Handover-Rows`）。下载前校验文件大小与 SHA-256：文件缺失或摘要不符返回 500，绝不回退到当前日档内容。 |

网页（`GET /`）在"已发布观测"下新增**复核交接档**面板：选断面/UTC 日期，可指定快照修订号（留空=当前已发布修订），一键生成或刷新档案列表，并逐个下载 CSV。

归档一致性设计：

- **单快照读取**：`_read_snapshot` 在一个 SQLite 读事务里同时取修订号、发布元数据和整日行；返回前再校验所有行修订号一致。归档与另一进程的整日发布并发时，只会看到某一个已提交修订的完整 24 行，不会拼出跨修订混合数据。
- **幂等与跨进程互斥**：`handovers` 表 `(section, date, revision)` 唯一索引；两实例同时归档只登记一个档案，另一请求返回原档并清理自己多写的文件。
- **不留残档**：CSV 先写临时文件 → `fsync` → 原子 `rename` → 登记事务；任一步失败都会删除已写文件，登记失败的回滚同样清理，因此中断后不存在可下载的半成品。恢复/重试会重新走完整流程；启动时 `cleanup_orphans` 仅清理超过 1 小时的孤儿文件，避免误删另一进程正在登记的文件。
- **只读下载**：下载读取登记的 `filename` 原始字节并核对 `size_bytes`/`sha256`，不查询 `published` 表，旧档不随日档更新改变。

演示钩子（仅当 `HANDOVER_TEST_HOOKS=1`，Compose 中两个 web 均开启）提供 `/api/_test/*`：一次性中断注入、篡改/删除档案文件、列出档案目录，用于自动化验证中断与损坏场景。生产部署应去掉该环境变量。

## 快速启动（Docker Compose）

```bash
./run-demo.sh                 # 构建、起栈、等待健康、自动跑全部演示检查
# 或手动：
docker compose up -d --build
# 网页 http://localhost:8011 （或 8012），县站文档 http://localhost:8001/docs
```

手动的中途更正样例（curl 逐步可观察）：

```bash
./demo/mid-correction.sh
```

### 不用 Docker 的本地运行

一键（自动起县站 + 两个 web 进程，跑完清理临时数据）：

```bash
python3 -I demo/demo.py --start
```

手动：

```bash
python3 -m venv .venv && .venv/bin/pip install -r web/requirements.txt -r station/requirements.txt
PAGE_SIZE=8 .venv/bin/python -m uvicorn station.app.main:app --port 8001 &
DB_PATH=$PWD/data/quality.db STATION_URL=http://127.0.0.1:8001 WORKER_NAME=web1 HANDOVER_TEST_HOOKS=1 \
  .venv/bin/python -m uvicorn web.app.main:app --port 8011 &
DB_PATH=$PWD/data/quality.db STATION_URL=http://127.0.0.1:8001 WORKER_NAME=web2 HANDOVER_TEST_HOOKS=1 \
  .venv/bin/python -m uvicorn web.app.main:app --port 8012 &
python3 -I demo/demo.py
```

## 演示场景（`demo/demo.py`，66 项断言）

1. **中途更正**：采集开始后县站更正 08:00 数值（修订号 1→2）→ 采集因 409 失败、无半成品公布、旧游标确认为 409；重新采集后整日按修订号 2 原子发布，24 行时刻唯一、来源页齐全。
2. **读取失败重试**：注入一次性 500 → 仅当前页重试（`page_attempts` 大于页数），最终成功发布。
3. **双进程并发 + 陈旧修订防护**：两进程同时采集同日 → 一个 202、一个 409；再构造"源端退回旧修订号的迟到采集"，发布事务拒绝旧修订号，已发布的新版本原样保留；另有直接对 DB 事务的防护单测。
4. **交接档**：两实例并发归档同一修订只登记一个档案；CSV 含时刻/数值/来源页，SHA-256 与整日内容一致；同修订重复归档（含另一进程、显式修订号）返回原档；县站更正并重新发布后旧档字节与摘要不变；未提交修订号被拒；写入后中断不留任何可下载残档、重试完成；篡改/删除文件时下载报 500 而非重建。
5. **归档与发布并发**：两个进程持续归档的同时连续发布修订 1→2→3，每个档案都恰好是某一已提交修订的完整 24 行观测，无跨修订混合。

另外验证过：worker 进程被 `kill -9` 后重启，遗留的 running 任务自动标记为 `interrupted`，可重新采集。

## 关键设计

- **发布事务**（`web/app/db.py: publish_version`）：`BEGIN IMMEDIATE` 下检查已发布修订号 → 删除旧整天数据 → 插入新整天 → 任务置成功，单事务提交；任何步骤异常都回滚。
- **活动任务互斥**：`runs` 表上 `WHERE status='running'` 的部分唯一索引，跨进程互斥，不依赖单机内存锁。
- **采集状态机**（`web/app/collector.py`）：metadata 钉修订号 → 逐页（重试仅当前页、校验修订号/总行数/页序/时刻唯一）→ 收齐行数 → 再查一次 metadata 复核修订号 → 整日发布。
- **崩溃恢复**：worker 启动时把本库遗留的 `running` 任务标记为 `interrupted`，避免互斥锁泄漏。
- **SQLite 并发**：WAL + `busy_timeout=30s`，两个 uvicorn 进程共享同一数据库文件。
- **交接档事务边界**（`web/app/archive.py`）：读事务取单一已提交快照 → 写临时文件并 fsync → 原子 rename → `BEGIN IMMEDIATE` 登记（唯一索引仲裁）；失败分支删除未登记文件，下载端只读文件并校验摘要。

## 目录

```
station/            县站模拟服务（FastAPI）
  app/main.py       分页/游标/修订号/管理端
  app/data.py       确定性 24 小时合成数据
web/                采集网页（FastAPI + SQLite）
  app/main.py       HTTP API 与网页
  app/collector.py  逐页核对、重试、整日发布状态机
  app/station_client.py
  app/db.py         SQLite 模式、互斥与发布事务
  app/archive.py    交接 CSV 冻结归档、快照读取、幂等登记、摘要校验
demo/demo.py        五场景自动化演示（python -I，仅标准库 + 本仓库 db/archive 模块）
demo/mid-correction.sh  curl 手动中途更正样例
docker-compose.yml  station + web1 + web2 + 共享卷
```
