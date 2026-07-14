# Phase 1 基础与实时看板实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 交付可在 Mac 和阿里云服务器运行的最小闭环：单管理员登录、最多 20 只自选、腾讯主行情、AKShare/东财备用行情、30 秒 Worker 轮询、SQLite 持久化、SSE 实时总览和仅本机绑定的 Docker Compose。

**Architecture:** FastAPI Web 与 APScheduler Worker 是同一 Python 包的两个入口。Web/API/Worker 只调用 Service，Service 通过 Repository 与 Provider 工作。Worker 将标准化行情写入 SQLite；Web 的 SSE 流按游标增量读取 SQLite，因此两个进程无需 Redis 即可同步。

**Tech Stack:** Python 3.12、FastAPI、Uvicorn、Jinja2、HTMX 2.0.7（本地静态文件）、Pydantic Settings、SQLAlchemy 2、Alembic、SQLite WAL、APScheduler 3、httpx、AKShare、exchange-calendars、argon2-cffi、pytest、pytest-asyncio、Docker Compose。

## Global Constraints

- 规格来源：[`docs/superpowers/specs/2026-07-14-personal-stock-watch-design.md`](../specs/2026-07-14-personal-stock-watch-design.md)。
- Phase 1 不实现告警、PushPlus、资讯、DeepSeek、持仓建议和全市场选股；只预留明确的 Service/Provider 边界。
- 应用时间统一存 UTC；交易时段判断用 `Asia/Shanghai`。
- 自选去重后最多 20 只；数量限制必须在数据库事务内验证，不能只依赖页面。
- 腾讯连续失败 3 次后短路 5 分钟并切换备用源；任一成功请求关闭对应失败状态。
- `source_timestamp` 距当前时间超过 90 秒的行情标记 `stale`，不得伪装成实时数据。
- Docker 只允许 `127.0.0.1:8000:8000`；不得添加 80、443 或数据库端口。
- 密码使用 Argon2id；Session 为 HttpOnly、SameSite=Strict；写请求校验 CSRF；登录失败限速。
- 用户未要求 Git 提交，因此每个任务末尾仅审查状态和差异，不执行 `git commit`。

---

## 目标文件结构

```text
stock_watch/
├── .dockerignore
├── .env.example
├── Dockerfile
├── README.md
├── alembic.ini
├── docker-compose.yml
├── pyproject.toml
├── alembic/
│   ├── env.py
│   ├── script.py.mako
│   └── versions/0001_phase1_foundation.py
├── app/
│   ├── __init__.py
│   ├── api/
│   │   ├── dependencies.py
│   │   ├── router.py
│   │   └── routes/{auth,health,quotes,watchlist}.py
│   ├── auth/{csrf,passwords,sessions}.py
│   ├── cli.py
│   ├── config.py
│   ├── db/{base,models,session}.py
│   ├── domain/{instruments,quotes}.py
│   ├── jobs/{quote_polling,worker}.py
│   ├── main.py
│   ├── providers/{base,akshare_eastmoney,tencent}.py
│   ├── repositories/{auth,instruments,job_runs,quotes,watchlist}.py
│   ├── services/{auth,quotes,trading_calendar,watchlist}.py
│   └── web/
│       ├── routes.py
│       ├── static/{app.css,app.js,htmx.min.js}
│       └── templates/{base,dashboard,login}.html
├── scripts/fetch_vendor_assets.sh
└── tests/
    ├── conftest.py
    ├── contract/fixtures/{eastmoney_etf.json,eastmoney_stock.json,tencent_quotes.txt}
    ├── contract/test_quote_providers.py
    ├── integration/{test_auth_api,test_compose_config,test_dashboard,test_database,test_health,test_job_runs,test_quote_flow,test_quote_sse,test_watchlist_api}.py
    └── unit/{test_circuit_breaker,test_quote_freshness,test_quote_schedule,test_sessions,test_watchlist_limit}.py
```

上述每个 Python 包目录都创建空的 `__init__.py`；测试目录依靠 pytest 发现机制，不创建包标记。

## 统一接口约定

以下接口在 Task 1～7 中逐步建立，后续任务不得绕过它们：

```python
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Protocol

class InstrumentKind(StrEnum):
    STOCK = "stock"
    ETF = "etf"

class Market(StrEnum):
    SH = "SH"
    SZ = "SZ"

@dataclass(frozen=True, slots=True)
class InstrumentRef:
    symbol: str
    market: Market
    kind: InstrumentKind

@dataclass(frozen=True, slots=True)
class Quote:
    instrument: InstrumentRef
    name: str
    price: Decimal
    previous_close: Decimal
    open_price: Decimal | None
    high: Decimal | None
    low: Decimal | None
    volume: Decimal | None
    amount: Decimal | None
    source_timestamp: datetime
    received_at: datetime
    provider: str
    field_completeness: float
    stale: bool

class QuoteProvider(Protocol):
    name: str

    async def fetch_quotes(
        self, instruments: Sequence[InstrumentRef]
    ) -> list[Quote]: ...
```

API 错误统一返回：

```json
{
  "error": {
    "code": "watchlist_limit_reached",
    "message": "自选与持仓合计最多 20 只",
    "details": {}
  }
}
```

---

### Task 1：建立可测试的 Python 应用骨架

**Files:**

- Create: `pyproject.toml`
- Create: `.env.example`
- Create: `app/__init__.py`
- Create: `app/api/__init__.py`
- Create: `app/api/routes/__init__.py`
- Create: `app/config.py`
- Create: `app/main.py`
- Create: `app/api/router.py`
- Create: `app/api/routes/health.py`
- Create: `tests/conftest.py`
- Create: `tests/integration/test_health.py`

- [ ] **Step 1：写失败的健康检查测试**

```python
def test_health_returns_phase_and_status(client):
    response = client.get("/api/v1/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "phase": 1,
    }
```

- [ ] **Step 2：运行测试并确认因应用尚不存在而失败**

Run: `python3.12 -m venv .venv`

Run: `.venv/bin/pip install -e '.[dev]'`

Run: `.venv/bin/pytest tests/integration/test_health.py -q`

Expected: FAIL，错误指向 `app.main` 或 `/api/v1/health` 尚未实现；不能是依赖安装失败。

- [ ] **Step 3：定义依赖与配置**

`pyproject.toml` 使用 `setuptools`，要求 Python `>=3.12,<3.13`。运行依赖固定兼容范围：FastAPI `<1`、Uvicorn `<1`、Jinja2 `<4`、SQLAlchemy `>=2,<3`、Alembic `<2`、Pydantic Settings `<3`、httpx `<1`、APScheduler `>=3.11,<4`、AKShare `<2`、exchange-calendars `>=4,<5`、argon2-cffi `<26`、python-multipart `<1`、itsdangerous `<3`；开发依赖为 pytest、pytest-asyncio、coverage 和 ruff。

`Settings` 至少包含以下字段，并使用 `STOCK_WATCH_` 前缀：

```python
class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="STOCK_WATCH_",
        env_file=".env",
        extra="ignore",
    )
    environment: Literal["development", "test", "production"] = "development"
    database_url: str = "sqlite:///./data/stock_watch.db"
    session_secret: SecretStr
    app_timezone: str = "Asia/Shanghai"
    quote_interval_seconds: int = 30
    quote_stale_seconds: int = 90
    quote_circuit_failures: int = 3
    quote_circuit_seconds: int = 300
```

`.env.example` 只给出生成方式和非敏感默认值；不得包含可用密钥。`session_secret` 可用 `openssl rand -hex 32` 生成。

- [ ] **Step 4：实现应用工厂和健康检查**

```python
def create_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or get_settings()
    app = FastAPI(title="波段雷达", version="0.1.0")
    app.state.settings = resolved
    app.include_router(api_router, prefix="/api/v1")
    return app

app = create_app()
```

Task 1 的健康检查只证明 Web 进程可响应，不访问外部网络。数据库探测在 Task 2 建立 Session 后加入，避免应用骨架越层创建临时数据库逻辑。

- [ ] **Step 5：运行定向测试与静态检查**

Run: `.venv/bin/pytest tests/integration/test_health.py -q`

Run: `.venv/bin/ruff check app tests`

Expected: 测试 PASS；ruff 无错误。

- [ ] **Step 6：检查任务差异**

Run: `git status --short && git diff --check`

Expected: 只有 Task 1 相关文件和已批准的文档为新增；`git diff --check` 无输出。

---

### Task 2：建立 SQLite WAL、核心模型和首个迁移

**Files:**

- Create: `app/db/base.py`
- Create: `app/db/session.py`
- Create: `app/db/models.py`
- Create: `alembic.ini`
- Create: `alembic/env.py`
- Create: `alembic/script.py.mako`
- Create: `alembic/versions/0001_phase1_foundation.py`
- Modify: `app/api/routes/health.py`
- Modify: `tests/conftest.py`
- Create: `tests/integration/test_database.py`
- Modify: `tests/integration/test_health.py`

- [ ] **Step 1：写迁移和约束测试**

测试创建临时 SQLite 文件，执行 `alembic upgrade head`，然后断言存在：`users`、`instruments`、`watchlist_items`、`quote_snapshots`、`job_runs`。另外验证：

```python
assert scalar("PRAGMA journal_mode").lower() == "wal"
assert unique_constraint_exists("instruments", ["symbol", "market"])
assert unique_constraint_exists("watchlist_items", ["instrument_id"])
assert index_exists("quote_snapshots", ["instrument_id", "source_timestamp"])
assert unique_constraint_exists(
    "quote_snapshots", ["instrument_id", "provider", "source_timestamp"]
)
assert trigger_exists("watchlist_items", "enforce_watchlist_limit_20")
assert unique_constraint_exists(
    "job_runs", ["job_name", "scheduled_trade_date", "version"]
)
```

健康检查测试同时更新为：数据库 `SELECT 1` 成功时返回 `database: "ok"`；连接失败时返回 HTTP 503、`status: "degraded"`、`database: "error"`。

- [ ] **Step 2：运行测试确认缺少模型和迁移**

Run: `.venv/bin/pytest tests/integration/test_database.py -q`

Expected: FAIL，明确指出迁移文件或目标表不存在。

- [ ] **Step 3：实现数据库连接和事务依赖**

连接 SQLite 时设置 `check_same_thread=False`、`timeout=30`。每次连接设置：

```sql
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=30000;
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
```

提供 `session_scope()` 上下文管理器：成功时 commit，异常时 rollback，最后 close。Web 的 `get_db_session()` 每请求只产生一个 Session。

SQLite 不原生保留时区，增加 `UTCDateTime` TypeDecorator：写入前把 aware datetime 转为 UTC 并去掉 tzinfo，读取后补回 `timezone.utc`；拒绝写入 naive datetime。所有模型时间列使用该类型。

- [ ] **Step 4：实现 Phase 1 模型**

字段约定：

- `users`: `id`、`username` 唯一、`password_hash`、`is_active`、`created_at`、`updated_at`。
- `instruments`: `id`、六位 `symbol`、`market`、`kind`、`name`、`lot_size`、`active`、时间戳；`symbol + market` 唯一。
- `watchlist_items`: `id`、`instrument_id` 唯一外键、`sort_order`、`note`、`created_at`；`BEFORE INSERT` 触发器在已有 20 行时执行 `RAISE(ABORT, 'watchlist_limit_reached')`。
- `quote_snapshots`: 标的外键、OHLC、现价、昨收、量额、`provider`、`source_timestamp`、`received_at`、`field_completeness`、`stale`；`instrument_id + provider + source_timestamp` 唯一，重试时不得重复写入同一快照。
- `job_runs`: `job_name`、`scheduled_trade_date`、`version`、`status`、`started_at`、`finished_at`、`error_code`；幂等三列唯一。

价格与金额使用 `Numeric`，时间列必须是 timezone-aware UTC。外键删除行为：删除标的前必须先移除自选和快照，不使用静默级联。

- [ ] **Step 5：实现并执行迁移**

Run: `.venv/bin/alembic upgrade head`

Run: `.venv/bin/alembic current`

Expected: 当前 revision 为 `0001_phase1_foundation (head)`。

- [ ] **Step 6：运行数据库测试**

Run: `.venv/bin/pytest tests/integration/test_database.py -q`

Expected: PASS，临时数据库和实际开发数据库都能升级到 head。

- [ ] **Step 7：检查任务差异**

Run: `git status --short && git diff --check`

Expected: 无未预期文件，无空白错误。

---

### Task 3：实现单管理员认证、Session、CSRF 与登录限速

**Files:**

- Create: `app/auth/passwords.py`
- Create: `app/auth/sessions.py`
- Create: `app/auth/csrf.py`
- Create: `app/repositories/auth.py`
- Create: `app/services/auth.py`
- Create: `app/api/dependencies.py`
- Create: `app/api/routes/auth.py`
- Create: `app/cli.py`
- Modify: `app/main.py`
- Create: `tests/unit/test_sessions.py`
- Create: `tests/integration/test_auth_api.py`

- [ ] **Step 1：写认证失败测试**

覆盖：正确密码登录、错误密码、5 次失败后 15 分钟锁定、未登录访问受保护 API 返回 401、缺少/错误 CSRF 返回 403、30 分钟无操作过期、8 小时绝对过期、退出后 Session 无效。

核心断言：

```python
assert login("admin", "correct").status_code == 204
assert client.get("/api/v1/watchlist").status_code == 200
assert anonymous.get("/api/v1/watchlist").status_code == 401
assert client.post("/api/v1/watchlist", json=payload).status_code == 403
```

- [ ] **Step 2：运行测试确认认证模块不存在**

Run: `.venv/bin/pytest tests/unit/test_sessions.py tests/integration/test_auth_api.py -q`

Expected: FAIL，错误来自缺少认证实现。

- [ ] **Step 3：实现密码和管理员初始化**

使用 `argon2.PasswordHasher` 的 Argon2id 默认安全参数，登录成功后若 `check_needs_rehash` 为真则更新哈希。CLI：

```text
python -m app.cli create-admin --username admin
```

密码只从隐藏交互输入或 `STOCK_WATCH_ADMIN_PASSWORD_FILE` 读取，禁止出现在命令参数、日志和 shell 历史中。已有管理员时默认拒绝覆盖。

- [ ] **Step 4：实现签名 Session 和限速**

Session Cookie 名 `stock_watch_session`，内容只包含 `user_id`、`issued_at`、`last_seen` 和随机 CSRF Token；设置 `HttpOnly`、`SameSite=Strict`、`Path=/`。生产环境经 SSH 隧道仍是浏览器到本机 HTTP，因此首版 `Secure=False`，README 明确只允许隧道访问。

登录限速按 `username + client_ip` 保存在进程内有界缓存；单用户服务重启会清空计数，但公网仅开放 SSH。达到 5 次失败后 15 分钟拒绝，成功登录清零。

- [ ] **Step 5：实现鉴权依赖和 CSRF**

- 受保护 API 调用 `require_user()`。
- POST/PUT/PATCH/DELETE 调用 `require_csrf()`，比较 `X-CSRF-Token` 与 Session 值并使用常量时间比较。
- 登录、健康检查是匿名端点；登录本身校验同源 `Origin/Host`，并限速。
- 不在响应、错误或日志中返回密码哈希和完整 Cookie。

- [ ] **Step 6：运行认证测试**

Run: `.venv/bin/pytest tests/unit/test_sessions.py tests/integration/test_auth_api.py -q`

Expected: PASS。

- [ ] **Step 7：检查任务差异**

Run: `git status --short && git diff --check`

Expected: 只增加认证链路；无真实密码或 Session secret。

---

### Task 4：实现标的规范化、自选仓储与 20 只事务限制

**Files:**

- Create: `app/domain/instruments.py`
- Create: `app/repositories/instruments.py`
- Create: `app/repositories/watchlist.py`
- Create: `app/services/watchlist.py`
- Create: `app/api/routes/watchlist.py`
- Modify: `app/api/router.py`
- Create: `tests/unit/test_watchlist_limit.py`
- Create: `tests/integration/test_watchlist_api.py`

- [ ] **Step 1：写规范化与上限测试**

覆盖：

```python
assert parse_instrument("600000") == InstrumentRef("600000", Market.SH, InstrumentKind.STOCK)
assert parse_instrument("000001") == InstrumentRef("000001", Market.SZ, InstrumentKind.STOCK)
assert parse_instrument("510300") == InstrumentRef("510300", Market.SH, InstrumentKind.ETF)
```

ETF 类型不能只凭代码永久推断：API 新增时允许显式传 `kind`，主数据返回后覆盖推断。两个并发事务同时从 19 只新增时，最终只能成功一个，另一个返回 `watchlist_limit_reached`。

- [ ] **Step 2：运行测试确认失败**

Run: `.venv/bin/pytest tests/unit/test_watchlist_limit.py tests/integration/test_watchlist_api.py -q`

Expected: FAIL，缺少领域解析或服务。

- [ ] **Step 3：实现 Repository 和 Service**

`WatchlistService` 公开：

```python
class WatchlistService:
    def list_items(self) -> list[WatchlistItemView]: ...
    def add_item(self, command: AddWatchlistItem) -> WatchlistItemView: ...
    def update_item(self, item_id: int, command: UpdateWatchlistItem) -> WatchlistItemView: ...
    def remove_item(self, item_id: int) -> None: ...
```

新增在同一事务中计数、upsert instrument、插入 watchlist。Service 的计数用于尽早返回友好错误，Task 2 的数据库触发器是并发兜底；捕获触发器产生的 `IntegrityError` 并映射为 409 `watchlist_limit_reached`。重复标的返回 409 `watchlist_item_exists`。

- [ ] **Step 4：实现 `/api/v1/watchlist`**

- `GET /api/v1/watchlist`：按 `sort_order, id` 返回。
- `POST /api/v1/watchlist`：接收 `symbol`、`market`、`kind`、`note`。
- `PATCH /api/v1/watchlist/{id}`：只更新排序和备注。
- `DELETE /api/v1/watchlist/{id}`：删除自选，不删除标的历史行情。

所有写请求要求登录和 CSRF。Pydantic 验证六位数字代码、市场枚举、备注最大 200 字。

- [ ] **Step 5：运行自选测试**

Run: `.venv/bin/pytest tests/unit/test_watchlist_limit.py tests/integration/test_watchlist_api.py -q`

Expected: PASS，包括并发上限、重复项和鉴权。

- [ ] **Step 6：检查任务差异**

Run: `git status --short && git diff --check`

Expected: API 没有直接访问 ORM；限制逻辑只存在于 Service/Repository 事务边界。

---

### Task 5：实现行情 Provider 契约、解析与新鲜度

**Files:**

- Create: `app/domain/quotes.py`
- Create: `app/providers/base.py`
- Create: `app/providers/tencent.py`
- Create: `app/providers/akshare_eastmoney.py`
- Create: `tests/contract/fixtures/tencent_quotes.txt`
- Create: `tests/contract/fixtures/eastmoney_stock.json`
- Create: `tests/contract/fixtures/eastmoney_etf.json`
- Create: `tests/contract/test_quote_providers.py`
- Create: `tests/unit/test_quote_freshness.py`

- [ ] **Step 1：保存脱敏 Fixture 并写契约测试**

Fixture 只保留 2 只股票和 1 只 ETF 的公开行情字段，不含 Cookie、Token、请求头和用户自选。所有 Provider 必须满足：

```python
quotes = await provider.fetch_quotes(instruments)
assert {q.instrument.symbol for q in quotes} == {"600000", "000001", "510300"}
assert all(q.source_timestamp.tzinfo is not None for q in quotes)
assert all(q.received_at.tzinfo is not None for q in quotes)
assert all(q.provider in {"tencent", "akshare_eastmoney"} for q in quotes)
assert all(0 <= q.field_completeness <= 1 for q in quotes)
```

测试还覆盖 GBK 解码、字段缺失、零昨收、停牌空价格、上游时间格式错误和单个标的缺失。

- [ ] **Step 2：运行契约测试确认失败**

Run: `.venv/bin/pytest tests/contract/test_quote_providers.py tests/unit/test_quote_freshness.py -q`

Expected: FAIL，缺少 Provider 和标准模型。

- [ ] **Step 3：实现腾讯批量 Provider**

请求 `https://qt.gtimg.cn/q=<comma-separated-codes>`，代码映射为 `sh600000`、`sz000001`、`sh510300`。使用 `httpx.AsyncClient`，连接超时 3 秒、读取超时 5 秒、总重试最多 2 次，仅对连接错误、超时、429 和 5xx 指数退避；解析与 4xx 不重试。

响应按 GBK 解码。每只标的独立解析，单只坏记录不丢弃同批其他记录；如果请求标的全无有效记录则抛 `ProviderUnavailable`。

- [ ] **Step 4：实现 AKShare/东财备用 Provider**

同步 AKShare 调用放入 `asyncio.to_thread`：股票调用 `stock_zh_a_spot_em()`，ETF 调用 `fund_etf_spot_em()`；按请求代码过滤后标准化。一次回退结果缓存 20 秒，避免连续故障时每 30 秒重复拉取全市场。

AKShare 返回表结构变化、空表或缺少必要列时抛稳定错误码 `upstream_schema_changed`，日志只记录 Provider、字段名与异常类型。

- [ ] **Step 5：实现新鲜度规则**

```python
def is_quote_stale(source_timestamp: datetime, now: datetime, threshold: int = 90) -> bool:
    if source_timestamp.tzinfo is None or now.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    age = (now.astimezone(UTC) - source_timestamp.astimezone(UTC)).total_seconds()
    return age < -5 or age > threshold
```

未来时间超过 5 秒同样视为异常 stale。字段完整度按必需/可选字段总数计算，但价格、昨收或时间缺失时整条记录无效。

- [ ] **Step 6：运行 Provider 测试**

Run: `.venv/bin/pytest tests/contract/test_quote_providers.py tests/unit/test_quote_freshness.py -q`

Expected: PASS，测试全程不访问网络。

- [ ] **Step 7：检查任务差异**

Run: `git status --short && git diff --check`

Expected: Fixture 无敏感数据，业务层未出现第三方字段名。

---

### Task 6：实现主备切换、短路器、行情落库和查询 API

**Files:**

- Create: `app/repositories/quotes.py`
- Create: `app/services/quotes.py`
- Create: `app/api/routes/quotes.py`
- Modify: `app/api/router.py`
- Create: `tests/unit/test_circuit_breaker.py`
- Create: `tests/integration/test_quote_flow.py`

- [ ] **Step 1：写主备与 stale 集成测试**

场景：主源成功；主源失败备用成功；连续 3 次主源失败后 5 分钟内不再调用主源；冷却后半开探测成功恢复；主备都失败保留旧值但不写伪快照；90 秒旧快照 API 返回 `stale: true`。

```python
result = await service.refresh_watchlist_quotes(now=fixed_now)
assert result.provider == "akshare_eastmoney"
assert result.saved_count == 3
assert primary.calls == 1
assert fallback.calls == 1
```

- [ ] **Step 2：运行测试确认失败**

Run: `.venv/bin/pytest tests/unit/test_circuit_breaker.py tests/integration/test_quote_flow.py -q`

Expected: FAIL，缺少行情 Service 和 Repository。

- [ ] **Step 3：实现进程内短路器**

状态为 `closed/open/half_open`；连续失败阈值 3，open 300 秒。一次成功立即归零；业务解析失败计入失败；单只缺失但批次有有效结果不打开短路器。Worker 只有一个实例，因此 Phase 1 不将短路状态写入数据库；重启后重新探测主源是安全行为。

- [ ] **Step 4：实现行情 Service 和 Repository**

`QuoteService.refresh_watchlist_quotes()`：读取自选标的 → 调主源/备用 → 重新计算新鲜度 → 单事务批量写入快照 → 返回刷新摘要。任何数据库异常必须 rollback，并报告 `saved_count=0`。

`QuoteService.get_latest_quotes()` 使用窗口查询或 `MAX(source_timestamp)` 为每只标的取最新一条；读取时基于当前时间重新计算 stale，不能只信写入时布尔值。

- [ ] **Step 5：实现查询 API**

- `GET /api/v1/quotes/latest`：返回全部自选最新行情。
- `GET /api/v1/quotes/latest/{instrument_id}`：返回单标的或 404。
- 每条记录包含 `provider`、`source_timestamp`、`received_at`、`stale`、`field_completeness`。
- 不提供写行情的公网 API；刷新只能由 Worker 或未来受保护管理命令触发。

- [ ] **Step 6：运行行情链路测试**

Run: `.venv/bin/pytest tests/unit/test_circuit_breaker.py tests/integration/test_quote_flow.py -q`

Expected: PASS。

- [ ] **Step 7：检查任务差异**

Run: `git status --short && git diff --check`

Expected: Provider 不写数据库；API 不调用 Provider；Service 是唯一编排层。

---

### Task 7：实现唯一 Worker、30 秒轮询和任务幂等记录

**Files:**

- Create: `app/repositories/job_runs.py`
- Create: `app/jobs/quote_polling.py`
- Create: `app/jobs/worker.py`
- Create: `app/services/trading_calendar.py`
- Create: `tests/unit/test_quote_schedule.py`
- Create: `tests/integration/test_job_runs.py`

- [ ] **Step 1：写交易时段和任务测试**

按 `Asia/Shanghai` 覆盖边界：09:29:59 false、09:30 true、11:30 true、11:30:01 false、12:59:59 false、13:00 true、15:00 true、15:00:01 false、周末 false。使用 `exchange_calendars` 的 `XSHG` 日历验证交易日，增加国庆节休市 false 与正常工作日 true 用例；日历超出库覆盖范围时返回 false 并记录 `calendar_out_of_range`，不能猜测为交易日。

任务测试断言同一 `job_name + date + version` 只能占用一次，并发第二次返回 `already_claimed`。行情任务的 `version` 包含预定槽位 `phase1-HHMMSS`，例如 `phase1-093000`、`phase1-093030`，两个 30 秒槽位不能相互阻塞。

- [ ] **Step 2：运行测试确认失败**

Run: `.venv/bin/pytest tests/unit/test_quote_schedule.py tests/integration/test_job_runs.py -q`

Expected: FAIL，缺少调度与幂等实现。

- [ ] **Step 3：实现轮询任务**

`TradingCalendarService` 封装 `exchange_calendars.get_calendar("XSHG")`，业务和 Worker 不直接依赖 pandas Timestamp。Phase 5 接入 Tushare `trade_cal` 时只替换该 Service 的数据实现。

APScheduler 使用 `BlockingScheduler(timezone="Asia/Shanghai")`，只在 `python -m app.jobs.worker` 入口创建。注册 interval 30 秒任务：

```python
scheduler.add_job(
    poll_quotes,
    trigger="interval",
    seconds=settings.quote_interval_seconds,
    id="quote_polling",
    max_instances=1,
    coalesce=True,
    misfire_grace_time=15,
)
```

每次触发先判断交易时段；非交易时段立即返回。行情轮询按预定触发时间向下对齐到 00/30 秒，以 `job_name=quote_polling`、交易日期和 `version=phase1-HHMMSS` 原子占用运行记录；同槽重入被拒绝，下一个 30 秒槽位使用不同 version 正常执行。

- [ ] **Step 4：实现优雅退出和日志**

SIGTERM/SIGINT 停止接收新任务，等待当前数据库事务结束后退出。日志输出任务名、开始/结束时间、主备 Provider、请求标的数、保存数、stale 数和稳定错误码；不记录完整上游响应。

- [ ] **Step 5：运行调度测试**

Run: `.venv/bin/pytest tests/unit/test_quote_schedule.py tests/integration/test_job_runs.py -q`

Expected: PASS。

- [ ] **Step 6：检查任务差异**

Run: `rg -n 'APScheduler|BlockingScheduler|add_job' app`

Expected: 调度器构造只出现在 `app/jobs/worker.py`，Web 进程没有注册任务。

---

### Task 8：实现登录页、实时总览和 SQLite 驱动 SSE

**Files:**

- Create: `app/web/routes.py`
- Create: `app/web/templates/base.html`
- Create: `app/web/templates/login.html`
- Create: `app/web/templates/dashboard.html`
- Create: `app/web/static/app.css`
- Create: `app/web/static/app.js`
- Create: `scripts/fetch_vendor_assets.sh`
- Create: `app/web/static/htmx.min.js`
- Modify: `app/main.py`
- Create: `tests/integration/test_dashboard.py`
- Create: `tests/integration/test_quote_sse.py`

- [ ] **Step 1：写页面与 SSE 失败测试**

覆盖：匿名访问 `/` 跳转 `/login`；登录页不泄露 Session 信息；登录后总览包含市场状态、数据时间、Provider、stale 标志、自选添加/删除控件；`GET /api/v1/stream/quotes?after_id=0` 返回 `text/event-stream` 并包含增量快照。

SSE 格式：

```text
id: 42
event: quote
data: {"instrument_id":1,"price":"10.23","stale":false}

```

- [ ] **Step 2：运行测试确认页面未实现**

Run: `.venv/bin/pytest tests/integration/test_dashboard.py tests/integration/test_quote_sse.py -q`

Expected: FAIL，缺少 Web 路由和 SSE。

- [ ] **Step 3：引入本地 HTMX 静态资源**

`scripts/fetch_vendor_assets.sh` 固定下载 `https://unpkg.com/htmx.org@2.0.7/dist/htmx.min.js` 到临时文件，验证 SHA-256 `60231ae6ba9db3825eb15a261122d5f55921c4d53b66bf637dc18b4ee27c79f9` 后原子替换 `app/web/static/htmx.min.js`；摘要不匹配必须失败，运行时不访问 CDN。

- [ ] **Step 4：实现 Jinja 页面**

界面名称“波段雷达”。总览首屏服务端渲染；添加/删除自选使用 HTMX 片段更新；所有写请求从 `<meta name="csrf-token">` 注入 `X-CSRF-Token`。HTML 对备注和名称使用默认转义，不允许 `|safe` 渲染上游内容。

总览必须明确区分：

- 实时：绿色，`age <= 90s`。
- 已过期：橙色，继续显示最后值与数据时间，但文案为“行情已过期，不产生新建议”。
- 暂无数据：灰色，不显示 0 元假价格。

- [ ] **Step 5：实现跨进程 SSE**

SSE 每 1 秒查询 `quote_snapshots.id > after_id`，每批最多 100 条；无数据每 15 秒发送注释心跳 `: keep-alive`；连接断开时结束生成器。`Last-Event-ID` 请求头优先于 query 参数。每个事件在发送前按当前时间重算 stale。

浏览器 EventSource 断线自动重连；收到 quote 后只更新对应 DOM 行。页面不可见时保持连接，由服务端心跳负责探活；首版用户只有一个浏览器，不增加连接池组件。

- [ ] **Step 6：运行页面和 SSE 测试**

Run: `.venv/bin/pytest tests/integration/test_dashboard.py tests/integration/test_quote_sse.py -q`

Expected: PASS。

- [ ] **Step 7：执行手工本地冒烟**

Run: `.venv/bin/uvicorn app.main:app --host 127.0.0.1 --port 8000`

Expected: 浏览器登录后能添加/删除自选；有 Fixture/测试注入行情时行内数据无需整页刷新；匿名浏览器不能打开总览。

- [ ] **Step 8：检查任务差异**

Run: `git status --short && git diff --check`

Expected: 页面无 CDN URL、无内联密钥、无未转义上游 HTML。

---

### Task 9：实现本地与服务器一致的 Docker Compose

**Files:**

- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Create: `.dockerignore`
- Modify: `.env.example`
- Create: `README.md`
- Create: `tests/integration/test_compose_config.py`

- [ ] **Step 1：写 Compose 安全约束测试**

解析 `docker compose config --format json`，断言：

```python
assert set(services) == {"web", "worker"}
assert web["ports"] == [{"host_ip": "127.0.0.1", "published": "8000", "target": 8000, "protocol": "tcp"}]
assert "ports" not in worker
assert web["image"] == worker["image"]
assert web["volumes"] == worker["volumes"]
```

并断言没有 80、443、PostgreSQL、Redis 或 Docker socket。

- [ ] **Step 2：运行测试确认 Compose 尚不存在**

Run: `.venv/bin/pytest tests/integration/test_compose_config.py -q`

Expected: FAIL，缺少 Compose 配置。

- [ ] **Step 3：实现单镜像双进程**

Dockerfile 基于 `python:3.12-slim`，创建非 root 用户，安装包后复制应用。Compose：

- `web` 启动前运行 `alembic upgrade head`，然后运行 Uvicorn；只绑定容器 `0.0.0.0:8000`，宿主发布固定为 `127.0.0.1:8000:8000`。
- `worker` 运行 `python -m app.jobs.worker`，不发布端口。
- `worker` 通过 `depends_on: web: condition: service_healthy` 等待迁移和健康检查完成，再启动调度器。
- 两者共享命名卷或绑定目录 `/app/data` 与 `/app/logs`。
- 两者使用同一 `.env`，但 Compose 配置和日志不打印 secret。
- Web healthcheck 调 `/api/v1/health`；Worker healthcheck 检查进程与最近运行状态，不依赖外网。
- 重启策略 `unless-stopped`，设置日志轮转 `10m × 3`。

- [ ] **Step 4：编写运行说明**

README 给出精确顺序：复制 `.env.example`、生成 Session secret、构建、迁移、创建管理员、启动、查看日志、停止。服务器访问命令固定为：

```bash
ssh -N -L 8000:127.0.0.1:8000 <server-user>@<server-ip>
```

尖括号仅表示用户必须替换的部署参数，不进入应用配置。README 明确阿里云安全组只开放来源为家庭公网 IP `/32` 的 TCP 22，不开放 80/443/8000。

- [ ] **Step 5：验证 Compose 配置和容器启动**

Run: `docker compose config && docker compose build && docker compose up -d && docker compose ps`

Expected: `web` 与 `worker` 均 healthy/running；端口显示 `127.0.0.1:8000->8000/tcp`，Worker 无端口。

- [ ] **Step 6：验证宿主端口**

Run: `curl --fail http://127.0.0.1:8000/api/v1/health`

Expected: HTTP 200，`status=ok`、`phase=1`、`database=ok`。

- [ ] **Step 7：检查任务差异**

Run: `git status --short && git diff --check`

Expected: `.env`、数据库、日志和容器数据未被 Git 跟踪。

---

### Task 10：完成 Phase 1 回归、在线冒烟与验收记录

**Files:**

- Create: `docs/acceptance/phase-1.md`
- Modify: `README.md`

- [ ] **Step 1：运行全部离线测试**

Run: `.venv/bin/pytest -q`

Expected: 全部 PASS，无测试访问公网。

- [ ] **Step 2：运行覆盖率和静态检查**

Run: `.venv/bin/coverage run -m pytest && .venv/bin/coverage report --fail-under=80 && .venv/bin/ruff check app tests`

Expected: 总覆盖率至少 80%，ruff 无错误。Provider 解析、20 只限制、短路器、新鲜度、鉴权和 CSRF 分支必须被覆盖，不能用排除标记绕过。

- [ ] **Step 3：执行受控在线行情冒烟**

新增一个不落库的 CLI `python -m app.cli test-quote-provider --symbol 510300 --market SH --kind etf`，分别测试腾讯主源和备用源。输出只包含 Provider、代码、数据时间、价格是否有效和字段完整度，不打印完整上游响应。

Expected: 至少主源成功；若备用源因临时网络限制失败，在验收记录中写明稳定错误码和时间，不把离线测试改成依赖网络。

- [ ] **Step 4：验证主备、stale 和 SSE**

在测试配置中使主 Provider 连续失败 3 次，确认 Worker 切备用；再注入超过 90 秒的 Fixture，确认 API 和页面同时显示 stale，且 SSE 事件也为 stale。

Expected: 备用成功时正常写库；主备失败不写伪快照；页面保留最后值并显示“行情已过期，不产生新建议”。

- [ ] **Step 5：验证资源占用**

用 20 只标的运行 Web + Worker 30 分钟，记录容器 CPU、内存、数据库增量和平均请求耗时。

Expected: 两个容器总内存稳定低于 1.5 GiB，无持续增长；30 秒轮询无重叠任务；SQLite 无 `database is locked`。

- [ ] **Step 6：记录 Phase 1 验收**

`docs/acceptance/phase-1.md` 写入：日期、环境、测试命令和实际输出摘要、在线 Provider 结果、端口绑定、资源数据、`exchange_calendars` 的日历覆盖截止日期及 Phase 2 进入条件。

- [ ] **Step 7：最终工作区审查**

Run: `git status --short && git diff --stat && git diff --check`

Expected: 只有 Phase 1 代码、测试、文档和已批准设计；无 `.env`、数据库、日志、缓存、构建产物或真实密钥。不得自动提交。

## Phase 1 完成定义

- [ ] 管理员可登录、退出，未登录与 CSRF 攻击被拒绝。
- [ ] 可新增、排序、备注和移除 A 股/ETF 自选；并发情况下仍不超过 20 只。
- [ ] Worker 仅在交易时段约每 30 秒抓取行情，Web 进程不注册调度任务。
- [ ] 腾讯主源失败 3 次后切换 AKShare/东财备用；短路冷却后可恢复。
- [ ] 行情写入 SQLite WAL，页面通过 SSE 增量更新；90 秒 stale 标志一致。
- [ ] Web 与 Worker 使用同一镜像；宿主只监听 `127.0.0.1:8000`。
- [ ] 离线测试、覆盖率、静态检查、Compose、安全端口和在线冒烟均有验收证据。

## 回滚边界

- Phase 1 只有 `0001_phase1_foundation` 初始迁移；开发期可删除测试数据库重建，服务器数据不得直接删除。
- 容器失败时停止 Compose 不影响代码；数据库文件和 WAL/SHM 必须一起保留，恢复前先停止 Web/Worker。
- Provider 在线失败时保持 Fixture 契约测试和 stale 降级，不以临时移除测试、扩大重试或降低超时作为修复。
- 任何需要开放 80/443/8000、切换公网 Web 或更改阿里云安全组范围的动作都超出本阶段授权，必须单独确认。
