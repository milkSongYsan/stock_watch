# 波段雷达开发路线图

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 以六个可独立验收的阶段交付个人 A 股与场内 ETF 盯盘、资讯分析、风险建议和收盘选股系统。

**Architecture:** 保持一个仓库、一个镜像、Web 与 Worker 两个进程的 API 优先模块化单体。页面、API 与定时任务只调用 Service；Service 通过 Repository 和 Provider 访问 SQLite 与第三方接口。

**Tech Stack:** Python 3.12、FastAPI、Jinja2、HTMX、SSE、SQLAlchemy 2、Alembic、SQLite WAL、APScheduler、httpx、Polars/NumPy、pytest、Playwright、Docker Compose。

## Global Constraints

- 详细设计以 [`docs/superpowers/specs/2026-07-14-personal-stock-watch-design.md`](../specs/2026-07-14-personal-stock-watch-design.md) 为唯一产品规格来源。
- 只供单个个人用户研究；不连接券商、不自动下单、不公开荐股。
- Web 只绑定服务器 `127.0.0.1:8000`，通过 SSH 隧道访问；不得增加公网 HTTP 端口。
- Web 与 Worker 复用 Service、Repository、Provider 和数据库模型，Worker 是唯一调度器。
- 外部接口全部有超时、有限重试、降级、脱敏日志和可测试 Provider 契约。
- 行情超过 90 秒时停止产生新建议与新告警。
- 不引入 Redis、Celery、PostgreSQL、Node 构建或独立 SPA。
- 每个阶段先写失败测试，再写最小实现；阶段验收通过后才开始下一阶段。
- 用户未要求 Git 提交，本路线图和各阶段计划不包含自动提交；每个任务只检查 `git status --short` 与 `git diff`。

---

## 阶段与交付物

| 阶段 | 可工作的增量 | 关键验收 |
| --- | --- | --- |
| Phase 1 | 基础、鉴权、自选、实时行情、SSE 总览、Docker | 登录后可维护最多 20 只标的；交易时段约 30 秒更新；主源失败可切备用；页面显示 stale |
| Phase 2 | 技术指标、规则告警、降噪、PushPlus | 持续条件不重复推送；恢复后可再次触发；通知失败不丢事件 |
| Phase 3 | 巨潮、博查、东财资讯、预算、DeepSeek、详情页 | 资讯可追溯；博查日硬限额生效；AI 失败保留规则结果 |
| Phase 4 | 账户、持仓、风险与波段建议 | 1% 单笔风险与 20% 仓位上限正确；过期或无效数据不给数量 |
| Phase 5 | 全市场日线、四策略选股、候选复核、每日总结 | 2C4G 上 20 分钟内完成；只有前 10 名调用 AI；候选需人工确认 |
| Phase 6 | 故障演练、资源压测、备份恢复、运维与正式部署 | 端口仅 SSH；备份真实恢复成功；重启不重复任务/推送；预算与密钥脱敏 |

## 依赖关系

```text
Phase 1 基础与实时看板
  └─ Phase 2 规则告警与微信
       └─ Phase 3 资讯与 AI
            ├─ Phase 4 持仓与建议
            └─ Phase 5 收盘选股
                 └─ Phase 6 加固与交付
```

Phase 4 和 Phase 5 都依赖 Phase 3 的资讯、预算和 AI Provider，但彼此业务逻辑独立。为了减少数据库迁移与页面冲突，仍按 Phase 4 后 Phase 5 顺序落地。

## 计划文件策略

- Phase 1 的详细计划随本路线图创建：`2026-07-14-phase-1-foundation-dashboard.md`。
- Phase 2～6 在上一阶段验收后分别创建详细计划，届时以实际代码结构、迁移版本和测试结果为依据，不预先假设尚未存在的接口。
- 每份阶段计划必须列出精确文件路径、测试用例、实现接口、执行命令、预期结果和回滚边界。

## 总体验收门

- [ ] 单元、Provider 契约、数据库/API 集成、调度和关键 Web 测试通过。
- [ ] 生产 Compose 只发布 `127.0.0.1:8000:8000`，服务器安全组无 80/443/8000 入站。
- [ ] 腾讯主源失败后自动使用备用源，主备失败时 90 秒 stale 保护生效。
- [ ] 博查达到 ¥10/日、DeepSeek 达到 ¥20/月后自动请求硬停止。
- [ ] 同一交易日选股、AI 复核、每日总结在重启后不重复。
- [ ] SQLite 备份通过全新目录的恢复演练。
- [ ] 所有页面、日志和 API 均不回显密钥、Token 或完整认证请求头。
