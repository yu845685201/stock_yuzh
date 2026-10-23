# AI-Trader 深度分析

> **分析日期**：2026-10-11 · **仓库**：[HKUDS/AI-Trader](https://github.com/HKUDS/AI-Trader)（本地 HEAD `d03ff6c`，2026-06-11）
> **分析依据**：本地克隆源码（`refrences/HKUDS__AI-Trader`），精读 `skills/*/SKILL.md`、`service/server/{price_fetcher,signal_quality,challenge_scoring,fees,market_intel}.py`、`service/server/{main,routes_*}.py` 与 README。所有结论来自源码实现与 SKILL 文档，非推断。
> **配套文档**：[开源股票分析项目深度分析-20261011](./开源股票分析项目深度分析-20261011.md)

---

## 0 定位与架构总览

AI-Trader 不是"分析引擎"，而是一个 **Agent-Native 交易平台**（线上版 ai4trade.ai + 开源自托管）。定位："人类有自己的交易平台，AI agent 也该有自己的"。

核心机制：任何 AI agent（OpenClaw/Claude Code/Codex/Cursor/nanobot 等）只要读一段 SKILL.md 就能自助注册、领 $100,000 模拟资金、发交易信号/策略/讨论、跟单、参加挑战赛、赚积分——**AI 之间协作与辩论产生集体智能**（Collective Intelligence Trading）。

```
AI Agent ──读 SKILL.md──▶ 自助注册(selfRegister)→JWT token
   │                         │
   ├─ 发信号(/api/signals/realtime|strategy|discussion)
   ├─ 跟单(/api/signals/follow → /api/positions 镜像复制)
   ├─ 挑战赛(/api/challenges/{key}/join|trade|submit + 团队)
   └─ 心跳轮询(/api/claw/agents/heartbeat) 收消息/任务
            ▼
   service/server/ (FastAPI) ── worker(后台任务) ── DB(SQLite/PostgreSQL)
```

### 三条并行主线

| 主线 | 目录/文件 | 说明 |
|---|---|---|
| 信号与跟单 | `routes_signals.py` / `routes_trading.py` | 四类信号 + follow + 复制持仓 |
| 挑战/竞赛 | `challenges.py` / `challenge_scoring.py` / `team_*` | 个人/团队/混合赛 + 持仓重放打分 |
| 实验框架 | `experiments.py` / `experiment_metrics.py` / `experiment_events.py` | A/B 变体、曝光追踪、自动结算 |
| 市场情报 | `market_intel.py` / `price_fetcher.py` | 统一行情快照 + 三市场行情源 |
| 研究导出 | `research/` | 网络边/agent 特征/指标 数据集构建 |

### 信号类型（SKILL.md）

`position`（当前持仓）/ `trade`（含 PnL 的已了结交易）/ `strategy`（策略分析）/ `discussion`（讨论帖）。三市场：`us-stock` / `crypto` / `polymarket`；动作 `buy/sell/short/cover`（polymarket 只支持 buy/sell）。

---

## 1 数据源来源（`price_fetcher.py`）

| 市场 | 主源 | 兜底 | 说明 |
|---|---|---|---|
| us-stock | Alpha Vantage `TIME_SERIES_INTRADAY`（1min，实时） | yfinance（1m→1d 两级兜底） | AV key 缺失/限流/无可用价时落到 yfinance |
| crypto | Hyperliquid 公开 info 端点（`l2Book`/`candleSnapshot`） | — | 已弃用 Alpha Vantage crypto 端点 |
| polymarket | Gamma API（市场发现 + `outcomePrices`）+ CLOB（`/book` 订单簿） | Gamma `lastTradePrice/outcomePrice` | 价格取最优买/卖价中点 |

### 行情可靠性设计（`price_fetcher.py` 里值得抄的实现）

- **每 provider 冷却**（`_activate_provider_cooldown`）：HTTP 429 → 冷却 60s（`PRICE_FETCH_RATE_LIMIT_COOLDOWN_SECONDS`）；5xx → 冷却 20s；超时/连接错误 → 冷却 20s。冷却期内的请求直接抛错，不堆积。
- **指数退避 + 抖动**：`_retry_delay` = `base × 2^attempt + uniform(0, base×0.25)`，`base=0.35s`，重试上限 `MAX_RETRIES=2`。
- **重试白名单状态码**：`{429, 500, 502, 503, 504}`。
- **Hyperliquid 符号缓存**：`/info` meta 拉取 unlias 清单，TTL 300s（失败 30s），未列出的符号直接报"not listed"。
- **Polymarket 价格守卫**：`_polymarket_price_valid` 强制 `0 ≤ price ≤ 1`（概率），把 token_id 等噪声（如 `1.5e+73`）挡在价格之外；`outcome`（Yes/No）解析到具体 `token_id` 才报价。
- **美股时区**：Alpha Vantage 返回 ET 时间，`_get_us_stock_price` 先 UTC 再转 ET 精确匹配 1min bar，找不到取"最接近的之前"bar（候选循环取 min diff）。
- **yfinance 兜底**：预处理 MultiIndex 列、tz_localize(ET)→convert(UTC)，`Close/Adj Close` 逐列探测，取 target 之前的最近收盘。

---

## 2 分析逻辑与"智能"机制

### 2.1 信号质量启发式打分（`signal_quality.py:score_signal_quality`）

对每个 strategy/discussion/operation 信号打 5 维分（`overall = verifiability*0.3 + evidence*0.25 + specificity*0.2 + novelty*0.15 + review*0.1`，全 clamp 到 [0,5]）：

| 维度 | 计算 |
|---|---|
| verifiability（可验证性） | 1.0 底 + 有方向 +1.2 + 有 symbol +0.8 + 有目标价/目标概率 +1.2 |
| evidence（证据） | `min(5, len/160 + (含 because/risk/evidence/data/chart/catalyst)*0.7)` |
| specificity（具体性） | 1.0 + 有 symbol +1 + 有 tags +1 + `min(len/320, 2)` |
| novelty（新颖性） | 非重复 5.0，重复则 `max(0.5, 5-重复次数)` |
| review（评审） | 1.0 + 被采纳回复（accepted_reply_id 非空）+2.0 |

预测抽取（`extract_prediction_from_signal`）用关键词正则：方向（buy/long/bull/upside/breakout/看多→up 等）、`target|tp|目标价|price` 后跟数字→目标价、`confidence` 后数字→置信度（>1 视为百分数除 100）。产出入 `signal_predictions` 表（horizon/invalid_if 字段预留但当前为 None）。`MODEL_VERSION = "heuristic-v1"`。

### 2.2 挑战赛持仓重放打分（`challenge_scoring.py:score_agent_trades`）

逐笔按 `executed_at` 排序**重放**持仓，产出 `return_pct / max_drawdown / risk_adjusted_score / final_score / trade_count / disqualified_reason`：

- 多头 PnL `mark*qty`；空头（卖空）持仓值 `(2*entry - mark)*|qty|`，`cover` 现金回补 `((2*entry)-price)*qty`。
- **逐笔违规判定**（写死规则，命中即 `disqualified_reason` 并 break）：`buy_used_while_short` / `sell_exceeds_challenge_long` / `short_used_while_long` / `cover_exceeds_challenge_short` / `unsupported_side` / `invalid_trade_snapshot`（价或量 ≤0）/ `max_position_pct_exceeded`。
- 回撤：`update_drawdown` 用峰值计算最大回撤；`rules.disqualify_on_drawdown` 时超 `max_drawdown_pct` 判违规。
- `scoring_method`：`return-only`（final=return_pct）或 `risk-adjusted`（`final = return_pct - max(0, max_drawdown - allowed_drawdown)*drawdown_penalty`）。
- 结算：可传 `mark_prices`（当前标记价）对持仓做**最后一个时点的市值重估**（`marked_to_market`）。

`rank_scored_results` 只对"未违规 + final_score 非空 + trade_count>0"的候选排名。

### 2.3 团队赛 + 投票（SKILL.md team flow）

`mode=team/hybrid` 挑战：建队（captain/risk 等角色）、`team_thesis/risk_review/trade_proposal/trade_approval/post_trade_review` 五类提交、按 submission 投票 `approve/reject/revise`（每人一队一票，contributing agent 记录在 team trade 上以做贡献分析）。

### 2.4 实验框架（`experiments.py` / `experiment_metrics.py`）

A/B 变体实验：`variant_key`（如 control）、`MONTHLY_CHALLENGE_EXPERIMENT_KEY` 月度赛、曝光追踪（prompt 曝光与显式消息读取分开计量）、过期实验在读取时自动完成、实验台用与排行榜相同的 live mark-to-market 打分展示变体表现。

### 2.5 心跳与通知（SKILL.md heartbeat）

Agent 每 30–60s `POST /api/claw/agents/heartbeat` 拉最多 50 条未读消息 + 10 个待处理任务；响应带 `recommended_poll_interval_seconds` + `has_more_messages/has_more_tasks`（控制是否立即再拉）。WebSocket `wss://ai4trade.ai/ws/notify/{client_id}` 推 9 类通知（new_follower/discussion_reply/strategy_reply_accepted…）。SKILL 强调"heartbeat 不是可选，不轮询就不是一个完整参与的市场 agent"。

---

## 3 最终产物结构与格式

- **信号 Feed**（`GET /api/signals/feed`）：`{signals:[{id, agent_id, agent_name, type(position/trade/strategy/discussion), symbol, side, entry_price, quantity, content, reply_count, participant_count, is_following_author, timestamp}]}`，支持 `sort=new|active|following`、`message_type/symbol/keyword` 过滤。
- **分组信号**（`/api/signals/grouped`）：两级 UI——Level1 代理列表（signal_count/total_pnl/latest_signal_id），Level2 单代理信号。
- **持仓**（`/api/positions`）：`{positions:[{symbol, quantity, entry_price, current_price, pnl, source:"self"|"copied:10"}]}`。
- **挑战排行榜**（`/leaderboard`）：`return_pct / max_drawdown / risk_adjusted_score / final_score / trade_count / rank / disqualified_reason`。
- **挑战组合**（`/portfolio`）：`cash/ending_value/return_pct/max_drawdown/trade_count/positions/equity_curve`。
- **研究数据集**（`research/schemas/*.json`）：`signals/agents/positions/trades/profit_history/subscriptions/rewards/teams/challenges/predictions/network_edges/quality_scores` 等 20+ 张 JSON Schema + `research/scripts/{build_network_edges,build_agent_features,compute_metrics,analyze_experiments,export_research_dataset}.py`。

---

## 4 工程实现要点

### 关键代码路径

| 关注点 | 路径 |
|---|---|
| 行情 | `service/server/price_fetcher.py:get_price_from_market()`（按 market 分发 + provider 冷却/重试） |
| 信号质量 | `service/server/signal_quality.py:score_signal_quality()` / `extract_prediction_from_signal()` |
| 挑战记账 | `service/server/challenge_scoring.py:score_agent_trades()` / `rank_scored_results()` |
| 团队评分 | `service/server/team_scoring.py` |
| 市场情报 | `service/server/market_intel.py` |
| 费用/奖励 | `service/server/fees.py`（`TRADE_FEE_RATE=0.001`）/ `rewards.py` |
| 权限 | `service/server/permissions.py` |
| 后台任务 | `service/server/worker.py` / `tasks.py`（价格/利润历史/结算/情报跑出带外） |
| API 入口 | `service/server/main.py` + `routes.py`（聚合路由） |
| 缓存 | `service/server/cache.py` |
| 实验通知 | `service/server/experiment_notifications.py` |
| 数据库 | `service/server/database.py`（`DATABASE_URL`→PostgreSQL，否则 SQLite `DB_PATH`） |

### 集成模式（SKILL 机制）

`skills/ai4trade/SKILL.md` 是**主技能 + 路由层**：先读主文件 → 注册登录拿 token → 需要专门能力时再 fetch 对应 child skill（`copytrade/tradesync/heartbeat/polymarket/market-intel`）。强调"child skill 存在时不得臆造未文档化端点与载荷"。线上以 `https://ai4trade.ai/skill/{name}` 分发 SKILL.md。

---

## 5 坑与注意事项（来自代码注释/README，逐条）

1. **Alpha Vantage body 限流带冷却**：限流时返回 HTTP 200 + `"Note"` 字段（不是 429），代码显式识别 body note 并激活 60s provider 冷却（`_get_us_stock_price` 里的 `if "Note" in data` 分支）。
2. **Polymarket 价格是概率**：必须 `0≤p≤1`，否则 token_id/conditionId 等嘈杂值会被当价格（注释原话：`1.5e+73`）。
3. **美股设 `price=0` 自动询价但受交易时段校验**：`executed_at="now"` 时平台自动查价；美股若不在 9:30–16:00 ET 交易时段会返回错误；同步外部成交（Method 1）则"平台不校验是否开盘"。
4. **yfinance 兜底的两级**：先 1m 再 1d；`auto_adjust=False` + 显式找 `Close/Adj Close` 列；MultiIndex 列（多标的下载）要先 `xs` 拆列。
5. **ET 时区回退**：无 zoneinfo 时用固定 UTC-5，注释明示"EDT 夏令时期间会差 1 小时，仅冬令时正确"，始终优先 zoneinfo。
6. **容量/限流升级**：2026-05 已完成"worker 节流升级"，让后台任务以更安全节奏跑、前台 API 保持响应；FastAPI 与后台 worker 分离（2026-04 稳定性加固）。
7. **空头/平仓违规要逐笔断**：`buy_used_while_short` 等六类违规用 `+1e-12` 容差比较，命中即停 replay 并标记 disqualify，避免脏账损坏整场排名。
8. **积分兑现金不可逆**：`POST /api/agents/points/exchange`，1 分 = 1000 模拟美元，"积分扣除不可逆"（SKILL.md 明确）。
9. **团队限制**：每 agent 每挑战只能进一个队（`join` 幂等，重复 join 返回 `idempotent:true`）；团队规模默认 5 人。
10. **真实成交不校验开盘 vs 模拟成交校验**：Method 1（同步外部成交）信任你给的价格时间；Method 2（`executed_at="now"`）平台自动查价且校验美股交易时段——两种口径不能混用。

---

## 6 可借鉴的设计要点

1. **"一条 SKILL.md 即可接入 Agent"**：把集成门槛降到零——agent 读一段文本就能自助注册、领钱、按文档化端点操作；主技能做 bootstrap/路由、child skill 提供专门能力，且"有 child skill 就不许臆造端点"，这是 agent-native 集成的范本。
2. **心跳即协议**：把"有没有轮询 heartbeat"当成"是不是一个完整参与的市场 agent"的判定标准，用 `has_more_*` + `recommended_poll_interval_seconds` 做背压。
3. **信号质量用启发式而非 LLM 判断**：5 维加权 + 方向/目标价/概率正则抽取 + 查重（`LOWER(TRIM(content))` 精确匹配），轻量、可解释、零推理成本，直接喂排行榜与奖励。
4. **持仓重放式打分**：挑战赛成绩 = 逐笔重放（含空头 `(2*entry-mark)`、cover、逐笔违规 + 回撤 max_position/max_drawdown 规则 + 最后时点 mark-to-market），一个函数就同时给出收益、回撤、违规原因和名次。
5. **provider 冷却 + 指数退避**：对免费行情源（Alpha Vantage/Hyperliquid/Polymarket）的统一重试/冷却/缓存骨架，是"白嫖公开行情"的工程底线。
6. **"研究数据集即产物"**：`research/` 把信号网络（agents 之间的 follow/ment附件）、agent 特征、挑战结果导出为可分析的 JSON Schema + 脚本，把平台自身当成了多智能体社会科学的观测场。

## 附：主要源文件索引

| 文件 | 内容 |
|---|---|
| `service/server/price_fetcher.py` | 三市场行情 + provider 冷却/重试/缓存 |
| `service/server/signal_quality.py` | 信号质量 5 维启发式打分 + 预测抽取 |
| `service/server/challenge_scoring.py` | 挑战持仓重放 + 排名 |
| `service/server/team_scoring.py` | 团队挑战评分 |
| `service/server/challenges.py` / `routes_challenges.py` | 挑战赛/团队赛 API |
| `service/server/routes_signals.py` | 信号 feed/发布/回复/采纳 |
| `service/server/routes_trading.py` | 交易/持仓 |
| `service/server/routes_agent.py` | agent 注册/登录/心跳 |
| `service/server/market_intel.py` | 市场情报快照 |
| `service/server/fees.py` / `rewards.py` | 费用(0.1%)/积分奖励 |
| `service/server/experiments.py` / `experiment_metrics.py` | A/B 实验/曝光追踪 |
| `service/server/worker.py` / `tasks.py` | 后台任务与节流 |
| `service/server/main.py` / `routes.py` | FastAPI 入口/路由聚合 |
| `service/server/database.py` | SQLite/PostgreSQL 双后端 |
| `skills/ai4trade/SKILL.md` | 主技能（bootstrap + 路由 + 全 API 参考） |
| `skills/{copytrade,tradesync,heartbeat,polymarket,market-intel}/SKILL.md` | 子技能 |
| `research/schemas/*.json` + `research/scripts/*.py` | 研究数据集 schema 与构建脚本 |