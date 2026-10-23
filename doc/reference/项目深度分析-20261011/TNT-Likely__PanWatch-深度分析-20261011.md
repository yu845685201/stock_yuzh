# PanWatch（盯盘侠）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：https://github.com/TNT-Likely/PanWatch · **分析对象版本**：main 分支（commit `1ebec29`，2026-10-11）
> **分析依据**：本地克隆源码（`refrences/TNT-Likely__PanWatch`），精读 `src/modules/`（market/research/automation/paper_trading/portfolio）、`packages/marketdata/`、`prompts/`、`src/platform/`。
> **配套文档**：../开源股票分析项目清单-20261010.md、../开源股票分析项目深度分析-20261011.md

---

## 0. 定位与使用形态

自托管 AI 股票监控系统（Docker 单容器 + PWA 前端），管理 A 股/港股/美股持仓。深度分析基于 **TradingAgents 多智能体框架**（git 依赖锁定 v0.5.0，不启用则零开销），3–5 分钟产出带多空辩论的 PM 决策备忘录。

技术栈：Python（FastAPI/SQLAlchemy/APScheduler/OpenAI SDK）+ React 18/TypeScript/Tailwind/shadcn 前端；LLM 走 OpenAI 兼容接口或 Ollama 本地模型；通知经 `apprise` 统一分发；Docker 镜像内置 Playwright/Chromium 截图；可选 OpenTelemetry 链路导出。

---

## 1. 功能模块全景

| 栏目/模块 | 能力 |
|---|---|
| 组合总览 Portfolios | 多账户总览、股票/现金/盈亏 |
| AI 评分机会清单 | 入场位/目标位/风险上下文 |
| 模拟盘 Paper Trading | 净值曲线、分市场资金分配、T+1 约束、信号自动开仓 |
| 深度分析（TradingAgents） | 4 分析师 + 多空辩论 + 风控 + PM 决策备忘，完整推理链可回看 |
| 技术共振面板 | MACD/RSI/KDJ 多指标共振 |
| 价格警报 Price Alert | AND/OR 组合、五类条件、冷却/每日上限/有效期/一次性 |
| 定时 Agent | 盘前展望（9:00）/盘中监控（每 5 分钟）/日报（15:30），交易所日历过滤节假日 |
| 个股分析 | 日K + 支撑压力 + 技术指标 + 公告/新闻 |
| 数据源面板 | 多 provider 主备切换、北向资金、资金流 |
| PDF 导出 | WeasyPrint 详情报告 |

---

## 2. 数据源来源与采集机制

数据源抽成 `packages/marketdata` 独立包，按 `ports.py`（协议）+ `registry.py`（注册）+ `vendors/`（实现）+ `engine.py`（编排/主备故障转移）分层。

### 2.1 逐接口明细（vendors）

| 源 | 接口 | 内容 | 说明 |
|---|---|---|---|
| **腾讯行情** | `http://qt.gtimg.cn/q=`（GBK） | A 股实时行情（量比 = parts[49]） | 移植自 akshare_collector 解析 |
| **新浪** | `https://hq.sinajs.cn/list=` | 美股 `gb_{code}`、港股 `rt_hk{code}` | Referer 校验 |
| **东方财富** | `push2.eastmoney.com/api/qt/stock/get`、`push2his/.../kline/get` | 盘口五档（字段映射交叉核对 akshare `stock_ask_bid_em.py`）、K 线 | A 股行情 |
| **东方财富公告/搜索** | `np-anotice-stock.eastmoney.com/api/security/ann`、`so.eastmoney.com/search/jsonp` | 公告、个股新闻 | 按 keyword 检索 |
| **雪球** | `xueqiu.com/statuses/stock_timeline.json` | 个股时间线新闻 | |
| **同花顺(核新)** | `data.hexin.cn/market/hsgtApi/method/dayChart/` | 北向资金（沪深股通） | Referer |
| **Yahoo/Stooq** | `query2.finance.yahoo.com/v8/finance/chart`、`stooq.com/q/d/l/` | 美股/港股 K 线、行情 | |
| **yfinance** | yfinance | 美股行情/K线/基本面 | 港美股 |

### 2.2 采集层纪律

- **主备故障转移**：QuoteOrchestrator 走多 provider 主备（`src/platform/marketdata/marketdata_client.py`），单市场挂不影响其他市场（price_alert_engine 注释："One unavailable market must not suppress healthy markets"）。
- **行情缓存 TTL**：报价 5s、K线 60s（`PriceAlertEngine.__init__`）。
- **ST/涨跌停与日期校验**：`quote_date_is_current` 判行情日期是否当前交易日（`src/platform/marketdata/quote_display.py`）。
- **僵尸报价判定**：`src/platform/marketdata/outcome_prices.py`、`cn_symbol.py`（A 股代码规范化）。

---

## 3. 分析逻辑

### 3.1 价格警报引擎（异动判定核心，`price_alert_engine.py`）

**规则结构**：`condition_group = {op: and|or, items: [condition]}`，条件类型五类：

| 条件 type | 取值字段 |
|---|---|
| `price` | 现价 |
| `change_pct` | 涨跌幅 |
| `turnover` | 换手率 |
| `volume` | 成交量 |
| `volume_ratio` | 量比（优先报价里的量比，缺则回退 K 线摘要） |

运算符：`> >= < <= = != between(区间)`。

**五重触发闸门**（`_can_trigger`）：
1. `enabled` 开关；
2. `expire_at` 过期；
3. **交易日门控**：非交易日不触发（`is_trading_day`）；`market_hours_mode=trading_only` 再叠加交易时段；
4. **每日上限** `max_triggers_per_day`（跨日按 `trigger_date == today` 重置计数）；
5. **去重**：`repeat_mode=once`（触发一次即禁用）+ `cooldown_minutes` 冷却（不足间隔不触发）。

命中落库 `PriceAlertHit`（含 `trigger_bucket` 分钟桶做幂等去重）→ 独立 worker 恢复持久队列送通知（规则即使被 once 禁用也能补送达）。扫描前先批量 gate（避免无效行情/K线请求），`scan_once` 每 80s 一轮。health 表逐规则记录 consecutive_failures，区分 `stale_quote/source_failed/incomplete_data` 等状态。

### 3.2 盘中异动判定（prompt 层 + 程序层）

**自适应异动阈值**（intraday_monitor prompt 原文）：`自适应阈值 = max(固定阈值, 1.5×ATR%)`——"高波动股票 ±5% 可能只是常态，低波动股票 ±3% 已属异动"，涨跌幅超过该阈值才视为真正价格异动，避免对高波动股常态波动误报。短线/波段/长线三档固定下限参考：≥2% / ≥3% / ≥5%。

**技术共振面板** + prompt 内置指标规则：均线排列（MA5>MA10>MA20>MA60 多头）、MACD 金叉死叉 + 柱状图转势、RSI（>80 严重超买/<30 超卖/<20 关注反弹）、KDJ（J>100 超买/J<0 超卖）、布林带（突破上轨/跌破下轨/收口蓄势/开口放大）、量能（放量上涨健康/缩量上涨乏力/放量下跌恐慌）、ATR% 相对波动、K 线形态（锤子线/吞没/十字星/大阳大阴）。

### 3.3 定时 Agent（三类工作流，`agent_catalog.py`）

| Agent | 名称 | cron | 模式 | 关键 config |
|---|---|---|---|---|
| premarket_outlook | 盘前分析 | `0 9 * * 1-5` | batch | 隔夜美股+新闻+技术状态+昨日 ta_verdict |
| intraday_monitor | 盘中监测 | `*/5 9-15 * * 1-5` | single | `event_only=True`、price_alert_threshold 3.0%、volume_alert_ratio 2.0、stop_loss_warning −5%、take_profit_warning 10%、throttle 30min |
| daily_report | 收盘复盘 | `30 15 * * 1-5` | batch | 市场回顾+个股复盘+次日关注 |

输出结构统一：`<!--PANWATCH_JSON-->` 包裹的 JSON 块，`action ∈ buy/add/reduce/sell/hold/watch/avoid` 七元动作，含 `attention_required`（纯风险提醒用 watch+true，不把提醒当买卖动作）、triggers/invalidations/risks 数组。盘前展望引 `ta_verdict`（TradingAgents 结论）作高权重先验、`relative_strength`（个股近 5/20 日相对所属指数超额收益）做轮动判断、公告 `content_fulltext` 判断催化剂实质。

### 3.4 TradingAgents 深度分析（`agent_catalog` tradingagents）

config：`analyst_types = [market, social, news, fundamentals]`（四类分析师）、`debate_rounds=1`（多空辩论 1 轮）、`cache_ttl_hours=12`、quick/deep 双模型分层、`collection_timeout_seconds=45`（单数据源超时降级）、`llm_timeout_seconds=300`、`llm_max_retries=0`（深研失败快速落终态不在图内重试）、`enable_sec_edgar`（仅美股）、`holding_period_days=5`（决策质量回测默认持仓期）、`emit_paper_trading_signal=False`（是否把 BUY 写入 StrategySignalRun 驱动模拟盘自动开仓，默认关）。

### 3.5 预测回填（`research/prediction_outcome.py`）

Agent 预测落 `StrategySignalRun`，`evaluate_pending_prediction_outcomes` 按持仓期 `holding_period_days`（默认 5 交易日）取"到期后 N 交易日收盘价"，对账方向正确率（`_find_close_after_n_trading_days`）。`context_scheduler` 定时评估 + 清理。

### 3.6 模拟盘（`paper_trading_engine.py`）

- **T+1**：`paper_trading_t1_locked`——"A 股实行 T+1：当日买入的持仓须到下一交易日才能卖出"（用上海采购日判定，`sell_block_reason`）。
- **成本模型**与回测共用同一 `CostModel`（A股佣金/印花税/滑点口径）。
- **分市场资金分配** `market_allocations_or_default`（初始本金按比例分给 A/港/美，各自已实现盈亏 + 未平仓成本核算可用现金）。
- 信号按 `rank_score` 降序限额 50 只开仓。

---

## 4. 最终产物结构与格式

| 产物 | 结构 | 落盘/通道 |
|---|---|---|
| 盘前展望 | Markdown（隔夜影响/今日展望/今日关注/个股票建议摘要）+ `<!--PANWATCH_JSON-->` | Telegram/企微/钉钉/飞书/Bark/Webhook（apprise） |
| 盘中监测 | 纯 JSON（action/action_label/attention_required/signal/reason/triggers/invalidations/risks） | 同上；event_only 模式仅异常推送 |
| 收盘复盘 | Markdown 日报（市场回顾/个股复盘/次日关注） | 同上 |
| TradingAgents 决策 | PM 决策备忘（评级/结论/完整推理链） | 前端可回看 + 预测回填 |
| 价格警报 | 标题+现价+涨跌幅+命中条件 | 通知 + PriceAlertHit 落库 + health |
| 模拟盘 | 净值曲线/分市场现金/交易记录 | SQLite |
| PDF 报告 | 详情报告 | WeasyPrint 导出 |

---

## 5. 坑与注意事项（来自代码注释/README/CHANGELOG）

1. **T+1 语义**：模拟盘当日买入不能当日卖出，用上海采购日判定（SQLite 存 naive UTC，`sell_block_reason` 显式处理）。
2. **异动阈值要相对个股自身波动**："波动幅度优先用相对个股自身波动率衡量"——固定百分比只是下限，防止把高波动股常态当异动（intraday_monitor prompt）。
3. **quote_date 校验**：非交易日的旧行情不能被当成当前快照触发警报（`stale_quote` 状态）。
4. **once 触发的规则禁用后仍要送达**：独立 worker 恢复持久队列——"即使规则 has become disabled by its once-only trigger"也要把已命中的通知送达。
5. **deep 分析失败快速落终态**：`llm_max_retries=0`，"深度分析失败快速落终态，不在图内重复重试"。
6. **TradingAgents 不启用零开销**：git 依赖按需加载，默认关闭。
7. **通知去重与静默时段**：`notify_dedupe`（TTL 按 agent 可覆盖）+ `quiet_hours`（跨午夜区间，start==end 视为全天静默）。
8. **北向资金已停止实时披露**：同花顺 hexin 接口仍可用但需注意 A 股北向实时数据自 2024-08 起不再逐日披露（README 未详述，接口保留）。
9. **Playwright 截图内置镜像**：Docker 镜像自带 Chromium，仅作截图用途。

---

## 6. 可借鉴的设计要点

1. **"自适应异动阈值 = max(固定, 1.5×ATR%)"**：把"异动"从绝对百分比改造为相对个股自身波动率，是盯盘误报治理的关键一招，短/波段/长线三档下限分档清晰。
2. **七元动作 + attention_required 分离**：`buy/add/reduce/sell/hold/watch/avoid` + 布尔 attention，把"风险提醒"和"买卖方向"显式分开（watch+attention_required 不当作交易动作），程序解析友好。
3. **五重触发闸门前置 gate**：enabled/expire/交易日/每日上限/冷却一次性全在前置 pass，扫描前先批量 gate 避免无效行情请求——省请求且语义清晰。
4. **TradeAgents 决策连接模拟盘**：`emit_paper_trading_signal` 把 BUY 决策写入 StrategySignalRun 驱动模拟盘自动开仓（默认关）+ 预测回填对账，形成"分析→信号→模拟盘→绩效回看"闭环。
5. **数据源主备故障转移**：QuoteOrchestrator 多 provider 主备 + 单市场故障隔离，`marketdata` 独立成包可移植。
6. **通知策略细粒度**：quiet_hours 跨午夜 + 按 agent 的 dedupe TTL 覆盖 + retry/backoff——比单一 webhook 直发更工程化。

---

## 7. 主要源文件索引

| 文件 | 内容 |
|---|---|
| `server.py` | FastAPI 主应用（60KB） |
| `src/modules/market/price_alert_engine.py` | 价格警报五重闸门 + AND/OR 评估 |
| `src/modules/market/price_alert_service.py` / `price_alert_scheduler.py` | 警报服务与调度 |
| `src/modules/automation/agent_catalog.py` | 三类工作流 Agent + TradingAgents 深度分析 config |
| `src/modules/automation/agent_scheduler.py` / `agent_runs.py` | Agent 调度与运行记录 |
| `prompts/premarket_outlook.txt` | 盘前展望 prompt（ta_verdict/relative_strength/JSON 块） |
| `prompts/intraday_monitor.txt` | 盘中监控 prompt（自适应阈值/七元动作/指标规则） |
| `prompts/daily_report.txt` | 收盘复盘 prompt |
| `src/modules/paper_trading/paper_trading_engine.py` | 模拟盘引擎（T+1/成本模型/分市场资金） |
| `src/modules/research/prediction_outcome.py` | 预测回填（持仓期 N 交易日对账） |
| `src/modules/research/context_scheduler.py` | 机会刷新/预测评估/上下文清理 |
| `packages/marketdata/src/marketdata/`（ports/registry/engine/vendors） | 数据源插件化框架 |
| `packages/marketdata/src/marketdata/vendors/{tencent,sina,eastmoney,news,northbound,kline}.py` | 腾讯/新浪/东财/雪球/核新北向/K线 vendors |
| `src/platform/notifications/`（notifier/notify_dedupe/notify_policy） | 通知分发/去重/静默与重试策略 |
| `src/platform/scheduling/trading_calendar.py` | 交易所交易日历（Agent 调度过滤节假日） |
| `src/platform/ai/ai_client.py` / `ai_failover.py` | LLM 客户端与故障转移 |
| `src/modules/reporting/pdf_export.py` | WeasyPrint PDF 导出 |