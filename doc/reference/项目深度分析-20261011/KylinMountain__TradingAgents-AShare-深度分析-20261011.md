# TradingAgents-AShare 深度分析

> 分析日期：2026-10-11 · 仓库 URL：https://github.com/KylinMountain/TradingAgents-AShare · 分析依据：本地克隆源码（refrences/KylinMountain__TradingAgents-AShare，v0.5.0）· 配套：../开源股票分析项目清单-20261010.md

## 0 定位与架构总览

TradingAgents（TauricResearch，Apache 2.0）的 A 股二次开发，但定位与同为 fork 的 TradingAgents-astock 差异明显——这是一个**完整自托管投研 Web 系统**（FastAPI + React + SQLite + 定时调度器），14 名 Agent（7 分析师 + Bull/Bear + Research Manager + Trader + 3 风控 + Risk Judge），带组织级部署（JWT 登录、API Token、企业微信推送、持仓看板、回测）。注意许可：核心沿用 Apache 2.0，但新增 `api/`、`frontend/` 及核心深度修改采用 **PolyForm Noncommercial 1.0.0**（商用需授权）。

流水线（`graph/setup.py::GraphSetup.setup_graph`）：

```
START ──fan-out──> Market | Social | News | Fundamentals | Macro | SmartMoney | VolumePrice
        （7 分析师 async 并行 + 各自 ToolNode 工具循环，Done 节点汇合）
   → Bull Researcher ⇄ Bear Researcher（Claim 驱动辩论）
   → Research Manager（deep LLM，出投资计划）
   → Trader（出交易方案，可被风控打回重写）
   → Aggressive ⇄ Conservative ⇄ Neutral（风控三方辩论）
   → Risk Judge（deep LLM，裁决 pass/revise/reject）
        └─ revise → 回到 Trader 重写；pass/reject → END
```

与上游 TradingAgents 最本质的三处重构：

1. **分析师改"预采集数据池 + 静态读入"**：不再 bind_tools 现取现用，而是 `DataCollector` 先一次拉齐 13 类数据+本地算指标，分析师节点从池里按需取值（`pool.get("fund_flow_individual")` 等），大幅减少重复 LLM 工具调用、加速并预量化。
2. **Claim 驱动辩论**：Bull/Bear 每轮用 HTML 注释标签 `<!-- MARKER:{...} -->` 携带结构化 JSON（新 claim、响应/unresolve/resolve 的 claim id），辩论状态维护 claim 生命周期。
3. **Risk Judge 打回闭环**：风控不只是"辩"，而是出 `pass/revise/reject` 硬裁决，`revise` 时带 hard/soft 约束把 Trader 打回重写（`should_revise_after_risk_judge` → Trader）。

## 1 数据源来源（逐接口）

Provider 抽象架构（`dataflows/providers/base.py` 定义 `BaseMarketDataProvider` 接口，`registry.py` 注册 6 个实现），默认数据路由可在 config 里配 provider 优先级/分工具指定。

| Provider（name） | 底层接口 | 用途 | 鉴权 |
|---|---|---|---|
| `cn_akshare`（`CnAkshareProvider`，主源） | akshare 库：`stock_zh_a_hist`/`stock_zh_a_daily`/`stock_zh_a_hist_tx`（K 线多源回退东财/新浪/腾讯）、`fund_etf_hist_sina`/`fund_etf_hist_em`（ETF）、`stock_individual_spot_xq`（雪球实时）、`stock_individual_info_em`/`stock_individual_basic_info_xq`、`stock_financial_abstract`/`_report_sina`/`_abstract_new_ths`（财务）、`stock_news_em`（新闻）、`stock_main_stock_holder`（股东）、`stock_fund_flow_industry`（板块资金）、`stock_individual_fund_flow`（个股资金）、`stock_lhb_detail_em`（龙虎榜）、`stock_zt_pool_em`（涨停池）、`stock_hot_follow_xq`（雪球热搜） | 全量 A 股行情/财务/新闻/资金/龙虎榜 | 无（雪球实时需可配 XQ token） |
| `cn_investoday`（`CnInvestodayProvider`） | 今日投资 REST `https://data-api.investoday.net/data`：`/stock/adjusted-quotes`（前复权K线）、`/stock/company/profiles`、`/stock/balance-sheets[-q]`、`/stock/income-statements[-q]`、`/stock/cash-flows[-q]`、`/news[/entity-related]`、`/stock/exec-shrhld-chg`（高管持股变动）、`/stock-quote/realtime` | 备源/替代源，前复权K线+三大报表+新闻+高管持股 | 需 `investoday_api_key`（header `apiKey`），`code==0` 才有效 |
| `cn_baostock`（`CnBaoStockProvider`） | baostock 库 | 备源 | 无 |
| `yfinance`/`alpha_vantage` | Yahoo Finance / Alpha Vantage | 美股兜底（保留自上游） | 无/需 key |
| `cn_stub`（`CnStubProvider`） | 占位符，`is_placeholder=True`，所有方法 `NotImplementedError` | 未实现的 CN 提供商占位 | — |

数据分层：`interface.py::route_to_vendor()` 按 `data_vendors`/`tool_vendors` 路由到 provider；`trade_calendar.py` 提供 `cn_no_data_reason()`（用交易日历给 N/A 填"suspend/holiday/非交易日"语义）。akshare provider 内置线程信号量并发闸门（`total=5`、`scheduled_max=3`）防高频反爬触发限流。

## 2 分析逻辑与 Agent 机制

### 2.1 DataCollector（程序算数，AI 解读）

`graph/data_collector.py`：`_fetch_all()` 用 `ThreadPoolExecutor(max_workers=min(10, tasks))` 一次性拉 stock_data/news/global_news/板块资金/个股资金/龙虎榜/股东/涨停池/雪球热搜/财务三表+快照共 13 类，`FETCH_ALL_TIMEOUT=300s` 硬上限（注释记录生产事故：网络丢包单源永久卡死 → 拿 per-key 锁拖死后续分析 → 占满 asyncio 线程池 64/64 全僵死 → 前端 524）。数据以 `ticker_date` 为 key 缓存 + per-key 锁 + 引用计数 evict。核心加速是**本地批量算指标**：用 stockstats 一次性算 `close_50_sma/close_200_sma/close_10_ema/rsi_14/macd/boll/boll_ub/boll_lb/atr/vwma`，并 `_compute_vpa_indicators()` 预计算量价分析（放量/缩量、量价背离、顶部背离/底部放量/卖压衰竭、Selling Climax 卖出高潮、高位放量滞涨——阈值如 `volume_ratio>2.0 且 pct<-3% 且 close_position>0.5` 判卖出高潮）。

### 2.2 7 个分析师（并行 + 静态读池）

`agents/analysts/*`，每个 `create_xxx_analyst(llm, data_collector)` async 节点，从池取数（池为空才 `_fetch_direct` 兜底），用 `llm.astream` Token 级流式输出到 `agent_states.current_tracker_var`，产出 `xxx_report` 与 `analyst_traces`（每条含 agent/horizon/data_window/verdict/confidence）：

| 角色（`..._report` 字段） | 数据来源（池 key 或工具） | 视角 |
|---|---|---|
| 市场 Market | stock_data + 10 项指标 | 技术面（固定 short 短窗） |
| 舆情 Social | news + 雪球热搜 | 散户情绪 |
| 新闻 News | news + global_news | 新闻/公告 |
| 基本面 Fundamentals | fundamentals/balance_sheet/cashflow/income | 财报 |
| 宏观 Macro | global_news + fundamentals | 宏观/利率/政策 |
| 主力资金 SmartMoney | fund_flow_individual + lhb + vwma | 资金博弈（game_theory_tools 5 工具：get_board_fund_flow/get_individual_fund_flow/get_lhb_detail/get_zt_pool/get_hot_stocks_xq） |
| 量价 VolumePrice | VPA 预计算指标 | 量价关系形态 |

`extract_verdict` 从每份报告末尾提取方向 + 置信度，供 `analyst_traces`/决策卡片用。

### 2.3 Claim 驱动辩论（debate_utils.py）

Bull/Bear 每轮输出经 `<!-- 辩论标记:{...} -->` 携带结构化 JSON（`extract_tagged_json`），字段：`new_claims[]`（claim/evidence≤3/confidence/target_claim_ids）、`responded_claim_ids`/`resolved_claim_ids`/`unresolved_claim_ids`、`next_focus_claim_ids`、`round_summary`、`round_goal`。辩论状态维护 claim 生命周期：open→addressed→resolved/unresolved，每轮只聚焦 `focus_claim_ids`（最多2条）+ `round_goal`（`default_round_goal()` 按域 investment/risk 各 5 轮递进目标："先建立核心 claim → 攻击对手最脆弱假设 → 谈时机窗口 → 谈失败路径 → 收口"）。轮数由 `conditional_logic.should_continue_debate/max_debate_rounds` 控制。

### 2.4 Risk Judge 打回闭环（managers/risk_manager.py）

承接三方风控辩论后，deep LLM 输出 `<!-- RISK_JUDGE:{"verdict":"pass|revise|reject","hard_constraints":[...],"soft_constraints":[...],"execution_preconditions":[...],"de_risk_triggers":[...],"revision_reason":"..."} -->`。`extract_risk_judge_result` 解析；解析失败一律按 `reject` 处理并在正文加 `[系统说明]`。`verdict=revise` 时写 `risk_feedback_state`（`retry_count`/`max_retries`），经 `should_revise_after_risk_judge` 回到 Trader 重写（Trader 提示词注入 `summarize_risk_feedback`，重写后清空 risk_debate_state 重新风控）。`reject` 是"解析失败"的安全兜底，不是中断语义。

### 2.5 记忆（BM25 词法检索）

`agents/utils/memory.py::FinancialSituationMemory`：每个角色（bull/bear/trader/invest_judge/risk_manager）独立记忆，用 `rank_bm25.BM25Okapi` 做词法相似度检索（无 API 调用、离线），`get_memories(curr_situation, n_matches=2)` 抽取相似历史情境的 `recommendation` 注入提示词。注意：这是词法 BM25，非向量语义检索。

### 2.6 回测（backtest_service.py）

非侵入式：直接复用 `TradingAgentsGraph.propagate()` 逐日跑历史分析，再对比后续价格。`_get_trading_dates()` 只取工作日（weekday<5，非真实交易日历）；`_classify_decision()` 关键词判 BUY/SELL/HOLD；BUY 看 +return、SELL 看 −return（`raw_return if action=="BUY" else -raw_return`）判赢家；`entry_price`=`_get_price_on`（分析日收盘）、`exit_price`=`_get_price_after(symbol, base_date, hold_days)`（hold_days 后收盘）；统计 `win_rate/avg_return_pct/best/worst`。结果存内存字典（无 DB 表）。

## 3 最终产物结构与格式

- **结构化研报**（`api/services/report_service.py`）：从终审文本用正则 + 辅助 LLM 抽取 `direction`（从 `<!-- 裁决 -->` tag）、`confidence`（0-100，正则 `置信度[:：]\s*(\d+)%`）、`target_price`/`stop_loss_price`（`_extract_price_regex`，从 final_trade_decision 兜底到 trader_investment_plan 提取），落到 `ReportDB` 表；报告含分节（7 分析师报告、辩论、交易、风控）。
- **决策卡片**：方向 + 置信度 + 目标价 + 止损价 + 核心风险。
- **REST API**（`api/main.py`，约 55 个端点）：`POST /v1/analyze`→`job_id`、`GET /v1/jobs/{id}/events`（SSE 流）、`/v1/reports` 检索、`/v1/reports/latest-by-symbols` 批量、`/v1/backtest`、`/v1/portfolio/*`（导入/`parse-image` VLM 解析持仓截图）、`/v1/dashboard/tracking-board`、`/v1/scheduled*`、`/v1/watchlist`、`/v1/config/warmup`、`/v1/chat/completions`（OpenAI 兼容转发）；JWT + API Token 双认证。
- **推送**：企业微信 webhook（`wecom_notification_service.py`）、定时邮件报告（`email_report_service.py`）。
- **OpenClaw 技能**：`skills/tradingagents-analysis/SKILL.md` + `analyze.sh` 供 OpenClaw/Claude Code 调 API。

## 4 工程实现要点（关键代码路径）

| 关注点 | 实现 |
|---|---|
| 意图解析 | `graph/intent_parser.py::parse_intent`：LLM 结构化识别 ticker/focus_areas/specific_questions/user_context；`_extract_user_context_fallback` 纯正则兜底提取目标（建仓/加仓/减仓/止损/持有/观察）、风险偏好（保守/平衡/激进）、期限（短线/波段/中线/长期）、仓位（满仓100/重仓80/半仓50/轻仓20/空仓0）、成本价/仓位股数/最大亏损%、约束（不加杠杆/不追高/只做T+1/不能补仓/不接受隔夜） |
| 多期限 | `build_horizon_context`：short=14 天技术主导、medium=90 天基本面主导；但 `_fetch_all` 始终拉满（含 365 天回看供 200 SMA），horizon 只影响分析窗口不影响采集 |
| 结构化提取容错 | v0.5.0 用 json-repair 替代正则剥离；Pydantic 容忍 LLM 变体（数组→首元素、数字→字符串、缺字段默认值）；tag 解析失败按 reject 兜底 |
| 数据库 | SQLite WAL；Job 生命周期持久化（重启可恢复，`job_store.py`/`job_store_redis.py`）；`TA_MAX_WORKERS` 调并发；`TA_APP_SECRET_KEY` 加密用户 API Key + 签 JWT |
| 调度 | `scheduler/main.py` + FastAPI lifespan 内嵌 asyncio；交易日判断、防重复触发（启动前标 last_run_date）、串行预采集、连续失败 3 次自动停用 |
| VLM | `vlm_service.py`：默认 GLM-4V-Flash（`open.bigmodel.cn`），OpenAI/Anthropic 双协议，解析持仓截图 |

## 5 坑与注意事项（来自 CHANGELOG/代码注释）

1. **数据抓取线程池卡死事故**（data_collector.py 注释）：网络丢包时单数据源永久卡死（SSL/读无超时），拿 per-key 锁拖死后续同标的分析，逐渐占满 asyncio 线程池 → 64/64 全僵死 → 前端 524。处置：`FETCH_ALL_TIMEOUT` 硬超时 + `wait=False` 关闭 executor（卡死线程交给 socket 超时自裁）。
2. **mini_racer/V8 多线程 crash**（v0.5.0）：交易日历（依赖 mini_racer 跑 V8）多线程崩溃 → 启动时预加载、后续全走缓存。
3. **SQLAlchemy TimeoutError**（v0.4.4）：统一 API 端点与后台任务的数据库 session 生命周期。
4. **Resource/Semaphore 泄漏**（v0.4.4）：FastAPI lifespan 里加 executor shutdown。
5. **结构化提取失败全线 400**（v0.4.2）：LLM 返回 markdown 代码围栏/非标准 JSON → 显式恢复缺失 JSON 关键字 + json-repair。
6. **风控裁决机读块解析失败按 reject 处理**：`extract_risk_judge_result` 里 verdict 不在 {pass,revise,reject} 或 tag JSON 坏，一律判 reject 而非静默通过（宁可重写不可漏风险）。
7. **回测非真实交易日历**：`_get_trading_dates` 只排除周末（Mon-Fri），法定节假日/调休会被当成交易日采样，且窗口直接做 `weekday()<5` 判断——回测口径粗糙，是"观察一致性"而非严谨回测。
8. **回测价格对齐漏洞**：`_get_price_after` 用 `hold_days` 条 K 线近似持有期，`len(df)<hold_days` 时截断成 `len(df)-1`，跨停牌/长期停牌标的会失真；`_classify_decision` 纯关键词匹配（"BUY/增持/买入/BULLISH"），弱模型换写法会误归 HOLD。
9. **雪球热搜/实时接口依赖 token**：`stock_individual_spot_xq` 需雪球 token（可配），未配则实时行情降级。
10. **investoday 语义陷阱**：`get_insider_transactions` 明确标注其 `/stock/exec-shrhld-chg` 是"高管持股变动明细"，**不等同全量股东增减持/大宗交易**，返回文字自带该口径说明。
11. **商用许可**：`api/`、`frontend/` 及核心深度修改为 PolyForm Noncommercial，商用需授权（README「许可说明」）。

## 6 可借鉴的设计要点

1. **"预采集池 + 静态读取"替代工具循环**：把 13 类数据 + 指标 + VPA 全部程序预计算，Agent 只"读池写报告"，把 LLM 从重复工具调用和算术里解放出来——同时天然支持"程序算数、AI 解读"，且多分析师共享一趟采集免重复反爬。
2. **Claim 生命周期管理**：辩论不赌"无限话痨"，而是把论点建模成 claim（open/addressed/resolved/unresolved + evidence + confidence + target），每轮限定聚焦 2 条 + 递进 round_goal，机制上逼出收敛。
3. **Risk Judge 硬裁决 + 打回重写**：把风控从"给意见"升级为带 hard/soft/执行前提/降风险触发器的结构化裁决，`revise` 回环且解析失败宁 `reject`——风控护栏的工程化范本。
4. **BM25 词法记忆**：零外部依赖、离线、可解释的相似情境记忆，虽不如向量检索语义强，但对"当前是否与历史某交易情境相似"这类轻量场景够用且零成本。
5. **意图解析双通道**：LLM 结构化 + 纯正则兜底（目标/风险/仓位/成本/约束关键词表），LLM 失败也能拿回可用的用户上下文，且正则兜底可用于测试基线。

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `tradingagents/graph/trading_graph.py` | 主入口，per-role memory 组装、图编译 |
| `tradingagents/graph/setup.py` | 拓扑：分析器 fan-out、Risk Judge 打回边 |
| `tradingagents/graph/data_collector.py` | 预采集池、VPA/指标本地计算、超时防护 |
| `tradingagents/graph/intent_parser.py` | 意图解析 + 正则兜底 |
| `tradingagents/graph/conditional_logic.py` | 辩论/风控/打回条件 |
| `tradingagents/dataflows/providers/*` | 6 个 Provider（cn_akshare 为主） |
| `tradingagents/dataflows/interface.py`、`trade_calendar.py` | 路由、交易日历 |
| `tradingagents/agents/analysts/*` | 7 个分析师（macro/smart_money/volume_price 为新增） |
| `tradingagents/agents/utils/debate_utils.py` | Claim 生命周期 + Risk Judge 结果解析 |
| `tradingagents/agents/utils/memory.py` | BM25 情境记忆 |
| `tradingagents/agents/managers/risk_manager.py` | Risk Judge 裁决 |
| `tradingagents/agents/trader/trader.py` | Trader + 风控打回重写 |
| `tradingagents/prompts/zh.py`、`en.py`、`catalog.py` | 集中式提示词库 |
| `api/main.py`、`api/services/*` | REST API、报告/回测/持仓/VLM/企微推送/调度 |
| `scheduler/main.py` | 定时分析调度器 |
| `skills/tradingagents-analysis/` | OpenClaw 技能 |