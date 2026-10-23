# TradingAgents 深度分析

> **分析日期**：2026-10-11 · **仓库**：[TauricResearch/TradingAgents](https://github.com/TauricResearch/TradingAgents) v0.6.0（`tradingagents/__init__.py.__version__ = "0.6.0"`，本地 commit `1394a3f`，2026-10-03）
> **分析依据**：本地克隆源码（`refrences/TradingAgents`），逐文件精读 `tradingagents/graph/`、`tradingagents/agents/`、`tradingagents/dataflows/`、`tradingagents/memory/`、`tradingagents/llm_clients/` 与 README/CHANGELOG，所有结论来自源码实现与代码注释，非推断。
> **配套文档**：[开源股票分析项目深度分析-20261011](./开源股票分析项目深度分析-20261011.md)
> **论文**：arXiv:2412.20138（TradingAgents: Multi-Agents LLM Financial Trading Framework）

---

## 0 定位与架构总览

TradingAgents 是一个**仿真交易公司组织架构**的多智能体 LLM 交易框架，用 LangGraph（`langgraph.graph.StateGraph`）把"分析师团队 → 研究员辩论 → 交易员 → 风控辩论 → 组合经理终审"串成一条有向图。核心价值点在于：**(1) 角色化分工与两轮结构化辩论；(2) quick/deep 两档 LLM 分层（分析师/研究员跑便宜快模型，经理跑强模型）；(3) 全链路 point-in-time（点对点时间一致性）与"记忆日志 + 决策结算 + 反思"闭环**。

### 流水线（graph 节点级）

```
                       ┌── Market Analyst ──────┐
                       ├── Sentiment Analyst ───┤  (并行，各自私有消息史 + 工具)
START ──> [分析师]     ├── News Analyst ────────┤──┐
                       ├── Fundamentals Analyst ┤  │
                       └── Memory Log (并行结算) ┘  │
                                                   ▼
                              Bull Researcher ⇄ Bear Researcher   (辩论轮次 max_debate_rounds)
                                                   │
                                                   ▼
                                          Research Manager (deep)  →  investment_plan
                                                   │
                                                   ▼
                                                 Trader            →  trader_investment_plan
                                                   │
                     Aggressive ⇄ Conservative ⇄ Neutral (风险辩论 max_risk_discuss_rounds)
                                                   │
                                                   ▼
                                          Portfolio Manager (deep)  →  final_trade_decision + final_rating
                                                   │
                                                  END
```

上表对应 `setup.py:GraphSetup.setup_graph()`。**边的关键点**：分析师节点通过 `workflow.add_edge(first_steps, "Bull Researcher")` 汇流——LangGraph 的 `add_edge(list, node)` 语义是所有前驱完成后再进下一节点，即"每个分析师都交了报告才开辩"。辩论与风控都用 `add_conditional_edges` + `ConditionalLogic` 路由。

---

## 1 数据源来源

数据层在 `tradingagents/dataflows/`，按"类别 → 工具 → 厂商链"三级组织，`router.py:route_to_vendor` 负责解析配置厂商链并按序回退。厂商能力矩阵见 `router.py:VENDOR_METHODS`。

### 1.1 厂商与类别映射

| 类别（TOOLS_CATEGORIES） | 工具 | 厂商（VENDOR_METHODS key） |
|---|---|---|
| `core_stock_apis` | `get_stock_data` | `alpha_vantage` / `yfinance` |
| `technical_indicators` | `get_indicators` | `alpha_vantage` / `yfinance` |
| `fundamental_data` | `get_fundamentals` / `get_balance_sheet` / `get_cashflow` / `get_income_statement` | `alpha_vantage` / `yfinance`，三表额外 `sec_edgar` |
| `news_data` | `get_news` / `get_global_news` / `get_insider_transactions` | `alpha_vantage` / `yfinance` |
| `macro_data` | `get_macro_indicators` | `fred`（需 `FRED_API_KEY`） |
| `prediction_markets` | `get_prediction_markets` | `polymarket`（无 key） |

### 1.2 逐接口表

| 工具 | 用途 | 覆盖市场 | 鉴权 |
|---|---|---|---|
| `get_stock_data` | OHLCV 日线（CSV 落盘后给指标用） | Yahoo Finance 覆盖的全部市场（`0700.HK`、`7203.T`、A 股 `.SS/.SZ`、`BTC-USD` 等） | 无（yfinance）或 `ALPHA_VANTAGE_API_KEY` |
| `get_indicators` | 单一技术指标（rsi/macd/boll/atr/sma/ema/vwma 等，一次一个） | 同上 | 同上 |
| `get_verified_market_snapshot` | **确定性核对快照**：curr_date 或之前的最近 OHLCV 行 + 常用指标 + 最近收盘，作为"精确价格/指标声明的唯一真相源" | 同上 | 同上 |
| `get_fundamentals` / 三张财报表 | 公司基本面/资产负债表/现金流/利润表 | 美股（SEC EDGAR 按 filing 日期 as-filed），其余走 Yahoo | EDGAR 无 key；Yahoo 无 key |
| `get_insider_transactions` | 内部人买卖 | 美股 | alpha_vantage 或 Yahoo |
| `get_news`（单票）/ `get_global_news`（宏观） | 新闻头条 | 全市场 / 全球宏观 | 同上 |
| `get_macro_indicators` | FRED 宏观序列（cpi/core_pce/unemployment/fed_funds_rate/10y_treasury/yield_curve，含欧元区镜像序列） | 美国 + 欧元区 | `FRED_API_KEY` |
| `get_prediction_markets` | Polymarket 事件隐含概率（Fed 决议/衰退/选举） | 事件类 | 无（keyless） |
| 情绪三段预取（非工具） | StockTwits cashtag 流（带 Bullish/Bearish 标签）+ Reddit（`r/wallstreetbets`/`r/stocks`/`r/investing` 或 crypto 社区） | 美股/币圈 | 无；可选 `TYPESAFE_API_KEY` 走 Jev 过滤 |

### 1.3 厂商回退语义（`router.py`）

- 配置字符串**即厂商链**，绝不把请求偷偷路由到用户没选的厂商（注释 `#988/#289`：曾导致跨厂商口径不一致）。
- `default` 哨兵 = 使用全部可用厂商。
- 错误三态区分：`VendorUnavailableError`（限流/网络）、`VendorNotConfiguredError`、`NoMarketDataError`（已应答但确无数据）。全部限流时返回 `DATA_UNAVAILABLE:` 哨兵文本，绝不编值；全部"无数据"返回 `NO_DATA_AVAILABLE:` 哨兵并附具体原因（invalid symbol / stale）。
- `macro_data`、`prediction_markets` 属 `OPTIONAL_CATEGORIES`——失败只降级为哨兵文本，不终止 run（"flavour data 不该因坏 key 炸掉整次分析"）。

---

## 2 分析逻辑与 Agent 机制

### 2.1 角色分工（11 个 agent 节点）

| Agent | 创建函数 | LLM 档 | 结构化输出 | 依赖输入 |
|---|---|---|---|---|
| Market Analyst | `create_market_analyst` | quick | 无（prose + Markdown 表） | 自取 get_stock_data/get_indicators/get_verified_market_snapshot |
| Sentiment Analyst | `create_sentiment_analyst` | quick | `SentimentReport` | 预取 news/StockTwits/Reddit（无工具） |
| News Analyst | `create_news_analyst` | quick | 无 | 自取 get_news/get_global_news/get_macro_indicators/get_prediction_markets |
| Fundamentals Analyst | `create_fundamentals_analyst` | quick | 无 | 自取 4 个基本面工具 + 内部人 |
| Bull / Bear Researcher | `create_bull_researcher` / `create_bear_researcher` | quick | 无 | 四份分析师报告 + 对手上一轮观点 |
| Research Manager | `create_research_manager` | **deep** | `ResearchPlan` | 辩论史 |
| Trader | `create_trader` | quick | `TraderProposal` | Research Plan + 市场报告（用于价格落地）|
| Aggressive/Neutral/Conservative | `create_*_debator` | quick | 无 | Trader 提案 + 四份报告 + 对手观点 |
| Portfolio Manager | `create_portfolio_manager` | **deep** | `PortfolioDecision` | 风险辩论史 + 记忆 lessons + 组合上下文 |

**LLM 分层（`trading_graph.py` + `default_config.py`）**：
- `quick_think_llm`（默认 `gpt-6-luna`）：服务分析师、研究员、辩论者、交易员。
- `deep_think_llm`（默认 `gpt-6-sol`）：服务 Research Manager 与 Portfolio Manager。
- 两档可各自独立 provider（`quick_think_provider`/`deep_think_provider`，如 deep 走 Anthropic Claude、quick 走 OpenAI），通过 `create_tier_client(config, "deep"|"quick")` 构建。

### 2.2 分析师：私有子图 + 工具轮次上限

每个带工具的分析师编译成一个**独立子图**（`setup.py:_analyst_graph`），输出 schema 只有该分析的 `report_key`：

- 子图节点：`agent`（bind_tools）→ 条件边 `_tools_or_done`（末条消息有 `tool_calls` 则去 `tools` 节点，否则 END）→ `tools`(ToolNode) → `more_or_wrap_up`（累计工具回合数 ≥ `max_tool_rounds` 则去 `wrap_up`，否则回 `agent`）。
- **`WRAP_UP` 兜底**（`turn.py`）：用完回合数后追加一条固定 HumanMessage，要求"立即写最终报告并说明哪些数据没取到"，且这一轮 `tool_names="none; your tool rounds are spent"` 不绑工具——防止模型无限调用工具把图跑进 recursion limit（`#1420`）。
- 私有消息史隔离：分析师之间互不写入对方字段、工具调用不互相可见（`stream_run` 依赖子图 `subgraphs=True` 逐任务产出）。
- **默认参数**：`max_tool_rounds=20`、`max_recur_limit=100`；构造时校验 `2*max_tool_rounds + 2 < max_recur_limit`，否则直接抛 `ValueError`。

### 2.3 辩论机制（两轮）

**研究辩论**（Bull ⇄ Bear，`conditional_logic.py:should_continue_debate`）：
- `count >= 2 * max_debate_rounds`（默认 `max_debate_rounds=1` → 各发言 1 次共 2 次）→ 转 Research Manager；否则看 `current_response.startswith("Bull")` → 转 Bear，否则转 Bull。
- `DEBATE_PATH_MAP` 覆盖所有路由目标，防路径兜底崩溃（`#1088`）。

**风险辩论**（Aggressive ⇄ Conservative ⇄ Neutral，`should_continue_risk_analysis`）：
- `count >= 3 * max_risk_discuss_rounds`（默认 1 → 三人各 1 次共 3 次）→ 转 Portfolio Manager；否则按 `latest_speaker` 前缀轮转三人。

**辩论状态机**（`state.py`）：`InvestDebateState`（bull_history/bear_history/history/current_response/count）与 `RiskDebateState`（三人各自 history + latest_speaker + 各自 current_response + count）。每个辩论者用 `history + 对手上一段` 作为上下文，把论点存回对应 history。

### 2.4 Prompt 链要点（源码级）

- **反捏造对手**（`context.py:opponent_argument_or_opening`）：对手空时插入 `"(The bear analyst has not spoken yet — open the debate with your own case.)"`，否则"反驳对手"的 prompt 会让模型**编造**对方观点（`#1176`）。同理 `report_or_absent` 把空报告替换为 `"(No market report in this run: it is not available, not an empty finding.)"`。
- **身份锚定**（`context.py:build_instrument_context` + `resolve_instrument_identity`）：run 开始时用 yfinance 解析公司名/行业/交易所，注入每个 agent 上下文，防"靠价格图形状幻觉成另一家公司"（`#814`）。历史日期 run 只给当前名（标注"可能当时不叫这名"），不给行业/交易所（防点对点泄漏）。
- **语言指令**（`get_language_instruction`）：非英文时明确"整个响应用 X 语言，但 `**Rating**:`、`FINAL TRANSACTION PROPOSAL:` 这类程序读取的标签行保持英文原样"（翻译后的 rating 行会被正则误读，`#1435`）。
- **评级尺度反犹豫**：Research Manager / Portfolio Manager 的 prompt 都写死"辩论总有冲突，判定谁强就是你的工作，冲突本身不是 Hold 的理由；只在证据仍均衡或太薄时才 Hold"。
- **禁止外挂工具**（`structured.py:NO_EXTERNAL_TOOLS`）：经理/交易员 prompt 里明示"你没有工具、不要调用任何工具"。
- **价格落地**（`trader.py`）：Trader 若拿到市场报告则额外注入 "Ground concrete price levels ... in the technical market report's price structure"（`#1167`）；并强制 entry/stop 用绝对价格而非百分比（`#1288`）。
- **情绪读法**（`sentiment_analyst.py`）：内置 Best Practices——"70/30 多空比温和看多，≥90/10 可能是过度延伸/反向风险；几十条消息的样本要用条数而非百分比；可信度按数据质量给 low/medium/high"。

### 2.5 结构化输出与评级

`schema.py` 用 Pydantic 定义四个结构化输出，靠各 provider 原生模式（OpenAI/xAI `json_schema`、Gemini `response_schema`、Anthropic tool-use）直接产出，失败回退自由文本（`structured.py:invoke_structured_or_freetext`）。

| Schema | 字段 |
|---|---|
| `ResearchPlan` | `recommendation`(5 档) / `rationale` / `strategic_actions` |
| `TraderProposal` | `action`(Buy/Hold/Sell 3 档) / `reasoning` / `entry_price?` / `stop_loss?` / `position_sizing?` |
| `PortfolioDecision` | `rating`(5 档) / `executive_summary` / `investment_thesis` / `price_target?` / `time_horizon?` |
| `SentimentReport` | `overall_band`(Bullish/Mildly Bullish/Neutral/Mixed/Mildly Bearish/Bearish 6 档) / `overall_score`(0–10，`ge=0 le=10`) / `confidence`(low/medium/high) / `narrative` |

**5 档评级 + REVIEW 哨兵**（`rating.py`）：
- `RATINGS_5_TIER = (Buy, Overweight, Hold, Underweight, Sell)`。
- `parse_rating` 读不出评级时返回 `REVIEW`（**不是**可交易的 Hold——"读不出来≠持仓不动"，`#1170`），下游须用 `is_review()` 判断。
- `extract_rating` 用两组正则：`_RATING_LINE_RE`（认决策自己的 rating 行，容忍 markdown 加粗/破折号/全角冒号，经 NFKC 归一化；排除 `Rating Scale:` 这种图例行）与 `_RATING_LABEL_RE`（背景标签）。宁可不给结论也不从"not a Sell"这类句子误读方向。
- 数值字段去噪（`_coerce_optional_float`）：把 `"None"/"N/A"` 占位、`"15%"` 这种"把百分比当价格"、`"$1,234.50"` 千分位都规范掉——"把 15% 读成 15 会在 600 美元股票上挂 15 美元的止损"（`#1058`）。

### 2.6 记忆/反思/结算闭环（`memory/`）

这是本项目最独特的机制，三步闭环：

1. **写入**（`log.py:store_decision`）：每次 `propagate()` 完成后把最终决策**追加**到 `~/.tradingagents/memory/trading_memory.md`，条目形如 `[{date} | {ticker} | {rating} | pending]`，用 `<!-- ENTRY_END -->` 作硬分隔符（HTML 注释不会出现在 LLM 输出里）。写入带同 ticker+date 幂等去重。
2. **结算**（`settlement.py:settle_pending`）：当某个决策的**持有窗口已走完**（`holding_period_days=5` 个交易日），取票和基准的收盘，算 `raw_return` 与 `alpha_return`（相对 `resolve_benchmark` 选出的地区基准，如 `.SS→000001.SS`、美股→SPY）。**窗口未走完则 return None、保持 pending 下轮再试**（`#1169`），绝不提前结算部分收益。
3. **反思**（`reflection.py`）：用 quick 模型把 (raw/alpha/决策文本) 压成 **2-4 句 plain prose 教训**，供后续同票 run 注入 Portfolio Manager（`get_past_context`：同票最近 5 条 + 跨票 3 条）。

**点对点防前视**（多处协同）：
- `_memory_as_of`：历史/回测 run 只注入 `resolved_date <= trade_date` 的教训（`tag` 尾字段 `resolved:{date}`，`#1251`）。
- `fetch_returns` 把 `resolution_date`（最后一个价格 bar 日期）写回 tag。
- 数据工具全部用 `InjectedState("trade_date")` + `as_of/as_of_window` 把请求窗口 clamp 到分析日（`tools.py`），"不管模型问什么日期，绝不服务分析日之后的数据"。
- SEC EDGAR 按 filing 日期提供 as-filed 报表（如 Apple 2008 总资产在重述前后读不同值）；Yahoo 报表按覆盖期而非发布期标注，故历史 run 被告知 withheld；内部人交易按发生日而非申报日。

### 2.7 回测机制（`backtest.py`）

- `iter_grid(start, end, every_n_days)` 生成日期网格，`run_backtest(tickers, dates, config, selected_analysts)` 逐格跑同一 pipeline。
- 每格按**相对地区基准的已实现 alpha** 打分，按 rating 分组呈现。
- 不写用户自己的记忆日志；`run_id` 相同则跳过已跑格子，支持断点续扫。

---

## 3 最终产物结构与格式

### 3.1 报告树（`reporting.py:write_report_tree`）

保存目录结构（`save_path` 默认 `llm_provider` 无关，位于 `{results_dir}/reports/{TICKER}_{时间戳}/`）：

```
1_analysts/{market,sentiment,news,fundamentals}.md
2_research/{bull,bear,manager}.md
3_trading/trader.md
4_risk/{aggressive,conservative,neutral}.md
5_portfolio/decision.md
complete_report.md        # 汇总，含 header（版本/模型/分析师/厂商/轮次）
complete_report.html      # 单页自包含版（report_html.py），v0.6.0 新增
```

### 3.2 `complete_report.md` header（`reporting.py:_header`）

report 首部固定记录：分析日期、最终 `Rating`、生成时间、`TradingAgents {version}: deep {provider} {model}, quick {provider} {model}`、`Analysts: {list}; research debate rounds {n}, risk debate rounds {m}`、`Data vendors: {category} {vendor}, ...`、以及 `Memory log:` 结算异常说明（若本轮有决策未能结算）。

### 3.3 状态落盘（`trading_graph.py:_log_state`）

`{results_dir}/{TICKER}/TradingAgentsStrategy_logs/full_states_log_{date}.json`，含 `market_report/sentiment_report/news_report/fundamentals_report` + `investment_debate_state`(bull/bear/history/current_response) + `trader_investment_plan` + `risk_debate_state`(三人 history) + `investment_plan` + `final_trade_decision` + `final_rating` + `run_settings`。ticker 经 `safe_ticker_component` 过滤防目录穿越。

### 3.4 CLI 交互

`cli/main.py` 提供逐题交互（ticker/日期/provider/深度等，上次答案成为默认值）；`--ticker --date --analysts --save --no-show` 等 flag 可逐题跳过；`TRADINGAGENTS_*` 环境变量整体跳过对应步骤。报告另产出 `complete_report.html` 单页（`--no-html` 可跳过）。

---

## 4 工程实现要点

### 关键代码路径

| 关注点 | 文件/函数 |
|---|---|
| 图编译与入口 | `tradingagents/graph/trading_graph.py:TradingAgentsGraph.propagate()` / `_run_graph()` |
| 节点注册与连线 | `tradingagents/graph/setup.py:GraphSetup.setup_graph()` / `_analyst_graph()` |
| 状态初始化 | `tradingagents/graph/propagation.py:Propagator.create_initial_state()` |
| 辩论路由 | `tradingagents/graph/conditional_logic.py:ConditionalLogic` |
| 工具回合限额 | `tradingagents/agents/analysts/turn.py:take_turn()` / `WRAP_UP` |
| 数据工具 | `tradingagents/agents/tools.py`（`InjectedState` + `as_of`） |
| 厂商路由 | `tradingagents/dataflows/router.py:route_to_vendor()` |
| 结构化输出 | `tradingagents/agents/structured.py` / `schemas.py` |
| 评级解析 | `tradingagents/agents/rating.py` |
| 记忆闭环 | `tradingagents/memory/{log,settlement,reflection}.py` |
| LLM 客户端 | `tradingagents/llm_clients/{factory,openai_client,anthropic_client,google_client,...}.py` + `model_catalog.py` |
| 检查点续跑 | `tradingagents/graph/checkpointer.py`（per-ticker SqliteSaver） |

### 检查点续跑（opt-in `--checkpoint`）

- `checkpoint_enabled` 时图用 `SqliteSaver` 重编译，DB 在 `{cache}/checkpoints/{TICKER}.db`。
- thread_id 由 `_run_signature` 生成：`analysts=...|debate=n|risk=n|asset=...|portfolio=fingerprint|analysts=parallel|memory=parallel|settings=sha256[:12]`——任何改变图形态或产出的配置变化都会导致全新 thread，避免"带旧配置产物续跑"（`#1089`）。
- 续跑时 `checkpoint_input` 返回 `None`（LangGraph `invoke(None)` 断点续传），成功完成后清 checkpoint。

---

## 5 坑与注意事项（全部来自代码注释/CHANGELOG）

逐条列出（每条标注出处编号或文件）：

1. **前视/泄漏**：多轮修复主题。FRED 宏观、情绪源、记忆日志三个路径都曾存在 look-ahead。现统一靠 `as_of/as_of_window`、`InjectedState(trade_date)`、`resolution_date` 过滤（README + `date_window.py`）。
2. **"读不出评级"不能降级成 Hold**：`parse_rating` 返回 `REVIEW` 哨兵，因为"真实的 Hold 会被下一轮当作从未做出的 call 引用回来"（`rating.py` 注释，`#1170`）。
3. **把百分比当价格**：Trader 的 entry/stop 若允许百分比，模型会写 "15%"，结构化解析得到 15 美元止损（`schema.py:_coerce_optional_float` 注释，`#1288`）。
4. **空对手观点 → 模型编造对方立场**：`opponent_argument_or_opening` 用"尚未发言"标记替代空串（`#1176`）。
5. **空报告被读成"空发现"**：`report_or_absent` 用"(No X report ...)"标记替代空白（`#1176`）。
6. **翻译评级标签导致正则误读**：非英文输出必须保留 `**Rating**:` 等标签行的英文（`#1435`）。
7. **价格图形态 → 幻觉成另一家公司**：`resolve_instrument_identity` 注入真实公司名/行业（`#814`）。
8. **厂商静默回退导致跨源不一致**：旧版会在未选厂商间回退；现配置串即厂商链，默认 `default` 才用全部（`router.py` 注释 `#988/#289/#989`）。
9. **坏主源被备源掩盖**：router 在返回 NO_DATA 时仍会在日志 warning 记录"某厂商真报错"，防决定被兜底藏住（`#989`）。
10. **无限工具调用卡死图**：`max_tool_rounds=20` + `WRAP_UP` 兜底 + 构造期校验 `2*rounds+2 < recursion_limit`（`#1420`）。
11. **提前结算部分收益**：结算要求窗口满 5 个交易日才给 return，未满返回 None 保持 pending（`#1169`）。
12. **Yahoo 日 bar 时区**：结算时按"各自市场的日历日"对齐股票与基准（`_by_day` 去时区），否则跨时区票价对不上。
13. **CLI streaming 曾无视 --checkpoint**：检查点曾只活在 `propagate()` 里，CLI stream 路径用无检查点图导致 flag 空转（`begin_checkpoint` 注释 `#1249`）。
14. **断点续跑重复消息**：重新传入初始状态会被 message reducer 追加导致重复；须传 `None` 续跑（`#1249`）。
15. **backend_url 跨 provider 泄漏**：提供 provider 专属 URL 需区分 tier，否则 OpenAI `/v1` 会被转发到 Gemini 产生畸形 URL（`default_config.py` 注释）。
16. **推理模型忽略 temperature**：默认 GPT-6 家族等 reasoning 模型不守 temperature；要复现须换非推理模型（README Reproducibility 节）。
17. **sentiment 源非点对点**：StockTwits/Reddit 只给近期不归档，历史 run 的情绪输入不保证 past 时点一致（`sentiment_analyst.py` 模块 docstring）。
18. **Jev 未配置时帖子不过滤**：无 `TYPESAFE_API_KEY` 时社交帖径直通过，无 bull/bear stance 计数（README）。
19. **EDGAR 需带联系地址**：SEC 拒绝无 User-Agent 联系方式的请求，默认给一个项目地址、建议自设 `SEC_EDGAR_USER_AGENT`（README）。
20. **不带组合≠空仓**：`portfolio=None` 时 PM 被告知"你不知道我方持仓现金，别按空仓 sizing"（`context.py:get_portfolio_context_from_state`）；空 positions 列表才算 flat book。

---

## 6 可借鉴的设计要点

1. **quick/deep 双档 LLM + 每档独立 provider**：分析/辩论/交易用快模型省钱，两个"拍板"经理用强模型；两档可跨厂商（经理走 Claude、其他走 OpenAI），工程上按 tier 单独建 client（`create_tier_client`）。
2. **两轮结构化辩论 + 显式轮次上限**：`should_continue_debate` / `should_continue_risk_analysis` 用 `count >= N*rounds` 硬截止，辩论者保留各自 history + 对手上一段，"评判强方"写进经理 prompt 以对抗"冲突就 Hold"的惰性。
3. **"读不出就 REVIEW，不降级"**：评级解析失败用不可交易的 `REVIEW` 哨兵而非 Hold，配合正则对"not a Sell / Rating Scale 图例"的防御，是 LLM 输出稳健性范本。
4. **点对点一致性贯穿全链路**：数据工具用 `InjectedState` 从图状态读 trade_date、统一 `as_of` clamp；EDGAR as-filed、记忆 lessons 按 `resolved_date` 过滤。这是做历史回测不泄漏的关键基建。
5. **记忆→结算→反思闭环**：追加式 markdown 日志 + 持有窗口跑完才结算 + 把定金句压缩成 2-4 句教训注入未来 run，是"多智能体持续学习"的轻量实现。
6. **不确定性全用哨兵文本而非估算**：`DATA_UNAVAILABLE` / `NO_DATA_AVAILABLE` / `(No X report)` / `(has not spoken yet)` 处处显式标注"缺≠无"，直击 LLM 幻觉。
7. **结构化输出 + 自由文本双路径**：`invoke_structured_or_freetext` 保证不支持的 provider 也能跑，且 render 回统一 markdown 半结构，下游存储/解析不变。

## 附：主要源文件索引

| 文件 | 内容 |
|---|---|
| `tradingagents/graph/trading_graph.py` | 主类 `TradingAgentsGraph`：入口 `propagate()`、检查点、决策记录、记忆结算编排 |
| `tradingagents/graph/setup.py` | `GraphSetup.setup_graph()` 组装 StateGraph；`_analyst_graph()` 分析师私有子图 |
| `tradingagents/graph/conditional_logic.py` | 两轮辩论的轮次路由 |
| `tradingagents/graph/propagation.py` | `create_initial_state()` 初始状态结构 |
| `tradingagents/graph/analyst_execution.py` | 分析师 node spec 定义（key/node/report_key/tools） |
| `tradingagents/agents/tools.py` | 全部数据工具（`InjectedState` + `as_of`） |
| `tradingagents/agents/analysts/*.py` | 四分析师 prompt 与报告生成 |
| `tradingagents/agents/researchers/*.py` | 多空研究员辩论 |
| `tradingagents/agents/risk_mgmt/*.py` | 激进/中性/保守风控辩论 |
| `tradingagents/agents/trader/trader.py` | 交易员提案 |
| `tradingagents/agents/managers/*.py` | Research Manager / Portfolio Manager |
| `tradingagents/agents/schemas.py` | 4 个 Pydantic 结构化输出 schema |
| `tradingagents/agents/rating.py` | 5 档评级 + REVIEW 哨兵解析 |
| `tradingagents/agents/context.py` | 身份锚定、语言指令、空值哨兵 |
| `tradingagents/dataflows/router.py` | 厂商链路由与错误三态 |
| `tradingagents/dataflows/config.py` | run 级 config 上下文（ContextVar） |
| `tradingagents/dataflows/vendors/*` | yfinance/alpha_vantage/sec_edgar/fred/polymarket/stocktwits/reddit 具体封装 |
| `tradingagents/memory/log.py` | 追加式记忆日志 |
| `tradingagents/memory/settlement.py` | 决策结算 + 基准 alpha 计算 |
| `tradingagents/memory/reflection.py` | 反思生成 |
| `tradingagents/reporting.py` | 报告树写入 |
| `tradingagents/default_config.py` | 全部默认配置 + `TRADINGAGENTS_*` 环境变量映射 |