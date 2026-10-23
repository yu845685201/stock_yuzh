# TradingAgents-astock 深度分析

> 分析日期：2026-10-11 · 仓库 URL：https://github.com/simonlin1212/TradingAgents-astock · 分析依据：本地克隆源码（refrences/simonlin1212__TradingAgents-astock，v0.5.21）· 配套：../开源股票分析项目清单-20261010.md

## 0 定位与架构总览

TradingAgents 论文（arXiv:2412.20138，源自 TauricResearch）的 **A 股深度特化 fork**：把"分析师团队 → 多空辩论 → 研究经理 → 交易员 → 三方风控辩论 → 组合经理"这套 LangGraph 多智能体流水线，从数据层、Agent 角色、交易规则三个维度本土化。定位明确写死在 README 末尾——"框架的工程实现，不是投资产品"。全 Apache 2.0、`pip install -e .` 即跑、零外部付费数据服务。

编排流水线（`tradingagents/graph/setup.py` 的 `GraphSetup.setup_graph()` 逐节点加边）：

```
START → Market Analyst → Social → News → Fundamentals → Policy → Hot Money → Lockup
      （7 个分析师串行，每个带 ToolNode 工具循环 + Msg Clear 清消息）
   → Quality Gate（质量门控）
   → Bull Researcher ⇄ Bear Researcher（多空辩论，max_debate_rounds 轮）
   → Research Manager（综合出投资计划，deep LLM）
   → Trader（套 A 股交易约束）
   → Aggressive ⇄ Conservative ⇄ Neutral（三方风控辩论，max_risk_discuss_rounds 轮）
   → Portfolio Manager（终审，deep LLM）→ END
```

两类核心设计：

1. **双 LLM 分档**（`DEEP_ROLES = {"research_manager", "portfolio_manager"}`）：需要综合全局做决策的两个节点走 `deep_think_llm`，其余全部走 `quick_think_llm`。默认 MiniMax（`deep_think_llm=MiniMax-M2.7`、`quick_think_llm=MiniMax-M2.7-highspeed`）。
2. **"报告中文输出、内部辩论保持英文"**（`get_language_instruction()`）：辩论/研究经理/交易员/风控内部一律英文保证推理质量，只在 7 个分析师和组合经理的对外报告注入中文要求。

## 1 数据源来源（逐接口）

全部免费免 Key。`tradingagents/dataflows/a_stock.py` 内所有 URL 均直连 HTTP（除 mootdx 为 TCP），无 akshare/tushare 依赖。东财请求统一走 `_em_get()` 节流入口（串行限流 ≥EM_MIN_INTERVAL 秒 + 0.1~0.5s 随机抖动 + Keep-Alive 复用）。

| 接口名（函数/URL） | 用途 | 覆盖 | 鉴权 |
|---|---|---|---|
| `get_stock_data` → `_mootdx_call("bars")`（TCP 7709，800 根日线） | OHLCV K 线 | 沪深京 A 股 | 无，自建服务器探测（`_reachable_tdx_servers`/`_probe_tdx`，默认 2s 超时）+ BESTIP 保护 |
| `_sina_kline_fallback`（money.finance.sina.com.cn CN_MarketData.getKLineData） | K 线兜底/补漏 | 沪深 | 无 |
| `_tencent_quote`（qt.gtimg.cn/q=） | PE(TTM/静态)/PB/市值/流通市值/换手率/涨跌幅/涨停价跌停价 | 沪深 | 无 |
| `get_fundamentals` → mootdx `finance` 快照（eps/bvps/roe/profit/income/股本） | 财务快照（季度） | 沪深 | 无 |
| 东财 `push2.eastmoney.com/api/qt/stock/get`（f57/f58/f84/f85/f127/f116/f117/f189） | 行业/总股本/流通股本/总市值/流通市值/上市日期 | 沪深 | 无（走 `_em_get` 限流） |
| `get_balance_sheet`/`get_cashflow`/`get_income_statement` → 新浪 `quotes.sina.cn/cn/api/openapi.php/CompanyFinanceService.getFinanceReport2022` | 财报三表（近 20 期起，历史拉更多期） | 沪深京 | 无 |
| `_ths_eps_forecast`/`get_profit_forecast`（basic.10jqka.com.cn/new/{code}/worth.html） | EPS 一致预期（机构数/最小值/均值/最大值）→ 前向 PE/PEG/消化 PE 年数 | 沪深 | 无（GBK 编码，经 `pd.read_html` 解析） |
| `get_news` → 东财 `search-api-web.eastmoney.com/search/jsonp`（备源新浪 `vip.stock.finance.sina.com.cn/corp/view/vCB_AllNewsStock.php`） | 个股新闻 | 沪深 | 无 |
| `get_global_news` → 财联社 `cls.cn/nodeapi/telegraphList`（兜底东财 `np-weblist.eastmoney.com/comm/web/getFastNewsList`） | 全球宏观快讯 | 全市场 | 无 |
| `get_hot_stocks` → 同花顺 `zx.10jqka.com.cn/event/api/getharden/` | 当日涨停股 + 题材归因 reason tags（同花顺编辑部标注） | 沪深 | 无 |
| `get_northbound_flow` → 同花顺 `data.hexin.cn/market/hsgtApi/method/dayChart/` | 北向资金（沪深股通）分钟级/20 日历史 | 沪深 | 无 |
| `get_concept_blocks` → 百度 `finance.pae.baidu.com/api/getrelatedblock` | 概念/行业（申万）/地域板块归属 + 当日涨幅 | 沪深 | 无 |
| `get_fund_flow` → 东财 `push2.eastmoney.com/api/qt/stock/fflow/kline/get` | 个股主力资金（超大/大/中/小单，实时+20 日） | 沪深 | 无 |
| `get_dragon_tiger_board` → 东财 `datacenter-web.eastmoney.com/api/data/v1/get`（reportName 龙虎榜） | 龙虎榜上榜记录、买卖营业部席位、机构参与 | 沪深 | 无 |
| `get_lockup_expiry` → 东财 datacenter（reportName 限售解禁） | 历史解禁 + 未来 90 天待解禁（数量/占比/影响） | 沪深 | 无 |
| `get_industry_comparison` → 东财 `push2.eastmoney.com/api/qt/clist/get` | 90 个行业涨跌幅/成交额/净流入排名 + 领涨股 | 沪深 | 无 |

数据分层抽象在 `dataflows/interface.py`：`route_to_vendor()` 按 `data_vendors`/`tool_vendors` 配置把工具名路由到 `a_stock`/`alpha_vantage`/`yfinance` 三个 vendor 之一（默认全部 `a_stock`），且只对 `AlphaVantageRateLimitError` 做 vendor 级 fallback。

## 2 分析逻辑与 Agent 机制

### 2.1 Agent 角色与协作流程

**7 个分析师**（`agents/analysts/*`，每个 `create_xxx_analyst(llm)` 返回节点工厂，`llm.bind_tools(tools)` 工具循环 + `tool_calls==0` 时把 content 落入对应 report 字段）：

| 角色（文件） | 工具集 | 输出 state 字段 |
|---|---|---|
| 市场分析师 `market_analyst.py` | get_stock_data, get_indicators | market_report |
| 舆情分析师 `social_media_analyst.py` | get_news/fund_flow/hot_stocks/stock_data | sentiment_report |
| 新闻分析师 `news_analyst.py` | get_news/global_news/insider | news_report |
| 基本面分析师 `fundamentals_analyst.py` | fundamentals/balance_sheet/cashflow/income/profit_forecast/industry_comparison | fundamentals_report |
| 🏛️政策分析师 `policy_analyst.py` | get_news, get_global_news | policy_report |
| 🔥游资追踪师 `hot_money_tracker.py` | stock_data/news/insider/hot_stocks/northbound/concept/fund_flow/dragon_tiger/industry_comparison（9 个工具） | hot_money_report |
| 🔓解禁监控师 `lockup_watcher.py` | insider/news/fundamentals/lockup_expiry | lockup_report |

每个分析师系统提示词都带「必采清单」（数据点必须出现，取不到标 `[数据缺失: xxx]`）。三个 A 股角色的提示词内置完整制度知识，如解禁监控师的减持新规（持股 5%+ 每 90 天集中竞价 ≤1%、大宗 ≤2%；董监高每年 ≤25%；2024《减持管理办法》第十/十一条的破发/破净/分红不达标三禁止情形）。

**工具层**（`agents/utils/core_stock_tools.py` 等 6 个文件）：LangChain `@tool` 包装，最终都 `route_to_vendor()`。信号层工具（`signal_data_tools.py`）含防前视告警——`get_profit_forecast` 的 `curr_date` 必填，靠它判断是否复盘历史并加"未来函数"警示。

**质量门控**（`agents/quality_gate.py`）：分析师与 Bull Researcher 之间的独立节点，两层——Layer1 硬检查（`_hard_check_report`：`MIN_REPORT_LENGTH=200`、`FAILURE_MARKERS`（"无法获取"/"unable to fetch" 等 5 类失败句）、`[数据缺失` 计数 ≥3 判 C、有无 Markdown 表格）；Layer2 LLM 复审（报告超 3000 字截断后送审，失败报告 ≥4 份时跳过 LLM 复审）。产出 `data_quality_summary` 进下游。

**Bull/Bear 辩论**（`researchers/`）+ **Research Manager**（`managers/research_manager.py`，deep LLM，结构化 `ResearchPlan`，五档评级 Buy/Overweight/Hold/Underweight/Sell）+ **Trader**（`trader/trader.py`，结构化 `TraderProposal`，明确要求"不要给买卖价位/止损/目标价/仓位"）+ **三方风控辩论**（`risk_mgmt/` aggressive/neutral/conservative）+ **Portfolio Manager**（deep LLM 终审）。多空与风控辩论用 `conditional_logic.py` 的 `should_continue_debate`/`should_continue_risk_analysis` 控制轮数（默认 `max_debate_rounds=1`、`max_risk_discuss_rounds=1`）。

### 2.2 Trader 的 A 股交易约束（trader.py 系统提示词硬编码）

- T+1：当日买入下一交易日才能卖；
- 涨跌停：主板 ±10%、创业板/科创板 ±20%、北交所 ±30%；明确注明"ST/*ST 不缩窄——主板 ST 2026-07-06 起从 ±5% 改为 ±10%，创业板/科创板 ST 一直是 ±20%"；
- 新股前 5 个交易日无涨跌幅限制（北交所仅首日）；
- 最小手数：主板/创业板 100 股（百股整数倍）、科创板 200 股起（1 股递增）、北交所 100 股起；
- 交易时段：集合竞价 09:15-09:25、连续 09:30-11:30/13:00-14:57、收盘竞价 14:57-15:00、盘后固定价格 15:05-15:30（2026-07-06 起全 A 股）。

### 2.3 记忆与自我反思闭环（agents/utils/memory.py + graph/reflection.py）

`TradingMemoryLog` 是追加式 Markdown 日志。两阶段：

- **Phase A**（`store_decision`，零 LLM）：每次分析落 `[日期 | 代码 | 评级 | pending]` 条目，分隔符 `<!-- ENTRY_END -->`（HTML 注释不会出现在 LLM 正文里，作硬分隔安全）。
- **Phase B**（`_resolve_pending_entries`）：下次跑同一只票时，先对 pending 条目拉真实行情（`_fetch_returns`，用 **yfinance** 只做回填收益用途，`_normalize_yfinance_ticker` 把 6 位码映射成 `.SS/.SZ`，北交所 920/43x/83x/87x 因 Yahoo 无覆盖直接短路跳过），算 raw_return 与 alpha（对沪深 300 `000300.SS`）→ `Reflector.reflect_on_final_decision()` 生成反思 → 原子批量写入（tmp 文件 + `os.replace()` 防写崩）。

`get_past_context()` 把同票最近 5 条 + 跨票最近 3 条反思注入下一次分析的初始 state（`propagator.create_initial_state(..., past_context=...)`）。

### 2.4 决策绩效统计（performance.py，`tradingagents performance`）

零 LLM 调用、只读记忆日志。核心指标 **`direction_accuracy`（方向正确率）**：只统计有方向的评级（Hold 记为无方向），`看多要跑赢、看空要跑输` 才算对，判定用 alpha（拿不到 alpha 退回绝对收益）。另给 `up_rate`（标的上涨占比）与 `outperform_rate`（跑赢沪深300占比），并明确注释"这两个只描述标的，与判断对错无关"。还有 `rating_monotonicity()` 评级区分度检验（Buy→Sell 平均 alpha 是否单调递减，五档都有样本才下结论）。`MIN_MEANINGFUL_SAMPLE = 20`，不足自动标"这些比率基本是噪音"；收益解析不出来的记录**跳过**（不当 0%）而非计入——防止统计被拉向中性。

## 3 最终产物结构与格式

- **决策信号**：`signal_processing.py` 的 `SignalProcessor.process_signal()` 从 Portfolio Manager 终审文本提取核心决策（Buy/Hold/Sell），`propagate()` 返回 `(final_state, decision)`。
- **完整 state 落盘**（`_log_state`）：`{results_dir}/{ticker}/TradingAgentsStrategy_logs/full_states_log_{date}.json`，含 7 份分析师报告、debate/risk 两段完整历史、trader_plan、investment_plan、final_trade_decision、missing_data 快照。
- **CLI/批量报告**：`examples/run_cases.py` 复用 `save_report_to_disk()` 输出 `complete_report.md`（分析师/研究/交易/风险/组合五分区）+ `summary.json`。
- **Web UI**（Streamlit，`web/app.py`，12 阶段 pipeline 进度）：信号卡片、7 份报告可展开、可导出 Markdown（零依赖）或 PDF（`web/pdf_export.py` 使用 fpdf2，自动适配中文字体）；历史记录自动保存；侧栏 10 个 LLM 供应商 + OpenAI 兼容自定义 base_url，配置写入 `~/.tradingagents/llm_config.json`。
- **绩效报告**：`tradingagents performance [--json]` 输出人读 Markdown/机器读 JSON。

## 4 工程实现要点（关键代码路径）

| 关注点 | 实现 |
|---|---|
| 配置合并 | `trading_graph.py::merge_config()` 浅合并 `DEFAULT_CONFIG`，修复了"直接整体替换导致照 README 粘贴即崩"（#101） |
| role_llms 分角色模型 | `_build_role_llms()`：相同 (provider,model,base_url,api_key) 复用实例；角色名写错直接 `ValueError`；换 provider 不带主 provider 的 `backend_url` 与专属参数（`_PROVIDER_SPECIFIC_KWARGS`） |
| 超时/重试 | `_resilience_kwargs(target_provider)`：所有 provider 给 `timeout`（默认 150s，堵住 LangChain 把 `None` 显式传 httpx 导致"永不返回"的洞，#100）；仅 OpenAI 兼容类给 `max_retries=0` + 应用层 `app_retries=3` 指数退避（5s→10s→20s） |
| Claude 个人订阅 | `claude_agent_sdk` provider（`[agentsdk]` extra），`deep/quick_think_provider_override` 走个人订阅额度；降级配置 `agent_sdk_fallback_provider/model` 必须成对、启动时校验；`ANTHROPIC_API_KEY` 与订阅并存时从子进程剥离防悄悄按 token 计费 |
| 断点续跑 | `checkpoint_enabled` 时用 langgraph SqliteSaver + per-ticker thread_id 恢复；`graph/checkpointer.py` |
| 缺失数据追踪 | `dataflows/missing_data.py`：`attach_missing_data_snapshot`/`make_tool_call_recorder`（wrap_tool_call 记录每次工具调用的缺失结果）/`mark_resolved_tasks_consumed`，v0.5.18 起支持缺数据任务重试重放 |
| 评级解析 | `rating.py::parse_rating()` 四级优先（英文标签→中文标签→裸英文词→裸中文词），中文五档映射（买入/增持/持有/减持/卖出），`_WORD_CONTINUATION` 边界修复"连字符/加粗被误判评级" |

## 5 坑与注意事项（来自 CHANGELOG/代码注释，逐条）

1. **财报三表曾静默空表 5 个月**（v0.5.21，#116/#117）：2026-05-17 去 akshare 时新浪财报解析只认 `data.<fzb|lrb|llb>` 行列表，而实际结构是 `data.report_list{报告期: {..., data:[...]}}`，导致 balance_sheet/cashflow/income 一直返回"No data found"正常字符串而非报错，基本面分析师把"没有财报"当事实写进报告。
2. **财报未来函数**（v0.5.21）：原按"报告期 ≤ 分析日"筛选会把 8 月才公布的半年报在 07-15 就露给模型。现按"公告日 < 分析日 或 入库时间(北京) < 分析日"判定已公布；两日期都被改写晚于一年以上时按法定披露截止日（一三季报 1 个月/半年报 2 个月/年报 4 个月）判断；识别新浪 2020-03-13 全市场批量入库日并与"逾期公司一次补发多期"区分。
3. **谷歌依赖结构性冲突**（v0.3.1，#87）：`langchain-google-genai>=4.0.0` 要求 `httpx>=0.28.1`，mootdx 钉死 `httpx>=0.25,<0.26`，无解——直接移除 `[google]` extra；mootdx 取数走 TCP、运行时不 import httpx，故抬高 httpx 不影响取数。
4. **未来函数告警一度是死代码**（v0.5.6→v0.5.8）：`get_profit_forecast` 的 `curr_date` 原为可选，模型传空串时判定"非历史"，告警永不触发，今天的预测被当分析日当天事实。
5. **历史资金流被过滤成一条不剩**（v0.5.6）：放大窗口后没裁回；v0.5.8 修复。
6. **评级静默误判 Hold**（rating.py 多轮修复 v0.5.10~v0.5.14）：中文标签后跟英文评级词（"最终评级：Buy"）躲过所有规则落到默认 Hold；边界从 `(?![A-Za-z])`→枚举标点→最终收敛为"后面字符会不会让它变成另一个词"（`Sell-off`/`Buy2024`/`Underweight(估值偏高)`/`建议：**Sell**-off` 的正则回溯陷阱）。
7. **方向正确率显著性分母错**（v0.5.8）：曾用"已结算总数"判定显著性，20 条里只有 1 条有方向时会抑制警告却显示"方向正确率 100%"；改为按 `directional_count` 单独判。
8. **绩效对看空评级算反**（v0.5.5）：看空要"跑输"才算对，曾用绝对收益判。
9. **持有期解析**：记忆日志 tag 是 `5d` 带后缀，直接 `int()` 吞成 None → 平均持有期永远显示不出来（performance.py 注释）。
10. **mootdx 脆弱性**：#90 反复降级永不恢复、首次全表不可用卡几分钟、只试 10 台就判"全网不可达"、提前收手漏服务器、"探测会覆写用户配置的服务器"（v0.5.10）、BESTIP 快照取到空默认值（v0.5.11）——围绕 `_get_mootdx_client`/`_reachable_tdx_servers`/`_preserve_mootdx_bestip` 的一串修复。
11. **东财封禁触发条件**（README 注明实测）：每秒 >5 请求 / 并发 ≥10 / 1 分钟 ≥200 触发封 IP，故 `_em_get()` 串行限流 + 抖动。
12. **DeepSeek V4/MiniMax M2.x 结构化输出不稳定**（#83）：走 `structured.py::invoke_structured_or_freetext`——结构化失败回落自由文本再用 `parse_rating` 兜底解析。
13. **北交所代码 Yahoo 无覆盖**：`_is_unsupported_by_yfinance` 对 920/43x/83x/87x 短路，避免每次空跑一个请求、记忆条目永远 pending。
14. **深市/北交所财务取数**：v0.5.21 实测 600519（沪）、000001（深）、920819（北交所）三张表都能取到，此前只有"不支持"或空表。

## 6 可借鉴的设计要点

1. **"报告中文、辩论英文"的分层语言策略**：把"输出可读性"与"推理质量"解耦，是对中文模型/英文底座混用的务实折中。
2. **评级解析的正则收敛方法论**：判定规则从"枚举允许字符"改为"问一个字符会不会把它变成另一个词"——这是可复用的 NLP 边界设计，且把轮次教训写进注释。
3. **绩效统计的自省纪律**：方向正确率只认 alpha 口径、Hold 不计入、样本 <20 自标噪音、解析失败跳过不归零、评级单调性检验——处处在防"拿稀疏样本自欺"。
4. **记忆闭环的零成本设计**：判决零 LLM 落盘，回填收益只在下次跑同票时惰性发生，记忆注入只取同票 5 + 跨票 3，控制上下文膨胀。
5. **对国内公开数据源的防御式降级**：mootdx→新浪 K 线兜底、东财→新浪新闻兜底、东财→财联社快讯兜底、腾讯实时估值、mootdx TCP 不封 IP 优先于东财 HTTP，配合 `_em_get` 节流——一套成熟的多源容错链路。
6. **健康依赖自检**：把"移除有解冲突的 extra、运行时校验降级配置、缺 key 指名报变量名"当成一等工程实践。

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `tradingagents/graph/trading_graph.py` | 主入口 `TradingAgentsGraph`：双 LLM 客户端、role_llms、超时/重试、订阅降级、记忆回填、`propagate()` |
| `tradingagents/graph/setup.py` | LangGraph 拓扑（节点+边+`DEEP_ROLES`/`ROLE_KEYS`） |
| `tradingagents/graph/conditional_logic.py` | 多空/风控辩论轮数控制 |
| `tradingagents/graph/reflection.py` | `Reflector.reflect_on_final_decision()` 反思 |
| `tradingagents/dataflows/a_stock.py` | A 股数据 vendor 全部实现（约 2500 行，真实数据源） |
| `tradingagents/dataflows/interface.py` | 工具→vendor 路由、`VENDOR_METHODS` 映射、fallback 链 |
| `tradingagents/dataflows/missing_data.py` | 缺失数据追踪/重试/工具调用记录 |
| `tradingagents/agents/analysts/*.py` | 7 个分析师节点（policy/hot_money/lockup 为 A 股特化） |
| `tradingagents/agents/trader/trader.py` | Trader（A 股 T+1/涨跌停/手数/时段约束） |
| `tradingagents/agents/managers/*.py` | Research Manager + Portfolio Manager |
| `tradingagents/agents/researchers/*.py`、`risk_mgmt/*.py` | 多空研究员、三方风控辩手 |
| `tradingagents/agents/utils/memory.py` | `TradingMemoryLog` 追加式决策日志 |
| `tradingagents/agents/utils/rating.py` | 五档评级中英文解析 |
| `tradingagents/agents/utils/structured.py` | 结构化输出 + 自由文本回落 |
| `tradingagents/agents/quality_gate.py` | 报告两层质量门控 |
| `tradingagents/performance.py` | 决策绩效统计（direction_accuracy/评级单调性） |
| `tradingagents/llm_clients/` | 11 家 LLM client 工厂、model_catalog、validators |
| `CHANGELOG.md` / `CHANGES_FROM_UPSTREAM.md` | 版本坑记录 / 与上游改动对照 |