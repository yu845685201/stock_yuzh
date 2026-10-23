# daily_stock_analysis 深度分析

> **分析日期**：2026-10-11 · **仓库**：[ZhuLinsen/daily_stock_analysis](https://github.com/ZhuLinsen/daily_stock_analysis) v3.32.0（本地 HEAD `c3349de`，2026-10-11，含 `[Unreleased]` 未发布变更） · **论文**：arXiv:2608.26990
> **分析依据**：本地克隆源码（`refrences/daily_stock_analysis`），精读 `src/agent/`（多智能体编排）、`src/schemas/`、`data_provider/`、`strategies/`、`src/analyzer.py` / `src/stock_analyzer.py` / `src/market_analyzer.py` 与 `AGENTS.md`、`docs/CHANGELOG.md`。所有结论来自源码实现、代码注释与 CHANGELOG，非推断。
> **配套文档**：[开源股票分析项目深度分析-20261011](./开源股票分析项目深度分析-20261011.md)

---

## 0 定位与架构总览

"股票智能分析系统"：面向 **A股/港股/美股/日股/韩股/台股/ETF** 自选股的每日 AI 分析 + 决策仪表盘 + 多渠道推送（企业微信/飞书/Telegram/Discord/Slack/邮件）。核心是两条可切换的分析流水线：

1. **经典单 Agent 流水线**（`src/analyzer.py` + `src/stock_analyzer.py`）：抓数据 → 技术/新闻检索 → LLM 出报告 → 推送。
2. **多 Agent 流水线**（`src/agent/`，`AGENT_ARCH=multi` 时启用，由 `AgentOrchestrator` 编排）：Technical → Intel → Risk →（specialist/skill 批量）→ Decision 五阶段。

### 多 Agent 编排流水线（`AgentOrchestrator._execute_pipeline`）

```
阶段链（_build_agent_chain，按 mode 决定）：
  quick      : technical ────────────────────────▶ decision        (~2 次 LLM 调用)
  standard   : technical ─▶ intel ──────────────▶ decision        (默认)
  full       : technical ─▶ intel ─▶ risk ──────▶ decision
  specialist : technical ─▶ intel ─▶ risk ─▶ [skill 批量并发] ─▶ decision
                              │                     ▲
                              └─ 非关键阶段(intel/risk/skill)失败可降级，不中断
```

- `VALID_MODES = ("quick", "standard", "full", "specialist")`；配置 `mode` 传入 `strategy|skill` 会归一化为 `specialist`。
- 每个阶段是一个继承 `BaseAgent` 的子类（`technical/intel/risk/decision` + 批量 `skill`），`run(ctx, ...)` 返回 `StageResult`，用共享 `AgentContext` 串行传递观点（`ctx.opinions`、`ctx.risk_flags`）。
- 非关键阶段判定 `NON_CRITICAL_BASE_STAGES = {"intel", "risk"}` + skill agent 名字集合：这些阶段 `FAILED` 时只降级记录（`_record_degraded_stage`）不中断整条 pipeline；technical/decision 失败则整体终止。
- `specialist` 模式的 skill 子代理在 decision **之前**惰性插入，用线程池并发跑（`AgentSkillScheduler`，并发 `agent_skill_concurrency` 默认 3、上限 4），结果经 `StrategyEngine` 合并成 `skill_consensus`。

### 关键设计红线（来自 AGENTS.md / 代码注释）

- 内部把"策略"统一叫 `skill`（可复用策略能力包），YAML 声明、代码/配置/API 字段用 `skill`。
- 数据源/单一通知渠道失败**不应拖垮主流程**（fail-soft）；只有核心阶段失败才 abort。

---

## 1 数据源来源

行情取数统一在 `data_provider/`，`base.py` 里 `BaseFetcher`（`priority` 数字越小越优先）+ `DataFetcherManager`（`self._fetchers = sorted(..., key=priority)`）做**优先级链式 fallback**。每个数据源一个 `*_fetcher.py`。

### 1.1 行情/基础数据源

| Fetcher | 提供内容 | 覆盖市场 | 鉴权 |
|---|---|---|---|
| `akshare_fetcher.py` | 行情、K线、大盘统计、板块/概念排名、涨停池、龙虎榜、财报、资金流 | A股为主 | 无（免费，受限流） |
| `baostock_fetcher.py` | 日线/历史 | A股 | 无 |
| `pytdx_fetcher.py` | 实时行情、分时（TDX 协议） | A股 | 无 |
| `tencent_fetcher.py` | 实时行情、日线 | A股/港股 | 无（有坑见 §5） |
| `efinance_fetcher.py` | 行情 | A股/港美股 | 无 |
| `yfinance_fetcher.py` | 行情、K线、基本面（`yfinance_fundamental_adapter.py`） | 美股/港股等 | 无 |
| `tushare_fetcher.py` | 行情、财务、龙虎榜等 | A股 | `TUSHARE_TOKEN` |
| `tickflow_fetcher.py` | 行情（token 型） | A股/港美股 | `TICKFLOW_TOKEN`（默认 priority=2） |
| `longbridge_fetcher.py` | 行情（港美股） | 港/美 | `LONGBRIDGE_*` |
| `futu_fetcher.py` | 行情（`futu_fundamental_adapter.py`） | 港/美 | Futu OpenD |
| `finnhub_fetcher.py` | 美股行情/新闻 | 美股 | `FINNHUB_API_KEY` |
| `alphavantage_fetcher.py` | 美股行情 | 美股 | `ALPHA_VANTAGE_API_KEY` |
| `miaoxiang_fetcher.py` | 妙想（复盘数据/资金流） | A股 | 需配置 |

### 1.2 新闻搜索

| 服务 | 说明 |
|---|---|
| Anspire / AIHubMix | 一 Key 同时提供 LLM + 联网搜索 |
| SerpAPI / Tavily / Bocha / Brave / MiniMax / SearXNG | 通用搜索（`search_service.py`） |

### 1.3 fallback 语义（`base.py:DataFetcherManager`）

- `source_order`/`priority` 决定尝试顺序，逐个尝试，失败记录 `fallback_to`（下一个源名）作为诊断，成功源会带上 `fallback_from` 字段。
- 支持 `*_PRIORITY` 环境变量即时调整单项优先级（如 `YFINANCE_PRIORITY=0`）；指数/Longbridge `preferred` 锚定首选不被普通优先级覆盖。
- 三类错误（`DataFetchError` / `RateLimitError` / `DataSourceUnavailableError`）区分处理；`get_realtime_quote` 港股市有独立 `futu_hk_realtime_source_priority` 链。
- 免费源（AkShare/Baostock/YFinance）默认零配置可跑，"免费源受上游限流/接口变动影响，稳定性不保证"，定时批量建议 Token 型源。

---

## 2 分析逻辑与 Agent 机制

### 2.1 角色分工（`src/agent/agents/`）

| Agent | agent_name | max_steps | 工具子集 | 结构化输出 |
|---|---|---|---|---|
| TechnicalAgent | `technical` | 6 | 实时价/日线/趋势/MA/量能/形态/筹码/上下文 8 个工具 | `{signal(strong_buy..strong_sell), confidence, reasoning, key_levels{support/resistance/stop_loss}, trend_score, ma_alignment, volume_status, pattern}` |
| IntelAgent | `intel` | 4 | `search_stock_news` / `search_comprehensive_intel` / `get_stock_info` / `get_capital_flow` + FXMacroData | `{signal, confidence, reasoning, risk_alerts[], positive_catalysts[], sentiment_label, capital_flow_signal, key_news[]}` |
| RiskAgent | `risk` | 4 | 新闻/实时价/股票信息 + FXMacroData（宏观日历/公告/风险情绪/事件预测） | `{risk_level, risk_score, flags[{category,severity,description,source}], veto_buy, reasoning, signal_adjustment}` |
| SkillAgent（批量并发） | `skill:{id}` | — | 由策略 YAML `required_tools` 决定 | skill 观点（signal/confidence/reasoning） |
| DecisionAgent | `decision` | 3 | **无工具**（`tool_names=[]`，纯综合） | Decision Dashboard JSON |

**基类机制**（`base_agent.py:BaseAgent`）：
- `run()` 五步：build system+user → 注入预取数据（`_inject_cached_data`，每字段截断 8000 字符，前一轮已抓的数据不重复抓）→ `run_agent_loop`（ReAct 循环，`max_steps` 上限、`max_wall_clock_seconds` 超时）→ `post_process` 结构化 → append 到 `ctx.opinions`。
- **记忆校准** `_apply_memory_calibration`：`opinion.confidence = raw_confidence × calibration.calibration_factor`（历史命中率校准因子），结果写进 `result.meta["memory_calibration"]`。
- **记忆注入** `_build_memory_context`：同票最近 3 次分析（`signal/sentiment/price/outcome_5d/outcome_20d/was_correct`）+ 决策信号复盘，且明示"只当上下文，别照抄；别把历史命中率当买卖强度信号"。

### 2.2 编排器机制（`orchestrator.py:AgentOrchestrator`）

- **时间预算与降级**（`_execute_pipeline`）：`agent_orchestrator_timeout_s`（默认 0=禁用）为整条 pipeline 的协作预算。剩余不足 `_MIN_STAGE_BUDGET_S=15` 秒（且已有阶段完成）则跳过后续阶段，board 产出 `_build_budget_skip_result`（`"多 Agent 预算不足…自动降级生成"`并打 `[降级结果]` 前缀）；超时则 `_build_timeout_result`。跳过/超时前会先 `_apply_partition_fallback`（把无效 skill 观点分流到诊断区）。
- **每 agent 独立超时 clamp**（`_get_sub_agent_timeout_map`）：`agent_technical_agent_timeout_s` / `agent_intel_agent_timeout_s` / … 逐 agent 配置，与 pipeline 剩余预算取 min。
- **max_steps 语义**（`_prepare_agent`）：全局 `max_steps` 等于默认（`AGENT_MAX_STEPS_DEFAULT=10`）时各 agent 用自己的 per-agent 上限（decision 只 3 步，不被撑到 10）；用户显式调大才全量生效；调小则所有 agent 被压到全局值。
- **skill 批量调度**（`_run_specialist_agent_batch`）：`AgentSkillScheduler(max_concurrency, timeout_per_skill)` 线程池并发；预算切分 `_skill_batch_timeout_slice` 按 `ceil(count/worker)` 波次均摊。
- **风险接管**（`_apply_risk_override`，`agent_risk_override` 默认 true）：`build_risk_override_plan` + `build_risk_override_application` 在 decision 结果上后置应用。soft → 下调信号，hard → 否决买入（`veto_buy`）。接管后 sentiment_score 会被 clamp 到对应档（buy 60-79 / hold 40-59 / sell 0-39，`_adjust_sentiment_score`），分析摘要加 `[风控下调: X -> Y]` 前缀，operation_advice 改写为 `买入（原建议已被风控下调）` 等。

### 2.3 决策权重与打分（`decision_agent.py` system_prompt 内写死）

- **信号权重**：技术 ~40% / Intel·情绪 ~30% / 风险 ~30%（**负向 override：任一 high 风险把信号封顶到 hold**）；skill 观点存在时按 20% 权重 blend，其余同比例缩减。
- **sentiment_score 分段**：80-100 buy（全满足）/ 60-79 buy（有瑕疵）/ 40-59 hold（混合或风险在）/ 20-39 sell（负趋势+风险）/ 0-19 sell（重大风险+看空）。
- **行动护栏（Actionability Guardrails）**：不得因单日涨跌直接在 buy/sell 间翻转；价格夹在支撑阻力之间且资金流不单边时倾向 hold/watch；buy 需支撑确认或放量突破阻力；sell 需跌破支撑、主力持续流出或风险显著。
- **signal_attribution**（可选）：`technical_indicators/news_sentiment/fundamentals/market_conditions` 四权重（0-100，非零须和为 100，全零=无有效信号不许伪造）+ `strongest_bullish_signal/bearish_signal`。

### 2.4 Skill（策略）机制（`src/agent/skills/` + `strategies/*.yaml`）

- 策略 = 自然语言 YAML，**零代码扩展**（`strategies/README.md`）。字段：`name/display_name/description/category/core_rules[]/required_tools[]/aliases[]/default_active/default_router/default_priority/market_regimes[]/instructions`（instructions 是核心，用自然语言写判断/入场/出场，可引用工具名）。
- 内置 15 策略 YAML：`bottom_volume`(缩量见底)、`box_oscillation`(箱体震荡)、`bull_trend`、`chan_theory`(缠论)、`dragon_head`(龙头)、`emotion_cycle`(情绪周期)、`event_driven`(事件驱动)、`expectation_repricing`、`growth_quality`(成长质量)、`hot_theme`(热点)、`ma_golden_cross`(均线金叉)、`one_yang_three_yin`(一阳三阴)、`shrink_pullback`(缩量回调)、`volume_breakout`(放量突破)、`wave_theory`(波浪)。
- **SkillRouter**（`skills/router.py`，规则型选择）：优先级 = 用户显式指定 → `agent_skill_routing`=manual 用配置 → regime 检测 → default 集。`_detect_regime` 用 technical 观点判定：bullish 且 trend_score≥70 → `trending_up`；bearish 且 ≤30 → `trending_down`；neutral 或 35≤score≤65 → `sideways`；`volume_status==heavy` 且 30<score<70 → `volatile`；`sector_hot` 单独。
- **StrategyEngine / SkillAggregator**：把多个 skill 观点合并成加权 `skill_consensus`。**只有 engine 能写 `strategy_synthesis`**（orchestrator 会把 LLM 手写的该字段 pop 掉）。
- **证据链与诊断分离**（`_partition_skill_opinions` / `_run_strategy_engine`）：signal 缺失/无法识别的 skill 观点被移入 `ctx.meta["invalid_opinions"]`（diagnostics），**绝不回流证据链**；DecisionAgent 只消费 valid 观点，invalid 仅被指示"在 data_limitations 中标注，不得作为决策依据"。

### 2.5 意图识别（Web Chat 层，`web_intent_resolver.py`）

六步流水线把一条用户消息解析为保序任务列表：多任务切分（消费型标点+顺序连接词）、同句平权分解（个股/指数/市场词/板块/追问继承/存疑代码判定）、跨市场同名歧义聚合为 `confirm_stock` 确认（组级 pending，矛盾回复判新话题，拒绝词取消全链）、LLM 多意图兜底协议（`intents` 数组保序、`followup` 伪标签继承、`stock_code` 三道防幻觉闸门）。意图枚举 `WebIntent`（五互斥意图 + 上下文继承）。

---

## 3 最终产物结构与格式

### 3.1 Decision Dashboard JSON（`src/schemas/report_schema.py:AnalysisReportSchema`）

顶层键：`stock_name / sentiment_score(0-100) / trend_prediction / operation_advice / decision_type(buy|hold|sell，枚举不许发明新值) / confidence_level / analysis_summary / key_points / risk_warning / dashboard{...}`。

`dashboard` 嵌套块（`Dashboard`）：

| 块 | 关键字段 |
|---|---|
| `core_conclusion` | `one_sentence`(≤30字) / `signal_type`(🟢买入信号/🟡持有观望/🔴卖出信号) / `time_sensitivity` / `position_advice{no_position, has_position}` |
| `data_perspective` | `trend_status{ma_alignment,is_bullish,trend_score}` / `price_position{current_price,ma5/10/20,bias_ma5,bias_status,support/resistance}` / `volume_analysis` / `chip_structure{profit_ratio,avg_cost,concentration,chip_health}` |
| `intelligence` | `latest_news / risk_alerts[] / positive_catalysts[] / earnings_outlook / sentiment_summary` |
| `battle_plan` | `sniper_points{ideal_buy,secondary_buy,stop_loss,take_profit}` / `position_strategy{suggested_position,entry_plan,risk_control}` / `action_checklist[]` |
| `phase_decision` | `phase_context/action_window/immediate_action/watch_conditions[]/next_check_time/confidence_reason/data_limitations[]`（盘中阶段护栏） |
| `signal_attribution` | 四权重 + 最强多/空信号名（见 §2.3） |
| `agent_disagreement_explanation` | `base_disagreement/risk_control/degraded_events/pipeline_termination/data_quality/final_action/decision_path`（编排护栏的"可解释性"投影） |

`agent_disagreement_explanation` 里几个 Pydantic 模型带校验：`RiskControlExplanation` 要求 applied 时必须有 from→to 迁移且结束在 post_risk_signal；`AgentDisagreementExplanation` 校验 action 链连续（`final_action` 必须等于链末）。

### 3.2 报告文件

- 经典流水线生成 Markdown 报告（模板 `templates/`），可 `md2img`/`share_image` 转图分享。
- 多 Agent 流水线产物 = 上述 dashboard JSON + `stage_trajectories`（每阶段 status/steps/tool_calls/failure_reason/duration/tokens）+ `runtime_facts`。
- 推送通道：企业微信/飞书/Telegram/Discord/Slack 机器人 + 邮件（`src/notification*`、`bot/platforms/`）。

---

## 4 工程实现要点

### 关键代码路径

| 关注点 | 路径 |
|---|---|
| 任务入口 | `main.py`（`--stocks/--market-review/--schedule/--serve/--etf-rotation` 等） |
| 多 Agent 编排 | `src/agent/orchestrator.py:AgentOrchestrator._execute_pipeline()` / `_build_agent_chain()` |
| 工厂切换 | `src/agent/factory.py`（`AGENT_ARCH=single|multi`；`_VALID_AGENT_ARCH`） |
| Agent 基类 | `src/agent/agents/base_agent.py:BaseAgent.run()` |
| ReAct 循环 | `src/agent/runner.py:run_agent_loop()` / `parse_dashboard_json()` |
| 工具注册 | `src/agent/tools/registry.py`（`@tool(name=..., category=...)` + 类别超时 map） + `analysis_tools/data_tools/market_tools/search_tools/backtest_tools/fxmacrodata_tools` |
| 状态原型 | `src/agent/protocols.py:AgentContext / AgentOpinion / StageResult / StageStatus` |
| 记忆 | `src/agent/memory.py:AgentMemory`（历史 + 校准因子） |
| 数据源 | `data_provider/base.py:DataFetcherManager` + `*_fetcher.py` |
| 策略引擎 | `src/agent/skills/engine.py:StrategyEngine` / `scheduler.py:AgentSkillScheduler` |
| 报告 schema | `src/schemas/report_schema.py` |
| 决策信号落库 | `src/repositories/decision_signal_repo.py` + `src/services/decision_signal_outcome_service.py`（5/20 日复盘） |

### 工具面（供 agent 调用，`src/agent/tools/`）

`get_realtime_quote / get_daily_history / get_chip_distribution / get_analysis_context / get_stock_info / get_portfolio_snapshot / get_capital_flow`（data）；`analyze_trend / calculate_ma / get_volume_analysis / analyze_pattern`（analysis）；`get_market_indices / get_sector_rankings`（market）；`search_stock_news / search_comprehensive_intel`（search）；`get_skill_backtest_summary / get_strategy_backtest_summary / get_stock_backtest_summary`（backtest）；FXMacroData 宏观工具族（`fxmacrodata_tools.py`，USD 基础数据免 key）。

---

## 5 坑与注意事项（来自代码注释/CHANGELOG，逐条）

1. **腾讯科创板成交量单位是"股"不是"手"**：统一 ×100 会让 688/689 票的 `volume` 大 100 倍，需按号段区分换算；已入库历史需限定 `data_source='TencentFetcher'` 迁移修正（CHANGELOG `#2350`）。
2. **免费源不可靠**：AkShare/Baostock/YFinance 受上游限流、接口变动、网络波动影响；"上游无响应会长期占用分析线程"，故 AkShare 大盘统计、基本面/资金流/龙虎榜都加了子进程硬超时 + 跨管理器抑制重复请求（`#2340`）。
3. **yfinance 基本面同比别用固定 `iloc[4]`**：早期版本固定取第 4 列算去年同期，缺列时会把更早一季误当去年，现按日期匹配去年同期，找不到才回退 `info`（CHANGELOG）。
4. **收盘后漏取当日 K 线**：yfinance 日线 end 是排他的，需按美东日期把 end 推进到下一自然日；否则收盘后会漏当天（CHANGELOG）。
5. **休市/盘前占位 K 线**：选股日线会收到休市日或盘前占位线，须逐行核验带有效收盘价的交易日，拒绝夹杂休市日或无法解析日期的来源（CHANGELOG）。
6. **盘中缓存被"新鲜"误用**：旧版缓存无时区时回退文件 mtime，跨时区迁移后会把盘中日线当新鲜缓存；现改持久化获取时间判定，盘前占位 K 线不被当作盘中新鲜缓存（CHANGELOG）。
7. **局部模型不认中文模板**：`REPORT_LANGUAGE=en` 时若在中文模板末尾追加英文约束，`ollama/qwen3:14b` 之类小模型仍输出中文；现 en 用完整英文模板、ko 复用英文骨架（`#2352`）。
8. **妙想资金流按金额选表**：不能把收盘价或占比误当主力净流入金额，要按明确金额指标选表选列（CHANGELOG）。
9. **股票/指数身份混用**：同一代码的股票与指数要隔离，日线缓存须保留身份，防止模型改名换对象导致新闻对象漂移（CHANGELOG 多处）。
10. **推理/观测隔离**：选股流程里本地计算的因子（observed）与模型推断（inferred）必须分开标记，缺省/零分也要保留实际因子权重，不能拿策略目录原始重量补历史（CHANGELOG）。
11. **超时不虚构成交**：ETF 轮动回测在缺价日不虚构成交，防守资产缺价用现金；ETF 轮动按真实交易日历保留缺口防止调仓顺延/周末信号提前（CHANGELOG）。
12. **风险不降级成 Hold**：多 Agent 里 high 风险把信号封顶 hold 是"负向 override"，不是普通降级；RiskAgent 输出 `veto_buy` 硬否决。
13. **无效 skill 观点必须隔离**：signal 缺失/无法识别的 skill 观点只能进 `invalid_opinions` 诊断区，绝不进证据链；DecisionAgent 被告知"仅供 data_limitations 标注，不得作为决策依据"。
14. **JSON 解析失败≠空报告**：`parse_dashboard_json` 失败时 raw 文本存 `final_dashboard_raw` 兜底，最终 `_fallback_summary` 用已完成 opinions 拼纯文本。
15. **置信度与数据新鲜度绑定**：quote/日线/技术数据 stale/fallback/missing/fetch_failed/partial/estimated 时，`confidence_level` 不许为 High/高，且必须在 `confidence_reason`/`data_limitations` 反映（decision prompt 硬约束）。
16. **锁/轮询竞态**：告警列表只读展开持仓以避免请求时查询实时行情重写缓存；CLI 与 Web/API 调度共享 SQLite 计划认领避免重复推送（`#2349`）。
17. **超时后要推已落库**：runtime scheduler 硬超时后默认发送部分完成通知（`DSA_TIMEOUT_PARTIAL_NOTIFY` 未设或 true）——此前超时不推已落库个股会静默丢结果（`#2328`）。

---

## 6 可借鉴的设计要点

1. **"策略即 YAML"**：15 种交易策略用自然语言 YAML 声明（`required_tools` + `instructions`），零代码扩展，内置模板含缠论/波浪/情绪周期等本土打法，脚注写死"核心交易理念 1-7"（严进/趋势/量能/回踩/利空否决/量价/龙头放宽）。
2. **多阶段 Agent + 分层降级**：intel/risk/skill 定义为非关键阶段，失败/超时只降级不中断；15 秒最小阶段预算 + 每 agent 独立超时 clamp + 线程池说波次切分，是"LLM 流水线要有硬预算"的工程范本。
3. **风险后置接管（risk override）**：RiskAgent 给 soft/hard 两级旗标，编排器在 decision 之后统一降级/否决，再 clamp sentiment_score、改写摘要前缀和操作建议——用确定性规则兜住 LLM 的过度乐观。
4. **证据链与诊断分离**：无效 skill 观点单独进 diagnostics，下游决策只吃 valid 意见，且明确提示"不得作为决策依据"——对多 Agent 观点污染的治理很清晰。
5. **确定性规则补齐 LLM 输出**：`signal_attribution` 权重归一化到 100、价层去噪 `_coerce_level_value`（剥千分位/百分比）、`_pick_first_level` 兜底取支撑阻力，尽量不让 LLM 手写的坏数进最终键盘。
6. **记忆校准**：用历史命中率 `calibration_factor` 乘到 confidence，并记录 raw/calibrated 两值，是"让模型学会自省"的轻量做法。
7. **意图识别六步流水线**：把"跨市场同名歧义""防幻觉股票代码""多意图保序"做成显式状态机 + LLM 兜底，规避纯 LLM 解析的不可控。

## 附：主要源文件索引

| 文件 | 内容 |
|---|---|
| `src/agent/orchestrator.py` | 多 Agent 编排器（模式/预算/降级/风险接管/最终 dashboard 组装） |
| `src/agent/agents/base_agent.py` | Agent 基类（run 五步、缓存注入、记忆上下文/校准） |
| `src/agent/agents/{technical,intel,risk,decision}_agent.py` | 四阶段 Agent（prompt + post_process） |
| `src/agent/skills/{router,scheduler,engine,aggregator,skill_agent}.py` | 策略选择/并发调度/共识引擎 |
| `src/agent/strategies/*.py` | 旧策略兼容层 |
| `src/agent/runner.py` | ReAct 循环 + dashboard JSON 解析 |
| `src/agent/llm_adapter.py` | LLM + 工具适配 |
| `src/agent/tools/*.py` | 工具注册与实现（data/analysis/market/search/backtest/fxmacro） |
| `src/agent/protocols.py` | AgentContext / AgentOpinion / StageResult 协议 |
| `src/agent/memory.py` | 分析历史记忆 + 校准因子 |
| `src/schemas/report_schema.py` | Decision Dashboard Pydantic schema |
| `strategies/*.yaml` | 15 个内置交易策略（自然语言） |
| `data_provider/base.py` | DataFetcherManager（优先级链 fallback） |
| `data_provider/*_fetcher.py` | 各数据源适配 |
| `src/analyzer.py` / `src/stock_analyzer.py` / `src/market_analyzer.py` | 经典单 Agent 流水线（个股/大盘） |
| `main.py` | CLI 主入口 |
| `docs/CHANGELOG.md` | 收录大量修复与数据源坑 |