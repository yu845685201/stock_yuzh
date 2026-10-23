# TradingAgents-CN 深度分析

> **分析日期**：2026-10-11 · **仓库**：[hsliuping/TradingAgents-CN](https://github.com/hsliuping/TradingAgents-CN) v3.0.0 社区版（本地 HEAD `51060a7`，2026-09-22）
> **分析依据**：本地克隆源码（`refrences/hsliuping__TradingAgents-CN`），精读 `tradingagents/core/engine/`（阶段引擎/数据契约/血缘）、`tradingagents/agents/`（7 分析师）、`tradingagents/agents/utils/compliance_prompting.py`、`tradingagents/dataflows/`、`core/`（应用层）、`tradingagents/constants/data_sources.py` 与 README/CHANGELOG。所有结论来自源码实现与注释，非推断。
> **配套文档**：[开源股票分析项目深度分析-20261011](./开源股票分析项目深度分析-20261011.md)
> **上游**：本项目是 [TauricResearch/TradingAgents](https://github.com/TauricResearch/TradingAgents) 的 A 股专项分支（混合授权：核心引擎 Apache-2.0，应用层 Source-Available 专有）。

---

## 0 定位与架构总览

TradingAgents-CN 是**面向 A 股的多智能体研究辅助系统**（非交易工具、非投资建议，输出"研究观察"）。相比上游 TradingAgents（美股/全球、交易公司拟真），本分支的差异化在于：

1. **A 股数据源**：Tushare / AKShare / BaoStock（+ 港美股 yfinance/Finnhub/Alpha Vantage + Wind/Choice/QMT/Quandl），MongoDB 本地缓存为最高优先级数据源。
2. **7 个分析师**：在四大类基础上新增 `sector_analyst`（板块）、`index_analyst`（大盘）、`china_market_analyst`（中国市场特征/政策）。
3. **研究辅助合规化**：全角色注入"合规研究模式约束"——严禁输出买卖动作、目标价/止损止盈、仓位比例、交易时机。
4. **五阶段引擎 + 数据契约**：把分析流程重构成 `数据采集 → 分析师 → 研究辩论 → 交易决策 → 风险评估` 五个阶段，阶段之间通过**五层数据契约 + 血缘追踪**通信。
5. **平台化**：FastAPI + Vue3 + MongoDB/Redis；智能助手、Skill 中心、模拟交易、组合分析、学习中心、通知中心。

### 五阶段流水线（`tradingagents/core/engine/stock_analysis_engine.py`）

```
AnalysisPhase 顺序执行（每阶段有独立 PhaseExecutor，execute(context, data_manager)）：
  DATA_COLLECTION  →  ANALYSTS  →  RESEARCH_DEBATE  →  TRADE_DECISION  →  RISK_ASSESSMENT
  (数据采集)          (分析师,并行)   (多空研究员辩论)      (交易员整合)        (风险经理)
  任一阶段失败 → 整体 success=False 并 break
```

`StockAnalysisEngine.analyze(ticker, trade_date, company_name, market_type="cn")` 是主入口；阶段执行器可 `register_phase_executor` 外部插拔。

### 数据契约分层（`data_contract.py:DataLayer`）

| 层 | 枚举值 | 内容 |
|---|---|---|
| CONTEXT | `context` | 基础上下文（ticker/trade_date/market_type） |
| RAW_DATA | `raw_data` | 原始数据（价格/新闻/财务） |
| ANALYSIS_DATA | `analysis_data` | 结构化分析结果（技术指标/估值） |
| REPORTS | `reports` | 各分析师文本报告 |
| DECISIONS | `decisions` | 辩论历史/交易信号/风险评估 |

`AgentDataContract` 声明式描述每个 Agent 的 `inputs`（DataAccess: layer/fields/required）、`outputs`、`depends_on`，配合 `contract_validator.py` 校验契约完整性；`AnalysisContext.set(layer, key, value, source=...)` 每次写入带 `source`，形成**数据血缘**（verify 时 report/decision 可追溯来源）。

---

## 1 数据源来源

### 1.1 数据源注册表（`constants/data_sources.py:DATA_SOURCE_REGISTRY`）

| 数据源 | 市场 | 免 Key | 说明 |
|---|---|---|---|
| **MongoDB**（缓存） | a/hk/us/crypto/futures | ✅ | **最高优先级**本地缓存 |
| Tushare | A股 | ❌ | 高质量历史+实时+财务 |
| AKShare | A股+港股 | ✅ | 开源免费 |
| BaoStock | A股 | ✅ | 免费稳定历史 |
| yfinance / Yahoo | 美股+港股 | ✅ | 全球 |
| Finnhub | 美股 | ✅(免费版) | 实时+新闻 |
| Alpha Vantage | 美股 | ✅(免费版) | 技术指标 |
| IEX Cloud | 美股 | ❌ | 付费 |
| Wind / Choice | a/hk/us | ❌ | 专业终端 |
| QMT(迅投) | A股 | ✅(需券商) | 需 QMT 客户端运行，**不支持新闻** |
| Quandl | 美股 | ✅(免费版) | 经济数据 |
| Local File / Local / Custom | 全市场 | ✅ | 本地/API 导入/自定义 |

数据源同步体系（`dataflows/` + `core/skill_runtime/`）分**基础数据 / 历史行情 / 财务数据**三步同步，数据检验**纯本地只读**（v3.0 修复：不再现场拉外部数据，避免检验期间接口阻塞）。

### 1.2 取数层组织

- `tradingagents/dataflows/providers/china/{tushare,akshare,baostock}.py`：A 股三源实现 + `fundamentals_snapshot.py` 基本面快照。
- `tradingagents/dataflows/providers/us/{yfinance,finnhub,alpha_vantage_*}.py`、`providers/hk/{hk_stock,improved_hk}.py`。
- `tradingagents/dataflows/data_source_manager.py` / `stock_data_service.py`：统一路由 + 缓存。
- `tradingagents/dataflows/cache/`：`db_cache` / `mongodb_cache_adapter` / `file_cache` / `adaptive`（自适应缓存）/ `integrated`。
- `tradingagents/dataflows/news/{chinese_finance,realtime_news,google_news,reddit}.py`、`realtime_news_utils.py`、`data_completeness_checker.py`。
- `tradingagents/utils/enhanced_news_filter.py` / `news_filter.py`：A 股新闻去噪/相关性过滤。

---

## 2 分析逻辑与 Agent 机制

### 2.1 角色分工（7 分析师 + 辩论/决策层）

| Agent | 文件 | 定位 |
|---|---|---|
| Market Analyst | `agents/analysts/market_analyst.py` | 技术面（趋势/量价/形态） |
| News Analyst | `news_analyst.py` | 新闻事件 |
| Social Media Analyst | `social_media_analyst.py` | 舆情/情绪 |
| Fundamentals Analyst | `fundamentals_analyst.py` | 财务质量/估值 |
| Sector Analyst | `sector_analyst.py` | 板块表现/轮动/资金流 |
| Index Analyst | `index_analyst.py` | 大盘/风格/系统性风险 |
| China Market Analyst | `china_market_analyst.py` | 中国市场制度/政策/行业资金 |
| Bull/Bear Researcher | `researchers/` | 多空研究辩论 |
| Research Manager + Risk Manager | `managers/`（`risk_manager.py` 为新增） | 研究结论 + 风险治理 |
| Trader | `trader/trader.py` | 研究整合（非执行者） |
| Aggressive/Conservative/Neutral Debator | `risk_mgmt/` | 三方风险辩论 |

`graph/setup.py` 里分析师名映射 `{market/social/news/fundamentals/index_analyst/sector_analyst}`，另支持从 `core.agents.analyst_registry` 动态加载"无需工具调用"的扩展分析师（`requires_tools=False` 的一类，如 Sector/Index）。

### 2.2 阶段执行

- **AnalystsPhase**（`phase_executors/analysts.py`）：`ThreadPoolExecutor(max_workers=4)` 并行跑所选分析师，每个分析师结果经 `AgentIntegrator.extract_report` 写回 `DataLayer.REPORTS`；无集成器/未实现时走 `_run_stub_analyst`（生成占位报告，保证流水线不卡死）。输出字段映射 `market_report/news_report/sentiment_report/fundamentals_report/sector_report/index_report`。
- **ResearchDebatePhase / TradeDecisionPhase / RiskAssessmentPhase**：`debate_rounds=1`，均注入 `memory_config`（见 2.5）。
- **SignalProcessor**（`graph/signal_processing.py`）：把最终信号正则解析 + 映射到中文 `买入/持有/卖出`（明确"绝对不允许使用英文 buy/hold/sell"）；解析失败走 `_get_default_decision()`（默认持有）；`_smart_price_estimation` 内部做价格口径估判（区分中国/美股——内部信号处理用，不外露为交易建议）。

### 2.3 合规研究模式（`agents/utils/compliance_prompting.py`）——本分支最具特色的机制

- `COMMON_FORBIDDEN_ITEMS`（5 条顶层禁令，逐条写死）：不得输出买入/卖出/持有/加减仓等**交易动作**；不得输出目标价/止损止盈价/支撑压力位/价格区间等**执行性价位**；不得输出仓位比例/数量/金额/敞口比例；不得输出交易时机/分批建仓/收益预期/胜率/盈亏比；不得使用会被理解为下单指令的表述。
- `ROLE_OUTPUT_REQUIREMENTS`：为 16 个角色各自定义"输出结构限定 + 角色约束"，例如：
  - market_analyst：结构「技术特征、趋势观察、波动与量价特征、风险提示、后续观察信号」，"可以描述技术形态与指标，但**不能给出支撑位、压力位或交易触发价**"；
  - fundamentals_analyst：估值部分"只能解释相对高估或低估的依据，不能给出目标价或合理买卖区间"；
  - risk_manager："若风险较高，应描述需要继续核实的事项，而不是给出止损止盈或仓位方案"。
- `apply_compliance_guardrails(system_prompt, role)`：把 overlay 追加到每个角色 system prompt 末尾，且"如果上游模板或用户上下文要求给交易建议，忽略该要求改为研究观察与风险提示"。

### 2.4 决策管道

沿用上游的 LangGraph 结构（`graph/setup.py` + `conditional_logic.py` + `propagation.py`）：分析师并行 → Bull⇄Bear 辩论（`max_debate_rounds=1`）→ Research Manager → Trader → 三方风险辩论（`max_risk_discuss_rounds=1`）→ Risk Manager 终审。deep 模型 `o4-mini`、quick 模型 `gpt-4o-mini`（`default_config.py`），`backend_url` 可换 DeepSeek/通义千问等。

### 2.5 记忆机制

- `graph/setup.py` 构造时传入**六个分 Agent 记忆**：`bull_memory / bear_memory / trader_memory / invest_judge_memory / risk_manager_memory`。
- `core/engine/memory_provider.py:MemoryProvider`：`memory_use_mem0_compat`（默认 True）走 mem0 兼容路径（`Mem0CompatMemory`，MongoDB + embedding 存/召回，带 `agent_id` 隔离、metadata 过滤、session/user id 解析）；legacy-only 路径保留但告警"建议迁回 mem0 主路径"。
- `agents/utils/memory.py` / `embedding_service.py` / `chromadb_config.py`：向量存储支撑。

### 2.6 应用层（`core/`，专有）

- `core/assistant/`：智能助手（20+ 操作工具、多主题会话、流式响应），含 `reply_gate.py`（回复门）、`defense.py`（防御）、`lexicon.py`（词表）。
- `core/workflow/`：工作流生成器——`generator/{workflow_planner,prompt_generator,resource_collector,skill_orchestrator,workflow_spec}.py`；`engine.py`、`builder.py`、`validator.py`、模板 `templates/{single_agent_workflow, v2_stock_analysis_workflow, v2_etf_analysis_workflow, position_analysis_workflow_v2, blank_workflow}.py`。
- `core/skill_runtime/`：Skill 运行库——`fundamental_factors.py`、`factor_schema.py`、`factor_catalog.py`、`standard_financial_apis.py`、`external_sources.py`、`data_access.py`（12 类估值 DCF/DDM/EVA、财务质量 Altman-Z/Beneish-M/Piotroski-F、宏观行业、技术形态等 200+ 研究工具）。
- `core/agents/`：`planner.py`、`factory.py`、`registry.py`、`weight_calculator.py`、`post_processor.py`、`execution_trace_recorder.py`，及持仓/风险/多空/ETF 等 adapters。
- `core/hitl/`：人机协作（human-in-the-loop）。
- `core/backtest/`、`core/mcp/`、`core/gateway/`（含 licensing 授权体系）——Pro 专属能力在社区版代码里可见但受 license 门禁。

---

## 3 最终产物结构与格式

- **研究报告**（`Reports` 层 + `reports/` 目录）：各分析师文本报告 + 研究结论 + 风险汇总。合规约束下，输出为结构化"研究观察"章节（技术特征/事件摘要/板块位置/风险提示/后续观察信号），**不含**目标价/止损/买卖指令。
- **最终决策**（`DECISIONS` 层 `final_decision`）：内部信号经 `SignalProcessor` 规整为「买入/持有/卖出」（供内部研究结论使用），研究报告面向用户呈现时保持"研究辅助"措辞。
- **组合分析**：VaR、压力测试、Brinson 归因等 20+ 工具（`core/agents/adapters/position/` + `skill_runtime/sync_wrappers.py`）。
- **模拟交易**：虚拟资金 + 操作复盘 + 执行一致性检查。
- **推送**：站内通知 + WebSocket 推送 + SSE 事件流；邮件等为 Pro 能力。

---

## 4 工程实现要点

### 关键代码路径

| 关注点 | 路径 |
|---|---|
| 分析引擎主入口 | `tradingagents/core/engine/stock_analysis_engine.py:StockAnalysisEngine.analyze()` |
| 五层数据契约 | `tradingagents/core/engine/data_contract.py:DataLayer/AgentDataContract` |
| 契约校验 | `tradingagents/core/engine/contract_validator.py` |
| 上下文与血缘 | `tradingagents/core/engine/analysis_context.py:AnalysisContext.set(..., source=)` |
| 数据访问管理 | `tradingagents/core/engine/data_access_manager.py:DataAccessManager` |
| Agent 集成（Context↔State / 报告提取） | `tradingagents/core/engine/agent_integrator.py` |
| 阶段执行器 | `tradingagents/core/engine/phase_executors/{data_collection,analysts,research_debate,trade_decision,risk_assessment}.py` |
| 合规约束 | `tradingagents/agents/utils/compliance_prompting.py` |
| 信号规整 | `tradingagents/graph/signal_processing.py:SignalProcessor.process_signal` |
| 记忆 | `tradingagents/core/engine/memory_provider.py` + `agents/utils/{memory,embedding_service,chromadb_config}.py` |
| 应用层助手 | `core/assistant/{reply_gate,defense,lexicon}.py` |
| 工作流生成 | `core/workflow/generator/{workflow_planner,prompt_generator,skill_orchestrator}.py` |
| Skill 运行库 | `core/skill_runtime/{fundamental_factors,factor_catalog,data_access}.py` |
| 后端入口 | `app/main.py`（FastAPI）+ `app/router_registry.py` + `app/worker.py` |
| 数据源常量 | `tradingagents/constants/data_sources.py` |

---

## 5 坑与注意事项（来自代码注释/CHANGELOG，逐条）

1. **356 万文档 $or 全表扫描**：历史数据查询 $or 双字段探测从 3s 优化到 0.03s——数据量上来后必须建复合索引/优化探测（CHANGELOG）。
2. **数据检验不能现场拉取**：原版数据检验现场拉外部接口会阻塞校验；v3.0 改纯本地只读（CHANGELOG）。
3. **GapJob 断链刷 WARNING**：社区版缺失 Pro 模块时，模块缺失只一次性提示后静默停用，避免刷屏（CHANGELOG）。
4. **MongoDB 认证**：`start_dev.ps1` 需版本化启动注入 MongoDB 认证（`requires authentication` 报错根因）；开发环境用版本隔离端口与数据卷（v3.0/v2.1/v2.0 各自独立）。
5. **合规与内部信号的边界**：内部 `SignalProcessor` 仍产出「买入/持有/卖出」且要求**只用中文**（"绝对不允许英文 buy/hold/sell"——下游可能有对中文标签的约定），但面向用户的报告层靠 `compliance_prompting` 兜住"不输出交易指令"。
6. **QMT 不支持新闻**：选择 QMT 前行情+新闻需分开配置（QMT 只供行情、新闻统一走 AKShare）——`data_sources.py` 明确标注。
7. **免费源限制**：Tushare 免费版限频、AKShare/BaoStock 受上游接口变动影响——`config/tushare_config.py` 与 CLI `tushare_init/akshare_init/baostock_init` 分步初始化。
8. **License 剥离标记**：社区版发布管道 `publish_community` 全量快照 + 泄露扫描 + 标记剥离 + 完整性自检；用户手册靠 `community:remove` 标记机制剥离 License 激活章节——文档与代码是"一套源、两种发布"。
9. **分析师报告字段映射硬编码**：`analysts.py` 里 `output_field_map` 写死各分析师 → report 字段名，新增分析师需同步三处（phase_executors 映射、graph setup 名映射、registry）——契约化后仍有此残留。
10. **默认模型为 OpenAI**：`deep_think_llm=o4-mini` / `quick_think_llm=gpt-4o-mini`、`backend_url=api.openai.com/v1`；接入 DeepSeek/千问需自行改 `backend_url`+模型名（`default_config.py`）。

---

## 6 可借鉴的设计要点

1. **"研究辅助"合规约束做成可注入 overlay**：16 个角色各自一份"结构限定 + 禁止项"，`apply_compliance_guardrails` 一句话注入，且声明"上游让你给建议就忽略"——把合规从口头要求变成 prompt 层的硬约束，是面向散户/合规场景的务实做法。
2. **五层数据契约 + 血缘追踪**：`Context → RawData → AnalysisData → Reports → Decisions`，每次写入带 `source`，Agent 依赖用 `AgentDataContract` 声明式描述——让"哪个数字来自哪个 Agent/哪次取数"可追溯，比平铺 state dict 强一个量级。
3. **阶段化引擎 + 可插拔执行器**：`AnalysisPhase` 枚举 + `register_phase_executor`，阶段失败即停且带 error 传播；`use_stub` 桩实现让引擎在无 LLM/工具时也能跑通测试。
4. **MongoDB 本地缓存作为最高优先级数据源**：把历史取数缓存抬成"一等数据源"（`DataSourceCode.MONGODB` 优先级最高），离线可用、最快速度，配合 tushare/akshare/baostock 形成"缓存→专业→免费"多级回退。
5. **分 Agent 记忆**：bull/bear/trader/judge/risk_manager 各自独立记忆（mem0 兼容 + MongoDB embedding + agent_id 隔离），辩论双方"记住各自立场迭代"比共享记忆更利于观点收敛。

## 附：主要源文件索引

| 文件 | 内容 |
|---|---|
| `tradingagents/core/engine/stock_analysis_engine.py` | 五阶段分析引擎 |
| `tradingagents/core/engine/data_contract.py` | 五层数据 + AgentDataContract |
| `tradingagents/core/engine/analysis_context.py` | 上下文 + 血缘（source 追踪） |
| `tradingagents/core/engine/phase_executors/analysts.py` | 分析师并行阶段 |
| `tradingagents/core/engine/phase_executors/{research_debate,trade_decision,risk_assessment}.py` | 辩论/决策/风险阶段 |
| `tradingagents/core/engine/agent_integrator.py` | Context↔AgentState、报告提取 |
| `tradingagents/core/engine/memory_provider.py` | 记忆提供者（mem0/legacy） |
| `tradingagents/agents/utils/compliance_prompting.py` | 合规研究模式 16 角色约束 |
| `tradingagents/agents/analysts/{market,news,social_media,fundamentals,sector,index,china_market}_analyst.py` | 7 分析师 |
| `tradingagents/agents/managers/{research_manager,risk_manager}.py` | 研究/风险经理 |
| `tradingagents/graph/{trading_graph,setup,conditional_logic,propagation,signal_processing}.py` | LangGraph 编排 + 信号规整 |
| `tradingagents/constants/data_sources.py` | 数据源注册表（18 种） |
| `tradingagents/dataflows/providers/{china,us,hk}/` | 各市场数据源实现 |
| `tradingagents/dataflows/cache/` | MongoDB/文件/自适应缓存 |
| `core/assistant/` | 智能助手（回复门/防御/词表） |
| `core/workflow/generator/` | 工作流生成器（规划/提示词/技能编排） |
| `core/skill_runtime/` | Skill 运行库（因子/估值/财务质量） |
| `core/agents/{planner,factory,registry}.py` | 应用层 Agent 规划与注册 |
| `CHANGELOG.md` | v3.0.0 变更与修复记录 |