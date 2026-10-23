# FinRobot 深度分析

> **分析日期**：2026-10-11 · **仓库**：[AI4Finance-Foundation/FinRobot](https://github.com/AI4Finance-Foundation/FinRobot)（本地 HEAD `2717499`，2026-09-28）
> **分析依据**：本地克隆源码（`refrences/AI4Finance-Foundation__FinRobot`），精读三目录 `finrobot_autogen/`、`finrobot_equity/`、`finrobot_desktop/`（重点是 production 的 `finrobot_desktop/finrobot/engine/`）。所有结论来自源码实现与 docstring，非推断。
> **配套文档**：[开源股票分析项目深度分析-20261011](./开源股票分析项目深度分析-20261011.md)
> **论文**：arXiv:2405.14767（FinRobot whitepaper）

---

## 0 定位与架构总览

FinRobot 是 AI4Finance 的**开源 agentic AI 金融决策智能框架**。核心原则一句话（README 原文）：**"models reason, software computes, agents orchestrate, systems verify"** —— 模型可替换，真正让系统可靠的是周围的金融基础设施：工具、工作流、确定性计算、校验、溯源、评测。

与 FinGPT（单模型范式）不同，FinRobot 提供端到端 agentic 工作流，覆盖股权研究、量化交易、风险分析、投行、私募、财富管理。

### 三代并存（不是同一东西的三个版本）

| 版本 | Agent 框架 | 目录 | 定位 |
|---|---|---|---|
| V0 | AutoGen | `finrobot_autogen/` | 教育/参考——理解金融 Agent 怎么接线，复现论文，不建议生产 |
| V1 | OpenAI Agents SDK | `finrobot_equity/` | 自托管 Web 报告生成器（ticker→HTML/PDF 报告） |
| V2 | PydanticAI | `finrobot_desktop/` | **生产系统**（桌面工作站 + Web/CLI），9 Agent + 32 确定性算子 + 7 pipeline |
| V3 | DeepSeek-Harness | — | 开发中，"同样验证约束下更多自主性" |

`pip install finrobot` 装的是 V0（源码迁到 `finrobot_autogen/` 但 import 名不变）。

### 一条贯穿三代的设计红线

**"确定性的金融计算"与"LLM 叙事"严格分离**：所有数字由纯 Python 计算算子产出（带完整 provenance），模型只管推理/综合/解释/写报告。DCF/DDM/LBO/WACC/可比公司/蒙特卡洛全部走确定性代码路径。README 三行 slogan：`Numbers are code-calculated / Narratives are LLM-assisted / Every output is provenance-tracked`。

---

## 1 数据源来源（V2 `finrobot_desktop/finrobot/engine/data/`）

### 1.1 数据提供者链（`data/factory.py:build_provider_chain`）

优先级从高到低（`DataLayer.fetch` 顺链回退，`DataCache` 层缓存）：

| 顺序 | Provider | 提供 | 条件 |
|---|---|---|---|
| 1 | FMP | 财务三表(最准)+D&A+盈利惊喜+可交叉验证 | 有 `fmp_api_key` 才挂 |
| 2 | Finnhub | 新闻、公司画像 | 有 key 才挂 |
| 3 | yfinance | 价格回退、历史、无 FMP 时的财务 | **始终挂**（"Without it, yfinance handles financials, DCF uses simplified D&A formula, 10-20% deviation"） |
| 4 | SEC EDGAR (EdgarTools 5.31) | 10-K/10-Q/8-K/Form 4/13F/DEF 14A | **条件挂载**：`_is_valid_identity(settings.sec_user_agent)`（`Name email@domain` 有效才注册；否则 SKIP，SEC 相关路由降级"data unavailable"） |
| 5 | Adanos | 零售情绪/预测市场 | 有 `adanos_api_key` 才挂 |
| 6 | NewsAggregator | 新闻聚合 | 始终挂；用 yfinance `Ticker.news`（免费）+ Alpha Vantage sentiment（有 key 时） |

**两个数据契约**（`layer.py`）：
- `fetch()`：普通路径；NEWS 走 raw。
- `fetch_canonical()`：PRICE/FINANCIALS 必须走"经过校验 + provenance 盖章"的规范化契约（`NormalizedPrice`/规范快照），是**唯一真相源**——因为曾出现同一 artifact 里两个"当前价"（$391 叙事 vs $408.95 结构）的 bug。

### 1.2 技术栈

PydanticAI Agent + FastAPI + SQLite(aiosqlite) + React 19/Vite/Zustand/Tauri(Rust)。~184k 行，7 数据提供者 + failover。

---

## 2 分析逻辑与 Agent 机制（V2 核心）

### 2.1 9 个 Agent

| Agent | 角色 | 实现 |
|---|---|---|
| Lead Agent | 编排 + 只读工具 | 编排 5 个 pipeline 角色子 Agent + 3 个辩论 Agent |
| Data / Analysis / Modeling / Synthesis / Report | 5 个 pipeline 子 Agent | `agents/factory.py:create_sub_agents`，指令各载自 `instructions/{role}_agent.md` |
| Bull / Bear / Judge | 3 个辩论 Agent | `Bull ↔ Bear → Judge`（多空辩论到裁决） |

流水线（README 图）：`Data → Analysis → Modeling → Synthesis → Report`，再 `Bull ↔ Bear → Judge`，产出可追溯投研输出。

### 2.2 工具契约与 `ModelRetry` 语义（`factory.py` 里的 `query_financial_data` tool）

子 Agent 唯一的数据工具 `query_financial_data(ticker, data_type)`，两个入参都是 LLM 选的，故在进 fetch 之前**先当闸门**：

- **坏 ticker** → 返回错误字符串（通常是模型幻觉的代码，让它看到错误并自己补叙）；
- **坏 data_type** → `raise ModelRetry(...)`（枚举外，模型**能**自纠，pydantic-ai 会把它变回 `RetryPromptPart` 喂回去，受 `max_retries` 限制）。docstring 明确记录历史 bug："之前非法 data_type raise 一个裸 ValueError，pydantic-ai 不捕获，一路穿透子 Agent run，烧掉整个 pipeline step"。
- **PRICE / FINANCIALS** 强制走 `fetch_canonical`（规范化契约，quote 数字须逐字引用，"single source of truth"）；`NormalizedPrice` 只回 **most-recent bars + derived summary**——之前 agent 把 52 周升序序列里的**最旧** bar 当成"recent"叙述（2026-06-09 TSLA 的一年旧窗口）。

### 2.3 确定性计算算子（`engine/compute/operators/`，32 个纯 Python）

26 个估值/分析算子 + 6 个审计算子，7 个 coordinators：

**估值算子**：`dcf.py`、`ddm.py`、`lbo.py`、`sotp.py`、`sotp_scenario.py`、`monte_carlo.py`、`multiples.py`、`residual_income.py`、`wacc.py`、`valuation_aggregator.py`、`valuation_synthesis.py`、`earnings.py`、`forward_estimates.py`、`catalyst.py`、`cyclical_peers.py`、`peer_screen.py`、`ownership.py`、`signal.py`、`sniper.py`、`report_drift.py`、`xbrl_aligned_comps.py` 及各 `*_seed.py`。

**关键设计："算子输出数字 + 机器码，绝不输出人读散文"**——`dcf.py:ReverseSolveReason` 枚举（`SOLVED / NOT_CONVERGED / TARGET_ABOVE_RANGE / TARGET_BELOW_RANGE / OUT_OF_WACC_RANGE / GORDON_UNDEFINED`）替换了原来的英文 f-string 提示（后者绕过 i18n 被面板原样渲染），展示层再映射本地化句子、插值结构化字段，"展示不重算任何一个数字"。

`calculate_dcf` docstring 自述："这段代码做的是原始 LLM 做不了的事：逐年投影 revenue/EBITDA/FCF、按正确时点折现、Gordon-Growth 终值、EV−净债务得股权价值。每个数字可从类型化 `DCFInputs` 复现；LLM 只选假设。"（`mid_year=True` 用年中惯例，投行标准，通常提高估值 3-6%）。

### 2.4 审计算子（`compute/operators/audit/`，6 个，defense-in-depth）

| 算子 | 防什么 |
|---|---|
| `currency_caliber.py` | 汇率口径：quote 货币分子 / reporting 货币分母的比值（PE/EV/EV-EBITDA/EV-Rev）维度错误（SAP 29x、TSM 0.16x 类 bug）；`foreign_issuer_usd_tags` 防"外国发行人双 USD 标签"（red-team BP case） |
| `ev_bridge.py` | EV 桥接 |
| `narrative_divergence.py` | 叙事与动量背离 |
| `narrative_numeric_grounding.py` | 叙事绑定数字（见下） |
| `sector_sign.py` | 板块符号 |
| `ttm_period.py` | TTM 期间一致性 |

**`narrative_numeric_grounding`（最值得抄）**：盲审 GOOGL/MSFT/KO 产物发现 `narrative/catalysts/risks` 全是"模板化、只有结论没有数字"（GOOGL 用虚高的 headline P/E 说"有吸引力"却丢掉自己的核心 P/E；MSFT 全是"strong moat"空话；KO 催化无日期无幅度）。于是：(1) thesis prompt 注入 **NARRATIVE ARGUMENT RULE**（每段/每条必须引用白名单数字）；(2) 本模块做**确定性兜底**——正则剥离"裸日历年(1900-2099)"和"序数(1st/2nd)"两类假阳性后，检查是否有数字残留；无数字的段落/条目打非阻断 `[NARRATIVE-NUMERIC]` warning（只降 READ 评级，不动 verdict/price_target/confidence）。

### 2.5 7 条 research pipelines（`engine/pipelines/`）

`equity_research`（主，13 章报告）/ `dcf` / `comps` / `ddm` / `lbo` / `earnings_analysis` / `ic_memo`。`base.py`/`step.py`/`runner.py`/`validators.py`/`registry.py` 构成 pipeline 基座；`_thesis_prompt.py` 是论文叙事的 prompt 装配。

### 2.6 V0（AutoGen）与 V1（OpenAI Agents SDK）要点

- **V0 `workflow.py`**：`FinRobot(AssistantAgent)` 从 `agent_library.py` 库按名加载配置；`_preprocess_config` 把 `responsibilities` 拼成 `role_system_message`、`group_desc` 拼成 `leader_system_message`。四种工作流：`SingleAssistant`（单 Agent + UserProxy + code exec）、`SingleAssistantRAG`（注册 RAG 函数工具）、`SingleAssistantShadow`（影子 Agent：`register_nested_chats` 触发 `instruction_trigger`，用于让 agent 自我审查指令）、`MultiAssistant`（GroupChat + 自定义 speaker selection）与 `MultiAssistantWithLeader`（Leader + 嵌套 chat，按 `[AgentName]` 文本触发下放任务）。
- **V1 `equity_agents/`**：9 个报告章节 Agent（company_overview / risks / investment_overview / valuation_overview / major_takeaways / news_summary / competitor_analysis / tagline + agent_manager），配 `valuation_engine.py` / `sensitivity_analyzer.py` / `financial_data_processor.py` / `html_renderer.py`，产出 `finrobot_equity/core/output/*.html`。

---

## 3 最终产物结构与格式

- **13 章股权研究报告**（`equity_research` pipeline，桌面端 `research report` 视图）：含 12 个月目标价 + 区间、模型置信度、审计算子提出的数据 caveats。
- **DCF 表格**：标注 `code-computed`（十年预测表），旁栏是 LLM 把三种估值方法读回来说的段落。
- **机器码 + 结构字段**：估值算子返回 `DCFResult` 等类型化结果 + 枚举机器码，展示层本地化渲染。
- **`Finding` 结构**（审计）：`{field_key, check, severity(blocked_field/review/warning), evidence}`，可追溯地挂在报告/快照上。
- **HTML/PDF**（V1 的 `pdf_generator.py` / `html_template_professional.py`）。
- **Skill 库**：56 个分析师 playbook（股权研究/投行/私募/财富管理）。

---

## 4 工程实现要点

### 关键代码路径（V2）

| 关注点 | 路径 |
|---|---|
| Agent 工厂（9 Agent） | `finrobot_desktop/finrobot/engine/agents/factory.py:create_sub_agents` |
| 数据工具（canonical + ModelRetry） | `agents/factory.py:query_financial_data` |
| 数据层 + provider 链 | `engine/data/{factory,layer,cache}.py`（`build_provider_chain` / `fetch_canonical`） |
| 估值算子 | `engine/compute/operators/{dcf,ddm,lbo,sotp,monte_carlo,wacc,multiples,...}.py` |
| 审计算子 | `engine/compute/operators/audit/{currency_caliber,ev_bridge,narrative_divergence,narrative_numeric_grounding,sector_sign,ttm_period}.py` |
| coordinators | `engine/compute/coordinators/{market,news,extractor,historical_extractor,segment_extractor,technical_payload,dcf_seed}.py` |
| pipelines | `engine/pipelines/{equity_research,dcf,comps,ddm,lbo,earnings_analysis,ic_memo}.py` |
| 汇率归一 | `engine/data/normalize/`（`fx_normalize.normalize_financialdata_to_usd`） |
| V0 工作流 | `finrobot_autogen/finrobot/agents/{workflow,agent_library,prompts}.py` |
| V1 报告 | `finrobot_equity/core/src/modules/{equity_agents/*,valuation_engine,sensitivity_analyzer,html_renderer}.py` |

### 冷启动优化

`builder` docstring 记录：sidecar boot 路径先塞占位 DataLayer，再在 warmup 任务里 `add_providers(...)`，把 ~0.5s 的 provider 模块 import 移出冷启动关键路径；`shutdown_data_layer` 收尾 `aiosqlite` 线程 + WAL checkpoint，避免"Event loop is closed" 挂起（多脚本退出阻塞数分钟，2026-06-24 实况）。

---

## 5 坑与注意事项（全部来自代码 docstring/README，逐条）

1. **同一 artifact 两个"当前价"**：data agent 直接从 raw `fetch()` 叙述价格，而结构/估值字段用 canonical 契约 → 一个 artifact 里 $391（叙事）vs $408.95（结构）。修法：PRICE/FINANCIALS 强制 `fetch_canonical`，quote 数字"逐字引用"（`factory.py` 注释，2026-06-09 TSLA）。
2. **裸 ValueError 烧掉整个 pipeline step**：非法 `data_type` 在工具里 raise，pydantic-ai 不捕获 → 穿透子 Agent。修法：坏枚举改 `ModelRetry`（可自纠→回喂），坏 ticker 返回字符串（不可自纠→让它看着错叙述）。
3. **把最旧 bar 当"recent"叙述**：52 周升序序列直灌，agent 讲"recent"却引用最旧 bar。修法：PRICE 只回 most-recent bars + derived summary。
4. **汇率口径错**：外国 ADR（TSM USD/TWD、SAP USD/EUR）quote 货币分子 / reporting 货币分母的直接比（PE/EV/EBITDA）维度污染（SAP 29x、TSM 0.16x）。主 path 靠 `fx_normalize` 归一 USD/USD；`currency_caliber` / `foreign_issuer_usd_tags` 做 defense-in-depth backstop。
5. **外国发行人双 USD 标签不可信**：yfinance 把本土货币报送者误标 USD/USD 时，两个标签一致会让 `currency_caliber` 眼盲（red-team BP case）；`foreign_issuer_usd_tags` 专门抓"国家≠美国 + 双 USD + 无 FX 归一痕迹 + is_adr=None"的组合。
6. **叙事空转无数字**：GOOGL/MSFT/KO 的 thesis 输出"只有结论没有支撑数字"。修法：prompt 注入 NARRATIVE ARGUMENT RULE + `narrative_numeric_grounding` 确定性兜底（剥年份/序数两类假阳性，"别把年份误判为数字支撑"）。
7. **算子返回英文散文绕过 i18n**：reverse-DCF solver 返回英文 f-string，被面板原样渲染。修法：改 `ReverseSolveReason` 枚举机器码，展示层映射本地化句子，不重算。
8. **SEC EDGAR 条件挂载**：`sec_user_agent` 身份无效则 SKIP provider，应用照常启动、SEC 路由降级"data unavailable"+ banner 引导配置——"应用必须在身份缺失时也能干净启动"被写进 spec。
9. **无 FMP 时财务走 yfinance**：DCF 用简化 D&A 公式会产生 10-20% 偏差；`validate_runtime_config` 在 FMP 未设时打 warning（`factory.py` 注释原话）。
10. **Yahoo RSS 404**（BUG-072）：NewsAggregator 旧免费源 Yahoo RSS 已被下线，改为经 yfinance `Ticker.news` 读同批头条。
11. **冷启动 import + 退出挂起**：provider 模块 import ~0.5s 移出冷启动；`shutdown_data_layer` 收 aiosqlite 线程防"Event loop is closed"（2026-06-24）。
12. **桌面包未公证**：macOS 未 Apple-notarized，首次启动需 `xattr -cr /Applications/FinRobot.app`（README）。

---

## 6 可借鉴的设计要点

1. **"数字代码算、叙事模型写"的硬分离 + provenance**：这是本项目全部价值所在——估值走纯 Python 算子、返回类型化结果 + 机器码而非散文，LLM 只选假设和写解读。要让 LLM 不碰数字、数字可复现、展示不重算。
2. **`fetch_canonical` 单一真相源**：价格/财务强制走规范化契约，"one physical number" 杜绝"叙事里的价≠结构里的价"，配合"逐字引用"提示和 most-recent-bars 摘要防前视。
3. **`ModelRetry` 与"错误字符串"的二分**：LLM 选的参数分两类——能自纠的（枚举外→`ModelRetry` 回喂）和不能自纠的（幻觉 ticker→返回错误字符串让它看着错叙述），这是工具层防幻觉的精细解法。
4. **审计算子做 defense-in-depth backstop**：先把正确规则注入 prompt（NARRATIVE ARGUMENT RULE / FX 归一），再写一个确定性 checker 在代码里抓"不配合的 LLM"——镜像 pattern（inject instruction then verify in code）用了两处。
5. **汇率口径是金融数据的第一坑**：ADRs 的 quote/reporting 双货币、双 USD 标签误判，都是"确定性 sanity bounds 抓不到的静默缺陷"（倍数落在合理区间内但维度错），必须专门 verifier。
6. **数据 provider 链 + 条件挂载**：FMP(有 key) → Finnhub(有 key) → yfinance(始终) → SEC EDGAR(身份有效) → Adanos(有 key) → NewsAggregator(始终)，每个源挂不挂都有明确降级语义与 warning。
7. **三代同库演进**：把"教育/参考(V0)→自托管报告(V1)→生产(V2)"并存于一个仓库，让人按需取用，且每代记录其 agent 框架的选择逻辑。

## 附：主要源文件索引

| 文件 | 内容 |
|---|---|
| `finrobot_desktop/finrobot/engine/agents/factory.py` | 9 Agent 创建 + `query_financial_data` 工具（canonical/ModelRetry） |
| `finrobot_desktop/finrobot/engine/agents/instructions/*_agent.md` | 5 子 Agent + 3 辩论 Agent 指令 |
| `finrobot_desktop/finrobot/engine/data/factory.py` | provider 链组装（`build_provider_chain`） |
| `finrobot_desktop/finrobot/engine/data/layer.py` | `DataLayer.fetch` / `fetch_canonical` |
| `finrobot_desktop/finrobot/engine/data/providers/*.py` | 7 个数据源实现 |
| `finrobot_desktop/finrobot/engine/data/normalize/` | FX 归一（`fx_normalize`） |
| `finrobot_desktop/finrobot/engine/compute/operators/dcf.py` | DCF + `ReverseSolveReason` 机器码 |
| `finrobot_desktop/finrobot/engine/compute/operators/{wacc,ddm,lbo,sotp,monte_carlo,multiples,...}.py` | 估值算子族 |
| `finrobot_desktop/finrobot/engine/compute/operators/audit/*.py` | 6 个审计算子（含 `narrative_numeric_grounding`、`currency_caliber`） |
| `finrobot_desktop/finrobot/engine/pipelines/*.py` | 7 条 pipeline（`equity_research` 等） |
| `finrobot_autogen/finrobot/agents/workflow.py` | V0 AutoGen 四种工作流 |
| `finrobot_autogen/finrobot/agents/{agent_library,prompts}.py` | V0 agent 库与 prompt 模板 |
| `finrobot_autogen/finrobot/data_source/*.py` | V0 数据源（finnhub/fmp/sec/yfinance/reddit） |
| `finrobot_equity/core/src/modules/equity_agents/*.py` | V1 报告章节 Agent |
| `finrobot_equity/core/src/modules/{valuation_engine,sensitivity_analyzer,html_renderer}.py` | V1 估值/敏感性/渲染 |