# FinGPT 深度分析

> **分析日期**：2026-10-11 · **仓库**：[AI4Finance-Foundation/FinGPT](https://github.com/AI4Finance-Foundation/FinGPT)（本地 HEAD `fdb04c9`，2026-09-23）
> **分析依据**：本地克隆源码（`refrences/AI4Finance-Foundation__FinGPT`），精读 `fingpt/FinGPT_Forecaster/`、`fingpt/FinGPT_ForexIntelligence/`、`fingpt/FinGPT_MultiAgentsRAG/`、`fingpt/FinGPT_Sentiment_Analysis_v1/`、`fingpt/FinGPT_Benchmark/`、`finogrid/` 与 README。所有结论来自源码实现与注释，非推断。
> **配套文档**：[开源股票分析项目深度分析-20261011](./开源股票分析项目深度分析-20261011.md)
> **论文**：arXiv:2306.06031（FinGPT）、arXiv:2310.04793（Benchmark）、arXiv:2307.10485（Democratizing data）、arXiv:2306.12659（Instruct-FinGPT）

---

## 0 定位与架构总览

FinGPT 是 AI4Finance Foundation 的**开源金融大语言模型框架**，核心主张三点（README 明确写出）：

1. **轻量适配优于重训**：BloombergGPT 混训耗资 ~$3M/53 天，而 FinGPT 用 LoRA 微调开源底座，单次微调成本 < $300，可月/周级更新。
2. **众包金融数据**：用自动数据策展管道（FinGPT-Forecaster 的 RLSP 流程 + 新闻/情绪多源）替代 Bloomberg 特权数据。
3. **RLHF 是"秘密配方"**：个人偏好（风险厌恶度、投资习惯、个性化 robo-advisor）靠 RLHF 注入——这是 BloombergGPT 没有的。

它不是单一系统，而是一个**子项目集合**（同一 `fingpt/` 包下多个平行模块）：

| 子项目 | 定位 | 状态 |
|---|---|---|
| `FinGPT_Forecaster` | 基于指令微调 Llama 的 robo-advisor（预测涨跌分箱 + 理由） | 已发布（HF 有模型） |
| `FinGPT_ForexIntelligence` | 机构式外汇分析的**多分析器编排框架**（8 分析器 + 权重 + 择时规则） | 框架成型，数据源仅 yfinance/tradingeconomics 实装 |
| `FinGPT_Sentiment_Analysis_v1` | 金融情绪分析（LoRA 微调，v1.0/v1.1） | 已发布 |
| `FinGPT_RAG` | 多源新闻检索增强（CNBC/SeekingAlpha/Yahoo/Google 爬虫 + 外部 LLM 分类） | 实验性 |
| `FinGPT_Benchmark` | 金融多任务评测基准（FinBERT-FinLLM 系列） | 已发布 |
| `FinGPT_MultiAgentsRAG` | 多智能体 RAG + 幻觉评测（HaluEval/TruthfulQA/MMLU） | 主要是 notebook + 评测脚本 |
| `finogrid/` | B2B 稳定币付款编排 + Agent-to-Agent(A2A) 微交易（FinGPT 智能体 off-hot-path 辅助） | 新平台，与 LLM 分析主线平行 |

---

## 1 数据源来源

### 1.1 FinGPT-Forecaster 的数据链（`prompt.py` + `market_sentiment.py`）

| 数据 | 来源 | 说明 |
|---|---|---|
| 公司画像 | Finnhub `company_profile2`（`FINNHUB_KEY`） | 拼"公司简介"自然语言段 |
| 加密画像 | yfinance `Ticker.info` | 拼"加密货币简介"段 |
| 历史新闻 | 数据集里已落盘的 `News` 列（JSON） | 按 `date[:8] ≤ end_date` 过滤，剔除"Looking for stock market analysis..."这类推广摘要 |
| 基本面 | 数据集 `Basics` 列 | period 带头，其余 k:v 逐行 |
| 市场情绪（新增） | Adanos API（`ADANOS_API_KEY`），4 源 `reddit/x/news/polymarket` | `SUPPORTED_SOURCES`，`MAX_LOOKBACK_DAYS=90` |

**市场情绪聚合逻辑**（`summarize_market_sentiment` + `_aggregate_period_signal`）：每日 `sentiment_score/mentions` → 按窗口取 `average_sentiment_score`（来源内平均）+ `total_mentions` → 跨源再平均；标签 `_sentiment_label`：`≥0.2 bullish`、`≤-0.2 bearish`、其余 `mixed`；`_source_alignment`：单源 `single-source`、极差 `≤0.15 aligned`、`≤0.4 mixed`、否则 `divergent`。

### 1.2 FinGPT-ForexIntelligence 的数据源（`config.py:DATA_SOURCES` + `data_sources.py`）

| 源 | base_url | key env | rate_limit | 实现状态 |
|---|---|---|---|---|
| Bloomberg | api.bloomberg.com | `BLOOMBERG_API_KEY` | 10/min | **未实装**（`logger.warning("Source ... not yet implemented")`） |
| Reuters | api.reuters.com | `REUTERS_API_KEY` | 15/min | 未实装 |
| Trading Economics | api.tradingeconomics.com | `TRADING_ECONOMICS_API_KEY` | 20/min | ✅ `TradingEconomicsSource` |
| FXStreet | fxstreet.com | `FXSTREET_API_KEY` | 30/min | 未实装 |
| Yahoo Finance | query1.finance.yahoo.com | — | 100/min | ✅ `YahooFinanceSource` |

`ForexDataSourceManager` 默认 `sources=["yfinance"]`（"Default to free source"），带 1 小时 TTL 内存缓存 + 逐源 fallback（`get_market_data` 空则试下一个）。新闻情绪在 `YahooFinanceSource.get_news_sentiment` 里**hardcode 返回 `sentiment:0`**（注释"生产应做 NLP"）——即框架的"分析器"多数是规则/占位，真正 LLM 只出现在 Forecaster。

### 1.3 finogrid 的支付 rails（`finogrid/corridors/`）

非行情数据，是跨境支付 rails：Brazil(PIX)/Nigeria(NIBSS)/India(UPI)/Argentina(CBU)/Vietnam(Napas)，Partner 用 Bridge + Plaid。`BrazilAdapter.config` 写死 `min_amount_usd=10 / max=50k`、`kyt_risk_threshold=7`、`wallet_sla_minutes=60` 等。

---

## 2 分析逻辑与"Agent/模型"机制

### 2.1 FinGPT-Forecaster（robo-advisor LLM）

**核心思路**：把"预测下周涨跌分箱 + 给理由"做成指令微调任务，训练 Llama（LoRA）。

**Prompt 构建链**（`prompt.py:get_all_prompts`）：
1. `info_prompt`：公司/加密简介段。
2. 循环每周窗口：取**过去 1–3 周**的子窗口做 few-shot 上下文（`sample_news` 每周期最多采样 5 条新闻；`prev_rows` 队列最多保留 `max_past_weeks`）。
3. 本周 `head`（`From {start} to {end}, price {increased/decreased} from X to Y`）+ 过滤后的新闻 + 基本面 + 市场情绪。
4. `PROMPT_END`：要求"先给 2-4 个正面因素和 2-4 个顾虑（从新闻推断），再预测下周 {prediction}，并写支撑总结；**预测必须从分析末尾推出，不能作为分析的基础因子**"。
5. 标签 `map_bin_label`：把 U/D+数字映射成自然语言（`U3→up by 2-3%`、`D5+→down by more than 5%`）。

**训练**（`train_lora.py`）：`train.sh` 调 `torchrun`，LoRA 微调（模块在 `FinGPT_RAG/instruct-FinGPT/training/utils/module/lora.py`）。`RLSP_GUIDE.md` 描述"RAG + LoRA + 强化学习"组合。中文版 `FinGPT-Forecaster-Chinese/`（`Ashare_data.py`、`Inference_datapipe.py`）对应 A 股。

### 2.2 FinGPT-ForexIntelligence（institutional FX 多分析器框架）

`ForexIntelligenceFramework` 编排 8 个分析器 + 2 个系统 + 1 个择时模型（`core.py`）：

| 组件 | 职责 |
|---|---|
| `GlobalMacroAnalyzer` / `CapitalFlowAnalyzer` / `EconomicDataAnalyzer` / `CentralBankAnalyzer` / `GeopoliticalAnalyzer` / `CommodityAnalyzer` / `CurrencyStrengthAnalyzer` / `SessionAnalyzer` | 八个维度打分 |
| `TradeOpportunityGenerator` | 基于货币强弱 + 会话偏置生成机会 |
| `EventAnticipationSystem` / `TradeTimingModel` | 事件预期 + 择时过滤（`timing_assessment["recommend"] != "avoid"` 才保留） |

**已写死的量化参数**（`config.py`，可直接借鉴）：

- **分析权重** `ANALYSIS_WEIGHTS`：global_macro 0.20 / capital_flows 0.15 / economic_data 0.20 / central_bank_policy 0.15 / geopolitics 0.10 / commodity_links 0.08 / currency_strength 0.07 / session_analysis 0.05。
- **风险阈值** `RISK_THRESHOLDS`：risk_on（VIX<15、利差 0.5、股指 +1%）；risk_off（VIX>25、利差 -0.3、股指 -1%）。
- **货币强弱打分** `STRENGTH_SCORES`：very_strong 8.0 → very_weak -8.0（七档）。
- **事件影响** `EVENT_IMPACT`：HIGH 1.0 / MEDIUM 0.6 / LOW 0.3。
- **经济指标分级** `ECONOMIC_INDICATORS`：HIGH（利率决议 1.0、CPI 0.9、GDP 0.85、就业 0.85、PMI 0.8）/ MEDIUM（零售 0.7、贸易 0.7、工业产值 0.65）/ LOW（消费信心 0.5、建筑许可 0.4）。
- **择时规则** `TRADE_TIMING_RULES`：高影响事件前后 30 分钟避免入场；伦敦/纽约重叠期优先；低流动性会话降仓；趋势与会话偏置同向时提高置信。
- **商品货币联动** `COMMODITY_CURRENCY_LINKS`：AUD→铁矿石、CAD→原油、NZD→乳制品、USD→黄金(inverse)、CHF→黄金。
- **交易会话** `TRADING_SESSIONS`：asia(00–08 UTC)/london(07–16)/new_york(13–22)，各带 liquidity/volatility/关键货币对。

### 2.3 FinGPT-MultiAgentsRAG（多智能体 + 幻觉评测）

代码主体是 notebook + 评测脚本：`MultiAgents/inference_mmlu_Llama2*.ipynb`（多智能体 MMLU 推理），评测用 `Evaluation_methods/{HaluEval,TruthfulQA,MMLU}/`（幻觉基准、诚实性、知识）+ `RAG/RAG_part.ipynb`。定位是"多智能体辩论 + RAG 是否能提升金融 LLM 鲁棒性与诚实性"的研究脚手架，非可直接部署的 Agent 系统。

### 2.4 finogrid 的 Agent 层（`finogrid/agents/`，off-hot-path）

5 个**只读、改变不了资金与生产配置**的智能体（每个 `agent.py` docstring 明确 "Has NO ability to move funds"）：`TreasuryStrategy`（用 FinGPT forecaster+corridor 建模 prefunding 情景）、`OpsOversight`、`AuditGovernance`、`ProcessImprovement`、`InternalSupport`。LLM 集成在 `fingpt_integration/`：`minimax_llm_client.py`、`sentiment/{minimax_provider,openai_fallback,crypto_sentiment}.py`、`forecaster/corridor_forecaster.py`、`rag/knowledge_base.py`。MCP server 组：`identity/wallet_factory/kyt_aml/kya_validator/plaid/bridge`。

---

## 3 最终产物结构与格式

- **Forecaster 输出**：自然语言研究报告（正面因素/顾虑 + 下周涨跌分箱预测 + 支撑总结），demo 在 HF Space `FinGPT/FinGPT-Forecaster`。
- **ForexIntelligence 输出**：`ForexAnalysisResult`（`timestamp / global_sentiment / currency_rankings / session_biases / trade_opportunities / upcoming_events / capital_flow_summary / risk_analysis / commodity_impacts`），`generate_report(format="json|text|html")` 输出，`save_analysis(filepath)` 落盘。
- **Benchmark**：`fingpt/FinGPT_Benchmark/benchmarks/` 多任务评测（FinBERT/FinLLM 系列），`train_lora.py` + HF 模型库 `FinGPT/*`。
- **finogrid**：`agent_ledger`/`mandate`/`routing`/`audit`/`execution` 数据模型（`database/models/`），A2A 微交易 + Mandate 访问控制 + KYA。

---

## 4 工程实现要点

### 关键代码路径

| 关注点 | 路径 |
|---|---|
| 预测 prompt 构建 | `fingpt/FinGPT_Forecaster/prompt.py:get_all_prompts()` |
| 情绪聚合 | `fingpt/FinGPT_Forecaster/market_sentiment.py:summarize_market_sentiment()` / `enrich_recent_market_sentiment()` |
| LoRA 训练 | `fingpt/FinGPT_Forecaster/train_lora.py`（+ `train.sh`） |
| 外汇编排 | `fingpt/FinGPT_ForexIntelligence/core.py:ForexIntelligenceFramework.run_daily_analysis()` |
| 量化参数 | `fingpt/FinGPT_ForexIntelligence/config.py`（权重/阈值/规则/会话） |
| 数据源管理 | `fingpt/FinGPT_ForexIntelligence/data_sources.py:ForexDataSourceManager` |
| 多智能体评测 | `fingpt/FinGPT_MultiAgentsRAG/{MultiAgents,Evaluation_methods}/` |
| 情绪数据集 | `fingpt/FinGPT_Sentiment_Analysis_v1/FinGPT_v1.0/{making_dataset,training,inferencing}/` |
| 多源检索 | `fingpt/FinGPT_RAG/multisource_retrieval/{news_scraper,scrapers/}` |
| 财报/基准 | `fingpt/FinGPT_Benchmark/` |
| A2A 支付/Agent | `finogrid/{corridors,agents,mcp,fingpt_integration,database}` |

---

## 5 坑与注意事项（来自代码注释/README，逐条）

1. **数据源配置"名不副实"**：`ForexIntelligence/config.py:DATA_SOURCES` 列了 Bloomberg/Reuters/FXStreet 并给了 reliability_score，但 `data_sources.py:_initialize_sources` 只实装 yfinance 和 tradingeconomics，其余打印 `Source ... not yet implemented`；默认只跑 yfinance。
2. **新闻情绪是占位**：`YahooFinanceSource.get_news_sentiment` 和 `TradingEconomicsSource.get_news_sentiment` 都硬编码 `sentiment: 0`（注释"生产需 NLP"）——ForexIntelligence 的"情绪"维度目前无真实 LLM 计算。
3. **FaFinnhub 新闻要过滤推广**：`get_prompt_by_row` 显式丢弃 `summary.startswith("Looking for stock market analysis and research with proves results?")` 的垃圾摘要。
4. **过去几周做 few-shot 的随机性**：`min_past_weeks=1 / max_past_weeks=3` 随机取子窗口 + 每周新闻 `random.sample` 最多 5 条——训练样本会随 random 变化，复现性弱。
5. **标签分箱的语文映射**：`map_bin_label` 是手写替换（`U3→"up by 2-3%"`、`D5+→"down by more than 5%"`），分箱边界（0-1/1-2/2-3/3-4/4-5/5+）与映射字符串严格耦合，改分箱要同步改这里。
6. **成本/假设类声明需谨慎**：README 的"$300/微调"是相对 BloombergGPT $3M 的对比宣传口径，非可复现基准。
7. **finogrid 智能体"无资金权限"是设计约束**：5 个 agent 全部 read-only、future-looking，docstring 强调 "Has NO ability to move funds or modify production config"——评测这类 infra 项目时别把它们当交易/分析 agent。
8. **MultiAgentsRAG 是研究脚手架**：核心是 notebook + 三套幻觉/诚实性/知识评测脚本，没有可独立运行的"多智能体服务"。
9. **yfinance/te 的数据格式耦合**：`_convert_to_yf_symbol`（`EUR/USD→EURUSD=X`）与 `_convert_to_te_symbol`（去斜杠）是两套映射，新增货币对要留意两处。

---

## 6 可借鉴的设计要点

1. **轻量适配范式的具象化**：LoRA 微调开源底座 + 月/周更新，把"金融 LLM 的时效性"从"重训"变成"快速再适配"——这是 FinGPT 对 BloombergGPT 的核心答辩。
2. **把预测任务做成"分箱 + 自我解释"的指令**：`PROMPT_END` 要求"预测必须从分析末尾推出、不得作为分析的前置因子"，用 prompt 结构组织"理由→结论"的因果顺序，是避免 LLM 先定结论再找理由的务实约束。
3. **多源情绪的"对齐度"指标**：`_source_alignment` 用跨源极差（≤0.15 aligned / ≤0.4 mixed / >0.4 divergent）把"Reddit 与新闻打架"量化——这本身就是信号，比单独一个均值更有信息量。
4. **把机构分析框架做成可配置参数表**：8 维度权重、VIX/利差风险阈值、货币强弱七档、事件影响三档、择时四规则，全部集中在一个 `config.py`，可直接复用到自建的多因子/多智能体外汇研判。
5. **"未实现"显式标注**：数据源 manager 对未实装源打 `logger.warning` 而非静默忽略——诚实面对"配置声明的能力边界"。
6. **评测即研究所产**：HaluEval/TruthfulQA/MMLU 三套幻觉/诚实性/知识评测常备，把 LLM 的鲁棒性验证当成和多智能体推理同等的产出。

## 附：主要源文件索引

| 文件 | 内容 |
|---|---|
| `fingpt/FinGPT_Forecaster/prompt.py` | 预测 prompt 构建（公司简介/新闻/基本面/情绪/预测） |
| `fingpt/FinGPT_Forecaster/market_sentiment.py` | Adanos 四源情绪聚合与格式化 |
| `fingpt/FinGPT_Forecaster/data.py` / `data_pipeline.py` / `data_infererence_fetch.py` | 数据管道与推理取数 |
| `fingpt/FinGPT_Forecaster/train_lora.py` | LoRA 微调训练 |
| `fingpt/FinGPT_Forecaster/RLSP_GUIDE.md` | RAG+LoRA+RL 指南 |
| `fingpt/FinGPT_ForexIntelligence/core.py` | 外汇多分析器编排 |
| `fingpt/FinGPT_ForexIntelligence/config.py` | 权重/阈值/会话/规则参数表 |
| `fingpt/FinGPT_ForexIntelligence/analyzers.py` | 8 个分析器（930 行） |
| `fingpt/FinGPT_ForexIntelligence/data_sources.py` | 数据源管理 + yfinance/te 实现 |
| `fingpt/FinGPT_ForexIntelligence/{trade_generator,timing_model,event_system,reporter}.py` | 机会/择时/事件/报告 |
| `fingpt/FinGPT_MultiAgentsRAG/` | 多智能体 + 幻觉/诚实性/知识评测 |
| `fingpt/FinGPT_Sentiment_Analysis_v1/FinGPT_v1.0/` | 情绪数据集构建/微调/推理 |
| `fingpt/FinGPT_Benchmark/{benchmarks,train_lora,utils}.py` | 金融多任务基准 |
| `fingpt/FinGPT_RAG/multisource_retrieval/` | 多源新闻检索与分类 |
| `finogrid/agents/*.py` | 5 个只读金融智能体 |
| `finogrid/{corridors,mcp,fingpt_integration,database}/` | 支付 rails / MCP / LLM 集成 / 数据模型 |