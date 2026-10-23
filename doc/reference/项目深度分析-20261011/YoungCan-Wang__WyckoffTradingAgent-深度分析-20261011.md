# WyckoffTradingAgent 深度分析

> 分析日期：2026-10-11 · 仓库 URL：https://github.com/YoungCan-Wang/WyckoffTradingAgent · 分析依据：本地克隆源码（refrences/YoungCan-Wang__WyckoffTradingAgent，v0.9.x）· 配套：../开源股票分析项目清单-20261010.md

## 0 定位与架构总览

基于 **威科夫（Wyckoff）方法论** 的多市场（A股/港股/美股/ETF）量化选股 + AI 研报 + OMS 持仓决断系统。与 TradingAgents 系"多智能体辩论"范式不同，它是一套**"程序算数、AI 复核、风控引擎执行"**的确定性管线：Python CLI/后台任务 + React 前端 + MCP + GitHub Actions 定时调度，数据与计算分离（快照离线回放）。核心设计红线贯穿全部代码——**AI 是参谋，不是决策者**；陷阱状态（pending/confirmed/起跳板）与可执行决策（BUY）严格分层。

五步流水线（README_STRATEGY + `workflows/`）：

```
Step1 数据工程（每日全市场快照 => data/funnel_snapshots，320 交易日窗口）
  ↓
Step2 漏斗选股（wyckoff_funnel.py：大盘水温总闸 → L1 流动性 → L2 八通道强度
                  → 主线引擎(并行) → L3 板块共振 → L4 威科夫形态触发 → L5 退出 → 损失护栏）
  ↓
Step3 AI 研报（step3_batch_report.py：特征切片 → RAG 防雷 → 三阵营审判）
  ↓
Step4 OMS 持仓决断（step4_rebalancer.py：五动作工单，风控引擎最终刹车）
  ↓
盘后信号反馈闭环（signal_feedback：观察样本 → outcomes 回填 → 下一轮 health/registry）
```

## 1 数据源来源（逐接口）

统一门面 `integrations/data_source.py::fetch_stock_hist()`，五源**顺序降级链**（`_stock_fetchers()`）：

| 顺序 | 数据源 | 提供内容 | 鉴权/开关 |
|---|---|---|---|
| 1 | **TickFlow**（`data_source_tickflow.py`，批量日线） | 全市场日线（A股 + 港美股批量，320 交易日窗口） | 付费 `TICKFLOW_API_KEY`；限流时记录事件+提示并降级 |
| 2 | **Tushare**（`data_source_tushare.py`/`tushare_client.py`） | A 股日线兜底 + `hk_basic` 港股名单 | token 权限分级 |
| 3 | **akshare**（`data_source_akshare.py`） | 免费兜底 | `DATA_SOURCE_DISABLE_AKSHARE` 可关 |
| 4 | **baostock**（`data_source_baostock.py`） | 免费兜底，带**熔断器**（`baostock_circuit_state` 失败熔断） | `DATA_SOURCE_DISABLE_BAOSTOCK` |
| 5 | **efinance**（`data_source_efinance.py`） | 最后兜底 | `DATA_SOURCE_DISABLE_EFINANCE` |

其它数据面：指数行情 `integrations/index_data_source.py`（akshare）、全市场实时快照 `integrations/spot_snapshot.py`、主题概念热度 `integrations/ths_hot_concept.py`（同花顺）+ `supabase_concept_heat.py`、盘前风险输入 A50 期指/VIX（`workflows/premarket_risk_inputs.py`）、持久化层 `integrations/supabase_*.py`（推荐/信号反馈/持仓/影子账本/主题雷达等 20+ 表）、本地库 `integrations/local_db.py`。

五源全败时抛 `RuntimeError` 并给**分诊提示**（`_network_hint_from_details`）：DNS/SSL/断连/只读 site-packages 四类原因分别给中文排查建议，且按"是否配了 tickflow/tushare"给升级指引。

## 2 分析逻辑与 Agent 机制

### 2.1 市场水温总闸（tools/market_regime.py）

先选环境再选股。`analyze_benchmark_and_tune_cfg()` 用指数相对 MA50/MA200 与 MA50 斜率判定 `_structural_regime`（BULL/TRANSITION/BEAR），叠加市场广度 `calc_market_breadth` 与 PV（price-volume）outlook 调档。状态机：`NEUTRAL`（生产禁新开）、`RISK_ON`（过热禁追新）、`CRASH`→`PANIC_REPAIR`（三日状态机）→`PANIC_REPAIR_CONFIRMED`（允许最多一只 5% PROBE）/`PANIC_REPAIR_INTRADAY`、`BEAR_REBOUND`。水温直接把 `tune_cfg_for_regime` 写回 FunnelConfig 动态调漏斗参数。

### 2.2 六层漏斗（core/wyckoff_engine.py，约 2600 行）

- **L1 流动性**（`layer1_filter`）：剔除 ST/非 A 股板块；收盘价 ≥2 元；市值 ≥25 亿（日均成交额 ≥8000 万可放行）；近 20 日均成交额 ≥4000 万。L1 不识别停牌/次新——停牌靠"取不到 K 线"被动排除，次新靠 L2 的 MA200 门槛间接排除。
- **L2 八通道**（`core/layer2_strength.py`，函数级：`pre_ignition_ok`/`ambush_channel_ok`/`accumulation_channel_ok`/`dry_volume_channel_ok`/`rs_divergence_channel_ok`/`breakout_accel_channel_ok`/`trend_continuation_channel_ok`/`_momentum_channel_ok` + `_sos_channel_ok`）：主升/点火/潜伏/吸筹/地量/护盘/趋势延续/加速突破，各通道独立判定再归并。`diagnose_layer2_symbol_failure` 输出"最接近通道(缺口X%)"的失败缺口排序。
- **主线引擎**（core/mainline_engine.py，并联在 L1 后）：主题归一化（英文缩写独立词边界，避免 `MicroLED` 误命中 `CRO`）、`rotation_watch` 轮动观察（5/10/20 日相对动量）；主线票可绕过 L2 但**必须过 timing_score**，不能绕过买点确认。
- **L3 板块共振**（`layer3_sector_resonance`）：Top5 纯行业单标签聚类（`top_n_sectors=5, use_concept_map=False`），候选 <3 只降级全量通过；实测全市场 5510 只 6 年 22.7 万笔，扣费净均值 +0.235%（t=15.57），较旧 11 参数多标签（-0.027%）提升 +0.262pp。
- **L4 威科夫形态**（`layer4_triggers` → `_detect_spring`/`_detect_lps`/`_detect_evr`/`_detect_sos`/`_detect_compression`/`_detect_trend_pullback`）：
  - 生产**启用**三强：Spring（`_spring_support_level` 60 日 swing low 中位支撑；EVR 量比 ≥1.3 且换手 ≥1%、跌幅 ≤-2%；Trend Pullback 5%-20% 回撤缩量）。三强全市场扣 0.202% 摩擦后净 +0.745%（t=13.64）。
  - 生产**关闭**三形态（全市场 6 年回测负超额）：SOS -0.061%、LPS -0.159%（Fib 版 -1.319%）、Compression -0.170%。
  - 板块波动缩放 `_board_volatility_scale`：冰价类阈值按创业板 ×1.2、科创板 ×1.4、北交所 ×1.2 缩放；量比类不缩放（各板块当日量/前 4 日均量 p90 都在 1.47±0.02）。
- **L5 退出**：`upthrust_warning`（高位放量越过 swing high 收回留长上影 = UTAD 假突破），高位缩量。
- **损失护栏**（core/candidate_policy.py）：单 EVR/LPS/Trend Pullback 默认只观察；纯 SOS 需 Springboard ABC **3/3** 全满足；送审最多 8 只、单行业 ≤2 只；RISK_ON/BEAR_REBOUND 追涨拦截。

### 2.3 Step3 AI 三阵营审判（workflows/step3_*.py）

输入**不是原始 K 线**而是压缩特征（特征切片：均线位阶/乖离、近 15 日量价切片、60 日高光事件、程序定的 `candidate_theme/phase/role`）。系统提示词 `core/prompts.py::WYCKOFF_FUNNEL_SYSTEM_PROMPT`——"你是理查德·威科夫，Alpha 投委会首席投资官"，要求对每只候选分类三阵营：

1. 💀 逻辑破产（Invalidated）—— 派发/诱多/破位；
2. ⏳ 储备营地（Building Cause）—— 结构在但确认不足；
3. 🏹 处于起跳板（On the Springboard）—— 需求主导，具 OMS 复核资格，**不等于买入**。

每个起跳板必须写"如果我判断错了，最可能错在哪"。默认 `STEP4_AI_CANDIDATE_POLICY=veto_only`：AI 只能用"逻辑破产"一票否决，不能把规则未准入候选升级。AI 不得自创数字、不得算金额/仓位/股数。**RAG 防雷**（`step3_rag.py`/`integrations/rag_veto.py`）：进模型前用外部新闻检索负面信息（立案/造假/退市风险），命中一票否决。Step3 结果带 snapshot cache（`report_snapshot_cache.py`，按 data hash 命中复用，避免同数据重复烧 LLM）。

### 2.4 Step4 OMS（workflows/step4_*.py）

风控引擎是最终刹车。五动作工单：`EXIT / TRIM / HOLD / PROBE / ATTACK`，优先级 `EXIT > TRIM > HOLD > PROBE/ATTACK`。关键规则（README_STRATEGY §4）：

- **准入**：候选必须跨日 `confirmed`（状态机 `survived` 守结构 → `VALIDATED` 需求确认，兼容存为 `confirmed`；跌破则 `expired`），且无 `new_buy_allowed=false`/`blocked_*`。
- **唯一买区**：AI 给结构入场区间，OMS 取该区间上界与涨幅防追高/ATR 防追高上限的**最严值**，生成报告里唯一"明日允许买入区间"；次日开盘价高于上界不追、低于下界不抄底。
- **T+1 冻结**：A 股当日买入持股按代码聚合，买日=今日则整仓冻结，EXIT/TRIM 一律拒单标 `T+1限制`；港美股 T+0 不套用。
- **止损继承**：新开仓生产 5%（代码默认 12% 不代表线上）；已存止损只能收紧；持仓诊断固定 -12% 兜底 + ATR 上限内放宽；+18% 止盈目标。
- **降级保护**：新仓保护期内若模型给"同时高于成本和现价"的倒挂脏止损，其单独触发的 EXIT/TRIM 降级 HOLD；LLM 不可用时仅追加保护性 HOLD/EXIT。
- **AI 权限封顶**：AI 输出 ATTACK 被程序降为 PROBE，不能扩大规则授予的风险权限。

### 2.5 盘前风控与信号反馈闭环

盘前风控（08:20）监测 A50期指/VIX，四档 `NORMAL/CAUTION/RISK_OFF/BLACK_SWAN`，关键输入缺失且未硬拦截时进 `UNKNOWN` 闸门（fail-closed）。信号反馈闭环（`signal_feedback` + `shadow_ledger`）：`off/shadow/on` 三态影子账本，先写观察样本、盘后算 outcomes、下一轮才读新 health，Shadow 不改变真实推荐。

### 2.6 LLM 层（integrations/llm_client.py）

`call_llm()` 统一层：Gemini 原生（主，`google.genai`）+ OpenAI 兼容 + LiteLLM 适配 + DeepSeek 官方（`core/deepseek.py`，V4 Flash/Pro 1M 上下文、thinking off/low/high/max、Responses 联网）。`provider_route_chain` 支持 primary+fallbacks 多 provider 降级链；`temperature=0.4`；截断识别（Gemini finish_reason / OpenAI `finish_reason==length`）→ 输出预算翻倍重试一次（上限 32768 token）；DeepSeek 后台任务 `DEEPSEEK_BACKGROUND_MIN_OUTPUT_TOKENS` 保底。

## 3 最终产物结构与格式

- **每日报告**（飞书/企微/钉钉推送）：报告顶部固定 `core/execution_playbook.py` 的「🧭执行纪律」区块；盘中"一眼结论"+"主线与轮动"+候选+市场证据（发现行情与交易许可分开展示）。
- **三维参数状态**：`DETECTED`（当日结构命中）/`SURVIVED`（跨日未失效但需求证据不足）/`VALIDATED`（守位+需求确认）/`OMS_APPROVED`（可执行）。
- **研报**：执行摘要、实际送审清单、三阵营分类、A/B/C≥2 形态观察、合规版市场简报（`STEP3_SEND_COMPLIANCE_BRIEF` 可关）。
- **回测报告**（`core/backtest_*` + `workflows/backtest_*`）：扣费净收益、MFE/MAE、边际成交（signal_date+code 配对 common/reference-only/variant-only）、单时期信号台账复用（10 次→1 次重放）。
- **Web/CLI/MCP**：React 前端（CF Pages，no-new-pages 只走 Agent 聊天）+ `cli/` + `mcp_server.py` + Electron 桌面（desktop/，图表标注仅本地）。

## 4 工程实现要点（关键代码路径）

| 关注点 | 实现 |
|---|---|
| 数据/计算分离 | `integrations/funnel_snapshot.py` + `data/funnel_snapshots`：全量拉取一次序列化，回测调参离线不重复拉 |
| 存储 | Supabase 20+ 表（`supabase_*.py`），推荐/信号/持仓/影子账本分离，`sync.py` 统一入库 |
| T+1/复权口径 | `core/_price_math.py`、`core/trade_friction.py::round_trip_cost_pct()`（约 0.202% 往返摩擦）、`core/trade_fill.py` 成交填充 |
| 回测引擎 | `backtest_runner.py`/`backtest_replay.py`：同一信号台账复用到全部持有期/止损/止盈网格；walk-forward；`backtest_grid_ranking.py` 与 `ranker_weight_eval.py`/`trigger_weight_eval.py` 权重评估 |
| 研究治理 | `core/strategy_policy_governor.py`/`dynamic_policy.py`：`off`→`shadow`→`on` 的动态策略晋级需过跨周期回测+人工复核 gate |
| 函数质量门禁 | `AGENTS.md` + `scripts/quality_gate.py`：函数硬上限（core/agents/tools/workflows ≤70 行、scripts ≤100 行），`.metrics/func_whitelist.json` 记债 |

## 5 坑与注意事项（来自代码注释/AGENTS.md/CHANGELOG）

1. **cron 间隔 ≠ 执行顺序**（AGENTS.md Rule 7）：GitHub `schedule` 派发实测比 cron 晚 3~4.5 小时且顺序不可控，2026-09-12 前两个 workflow 错开 40 分钟靠 `actions/cache` 跨流程接面板，实跑失败在"缺 `trigger_panel.csv`"。教训：B 依赖 A 的产物就放同一 workflow 用 `needs`。
2. **同一天三个错误策略结论（2026-08-20）**：手搓代理逻辑得出错误结论——地量通道差点被删（用"20 日均量/60 日"代理生产实际是 250 日分位）、Spring 被误判 MA 受损（只 11 只/天样本）、SOS 报 +0.43pp 超额但一致 universe 重跑是 −0.80pp。每个错误的指向都"有利于行动"。由此立了 9 条证据规则。
3. **策略证据规则**（AGENTS.md）：必须扣成本（round_trip ~0.202%）报 net、dropna 是 survivorship 陷阱、beta≠alpha、单市场段≠证据、多变更需合并跑（2026-08-21 两个独立"有效"变更叠加后反而 7.45pp 更差，因为都打同一失败且空位给了更弱候选）、issue 结论必须挂生产 run 链接、改候选集的变体必须看边际成交（#476：+0.52pp 均值差，但 171 只新增票平均 -2.07% vs 227 只移除票 -1.17%）。
4. **负超额形态下线**：SOS/LPS/Compression 全市场 6 年回测负超额，默认关闭；`CRASH_LEFT_PROBE`（抗跌观察车道）T+5 均收 -1.76%、胜率 31%，系统性负期望已下线——生产中回测证据能"杀死"信号。
5. **Tushare 数据坑**：港股名单是 Tushare `hk_basic`、美股是 Nasdaq Trader 正股目录（剔 ETF/权证/优先股）；名单过短拒绝覆盖。
6. **MINI racer/V8 多线程 crash**（TradingAgents 同款教训）。
7. **EFinance 只读缓存**：部署环境 site-packages 只读时 efinance 本地缓存写失败 → 分诊提示里单独识别 permission denied。
8. **DeepSeek 官方端点迁移**：旧 `deepseek-chat`/`deepseek-reasoner` 在官方端点按原思考语义迁移，代理端点保持普通兼容协议——两条路不能混。
9. **VLM/读盘室密钥**：生产 Worker 强制独立工具审批签名密钥，密钥缺失/强度不足在部署即 fail，不等到用户对话才暴露。

## 6 可借鉴的设计要点

1. **"研究状态 vs 可执行决策"硬分层**：pending/confirmed/起跳板/VALIDATED 是研究语义，BUY 必须过市场闸门 + OMS 区间 + T+1；report 里反复用 HTML 注释强调。杜绝"AI 说能买就买"。
2. **用全市场回测"杀死"信号**：SOS/LPS/Compression 因负超额默认关闭、CRASH_LEFT_PROBE 因负期望下线——证据驱动的信号生产线，而不是堆指标。
3. **net-of-cost 与边际成交审计**：任何结论必须扣 0.202% 摩擦报净收益、改候选集的变体必须逐笔配对看"换进来 vs 换出去"的质量——反 self-deception 的量化纪律（比 TradingAgents-astock 的 performance 更进一步）。
4. **五源降级 + 失败分诊**：数据源失败不只 abort，而是逐源记 detail 并给 DNS/SSL/断连/权限四类中文排查提示，运维友好。
5. **LLM 截断预算重试**：识别 Gemini/OpenAI 的"输出截断"并非成功，预算只放大一次（×2 到 32768 cap），避免复杂 prompt 靠无上限抬高预算掩盖拆分问题。
6. **AI 结构化三阵营 + RAG 一票否决**：把 AI 角色收窄为"审判员"（三类定性 + 必写'错在哪'），负面信息检索做硬 veto，AI 权限被程序封顶（ATTACK→PROBE）。

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `core/wyckoff_engine.py` | L1/L3/L4/L5 核心引擎 + 威科夫形态检测（~2600 行） |
| `core/layer2_strength.py` | 八通道强度 + 逐股失败缺口诊断 |
| `core/mainline_engine.py` | 主线引擎（主题归一化、rotation_watch、timing gate） |
| `core/candidate_policy.py` | 正式候选损失护栏 |
| `tools/market_regime.py` | 大盘水温总闸（结构/趋势/广度/恐慌修复状态机） |
| `core/prompts.py` | `WYCKOFF_FUNNEL_SYSTEM_PROMPT`（三阵营审判 + Alpha 投委会） |
| `workflows/wyckoff_funnel.py`、`daily_job*.py` | Step2 漏斗 + 定时任务编排 |
| `workflows/step3_batch_report.py`、`step3_llm.py`、`step3_rag.py` | 三阵营审判、LLM 调用、RAG 防雷 |
| `workflows/step4_rebalancer.py`、`step4_order_engine.py` | OMS 五动作工单 |
| `integrations/data_source.py` + `data_source_{tickflow,tushare,akshare,baostock,efinance}.py` | 五源降级链 |
| `integrations/llm_client.py`、`llm_adapter.py`、`core/deepseek.py` | 统一 LLM 层 |
| `integrations/supabase_*.py`、`sync.py` | Supabase 持久化 |
| `core/trade_friction.py`、`trade_fill.py` | 摩擦成本/成交填充 |
| `workflows/backtest_*.py`、`core/backtest_*.py` | 回测体系（信号台账复用/walk-forward/权重评估） |
| `core/signal_feedback.py`、`shadow_ledger*.py`、`dynamic_policy.py` | 信号反馈闭环 + 影子账本 + 动态策略晋级 |
| `AGENTS.md` | 开发/策略证据规则（9 条 + cron 陷阱） |
| `README_STRATEGY.md`、`GLOSSARY.md` | 策略手册（全部阈值口径）、术语表 |