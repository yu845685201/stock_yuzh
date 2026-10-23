# HunterCode（hunter-community）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：https://github.com/agentpit-io/hunter-community · **分析对象版本**：main 分支（commit `4e36953`，2026-10-10）
> **分析依据**：本地克隆源码（`refrences/agentpit-io__hunter-community`），精读 `apps/{api,paper,fin-worker}`、`skills/`、`tools/`、`docker-compose.yml`、README、doc/产品说明书.md。
> **配套文档**：../开源股票分析项目清单-20261010.md、../开源股票分析项目深度分析-20261011.md

---

## 0. 定位与使用形态

定位"腾讯 WorkBuddy 金融版的开源本地替代"的**自部署 AI 投研助手**（覆盖 A 股/港股/美股），面向私募与专业个人投资者。对话、持仓、投资论点全部存本地数据库，Six-service 容器化（postgres/redis/api/paper/temporal/temporal-ui/fin-worker/web + llm-shim/opencode），浏览器首启向导（环境自检→选模型→填 key 当场测连通/对话/工具调用→数据供给→完成，不重启热生效）。

**SKILL 驱动**：用户自写分析方法论（SKILL.md）驱动大模型工作——查行情、拉新闻、个股深度分析（UZI）、Kronos 时序预测、选股器全市场扫描。Apache-2.0，入围 GOAI 开源大赛赛道二 TOP 15。桌面启动器 HunterLauncher 一键装。

---

## 1. 功能模块全景

| 模块 | 能力 |
|---|---|
| 对话界面（web） | 富卡片（实时价 + 52 周分位 + AI 短评），A 股/港/美 |
| UZI 深度分析 | 22 维度个股报告（基本面/技术面/资金面 + 投委会结论） |
| 大佬评审团 investor_panel | 66 位大佬 9 流派投票打分 |
| 杀猪盘检测 trap_detector | 8 信号扫描 + 风险评级 |
| 龙虎榜分析 lhb_analyzer | 游资席位识别/机构 vs 游资博弈/辨识度龙头 |
| 风险画像 risk_profile | 个人风险偏好画像 |
| 选股器 screener | 全市场扫描 A/港/美 |
| 策略中心 | 研究策略 + 模拟盘（paper）+ fin-worker 定时流水线 |
| 模拟盘 paper | 六条风控规则 + T+1 + 涨跌停 + 费用模型 |
| Kronos 时序预测 | 走势预测（kronos-mcp） |
| 小鹿智能体 | 实验助手（单字段买入候选，不自动应用） |

---

## 2. 数据源来源与采集机制

三选一数据供给模式（README）：

| 模式 | 内容 |
|---|---|
| ① 免费开源源 | A 股走 **AKShare**（行情/财务/K线，`apps/api/agents/data_sources/akshare_*`）；港美股走腾讯/新浪免费通道 |
| ② 自接 MCP/数据商 | 券商、自建 MCP 均可（`scripts/check_source_upstream.py` 校验上游） |
| ③ 平台数据管道 | `hunt_tools_` key（行情、财报、新闻、UZI 深度分析数据与 Kronos 走势预测） |

内置 MCP 工具（`tools/`）：`akshare-mcp`（A 股行情/财务）、`kronos-mcp`（时序预测）、`truesource-mcp`（当前价兜底）。`tools/*/server.json` 注册。

数据源合规与可靠性：`doc/data-source/COMPLIANCE_2026-09-13.md`、`scripts/check_source_upstream.py`（上游校验）、`fin-worker/app/gating.py`（门控）。

---

## 3. 分析逻辑

### 3.1 SKILL 驱动（分析方法论即代码）

`skills/` 目录是分析能力本体，每个 SKILL 声 `needs_tools`（如 `uzi_stock_deep_analysis` / `watchlist_stock_news`）+ 工作流 + 硬约束：

- **UZI 深度分析** `deep_analysis/SKILL.md`（v3.9.4）：采集 22 维度数据 → 应用 UZI 180 条量化规则初筛 → 分析财务造假风险与行业竞争力 → 按模板输出「投资论点/风险点/估值区间」。
- **投资人评审团** `investor_panel/SKILL.md`（v3.9.5）：66 位投资者分 9 流派（经典价值 8 席/质量成长 8/颠覆式创新 8/宏观周期 8/趋势动量 8/游资情绪 8/量化系统化 7/逆向做空 6/尾部风险 5）。**投票规则**（voting-rules.md）：每流派一张打分维度表，逐维 `-2~+2`；分数和 `≥+3 投牛 / ≤-3 投熊 / -2~+2 中性`；**数据缺失维度记 0 分**；过半维度缺数据 → 该流派全记中性；**综合评分 = 50 + (牛票−熊票)/66 × 50**。硬约束：所有数字来自工具返回（不许编）、人物观点标"模拟推演"非本人言论、不给买卖评级/目标价/仓位。
- **杀猪盘检测** `trap_detector/SKILL.md`：8 信号（低质账号推荐/模板话术/VIP群引流/基本面热度脱节/K线异常拉升/老师人设/跨平台联动/虚假消息）→ 🟢安全/🟡注意/🟠警惕/🔴高度可疑。
- **龙虎榜** `lhb_analyzer/SKILL.md`：游资识别（对照 UZI 席位百科章盟主/佛山/拉萨天团）→ 射程判断（股本/题材/封板质量）→ 博弈分析（机构抱团 vs 游资混战）。

### 3.2 价格异动多智能体引擎（`apps/api/agents/graph.py`）

纯 Python asyncio（无 LangGraph/LangChain）。价格预警触发时：

```
Phase1 并行: MarketAnalyst（技术面）+ SentinelNewsAgent（新闻情报/事实核验/止损条件判定）
   ↓ 条件路由
kill_condition 命中? 
  YES → ExitStrategyAdvisor → RiskJudge → FinalDecision
  NO  → BullResearcher ⇄ BearResearcher（_DEBATE_ROUNDS=2 轮）→ ComprehensiveJudge → RiskJudge → FinalDecision
```

SentinelNewsAgent 输出 `sentinel_opinion/confidence/verified_facts/filtered_facts/kill_condition_triggered/debate_mode`——新闻侧先做事实核验与止损条件判定，命中止损直接走退出建议，不进入辩论。`_normalize_ticker`：6 开头→.SH、0/3→.SZ、4/8/9→.BJ。

### 3.3 模拟盘风控引擎（`apps/paper/app/risk/`）——确定性核心

六条纯函数串成一次委托预检，`engine.py` **按顺序跑完、收集全部不过的原因**（不是第一条不过就返回），`decline_reason` 拼接全部。AI 的单和人下的单走同一套（无 `source` 分支）。

| # | 规则 | 关键口径 |
|---|---|---|
| 1 | 停牌 halt | 停牌即拒单（先读，避免其他理由混淆） |
| 2 | 交易时段 session | 按 `fin_market_calendar` + timezone + sessions |
| 3 | 整手 lot | `lot_rule`（固定/比例）解析手数 |
| 4 | T+N t1 | `sellable_rule`(t_plus_n) + `sellable_days` |
| 5 | 涨跌停 price_limit | 三模式 pct/none/band（见下） |
| 6 | 费用 fee + 资金 funds | 费用模型版本化 + 可用资金 |

- **price_limit 三模式**：`pct`(CN_A 涨跌停)、`none`(HK/US 无涨跌幅限制，`checked=false` 必须留痕"未做校验"不静默跳过)、`band`(VCM/LULD 预留，**见到就拒绝**不做半成品)。涨跌停价 = `前收盘×(1±幅度)` **ROUND_HALF_UP 到分**（注释："不能用 Python 默认银行家舍入，否则 10.125→10.12"）；`limit_pct` 为 NULL → 拒绝不拿 0 顶替（"会被读成涨跌停 0%，是编数字"）。
- **fee 费用模型版本化**：`stamp_side`（印税方向）由表定——CN_A `sell`、HK `both`、US `none`；`transfer_fee_side` 同理（美股 SEC/FINRA 费仅卖出）。三个费用项 = 金额×费率，金额全程 Decimal、ROUND_HALF_UP 到 4 位小数；`fee_model_version` 随成交落库"报告里要能说清这笔用哪一版费率"。缺字段/非法方向 → 拒绝。
- **按市场取参数**：六条规则参数从 `fin_market_rule` 一行来，**取不到市场规则整笔拒绝**，"拿 A 股规则顶替港美股就是编数据"。

### 3.4 fin-worker 策略与研究（Temporal 流水线）

`fin-worker/app/strategy/stock_rules.py`：`tq_daily_v2` 白名单策略，8 个策略族：

| 策略族 | stop/trail/持仓数/weight/exposure/risk |
|---|---|
| trend_follow 趋势跟随 | stop 2.0 / trail 3.0 / 4 仓 / 0.20 / 0.80 / 0.0075 |
| mean_revert 均值回归 | stop 1.5 / days 10 / 4 仓 / 0.15 / 0.60 / 0.005 |
| leader_break 龙头突破 | stop 2.0 / trail 3.0 / 3 仓 / 0.20 / 0.60 |
| quant_rotate 量化轮动 | stop 2.5 / 4 仓 / 0.15 / 0.60 |
| contrarian 逆向修复 | stop 1.5 / days 7 / 3 仓 / 0.10 / 0.30 |
| value_hold/div_lowvol/event_driven | **blocked**（缺时点化财务/分红/事件数据） |

`PARAMETERS` 冻结全部指标常量 + `FINGERPRINT = sha256(VERSION+SPECS+PARAMETERS+源码)`，策略键 `tq_daily_v2_{family}`，修改行为必须发布新策略键。`validate_bars` 前置校验（日期唯一严格递增、量价非负、高低价合法）。shadow 账户可做动态策略验证（`shadow.py`）。

### 3.5 程序-AI 边界

- **纸面交易**：AI 的单走人单同一套风控，风控引擎纯函数，AI 不该也不能替风控。
- **大佬投票**：投票规则可机械推导（"不是拍脑袋编比例"），LLM 只做"先引真实数字再下判断"的角色推演。
- **小额实验助手**：AI 只选后端预设的单字段买入候选，"不能自动应用"（`AGENTS.md`），失败记录全部保留。

---

## 4. 最终产物结构与格式

| 产物 | 结构 | 落盘/通道 |
|---|---|---|
| 对话界面 | 富卡片（实时价 + 52 周分位 + AI 短评） | postgres + 本地 |
| UZI 深度报告 | 投资论点/风险点/估值区间（22 维度） | 报告页 + 历史 |
| 大佬评审团 | 一句话结论/投票分布/9 流派详述/最大分歧/跟踪清单 | 报告页 |
| 模拟盘 | 订单 + 委托预检结果（decline_reason/fee breakdown） | paper 应用 + Temporal |
| 价格异动决策 | FinalDecisionResult（decision/confidence/key_reason） | 对话/推送 |
| Kronos 预测 | 走势预测时间序列 | kronos-mcp |

---

## 5. 坑与注意事项（来自代码注释/AGENTS/README）

1. **"灰色按钮会骗人"**：`band` 价格限制模式本期不实现，见到就拒绝、不做半成品（`price_limit.py` 总控规则 §六.10）。
2. **银行家舍入坑**：涨跌停价用 `ROUND_HALF_UP` 到分，不能用 Python 默认银行家舍入（"否则 10.125→10.12"）。
3. **NULL ≠ 0**：`limit_pct` 为 NULL → 拒绝而不是拿 0 顶替（"是编数字"）；`mode=none` 的市场根本不该去读 instrument/prev_close。
4. **拿 A 股规则顶替港美股 = 编数据**：取不到市场规则整笔拒绝；费用印花税方向按表（CN_A 卖出/HK 双边/US 无）。
5. **费用模型版本随成交落库**：报告要能说清"这笔用哪一版费率"，费率/方向由表不给写死（`stamp_side`/`transfer_fee_side`）。
6. **AI 单和人单同一套风控**：`engine.py` 无 `source` 分支，`source` 只落库供归因。
7. **大佬投票"不能拍脑袋编比例"**：维度 -2~+2、缺数据记 0、过半缺维度流派全中性——投票必须可机械推导。
8. **人物观点是模拟推演**：写"模拟巴菲特的方法论推演"不写"巴菲特说"，并明示非本人言论。
9. **过拟合监测**：`docs/agent-overfit-monitor.md`——"缺失检查不计分、不把分数称赚钱概率/过拟合概率"，个人试验归档含失败结果。
10. **云端模板未上架**：Zeabur/Sealos/Railway 等只给文档没放按钮（"我们没这些平台账号，一次真实部署都没做过"），README 明示"哪些验过、哪些没验过"。

---

## 6. 可借鉴的设计要点

1. **六条风控纯函数串行收集全部理由**：一次委托预检查出"整手不对 + 资金不够"全部原因（`decline_reason` 拼接），改一处报一处 vs 一次全给——委托拒绝体验的细节。
2. **按市场取参数 + 找不到就拒绝**：风控规则不变、参数从 `fin_market_rule` 表来，杜绝"拿 A 股口径顶替港美股"的静默错误。
3. **费用模型版本化 + 方向列**：印花税/过户费方向（buy/sell/both/none）由表决定、版本随成交落库，交易成本可追溯、可复算。
4. **SKILL 即方法论**：分析方法论外置为 SKILL.md（22 维度/大佬投票/杀猪盘/龙虎榜），LLM 只做角色推演，投票/评级的确定性规则留在提示词里可机械复核。
5. **策略族冻结 + 指纹**：`tq_daily_v2` 策略参数与源码 sha256 指纹绑定，改行为必须发新策略键——研究可复现、不可静默漂移。
6. **价格异动多智能体 + 止损短路**：SentinelNewsAgent 先行核验事实与止损条件，命中止损直接退出建议（不进辩论），多空辩论 2 轮纯 asyncio 无重依赖。

---

## 7. 主要源文件索引

| 文件 | 内容 |
|---|---|
| `apps/api/agents/graph.py` | 价格异动多智能体图（并行→路由→辩论→风控裁判） |
| `apps/api/agents/`（bull/bear/risk_judge/comprehensive_judge/market_analyst/exit_strategy_advisor/sentinel/） | 多智能体各角色 |
| `apps/api/agents/data_sources/akshare_*.py` | A 股数据源（行情/财务/K线） |
| `skills/deep_analysis/SKILL.md` | UZI 22 维度深度分析 |
| `skills/investor_panel/SKILL.md` + `references/voting-rules.md` | 66 大佬 9 流派投票规则 |
| `skills/trap_detector/SKILL.md` | 杀猪盘 8 信号 |
| `skills/lhb_analyzer/SKILL.md` | 龙虎榜游资分析 |
| `apps/paper/app/risk/engine.py` | 风控引擎（六条串行收集全部理由） |
| `apps/paper/app/risk/{price_limit,fee,t1,lot,session,halt,funds}.py` | 涨跌停三模式/费用版本化/T+1/整手/时段/停牌/资金 |
| `apps/paper/app/ledger.py` / `recon.py` | 台账 + 对账（as-of） |
| `apps/fin-worker/app/strategy/stock_rules.py` | tq_daily_v2 策略族 + 冻结参数 + 指纹 |
| `apps/fin-worker/app/{workflows,activities,schedules,shadow,memory_gate}.py` | Temporal 流水线（collect/review/observe/propose）+ 影子账户 |
| `tools/{akshare-mcp,kronos-mcp,truesource-mcp}/` | 内置 MCP 数据/预测工具 |
| `docker-compose.yml` | 六服务编排（postgres/redis/api/paper/temporal/fin-worker/web） |
| `doc/data-source/COMPLIANCE_2026-09-13.md` | 数据源合规说明 |