# easy-stock 深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：https://github.com/jundizhou/easy-stock · **分析对象版本**：v1.5.2（commit `dc7f64d`，2026-10-10）
> **分析依据**：本地克隆源码（`refrences/jundizhou__easy-stock`），精读 `backend/internal/`（stockanalysis/portfolioinspection/portfoliooptimization/review/sector/strategy/inflection/marketemotion/narrative/providers）、`backend/docs/`（architecture/data-sources/model-reasoning）与 README，结论均来自源码注释与实现。
> **配套文档**：../开源股票分析项目清单-20261010.md、../开源股票分析项目深度分析-20261011.md

---

## 0. 定位与使用形态

A 股 AI 智能投研**桌面工作台**（Electron 宿主 + Go 后端 + React/TypeScript 前端），Local First（研究记录、登录态、模型会话全部本机保存），非商业许可证（Non-Commercial）。口号"让每一次判断都有证据"——覆盖盘中观察、盘后复盘、持仓巡检与 AI 持仓优化的完整闭环。

统一 Agent 服务（`agent.Service`）可选 **Hermes**（Chat Completions/Responses/Anthropic Messages）或 **Codex**（原生 Codex App Server，stdio JSON-RPC）双运行时，任务开始时绑定运行时/模型/配置快照。多个 SQLite 库分工：`reviews.db`（复盘）、`stock-research.db`（研究）、`portfolio-inspections.db`（巡检）、`market-emotion.db`（情绪）、`theme-radar.db`（题材）。

---

## 1. 功能模块全景

| 模块 | 能力 |
|---|---|
| 大 V 自动复盘 | 雪球/淘股吧/公众号文章采集 → 观点共识图（共识/分歧/盘面事实/次日验证条件）→ 定时 22:00 + 飞书/钉钉推送 |
| 超短连板分析 | 涨停池/连板梯队/晋级率/情绪历史同视图，历史快照识别情绪周期 |
| 趋势题材雷达 | 题材排名/涨跌强度/资金流/上涨宽度/持续天数；题材地图拆解产业链；题材指数日周月 K |
| 行情总览 | 指数/资金/榜单/龙虎榜/公告雷达/机构观点统一入口（可 AI 解读当前上下文） |
| 个股分级研究 | 量化速览/AI 快速研判/AI 标准研判/AI 深度研究四档 |
| 持仓巡检 | 个股报告复用 + 组合事实计算 + AI 四维评分（程序复算）+ 引用校验 |
| AI 持仓优化 | 候选筛选→个股研究→投资比较→程序配仓→独立复评（原/建议组合对照） |
| 游资心法库 | 40+ 篇内置原文 + 远端缓存 + 上下文检索 |
| 拐点评估 | 可解释锚点 + 环境/切换信号评估（另含离线回测、ClickHouse 读取辅助） |

---

## 2. 数据源来源与采集机制

### 2.1 逐接口明细

| 源 | 接口 | 内容 | 说明 |
|---|---|---|---|
| **短线侠/开盘啦** | `duanxianxia.com`（`DS` 服务） | 题材排名、龙一至龙五、涨停/连板池、逐股炒作题材、首封/末封时间、开板次数、板型 | **涨停池/题材归属首选源**（`providers/duanxianxia/`） |
| **东方财富** | `push2.eastmoney.com`、`push2his`、`datacenter-web` | 板块列表/行情/资金流/成分、个股 K 线、龙虎榜、两融 | 补充开盘啦缺字段（价格/换手/行业），涨停池兜底 |
| **新浪** | `hq.sinajs.cn`、`vip.stock.finance.sina.com.cn` | 实时行情、财务三表、资金流 | 板块成分兜底、财务 |
| **财联社** | `www.cls.cn` | 电报快讯 | 资讯 |
| **腾讯** | `qt.gtimg.cn` 等 | 港/美/指数行情、行业、估值 | 港美股（planned 部分已实现） |
| **期货持仓** | 交易所/第三方 | 股指期货多空持仓 | `providers/futuresposition/` |
| **雪球/淘股吧** | Electron Browser Bridge（持久登录态页面） | 大 V 文章原文 | `xueqiu-browser-bridge.cjs` / `taoguba-browser-bridge.cjs` |
| **微信公众号** | 本机 Python/FastAPI 链接解析服务 | 公众号文章 | `wechat-service.cjs`，明确处理"历史文章列表不可用" |

### 2.2 数据源融合与可靠性规则（`docs/data-sources.md`）

- **涨停池优先级**：开盘啦股票池优先（当日涨停池/连板数/首末封/开板/板型/题材）；开盘啦未更新到当日时用东财兜底，接口元数据标记兜底原因；个股题材直接用开盘啦标签，仅缺标签时用东财概念目录。
- **题材雷达优先级**：开盘啦当天快照优先；第三方刷新 **全局至少间隔 5 分钟**（限制时间与最近成功快照持久化到 `theme-radar.db`，重启不重置）；行业趋势强度与开盘啦题材**各自标准化后融合**成"当日/5 日融合强度"；开盘啦沿用历史快照时按交易日衰减，超 2 个交易日只展示行业趋势强度。
- **开盘啦题材 → 东财题材对照表**：本地维护近 20 交易日 60 个题材的板块/关键词对照，只维护题材级映射、不维护个股一一映射（个股始终从映射后的东财题材实时成分获取）。
- **可靠性规则**：Provider 必须带 mock 单测、联网源必须带 opt-in live test；`meta.source` 供上层区分来源而非假设固定上游。

### 2.3 采集层的坑（源码注释/README）

- **开盘啦题材榜是编辑归因**，实时行情/K/领导力指标仍由行情源计算（题材雷达 doc 原文）。
- **题材指数**区分"数据源指数"与"等权参考指数"，页面明确标注避免把参考曲线误当官方指数（README）。
- **微信历史文章列表不可用**：代码显式处理"公众号历史文章列表不可用"状态。
- **登录态按来源与配置隔离**：没有桌面桥接时回退到 Agent + agent-browser 复用导出登录态。

---

## 3. 分析逻辑

### 3.1 个股分级研究（四档 + 证据压缩 + 三层校验）

`stockanalysis/research_levels.go` 定义四档策略（`researchLevelPolicyFor`）：

| 档 | 日线数/相对强度 | 公告条数(字) | 证据上限 | 补证/提纲/修复 | 阶段超时 |
|---|---|---|---|---|---|
| 量化速览 | 300 根/20 | — | 16 limitation/8 锚点/8 基线维 | no（不调模型） | — |
| 快速研判 | 60 根/4 | 4 条×180 字 | 6 cards/4KB | no | 3min |
| 标准研判 | 100 根/6 | 8 条×700 字 | 12 cards/12KB | no | 3min |
| 深度研究 | 300 根/20 | 12 条×1500 字 | 16 cards/24KB | **outline+supplement+repair** | 8min |

深度研究流程（`research.go`）：模型**关闭工具**生成问题与替代解释 → `supplementStockResearch` 把问题映射到"允许的公告/研报/新闻/已有来源正文/本地方法论查询"做**只读补证** → 生成核心判断 + 交易条件 → **三层校验**（来源编号、引文、价格锚点/条件）通过才标记完成，失败保留量化快照与已完成阶段。

论文引用格式 `[资料:id]`，机器校验，漏引/错引直接拒（`research_validation.go` / `research_verification.go`）。报告区分"来源陈述"与"研究推断"，给出主营逻辑、核心分歧、反证、确认与失效条件（`decision.go`）。

### 3.2 持仓巡检（四维打分 + 程序复算）

`portfolioinspection/scoring_policy.go` 定义权重与统一标尺：

| 维度 | 权重 | 只评什么 |
|---|---|---|
| 持仓逻辑 holding_logic | **35%** | 盈利质量、增长兑现、估值/价格透支、催化与经营反证 |
| 组合结构 portfolio_structure | **25%** | 单票/前三集中、共同驱动、相关性与互补 |
| 风险管理 risk_capacity | **25%** | 股票自身波动回撤、流动性、跳空、退出可执行性 |
| 策略匹配 strategy_fit | **15%** | 研究周期、持仓角色、交易方式与所选风格 |

固定 0-100 整数标尺 + 锚点（90-100 突出、70-79 整体合理、40-59 重大缺陷、0-39 严重不足；"70 不要求无缺点也非保底，按事实选档，不为目标倒推、不按引用数量扣分"）。纪律："集中只扣结构、经营反证只扣逻辑、同一问题只归一维"；"分数非胜率或收益预测"；未知不假定优质或经营差；纯资料缺口只进 data_limitations，不进扣分项。

**程序-AI 边界**：组合事实（仓位/现金/集中度/静态止损风险）由 `metrics.go` 程序计算；AI 只按四维给解释与评分，后端校验事实与报告来源引用后**程序复算加权总分**（`scoring.go`）。个股报告 24 小时内复用（`research_reuse.go`）。

### 3.3 大 V 复盘（共识图 + 次日验证闭环）

- 采集：雪球/淘股吧 Browser Bridge + 微信解析 → 去重归档原文（`review/importer.go`）。
- 共识生成：`daily_summary.go`（prompt `daily-viewpoint-consensus-v10`）从文章集合提炼**共识/分歧/盘面事实/下一交易日验证条件**，结构化输出（`VerificationChecklist`）。
- 次日验证：`daily_validation.go`（`daily-viewpoint-validation-v1`）——规则核验结果作底稿 + AI 解释；AI 提示词红线："规则核验结果是底稿，不能把规则未验证的观点说成已验证；区分正确/部分正确/错误/无法验证；单日涨跌不能单独证明观点质量"。
- 定时：交易日前一自然日默认 **22:00（北京时间）**，按深交所交易日历跳过周末节假日；错过只在目标交易日 09:30 前补跑；断网用 7 天内完整缓存。

### 3.4 市场情绪（model v2 打分）

`httpapi/market_emotion.go`：`currentMarketEmotionModelVersion = 2`，7 日 bootstrap。**赚钱效应（profit）加权公式**（`scoreMarketEmotion`）：

```
profit = 0.35×昨日涨停溢价 + 0.25×昨日连板溢价 + 0.15×开盘溢价 + 0.25×核心收益
```

每项用 `centeredScore + blendedFactor`（相对历史分位），另有 `ThemeFocus`（题材集中度，`weightedThemeFocus`：权重 = max(连板数,1)²×(0.7+0.3×置信度)）与 `HighRiskScore`。盘中快照 TTL 10 分钟；模型版本升级自动重建历史。

### 3.5 拐点评估（题材大/小拐点）

`strategy/inflection/`（config + engine）：识别度五因子加权 `Height(高度)0.25 + Attention(关注)0.20 + Persistence(持续)0.20 + Influence(影响)0.20 + Resilience(韧性)0.15`；行为面强弱两套权重（弱势: Relative 0.35/VWAP 0.20/Board 0.20/Expectation 0.15/Liquidity 0.10；强势: Relative 0.25/VWAP 0.15/Board 0.30/Expectation 0.15/Liquidity 0.15）。三类锚点：旧赚钱效应锚 / 旧负反馈锚 / 新承接物。大拐点候选 62/确认 75 分，小拐点候选 58/确认 68 分，识别阈值 70。八种拐点类型（market_exhaustion 大拐点出清 / sentiment_repair 情绪修复 / high_low_switch 高低切换 / sector_rotation / individual_reversal / trend_reversal / index_reversal / expectation_gap）。**自警**："V1 为快照规则引擎，信号仍需通过历史回放、T+1 和真实成交约束验证"。

### 3.6 概念 → 叙事合并（`narrative/narrative.go`）

把东财概念标签合并为交易叙事：14 条规则关键词映射（AI 应用/光通信 CPO/AI 算力/机器人/低空经济/固态电池等）+ 19 个忽略标签（融资融券/沪深股通/指数成分类）+ 8 个结构性标签。`EvidenceBonus`：算力概念+云计算=更强因果证据（bonus 110）、AI 应用命中具体应用标签 bonus 120——奖励"更具体的概念组合"胜过宽泛重叠标签。`BreadthFactor` 区分结构性题材的宽度折扣（结构类 0.28，AI 类 0.58-0.68）。

---

## 4. 最终产物结构与格式

| 产物 | 结构 | 落盘/schema |
|---|---|---|
| 大 V 观点共识 | 共识/分歧/盘面事实/验证清单（`VerificationChecklist`） | `reviews.db`；飞书/钉钉按作者分观点分段推送 |
| 个股研究报告 | 主营逻辑/核心分歧/反证/确认条件/失效条件/交易条件，带 `[资料:id]` 引用 | `stock-research.db`（版本化证据快照 + 阶段检查点） |
| 持仓巡检报告 | 综合分 + 四维评分卡 + 事实/引用校验 | `portfolio-inspections.db` |
| 持仓优化方案 | 原组合 vs 建议组合对照 + 替换清单 | `portfolio-inspections.db` |
| 市场情绪历史 | 情绪分/赚钱效应/题材集中度/风险分 | `market-emotion.db` |
| 题材雷达 | 融合强度排名 + 题材指数日/周/月 K | `theme-radar.db` |
| 次日验证 | 逐条验证条件 verdict（正确/部分/错误/无法验证/暂不可验证） | `reviews.db` |
| 拐点评估 | 大/小拐点信号 + 因子分 + 锚点 + 警告 | `strategy/inflection` |

---

## 5. 坑与注意事项（来自代码注释/README/文档边界说明）

1. **非商业许可证**（LICENSE Non-Commercial）——商用需授权；商业版账号/密钥/服务地址属商业仓库（AGENTS.md 明确版本边界，商业缺陷不得回流开源）。
2. **开盘啦只负责题材归属**：实时行情/K/领导力指标仍由行情源计算，"开盘啦题材榜是编辑归因"。
3. **V1 拐点引擎是快照规则**：信号需历史回放、T+1、真实成交约束验证（引擎 warning 原文）。
4. **复盘定时需本机唤醒**：错过计划只 09:30 前补跑；应用未运行/电脑休眠则不复盘（README）。
5. **深度研究模型调用关闭工具**：模型只出问题，补证由 Go 只读做，防止模型用未受控工具编造证据。
6. **评分"不为目标倒推"**：程序复算权重分，AI 给的各维分与程序事实/引用校验绑定，防 AI 自由发挥（scoring_policy 原文）。
7. **集中度按股票内部比例**：满仓/现金 0/总仓位高低不加扣分；同一问题只归一维（结构或逻辑，不重复扣）。
8. **PK 数据源易变**：`push2` 板块成分可能失效，节点回退到本地 `theme.go` 的 `StockSymbols` + 新浪实时行情（docs/data-sources.md "stock fallback layer"）。

---

## 6. 可借鉴的设计要点

1. **四档研究分级 + 证据预算**：用"日线根数/公告条数/证据字节上限/是否补证"四组参数把同一个研究流水线切成成本梯度——量化速览零调用，深度研究才开补证，token 成本与深度严格挂钩。
2. **"来源陈述 vs 研究推断"分离 + 机器引用校验**：报告强制区分事实摘引与 AI 推断，引用 `[资料:id]` 由程序校验、错引拒收——是 vibe-astock"程序核对数字"在文本证据上的对应物。
3. **四维打分 + 程序复算 + 绝不倒推**：AI 只做解释，权重/归一化/总分全由程序算出，评分锚点文字化防漂移，"分数非胜率预测"写明用途边界。
4. **开盘啦/东财双源涨停池 + 兜底标记**：涨停池用结构性数据源（开盘啦，含板型/首末封），东财补价格换手并标注兜底原因，来源元数据全程保留（`SourceMeta`）。
5. **拐点引擎的"三类锚点"抽象**：旧赚钱效应/旧负反馈/新承接物三个锚点 + 识别度五因子加权 + 候选/确认双阈值——把"大小题材切换"落到可解释的规则引擎，且明示快照局限。
6. **双 Agent 运行时 + 配置事务**：Hermes/Codex 可切换，任务绑定运行时/模型/Skills/MCP 快照，模型密钥只在 Hermes `.env`、Codex 走子进程环境——模型连接治理清晰。

---

## 7. 主要源文件索引

| 文件 | 内容 |
|---|---|
| `backend/internal/stockanalysis/research_levels.go` | 四档研究策略（bars/公告/证据预算/补证开关） |
| `backend/internal/stockanalysis/research.go` | 深度研究编排（关闭工具→只读补证→三层校验） |
| `backend/internal/stockanalysis/decision.go` | 报告结构（主营/分歧/反证/确认/失效条件） |
| `backend/internal/portfolioinspection/scoring_policy.go` | 四维打分权重与统一标尺锚点 |
| `backend/internal/portfolioinspection/scoring.go` / `metrics.go` | 程序复算加权总分 / 组合事实计算 |
| `backend/internal/review/daily_summary.go` | 大 V 观点共识（consensus-v10） |
| `backend/internal/review/daily_validation.go` | 次日验证（validation-v1 + 规则核验底稿） |
| `backend/internal/marketemotion/` | 情绪 model v2 打分（赚钱效应加权公式） |
| `backend/internal/strategy/inflection/`（engine/config/types） | 拐点评估引擎（识别度/锚点/阈值） |
| `backend/internal/narrative/narrative.go` | 概念→叙事合并 + EvidenceBonus |
| `backend/internal/sector/`（radar/mapper/theme） | 题材雷达融合与开盘啦映射 |
| `backend/internal/providers/duanxianxia/` | 开盘啦涨停池/题材数据源 |
| `backend/internal/providers/eastmoney/` | 东财 K线/板块/龙虎榜/两融 |
| `backend/internal/agent/`（service/runtime/codex） | Hermes/Codex 双运行时 |
| `backend/docs/data-sources.md` | 数据源优先级与可靠性规则全文 |
| `backend/docs/architecture.md` | 运行结构与模块入口 |