# go-stock 深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：https://github.com/ArvinLovegood/go-stock · **分析对象版本**：v2026.10.11.1-release（commit `73d4dcf`）
> **分析依据**：本地克隆源码（`refrences/ArvinLovegood__go-stock`），逐文件精读 `backend/agent/`、`backend/data/`、根目录 `app*.go` 与前端 `src/components/kline/calc.ts`，结论均来自源码注释与实现。
> **配套文档**：../开源股票分析项目清单-20261010.md、../开源股票分析项目深度分析-20261011.md

---

## 0. 定位与使用形态

Wails v2.16.0 + Vue3 + NaiveUI + Vite 的 **Windows 桌面端** AI 股票分析工具（作者明示"仅供娱乐学习"）。后端 Go 单可执行文件，前端打包进 Wails WebView；macOS/Linux 有绿色版但"未测试或功能受限"。无云端服务，数据落本地 SQLite（`modernc.org/sqlite`，纯 Go 免 cgo），AI 分析结果可另存 Markdown。主要面向 Windows 10+，A 股为主、港股/美股/ETF/场内基金/永续合约（Binance/Bitget）为辅。

核心依赖：**eino v0.9.21**（字节跳动 Agent 编排框架，承载 React/PlanExecute/DeepAgents 三种模式）、**mcp-go v1.1.1**（MCP 客户端）、chromedp（浏览器抓取）、echarts v6（图表）。

---

## 1. 功能模块全景

| 栏目/模块 | 性质 | 能力 |
|---|---|---|
| 市场行情（market） | 页面 | A股/港/美股、ETF 搜索、指数与板块资金流、大盘情绪（财联社）、涨停梯队（uplimitLadder） |
| K线分析（stock/KLineChart） | 页面 | 复权切换、13 种绘图工具、波浪理论、成交量分布 VPVR、神奇九转（TD Sequential）、自动背离（RSI/MACD/KDJ）、BBI、涨跌停价位线、买卖点信号叠加 |
| AI 对话/智能体（agent-chat） | 页面 | React/PlanExecute/DeepAgents 三种 Agent 模式，150+ 数据工具，技能系统（eino ADK skill），MCP 扩展，视觉理解（截图/K线图问股），多轮追问 |
| 每日复盘（DailyReview） | 复盘体系 | AI 自动生成五章复盘报告，交易日定时执行，飞书/钉钉推送，落 SQLite |
| 盘前策略（MorningStrategy） | 复盘体系 | 六章盘前必读，消费昨日复盘全文做闭环 |
| 提示词模板回测/推荐回测（PromptBacktest） | 回测 | 历史行情重放+防未来数据，胜率/回撤/夏普/Jaccard 稳定性；AI 推荐按持有期对比沪深300 |
| 信号监控（SignalMonitorPanel） | 盯盘 | 后台买卖点/TEMA 信号监控，语音播报，飞书/钉钉/系统通知 |
| 每日操作计划（DailyOperationPlan） | 盯盘 | 多情景预案 + 止损/触发/目标价盘中预警（30 分钟/24 小时去重） |
| 龙虎榜/游资（LhbHotMoneyDaily） | 资金 | 席位明细 + 知名游资席位映射表（`data/hot_money_seats.json`） |
| 资讯聚合（newsList/HotEvents/MorningStrategy） | 资讯 | 财联社电报、部委政策新闻、个股公告、外媒简讯、财经日历 |
| 知识库/长期记忆 | 记忆 | chromem-go 向量库，私有文档 + 跨会话经验召回 + 用户画像学习 |
| 设置（settings） | 配置 | AI 模型多配置、Tushare/问财 Key、飞书钉钉 webhook、浏览器路径、代理 |

AI Agent 三种模式（`backend/agent/agent.go` 约 2007 行）：
- **React 模式**：标准 ReAct 循环，工具调用 + 观察 + 回答。
- **PlanExecute 模式**：内置三个角色系统消息——任务规划助手（拆 3-5 步）、任务执行助手（按计划调工具）、任务审核助手（决定下一步），子 Agent 委派。
- **DeepAgents 模式**：任务规划 + 子 Agent 委派 + 文件系统 + StreamingShell 集成（`deepagent` 可读源码跑脚本），配危险命令拦截与只读模式。

---

## 2. 数据源来源与采集机制

### 2.1 逐接口明细

| 源 | 接口 | 采集内容 | 限速/反爬/缓存 |
|---|---|---|---|
| **Tushare**（需 token，120 积分） | `api.tushare.pro` POST | A股 `daily`、港股 `hk_daily`、美股 `us_daily` 日线（`tushare_data_api.go`，字段固定 `open/high/low/close/pre_close/change/pct_chg/vol/amount`） | token 直接放 body `Params`；`crawlTimeOut` 秒级超时；无本地缓存 |
| **财联社行情** | `x-quote.cls.cn/v2/quote/a/stock/emotion` | 市场情绪：热度 0-100、涨跌停家数、开板率、获利率、连板梯队（一板~高度板 + 连板率） | `cls_market_api.go`；UA 随机 + Referer；10s 超时；`GetIndexTline` 逐日回溯最近 7 天找上一交易日成交额 |
| **财联社电报** | `www.cls.cn/api/cache?name=telegraph` / `telegraphList` | 快讯列表（telegraph 分页取） | `market_news_api.go`；`telegraph` 分页循环取直到 `has_next=false`，条目去重 |
| **同花顺问财** | `openapi.iwencai.com/v1/comprehensive/search`（网关版）+ `query2data` | 自然语言选股（`ChoiceStockByIndicators` / `SearchStock` 工具） | 需 `IwencaiApiKey`；自定义 Header `X-Claw-Skill-Id` 等；401 配额耗尽时正文是 JSON 字符串，`iwencaiExtractErrorMessage` 专门解析两种错误形态 |
| **东方财富数据中心** | `datacenter-web.eastmoney.com/api/data/v1/get` | 龙虎榜明细、宏观 GDP/CPI/PPI/PMI、机构调研 | reportName 参数区分报表；带 Origin/Referer |
| **东方财富研报** | `reportapi.eastmoney.com/report/list` / `list2` / `bk` / `jg` | 个股/行业/机构研报列表 | 分页 pageSize 50 |
| **东财 AI-SaaS 智能写稿** | `ai-saas.eastmoney.com/proxy/*` | 个股 AI 标签（`dialogTagsV2`）、AI 写研报、可比公司、热点发现（`eastmoney_api.go`） | 复用东财官方 robo-advisor 接口 |
| **新浪资金流** | `vip.stock.finance.sina.com.cn/.../MoneyFlow.ssl_bkzj_*` | 板块/个股资金流入流出 | `market_news_api.go`；json_v2.php 兼容回调 |
| **腾讯行情** | `proxy.finance.qq.com/ifzqgtimg/...` | 排行、涨跌幅（`indexRankDetail2`、`mktHs/rank`） | Referer 校验 |
| **TradingView 新闻** | `news-mediator.tradingview.com/news-flow/v2/news` + `news-headlines.../v3/story` | 中文财经新闻流、正文 | lang=zh-Hans 过滤 |
| **雪球** | `stock.xueqiu.com/v5/stock/hot_stock/list.json`、`hot_event/list.json` | 热门股、热点事件 | **需 Cookie**：`FetchXueqiuCookiesViaChromedp` 用 chromedp 隐性取 cookie |
| **巨羊公社** | `app.jiuyangongshe.com/jystock-app/api/v1/timeline/list` | 财经日历时间线 | Origin/Referer |
| **通达信 MAC/TCP** | `tdx_kline_api.go`（2395 行） | K线、分时、盘口、F10、板块、龙虎榜、主题轮动；集合竞价/个股资金流向（`tdx_mac_extra_api.go`） | 本地协议直连 TDX 服务器 |
| **Binance/Bitget** | 合约 API | 永续合约榜单/K线 | 合约专用代理、K线缓存 TTL 3s（5 秒轮询） |
| **通用网页 ** | chromedp 内置浏览器 | 新闻正文抓取 | 默认 Edge，`BrowserPath` 可配；`BrowserPool`（`pool.go`）复用页面；`GetHtml` 传 `waitVisible` 等元素就绪 |

### 2.2 采集层的坑（代码注释/CHANGELOG 记录）

- **问财配额耗尽的错误形态**：401 时 body 不是 JSON 对象而是 JSON 字符串（如 `"您今天的次数已用完..."`），若按对象解析会丢信息——`iwencaiExtractErrorMessage` 先尝试解析成字符串再解析对象。
- **雪球要 Cookie**：不先跑一次 chromedp 取 cookie，热门股接口直接 403。
- **信号计算刻意留在前端**：买卖点/TEMA 算法只存在一份 TypeScript 实现 `kline/calc.ts`，"移植到 Go 会造成永久双份维护且必然漂移（图上箭头与监控信号对不上）"；Go 侧只做按分钟唤醒（窗口最小化时 Chromium 会深度节流前端定时器）与通知分发（`app_signal_monitor.go` 注释原文）。
- **钉钉推送"提示成功却收不到"**：前端拼的字符串被原样当请求体，非法时钉钉返回 40035 却被当成成功——现统一规范化消息体并解析 errcode。
- **飞书/@所有人权限**：群未开放 @ 权限（飞书发送被拒/钉钉 450103）时自动去掉 @ 重发一次保必达。

---

## 3. 分析逻辑

### 3.1 AI 复盘与盘前策略（复盘类核心）

复盘入口 `backend/agent/daily_review_api.go`，全流程 `GenerateDailyReview`：

1. **防抖**：当日已有 `generating` 且未超时 10 分钟（防进程被杀永久卡 generating）直接拒绝；查询时陈旧 generating 记录超 10 分钟自动复位 failed。
2. **交易日门控**：`triggerType=="cron"` 且当日无市场统计 → 跳过（非交易日不生成空报告）。
3. **素材拼装** `buildDailyReviewMaterial(date)`（各段容错，缺失显式标 `（无数据）` 防幻觉）：
   - ①市场统计：当日收盘快照（涨/跌/涨停/跌停家数、情绪）+ 日内情绪演变 + 近 5 日趋势；
   - ②当日交易记录（`GetTradingRecordList`，含买入/卖出/盈亏合计）；
   - ③龙虎榜游资/机构动向（`LhbSeatApi.GetLhbDailySummary`，游资 top10 + 机构净额 top5；"龙虎榜一般收盘后约 17 点发布"）；
   - ④**昨日盘前策略预判**（`prevTradeDate` 跳过周末、不处理法定节假日）——用于"实际走势 vs 预判"检验，正文截断 1500 字；
   - ⑤前一交易日复盘摘要（衔接参考）。
4. **注入内置系统提示词**（`dailyReviewDefaultSysPrompt`，sysPromptId==0 时）："20 年 A 股实战盘后复盘教练"，硬规则 4 条 + 输出五章（市场概况与情绪定位 / 主线与板块 / 我的交易复盘 / 龙虎榜与资金 / 明日展望 ≤5 条）。核心纪律：只能基于素材、"（无数据）"如实说明、严禁编造数字/个股/板块、每个结论带括号数据依据、交易记录为空时明确"今日无交易"。
5. **流式落盘**：只用 `Content` 通道拼正文（`ReasoningContent` 是 `[STEP]` 中间过程不落库）；`progressEmitter` 按 300ms 节流合并 token 增量发前端（防 IPC 洪泛）。
6. 完成后存 `DailyReview`（Status success/摘要前 200 字）并异步 `SaveAIResponseResult` 同步到研究中心。

盘前策略 `morning_strategy_api.go` 同构，素材为 6 段：昨日复盘全文（截断 4000 字）/昨日市场统计/隔夜外围（`GlobalStockIndex` 缓存，关键词匹配美股三大+亚太+欧洲主指数）/昨日龙虎榜/自选股清单（前 20，带止损止盈）。输出六章：隔夜外围 / 昨日复盘要点 / 今日推演（乐观/中性/悲观三情景+触发条件）/ 关注方向与标的池（**必须来自素材**，禁止凭空推荐）/ 仓位与纪律 / 风险提示。

### 3.2 买卖点信号算法（前端唯一实现，`kline/calc.ts`）

**等权计数制**（2026-09 由加权制简化）：买卖点各 **9 路信号**，每路命中记 1 分（`BUY_SELL_MAX_SCORE = 9`），分三组：

- **震荡组 osc**：RSI(14) 上/下穿 30/70、KDJ(9) 低位金叉（K<35）/高位死叉（K>65）、CCI(20) 回穿 −100/+100；
- **动量组 mom**：MACD(12,26,9) 金叉/死叉、TEMA(21,5) 斜率转向、TRIX(15) 斜率转向、ADX 转向；
- **量价组**：均价线、放量（缩量）。

代码注释强调三组信号**同源性强**（RSI/KDJ/CCI 都是震荡指标，MACD/TEMA/TRIX 都派生自 EMA），设计目标是"信号多样性的最低成本版本"而非单信号边际强度；实测"把 CCI 换成同属 EMA 派生的 TRIX 会让卖点标准档 +0.80%→+0.40%"。

**四重门控**（纯因果，第 i 根只用 [0,i] 数据，无未来函数）：
1. **顺势门控**（日K类周期）：买点候选须收在 MA20 上方、卖点须收在 MA20 下方——实测买点 4097→3440 根、胜率 53.9%→54.9%；
2. **波动放大门控**（仅卖点）：当日 ATR14 ≥ 近 250 根 ATR 均值 × **1.3** 时不出卖点（买点侧同阈值"波动平稳"条件）；
3. **CHOP 门控**（趋势/震荡过滤）：买点只留趋势市（CHOP<45 三档超额 +0.85%~+0.97%）；卖点取 CHOP 两侧极值（<38.2 或 >61.8）排除过渡区；
4. **杠铃买点门控**：动量分支（atrRatio<1.3 && 60 日涨幅 ≤15%）或反转分支其一成立；**卖点确认门控**：当根收盘低于前根（阴跌确认）且 RSI14 ≤50。

买卖点各自独立聚类（同类 minGap 合并），"跨方向不设约束"——原交替校验会把"下跌中跌破上一个买点"的真实卖点丢掉（该卖点价低于前买点），已移除。

### 3.3 情绪分析（`stock_sentiment_analysis.go`）

基于**金融情感词典加权词频**（非 LLM）：`positiveFinanceWords`/`negativeFinanceWords` 映射词 → 权重（"涨停/牛市/暴涨/崩盘/跳水"=3.0，"上涨/反弹"=2.0，"大涨/飙升"=2.5 等），含程度副词放大、否定词取反（×1.5）。转折词前后分句，`postScore *= 1.5`（转折后情感更重要）。汇总 `score > 1.0` → 看涨，`< -1.0` → 看跌，否则中性。支持分词词典（jieba 式 `data/dict/zh/*.txt`）与自定义 `user.txt`。结果落 `CleanOldSentimentAnalyzes` 定时清理。

### 3.4 指标选股（自然语言）

`ChoiceStockByIndicators` 工具把自然语言条件交给问财 `SearchStock` 全文检索，返回结果转 Markdown 表（列名带 `[日期]` 与 `(单位)` 后缀）。工具描述内嵌 9 个示例涵盖 PE/净利润/换手率/量比/均线多头/涨停次数/市值区间等问财语法。AI 侧由模型把用户意图翻译成问财查句。

### 3.5 程序-AI 边界

- 数字全部由程序采集（Tushare/财联社/东财/新浪），AI 只做解读与生成文字；
- 复盘素材拼装时各段"无数据"显式标注，提示词硬性要求"严禁编造"，用括号标数据依据——但**没有** vibe-astock 那样逐数字程序核对的机制，靠提示词约束；
- 盘前策略关注标的池强制"来自素材"，防 AI 凭空荐股。

---

## 4. 最终产物结构与格式

| 产物 | 结构 | 落盘/schema |
|---|---|---|
| 每日复盘报告 | Markdown 五章（市场概况与情绪定位/主线与板块/我的交易复盘/龙虎榜与资金/明日展望） | SQLite `DailyReview{ReviewDate, Status, Content, Summary(200字), TriggerType, AiConfigId, SysPromptId, DurationMs}`，一天一条 Upsert 覆盖 |
| 盘前策略 | Markdown 六章（隔夜外围/昨日复盘要点/今日推演/关注方向/仓位纪律/风险提示） | SQLite `MorningStrategy{StrategyDate, ...}` |
| AI 分析报告 | 存 Markdown 文件（可选目录） | `SaveAIResponseResult(type, title, content, question, prompt)` |
| 信号监控 | 系统通知 + 飞书/钉钉 markdown（content）+ 纯文本（plain，系统气泡不显裸符号） | `SignalRecord` 流水落 SQLite，`GetSignalStats` 配对买点卖点算收益胜率 |
| 预警推送 | 飞书卡片（>3000 字截断）/钉钉（>3000 字截断）/系统桌面通知 | `NotifyChannel app/feishu/dingding` |
| 推荐回测 | 统计页（胜率/收益/回撤/夏普/Jaccard） | 按提示词哈希/日期/个股/周期去重，含"同日横截面调整"（个股收益减同日同周期推荐均值） |

---

## 5. 坑与注意事项（来自源码注释/README/CHANGELOG）

1. **信号算法必须单份源码**：买卖点计算只在前端 TypeScript，Go 侧若复制会"永久双份漂移"（`app_signal_monitor.go` 头注释）。
2. **窗口最小化前端定时器被节流**：后台监控必须由 Go 侧每 60s 唤醒（`signalMonitorTick`）前端引擎，否则切走后信号停摆。
3. **收益统计口径**：买点卖点成交价应取信号所在 K 线**收盘价**，之前用箭头坐标（买点最低价/卖点最高价）会系统性高估收益（2026.09.23 修复）。
4. **龙虎榜约 17 点才发布**：复盘素材注明，避免盘中生成时误判"无数据"。
5. **问财/雪球/Tushare 均有额外配置门槛**：问财 key、雪球 cookie（chromedp 隐取）、Tushare token，缺配时对应工具 `isApiKeyConfigured` 过滤（`FilterToolsByApiKey`）。
6. **`prevTradeDate` 只跳周末不处理法定节假日**（函数注释原文），长假时盘前策略额外用"前一交易日无复盘且无市场统计"判定跳过。
7. **飞书/钉钉 @所有人可能被拒**：自动降级去掉 @ 重发。
8. **"仅供娱乐学习"**：README 明确不构成投资建议；主平台 Windows，其他平台功能受限。
9. **VIP 分层**：全功能开源，但"启动自动同步 24 小时资讯"等是赞助权益（README 赞助计划表）。
10. **浏览器抓取依赖 chromedp**：默认 Edge（可配路径），无外置浏览器则新闻抓取失效（`GetHtml` 走 `BrowserPath==""` 时用默认 chrome，仍有依赖）。

---

## 6. 可借鉴的设计要点

1. **信号算法前端单源 + 后端只做唤醒与分发**：把耗时/复杂的指标算法留在与图表同源的 TS，Go 侧只接管"窗口最小化定时器节流"这类前端做不了的事——职责边界清晰，避免双份实现漂移。
2. **复盘/策略的"素材拼装 + 硬性规则提示词"**：每段容错、"无数据"显式标注、结论带括号依据、标的必须来自素材——用 Prompt 约束幻觉的低成本方案（可作为没有程序逐数核对时的折中）。
3. **等权计数制共振信号 + 四重门控**：9 路信号分三组等权计数，注释里带完整实测数据（胜率/超额/样本量），把"为什么是这个阈值"写得清清楚楚，是可复刻的调参方法论。
4. **去重与幂等的工程细节**：generating 记录 10 分钟超时自动复位、价格区间进出只推一次、同日集合竞价相关去重、钉钉 errcode 解析——都是桌面定时任务的实用护栏。
5. **多源数据 + 浏览器兜底**：API（Tushare/东财/新浪/腾讯/财联社）+ 浏览器抓取（雪球 cookie、新闻正文）分层，配合"150+ 工具 + 工具按 key 过滤 + MCP 扩展"的 Agent 工具生态。

---

## 7. 主要源文件索引

| 文件 | 内容 |
|---|---|
| `backend/agent/daily_review_api.go` | 每日复盘全流程（素材拼装/防抖/五章提示词/流式落盘） |
| `backend/agent/morning_strategy_api.go` | 盘前策略全流程（六章/隔夜外围/自选清单） |
| `backend/agent/agent.go` | Agent 三种模式（React/PlanExecute/DeepAgents）、工具编排 |
| `backend/agent/agent_runner.go` / `agent_run.go` | Agent 运行入口/checkpoint |
| `backend/agent/builtin_models.go` | 内置模型端点（OpenAI 兼容/DeepSeek/Ollama 等） |
| `backend/agent/tools/choice_stock_by_indicators_tool.go` | 问财自然语言选股工具 |
| `backend/agent/tools/tool_groups.go` / `data/tools.go` | 150+ 数据工具注册与分组 |
| `backend/data/tushare_data_api.go` | Tushare 日线（A/港/美） |
| `backend/data/cls_market_api.go` | 财联社市场情绪/指数分时/板块异动 |
| `backend/data/market_news_api.go` | 财联社电报/东财公告研报/新浪资金流/雪球/tradingview 新闻聚合 |
| `backend/data/iwencai_api.go` | 问财 openapi 封装与错误解析 |
| `backend/data/eastmoney_api.go` | 东财 AI-SaaS 智能写稿 + 龙虎榜 + 宏观 |
| `backend/data/policy_news_api.go` | 部委政策新闻聚合 |
| `backend/data/stock_sentiment_analysis.go` | 金融词典加权词频情绪分析 |
| `backend/data/tdx_kline_api.go` / `tdx_mac_extra_api.go` | 通达信 MAC 协议 K线/盘口/板块/龙虎榜 |
| `backend/data/lhb_seat_api.go` + `data/hot_money_seats.json` | 龙虎榜游资席位识别 |
| `app_monitor_plan.go` | 每日操作计划盘中预警（止损/触发/目标，30min/24h 去重） |
| `app_signal_monitor.go` | 后台买卖点信号唤醒与通知分发 |
| `frontend/src/components/kline/calc.ts` | 买卖点 9 路信号 + 四重门控（唯一实现） |
| `frontend/src/components/kline/tdSequentialPrimitive.js` / `divergencePrimitive.js` | 神奇九转 / 自动背离 |