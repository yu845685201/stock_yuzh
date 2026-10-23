# InvesTool（axiaoxin-com/investool）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：[github.com/axiaoxin-com/investool](https://github.com/axiaoxin-com/investool) · **分析依据**：本地克隆源码（`refrences/axiaoxin-com__investool`，git commit `2323bfd` 2024-11-23，README 版本 `0.0.7`）· **配套**：`../开源股票分析项目清单-20261010.md`
> **方法**：精读 `core/checker.go`、`core/selector.go`、`models/stock.go`、`models/fund.go`、`datacenter/*`（数据源封装）与 README，全部结论来自源码实现。

---

## 0 定位与使用形态

InvesTool 是一个用 **Go** 实现的个人**基本面选股 + 基金筛选**工具。出发点（README 原文）：「用东方财富选股器筛出股票后数量较多，需要人工对每支股票财报指标逐一分析」，于是把这一繁琐过程自动化。形态从脚本演化而来，现以 **Web 网页版 + 命令行** 双形态提供。

- 技术栈：Go 1.20；`gin`（Web，端口 `:4869`，`config.toml` 可调）、`viper`（配置）、`urfave/cli`（CLI）、`excelize`（导出 xlsx）、`swag`（API 文档）、`ratelimiter`（令牌桶限流）、`gocron`（定时任务）、`zap`（日志）、`prometheus`（指标）。
- 命令：`exportor`（选股导出器）、`checker`（单/多股检测器）、`webserver`（Web）、`index`（指数成分股）、`json_dump`（JSON 导出）。
- 数据源：东方财富、亿牛网、新浪财经、天天基金、腾讯（搜索）、招商证券（主力资金）、中国债券信息网（国债收益率曲线）。
- 免责声明（README）：「程序代码仅供本人学习研究使用，不构成投资建议」。

---

## 1 功能模块全景

| 模块 | 位置 | 职责 |
|---|---|---|
| 选股器 | `core/selector.go` + `eastmoney/select_stocks.go` | 东财选股器初筛 → 并发拉基本面 → Checker 终筛 |
| 股票检测器 | `core/checker.go` | 对单/多股的 20+ 项基本面逐项判定是否合格 |
| 股票模型 | `models/stock.go` | 并发拉取财报/估值/股价等 15+ 组数据，算 RightPrice/PEG/波动率 |
| 基金模型 | `models/fund.go` | 基金绩效/排名/4433/相似度/经理筛选 |
| 数据封装 | `datacenter/*` | eastmoney/eniu/sina/qq/zszx/chinabond 接口 |
| 定时任务 | `cron/*` | 交易日同步全量基金 + 4433 列表落 JSON，同步全局变量 |
| 导出器 | `cmds/exportor_*.go` | xlsx/csv/json/png 导出 |
| Web/路由 | `routes/*` + `statics/html/*` | 页面 + REST API |

---

## 2 数据源来源与采集机制

| 源 | 接口（URL 摘录） | 内容 | 说明 |
|---|---|---|---|
| 东方财富·选股器 | `https://datacenter.eastmoney.com/stock/selection/api/data/get/`（`type=RPTA_APP_STOCKSELECT`, `client=APP`, `ps=100000`） | 初筛：ROE/净利增长/营收增长/股息率/PB/市值/PE 等 20 字段 | `Filter.String()` 拼 `(ROE_WEIGHT>=8.0)(…)` 过滤表达式 |
| 东方财富·财务 | `https://datacenter.eastmoney.com/securities/api/data/get`（多个 `reportName`） | 历史财报主/利润表/现金流、机构评级、盈利预测、十大流通股东 | `fina_main/fina_income/fina_cashflow/free_holders/…` |
| 东方财富·估值 | `https://datacenter.eastmoney.com/securities/api/data/get`（`valuation_status`）；`emstockdiag.eastmoney.com`（综合诊断/价值评估） | PE/PB/PS/PCF 估值状态、智能诊股综合评价/价值评估 | |
| 东方财富·历史PE | `https://emfront.eastmoney.com/APP_HSF10/CPBD/GZFX` | 历史市盈率（算中位数） | |
| 东方财富·基金 | `fundmapi/fundztapi/fundsuggest.eastmoney.com`、`j5.dfcfw.com`、`fund.eastmoney.com/Data/FundDataPortfolio_Interface.aspx` | 基金净值/绩效/经理/重仓 | 天天基金 |
| 亿牛网 | `https://eniu.com/chart/pricea/{code}/t/all` | 历史股价（复权前） | 用于算历史波动率 |
| 新浪财经 | `https://suggest3.sinajs.cn/suggest/key=` | 股票/基金关键词搜索建议 | |
| 腾讯 | `https://smartbox.gtimg.cn/s3/?v=2&q=…` | 关键词搜索 | |
| 招商证券 | `https://zszx.cmschina.com/pcnews/f10/stkcnmnyflow` | 主力资金净流入（近 60 日） | |
| 中国债券信息网 | `https://yield.chinabond.com.cn/cbweb-mn/yc/queryTree` 等 | 国债收益率曲线 | |

**限速/防封**：`ratelimiter`（`config.toml` 默认 `enable=true`，`type=mem` 令牌桶）；HTTP 客户端统一 `Timeout: 5 分钟`（`eastmoney.go`）；`uarand` 随机 UA；`goutils.retry-go` 重试。

**基金定时任务**（`cron/fund.go: SyncFund`）：每个**交易日**先 `goutils.IsTradingDay()` 判断，`QueryAllFundList` 拉全量（1 万多只含债基），`SearchFunds` 逐只补绩效，`Update4433` 过滤后把 `fundlist`/`4433List`/`typeList` 落本地 JSON 文件（`ioutil.WriteFile`），README 称全过程约 1 分 50 秒。

---

## 3 分析逻辑

### 3.1 选股器（Selector）

流程：`DefaultFilter` 初筛（东财选股器）→ 并发 `NewStock`（每股票拉 15 组数据）→ `Checker.CheckFundamentals` 终筛 → `SortByROE`。

`DefaultFilter`（`eastmoney/select_stocks.go`）：
- `MinROE=8.0`、`MinTotalMarketCap=100.0`（亿）、`MinPBNewMRQ=1.0`、`ExcludeCYB=true`（排 300 创业板）、`ExcludeKCB=true`（排 688 科创板）；其余（净利/营收增长率等）默认 0（不设门槛）。
- 初筛表达式：`(ROE_WEIGHT>=8.0)(NETPROFIT_YOY_RATIO>=0.0)…(PBNEWMRQ>=1.0)`，可选参数非 0 才追加（如 `DEBT_ASSET_RATIO<=60` 需显式设）。
- 并发用 `jobChan`（大小 `viper.GetFloat64("app.chan_size")`，默认 1）控制并发度，`sync.WaitGroup` + `recover` 兜底。

### 3.2 股票检测器（Checker.CheckFundamentals）—— 核心，逐项阈值

默认参数 `DefaultCheckerOptions`：

| 检测项 | 判定（源码逻辑） | 默认阈值 |
|---|---|---|
| ROE | 上一期年报 + 最新一期财报 ROE 均 < MinROE 判不过 | MinROE=8.0 |
| ROE 逐年递增 | ROE 均值 < NoCheckYearsROE 时才要求 N 年逐年递增 | NoCheckYearsROE=20，CheckYears=**5** |
| EPS | N 年内逐年递增且最后一年 >0 | 开（IsCheckEPSGrow=true） |
| 营收 | N 年内逐年递增且 >0 | 开 |
| 净利润 | N 年内逐年递增且 >0 | 开 |
| 整体质地 | 东财智能诊股「综合评价」∈ {优秀,良好} 否则不过 | 必检 |
| 行业估值 | 估值「高于行业均值水平」不过 | 必检 |
| 四率估值 | PE/PB/PS/PCF 全部「估值较高」不过 | 必检 |
| 合理股价 | `price > RightPrice` 不过（可关 IsCheckPriceByCalc） | 开（默认 true） |
| 负债率 | 非金融股 `Zcfzl > 60%` 不过 | 60（金融股跳过） |
| 历史波动率 | `HV > 1.0` 不过 | 1.0 |
| 市值 | `总市值 < 100 亿` 不过 | 100 亿 |
| 银行股专项 | ROA ≥0.5、资本充足率 ≥8%、不良贷款率 ≤3%、拨备覆盖率 ≥100% | 银行股专用 |
| 毛利率/净利率稳定性 | `IsStability`（N 年标准差 ≤1） | 默认关（false） |
| PEG | `PEG > 1.5` 或 `< 0` 不过 | 1.5 |
| 本业营收比 | `BYYSRatio ∈ [0.9, 1.1]` | 0.9~1.1 |
| 财报审计意见 | 非「标准无保留意见」不过 | 必检 |
| 股息率 | `Zxgxl < MinGxl` 不过 | 0（不检） |
| 负债流动比 | `Ld < 1` 不过 | 1 |
| 现金流量 | 经营净流量<0 / 投资净流量>0 / 自由现金流<0 | 默认关（IsCheckCashflow=false） |

### 3.3 关键指标公式

- **合理价** `RightPrice = PE中位数 × (去年EPS × (1 + 今年各期平均营收增长比/100))`（`models/stock.go`），`LastYearRightPrice` 同理回退一年（年初年报未披露时整体年份 -1 兼容）。
- **PEG = PE / 净利润 3 年复合增长率**（`BaseInfo.PE / NetprofitGrowthrate3Y`）。
- **本业营收比 = 营业利润 / (营业利润 + 营业外收入)**。
- **自由现金流**：`NetcashFree = NetcashOperate ± NetcashInvest`（投资净流量<0 时相加、>0 时相减）。
- **历史波动率**（`eniu/historical_price.go`）：对数收益 `ln(当日/前日)` 的标准差 × √250（YEAR；DAY/WEEK/MONTH 分别 ×1/√5/√21.75），方法引用 goodcalculators 的 HV 计算器。
- **逐年递增**：`HistoricalFinaMainData.IsIncreasingByYears(type, years, reportType)` 比较 N 个年报值；`IsStability` 判 N 年标准差。

### 3.4 基金 4433 法则（`Fund.Is4433`）

精确实现（`models/fund.go`）：
- 前置：**无 5 年数据直接不满足**（`Year5ProfitRatio==0 || Year5RankNum==0`）。
- `quarterRatio = 25`、`oneThirdRatio = 33.33`。
- 近 1 年排名比例 ≤25%；近 2/3/5 年及今年以来排名比例均 ≤25%；近 6 个月 ≤33.33%；近 3 个月 ≤33.33%。
- `FundList.Filter` 另支持自定义（规模 2–50 亿建议值、经理任职年限、1/3/5 年波动率/夏普/最大回撤均值、成立年限）。

### 3.5 基金持仓相似度（`Checker.GetFundStocksSimilarity`）

对每个基金与其他基金算 Jaccard 相似度 `|A∩B| / |A∪B|`（按重仓股名称集合），按相似度降序输出交集名单——用于识别「抱团」基金。

---

## 4 最终产物结构与格式

- **命令行导出**：`exportor` 按文件后缀（`xlsx/csv/json/png/all`）导出；xlsx 按行业、价格、历史波动率分工作表（`exportor_excel.go`）；png 生成自选股代码图片供东财 APP 扫码导入（`exportor_pic.go`）。
- **Web 页面**：股票筛选（`stock_index.html`）、基金筛选/4433（`fund_index/fund_filter/fund_table/fund_managers/fund_similarity`）、股票选基（`hold_stock_fund`）、评论（`comment`）等；后台 REST + swagger 文档（`/x/apidocs`）。
- **检测结果结构**：`CheckResult = map[检测项]map{desc, ok}`——每项返回「描述 + 是否合格」字符串，Web 渲染成逐项红绿清单。
- **JSON 落盘**：基金全量/4433/类型列表存 `materials.json` 等本地文件，供 Web 静态读取。

---

## 5 坑与注意事项

1. **README 与代码默认值不一致**：README「至少 3 年内逐年递增」，但代码 `DefaultCheckerOptions.CheckYears = 5`（CLI `checker.check_years` 默认也是 5）。以代码为准是 **5 年**；照 README 理解会低估筛选严格度。
2. **合理价默认开启过滤**：`IsCheckPriceByCalc` 默认 `true`，即「股价 > 估算合理价」的股票会被直接过滤；CLI `--checker.is_check_price_by_calc` 默认却是 false（README help 显示 default false，与 `DefaultCheckerOptions` 里 true 矛盾）——代码结构体默认 true、CLI 展示 default false，两处不一致。
3. **年初财报空窗兼容**：`NewStock` 与 `CheckFundamentals` 都有「新的一年刚开始，上一年年报未披露」的 nil 兜底（年份整体 -1），避免年初误杀，但也意味着年初用的是前年年报数据。
4. **东财接口强依赖 + 反爬**：`client=APP`、随机 UA、5 分钟超时、令牌桶限流都指向同一个事实——东财接口封禁风险高（README 强调「使用者不得干扰数据来源网站服务」）；README 同时提示「中报发布结束后满足条件的公司增多，筛选耗时变长，反向代理超时需调大」。
5. **历史 PE 中位数可能为空**：若某票历史 PE 列表为空，`GetMidValue` 返回 error，`RightPrice` 算不出，`IsCheckPriceByCalc=true` 时可能误过滤（或该项 desc 缺失），取决于并发 goroutine 的容错路径。
6. **波动率用「不复权」股价**：`eniu.com` 历史价未复权，派息/送股会制造假跳变、放大对数收益，从而高估历史波动率（对分红股尤其明显）。
7. **银行股规则硬编码到 OrgType**：银行股特判依赖 `GetOrgType()` 返回「银行」/「保险」，若东财行业分类变化会漏判；非金融股统一 60% 负债率一刀切。
8. **本业营收比除零风险**：`BYYSRatio = OperateProfit/(OperateProfit+NonbusinessIncome)`，当 `营业利润+营业外收入=0` 时得 0 或 NaN（源码未判），可能误判。
9. **限流并发度默认 1**：`app.chan_size=1` 是保守默认，全市场逐票串行拉 15 组数据会很慢（README 也提示需调大反代超时）。

---

## 6 可借鉴的设计要点

1. **「初筛（东财选股器）→ 终筛（自研检测器）」双层漏斗**：把「选股器」当粗筛，把「财报逐年递增/合理价/审计意见」这些东财不提供的逻辑放自研层，是性价比很高的架构——接口能做的不重复造轮子，接口没有的才自己算。
2. **检测项 = 单例可解释**：`CheckResult` 每个检测项带 `desc + ok`，把「为什么这票不合格」讲清楚（如 `ROE: 12.3%<br/>低于:8.0`），比一个 bool 输出对个人决策有用得多；Web 直接渲染成红绿清单。
3. **合理价/波动率/PEG/本业比等派生指标公式化并集中注释**：每个公式在源码注释里给了定义与出处（如 HV 引用 goodcalculators、本业比解释「垄断、必要性服务特征」），可复用到其他项目。
4. **基金 4433 的「无 5 年数据直接排除」前置**：防止新基金用短期高收益钻空子，是 4433 的常见漏洞补丁。
5. **Go 并发模型干净**：`sync.WaitGroup + channel 信号量 + recover` 的「每股票一个 goroutine」取数模式，配 `NewStock` 内部再把 15 组数据并发拉取，是数据聚合型工具的范式。
6. **数据源隔离为 datacenter 包**：每个上游一个子包 + README 说明，swap 数据源只动 datacenter，是抗上游易变的良好分层。
7. **4433 结果落 JSON 供 Web 静态读**：避免每次页面请求都重算 1 万多只基金，用定时任务预计算 + 文件缓存，兼顾实时性与性能。

---

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `core/selector.go` | 双层筛选主流程（初筛→并发→终筛→排序） |
| `core/checker.go` | 股票基本面 20+ 项检测（默认阈值）+ 基金持仓检测/相似度 |
| `core/searcher.go` | 关键词搜索股票/基金 |
| `models/stock.go` | Stock 模型 + RightPrice/PEG/波动率/自由现金流等派生指标 |
| `models/fund.go` | Fund/绩效/排名/`Is4433`/Filter/相似度 |
| `models/global.go` | 全局变量（全量基金/4433 列表内存态） |
| `datacenter/eastmoney/select_stocks.go` | 东财选股器初筛 + `DefaultFilter` |
| `datacenter/eastmoney/fina_main.go` 等 | 财报/估值/评级/预测/股东等东财接口 |
| `datacenter/eniu/historical_price.go` | 亿牛网历史股价 + 历史波动率计算 |
| `datacenter/zszx/net_inflows.go` | 招商证券主力资金净流入 |
| `datacenter/chinabond/*` | 中国债券信息网收益率曲线 |
| `cron/fund.go` / `cron/stock.go` / `cron/bond.go` | 基金/股票/债券定时同步 |
| `cmds/exportor_*.go` | xlsx/csv/json/png 导出器 |
| `routes/stock.go` / `routes/fund.go` | Web 股票/基金路由 |
| `config.toml` | Web/限流/日志/定时表达式配置 |