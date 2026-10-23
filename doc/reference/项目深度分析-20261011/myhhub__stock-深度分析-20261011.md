# InStock（myhhub/stock）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：[github.com/myhhub/stock](https://github.com/myhhub/stock) · **分析依据**：本地克隆源码（`refrences/myhhub__stock`，git commit `b6e0ca2` 2026-04-02）· **配套**：`../开源股票分析项目清单-20261010.md`
> **方法**：逐文件精读 `instock/core/`（抓取/指标/K线/筹码/策略/回测）、`instock/job/`（作业编排）、`instock/trade/`（自动交易）、`instock/web/`（Web 展示）与 README，全部结论来自源码实现与注释。

---

## 0 定位与使用形态

InStock 是一个「本地部署、MySQL 落盘、Web 可视化」的传统 A 股综合选股/分析系统。核心链路：**每日定时抓取东方财富等公开接口落库 → talib 计算技术指标 → 识别 K 线形态 → 跑 11 个内置选股策略 → 按买卖条件二次筛选 → 回测验证 → （可选）easytrader 自动打新/交易 → Tornado Web（`localhost:9988`）表格化展示**。

- 技术栈：Python 3.11；`talib`（指标+形态）、`pandas/numpy`、`tornado`（Web）、`SQLAlchemy + PyMySQL`（MySQL/MariaDB）、`easytrader`（交易）、`mini-racer`（骰子/JS 筹码）、`bokeh`（K线图）。
- 部署：常规安装（Windows 优先）或 Docker（`mayanghua/instock`）；库和表在首次运行时按 `tablestructure.py` 自动创建。
- 三种日志：`stock_execute_job.log`（抓取/分析）、`stock_web.log`（Web）、`stock_trade.log`（交易）。
- 声明：仅用于学习，README 明确「本系统中的表格为第三方商业控件（评估版）」。

---

## 1 功能模块全景

| 模块 | 位置 | 职责 |
|---|---|---|
| 综合选股 | `core/crawling/stock_selection.py` + `tablestructure.py: TABLE_CN_STOCK_SELECTION` | 用东财选股器接口拉 200+ 信息栏目（6 大类）落库 |
| 每日股票/ETF 数据 | `core/crawling/stock_hist_em.py`、`fund_etf_em.py` | 抓行情快照、历史 K 线 |
| 资金/大宗/分红/龙虎榜 | `stock_fund_em.py`、`stock_dzjy_em.py`、`stock_fhps_em.py`、`stock_lhb_em.py`(+`_sina.py`) | 资金流向、大宗交易、分红配送、龙虎榜 |
| 抢筹/涨停原因 | `stock_chip_race.py`、`stock_limitup_reason.py` | 早盘/尾盘抢筹、涨停揭秘（同花顺） |
| 指标计算 | `core/indicator/calculate_indicator.py` | 32+ 项技术指标（talib 为主 + 手写复算） |
| 买卖判定 | `job/indicators_data_daily_job.py: guess_buy/guess_sell` | 按指标超买/超卖阈值二次筛选 |
| K 线形态 | `core/pattern/pattern_recognitions.py` + `tablestructure.py: STOCK_KLINE_PATTERN_DATA` | 61 种 talib CDL 形态识别 |
| 筹码分布 | `core/kline/cyq.py` | 三角分布算法算获利盘/成本/集中度 |
| 策略选股 | `core/strategy/*.py`（11 个） | 内置 11 个策略，每个策略一个 `check` 函数 |
| 回测 | `core/backtest/rate_stats.py` | 计算未来 N 日累计涨跌幅（最多 100 日） |
| 自动交易 | `trade/` | easytrader 机器人引擎 + 打新策略示例 |
| 关注/置顶 | `cn_stock_attention` 表 + Web 层 | 关注股在各模块置顶标红 |
| 批量作业 | `job/execute_daily_job.py` 等 | 当前/单日/枚举/区间四种批量形式 |

---

## 2 数据源来源与采集机制

所有抓取均走 `requests`（`core/eastmoney_fetcher.py` 封装的 `eastmoney_fetcher` 类），统一注入 Cookie、代理、重试。全局代理+HealthCheck 在 `core/singleton_proxy.py`。

| 源 | 接口（URL） | 采集内容 | 落盘表 | 备注 |
|---|---|---|---|---|
| 东方财富·行情列表 | `http://82.push2.eastmoney.com/api/qt/clist/get`（`stock_hist_em.py`） | A 股全市场快照与历史日线（分页） | `cn_stock_spot` / 历史按代码存储 | pn/pz 分页，`fid`/`po` 控制排序 |
| 东方财富·历史K线 | `http://push2his.eastmoney.com/api/qt/stock/kline/get` | 单票 K 线/分时 | 指标计算的输入 | secid 区分沪深 |
| 东方财富·分时 | `https://push2.eastmoney.com/api/qt/stock/trends2/get` | 当日分时、资金流分时 | 实时视图 | |
| 东方财富·资金流向 | `http://push2.eastmoney.com/api/qt/clist/get`（`stock_fund_em.py`） | 个股/行业/概念资金流（今日/3/5/10日） | `cn_stock_fund_flow*` | 超大/大/中/小单四档净额 |
| 东方财富·选股器 | `https://data.eastmoney.com/dataapi/xuangu/list`（`stock_selection.py`） | 200+ 栏目综合选股快照 | `cn_stock_selection` | `ps=50` 翻页，页间 `sleep(random(1,1.5))` |
| 东方财富·数据中心 | `https://datacenter-web.eastmoney.com/api/data/v1/get` | 大宗交易、分红配送、龙虎榜、操盘必读 | `cn_stock_blocktrade/bonus/lhb/cpbd` | `reportName` 参数区分报告 |
| 东方财富·资金流日线 | `https://push2his.eastmoney.com/api/qt/stock/fflow/daykline/get` | 单票历史资金流 | `cn_stock_cpbd` 相关 | |
| 东方财富·ETF | `http://88.push2.eastmoney.com/api/qt/clist/get` + kline | ETF 列表与历史 | `cn_etf_spot` 等 | |
| 同花顺·涨停揭秘 | `http://zx.10jqka.com.cn/event/api/getharden/date/{date}/...charset/GBK/` + `.../harden/stockreason/id/{id}` | 涨停原因、连板、封板 | `cn_stock_limitup_reason` | GBK 编码，两步（列表→原因详情） |
| 新浪·交易日历 | `https://finance.sina.com.cn/realstock/company/klc_td_sh.txt` | 开市日文本 | 交易日判定 | `trade_date_hist.py` |
| 新浪·龙虎榜 | `vip.stock.finance.sina.com.cn/q/go.php/vLHBData/kind/{ggtj,yytj,jgzz,jgmx}` | 个股统计/营业部/机构/明细 | `cn_stock_top` | 与东财龙虎榜互为备份 |
| 通达信系·抢筹 | `http://excalc.icfqs.com:7616/TQLEX?Entry=HQServ.hq_nlp` | 早盘/尾盘抢筹排名 | `cn_stock_chip_race_open/end` | 老牌行情服务 |

关键采集机制（来自 `eastmoney_fetcher.py`）：
- **Cookie 三级回退**：环境变量 `EAST_MONEY_COOKIE` → `config/eastmoney_cookie.txt` → 内置默认 Cookie（README 提示默认 Cookie 可能过期，需每周换）。
- **重试**：`urllib3 Retry(total=3, backoff_factor=0.1)`，状态码 `429/500/502/503/504` 触发重试；应用层 `make_request` 再循环 3 次，随机延迟 1~3s。
- **代理**：`singleton_proxy.py` 读 `config/proxy.txt`（`ip:port` 或 `username:password@ip:port`），先对 `data.eastmoney.com` 发一次探测（`verify=False`）测代理可用性。
- **限速**：`stock_selection.py` 分页间 `sleep(random.uniform(1, 1.5))`。

---

## 3 分析逻辑

### 3.1 综合选股（东财选股器，200+ 栏目）

`stock_selection()` 直接套用东财选股器接口，`filter=(MARKET+in+("上交所主板","深交所主板","深交所创业板"))(NEW_PRICE>0)`，`sty` 拼接 `TABLE_CN_STOCK_SELECTION['columns']` 里的全部字段 `map` 名。栏目分 6 类：股票范围（市场/行业/地区/概念/风格/指数成份/上市时间）、基本面（估值/每股/盈利/成长/资本结构/股本股东）、技术面（MACD/KDJ 金叉、放量突破、均线排列、天量法则、射击之星/黄昏之星/早晨之星等约 32 个形态条目）、消息面（公告/机构关注）、人气指标（股吧人气排名等 8 项）、行情数据（股价表现/成交/资金流向/沪深股通）。

### 3.2 技术指标计算（`calculate_indicator.py`）

`get_indicators(data, end_date, threshold, calc_threshold)` 基于 talib + 手写复算，返回 ~45 个指标字段。重点：
- **MACD**：`fastperiod=12, slowperiod=26, signalperiod=9`（`macd/macds/macdh`）。
- **KDJ**：`STOCH(fastk_period=9, slowk_period=5, slowd_period=5)`，`kdjj = 3*kdjk - 2*kdjd`。
- **BOLL**：`BBANDS(timeperiod=20, nbdevup=2, nbdevdn=2)`。
- **CR**：手写，`m_price=amount/volume`（均价），`h_m=high-min(prev_均价,high)`、`m_l=prev_均价-min(prev_均价,low)`，`cr=SUM(h_m,26)/SUM(m_l,26)*100`，再算 5/10/20 日均线。
- **RSI**：`RSI(14)` + `rsi_6/12/24` 三档。
- **VR**：`VR=(AVS+CVS/2)/(BVS+CVS/2)*100`（涨量/跌量/平量 26 日），6 日均线。
- **ATR/TR**：`TR=max(high-low, |high-prev_close|, |prev_close-low|)`，`ATR(14)`。
- **DMI**：注释明确写「talib 计算公式和 stockstats 不同」，故改手写：`pdm=EMA(high_m>low_m?high_m:0,14)`、`pdi=pdm/atr*100`，`dx=|pdi-mdi|/(pdi+mdi)*100`，`adx=EMA(dx,6)`、`adxr=EMA(adx,6)`。**为了和同花顺/通达信对齐**。
- **CCI/MFI**：注释标注「stockstats 典型价采用均价(总额/成交量)计算」，故直接 `CCI(14)`、`MFI(14)`（与 stockstats 不一致，但算法注释说明了差异来源）。
- **STOCHRSI**：注释明确放弃 talib 版 `STOCHRSI`，改写 `(rsi - MIN(rsi,14)) / (MAX(rsi,14)-MIN(rsi,14))*100`，3 日均线为 D。
- **SuperTrend**：`m_atr=atr*3`，`hl_avg=(high+low)/2`，`b_ub/b_lb=hl_avg ± m_atr`，逐日滚动判定上下轨切换（`supertrend_ub/lb/supertrend`）。
- 其余：TRIX、SMA、W&R(6/10/14)、DMA(ma10-ma50)、AMA、OBV、SAR、PSY(12)、BRAR(26)、EMV(14→9均)、BIAS(6/12/24)、TEMA(14)、VWMA(14→6均)、PPO(12/26/1)、DPO、VHF、RVI(10)、FI(2/13)、ENE(ma10 上下轨 ±11%/-9%)、VOL(5/10)、MA20/MA200、WT。
- **NaN 处理**：几乎所有指标在 `np.errstate(divide='ignore')` 下把 NaN/Inf 归 0，最终 `get_indicator` 逐字段 `tail(1)` 取值并再保险一把 Inf/NaN→0。

### 3.3 买卖判定（超买/超卖阈值）

`guess_buy` / `guess_sell` 用 SQL WHERE 直接对 `cn_stock_indicators` 二次过滤：

| 方向 | 条件（全部 AND） |
|---|---|
| 买入 | `kdjk>=80 and kdjd>=70 and kdjj>=100 and rsi_6>=80 and cci>=100 and cr>=300 and wr_6>=-20 and vr>=160` |
| 卖出 | `kdjk<20 and kdjd<30 and kdjj<10 and rsi_6<20 and cci<-100 and cr<40 and wr_6<-80 and vr<40` |

### 3.4 基本面选股

`basic_data_other_daily_job.py: stock_spot_buy`：`pe9>0 and pe9<=20 and pbnewmrq<=10 and roe_weight>=15`（PE(TTM)、PB、加权 ROE），符合即入 `cn_stock_spot_buy`。

### 3.5 K 线形态识别（61 种）

`STOCK_KLINE_PATTERN_DATA` 把 talib 的 61 个 `CDL*` 函数映射成字段（如 `tow_crows=tl.CDL2CROWS`、`three_white_soldiers=tl.CDL3WHITESOLDIERS`、`morning_star=tl.CDLMORNINGSTAR` 等）。`get_pattern_recognitions` 对每根 K 线的 OHLC 逐字段调用，输出 `+100/-100/0`（正=买入信号、负=卖出信号、0=无）。`get_pattern_recognition` 判断当日是否命中任一形态（`!=0` 即有信号）。

### 3.6 筹码分布（`kline/cyq.py`）

三角分布模型（与东财一致）：
- 参数：`accuracy_factor=150`（纵轴刻度）、`range=120`（K线程数）、`cyq_days=210`（缺省 210 交易日成本）。
- 精度：`accuracy = max(0.01, (maxprice-minprice)/(factor-1))`（保证 ≥0.01）。
- 每日换手 `turnover_rate=min(1, turnover/100)`；典型价 `avg=(open+close+high+low)/4`；一字板（`high==low`）画法矩形面积为三角形 2 倍，`GPoint=[factor-1, ...]`。
- 上半三角/下半三角叠加（按 `(curprice-low)/(avg-low)` 或 `(high-curprice)/(high-avg)` 线性折算）。
- 输出：`profile`（分布数组）、`benefit_part`（获利比例）、`avg_cost`（50% 筹码成本）、`percent_chips`（90%/70% 筹码集中度 `(pr1-pr0)/(pr0+pr1)`）。

### 3.7 选股策略模板与 11 个内置策略

策略模板：每个策略一个模块，统一入口 `check(code_name, data, date=None, threshold=...)`，返回 bool；`tablestructure.py: TABLE_CN_STOCK_STRATEGIES` 用 `{name, cn, func, ...}` 注册，天然可插拔（README「封装了策略模板，方便扩展」）。

| 策略（模块） | 核心阈值条件 |
|---|---|
| 放量上涨 `enter.check_volume` | 涨跌幅≥2% 且收>开；成交额≥2 亿；当日量/5 日量均 ≥2 |
| 均线多头 `keep_increasing.check` | MA30 严格递增（30日前<20日前<10日前<当日）；当日 MA30 > 1.2×30日前 MA30 |
| 停机坪 `parking_apron.check` | 15 日内有涨>9.5% 且放量（复用海龟 enter）；涨停后第 1 日高开收涨且 0.97<收/开<1.03；第 2、3 日同样高开收涨且 0.97<收/开<1.03、-5<涨跌幅<5 |
| 回踩年线 `backtrace_ma250.check` | 前段（60日最高收盘价之前）由 MA250 下向上突破；后段收于年线上轨；最低价日与最高价日差 10–50 天；最高量/最低量>2 且 最低/最高<0.8 |
| 突破平台 `breakthrough_platform.check` | 60 日内某日 开<MA60≤收 且放量上涨；该日之前收盘价偏离 MA60 在 -5%~20% |
| 无大幅回撤 `low_backtrace_increase.check` | 60 日涨幅 ≥0.6；期间无单日跌>7%、高开低走>7%、两日累计跌>10%、两日高开低走累计>10% |
| 海龟法则 `turtle_trade.check_enter` | 当日收盘 ≥ 最近 60 日最高收盘价 |
| 高而窄旗形 `high_tight_flag.check_high_tight` | 上市≥60 日（且 `istop=True`，即龙虎榜有机构）；当日 high / 前 24~10 日最低 ≥1.9；前 24~10 日连续两天涨幅 ≥9.5% |
| 放量跌停 `climax_limitdown.check` | 跌>9.5%；成交额≥2 亿；量≥5 日均量 4 倍 |
| 低ATR成长 `low_atr.check_low_increase` | 上市≥250 日；近 10 日最高收盘比最低收盘高 1.1 倍；ATR（`Σ|p_change|/天数`）≤10 |

### 3.8 回测机制（`rate_stats.py`）

`get_rates(code_name, data, stock_column, threshold=101)`：以选股日为起点，向后取 `threshold=100` 个交易日（`RATE_FIELDS_COUNT=100`），逐日算 `sum_pct_change = round(100*(close_i - close_0)/close_0, 2)`，落地为一排「第 N 日累计涨跌幅」字段。`backtest_data_daily_job.py` 对 `cn_stock_indicators_buy/sell` 等表逐票回填未来收益率，供 Web 端统计「策略成功率」。

> 注：README 说「回测数据自动填补到当前」，即选股日之后的行情随时补算，属于**前视补全式回测**（不是阀门式模拟成交）。

### 3.9 自动交易链路（easytrader）

- `trade/trade_service.py`：`broker='gf_client'`（广发同花顺客户端），读 `config/trade_client.json`（账号/密码/`xiadan.exe` 路径）→ `MainEngine(broker, need_data, log_handler)` → `load_strategy()` → `start()`。
- 引擎（`trade/robot/engine/`）：`main_engine.py`（事件+时钟引擎组装）、`event_engine.py`（`instance` 单例，注册/触发事件）、`clock_engine.py`（定时触发：`register_moment` 定点、`register_interval` 循环，`clock_event` 范围 `[0.5,1,3,5,15,30,60]` 分钟 + `open/close`）。
- 策略（`infrastructure/strategy_template.py` 为基类）：
  - `stagging.py`：每天 10:00（`dt.time(10,0,0)`）触发 `self.user.auto_ipo()` **自动打新**；README 特别提醒「不想打新的删除 stagging.py 或不要启动交易服务」。
  - `stratey1.py`：示例，14:54:30 执行 `self.user.buy('000001', price=1, amount=100)` / `sell(...)`，只是占位（代码注释「--------写交易策略---------」留空）。
- 涉及验证码：README 要求安装 `tesseract`（自动识别验证码），说明交易端需验证码识别。

---

## 4 最终产物结构与格式

- **数据库**：MySQL/MariaDB，库表由 `tablestructure.py` 定义并在首次运行自动建表；股票主数据表 `cn_stock_spot`（30+ 字段：价格/涨跌/量比/换手/PE/PB/ROE/资产负债率/总市值等）、指标表 `cn_stock_indicators`（~45 指标列 + 外键）、形态表 `cn_stock_pattern`、策略表 `cn_stock_strategy_enter`（每个策略一张）、回测表 `cn_stock_backtest_data`（N 日收益率列）等。
- **Web（Tornado，:9988）**：视图字典驱动（`core/singleton_stock_web_module_data.py` 把每张表 + 中文列名注册成 `WebModuleData`），新增业务表单只需配视图字典即可出现界面。表布局用第三方评估版 Spread.Sheets 控件（README 声明）。
- **K 线图**：`core/kline/visualization.py` + bokeh（`indicators_web_dic.py` 定义了指标与绘图映射），叠 MACD/BOLL 等指标与筹码分布图。
- **选股结果字段**：综合选股表按东财 `sty` 原样列出的中文字段（代码/名称/涨跌幅/PE/PB/ROE/概念(CONCEPT 以逗号连接)/风格(STYLE)…），`CONCEPT`/`STYLE` 数组字段被 join 成逗号串落库。
- **交易日志**：每策略独立日志文件 `log/{strategy}.log`。

---

## 5 坑与注意事项（来自源码注释与 README）

1. **Cookie/代理是刚需**：东财对高频访问会限流，README 专门写「设置东方财富 Cookie」章节（Cookie 过几天到几周过期，需每周更新）；无 Cookie 用内置默认值「可能过期，仅作备选」。代理文件 `proxy.txt` 的格式示例全部标注「以上均为无效代理」。
2. **指标口径与同花顺/通达信对齐有差异**：源码多处注释「talib 计算公式和 stockstats 不同」，DMI/CCI/MFI/STOCHRSI 是**手写复算**以对齐主流软件；若换用 talib 原生函数结果会不一致。
3. **STOCHRSI 与 CCI 的典型价口径**：stockstats 用「均价=总额/成交量」，而本项目 `CCI/MFI` 用 talib（收盘价典型价）——注释承认两者不同但未统一，使用时需知悉。
4. **回测是前视补全、非真实撮合**：`rate_stats` 只算「选股日后 N 日累计涨跌幅」，不含成交/滑点/涨跌停能否买入的模拟，README 未强调这一点（属需注意的隐含边界）。
5. **打新默认开启**：`stagging.py` 每天 10:00 自动打新，README 用加粗「特别提醒」强调不想要就删文件或别启动交易服务——涉及真金白银。
6. **交易需验证码识别**：依赖 `tesseract`，且只支持 Windows（「自动交易目前只支持 windows」）；交易端 `broker` 需按券商在 `trade_service.py` 里手动配置。
7. **`stratey1.py` 是坏示例**：示例策略里 `buy_stock=[('000001',1,100)]` 写死平安银行、`self.user.buy(buy[0], price=buy[1], amount=buy[3])` 传入的是 `(code,1,100)` 三元素却取下标 3，实际是占位空实现，勿直接使用。
8. **数据库必须先就绪**：基本面选股等 job 用 `if not mdb.checkTableIsExist(...): return` 静默跳过，表不存在时任务会“成功但没干活”（日志里才看得出）。
9. **代理探测无证书校验**：`singleton_proxy.py` 探测请求 `verify=False`，存在中间人风险（内部工具可接受，但勿用于敏感网络）。
10. **综合选股翻页有随机 sleep**：`stock_selection.py` 每页 `sleep(1~1.5s)`，全市场约 100 页会耗 2~3 分钟，是「综合选股」接口最慢的一环。

---

## 6 可借鉴的设计要点

1. **「表结构即 Schema + 视图字典即 UI」**：`tablestructure.py` 把库表、字段类型、中文名、`size`（前端列宽）、策略 `func` 全部集中声明；业务表自动建表、Web 表单自动渲染，加一个数据模块几乎零前端成本——这是本项目可复用性最强的工程决策。
2. **策略 = 一个 `check()` 函数 + 一行注册**：11 个策略被抽象成统一接口并注册进 `TABLE_CN_STOCK_STRATEGIES`，扩展新策略只需写函数+加注册项，回测/Web 自动接入。
3. **指标计算对齐主流软件**：对 talib 与通达信/同花顺不一致的指标（DMI/CCI/MFI/STOCHRSI/CR/VR）选择手写复算并注释差异来源，保证「看图软件能对上」——对个人工具的可信度很重要。
4. **多源背靠背**：龙虎榜同时保留东财（`stock_lhb_em.py`）与新浪（`stock_lhb_sina.py`）两套实现；交易日历（新浪 txt）、涨停原因（同花顺）、抢筹（icfqs）构成对单一数据源的抗风险冗余。
5. **买卖二次筛选用 SQL 而非 Python**：指标计算全市场算一遍后，用一条 WHERE 把超买/超卖条件固化在 SQL 里二次分区，简单、可读、易改阈值。
6. **单例共享 + 多线程提速**：`singleton_stock.py`（历史数据单例）、`singleton_trade_date.py`（交易日历单例）避免重复 IO；`indicators_data_daily_job.py` 用 `ThreadPoolExecutor(max_workers=40)` 并发算指标。README 称 1 天全流程约 4 分钟。
7. **批量时间作业**：`execute_daily_job.py` 支持「当前/单日/枚举/区间」四种形式，且 `run_template.py` 统一封装参数与异常日志，历史补跑很方便。

---

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `instock/core/tablestructure.py` | 全部库表/字段/指标列/61 形态/11 策略的集中声明（约 1077 行） |
| `instock/core/crawling/stock_hist_em.py` | A 股行情快照与历史 K 线（push2 列表 + kline） |
| `instock/core/crawling/stock_selection.py` | 综合选股（东财选股器 200+ 栏目分页抓取） |
| `instock/core/crawling/stock_fund_em.py` | 个股/行业/概念资金流向（今日/3/5/10 日四档） |
| `instock/core/crawling/stock_lhb_em.py` / `stock_lhb_sina.py` | 龙虎榜（东财/新浪双实现） |
| `instock/core/crawling/stock_dzjy_em.py` | 大宗交易（datacenter-web） |
| `instock/core/crawling/stock_limitup_reason.py` | 涨停原因（同花顺 getharden 两步） |
| `instock/core/crawling/stock_chip_race.py` | 早盘/尾盘抢筹（icfqs TQLEX） |
| `instock/core/crawling/trade_date_hist.py` | 交易日历（新浪 txt） |
| `instock/core/eastmoney_fetcher.py` | 东财统一请求器（Cookie/代理/重试/限速） |
| `instock/core/indicator/calculate_indicator.py` | 32+ 技术指标计算（talib + 手写复算） |
| `instock/core/pattern/pattern_recognitions.py` | K 线形态识别调度 |
| `instock/core/kline/cyq.py` | 筹码分布三角模型（CYQCalculator） |
| `instock/core/kline/visualization.py` / `indicator_web_dic.py` | K 线/指标/筹码可视化 |
| `instock/core/strategy/*.py` | 11 个选股策略 |
| `instock/core/backtest/rate_stats.py` | N 日累计收益率回测 |
| `instock/job/indicators_data_daily_job.py` | 指标作业 + 买卖二次筛选（guess_buy/sell） |
| `instock/job/basic_data_other_daily_job.py` | 基本面选股（spot_buy）等非实时数据作业 |
| `instock/job/execute_daily_job.py` | 整体批量作业入口 |
| `instock/trade/robot/engine/{main,clock,event}_engine.py` | 交易机器人引擎 |
| `instock/trade/strategies/stagging.py` / `stratey1.py` | 打新策略 / 示例策略 |
| `instock/web/web_service.py` | Tornado Web 服务入口 |