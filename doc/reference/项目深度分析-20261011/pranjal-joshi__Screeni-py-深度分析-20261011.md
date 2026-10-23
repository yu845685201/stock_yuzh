# Screeni-py（pranjal-joshi）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：[github.com/pranjal-joshi/Screeni-py](https://github.com/pranjal-joshi/Screeni-py) · **分析依据**：本地克隆源码（`refrences/pranjal-joshi__Screeni-py`，git commit `ad1058a` 2026-09-06）· **配套**：`../开源股票分析项目清单-20261010.md`
> **方法**：精读 `src/screenipy.py`（主流程）、`src/classes/{Screener,ScreenipyTA,Fetcher,ConfigManager}.py`（筛选/指标/取数/配置）、`src/agents/*`（LLM Agent）与 README。
>
> ⚠️ **口径说明**：本项目是 **NSE（印度国家证券交易所）** 股票筛选器，并非 A 股系统（原清单把它归入「传统 A 股选股/综合系统」是分类偏差）。下文按源码实际实现如实记录。

---

## 0 定位与使用形态

Screeni-py 是一个 **GUI（Streamlit）+ CLI + AI Agent 三形态的 NSE 突破/盘整股票筛选器**，核心是找出「潜在突破位（Breakout）与下一阻力位（R）」的股票，附 50/200 均线信号、RSI、趋势强度、K 线形态。印度作者开发（MADE WITH ❤️ IN INDIA）。

- 形态：新版本 Streamlit GUI（`streamlit_app.py`）+ 老版本 CLI（`screenipy.py`，DEPRECATED）+ AI-native Agent（`--mode ai --agent swing_trader`）。
- 技术栈：Python；`talib`、`pandas/numpy`、`matplotlib`、`streamlit`、`openai-agents`（AI 模式，需另装）、`chromadb`（向量相似股搜索）、多进程 `multiprocessing` 消费队列。
- 部署：Docker（`joshipranjal/screeni-py:latest`，映射 8501 GUI + 8000 API）。

---

## 1 功能模块全景

| 模块 | 位置 | 职责 |
|---|---|---|
| 主流程 | `src/screenipy.py` | CLI 菜单/索引/筛选模式分发、多进程调度 |
| GUI | `src/streamlit_app.py` + `src/ui/*` | Streamlit 界面（classic/ai-native 双 tab） |
| 筛选引擎 | `src/classes/Screener.py` | 突破/盘整判定、5EMA 盯盘、形态匹配 |
| 技术指标 | `src/classes/ScreenipyTA.py` | talib 封装（EMA/RSI/MACD + 大量 CDL 形态） |
| 取数 | `src/classes/Fetcher.py` | NSE 股票列表/历史 K 线 |
| 配置 | `src/classes/ConfigManager.py` | `screenipy.ini` 参数读写 |
| AI Agent | `src/agents/*` + `personas/*.yaml` | 5 个人格化 LLM 筛选助手 |
| 向量检索 | `chromadb`（`nse_stocks` 集合） | 「找相似形态股」 |
| 并行 | `src/classes/ParallelProcessing.py` | `StockConsumer` 多进程消费 |

---

## 2 数据源来源与采集机制

| 源 | 说明 | 缓存 |
|---|---|---|
| NSE 官方数据 | `Fetcher.py` 拉 NSE 股票列表/指数成分/历史 K 线 | `screenipy.ini` 的 `cachestockdata=y`，pkl 落盘 |
| chromadb | `nse_stocks` 集合存股价向量（找相似形态） | 本地 `chromadb_store/`，启动时清空重建 |

**配置项**（`screenipy.ini`，README 示例）：`period=300d`、`daystolookback=30`、`duration=1d`、`minprice=30`、`maxprice=10000`、`volumeratio=2`（成交量相对 20 日均量的倍数）、`consolidationpercentage=10`（盘整区间%）、`shuffle=y`、`cachestockdata=y`、`onlystagetwostocks=y`、`useema=n`。

**索引选项**（CLI 菜单）：Nifty 50/Next 50/100/200/500、Smallcap/Midcap、新上市（IPO）、F&O 股、US S&P 500、板块指数、自选股（watchlist.xlsx）、全市场。

---

## 3 分析逻辑（筛选规则）

### 3.1 筛选模式（CLI 菜单 executeOption）

| 模式 | 含义 |
|---|---|
| 0 | Full Screening：只展示技术参数、无条件过滤 |
| 1 | Breakout 或 Consolidation（转折/盘整） |
| 2 | 近期放量突破（Breakout & Volume） |
| 3 | 盘整中（Consolidating） |
| 4 | 近 N 日最低成交量（Early Breakout Detection） |
| 5 | RSI 区间（用户输入 min/max） |
| 6 | 反转信号（Reversal） |
| 7 | K 线形态（Chart Patterns） |

### 3.2 结果表字段（README 详解）

| 字段 | 含义 |
|---|---|
| Stock | NSE 代码（Ctrl+Click 跳 TradingView） |
| Consolidating | 近 N 日价格区间（如 `Range = 20.0%`） |
| Breaking-Out | `B:302` 突破位 + `R:313` 下一阻力位 |
| LTP | 最新价（附涨跌幅） |
| Volume | 相对 20 日均量的倍数（如 `2.8x`） |
| MA-Signal | 50-200 MA/EMA 交叉（`200MA-Support`、`BullCross-50MA` 等） |
| RSI | 14 周期 RSI |
| Trend | 趋势线强度（`Strong Up`/`Weak Down`，按 N 日斜率陡峭度分级） |
| Pattern | K 线/形态（`Momentum Gainer`、`Inside Bar (N)`、`Bullish Engulfing` 等） |

### 3.3 技术指标/形态（`ScreenipyTA.py`）

- 基础：`EMA/SMA/MA/MACD/RSI/CCI`（talib）。
- 形态：`CDLMORNINGSTAR/DOJISTAR、EVENINGSTAR、LADDERBOTTOM、3LINESTRIKE、3BLACKCROWS、3INSIDE/3OUTSIDE、3WHITESOLDIERS、HARAMI、MARUBOZU、HANGINGMAN、HAMMER、INVERTEDHAMMER、SHOOTINGSTAR、DRAGONFLYDOJI、GRAVESTONEDOJI、DOJI、ENGULFING` 等 20+ 个 `CDL*` 函数（`.tail(1).item() != 0` 判当日命中）。

### 3.4 AI Agent 模式（`src/agents`）

- `--mode ai --agent <persona>`；5 个人格（`personas/day_trader/momentum_analyst/option_buyer/swing_trader/value_screener.yaml`）。
- `AgentLoader.load_all()` 加载人格，`ScreeniAgent(persona, llm_config).run_sync(query)` 交互式问答（openai-agents 框架）；CLI 进入 REPL。
- `screener_tools.py` 提供工具（供 LLM 调用筛选器）。

### 3.5 并行与进度

`StockConsumer` 多进程（`totalConsumers = cpu_count`，缓存开启时 -1，单核时强制 2）消费 `JoinableQueue`；`alive_progress` 进度条 + `multiprocessing.Value` 计数；`stockDict` 用 `multiprocessing.Manager().dict()` 共享缓存。

---

## 4 最终产物结构与格式

- **结果表**：`screenResults`（展示）与 `saveResults`（存档，列名稍异），`tabulate` 打印、可存 Excel。
- **上次结果持久化**：`src/last_screened_results.pkl`（格式化 + 未格式化两版）。
- **GUI**：Streamlit（`classic_tab.py` / `ai_native_tab.py` / `charts.py`）。
- **5EMA 盘中盯盘**：CLI「E」选项循环扫描，60s 刷新，新增信号时 `alertSound(beeps=5)`。

---

## 5 坑与注意事项

1. **不是 A 股系统**：面向 NSE（印度），数据为 NSE 代码（SBIN/TATAMOTORS 等）与 Nifty 指数；原清单归入「A 股选股」是分类口径错误，不能用于沪深选股。
2. **CLI 老版本已 DEPRECATED**：README 明确 CLI 版废弃，主推 Docker + Streamlit GUI；`screenipy.py` 仍保留但 `isGui()` 分支逐步切换。
3. **AI 模式需额外依赖**：`openai-agents` + `pyyaml`（`import` 失败时提示 `pip install openai-agents pyyaml`），且需配 LLM API（`llm_config.py`）。
4. **chromadb 启动即清空重建**：`chroma_client.delete_collection("nse_stocks")` 每次启动删掉向量库再按需重建，首次向量化全市场较慢；相似股搜索逻辑（`vectorSearch`）依赖该集合存在。
5. **多进程无 daemon 清理风险**：workers 都是 daemon，异常路径下（如 `winerror==5`）依赖手动 terminate，跨平台健壮性一般。
6. **趋势/突破为「内部计算」非图上画线**：README 明确「Trend 不显示在图上、内部计算」，Breakout 的 R（阻力位）也是算法估算值，用户需自行在 TradingView 核对。
7. **配置项需在 `screenipy.ini` 手动改**：无 GUI 化的全量配置页，`useema/onlystagetwostocks` 等冷门项易被忽略；README 建议按交易风格调 `duration`（周线用 `5d`）。

---

## 6 可借鉴的设计要点

1. **「突破位 B + 阻力位 R」双值输出**：不只给「是否突破」，还给「下一阻力位」，README 用「B:302, R:313」的玩法讲清，直接服务止盈/止损点位，是把筛选器做成「可执行信号」的好思路。
2. **多进程「生产者-消费者」并行筛选**：`JoinableQueue + results_queue + N 个 StockConsumer`，用 `csv/pkl` 缓存 + `multiprocessing.Manager().dict()` 共享，全市场几千只也能秒级—分钟级跑完。
3. **形态库集中封装**：`ScreenipyTA.py` 把 20+ `talib.CDL*` 形态统一成 `tail(1).item()!=0` 的「当日命中」接口，供「模式 7 形态选股」按需调用。
4. **向量相似股检索**：用 chromadb 存股价向量做「找形态相似的股票」，把「看图找相似」产品化。
5. **Docker + OTA 自动更新**：`OtaUpdater.checkForUpdate` 检查新版本、Docker 单镜像多架构（含 Mac M 系列），README 专门列表对比「EXE vs Docker」的迁移理由，适配纯 GUI 用户。
6. **AI 人格化（persona yaml）**：把「日交易者/动量分析师/期权买家/波段交易者/价值筛选者」做成可插拔人格文件，LLM Agent 复用同一套筛选工具，是「Screener + LLM」的增量式演进。

---

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `src/screenipy.py` | CLI 主流程（菜单/模式分发/多进程） |
| `src/streamlit_app.py` / `src/ui/*` | Streamlit GUI |
| `src/classes/Screener.py` | 突破/盘整/5EMA/形态筛选引擎 |
| `src/classes/ScreenipyTA.py` | talib 指标 + 20+ CDL 形态封装 |
| `src/classes/Fetcher.py` | NSE 数据拉取 |
| `src/classes/ConfigManager.py` | `screenipy.ini` 配置读写 |
| `src/classes/ParallelProcessing.py` | 多进程 StockConsumer |
| `src/classes/CandlePatterns.py` | K 线形态识别 |
| `src/agents/*` + `personas/*.yaml` | LLM Agent 与人格式 |