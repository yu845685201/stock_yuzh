# Qbot（UFund-Me/Qbot）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：https://github.com/UFund-Me/Qbot · **分析依据**：本地克隆源码（`refrences/UFund-Me__Qbot`，commit `f0425ae`，2026-03-11；Apache-2.0，"AI 智能量化投研平台"）· **配套**：../开源股票分析项目清单-20261010.md
> **分析方法**：精读回测引擎（`qbot/engine/backtest/`）、交易引擎（`qbot/engine/trade/`）、策略库（`qbot/strategies/`、`pytrader/backtest_strategies/`）、RSRS 择时（`rsrs.py`/`find_max_rsrs.py`）、数据/券商接入（`pytrader/easyquotation|easytrader`），结论均来自源码。Qbot 是"聚合型"平台：数据/券商/策略/回测多源拼装，而非自研低层。

---

## 0. 定位与架构总览

Qbot 是中文圈"AI 智能量化投研平台"，把**行情（jqdata/新浪/腾讯/集思录）→ 策略（单因子/RSRS/李金模型/RL/LSTM）→ 回测（向量化尾盘模板 + RSRS 择时）→ 模拟/实盘（easytrader 各券商 + vnpy + 掘金 + CTP + 币安）→ 前端（pyfunds/pytrader web + gui）**拼成一条 GUI/CLI 可操作的链路，并提供 Docker/微信机器人部署。核心价值在于"开箱即用的 A 股/基金/期货/加密多资产模板"，底层重用了 `easytrader`（券商 Web 客户端自动下单）、`easyquotation`（行情）、`vnpy` 等成熟开源件。

模块图：

```
┌──────────────────────────────────────────────────────────────┐
│ UI 层  qbot/gui · pytrader/frontend(web) · web_server.py      │
├──────────────────────────────────────────────────────────────┤
│ 策略层  qbot/strategies/(adx/arbr/boll/bigger_than_ema/lstm/   │
│   rl/sma_cross/clv/single_factor) · pytrader/backtest_strategies│
├──────────────────────────────────────────────────────────────┤
│ 回测层  qbot/engine/backtest/(backtest_base.py 模板 · rsrs.py) │
│   pytrader/newbacktest.py · find_max_rsrs.py(RSRS参数寻优)     │
├──────────────────────────────────────────────────────────────┤
│ 交易层  qbot/engine/trade/                                     │
│   trade_engine.py → SimTradeEngine(虚拟盘)/RealTradeEngine(实盘)│
│   engine_apis/{stocks,futures,btc,...}TradeEngine              │
├──────────────────────────────────────────────────────────────┤
│ 经纪接口  pytrader/easytrader(华泰/通达信/银河/同花顺/雪球)      │
│    qbot/vnpy · 掘金 · pyfutures/ctp · binance                   │
├──────────────────────────────────────────────────────────────┤
│ 行情接口  pytrader/easyquotation(sina/tencent/hk/jsl)          │
│    easyquant.use_quotation('jqdata') 聚合行情抽象               │
└──────────────────────────────────────────────────────────────┘
```

---

## 1. 数据接入与存储

### 1.1 行情抽象（easyquant + easyquotation）

`easyquant.quotation.use_quotation("jqdata")` 是统一行情入口，底层两种：
- **`pytrader/easyquotation/`**（shidenggui 的 easyquotation fork）：`sina.py`（新浪股票）、`tencent.py`（腾讯）、`hkquote.py`、`daykline.py`/`timekline.py`/`minutekline.py`（日/分时/分钟 K 线）、`jsl.py`（集思录，转债/基金）、`boc.py`（中行）、`stock_codes.conf`（股票代码表）；`basequotation.py` 是基础行情类；
- **`jqdata`（聚宽）**：`rsrs.py` 里 `jqdatasdk` 直接 `jq.auth(user,password)` 登录、`jq.get_price(stock, end_date, count, fields)`、`jq.get_trade_days`——需要聚宽账号（`jqdata.json` 存储）。

### 1.2 数据持久化（`qbot/data/`）

`dump_bin.py`（把行情 dump 成 bin，供 `check_dump_bin.py` 校验）、`dump_pit.py`（**point-in-time 数据 dump**，用于无未来函数训练）。这类脚本是 Qbot 里少见的"低层数据工作"。

### 1.3 券商/交易数据

`pytrader/easytrader/` 是 easytrader fork：`eastmoney_trader.py`（东方财富）、`webtrader.py`（通用 Web 下单）、`api.py`（`use('ht_client'/'tongda'/'yinhe'/'tonghuashun'/'xuqiu')` 各券商）、`remoteclient.py/server.py`（远程下单）、`mock_trader.py`（模拟）。`trade_days.json`/`eastmoney.json` 存交易日/东财配置。

---

## 2. 回测引擎机制

### 2.1 向量化尾盘模板（`qbot/engine/backtest/backtest_base.py::BacktestStrategyTemplate`）

这是 Qbot 回测的主范式——**"尾盘根据 K 线买卖"的向量化模板**：

- `__init__(stock_code, bars, days=250)` 存全量 K 线 `bars` 与信号列表；
- `process()`：逐日回放，`get_singal(self.bars[:-days+i-1])`——**用截止到前一交易日的数据算信号**（`[:-days+i-1]` 前缀切片，天然防前视），信号三态：`1` 满仓 / `0` 空仓 / `-1` 保持前仓；
- `output_earning_rate()`：`strategy = (1 + close.pct_change(1).fillna(0) * signals).cumprod()`、`base = close/close[0]`，输出策略净值 vs 基准；
- `get_singal`/`get_scores` 由子类实现（`RSIStrategy` 里 `get_singal` 读 `RSI(df.close, 21)`、`lower_rsi=40`/`upper_rsi=65`）。

注意：源码方法名是 `get_singal`（**拼写错误的 `singal`**，且 `self.signals` 与 `signal` 变量混用，`process()` 里 `singal = self.get_singal(...)`），是历史遗留，可照抄但需留意。

### 2.2 单因子策略库（`pytrader/backtest_strategies/`）

`RSI.py` / `MACD.py` / `CCI.py` / `BOLL.py`（各继承 `BacktestStrategyTemplate`，重写 `get_singal/get_scores`）；`pytrader/newbacktest.py`/`test_backtrade.py` 是回测入口。`qbot/strategies/` 下同名 + 更多：`adx_strategy.py`、`arbr_strategy.py`、`boll_strategy_bt.py`、`sma_cross_strategy_bt.py`、`bigger_than_ema.py`、`lstm_strategy_bt.py`（LSTM 择时）、`rl_strategy_bt.py`（强化学习）、`multi_strategy_bt.py`（多策略）、`undervalued_stock_picking_strategy.py`（低估值选股）、`klines_bt.py`/`k_lines.py`。

### 2.3 RSRS 择时（`rsrs.py` + `pytrader/find_max_rsrs.py`）

**RSRS（阻力支撑相对强度，光大证券研报指标）**是 Qbot 的招牌时机因子，源码实现清晰可复现：

1. `get_ols(x, y)`：对窗口内 `high ~ low` 做一元线性回归，`np.polyfit` 得斜率 slope 与 `r2 = 1 - SSR/TSS`（`sum((y-(slope*x+b))**2) / ((len(y)-1)*var(y))`）；
2. `get_zscore(slope_series)`：对滚动斜率序列做标准化 `(last - mean)/std`；
3. `get_rsrs_score(stock_code, days=60, slop_days=18, M=400, MA=10, buy_sail=0.7, sail_score=-1.4)`：滚动计算 slope 与 r²，`rsrs_score = zscore(slope_series[i:i+M]) * r2[M+i]`；
4. **信号规则**：`rsrs_score < sail_score(-1.4)` → 空仓；`rsrs_score > buy_sail(0.7) 且 10 日 MA 拐头向上（current_ma > last_ma）` → 满仓；`get_timing_signal` 用 `RSRS_threshold=0.7` 直接给 BUY/SELL/KEEP；
5. `find_max_rsrs.py::get_max_args`：对 `day=60, slop_days∈[18,5,10], M∈[600,400,200], buy_sail∈[0.4..0.8], sail_score∈[-0.9..-1.5]` **全参数网格暴力寻优**——README 里的"参数寻优"正是这个矩阵扫描。

`get_rank()`（`ETFQuant.get_rank`）：20 日收益做 `np.polyfit(np.arange(len), close/close[0], 1)[0]` 取斜率最大的标的，是配套的动量选股。

### 2.4 交易引擎（`qbot/engine/trade/`）

`trade_engine.py::TradeEngine(trade_opts, syslog)`：按 `trade_opts["class"]`（"虚拟盘"/"实盘"）分派到 `SimTradeEngine`/`RealTradeEngine`（`trade_sim.py`/`trade_real.py`）；接口 `login/get_cash/get_positions/start_trade/close`。`SimTradeEngine` 按 `trade_type`（股票/基金/期货/BTC/期权）分派到 `engine_apis/{stocks,futures,btc}.{Stock,Futures,Btc}TradeEngine`，平台（华泰/通达信/银河/同花顺/雪球/掘金）经 `easytrader.use(...)` 实例化。账户配置从 `qbot/engine/config.py` 的 `STOCK_SIM_ACCOUNT/FUNDS_SIM_ACCOUNT/FUTURES_SIM_ACCOUNT/BTC_SIM_ACCOUNT/OPTIONS_SIM_ACCOUNT` json 读取。

---

## 3. 策略 API 与扩展方式

最小策略=继承模板重写两个方法（`pytrader/backtest_strategies/RSI.py` 完整示例）：

```python
from backtest_strategies.backtest_strategy_template import BacktestStrategyTemplate

class RSIStrategy(BacktestStrategyTemplate):
    lower_rsi = 40   # 买入线
    upper_rsi = 65   # 卖出线
    def get_singal(self, df):
        rsi = self.get_scores(df)[-1]
        if rsi < self.lower_rsi: return 1   # 买
        if rsi > self.upper_rsi: return 0   # 卖
        return -1                            # 保持
    def get_scores(self, df):
        return RSI(df.close, 21)
```

信号契约：`1`=满仓、`0`=空仓、`-1`=保持。`RSI()` 函数来源未在仓库内注明（`backtest_strategies` 包外部依赖，**未能核实**其库名）。交易侧策略在 `qbot/strategies/` 引入 `BacktestStrategyTemplate`/vnpy cta 模板（`sma_cross_strategy_bt.py` 等 `_bt.py` 为回测版、无 `_bt` 为实盘版）。

---

## 4. 分析/优化能力

- **回测指标**：`BacktestStrategyTemplate.output_earning_rate` 产出 `strategy`/`base` 净值列，`show_plt` 双轴画"收益 + score"；RSRS 脚本打印"基准收益率 vs 策略收益率"。
- **参数寻优**：`find_max_rsrs.py::get_max_args` 对 RSRS 的 `slop_days/M/buy_sail/sail_score/MA` 全网格扫描（源码里注释掉的一组候选参数即搜索空间）+ `find_max_rsrs.py::get_max_args` 找 `max_score`。
- **机器学习/强化学习策略**：`qbot/strategies/lstm_strategy_bt.py`、`rl_strategy_bt.py`（RL 择时）、`multi_strategy_bt.py`；`qbot/data/dump_pit.py` 做 PIT 数据、`dump_bin.py` 加速读。
- **基金/期货**：`pyfunds/backtest`（基金回测 + `fund-strategies`）、`pyfutures/`（CTP 期货、`sma_cross_strategy_bt.py` 双均线示例）。

---

## 5. 绩效产物结构与指标清单

回测产物（`OutputEarningRate` 返回的 `df`）：`strategy`（策略累计净值，`(1+close.pct_change(1).fillna(0)*signals).cumprod()`）、`base`（基准净值 `close/close[0]`）、`signals`（逐日信号）、`score`（`get_scores` 指标值）。RSRS 脚本额外输出 `RSRS[code+"ma18"]`（rsrs_score 序列）、`slope[...]`（斜率序列）、`stock_price[...]`（价格）与"基准收益率/策略收益率"打印。交易引擎侧 `get_cash/get_positions` 返回券商的账户现金/持仓查询结果（非标准化指标）。

---

## 6. 坑与注意事项（来自源码）

1. **`singal` 拼写与变量名混用**：`backtest_base.py` 方法名 `get_singal`、`process()` 里 `singal = self.get_singal(...)`，且 `self.signals` 列表与局部 `signal` 并存——照抄会踩，重构需一次改全。
2. **信号延迟一格是"正确"的前视防护**：`process()` 用 `self.bars[:-days+i-1]`（不含当日）算第 i 日信号，"当前的持仓是上一天的信号"（源码注释），不要误当成 bug。
3. **`get_singal` 返回 `-1` 表示"保持"**：三态语义（1/0/-1）与常见 `0/1` 不同，写策略时 `signals.append(position)` 会保留上一仓位。
4. **jqdata 强依赖账号**：`rsrs.py`/`find_max_rsrs.py` 硬编码 `jq.auth(jqdata.json)`，无聚宽账号跑不通；换用 `easyquotation`（sina/tencent）则免费但接口易失效、字段缩写晦涩。
5. **券商 Web 下单是脆弱的模拟**：`easytrader` 通过券商 Web 客户端自动化下单（华泰 `ht_client`/同花顺 `tonghuashun`/雪球 `xuqiu` 等），非官方 API，反爬/登录验证码/窗口定位随时失效；`trade_sim.py` 里未匹配平台时报 `"当前还不支持该平台, 请联系微信 Yida_Zhang2"`。
6. **账户配置分散**：模拟/实盘账户放 `qbot/engine/config.py` 引用的 json（`STOCK_SIM_ACCOUNT` 等），`trade_sim.py` 中存在 `accounts[...]` vs `sim_accounts[...]` 变量名混用（部分分支引用了未定义 `accounts`，属历史 bug，**未能核实**当前是否已修）。
7. **RSRS 参数是"寻优出来"的**：`find_max_rsrs.py` 注释里挂着一整片手工网格扫描参数组合，意味着默认 `buy_sail=0.7/sail_score=-1.4` 是历史寻优结果，存在过拟合风险（代码本身并未做样本外验证）。

## 7. 可借鉴的设计要点

1. **RSRS 落地非常可读**：`slope(zscore) × r²` 两步 OLS + 滚动标准化 + MA 拐头确认，把研报指标写成 ~20 行可复现代码，是"研报→代码"的教科书式移植。
2. **模板化策略接口**：`BacktestStrategyTemplate` 把"算信号 get_singal / 算指标 get_scores"切成两个纯函数，回测、绘图、参数扫描复用同一份策略类——策略即净函数，引擎做统一回放。
3. **`trade_opts` 配置驱动的多资产分派**：`class/平台/trade_type/trade_code/strategy` 一个 dict 就决定走股票/基金/期货/BTC/期权的模拟或实盘引擎，扩展新资产只需加一个 `engine_apis/{asset}/{asset}_engine.py`。
4. **向量化净值公式显式写出**：`(1 + pct_change * signals).cumprod()` 一行即净值曲线，把"信号×次日收益"的复利口径写死在公式里，避免循环累乘的浮点误差与口径漂移。

## 附：主要源文件索引

| 文件 | 内容 |
|---|---|
| `qbot/engine/backtest/backtest_base.py` | `BacktestStrategyTemplate` 向量化回测模板 |
| `qbot/engine/backtest/backtest_main.py` | 回测入口（RSI/BOLL/MACD/CCI 分派） |
| `qbot/engine/backtest/rsrs.py` | RSRS 择时因子与 ETFQuant 类 |
| `pytrader/find_max_rsrs.py` | RSRS 参数网格寻优 |
| `pytrader/backtest_strategies/{RSI,MACD,CCI,BOLL}.py` | 单因子策略 |
| `qbot/strategies/*` | 策略库（adx/arbr/boll/lstm/rl/sma_cross/RSRS 等） |
| `qbot/engine/trade/trade_engine.py` | 交易引擎入口（模拟/实盘分派） |
| `qbot/engine/trade/trade_sim.py` / `trade_real.py` | 模拟/实盘引擎 |
| `qbot/engine/trade/engine_apis/*` | 各资产（股票/期货/BTC/基金/期权）交易引擎 |
| `pytrader/easytrader/*` | 券商 Web 下单（东财/通达信/银河/同花顺/雪球） |
| `pytrader/easyquotation/*` | 行情（新浪/腾讯/港股/集思录/日分钟 K） |
| `qbot/data/dump_bin.py` / `dump_pit.py` | 数据 dump（bin / point-in-time） |
| `qbot/config.py` / `qbot/engine/config.py` | 配置与账户路径 |
| `pyfunds/*` / `pyfutures/*` | 基金回测与期货 CTP |
| `qbot_main.py` / `main.py` / `monitoring.py` | 主入口与监控 |