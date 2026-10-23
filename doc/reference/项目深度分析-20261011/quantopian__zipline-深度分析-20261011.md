# Zipline 深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：https://github.com/quantopian/zipline · **分析依据**：本地克隆源码（`refrences/quantopian__zipline`，commit `014f1fc`，2020-10-14；Apache-2.0，项目已归档）· **配套**：../开源股票分析项目清单-20261010.md
> **分析方法**：精读账务/撮合（`zipline/finance/`）、滑点/佣金（`slippage.py`/`commission.py`）、订单（`order.py`/`execution.py`）、管线（`zipline/pipeline/`）、数据层（`zipline/data/`），结论均来自源码 docstring/注释。

---

## 0. 定位与架构总览

Zipline 是 Quantopian 开源的 Python 回测引擎（2020 年 Quantopian 关闭后归档，社区 fork `zipline-reloaded` 接续维护）。它有两个划时代的贡献：**①`Pipeline` 因子计算 API**（声明式、跨截面向量化因子图）与 **②bcolz 列存 + 复权数据库（adjustments）** 的高性能数据层，直接影响了 Qlib、Alphalens、Pyfolio 等后续项目。

分层架构（模块图）：

```
┌──────────────────────────────────────────────────────────────┐
│ TradingAlgorithm (zipline/algorithm.py)  —— 用户算法           │
│   initialize() / handle_data(context, data) / analyze()        │
│   data.current()/data.history()/record()/order()/order_target()│
├───────────────┬──────────────────────────────────────────────┤
│ 引擎/账务       │  Pipeline 因子引擎 (zipline/pipeline/)         │
│ finance/       │   Term(Loadable/Computable) → Factor/Filter/   │
│  blotter/      │   Classifier → 图执行 SimplePipelineEngine      │
│  slippage.py   │   run_pipeline → 截面 DataFrame                 │
│  commission.py │                                                │
│  order.py      │                                                │
│  metrics/      │                                                │
├───────────────┴──────────────────────────────────────────────┤
│ 数据层 (zipline/data/)                                          │
│  DataPortal(统一访问) → daily(bcolz) / minute(bcolz)            │
│  SQLite adjustments(拆股/分红/合并复权)  bundles(数据打包/摄取)   │
├──────────────────────────────────────────────────────────────┤
│ assets (zipline/assets/) —— SQLite asset_db(equities/futures/   │
│   exchanges) · AssetFinder · ContinuousFuture 换月              │
└──────────────────────────────────────────────────────────────┘
```

核心数据模型是 **`BarData`**（`zipline/protocol.py`，Cython 定义）与 **`Asset`**（整数 sid + 交易所元数据，替代裸 ticker）。回测按 `sim_params`（`SimulationParameters`）里的 `trading_calendar` 逐 session/min 推进，事件有：`handle_data`（每日/每分交易逻辑）、`before_trading_start`、`analyze`（结束评估）、以及订单成交回调。

---

## 1. 数据接入与存储

### 1.1 Bundle 与 bcolz 列存

数据摄取走 `zipline/data/bundles/`（`quandl`、`yahoo`、`csvdir`、`alpaca`、自定义），`zipline ingest -b <bundle>` 写进 `~/.zipline/data/<bundle>/`。**日线**用 `bcolz ctable`（`zipline/data/bcolz_daily_bars.py`），列：

```
[open, high, low, close, volume, day, id]   # 全部 float64；day 为 datetime64 天；id 为资产内部 id
```

`day` 列是日期（无日内时刻），`id` 是资产定位列（利数据表不按 symbol 分文件，而是单一 ctable + `calendar_offset` 记录每资产首日偏移，`_calc_day_offset` 用 `sessions.get_loc()` 求交易日内偏移）。**分钟线**用 `BcolzMinuteBarReader`（`zipline/data/minute_bars.py`），把 open/high/low/close/volume 各存一个 bcolz carray（OHLC 矩阵），支持 `get_value`/`load_raw_arrays`；`zipline/data/dispatch_bar_reader.py` 按分钟/日自动分派。

### 1.2 复权（adjustments）数据库

复权信息存 **SQLite**（`zipline/data/adjustments.py::SQLiteAdjustmentReader`），三张表：

| 类型 | 含义 |
|---|---|
| `splits` | 拆股：`ratio`（如 2.0 表示 1 拆 2） |
| `dividends` | 分红：`amount`/`payment` 字段，按 ex-date 记录 |
| `mergers` | 合并/换股 |

回测中 `handle_splits`（`MetricsTracker.handle_splits`）在 split 生效日**自动按 ratio 折算持仓股数**；价格复权由 `DataPortal` 用 `adjustments` 把原始价重算为 `AdjustedArray`（`zipline/data/_adjustments.pyx` + `adjustments.py` 的 `choose_values`/`adjust` 逻辑，`use_adjusted` 控制口径，默认 **Total Return（含分红再投资）**）。

### 1.3 数据访问 API（`BarData`）

- `data.current(asset, field)`：当日/当分钟值（field ∈ open/high/low/close/volume/price/last_traded）；
- `data.history(asset, field, bar_count, frequency)`：`frequency ∈ '1d'/'1m'`，返回 pd.Series；`data.history` 内部走 `HistoryLoader`（`zipline/data/history_loader.py`）；
- `data_portal`（`zipline/data/data_portal.py::DataPortal`）统一封装日线/分钟线、调整、`get_spot_value`/`get_adjustments`/`get_history_window`——`history` 只有从 `handle_data` 里拿到 `BarData`（已绑定当前时间）才能正确切窗口，防前视。

### 1.4 资产数据库

`zipline/assets/assets.py::AssetFinder` + SQLite schema（`asset_db_schema.py`）：`equities`（symbol/sid/exchange/start/end_date/股息因子等）、`futures`（root_symbol/expiration/multiplier）、`exchanges`；`ContinuousFuture`（`assets/continuous_futures.pyx`）支撑主力合约自动换月（`roll_finder.py` 定换月规则）。

---

## 2. 回测引擎机制

### 2.1 事件循环与账务

回测核心是 `SimulationBlotter`（`zipline/finance/blotter/simulation_blotter.py`）+ `MetricsTracker`（`zipline/finance/metrics/tracker.py`）：
- `SimulationBlotter.order(asset, amount, style)` 下订单，`open_orders` 按资产聚合，`max_shares=1e11` 限制单笔最大股数；
- 每根 bar 里 `slippage_models[Equity|Future].simulate(data, asset, orders_for_asset)` 逐单 `process_order` 尝试成交，产出 `Transaction`（`create_transaction`）；
- 佣金由 `commission_models[Equity|Future].calculate(order, transaction)` 按成交计算；
- `MetricsTracker` 维护 `account`（现金/权益）、`portfolio`（持仓/市值）、`positions`、`ledger`（逐 bar 净值/收益），`handle_minute_close`/`handle_market_close` 刷新净值与收益（`process_transaction`/`process_commission`/`process_order` 处理资金变动）。

### 2.2 订单生命周期与方法

`Order`（`zipline/finance/order.py`，`__slots__` 省内存）字段 `id/dt/asset/amount/filled/commission/stop/limit/stop_reached/limit_reached/direction/type`；**`amount` 正负即买卖方向**（`direction = copysign(1, amount)`）；状态 `ORDER_STATUS = OPEN/FILLED/CANCELLED/REJECTED/HELD`。

触发机制 `Order.check_triggers(price, dt)`：STOP/LIMIT 用位标志（`SELL=1<<0, BUY=1<<1, STOP=1<<2, LIMIT=1<<3`）；**StopLimit 单在 stop 触发后自动 `self.stop=None` 变成 LIMIT 单**（源码注释 "Change the STOP LIMIT order into a LIMIT order"）。

执行样式（`zipline/finance/execution.py::ExecutionStyle`）：`MarketOrder`、`LimitOrder(limit_price)`、`StopOrder(stop_price)`、`StopLimitOrder(limit_price, stop_price)`；`order()`/`order_percent()`/`order_target()`/`order_target_percent()`/`order_target_value()` 等语法糖把 `TradePercent` 转成 shares 发往 blotter（`TradingAlgorithm.order_target` 在 `zipline/finance/trading.py::SimulationExecutionParameters` 与 dispatch 逻辑处理）。默认撮合是**逐分钟/逐日尝试成交、未成交部分留到下根 bar**（`order.open_amount == order.amount - order.filled`）。

### 2.3 滑点模型（`slippage.py`）

| 模型 | 公式/默认 |
|---|---|
| `VolumeShareSlippage` | 买单 `price*(1+price_impact*volume_share²)`、卖单 `price*(1-price_impact*volume_share²)`；`volume_limit=0.025`（股票默认 2.5% 每分钟量）、`price_impact=0.1`；`volume_share=min(total_volume/volume, volume_limit)`；超过限额抛 `LiquidityExceeded` |
| `FixedSlippage(spread)` | 买 `close+spread/2`、卖 `close-spread/2`，无成交量限制 |
| `FixedBasisPointsSlippage` | **股票默认模型**：买 `price*(1+bp*0.0001)`、卖 `price*(1-bp*0.0001)`，`basis_points=5.0`、`volume_limit=0.1`（每分钟最多成交 10% 量） |
| `VolatilityVolumeShare`（期货） | `MI = eta * sigma * sqrt(psi)`，`new_price = price*(1 ± MI/10000)`；`sigma`=20 日年化波动、`psi`=本 bar 成交量/ADV、`eta` 按 root_symbol（`ROOT_SYMBOL_TO_ETA`）；默认期货滑点 |
| `MarketImpactBase`→`VolatilityVolumeShare`/子类 | 带 20 日 ADV 与波动率缓存（`ExpiringCache`）的冲击基类 |
| `NoSlippage` | 测试用，close 价全额成交 |

所有 fill 前有 `fill_price_worse_than_limit_price` 守卫：**限价单若冲击后价格劣于限价则不成交**（返回 `None, None`）。

### 2.4 佣金模型（`commission.py`）

| 模型 | 默认 |
|---|---|
| `PerShare`（股票默认） | `cost=0.001`（$0.001/股），可选 `min_trade_cost` |
| `PerDollar` | `cost=0.0015`（$0.0015/每美元成交额），`cost_per_share = price*cost_per_dollar` |
| `PerTrade` | 固定每笔（首笔全额、后续 0；doc 注明折扣券商典型 $5） |
| `PerContract`（期货默认） | `cost=0.85`/合约 + 每笔 `exchange_fee`（`FUTURE_EXCHANGE_FEES_BY_SYMBOL`） |
| `PerFutureTrade` | 每笔固定（借 `PerContract` 的 exchange_fee 实现） |
| `NoCommission` | 测试用 |

`calculate_per_unit_commission(order, transaction, cost, initial_commission, min_trade_cost)` 处理最低佣金与首笔交换费的分摊（多笔成交时"先付最低、达到阈值后再按单位递增"）。

### 2.5 交易日历与前视防护

- 日历：`zipline/utils/calendar_utils.py` 的 `get_calendar('NYSE')`（`TradingCalendar`，XLS/文本节假日表，含 `regular/half-day` 时段）；`sim_params.trading_calendar` 贯穿全引擎。
- **前视防护**：(1) fill 只基于"当时的 bar"，`slippage.SlippageModel.simulate` 里 `volume==0`/`isnull(price)` 直接跳过；(2) `data.history` 只看 `BarData` 绑定时刻之前；(3) 复权数据库保证"event 在 ex-date 生效，之前不可见"；(4) 分钟模拟 `emission_rate`（daily/minute）决定 `handle_data` 调用频率与盈亏报告粒度。

---

## 3. 策略 API 与 Pipeline 因子引擎

### 3.1 TradingAlgorithm

```python
from zipline.api import order, record, symbol
def initialize(context): ...
def handle_data(context, data):
    order(symbol('AAPL'), 10)
    record(AAPL=data.current(symbol('AAPL'), 'price'))
```

`TradingAlgorithm(sim_params=... )` 要求显式 `SimulationParameters`（`capital_base` 默认 `10**5`（源码 `SimulationParameters.__init__`，README 亦默认 $10,000,000，此处以 `zipline/utils/run_algo.py`/`simulate` 的 `capital_base` 传参为准，见《坑》第 10 条）、`data_frequency='daily'|'minute'`、`start`/`end`、`emission_rate`、`benchmark`）。经 `zipline/api.py` 的 `initialize` 装饰器把函数注入。`get_environment(field)` 返回 `data_frequency/start/end/capital_base` 等元信息。

### 3.2 Pipeline（zipline 最大特色）

因子用**声明式 `Term` 图**表达（`zipline/pipeline/term.py`），核心类：

- **`Term`**（抽象）→ **`LoadableTerm`**（直接从 loader 取数，如 `EquityPricing.close`、`USEquityPricing`）+ **`ComputableTerm`**（由 `inputs` 经 `compute(inputs, ...)` 算出）；
- `window_length`（滚动窗口，`>1` 时代数用 `AdjustedArray` 跨时间求值）、`mask`（默认 `AssetExists()`）、`inputs`；
- 三大子类族：**`Factor`**（数值因子，`zipline/pipeline/factors/`，如 `SimpleMovingAverage`/`RSI`/`Returns`/`MaxDrawdown`）、**`Filter`**（布尔筛选，`filters.py`，如 `top(N)`/`Latest`/`StaticAssets`）、**`Classifier`**（`classifiers/`，如 `Sector`/`CustomClassifier`）；
- 表达式运算靠运算符重载（`expression.py` 的 `NumericalExpression` 等），`sma = SimpleMovingAverage([close], window_length=30)`，`factor.top(10)`。

执行：`SimplePipelineEngine.compute_chunk(term)`（`engine.py`）**按需递归求因子图**（`graph` 拓扑；`window_safe` 保证无未来窗），`run_pipeline(pipeline, start, end)` 输出截面 DataFrame（index=交易日期、columns=asset，值=因子值），`_to_narrow`/`run_chunked_pipeline` 处理分块内存与 loaders。`init` 时 `attach_pipeline(pipeline, 'name')` + `pipeline_output('name')` 把因子接到主回测。

---

## 4. 分析/优化能力

- **绩效**：`MetricsTracker.handle_market_close` 采样净值后由 `PerformancePeriod`（`zipline/finance/performance/period.py`）经 **empyrical** 算 `returns/sharpe/sortino/alpha/beta/max_drawdown/annual_volatility/information_ratio/calmar` 等（`zipline` 依赖 `empyrical`）；`analyze(results, perf)` 约定回调。
- **分解**：`record()` 记录逐 bar 自定义指标（`recorded_vars`），`get_perf()` 取 `DailyPerformance`（`zipline/finance/performance/`）。
- **管线负载**：`zipline/pipeline/loaders/`（`BlazeLoader`、`DPipelineLoader`、`FrameLoader`、`EquityPricingLoader`）把 pipeline 接到 Blaze/本地 dataframe。
- **参数研究**：zipline 本身不内置网格优化（用户脚本循环），但 `zipline.data.bundles` 与 `run_algo`/`simulate`（`zipline/utils/run_algo.py`）提供 CLI 复用（`zipline run -f algo.py --start --end --capital-base --bundle --benchmark-symbol`）。

---

## 5. 绩效产物结构与指标清单

`algorithm.run()` 返回 `perf_manual`（逐 session 的 `DailyPerformance` 序列）与 `results`（`zipline.FinanceRuntime`）。关键字段（`zipline/finance/performance/` + `metrics/`）：

| 字段 | 含义 |
|---|---|
| `capital_used` / `starting_cash` / `ending_cash` / `ending_value` | 资金 / 起止现金 / 期末价值 |
| `pnl` / `returns` / `tnr` | 当日盈亏 / 收益率 / 累计净收益 |
| `gross_leverage` / `net_leverage` | 总杠杆 / 净杠杆 |
| `algo_volatility` | 策略年化波动 |
| `benchmark_period_return` / `algorithm_period_return` | 基准 / 策略区间收益 |
| `alpha` / `beta` | 对基准回归的 α/β |
| `sharpe` / `sortino` | 夏普（年化）/ 索提诺 |
| `max_drawdown` | 最大回撤 |
| `excess_return` / `tracking_error` / `information ratio`（`information_ratio`） | 超额收益 / 跟踪误差 / 信息比率 |

`results.orders`/`results.transactions` 为逐单/逐笔成交明细（`Order`/`Transaction` to_dict）。口径以 empyrical 实现为准（夏普默认年化、`daily_returns` 输入）。

---

## 6. 坑与注意事项（来自源码注释/docstring）

1. **StopLimit 触发后变 LIMIT**：`Order.check_triggers` 里 `stop_reached` 时 `self.stop=None`，之后按限价单撮合——若随后价格不满足限价会滞留，逻辑上等同于"止损转限价"。
2. **限价单受滑点挤压可能不成交**：`fill_price_worse_than_limit_price` 在现代冲击后价格劣于限价时放弃本次成交——大单+有限价时组合效果需注意。
3. **股票默认滑点是 `FixedBasisPointsSlippage` 而非 `VolumeShareSlippage`**：`SimulationBlotter` 注释明示 default-equity = `FixedBasisPointsSlippage()`（5bp，每分钟最多成交 10% 量）；文档里常被当作 `VolumeShare`，其实期货默认才是 `VolatilityVolumeShare`。
4. **`VolumeShareSlippage` 的 `LiquidityExceeded`**：同一分钟内对同一资产多笔订单累计成交量超过 `volume_limit*volume` 后，后续订单 `break`——订单会留到下分钟，不会超量成交。
5. **bcolz `day` 列无日内时分**：日线表 `day` 是日期，`id` 是资产定位；自己灌数据时列名/顺序（`open,high,low,close,volume,day,id`）需与 `BcolzDailyBarWriter` 完全一致，否则静默错配。
6. **复权默认 Total Return**：`use_adjusted` 默认 true 且含分红再投资；想要 raw/close-adjusted 需显式改 `data_portal` 的 `Adjustment` 配置，否则收益口径含股息。
7. **`max_shares=1e11`**：`SimulationBlotter.max_shares` 是单笔上限，超限订单会被拒——不是可配置的常见项，新手大单策略会 silently 被拒。
8. **数据频率与 `handle_data` 频率绑定**：`data_frequency='minute'` 时 `emission_rate` 决定 `handle_data` 每分钟还是每日触发；`data.history(..., '1d')` 在分钟模式下取的是"截至当前"的日线，不能跨未来。
9. **bcolz 依赖 `numexpr`/`bcolz` 已停更**：bcolz 项目本身已年久失修，安装依赖在 Python 3.8+ 常出问题（社区 fork zi line-reloaded 已换掉 bcolz）——旧标版本不升级会持续受限。
10. **`capital_base` 默认值口径不一**：源码 `SimulationParameters` 默认与 `run_algo.py` 的 `--capital-base` 传参默认值（$10,000,000）不同，务必显式传 `capital_base`，不要依赖"默认 10 万"的旧印象。

## 7. 可借鉴的设计要点

1. **`Pipeline` 声明式因子图**：把"因子→mask→窗口"抽象成 `Term` 组合图，引擎按依赖拓扑 + 分块（chunk）求值，`window_safe` 统一前视校验——这是"因子即表达式、跨截面向量化"的开创性 API，Qlib 的 `$表达式` 高度受其影响。
2. **复权独立成库（adjustments 数据库）**：拆股/分红/合并单独存 SQLite，成交量在事件日自动折算、价格复权按需重算——把"复权"从硬编码逻辑里抽离成数据，支持任意口径（raw/adjusted/total-return）。
3. **bcolz 列存 + id 定位**：日线用单一大 ctable（列方向 + 资产 id 偏移定位），是"海量标的时间序列"列存储的早期实践，秒级截取任意窗口。
4. **amount 符号即方向、`__slots__` 优化内存**：`Order` 用 `__slots__` 省内存（大规模仿真产生海量 Order），正负 amount 表达买卖——把交易语义最简化的工程取舍。
5. **滑点/佣金模型化并分资产类型注册**：`slippage_models`/`commission_models` 以资产类型为 key 默认注入，`FinancialModelMeta` 用元类校验模型允许的资产类型（`allowed_asset_types`），从类型系统上防止"把股票佣金模型套到期权"。

## 附：主要源文件索引

| 文件 | 内容 |
|---|---|
| `zipline/algorithm.py` | `TradingAlgorithm`、`initialize`/`handle_data`/`run`、`record` |
| `zipline/api.py` | 用户 API 装饰器与汇总 |
| `zipline/protocol.py` / `_protocol.pyx` | `BarData`/`Order`/`Portfolio`/`Position` 协议对象 |
| `zipline/finance/order.py` | `Order` 对象、STOP/LIMIT 触发状态机 |
| `zipline/finance/execution.py` | `ExecutionStyle`（Market/Limit/Stop/StopLimit） |
| `zipline/finance/slippage.py` | 滑点模型体系（VolumeShare/Fixed/BPS/VolatilityVolumeShare） |
| `zipline/finance/commission.py` | 佣金模型体系（PerShare/PerDollar/PerTrade/PerContract） |
| `zipline/finance/blotter/simulation_blotter.py` | `SimulationBlotter`、默认模型装配、订单队列 |
| `zipline/finance/metrics/tracker.py` | `MetricsTracker`、账户/持仓/净值、分钟/日收盘结算 |
| `zipline/finance/ledger.py` / `transaction.py` / `position.py` | 台账、成交、持仓 |
| `zipline/finance/performance/*` | 绩效计算（period 经 empyrical） |
| `zipline/data/data_portal.py` | `DataPortal` 统一数据访问与复权 |
| `zipline/data/bcolz_daily_bars.py` / `minute_bars.py` | bcolz 日线/分钟线读写 |
| `zipline/data/adjustments.py` / `_adjustments.pyx` | SQLite 复权库与 AdjustedArray |
| `zipline/data/bundles/` / `loader.py` | 数据摄取 bundle 与注册 |
| `zipline/assets/assets.py` / `asset_db_schema.py` / `asset_writer.py` | 资产库与 AssetFinder |
| `zipline/pipeline/term.py` / `engine.py` / `graph.py` | Pipeline 因子图与执行引擎 |
| `zipline/pipeline/factors/` / `filters/` / `classifiers/` | 内置因子/筛选/分类器 |
| `zipline/pipeline/data/` / `loaders/` | Pipeline 数据列与 loader |
| `zipline/utils/calendar_utils.py` / `run_algo.py` | 交易日历、CLI 运行 |