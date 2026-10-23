# Qlib 深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：https://github.com/microsoft/qlib · **分析依据**：本地克隆源码（`refrences/microsoft__qlib`，tag `v0.9.8`，commit `5435523`，2026-10-08）· **配套**：../开源股票分析项目清单-20261010.md
> **分析方法**：逐文件阅读数据层（`qlib/data/`）、回测引擎（`qlib/backtest/`）、工作流（`qlib/workflow/`）、贡献策略/模型（`qlib/contrib/`）与 config，所有结论均来自源码注释与实现。Qlib 是微软开源的"AI 导向"量化投资平台，论文 arXiv:2009.11189。

---

## 0. 定位与架构总览

Qlib 的定位不是"事件驱动的交易回测器"，而是**覆盖"数据处理 → 模型训练 → 回测 → 组合构建/归因"全链条的研究平台**，配置文件（`qlib.init(provider_uri=...)`）驱动一切。其核心创新是：**数据用 `$表达式` 引擎统一表达**，策略与执行器用**嵌套执行（Nested Execution）**统一表达，从而让"日频选股 + 分钟频执行"这类多频组合可以像搭积木一样组合。

分层架构（模块图）：

```
┌───────────────────────────────────────────────────────────────┐
│  workflow 层 (qlib/workflow)                                    │
│   Recorder / QlibRecorder —— 实验编排；RecordTemp 子类：         │
│   SignalRecord(预测) / SigAnaRecord(IC/IR) / PortAnaRecord(回测) │
│   qrun <config.yaml> 一键端到端；experiment manager (exp.py)      │
├───────────────────────────────────────────────────────────────┤
│  strategy 层 (qlib/strategy)    模型层 (qlib/model, contrib/model)│
│   BaseStrategy → TopkDropout    LightGBM/LSTM/... 训练器 trainer  │
├───────────────────────────────────────────────────────────────┤
│  backtest 层 (qlib/backtest)   —— 引擎本体                       │
│   backtest_loop / BaseExecutor(嵌套) / Exchange(撮合) /           │
│   Account/Position(账务) / decision(Order) / report(指标)        │
├───────────────────────────────────────────────────────────────┤
│  data 层 (qlib/data)           —— 数据与缓存                     │
│   D / Cal / Inst / FeatureD / ExpressionD / DatasetD / PITD      │
│   ExpressionOps 算子(54 类) + ops.py；storage(file_storage)      │
│   cache.py (MemCache H / DiskExpressionCache / DiskDatasetCache) │
├───────────────────────────────────────────────────────────────┤
│  storage 底层：~/.qlib/qlib_data/cn_data/{calendars,instruments, │
│   features, financial, dataset_cache, features_cache}/*.bin      │
└───────────────────────────────────────────────────────────────┘
```

关键入口文件与职责：

| 文件 | 职责 |
|---|---|
| `qlib/config.py` | QlibConfig 单例 `C`，全局默认参数（`provider_uri="~/.qlib/qlib_data/cn_data"`、`kernels=NUM_USABLE_CPU`、`default_disk_cache=1`、各市场 trade_unit/limit_threshold 等） |
| `qlib/data/data.py` | 6 类 Provider 基类 + LocalProvider/ClientProvider；`D`/`Cal`/`Inst`/`FeatureD`/`ExpressionD`/`DatasetD`/`PITD` 是 `Wrapper` 单例 |
| `qlib/data/ops.py` | `ExpressionOps` 算子体系（54 个类） |
| `qlib/data/storage/file_storage.py` | `FileCalendarStorage` / `FileInstrumentStorage` / `FileFeatureStorage`（`.bin`） |
| `qlib/data/cache.py` | 内存缓存 `H` + 磁盘表达式/数据集缓存 |

---

## 1. 数据接入与存储

### 1.1 标准数据格式（`.bin` + 表达式索引）

Qlib 的原始数据是**每个 (instrument, field, freq) 一个 `.bin` 文件**，文件路径规则见 `FileFeatureStorage.file_name`：

```
{provider_uri}/features/{instrument.lower()}/{field.lower()}.{freq.lower()}.bin
```

`.bin` 内部格式（`FileFeatureStorage.write` / `__getitem__`）：
- 文件用 `<f`（little-endian float32）二进制存储；
- **第一个 float 是该序列的起始日历索引 `start_index`**，其后是逐交易日的 float32 值；
- 读取时 `fp.seek(4*(i-start_index)+4)` 用 struct 逐点/切片读取，**靠在共享交易日历上的整数索引对齐时间**，而非在磁盘上存 datetime——这是 Qlib 比 pandas 整表读快的原因之一。
- `__len__` 返回 `文件字节数 // 4 - 1`（减去头部索引）。

配套文件：
- 日历 `{provider_uri}/calendars/day.txt`（每行一个 `YYYY-MM-DD`），`FileCalendarStorage.file_name` 为 `{freq}.txt`（future 日历为 `{freq}_future.txt`）；
- 股票池 `{provider_uri}/instruments/{market}.txt`，Tab 分隔三列 `instrument \t start_datetime \t end_datetime`（一只股票多段上市区间会多行），README 注释明确这是"可做退市/幸存者偏差处理的关键"。

### 1.2 表达式引擎（数据层核心）

`D.features(instruments, fields, ...)` 中的 `fields` 是**表达式字符串**，由 `ops.py` 的表达式解析器（`expression_parser.py` → `parse_expression`）解析成算子树再求值。关键类与语义：

| 类别 | 代表类 | 语义 |
|---|---|---|
| 元素运算 | `Abs`/`Sign`/`Log`/`Mask`/`Not` | 逐元素一元 |
| 二元运算 | `Add`/`Sub`/`Mul`/`Div`/`Power`/`Greater`/`Less`/`Gt`/`Ge`/`Lt`/`Le`/`Eq`/`Ne`/`And`/`Or` | 逐元素二元 |
| 时序/滚动 | `Rolling`（基类）→ `Ref`/`Mean`/`Sum`/`Std`/`Var`/`Skew`/`Kurt`/`Max`/`IdxMax`/`Min`/`IdxMin`/`Quantile`/`Med`/`Mad`/`Rank`/`Count`/`Delta`/`Slope`/`Rsquare`/`Resi`/`WMA`/`EMA` | 窗口算子，`Ref($close, -1)` 昨日收盘 |
| 滚动成对 | `PairRolling` → `Corr`/`Cov` | 双序列滚动相关/协方差 |
| 分支/重采样 | `If` / `TResample` | 条件 / 高频整合到低频 |
| 换标的 | `ChangeInstrument` | 跨标的取值 |

**未来窗口防护**：每个 `ExpressionOps` 实现 `get_extended_window_size()` 返回 `(left_extend, right_extend)`（如 `Ref(x, -1)` 需要左扩 1 天），数据查询时按 `query_start = start_index - left_etd`、`query_end = end_index + right_etd` 提前抓取（见 `LocalExpressionProvider.expression` 与 `DiskCache.update` 里的 `remove_n = min(rght_etd, ele_n)`），缓存更新时会**回退删除被未来数据污染的尾部**——这是 Qlib 前视偏差防护的落点之一（详情见 §2.4）。

### 1.3 本地存储引擎与缓存

三层缓存（`cache.py`）：

1. **内存缓存 `H = MemCache()`**：三个 LRU 单元 `H["c"]`（日历）、`H["i"]`（股票池）、`H["f"]`（feature），默认 `mem_cache_size_limit=500`（`config.py`），可用 `limit_type="sizeof"` 按字节限制。`MemoCacheExpire` 支持过期（`mem_cache_expire`）。
2. **表达式缓存 `DiskExpressionCache`**：为每 (instrument, field, freq) 生成 `{hash}.bin`（`_uri = hash_args(...)`）+ `.meta` pickle；只有**非原始 Feature 的复合表达式**才落缓存（`isinstance(parse_expression(field), Feature)` 为 False 时生成），原始字段直接透传。
3. **数据集缓存 `DiskDatasetCache`**：整个 DataFrame 用 HDF5 存，三件套 `{hash}.data`（写完 rename 去掉后缀）/`{hash}.index`/`{hash}.meta`；`.index` 用 `(start, end)` 记录每个交易日对应的行区间（start 闭、end 开，见 `IndexManager.build_index_from_data`），供 `read_data_from_cache` 快速切片。列名前做 `remove_fields_space`（去空格）保证一致。

并发与锁：Redis 读写锁（`CacheUtils.reader_lock`/`writer_lock`，`redis_lock` 库），meta 里记录 `visits`/`last_visit` 供 LRU 回收；未装 Redis 时本地仍可用（`SimpleDatasetCache` 用 pickle 落盘）。

### 1.4 Point-in-Time（PIT）数据库

`LocalPITProvider.period_feature` 支持查询"截至某日"的财务/季度数据，字段约定 **必须以 `_q`（季度）或 `_a`（年度）结尾**（如 `$$roewa_q`），底层文件：

```
{provider_uri}/financial/{instrument.lower()}/{field}.index  +  {field}.data
```

以 `cur_time_int = 年*10000+月*100+日` 二分查找修订版本记录（`np.searchsorted(data["date"], cur_time_int, "right")`），`end_index <= 0` 断言禁止查未来数据。表达式里用 `P` 算子把季度字段"摊平"成日频：`P($$roewa_q)`（源码注释明确：不能直接查 `$$roewa_q`，必须经 `P` 转换）。

### 1.5 数据源立场

Qlib 本身**不内置行情抓取**，只消费从 `scripts/data_collector/`（如 yahoo/cn_data 的 dump 脚本）灌入的标准化 `.bin` 数据；README/data 文档说明 cn_data 来自开源社区整理。`ClientProvider` 与 `qlib-server`（socketIO）支持"服务器算表达式、客户端读 NFS 缓存"的离线/在线分离部署。

---

## 2. 回测引擎机制

### 2.1 引擎类型：嵌套执行（近似事件驱动，但"订单"为批处理）

Qlib 回测按 **`trade_calendar` 逐步推进**（`backtest_loop` / `collect_data_loop` 里的 `while not executor.finished():` 循环），每步由 Strategy 产生 `BaseTradeDecision`（一批 `Order`），交给 Executor 撮合。它不是逐笔事件驱动，而是**按 bar（日/分钟）批量生成订单 → Exchange 批量撮合 → Account 账务更新**。`SimulatorExecutor` 支持两种订单执行序（源码注释详细说明）：

- `TT_SERIAL`（默认 "serial"）：订单按序执行，允许"先卖后用回款再买"；
- `TT_PARAL`（"parallel"）：并行执行，卖单与买单冲突会失败；实现里 `sorted(orders, key=lambda order: -order.direction)` 按方向排序（**假设单步内不会同向多空混用**，见注释）模拟并行。

**嵌套执行**是 Qlib 的招牌设计（`NestedExecutor`）：外层"日频"执行器在每一天内部再驱动一个"分钟频"的 inner_executor/inner_strategy 循环，`_init_sub_trading` 重置子层日历、`_update_trade_decision` 允许外层决策逐步更新、`align_range_limit` 对齐外层给定的交易时间窗。`Account` 在嵌套层间用 **shallow copy**（`copy.copy(common_infra.get("trade_account"))`，见 `reset_common_infra` 注释）：各层各有一份 `Account` 对象独立算指标，但**共享同一份 `Position`**。

### 2.2 撮合与成本模型（`qlib/backtest/exchange.py`）

订单类 `Order(stock_id, amount, direction, start_time, end_time, factor)`，`OrderDir.BUY/SELL`。撮合核心在 `Exchange.deal_order` → `_calc_trade_info_by_order`：

- **成交价 `deal_price`**：`$close`/`$open`/`$vwap` 表达式（`buy_price`/`sell_price` 可分别指定），`get_deal_price` 里若价格为 None/NaN/≤1e-8 则回退到 `get_close`；
- **成本参数（默认值，中国区 config）**：`open_cost=0.0015`（开仓费率）、`close_cost=0.0025`（平仓费率，含印花税口径）、`min_cost=5.0`（最低 5 元）、`impact_cost`（市场冲击成本，默认 0，推荐 0.1）；`trade_unit=100`（A 股整手，config.py 中 cn 市场 `trade_unit=100`，us/hk 分别为 1/1000）。
- **冲击成本**：`adj_cost_ratio = impact_cost * (trade_val / total_trade_val) ** 2`，按成交额占当日总成交额比重的平方放大（TWAP 类非线性冲击模型）；
- **现金约束裁剪**：买入时若现金不足，`_get_buy_amount_by_cash_limit` 按 `cash/(1+cost_ratio)/price` 与 `(cash-min_cost)/price`（临界价 `min_cost/cost_ratio+min_cost`）裁剪并 `round_amount_by_trade_unit` 取整；卖出裁剪到持仓数量（不可做空，注释 `# TODO: make the trading shortable`）；
- **成交额取整**：`round_amount_by_trade_unit` 用 `(deal_amount*factor+0.1)//trade_unit*trade_unit/factor`，`factor` 为复权因子（§2.3）；若 `factor` 缺失则 `trade_w_adj_price=True` 用复权价、**禁用 trade_unit**（会有 warning）。

### 2.3 复权处理

`Exchange.get_quote_from_qlib` 读取 `$factor` 字段（`factor.day.bin`）：存在且无 NaN → 用**未复权价 + factor 做数量对齐**（`trade_unit/factor`）；`factor` 文件缺失或 `$close` 存在但 factor 为 NaN → 自动切 `trade_w_adj_price=True`（**用复权价直接报单**，同时禁用 trade_unit 并告警"Order using adjusted_price"）。每笔成交价用 `deal_price` 与 `$close` 算组合市值时同源同口径，避免复权/不复权混用。

### 2.4 前视偏差防护

1. **表达式窗口外扩 + 缓存回退删除**（§1.2）：复合表达式的未来依赖窗口 `rght_etd` 在缓存增量更新时被显式 `f.truncate(...)` 删除尾部重算（`DiskExpressionCache.update` / `DiskDatasetCache.update`）。
2. **撮合价时间窗**：`check_stock_limit` / `check_stock_suspended` 只使用 `[start_time, end_time]` 闭区间数据，`limit_buy`/`limit_sell` 由 `$change` 或自定义表达式在当日前一天收盘计算。
3. **PIT 数据 `end_index <= 0` 断言**（§1.4）。
4. **标签（label）用未来一天收益**是**有意**的：`Ref($close, -2)/Ref($close, -1) - 1` 表示"明天相对今天的涨跌幅"，用于监督学习是标准做法，不是前视泄漏（训练/推断分频段切分见 §3）。

### 2.5 交易日历

`Cal.calendar(...)` 由 `FileCalendarStorage` 读 `day.txt`，`locate_index` 用 `bisect` 二分定位并把非法日期对齐到最近交易日（注释：未来日期需显式 `future=True` 否则抛 IndexError）。`Freq`（`utils/time.py`）统一解析 `day/1min/week/month`，`resam_calendar` 支持从粗频重采样出细频日历。

---

## 3. 策略/因子 API 与扩展方式

### 3.1 策略基类与"信号→仓位"管线

`BaseStrategy`（`qlib/strategy/base.py`）：核心抽象方法 `generate_trade_decision(execute_result) -> BaseTradeDecision`，基础设施由 `level_infra`（trade_calendar、executor）与 `common_infra`（trade_account、trade_exchange）注入。典型实用策略是看重库内置的：

- **`TopkDropoutStrategy`（`qlib/contrib/strategy/signal_strategy.py`）**：基于模型预测打分选股的核心策略。其信号优先级（源码 `get_object`/`generate_trade_decision` 逻辑）：`Signal`（最高，如 `{"BUY": 10, "SELL": 0}` pre-defined 仓位）> `score`（预测分数，TopK 选股 + Dropout 剔除）> `weight`（直接给权重）。常用参数：`topk=50`、`n_drop=5`、`risk_degree=0.95`（预留现金比例）、`only_tradable=True`。
- **`WeightStrategyBase`（`qlib/contrib/strategy/rule_strategy.py`）/ `EnhancedIndexingStrategy`（指数增强）**
- `qlib/backtest/high_performance_ds.py` 提供 `NumpyQuote`、`NumpyOrderIndicator` 等用 numpy 加速行情存取的高性能数据结构；`Exchange.generate_order_for_target_amount_position` 把"目标金额仓位"转成买卖 Order 列表（`random.seed(0)` 固定洗牌顺序保证结果可复现，见注释）。

最小策略结构（写文件后经 `PortAnaRecord`/`executor` config 引用）：

```python
from qlib.strategy.base import BaseStrategy

class MyStrategy(BaseStrategy):
    def generate_trade_decision(self, execute_result=None):
        # 访问 self.trade_position / self.trade_calendar / self.trade_exchange
        # 返回 BaseTradeDecision([Order(...)])
        ...
```

### 3.2 因子表达式与 Alpha158/Alpha360 机制（qlaib 特有）

`Alpha158` / `Alpha360` 定义在 `qlib/contrib/data/handler.py`，因子表达式字符串生成在 `qlib/contrib/data/loader.py`：

- **Alpha360**：近 **60 日**原始价量（OPEN/HIGH/LOW/CLOSE/VWAP/VOLUME 各 60 根，命名 `CLOSE0..59` 等），**全部除以最新值归一化**（`Ref($close,d)/$close`、`$volume/($volume+1e-12)`），所以最新 `CLOSE0=1`。标签 `Ref($close,-2)/Ref($close,-1)-1`。
- **Alpha158**：三类因子——`kbar`（K 线形态 9 个：`KMID/KLEN/KMID2/KUP/KUP2/KLOW/KLOW2/KSFT/KSFT2`）、`price`（默认只 `windows:[0]`，即 OPEN/HIGH/LOW/VWAP 当日/昨日）、`rolling`（滚动窗口算子，窗口默认 `[5,10,20,30,60]`，默认排除 `RANK`，其余 ROC/MA/STD/BETA/RSQR/RESI/MAX/MIN/QTLU/QTLD/RANK/RSV/IMAX/IMIN/IMXD/CORR/CORD/CNTP/CNTN/CNTD/SUMP/SUMN/SUMD/VMA/VSTD/WVMA/VSUMP/VSUMN/VSUMD 全部启用）。默认 `Alpha158` = kbar + price(windows=[0]) + rolling({}) 即可展开出 158 个因子（158 = 9 kbar + 4·2 price/volume + 19 类·5 窗口 rolling 的合计，源码未逐项声明，属公开口径，未能逐字核实确数）。

数据处理 Processor（`qlib/data/dataset/processor.py`，`handler.py` 引用）：
- **学习期** `_DEFAULT_LEARN_PROCESSORS`：`DropnaLabel`（丢 NaN 标签行）→ `CSZScoreNorm`（对 label 做拟合期标准化）；
- **推断期** `_DEFAULT_INFER_PROCESSORS`：`ProcessInf` → `ZScoreNorm` → `Fillna`（特征整体标准化+补 NaN）。
- `DataHandlerLP` 分 `PTYPE_A`/`PTYPE_B` 两阶段：A 先算特征标签再切 train/valid/test（处理器只 fit 训练段、transform 全段，防标准化泄漏）。

---

## 4. 分析/优化能力

### 4.1 绩效评估指标（`qlib/contrib/evaluate.py::risk_analysis`）

返回 `pd.Series` 五指标：`mean`、`std`、`annualized_return`、`information_ratio`、`max_drawdown`。两个关键设计点（源码注释明示）：
- **年化按"求和"而非"复利"**：`annualized_return = mean * N`、`max_drawdown = (r.cumsum() - r.cumsum().cummax()).min()`（`mode="sum"` 默认，`N=252` 日/`238` 为分钟频率基准），注释说明"为避免累计曲线指数型扭曲，Qlib 用求和累积收益"；`mode="product"` 提供几何/复利口径（`(1+r).cumprod()`）。
- `indicator_analysis`：对高频交易指标 `pa`（price advantage）、`pos`（positive rate 胜率）、`ffr`（fulfill rate 成交率）做 mean/amount_weighted/value_weighted 聚合。

### 4.2 组合优化与归因

- `qlib/backtest/profit_attribution.py`：`brinson_pa`、`decompose_portofolio`（收益分解到换手/配置/选择）、`get_stock_group`（按行业/市值分组）、`get_daily_bin_group`（基准分档）；
- `qlib/contrib/report/`（`analysis_model/`）提供 IC 分析、分组收益、绩效归因的可视化缓存函数（供 `graph` 画图）；`qlib/contrib/evaluate_portfolio.py` 提供组合优化（如 Black-Litterman、风险平价规划）。
- **模型动物园**（`examples/benchmarks/`，README 列出 LightGBM/XGBoost/CatBoost/Linear/MLP/LSTM/GRU/ALSTM/GATs/TFT/Transformer/Localformer/TCN/TabNet/IGMTF/HIST/TRA/ADD/ADARNN/KRNN/Sandwich/DoubleEnsemble/TCTS 等），统一 `qlib.contrib.model` 下 `DeepModelWrapper` 封装、`trainer.py` 训练循环。

### 4.3 参数扫描 / 记录 / 实验管理

- `qlib/workflow/record_temp.py`：`SignalRecord`（模型预测输出）、`SigAnaRecord`（`ana_long_short`、`ann_scaler=252`，算 IC/ICIR/RankIC/RankICIR）、`PortAnaRecord`（接回测）、`MultiPassPortAnaRecord`（`pass_num` 次回测、`random_init` 随机重组、`shuffle_init_score`，用于组合稳健性/多次采样消除随机性）。
- `qrun --config <yaml>` + `qlib/workflow/recorder.py`（`QlibRecorder`，artifact 目录 `mlruns`/自定义 `experiment_name`），`exp.py`/`expm.py` 管理实验序列；`qlib/contrib/rolling/` / `examples/model_rolling` 做滚动训练（rolling retrain）。

---

## 5. 绩效产物结构与指标清单

### 5.1 组合层（`PortfolioMetrics`，回测端口逐日记录）

`generate_portfolio_metrics_dataframe()` 输出列（按 `report.py` 定义，注释逐项说明口径）：

| 列 | 含义 |
|---|---|
| `account` | 账户总资产（现金+持仓，按当日 close 计价） |
| `return` | 当日组合收益（**不含交易费**，源码注释明确） |
| `total_turnover` / `turnover` | 累计成交额 / 当日换手率（`now_turnover/last_account_value`） |
| `total_cost` / `cost` | 累计成本(手续费+滑点) / 当日成本率（`now_cost/last_account_value`） |
| `value` | 持仓证券市值（不含现金） |
| `cash` | 现金 |
| `bench` | 基准当日收益（默认 `SH000300` 沪深300，`CSI300_BENCH`） |

账户首日 `return_rate=(now_earning+now_cost)/last_account_value`（`Account.update_portfolio_metrics`，注释 "use earning to calculate return, position's view, earning consider cost"）。

### 5.2 订单/交易层（`Indicator`，`report.py`）

每步聚合指标：`amount`（目标下单量）、`deal_amount`（实际成交量）、`inner_amount`（内层目标量）、`trade_price`（平均成交价）、`trade_value`、`trade_cost`、`trade_dir`、`ffr`（full fill rate）、`pa`（price advantage，`sign*(trade_price/base_price-1)`，`sign=1-trade_dir*2`）、`pos`（胜率 `pa>0` 占比）、以及 `base_price`/`base_volume`（PA 基准，`agg="twap"` 默认 / `"vwap"`，`_get_base_vol_pri` 计算）。`indicator_config` 可开启 `show_indicator`。

### 5.3 风险层（PortAnaRecord 落盘）

实验产物目录（默认 `mlruns/{exp_id}/artifacts/`）含：`report_normal_1day.pkl`（`risk` 五指标 + 年化）、`port_analysis_1day.pkl`、`ic.pkl`/`ric.pkl`、`sig_analysis`、`graph/` 图件；组合明细 `hist_positions`。指标名以上述 `risk_analysis` 五字段 + `SigAnaRecord` 的 IC/ICIR/RankIC/RankICIR 为准（qlib 官方基准报告称 `bench` 为 excess_return_with_cost/excess_return_without_cost，但源码 `risk_analysis` 只产出 `mean/std/annualized_return/information_ratio/max_drawdown`，超额收益需自行对 `return` 列与 `bench` 列差分——此点未能核实官方报告外的确切字段名）。

---

## 6. 坑与注意事项（来自源码注释/docstring）

1. **`$close` 为 None 即视为停牌**：`Exchange.quote_df` 注释——"if `$close` is None, the stock on that day is regarded as suspended"，停牌股 do not trade；策略若误读 NaN 可能静默错误。
2. **`trade_unit` 与复权因子耦合**：`factor` 缺失时自动切复权价模式并**禁用 trade_unit**（有 warning），此时最小手数、零股约束失效，回测成本口径会漂移。
3. **`volume_threshold` 的 `cum`/`current` 语义易错**（`_get_vol_limit` docstring 长说明）：`cum` 是随时间累积值（如 `0.2*DayCumsum($volume,'9:45','14:45')`），撮合时要**减去当日已成交量** `dealt_order_amount`；`current` 是实时值直接当容量上限；二者取 min 聚合。
4. **冲击成本 `impact_cost` 默认 0**：`open_cost/close_cost` 有默认值但 `impact_cost=0.0`，大盘较大订单需手动设（文档推荐 0.1），否则系统性高估收益。
5. **`parallel` vs `serial` 执行的现金冲突**：`TT_PARAL` 注释明确"假设单步内没有不同方向的订单"，若卖买并存会因子层先后顺序产生歧义。
6. **年化收益"求和 vs 复利"口径**：`risk_analysis` 默认 `mode="sum"`，与常见 `(1+r).cumprod()` 口径不同（注释 "implemented by design"），跨框架对比需换算。
7. **`Account` 在嵌套层用浅拷贝**：`position` 全层共享而各层指标独立——写 stratey 依赖"各层独立 metric"假设要理解这一点，否则会读出非预期值（`executor.py` 注释）。
8. **dataset cache 与 inst_processors 冲突**：多处 `raise ValueError`——`DiskDatasetCache`/`ClientDatasetProvider` 在 `disk_cache!=0`（或 `>0`）且带 `inst_processors` 时报错，需 `D.features(disk_cache=0)` 或 `qlib.init(dataset_cache=None)`（`FIXME` 注释亦指出 resample 后 cache 再截断会日期不完整）。
9. **缓存生命周期**：`for dict of instruments` 的 feature 不落缓存（`ClientDatasetProvider` warning "The dict of instruments will be cleaned every day"）。
10. **PIT 字段命名**：必须 `_q`/`_a` 结尾且查询用 `P($$field_q)`，直接查 `$$field_q` 抛 `ValueError`（`LocalPITProvider.period_feature`）。
11. **无 `limit_threshold` 时的告警**：中国区（`REG_CN`/`REG_TW`）不设涨跌停时源码会 warning "The stocks hit the limit may be bought/sold"——回测可能买进涨停/卖出跌停。
12. **并发写缓存竞争**：`DiskExpressionCache._expression` 注释 "Removing the reader lock may result in conflicts"、`CacheUtils.visit` 的 `FIXME` 指出多进程读 meta 可能异常——多进程跑同一 `provider_uri` 建议配套 Redis 锁。
13. **unreleased 破坏性变更**（README 顶部）：源码 HEAD 引入 `restricted_pickle_load` 与 `trusted=True` 门禁（重载模型/数据集/断点续训/DDG-DA 需 opt-in），HIST 映射与高频 cache 路径有迁移要求——用 `main` 源码而非 PyPI 时务必核对版本。

---

## 7. 可借鉴的设计要点

1. **`$表达式` 作为统一数据 DSL**：因子、Alpha158 一律表达式字符串，天然可做 diff、缓存键、权限控制（新版本甚至对表达式做白名单 `restricted`），是把"因子即数据"落到工程的最佳实践。
2. **嵌套执行（NestedExecutor）**：把"日频选股决策 + 分钟频执行"拆成可任意嵌套的策略/执行器对，用 `level_infra`/`common_infra` 注入依赖、`yield from` 让生成器兼具回放与 RL 训练（`collect_data` 兼顾 `track_data`），一套引擎服务回测与强化学习两套场景。
3. **浅拷贝账务**：`copy.copy(Account)` 共享 Position、隔离 Metrics，是"多层指标各算各的、持仓唯一"这类多频回测账务问题的优雅解法。
4. **缓存回退删除的未来数据防护**：缓存增量更新时 `f.truncate(size - ele_size * remove_n)` 把被 `right_extend` 污染的尾部删掉重算——把"前视窗"信息显式写进元数据并在写缓存时兜底。
5. **成本模型工程化**：`min_cost` 与 `cost_ratio` 的临界价分段、冲击成本平方放大、trade_unit/factor 对齐——复杂度集中在 `_calc_trade_info_by_order` 一处，清晰可移植。
6. **实验元数据即产物**：`RecordTemp` 三件套（Signal/SigAna/PortAna）+ `mlruns` 目录约定，让"预测→IC 评估→回测"端到端可复现、可清单化。

## 附：主要源文件索引

| 文件 | 内容 |
|---|---|
| `qlib/__init__.py` | `qlib.init()` 入口，注册 Provider Wrapper |
| `qlib/config.py` | `QlibConfig`/`C` 全局默认参数、DataPathManager |
| `qlib/data/data.py` | Provider 体系与 `D/Cal/Inst/FeatureD/ExpressionD/DatasetD/PITD` |
| `qlib/data/ops.py` | 表达式算子（Ref/Mean/Std/Corr/... 共 54 类） |
| `qlib/data/expression_parser.py` | 表达式解析器 |
| `qlib/data/storage/file_storage.py` | `.bin`/`.txt` 存储，`FileFeatureStorage` 头索引 |
| `qlib/data/cache.py` | `H` 内存缓存、`DiskExpressionCache`、`DiskDatasetCache`（HDF5+index） |
| `qlib/data/pit.py` | PIT 提供者/`P` 算子 |
| `qlib/backtest/backtest.py` | `backtest_loop`/`collect_data_loop` 主循环 |
| `qlib/backtest/executor.py` | `BaseExecutor`/`NestedExecutor`/`SimulatorExecutor` |
| `qlib/backtest/exchange.py` | 撮合、成本、涨跌停/成交量约束、trade_unit/factor |
| `qlib/backtest/account.py` | `Account` + `AccumulatedInfo`（rtn/earnings 口径） |
| `qlib/backtest/position.py` | `BasePosition`/`Position`/`InfPosition`，settle 逻辑 |
| `qlib/backtest/report.py` | `PortfolioMetrics` + `Indicator`（pa/ffr/pos） |
| `qlib/backtest/decision.py` | `Order`/`OrderDir`/`BaseTradeDecision` |
| `qlib/backtest/profit_attribution.py` | brinson 归因、收益分解 |
| `qlib/strategy/base.py` | `BaseStrategy`/`RLStrategy`/`RLIntStrategy` |
| `qlib/contrib/strategy/signal_strategy.py` | `TopkDropoutStrategy`/`WeightStrategyBase` |
| `qlib/contrib/data/handler.py` | `Alpha158`/`Alpha360` 及 learn/infer 处理器默认 |
| `qlib/contrib/data/loader.py` | `Alpha158DL`/`Alpha360DL` 因子表达式展开 |
| `qlib/contrib/evaluate.py` | `risk_analysis`/`indicator_analysis`/`backtest_daily` |
| `qlib/workflow/recorder.py` | `QlibRecorder` 实验目录 |
| `qlib/workflow/record_temp.py` | `SignalRecord`/`SigAnaRecord`/`PortAnaRecord` 等 |