# ABU（阿布量化，bbfamily/abu）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：https://github.com/bbfamily/abu · **分析依据**：本地克隆源码（`refrences/bbfamily__abu`，commit `d602d84`，2026-01-24；GPL-3.0，作者"阿布"，配套《量化交易之路》/《机器学习之路》）· **配套**：../开源股票分析项目清单-20261010.md
> **分析方法**：精读买入因子基类（`FactorBuyBu/ABuFactorBuyBase.py`）、资金（`TradeBu/ABuCapital.py`）、佣金（`TradeBu/ABuCommission.py`）、滑点（`SlippageBu/`）、仓位（`BetaBu/`）、绩效（`MetricsBu/ABuMetricsBase.py`）、决策框架（`UmpBu/`）、数据源（`MarketBu/ABuDataSource.py`+`CoreBu/ABuEnv.py`），结论均来自源码注释。

---

## 0. 定位与架构总览

阿布量化是面向 A 股/美股/港股/期货/加密货币的**中文量化回测教学框架**，设计哲学是"**因子化 + 裁判化**"：买入择时因子（`FactorBuyBu`）→ 趋势线（`TLineBu`）→ 选股因子（`PickStockBu`）→ 资金与仓位（`BetaBu`）→ 滑点/佣金 → 回测网络（`TradeBu`/`MetricsBu`）→ 机器学习裁判（`UmpBu` 用 sklearn 分类器做买卖决策）。核心对象是**上传因子对象 + `kl_pd`（K 线 DataFrame）+ `capital` 类**三件套。

模块图（`abupy/` 各子包）：

```
┌──────────────────────────────────────────────────────────────┐
│ 决策层 UmpBu  —— ump"主裁/边裁"(Main/Edge × Deg/Full/Mul/       │
│   Price/Wave/Jump) 用 sklearn 分类器给买卖信号投票             │
├──────────────────────────────────────────────────────────────┤
│ 策略层 FactorBuyBu(买入因子) FactorSellBu(卖出) PickStockBu(选股) │
│   TLineBu(趋势线) IndicatorBu(技术指标 ND 归一化系列)            │
├──────────────────────────────────────────────────────────────┤
│ 回测层 TradeBu(执行/订单/基准/佣金) BetaBu(ATR/Kelly 仓位)       │
│   MetricsBu(绩效/网格搜索/交叉验证) SlippageBu(滑点)             │
├──────────────────────────────────────────────────────────────┤
│ 数据层 MarketBu(数据源/缓存/标的) · CrawlBu(爬虫) RomDataBu      │
├──────────────────────────────────────────────────────────────┤
│ 平台层 CoreBu(环境/基础) UtilBu(工具) CheckBu AlphaBu MLBu DLBu  │
│   WidgetBu(UI) · abupy_ui(非编程界面) · abupy_lecture(教程)     │
└──────────────────────────────────────────────────────────────┘
```

---

## 1. 数据接入与存储

### 1.1 数据源（`MarketBu/ABuDataSource.py` + `CoreBu/ABuEnv.py`）

数据源用枚举 `EMarketSourceType` 绑定抓取实现类（`source_dict`）：`E_MARKET_SOURCE_bd→BDApi`、`tx→TXApi`、`nt→NTApi`、`sn_us→SNUSApi`（新浪美股）、`sn_futures→SNFuturesApi`/`sn_futures_gb→SNFuturesGBApi`（新浪期货）、`hb_tc→HBApi`（币类）。全局默认 `g_market_source = E_MARKET_SOURCE_bd`（注释说明各 source 对应 A 股/美股/期货/币类的网络/本地抓取；底层第三方接口具体名称源码未逐一标明，**未能核实** bd/tx/nt 的确切终端名）。

市场目标 `EMarketTargetType`：`us`（美股，默认）、`hs`（沪深）、`hk`、`futures_cn`（国内期货）、`futures_global`、`options_us`、`tc`（币）。`g_data_fetch_mode` 三种取数模式 `E_DATA_FETCH_NORMAL/FORCE_LOCAL/FORCE_NET`（正常链路 / 强制本地 / 强制网络）。

### 1.2 数据对象与缓存

- `AbuDataFeed`（`MarketBu/ABuDataFeed.py`）把原始抓取数据转成标准 `kl_pd`——列含 `date/open/high/low/close/volume/atr21/atr14` 等（`atr21` 为 21 日 ATR，`AbuCapital` 里直接引用 `kl_pd['atr21']`）；
- `AbuDataCache`（`MarketBu/ABuDataCache.py`）本地缓存；`AbuSymbol.Symbol` 系列（`ABuSymbolStock`/`ABuSymbolFutures`/`ABuSymbolPd`）封装标的代码/交易所；
- `ABuHkUnit`（港股每手股数）、`ABuIndustries`（行业）提供交易单位与行业维度；`ABuNetWork` 统一网络访问。

---

## 2. 回测引擎机制

### 2.1 资金与账务（`TradeBu/ABuCapital.py`）

`AbuCapital(init_cash, benchmark, user_commission_dict)`：按基准时间序列造一张**逐交易日资金表 `capital_pd`**（列 `cash_blance`、`stocks_blance`、`atr21`、`date`；首行 `cash_blance=read_cash`，其余 NaN 逐步回填）。每加一只交易标的，`init_k_line(a_symbol)` 在 `capital_pd` 上追加 4 列：`{symbol}_call_keep`（买涨持仓）、`{symbol}_call_worth`（买涨市值）、`{symbol}_put_keep`、`{symbol}_put_worth`——**多空（call/put）分列记账**，是这套框架的交易模型特征。佣金由 `AbuCommission` 实例化（可按 symbol 配置 `user_commission_dict`），滑点与盈亏由 `apply_k_line` 逐日 `apply` 回填。持仓按 `atr21` 参与仓位计算。

### 2.2 佣金（`TradeBu/ABuCommission.py`，函数级、口径可精确复现）

| 市场 | 函数 | 口径 |
|---|---|---|
| 美股 | `calc_commission_us(trade_cnt, price)` | 每股 `0.01`，最低消费 `2.99` |
| A股 | `calc_commission_cn` | 印花税万 3（`cost*0.0003`）+ 佣金万 2.5（`cost*0.00025`，单边最低 5） |
| 港股 | `calc_commission_hk` | 印花税千 1（`0.001`）+ 佣金千 2（`0.002`） |
| 币类 | `calc_commission_tc` | 双向千 2（`0.002`） |
| 中国期货 | `calc_commission_futures_cn` | 按商品查 `min_unit`（每手数量）与每手手续费，`commission = trade_cnt/min_unit*commission_unit` |

### 2.3 滑点（`SlippageBu/`）

`AbuSlippageBuyBase`/`AbuSlippageSellBase` 抽象买卖滑点，默认 `AbuSlippageBuyMean`（买入均值滑点）/`AbuSlippageSellMean`。买入因子基类 `_slippage_class_init` 默认 `kwargs.pop('slippage', AbuSlippageBuyMean)`——**默认按买入方向滑点**，可注入自定义滑点类。

### 2.4 仓位管理（`BetaBu/`）

买入因子默认 `position_class = AbuAtrPosition`（`ABuAtrPosition.py`，基于 ATR 波动率的仓位）；`ABuPositionBase.g_default_pos_class` 可全局覆盖；DIct 形式 `{'class': AbuAtrPosition, 'atr_base_price': 20, 'atr_pos_base': 0.5}` 传参（`_position_class_init` 里 pop `class` 后其余作为构造 kwargs）。另有 `AbuKellyPosition`（凯利公式仓位）、`AbuPtPosition`。

### 2.5 撮合/执行与订单

`TradeBu/ABuTradeExecute.py`：`calc_simple_profit(orders, kl_pd)`、`make_orders_pd`（订单→成交明细表，字段 `symbol/buy_date/buy_price/sell_date/sell_price/sell_type/sell_type_extra` 等）、`transform_action`（订单→动作表）、`apply_action_to_capital`（把动作应用到资金表）。`ABuOrder.py` 定义订单对象与 `OrderMarket` 等；`AbuBenchmark`（`TradeBu/ABuBenchmark.py`）做基准（含 `kl_pd`）。

---

## 3. 策略/因子 API 与扩展方式

### 3.1 买入因子基类（`FactorBuyBu/ABuFactorBuyBase.py`）——整框架的核心抽象

```python
class AbuFactorBuyBase(metaclass=ABCMeta, base=AbuParamBase):
    def __init__(self, capital, kl_pd, combine_kl_pd, benchmark, **kwargs):
        self.kl_pd = kl_pd            # 择时区间 K 线
        self.combine_kl_pd = combine_kl_pd  # 合并此前一年数据（给 ML 特征用）
        self.capital = capital
        self.benchmark = benchmark
        self._slippage_class_init(**kwargs)   # 默认 AbuSlippageBuyMean
        self._position_class_init(**kwargs)   # 默认 AbuAtrPosition
        self.ump_manger = AbuUmpManager(self) # ump 决策入口
        self.factor_name = self.__class__.__name__
        self.skip_days = 0          # 忽略交易日（因子冷却）
        self.lock_factor = False    # 是否被附着选股因子锁死
        self._init_self(**kwargs)   # 子类自有构造
```

约束（源码 docstring 明示）：**每个买入因子必须混入且只能混入一个方向 mixin**——`BuyCallMixin`（`buy_type_str='call'`、`expect_direction=1.0`，看涨）或 `BuyPutMixin`（`'put'`、`-1.0`，看跌），"一个因子不能同时又看涨又看跌"。因子内部可容纳专属卖出因子与选股因子（对本源生效、择时周期内月度/周度动态选股）。内置买入因子：`ABuFactorBuyBreak`（突破）、`ABuFactorBuyTrend`（趋势）、`ABuFactorBuyDM`、`ABuFactorBuyWD`（形态）等；卖出因子在 `FactorSellBu/`，选股在 `PickStockBu/`，趋势线在 `TLineBu/`（`ABuTLine`/`AbuTL` 系列）。

### 3.2 指标（`IndicatorBu/`）

`ABuNDBase` 是"归一化数据"（ND）基类，衍生 `ABuNDMa`（均线）、`ABuNDMacd`、`ABuNDRsi`、`ABuNDBoll`、`ABuNDAtr`——把指标值与价格**归一化到可比尺度**后再喂给 ump 分类器，这是阿布"机器学习择时"的预处理特色。

### 3.3 决策框架 Ump（`UmpBu/`）

ump = "裁判"：`AbuUmpBase`+`BuyUmpMixin`/`SellUmpMixin` 区分买卖裁判；`AbuUmpMainBase`（主裁）与 `AbuUmpEdgeBase`（边裁）各派生 6 类：`Deg`（角度）/`Full`（全量）/`Mul`（多）/`Price`（价格）/`Wave`（波浪）/`Jump`（跳空），用 sklearn 分类器（`CachedUmpManager` 缓存训练好的 `dump_file_fn()` pickle 模型）在买卖点做拦截/放行投票（`predict(x, need_hit_cnt=1)` 命中即 1）。`AbuUmpManager` 是因子对外统一入口。

---

## 4. 分析/优化能力

### 4.1 绩效指标（`MetricsBu/ABuMetricsBase.py`）

`AbuMetricsBase.fit_metrics` 流程 `_metrics_base_stats → _metrics_sell_stats → _metrics_action_stats → _metrics_extend_stats`，落盘字段：

- 基准/策略夏普：`benchmark_sharpe`、`algorithm_sharpe`（`stats.sharpe_ratio(...)`）；
- 卖出统计：`orders_pd['profit_cg'] = profit/(...)`、`profit_cg_hunder`（百分化）;
- **胜率/盈亏比**：`win_rate`、`gains_mean`（盈利单均值）、`losses_mean`（亏损单均值）、`win_loss_profit_rate`（= `-round(win_sum/loss_sum, 4)`，注意取负号）、`all_profit`（总盈亏）；`plot_order_returns_cmp` 打印"胜率/平均获利期望/盈亏比/所有交易收益比例和/所有交易总盈亏和"。

### 4.2 参数搜索与交叉验证（`MetricsBu/`）

- `ABuGridSearch` / `ABuGridHelper`：因子参数网格搜索（`ABuGridSearch`）；
- `ABuCrossVal`：交叉验证；`ABuMetricsScore`：评分；`ABuMetricsTC`（币类指标）、`ABuMetricsFutures`（期货指标）为分市场子类。
- `AlphaBu`（alpha 组合）、`ABuTradeDrawer`（交易绘图）、`ABuMLFeature`（机器学习特征）、`MLBu`/`DLBu`（机器学习/深度学习封装）。

---

## 5. 绩效产物结构与指标清单

`AbuMetricsBase` 汇集（`fit_metrics` 后可用属性）：`win_rate`（胜率）、`gains_mean`/`losses_mean`（平均盈/亏比例）、`win_loss_profit_rate`（盈亏比）、`all_profit`（总盈亏）、`benchmark_sharpe`/`algorithm_sharpe`、`order_has_ret`（有收益订单表，列 `buy_date/profit/profit_cg/profit_cg_hunder`）、`orders_pd`（全订单成交明细）、`action_pd`（动作表）、`capital.capital_pd`（逐日资金表：`cash_blance/stocks_blance/atr21/{symbol}_call_keep/_call_worth/_put_keep/_put_worth`）。`AbuMetricsScore`/`ABuGridHelper` 额外产出评分与网格结果表。

---

## 6. 坑与注意事项（来自源码注释/docstring）

1. **资金表列名拼写**：`cash_blance` 是**拼写错误保留**的历史列名（`blance`，非 balance），下游引用必须照抄，`ABuCapital.__init__` 里就是 `np.NAN * kl_pd.shape[0]` 与 `'cash_blance'`。
2. **`win_loss_profit_rate` 带负号**：`_metrics_sell_stats` 里 `-round(profit_cg_win_sum / profit_cg_loss_sum, 4)`，且"盈利和为零则把盈利和设 0.01、亏损和为零则把盈利和设 0.01"（防除零）——盈亏比的符号与补零口径需按源码理解。
3. **`commission` 消费默认按市场函数分派**：美股最低 2.99、A股佣金最低 5、印花税万 3，是**单边**计费；期货按 `min_unit/commission` 查表，币类双向千 2（源码注释"如需更精确请自定义 `calc_commission_*`"）。
4. **`skip_days` 是因子冷却**：`AbuFactorBuyBase.skip_days=0`（忽略交易日数），不是通用等待量，需因子自身实现"跳过 N 天再买"逻辑。
5. **`lock_factor` 只由附着选股因子改变**：源码注释"此值只通过本源择时买入因子附属的选股因子进行改变"，不要手工锁死后困惑为何不生效。
6. **数据源线上接口易失效**：阿布历史默认接网易/新浪等公开抓取，A 股源随接口调整经常失效（这是公开接口型项目的通病），代码支持 `g_private_data_source` 私有源与 `FORCE_LOCAL` 强制本地缓存。
7. **Ump 裁判必须先 fit 再拦截**：`CachedUmpManager.get_ump` 在 pickle 不存在时抛 `RuntimeError('you must first fit orders, or ... is not exist')`——必须先训练 ump 才能用 ump 决策入口。
8. **默认市场目标是美股**：`g_market_target = E_MARKET_TARGET_US`，做 A 股必须显式 `AbuMarket`/`ABuEnv` 切到 `hs`（或 `futures_cn`），否则取数/佣金/单位全部按美股口径。

## 7. 可借鉴的设计要点

1. **因子化的"买卖 + 方向 mixin"约束**：用 `BuyCallMixin`/`BuyPutMixin` 强制每个因子单方向、`expect_direction` 归一化——从类型层面杜绝"同因子多空混用"，中文教学框架里少见的严谨。
2. **call/put 分列记账**：资金表为每个标的开 `_call_keep/_call_worth/_put_keep/_put_worth` 四列，天然支持多空、期权/期货方向经营，无需在订单里复杂状态机。
3. **佣金"函数即模型"并可替换**：`AbuCommission(user_commission_dict)` 把佣金做成可传参替换的纯函数表，A/美/港/币/期货口径逐市场显式，正因如此本文档能逐条列公式。
4. **"裁判（ump）"投票制决策**：把机器学习分类器包装成"主裁/边裁"多模型投票（`need_hit_cnt` 阈值），结合 ND 归一化指标——把 ML 信号与规则因子解耦、可独立训练缓存，是可复用的"规则+ML 混合决策"范式。
5. **`{symbol}` 拼列名支持动态多标的**：`capital_pd` 按 symbol 动态加列，`apply_init_kl` 用 `set(action_pd.symbol)` 去重追加，多标的回测无需固定 schema。

## 附：主要源文件索引

| 文件 | 内容 |
|---|---|
| `abupy/CoreBu/ABuEnv.py` | 全局环境：市场目标/数据源/取数模式枚举 |
| `abupy/MarketBu/ABuDataSource.py` | 数据源绑定（bd/tx/nt/sn_us/sn_futures/hb_tc） |
| `abupy/MarketBu/ABuDataFeed.py` / `ABuDataCache.py` | 数据喂入 `kl_pd`（date/OHLCV/atr21）/缓存 |
| `abupy/MarketBu/ABuSymbolStock.py` / `ABuSymbolFutures.py` | 股票/期货标的封装 |
| `abupy/FactorBuyBu/ABuFactorBuyBase.py` | 买入因子基类（方向 mixin/滑点/仓位/ump） |
| `abupy/FactorBuyBu/ABuFactorBuyBreak.py` 等 | 突破/趋势/形态等买入因子 |
| `abupy/FactorSellBu/`、`PickStockBu/`、`TLineBu/` | 卖出因子/选股因子/趋势线 |
| `abupy/IndicatorBu/ABuNDBase.py` 及派生 | 归一化技术指标（MA/MACD/RSI/BOLL/ATR） |
| `abupy/TradeBu/ABuCapital.py` | 资金表与 call/put 分列记账 |
| `abupy/TradeBu/ABuCommission.py` | 分市场佣金函数 |
| `abupy/TradeBu/ABuTradeExecute.py` | 订单→成交→资金回填执行 |
| `abupy/SlippageBu/ABuSlippageBuyMean.py` 等 | 买卖滑点 |
| `abupy/BetaBu/ABuAtrPosition.py` / `ABuKellyPosition.py` | ATR/凯利仓位 |
| `abupy/MetricsBu/ABuMetricsBase.py` | 绩效指标（胜率/盈亏比/夏普） |
| `abupy/MetricsBu/ABuGridSearch.py` / `ABuCrossVal.py` | 网格搜索/交叉验证 |
| `abupy/UmpBu/ABuUmpBase.py` / `ABuUmpManager.py` | ump 决策主框架 |
| `abupy/UmpBu/ABuUmpMainBase.py` / `ABuUmpEdgeBase.py` | 主裁/边裁（Deg/Full/Mul/Price/Wave/Jump） |