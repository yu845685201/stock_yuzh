# StockSharp（S#，StockSharp/StockSharp）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：https://github.com/StockSharp/StockSharp · **分析依据**：本地克隆源码（`refrences/StockSharp__StockSharp`，commit `61cc0f1`，2026-10-11；产品版本 `5.0.0` 定义于 `props/common_meta.props`；随仓 `AGENTS.md`）· **配套**：../开源股票分析项目清单-20261010.md
> **分析方法**：精读回测仿真器（`Algo.Testing/MarketEmulatorSettings.cs`、`HistoryEmulationConnector.cs`）、订单/状态（`Messages/OrderStates.cs`/`OrderTypes.cs`）、策略基类（`Algo.Strategies/Strategy.cs`）、佣金（`Algo/Commissions/`）、撮合（`MatchingEngine/`）、统计（`Algo/Statistics/`）与 `AGENTS.md`（仓库规范），结论均来自源码注释。

---

## 0. 定位与架构总览

StockSharp（S#）是免费开源 **.NET 量化交易平台**，本仓库是它的**公共内核（API 库 + 引擎）**——券商/交易所连接器在独立仓库 `StockSharp/Connectors`（`AGENTS.md` 明确"Connectors are not in this repo"）。它把"协议消息（167 类）→ 领域实体 → 连接器 → 策略 → 指标 → 回测/实盘 → 设计器/图表"做成同构于实盘的引擎（回测的 `HistoryEmulationConnector` 与实盘 conn 实现同一 `IConnector` 接口）。

技术栈：**C#/.NET 10（主）+ .NET 6（旧）**；三个解决方案 `StockSharp.slnx`（全量含 connectors，仅 Windows）/`StockSharp_CI.slnx`（Windows CI）/`StockSharp_Tests.slnx`（跨平台构建+测试）；版本集中在 `props/common_versions.props`、装配名 `StockSharp.<ProjectName>`（`props/common_meta.props`）。

```
┌──────────────────────────────────────────────────────────────┐
│ UI  Designer(可视化策略设计器) · Charting · Configuration ·    │
│     Reporting · Alerts · Media · Diagram.Core(图编排)          │
├──────────────────────────────────────────────────────────────┤
│ 引擎 Algo                                                        │
│   Connector(IConnector 统一连接) · MarketRuleHelper(市场规则)    │
│   MarketTimer · Commissions(佣金) · Candles(蜡烛/压缩)          │
│   Expressions · Derivatives · Latency · Heartbeat/FillGaps      │
├───────────────┬──────────────────────────────────────────────┤
│     策略       │     回测/仿真      │   撮合引擎                 │
│ Algo.Strategies│ Algo.Testing       │ MatchingEngine             │
│  Strategy(基类)│ HistoryEmulationConn│ IOrderMatcher·IOrderBook  │
│  Optimization │ ector · MarketEmu-  │ MarginController           │
│  Protective   │ lator · Commission  │ EmulatedPortfolioManager   │
├───────────────┴──────────────────────────────────────────────┤
│ 指标 Algo.Indicators(192) · Algo.Export/Import/Compilation      │
│   Algo.Analytics(C#/F#/Py) · Algo.Gpu(GPU)                      │
├──────────────────────────────────────────────────────────────┤
│ 领域层 BusinessEntities(Security/Order/Position/Portfolio/      │
│   Exchange/IConnector/IMarketDataProvider) · Messages(167 类)   │
└──────────────────────────────────────────────────────────────┘
```

---

## 1. 数据接入与存储

- **连接器架构**（`BusinessEntities/IConnector.cs`）：`IConnector` 统一了行情/交易两套下游（`IMarketDataProvider`/`ISubscriptionProvider`/`ITransactionProvider`/`IPortfolioProvider`/`IPositionProvider`/`ISecurityProvider`），实盘券商与回测仿真器都实现它 → **回测与实盘共享整套上层代码**。
- **消息协议**（`Messages/`，167 类）：`SecurityMessage`/`Level1ChangeMessage`/`QuoteChangeMessage`（订阅簿增量）/`CandleMessage`/`ExecutionMessage`/`OrderRegisterMessage`/`ConnectMessage` 等；`BusinessEntities/Security.cs`/`Order.cs`/`Position.cs`/`Portfolio.cs`/`MyTrade.cs` 是领域实体（与消息互转）。
- **数据存储**：S# 的本地数据（`S#.Data` 格式，`SecurityId + MarketData` 二进制/CSV 存盘）与蜡烛压缩在 `Algo/Candles/` 与 `Algo/Storage`（本仓未见独立 Storage 项目，存储后端在关联包，**未能核实**具体文件路径）；`Algo.Export/Import` 负责跨格式导入导出（CSV/MetaTrader/文本）。
- **行情补全/质量**：`Algo/Level1DepthBuilderAdapter.cs`（从 Level1 拼盘口）、`FillGapsMessageAdapter.cs`（补缺口）、`FilteredMarketDepthAdapter.cs`（盘口过滤）、`HeartbeatMessageAdapter.cs`（心跳）、`LookupTrackingMessageAdapter.cs`（证券查找跟踪）。

---

## 2. 回测引擎机制

### 2.1 仿真连接器（`Algo.Testing/HistoryEmulationConnector.cs` + `BaseEmulationConnector`）

`HistoryEmulationConnector` 是回测入口（继承 `BaseEmulationConnector`），注册订单默认用名为 **Simulator** 的组合（"the portfolio with default name Simulator"）。它把历史数据经内置 **`IMarketEmulator`（模拟交易所）** 撮合，逐 bar 回放生成 `ExecutionMessage`/`Level1ChangeMessage`，上层策略无感知地像是在真盘。

### 2.2 撮合与仿真参数（`Algo.Testing/MarketEmulatorSettings.cs`）

成交价粒度 `EmulationCandlePrices`：**`Middle`（中价）/`Open`/`High`/`Low`/`Close`** 五选一（`CandlePrice`）。逐项默认值（均来自源码 XML 注释）：

| 参数 | 默认 | 语义 |
|---|---|---|
| `MatchOnTouch` | `true` | 价格为**触及即撮合**（等值也算成交），注释标注"乐观场景"（关闭则需严格优于订单价） |
| `Failing` | `0` | 新订单注册失败百分比（0=无失败，0-100） |
| `Latency` | `TimeSpan.Zero` | 订单注册最小延迟（0=即时被交易所接收） |
| `SpreadSize` | `2` | 从 tick/K 线合成盘口时的价差（价格档数），"双边都挂成交价"用 0 |
| `MaxDepth` | `5` | 从 tick 生成的盘口最大档数（无历史盘口时） |
| `PriceLimitOffset` | `40%` | 无 Level1 信息时，下一 session 涨跌停边界（相对上一笔 ±40%） |
| `IncreaseDepthVolume` | `true` | 大单挂出时向盘口增补量 |
| `IsSpreadCentered` | `false` | 价差是否对半均分（关闭则价差整体置于价格一侧） |
| `CheckTradingState`/`CheckMoney`/`CheckShortable`/`CheckTradableDates` | — | 交易时段/资金/可做空/交易日校验 |
| `CommissionRules` | `IEnumerable<ICommissionRule>` | 佣金规则组合 |

### 2.3 佣金规则（`Algo/Commissions/`，14 类）

`CommissionRule` 抽象基类 + `ICommissionRule`，按维度拆成一组可组合规则：`CommissionOrderRule`（按单）、`CommissionTradeRule`（按成交）、`CommissionOrderCountRule`/`TradeCountRule`（按笔数）、`CommissionOrderVolumeRule`/`TradeVolumeRule`（按量）、`CommissionTradePriceRule`（按价）、`CommissionTurnOverRule`（按成交额比例，典型券商费）、`CommissionSecurityTypeRule`/`CommissionSecurityIdRule`/`CommissionBoardCodeRule`（按证券类型/代码/交易所板），由 `CommissionManager`/`CommissionMessageAdapter` 组装执行。

### 2.4 订单模型（`Messages/OrderStates.cs` / `OrderTypes.cs` / `BusinessEntities/Order.cs`）

- `OrderStates`：`None`（未发送）→ `Active`（交易所接受并可成交）→ `Done`（完全成交/已撤销）；分支 `Failed`（拒绝）、`Pending`（待登记）；
- `OrderTypes`：`Limit`（限价）、`Market`（市价）、`Conditional`（条件单 = 止损/止盈）；
- `OrderCondition`（`Algo.Testing/EmulationOrderCondition.cs` 等）承载条件触发语义。

### 2.5 撮合引擎（`MatchingEngine/`）

独立子项目实现订单簿撮合：`IOrderMatcher`（匹配算法）、`IOrderBook`（订单簿）、`IOrderLifecycleManager`（订单生命周期）、`IStopOrderManager`（止损单）、`IMarginController`/`MarginController`（保证金）、`EmulatedPortfolioManager`（组合）、`MatchFill`（撮合成交）。

---

## 3. 策略 API 与扩展方式

### 3.1 策略基类（`Algo.Strategies/Strategy.cs`，partial 类）

`Strategy` 是 partial 拆分的大型基类（`Strategy_Parameters.cs`/`Strategy_Positions.cs`/`Strategy_MarketRulesAndMisc.cs`/`Strategy_HighLevelSubscriptions.cs`…十几个文件），构造时装配：

- **`PnLManager`**（`new PnLManager { UseOrderBook = true }`，`Algo/PnL/`）——按盘口或成交计算盈亏；
- **`StatisticManager`**（`Algo/Statistics/`）——绩效指标容器；
- `OrderProcessor`（`OrderPipeline.cs` 订单管线）、`Trades`（`TradePipeline.cs`）、`Positions`（`PositionPipeline.cs`）、`Subscriptions`（`SubscriptionRegistry.cs`）、`RiskManager`（`Algo/Risk/`）、`Parameters`（`StrategyParam`，`Strategy_Parameters.cs`）；
- **保护单** `ProtectiveController`（`Protective/`）：`TakeProfit`/`StopLoss`/`TrailingStop`（`_takeProfit/_stopLoss/_isStopTrailing`），本地止损 `_isLocalStop` 与 `_protectiveUseMarketOrders`；
- **生命周期状态机** `Engine`（`StrategyEngine.cs`：`Stopped→Starting→Running→Stopping→Stopped`），`CanTrade`/`RequestStop`/`_stopping` 门控下单。

回调：`OnStarted`/`OnStopped`/`ProcessMessage`/`OnOrderRegistered`/`OnNewMyTrade`/`OnPositionChanged`/`OnSecurityChanged`/`OnTick`/`OnCandle`；下单 `RegisterOrder`/`Buy`/`Sell`（经 `Volume` 属性 + `BuyVolume/SellVolume`，`WithVolume` 语义）。`Strategy` 也实现 `IPositionProvider`/`IMarketDataProvider`，可被别的策略当数据源嵌套。

### 3.2 指标（`Algo.Indicators/`，192 个）+ 规则

`BaseIndicator` 派生 192 个技术指标（`Sma`/`Ema`/`Macd`/`Rsi`/`BollingerBands`/`Atr`/`Adx`/`Ichimoku`…，其中 `Algo.Gpu` 提供 GPU 加速版本）；`Algo/MarketRuleHelper*.cs`（`MarketRuleHelper.cs`/`_Candle`/`_Order`/`_Position`/`_Time`/`_Subscription`）用声明式 `IMarketRule` 做"条件→动作"（如"价格破位→下单"），这是 S# 的规则引擎。

---

## 4. 分析/优化能力

### 4.1 统计（`Algo/Statistics/`，参数化指标族）

`StatisticManager` 聚合一组 `BaseStatisticParameter`（`IStatisticManager` 传入 `commission`)：

| 类别 | 参数（类名后缀 `..Parameter`） |
|---|---|
| 收益 | `NetProfit`、`NetProfitPercent`、`GrossProfit`、`GrossLoss`、`ProfitFactor`、`Return` |
| 回撤/风险 | `MaxDrawdown`、`MaxDrawdownPercent`、`MaxRelativeDrawdown`、`AverageDrawdown`、`RecoveryFactor`（净利/最大回撤）、`SterlingRatio`、`SharpeRatio`（`RiskAdjustedRatioParameter`）、`SortinoRatio`（同类） |
| 交易/持仓 | `WinRate`/`LossRate`、`TradeCount`、`RoundtripCount`（往返）、`OrderCount`、`PositionCount`、`AverageHoldingTime`、`TimeInMarket`、`MaxPosition`/`MaxShortPosition`、`MaxProfit`/`MaxProfitDate` |
| 订单质量 | `Commission`、`OrderRegisterErrorCount`/`OrderInsufficientFundErrorCount`/`OrderCancelErrorCount`、`Min/MaxLatencyRegistration`/`Cancellation` |

统计基类分 `ITradeStatisticParameter`/`IOrderStatisticParameter`/`IPositionStatisticParameter`/`IPnLStatisticParameter`，与 `TradeBuilder`（成交配对）联动。

### 4.2 参数优化与产品化

- **优化器**（`Algo.Strategies/Optimization/`）：`StrategyParam` 定义参数空间，网格/暴力搜索（配 `StrategyParamHelper`）；
- **编译/分析**：`Algo.Compilation`（C#/Python 策略代码动态编译即用）、`Algo.Analytics`（C#/F#/Python 统计分块，如 `Analytics` 找最优参数组合）；
- **可视化/报告**：`Designer`（可视化拖拽策略编排 + 回测）、`Charting.Interfaces`（K 线/指标绘图协议）、`Reporting`（交易报告生成）、`Configuration`（连接器配置）。
- **导出导入**：`Algo.Export`/`Algo.Import`（策略/数据/配置跨平台迁移）。

---

## 5. 绩效产物结构与指标清单

回测产出经 `IStatisticManager` 得到 §4.1 全表参数；`Designer`/`Reporting` 生成净值曲线、回撤、逐笔成交、盈亏分布等可视化产物；`PnLManager`（`UseOrderBook=true` 时按盘口档计盈亏）产出实时/历史 `PnLInfo`。`Alerts.Interfaces` 与 `Algo.Statistics` 的结果可导出报表。

---

## 6. 坑与注意事项（来自源码注释/AGENTS.md）

1. **`MatchOnTouch` 默认 `true` 是"乐观场景"**：源码注释直说"On by default (optimistic scenario)"——触及即成交会高估限价单成交概率，严谨回测应关闭。
2. **connectors 不在本仓库**：`AGENTS.md` 强调"Add or fix a broker/exchange connector in StockSharp/Connectors, not here"；且本仓库目录名必须叫 `StockSharp (GitHub)`、`../Connectors` 必须同级存在，否则 `StockSharp.slnx`/`StockSharp_CI.slnx` 引用失效（Windows 专属构建）。
3. **`StockSharp.slnx`/`StockSharp_CI.slnx` 只在 Windows 构建**：跨平台只能过 `StockSharp_Tests.slnx`（`AGENTS.md` "For anything meant to work cross-platform, build/test through StockSharp_Tests.slnx"）。
4. **三个 README 需同步**：`README.md`/`README.ru.md`/`README.zh.md` 由 `scripts/check-readme-connectors.py`/`check-readme-doc-links.py` 校验；改连接器清单或文档链接必须三份同步。
5. **本地化字符串别手改**：`Localization/strings.json`（英）+ 38 语言必须保持同键同占位符（`{0}`），改后跑 `sort-localization-strings.ps1` 规整，否则 CI `validate` job 直接挂（一个法文/乌兹别克文裸撇号 `'` 就触发 `UNSORTED`）。
6. **回测无盘口历史时靠参数造盘口**：`SpreadSize=2`/`MaxDepth=5`/`IncreaseDepthVolume`/`PriceLimitOffset=40%` 都是"没有历史盘口/Level1 时构造盘口"的近似（源码注释明示），逐笔策略的撮合精度取决于这些合成假设。
7. **`Failing`/`Latency` 默认关闭**：订单注册失败率与延迟模拟默认 0，不开启则无法反映成交不确定性与时延——与实盘差距需自行用这两个参数补。
8. **`PnLManager.UseOrderBook=true` 的盈亏口径**：按盘口档计盈亏会受合成盘口（§6.6）影响；纯 K 线回测可切 `UseOrderBook=false` 改按成交价计。
9. **.NET 10/.NET 6 双目标**：`Tests` 项目只 `net10.0`，CI 用 `dotnet test --filter <Name>` 加超时跑；本地开发环境 SDK 版本需与 `props/common_versions.props` 的 `NetTfm` 对齐。

## 7. 可借鉴的设计要点

1. **"回测即连接器"**：回测仿真器与实盘券商实现同一个 `IConnector`，策略/指标/统计/风控零改动切换——比 Lean 更进一步，把"环境差异"彻底收进连接器抽象。
2. **仿真参数显式成"乐观/近似"标注**：`MatchOnTouch`（乐观场景）、`PriceLimitOffset`（无 Level1 时）、`IncreaseDepthVolume`（大单补量）都在注释中标明近似性与触发条件，撮合假设可审计。
3. **佣金规则原子化组合**：14 个 `ICommissionRule` 按"订单/成交/笔数/量/金额/证券维度"正交拆解，券商费用表任意组合，无需整包替换。
4. **partial 类 + 按职责拆文件**：`Strategy` 拆十余个 partial 文件（`_Parameters`/`_Positions`/`_MarketRulesAndMisc`/`_HighLevelSubscriptions`…），巨类可维护。
5. **参数化统计族**：`StatisticParameter` 用类型系统区分 trade/order/position/PnL 维度，`RecoveryFactor`/`SterlingRatio` 等由底层参数组合派生——指标可插拔、可复算、可序列化。

## 附：主要源文件索引

| 文件 | 内容 |
|---|---|
| `AGENTS.md` | 仓库结构/构建/约定/CI 校验（本分析依据之一） |
| `Algo.Testing/HistoryEmulationConnector.cs` / `BaseEmulationConnector.cs` | 历史回测连接器 |
| `Algo.Testing/MarketEmulatorSettings.cs` | 撮合仿真参数（成交价/触及/价差/涨跌停/佣金） |
| `Algo.Testing/EmulationMessageAdapter.cs` / `MarketEmulatorAdapter.cs` / `IMarketEmulator.cs` | 模拟交易所消息适配 |
| `Algo/Commissions/CommissionRule.cs` + `Commission*Rule.cs` | 佣金规则（抽象 + 14 派生） |
| `MatchingEngine/IOrderMatcher.cs` / `IOrderBook.cs` / `MarginController.cs` | 订单簿撮合/保证金 |
| `Algo.Strategies/Strategy.cs`（+ partial 文件） | 策略基类（PnL/统计/风控/保护单/状态机） |
| `Algo.Strategies/Optimization/` | 参数优化 |
| `Algo.Indicators/` (192) / `Algo.Gpu/` | 技术指标与 GPU 版 |
| `Algo/Statistics/*Parameter.cs` / `IStatisticManager.cs` | 统计参数族 |
| `Algo/MarketRuleHelper*.cs` | 市场规则引擎 |
| `BusinessEntities/{Security,Order,Position,Portfolio,Exchange}.cs` / `IConnector.cs` | 领域实体与连接器接口 |
| `Messages/OrderStates.cs` / `OrderTypes.cs` / `OrderCondition.cs` | 订单状态/类型/条件 |
| `props/common_versions.props` / `common_meta.props` | 版本与装配命名集中管理 |
| `Designer/`、`Charting.Interfaces/`、`Reporting/`、`Diagram.Core/` | 设计器/图表/报告/图编排 |
| `Algo.Compilation/`、`Algo.Analytics*/`、`Algo.Export/Import` | 编译/分析/导入导出 |