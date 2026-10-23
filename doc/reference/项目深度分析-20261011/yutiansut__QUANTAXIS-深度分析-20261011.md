# QUANTAXIS（yutiansut/QUANTAXIS）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：https://github.com/yutiansut/QUANTAXIS · **分析依据**：本地克隆源码（`refrences/yutiansut__QUANTAXIS`，commit `232aa7a`，2026-09-18；MIT 协议，作者 yutiansut）· **配套**：../开源股票分析项目清单-20261010.md
> **分析方法**：精读账户/撮合（`QUANTAXIS/QIFI/QifiAccount.py`、`QAMarket/`）、Rust 回测桥（`QARSBridge/`）、数据源（`QAFetch/QATdx.py` 等）、数据结构（`QAData/`），结论均来自源码注释。

---

## 0. 定位与架构总览

QUANTAXIS 是中文圈**数据接入覆盖面最广的开源量化框架之一**：以 **MongoDB/ClickHouse** 为数据中心，以 **QIFI（Quantaxis Integrated Financial Interface）协议**统一"股票/期货/期权/加密"账户语义，并在近期引入 **Rust 回测内核（qars3）** 加速。定位是"数据 + 回测 + 实盘账户 + Web/CLI"一体化底座，上游用户面向二次开发。

模块结构（`QUANTAXIS/`）：

```
┌──────────────────────────────────────────────────────────────┐
│ 入口/服务  QAWebServer · QACmd · QASchedule · QAPubSub        │
├──────────────────────────────────────────────────────────────┤
│ 账户/撮合  QIFI/QifiAccount.py (QIFI 协议统一账户)             │
│   QAMarket/ QA_Position · QA_Order · market_preset            │
├──────────────────────────────────────────────────────────────┤
│ 回测       QARSBridge/ (Rust qars3: QARSBacktest·QARSStrategy) │
│   QAStrategy/(qactabase/qafactorbase/qahedgebase/qamultibase) │
├──────────────────────────────────────────────────────────────┤
│ 数据      QAData/(QADataStruct·base_datastruct·data_fq 复权)   │
│   QAFetch/(QATdx·QATushare·QABaostock·QAJQdata·QAEastMoney·   │
│            QAThs·QAHexun·币安/火币/OKEx·QAClickhouse)          │
│   存储: MongoDB(quantaxis.*) / ClickHouse                      │
├──────────────────────────────────────────────────────────────┤
│ 分析      QAAnalysis · QAFactor · QAIndicator · QASU · QAUtil  │
└──────────────────────────────────────────────────────────────┘
```

---

## 1. 数据接入与存储

### 1.1 数据源（`QAFetch/`，20+ 路）

| 文件 | 数据源 | 说明 |
|---|---|---|
| `QATdx.py`/`QATdx_adv.py` | **pytdx 通达信**（`pytdx.hq TdxHq_API`，端口 7709） | 股票/指数/期货日线分钟线；`select_best_ip()` 从 `stock_ip_list`/`future_ip_list` 选最优行情服务器，带 `retrying retry` |
| `QATushare.py` | Tushare（含 `save_tusharepro_pg.py` PG 版） | 财务/行情 |
| `QABaostock.py` | baostock | 免费 A 股 |
| `QAJQdata.py` | 聚宽 jqdata | 需账号 |
| `QAEastMoney.py` / `QAThs.py` / `QAHexun.py` / `QAKQ.py` | 东方财富/同花顺/和讯/宽客 | 公开接口抓取 |
| `QAbinance.py`/`QAhuobi.py`/`QAOKEx.py`/`QABitfinex.py`/`QABitmex.py` | 币安/火币/OKEx/Bitfinex/Bitmex | 加密货币 |
| `QAClickhouse.py` | ClickHouse | 列存后端 |
| `QACrawler.py` / `QAQuery.py`/`QAQuery_Async.py` | 爬虫/查询层 | 统一查询 |

### 1.2 存储引擎与数据结构

- **MongoDB** 是主存储：库 `quantaxis`（历史：`stock_day`/`stock_min`/`stock_tick`/`financialfiles`/`market`）、`QAREALTIME`（实时账户）；可选 **ClickHouse**（`qifi.*` 表，`insert_block_size` 批量写）。`QASetting`/`QAUtil` 里的 `mongo_ip`、`DATABASE` 统一管理连接。
- **数据结构**（`QAData/`）：`base_datastruct.py::_quotation_base`（行情基类，列 `code/open/high/low/close/volume/amount/date` 等）→ `QADataStruct.py` 的 `QA_DataStruct_Stock_day/Min`（含 `data` 属性、`add_func` 技术指标、`to_qfq/to_hfq` 复权转换）；`data_fq.py` 提供 `QA_data_stock_to_fq`（前/后复权）；`data_resample.py` 提供分钟/日/Tick 重采样；`QAPosition`/`QAOrder` 在 `QAMarket/`。

---

## 2. 回测引擎机制

### 2.1 QIFI 统一账户模型（`QIFI/QifiAccount.py::QIFI_Account`）

QIFI 账户**不区分股票/期货/加密**（源码注释："qifiaccount 不去区分你的持仓是股票还是期货，因此你可以实现跨市场的交易持仓管理"），关键字段：
- 可用资金 `money`、`pre_balance`（昨结）、`static_balance`（静态权益）、`deposit/withdraw/withdrawQuota`（入金/出金/可取）、`close_profit`（平仓盈亏）、`position_profit`/`float_profit`（持仓盈亏/浮动盈亏）、`margin`（保证金）、`frozen_margin`（冻结保证金）、`commission`；动态权益 `balance = static_balance + deposit - withdraw + float_profit + close_profit`；
- `positions`/`orders`/`trades`/`frozen`/`schedule`（预调仓）/`event`/`transfers` 六个映射；`model = SIM/REAL/BACKTEST` 三段态；`dbname ∈ {mongodb, clickhouse}`。

### 2.2 订单方向与撮合（`send_order`/`order_check`/`receive_deal`）

`ORDER_DIRECTION`（`QAMarket/QAOrder.py`）把"买卖 × 开平"压成一个整数由 `parse_orderdirection` 解出 `direction=BUY/SELL`、`offset=OPEN/CLOSE/CLOSETODAY`：
- `1(BUY)/2(BUY_OPEN)/3(BUY_CLOSE)/4(BUY_CLOSETODAY)` → BUY；`-1/-2/-3/-4` → SELL；
- `order_check`：开仓（BUY_OPEN/SELL_OPEN/BUY）按 `coeff = price * unit_table * buy_frozen_coeff`（由 `MARKET_PRESET.get_code(code)` 查每标的每手单位与保证金冻结系数）计算保证金并**冻结资金**（`self.frozen[order_id]`）；平仓（BUY_CLOSE/SELL_CLOSE 等）校验 `volume_long/volume_short − frozen` 的可用仓量，实现**T+1/today 仓**的 `volume_long_his/volume_long_today/volume_long_frozen_today` 冻结体系；
- `receive_deal`：成交时释放冻结、按 `trade_amount == volume_left`（全成交）或 `<`（部分成交）更新 `volume_left`/`status`（`ALIVE`/`FINISHED`/`CANCEL`），经 `get_position(code).update_pos(price, amount, towards)` 返回 `(margin, close_profit, commission)`，`money -= (margin - close_profit)`、`close_profit += (close_profit - commission)`；
- 默认佣金 `commission_fee = 0.0015`；`settle()` 每日结算（`pre_balance += deposit - withdraw + close_profit`，清空 `orders/trades/frozen/schedule`，持仓 `pos.settle()`，**先卖后买**执行预调仓 `schedule`）。

### 2.3 Rust 回测内核（`QARSBridge/qars_backtest.py`）

`QARSBacktest(start, end, data_path)` 包装 `qars3.Backtest`，`QARSStrategy` 继承 `qars3.Strategy`，回调 `on_start/on_bar(bar)/on_tick/on_end`（bar 为 `{"date","open","high","low","close","volume"}` dict）。docstring 声称**10-20x 加速、内存 -40%**、10 年千股日线约 3 秒（对比 Python 30 秒）。`qars_account.py` 是 Rust 侧的账户桥，`QIFI_PROTOCOL.md` 是协议说明。

### 2.4 策略基类（`QAStrategy/`）

`qactabase.py`（CTA 单标的）、`qafactorbase.py`（因子）、`qahedgebase.py`（对冲）、`qamultibase.py`（多标的）、`syncoms.py`（合成组合）——策略继承基类后在 `on_bar`/`on_tick` 里调 `self.buy/sell`（资金经 QIFI 账户）。

---

## 3. 策略/因子 API 与扩展方式

最小账户用法（`QifiAccount.py __main__` 尾部的自测即实例）：

```python
acc = QIFI_Account("x1", "x1", model="BACKTEST")
acc.initial()                       # create_backtestaccount: money/pre_balance=init_cash
r = acc.send_order('000001', 10, 12, ORDER_DIRECTION.BUY)   # 报单(冻结资金)
acc.receive_deal(r['instrument_id'], 11.8, r['volume'], r['towards'],
                 acc.dtstr, order_id=r['order_id'])          # 成交(解冻+平仓盈亏)
acc.settle()                        # 次日结算
```

扩展点：`order_check`（源码注释明确"可重写此方法；若修改需注意下单失败时恢复资金与仓位"）、`on_ordersend/on_reload/on_sync/on_tick/on_bar`、`MARKET_PRESET`（新增标的需要在 `market_preset.py` 里登记交易所/每手单位/冻结系数）。数据侧 `QAFetch/base.py` 的 `_select_market_code/_select_type/_select_bond_market_code` 按市场分派。

---

## 4. 分析/优化能力

- `QAAnalysis`/`QAFactor`/`QAIndicator`：绩效分析、因子计算、技术指标（`QAIndicator.__init__` 导 EMA/HHV/LLV/SMA 等，QADataStruct `add_func`/`add_factors` 就地挂指标列）；
- 复权/重采样：`QA_data_stock_to_fq`（qfq 前复权/hfq 后复权）、`QA_data_min_resample` 等；
- Web/调度：`QAWebServer`（docker web）、`QASchedule`（定时任务）、`QAPubSub`（发布订阅事件驱动）、`QACmd`（CLI）；
- Rust 并行回测与 `save_tdx_parallelism.py`（并行落库）、`save_*` 脚本族（`QASU/`）。

---

## 5. 绩效产物结构与指标清单

账户打印（`QIFI_Account.message`）字段：`money/available`（可用）、`pre_balance/static_balance/balance`（昨结/静态/动态权益）、`deposit/withdraw/withdrawQuota`、`close_profit/position_profit/float_profit`、`margin/frozen_margin`、`commission`、`risk_ratio = 1 - available/balance`（风险度）、`positions`（`QA_Position.message`：`long/short/today/his/frozen` 各分量、`last_price`、`position_profit`、`margin`）。Rust 回测 `QARSBacktest.run()` 返回 dict：`total_return`、`annual_return`、`sharpe_ratio`、`max_drawdown`、`trades`。

---

## 6. 坑与注意事项（来自源码注释）

1. **`balance/static_balance` 的动态口径**：`balance` 是动态权益（含 `float_profit`），`available=money`、`risk_ratio=1-available/balance`——空仓时 `balance≈money`、`risk_ratio≈0`，与券商"风险度越高风险越大"的直觉相反，需按公式理解。
2. **`order_check` 下单失败要手工恢复资金**：源码 docstring 特意警告"如果你做了对于账户资金的预操作请在结束的时候恢复；下单失败请恢复资金和仓位"——因为开仓是先减 `money` 再冻结。
3. **`commission_fee=0.0015` 是账户级默认**：不是券商实际口径，实盘需按期货公司/券商覆盖 `update_pos` 的 `commission` 计算。
4. **`volume_long_today/his` 区分 T+1**：`BUY`（股票买入）在 `order_check` 的注释里只检查历史持仓 `volume_long_his`，新买入今日不可卖由冻结 `volume_long_frozen_today` 实现——自建策略若绕过 `towards` 语义会破坏 T+1。
5. **PyTDX 与 Tushare 的版本冲突**（`QATdx.ping` 注释）："Tushare 内置的 pytdx 版本和 QUANTAXIS 使用的 pytdx 版本不同，请重新安装 pytdx"——两地包同时装易踩版本坑。
6. **数据落在通畅的公开接口上**：通达信行情服务器（`stock_ip_list`）需 `select_best_ip` 探测，接口失效/被封时静默拿到坏数据（`ping` 里 `BAD RESPONSE`）。
7. **Rust 回测是性能承诺而非全功能**：`QARSBridge` 是 qars3 的薄包装，`on_bar` 拿的是 dict，指标/因子需自行喂入；早期 Python 版账户语义（`QIFI_Account`）与 Rust 引擎是两套实现，切换要核对其行为差异。
8. **市场预设必须登记新标的**：`MARKET_PRESET.get_code(code)` 返回 `unit_table`/`buy_frozen_coeff`，未登记标的会 fallback 到默认系数 1（`get(...)` 缺省），保证金算错而不报错。

## 7. 可借鉴的设计要点

1. **QIFI 统一账户协议**：把"买卖方向 × 开平仓"压成 int（±1..±4），账户/订单/成交/持仓四件套走标准 message（`qifi.sql`/`qifi.md` 定义 schema），使股票、期货、加密、期权在同一张账户表上结算——跨资产实盘/回测可复用的账户抽象。
2. **数据源多路并置 + 统一查询层**：`QAFetch` 20+ 路源 + `QASU/save_*` 规范化落库 + `QAQuery`/`QAQuery_Advance` 统一查，把"源易变"与"存储稳定"解耦。
3. **"冻结-解冻"保证金账务**：开仓先按 `unit_table×buy_frozen_coeff` 冻结、成交/撤单再精确解冻（`receive_deal` 里部分成交按 `trade_amount*coeff` 释放），是期货撮合账务的经典正确姿势。
4. **Rust 内核 + Python 桥**：性能敏感的回测循环下沉 qars3，Python 只做策略回调与数据编排，`QARSBridge` 保留旧 API——"语言边界=性能边界"的渐进式演进范例。

## 附：主要源文件索引

| 文件 | 内容 |
|---|---|
| `QUANTAXIS/QIFI/QifiAccount.py` | QIFI 统一账户（下单/成交/结算/冻结） |
| `QUANTAXIS/QIFI/qifi.sql` / `qifi.md` | QIFI 账户/订单/持仓/成交 schema 协议 |
| `QUANTAXIS/QAMarket/QAOrder.py` | `QA_Order`/`ORDER_DIRECTION`/`QA_OrderQueue` |
| `QUANTAXIS/QAMarket/QAPosition.py` | `QA_Position`（多空/today/his 持仓分量） |
| `QUANTAXIS/QAMarket/market_preset.py` | `MARKET_PRESET` 标的预设（每手/保证金系数） |
| `QUANTAXIS/QARSBridge/qars_backtest.py` / `qars_account.py` | Rust 回测/账户桥 |
| `QUANTAXIS/QAStrategy/*` | CTA/因子/对冲/多标的策略基类 |
| `QUANTAXIS/QAData/QADataStruct.py` / `base_datastruct.py` | 行情 DataFrame 结构 |
| `QUANTAXIS/QAData/data_fq.py` / `data_resample.py` | 复权与重采样 |
| `QUANTAXIS/QAFetch/*` | 数据源（TDX/Tushare/baostock/jqdata/东财/同花顺/加密等） |
| `QUANTAXIS/QAAnalysis/`、`QAFactor/`、`QAIndicator/` | 分析/因子/指标 |
| `QUANTAXIS/QAUtil/QASetting.py` / `DATABASE` | 全局配置与数据库连接 |
| `QUANTAXIS/QAWebServer/`/`QACmd/`/`QASchedule/` | Web/CLI/定时 |
| `QUANTAXIS/QASU/save_*.py` | 数据落库脚本族 |