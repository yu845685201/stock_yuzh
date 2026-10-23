# Sequoia-X（sngyai/Sequoia-X）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：[github.com/sngyai/Sequoia-X](https://github.com/sngyai/Sequoia-X) · **分析依据**：本地克隆源码（`refrences/sngyai__Sequoia-X`，git commit `444c0db` 2026-05-09，pyproject 版本 `2.0.0`）· **配套**：`../开源股票分析项目清单-20261010.md`
> **方法**：精读 `data/engine.py`（数据引擎）、`strategy/*.py`（7 个策略）、`notify/feishu.py`（推送）、`core/config.py`、`main.py` 与 README，全部结论来自源码实现。

---

## 0 定位与使用形态

Sequoia-X V2 是一个**面向 A 股的轻量量化选股系统**：每日收盘后自动用 baostock 增量拉全市场日 K，本地 SQLite 存储，跑 7 个固定策略，将每策略选出的股票以飞书卡片推送到（可拆分的）多个飞书群机器人。

- 定位：个人选股播报工具，非回测框架（**无任何回测/成交模拟环节**），README 自称「现代 Python 工程化标准从零重构」的 V2。
- 两种模式：`python main.py`（日常：8 进程增量补数据 + 跑策略 + 飞书推送，2~3 分钟）；`python main.py --backfill`（回填：单线程保守拉全市场历史，约 12 分钟）。
- 技术栈：Python ≥3.10；`baostock`（行情）、`akshare`（仅定增公告用）、`pydantic-settings`（配置）、`rich`（日志）、`pandas`（向量化）、`requests`（飞书推送）；SQLite 原生 `sqlite3`；`uv`/`pip` 安装；`ruff`+`pytest`+`hypothesis`（属性测试）。
- 运行节律：README 建议 crontab `15 19 * * 1-5` 收盘后跑。

---

## 1 功能模块全景

| 模块 | 位置 | 职责 |
|---|---|---|
| 数据引擎 | `data/engine.py` | baostock 登录/拉取、SQLite 建表、回填、增量同步、股票列表 |
| 配置管理 | `core/config.py` | pydantic-settings 读 `.env`，扫描 `STRATEGY_WEBHOOK_*` 前缀 |
| 日志 | `core/logger.py` | rich 结构化日志（单例 `get_logger`） |
| 策略基类 | `strategy/base.py` | `BaseStrategy`（ABC），`run() -> list[str]` + `webhook_key` |
| 7 个策略 | `strategy/*.py` | 海龟/均线放量/高窄旗形/涨停洗盘/上升跌停/RPS/定增 |
| 飞书推送 | `notify/feishu.py` | 交互式卡片消息，按 webhook_key 路由到不同机器人 |
| 入口 | `main.py` | argparse 分发两种模式 + 遍历策略推送 |

---

## 2 数据源来源与采集机制

| 源 | 接口（函数） | 内容 | 频率/限制 | 缓存/落盘 |
|---|---|---|---|---|
| baostock | `bs.query_history_k_data_plus(bs_code, "date,open,high,low,close,volume,amount", frequency="d", adjustflag="1")` | 日 K（**后复权**）；`amount` 后被 rename 为 `turnover`（实际是成交额单位元） | 免费/无需注册/无限流 | SQLite `stock_daily` 表，`UNIQUE(symbol,date)` |
| baostock | `bs.query_stock_basic(code_name="", code="")` | 全市场代码列表（第 0 列代码、第 4 列 status、第 5 列 type） | 同上 | 内存 list（过滤 `status=='1' and stock_type=='1'`） |
| baostock | `bs.query_history_k_data_plus(..., "close,volume,turn", adjustflag="3")` | 不复权收盘 + 换手率（用于反推流通市值） | 仅海龟策略排序时对候选逐只查询 | 无持久化 |
| akshare | `ak.stock_qbzf_em()` | 东方财富「全部增发」公告表 | 仅定增策略 | 无持久化 |

**SQLite 表结构**（`_CREATE_TABLE_SQL`）：
```sql
CREATE TABLE IF NOT EXISTS stock_daily (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  symbol TEXT NOT NULL, date TEXT NOT NULL,
  open REAL, high REAL, low REAL, close REAL, volume REAL, turnover REAL,
  UNIQUE (symbol, date)
);
CREATE INDEX IF NOT EXISTS idx_symbol_date ON stock_daily (symbol, date);
```

**代码格式转换** `_to_baostock_code(symbol)`：`6/9 开头 → sh.XXX`，其余 → `sz.XXX`（简单前缀规则，未处理北交所 `4/8` 前缀——北交所代码在飞书 `_to_xueqiu_code` 里单独映射为 `BJ`）。

**采集机制细节**：
- **回填**（`backfill`）：单只失败重试 3 次间隔 2s/4s/8s（`2**(attempt+1)`）；每处理 200 只 `bs.logout()+login()` 重连防长连接超时；已入库自动 skip（`last_date >= today` 跳过），支持中断续跑。
- **增量**（`sync_today_bulk`）：`Pool(n_workers)`，`n_workers = min(8, len(tasks))`，任务按 `tasks[i::n_workers]` 切成 N 块并行；每 worker 独立 `bs.login()`；写库前先 `DELETE FROM stock_daily WHERE date=?` 再 `to_sql(..., method="multi", chunksize=500)`（防重复），`df=df[df["volume"]>0]` 过滤无效行。
- **容错/静默**：baostock `rs.error_code != "0"` 时回填 `raise RuntimeError(rs.error_msg)` 走重试，增量里则直接 `continue`（静默跳过该票）。

---

## 3 分析逻辑（选股规则与阈值）

策略统一返回 `list[str]`（代码列表），由 `main.py` 遍历推送。7 个策略条件全列如下（源码逐条核实）：

### 3.1 TurtleTradeStrategy（海龟突破）— `turtle_trade.py`
- `_MIN_BARS = 21`（20 日窗口 + 当日）。
- 条件 1 突破：`今日 close > 前20日 high 的滚动最大值`（`high.shift(1).rolling(20).max()`，**不含当日**）。
- 条件 2 流动性：`今日 turnover > 100_000_000`（成交额过亿；注意 turnover 实为 amount）。
- 条件 3 防诱多：`今日 close > 今日 open`（实体阳线）且 `今日 close > 昨日 close`（真涨，防假阳线）。
- 排序：对候选逐只用 baostock `adjustflag="3"` 查不复权 close/volume/turn，反推 `流通股本 = volume/(turn/100)`、`流通市值 = 流通股本 × close`，**按流通市值从大到小排序**。
- 源码注释里留了一句选股动机：「【新增防守条件】拒绝郑州煤电式的高开低走大阴线！」

### 3.2 MaVolumeStrategy（均线放量金叉）— `ma_volume.py`
- MA5 上穿 MA20（昨日 `ma5<ma20` 且今日 `ma5>ma20`）；
- 放量确认：`今日 volume > 20日均量 × 1.5`。

### 3.3 HighTightFlagStrategy（高窄旗形）— `high_tight_flag.py`
- `_MIN_BARS = 40`。
- 强动量：`过去40日 high.max / low.min > 1.6`（区间振幅超 60%）；
- 极度收敛：`最近10日 high.max / low.min < 1.15`（振幅低于 15%）；
- 高位抗跌：`近10日 low.min >= 40日 high.max × 0.8`（注释含此条件，docstring 未列）；
- 缩量：`今日 volume < 过去20日均量 × 0.6`（`iloc[-21:-1].mean()`，不含当日）。

### 3.4 LimitUpShakeoutStrategy（涨停洗盘）— `limit_up_shakeout.py`
- `_MIN_BARS = 3`。
- 昨日涨停：`昨日 close >= 前日 close × 1.095`；
- 今日收阴：`今日 close < 今日 open`；
- 今日放量：`今日 volume > 昨日 volume × 2.0`；
- 支撑不破：`今日 low >= 昨日 close`。

### 3.5 UptrendLimitDownStrategy（上升趋势跌停）— `uptrend_limit_down.py`
- `_MIN_BARS = 60`。
- 上升趋势：`昨日 MA20 > 昨日 MA60`（均线多头，用昨日值防未来函数）；
- 放量跌停：`今日 close <= 昨日 close × 0.905` 且 `今日 volume > 今日 20日均量 × 2.0`。

### 3.6 RpsBreakoutStrategy（RPS 极强动量突破）— `rps_breakout.py`
- `rps_period = 120`，`rps_threshold = 90`。
- 直接 `pd.read_sql` 读整表 → 纵向涨幅 `pct_change = (close - close.shift(120)) / close.shift(120)`（后复权口径）；
- 横向排位：最新交易日截面 `rank(pct=True) × 100` 得到 RPS，`RPS >= 90` 入选；
- 突破：`close >= 该股滚动120日最高价(roll_high) × 0.90`（即现价距 120 日高点不足 10%）。
- 该策略**不逐个取 OHLCV、不校验最小 K 线数**，是全表一次算的向量化做法（`roll_high` 用 `groupby+rolling(min_periods=60)`）。

### 3.7 PrivatePlacementStrategy（定增公告监控）— `private_placement.py`
- 不走行情，走 `akshare.stock_qbzf_em()`；
- 过滤 `发行方式 == "定向增发"`（排除公开增发）；
- 回看 `_LOOKBACK_DAYS = 7` 天内 `发行日期`；
- 按发行日期降序；代码提取 `str.extract(r"(\d{6})")` 去前缀，去重。

---

## 4 最终产物结构与格式

- **飞书交互式卡片**（`FeishuNotifier._build_card`）：
  - `msg_type = "interactive"`，`card.header.template = "blue"`，标题 `📈 Sequoia-X 选股播报 | {strategy_name}`；
  - 正文三段：`日期 / 策略 / 选股数量` + 分隔线 `hr` + `选股列表`；
  - 选股列表每只渲染为 `[名称](https://xueqiu.com/S/{雪球代码})` 超链接，空格连接；
  - 雪球代码映射 `_to_xueqiu_code`：`6 → SH`、`4/8 → BJ`、其余 → `SZ`。
- **webhook 路由**：每策略有 `webhook_key`（`ma_volume/turtle/flag/shakeout/limit_down/rps/private_placement`），`get_webhook_url(key)` 在 `Settings.strategy_webhooks` 未命中时回退到默认 `feishu_webhook_url`。
- **成功判定**：飞书真正成功标志是 `resp_json["code"] == 0`（而非仅 HTTP 200），异常不抛出只记 ERROR 日志。
- 无结果时 `main.py` 只打日志「无选股结果，跳过推送」，不推空卡片（`_build_card` 里的 `（无选股结果）` 分支实际不会被走到）。

---

## 5 坑与注意事项

1. **`turnover` 字段名误导**：baostock 返回的是 `amount`（成交额），`backfill` 里 `rename(columns={"amount":"turnover"})`；所有策略的「turnover」都是**成交额（元）**，不是换手率。看代码时别按换手率理解（`liquid = turnover > 1e8` 即「成交额过亿」）。
2. **涨停判定不分板块制度**：`昨天 close >= 前日 close × 1.095` 是统一 10% 口径，会（a）漏掉创业板/科创板 20cm 涨停、（b）漏掉北交所 30%、（c）ST 5% 也可能被误判为涨停。源码未注释此边界，属分析发现的隐含限制。同理跌停 `×0.905` 同口径问题。
3. **代码前缀规则不完整**：`_to_baostock_code` 只认 `6/9 → sh`、其余 `→ sz`，**北交所（4/8 开头）会被错误映射为 `sz`**；但飞书卡片侧面 `_to_xueqiu_code` 又单独处理了 `4/8 → BJ`，两处口径不一致，北交所票在数据侧取不到、在推送链路却又预留了映射。
4. **回填错误静默切换**：增量 `sync_today_bulk` 里 baostock `error_code != "0"` 直接 `continue`（静默漏票）；回填 `backfill` 里才抛异常走重试。若日常增量某票拉取失败，不会告警，只会少那一天的行情。
5. **全市场遍历是逐票 for 循环**：除 RPS 用全表向量化外，其余策略对每只 `get_ohlcv(symbol)` 单独开 SQLite 连接查一次（`get_ohlcv` 每次 `sqlite3.connect`），5200 只票下连接开销偏大；README「8 进程」只作用于取数，不作用于策略计算。
6. **定增策略需额外 akshare**：`private_placement` 是全项目唯一依赖 akshare 的模块，且强依赖东方财富接口可用性；取不到时 `return []` 静默。
7. **配置必填项会导致启动崩溃**：`Settings.feishu_webhook_url` 无默认值、缺失时抛 `ValidationError`（README 因此要求先 `cp .env.example .env` 填 webhook）。
8. **除权处理仅靠后复权**：全库统一 `adjustflag="1"`（后复权），历史价格不因除权重算，适合增量追加；但不同股票长期后复权价格绝对值不可比（只有相对强度口径下才可比），RPS/动量类策略用涨跌幅所以正确，直接看 price 绝对值的场景要留意。

---

## 6 可借鉴的设计要点

1. **策略接口极简可插拔**：`BaseStrategy` 只有 `run() -> list[str]` 和一个 `webhook_key`，`main.py` 里「策略列表（新增策略在此追加即可）」加一行类即可接入选股+推送全链路。
2. **免 Token、免注册、无限流的数据层**：用 baostock（后复权）替代东财反爬，配 SQLite 单文件库（README 强调「可直接拷贝到其他机器使用」），把个人工具的数据获取成本压到最低。
3. **「多 worker 独立 login + chunks 分片」的并行取数模式**：`tasks[i::n_workers]` 轮转分片 + Pool 映射，每个子进程独立 `bs.login()`，避免单连接串行瓶颈；回填「重试 3 次 + 每 200 只重连 + 跳过已入库」的续传设计很实。
4. **向量化纪律**：策略 docstring 反复写「严禁 iterrows」，全用 `rolling/shift/rank`，是给 AI 时代的协作说明（AI 生成代码时也延续此约束）。
5. **反「高开低走大阴线」的工程化教训**：海龟策略在「突破+流动性」基础上叠加「实体阳线+真涨」双层过滤，注释用「郑州煤电式」具体案例固化了踩坑记忆，是有价值的口径护栏。
6. **飞书 webhook 按策略分群**：`STRATEGY_WEBHOOK_<KEY>` 环境变量前缀扫描 + `get_webhook_url` 回退，一个系统可同时给多个主题群各推各的信号。
7. **属性测试**：`tests/` 引入 `hypothesis`，对配置/数据引擎/策略做属性测试（`pytest` + `pytest-mock`），比纯手写用例覆盖更广。

---

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `main.py` | 入口：argparse 分发回填/日常，策略列表注册与遍历推送 |
| `sequoia_x/core/config.py` | Settings（pydantic-settings）+ STRATEGY_WEBHOOK_ 前缀扫描 + webhook 回退 |
| `sequoia_x/core/logger.py` | rich 日志单例 |
| `sequoia_x/data/engine.py` | baostock 取数 + SQLite 建表/回填/增量/代码映射 |
| `sequoia_x/strategy/base.py` | 策略抽象基类 |
| `sequoia_x/strategy/turtle_trade.py` | 海龟 21 日突破 + 流通市值排序 + 反推市值技巧 |
| `sequoia_x/strategy/ma_volume.py` | MA5 金叉 MA20 + 1.5 倍放量 |
| `sequoia_x/strategy/high_tight_flag.py` | 40 日动量 + 10 日收敛 + 缩量 |
| `sequoia_x/strategy/limit_up_shakeout.py` | 昨日涨停 + 今日放量收阴不破昨收 |
| `sequoia_x/strategy/uptrend_limit_down.py` | 上升趋势 + 放量跌停 |
| `sequoia_x/strategy/rps_breakout.py` | 120 日 RPS≥90 + 距高点<10% 突破 |
| `sequoia_x/strategy/private_placement.py` | akshare 定增公告近 7 天监控 |
| `sequoia_x/notify/feishu.py` | 飞书交互卡片构建/推送/雪球代码映射 |
| `tests/*` | pytest + hypothesis 属性测试 |