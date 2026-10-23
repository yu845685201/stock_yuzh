# stock-picker（Jesse-J-whu）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：[github.com/Jesse-J-whu/stock-picker](https://github.com/Jesse-J-whu/stock-picker) · **分析依据**：本地克隆源码（`refrences/Jesse-J-whu__stock-picker`，git commit `c7c76a7` 2026-10-09）· **配套**：`../开源股票分析项目清单-20261010.md`
> **方法**：精读 `strategy.py`（周线爆发策略 + 双数据源 + HTML 生成）、`market_data.py`（Tushare 缓存层）、`qfq_data.py`（前复权校验）、`adjustment.py`（复权因子）与 README。

---

## 0 定位与使用形态

stock-picker 是一个**「周线爆发」A 股选股项目**，核心策略简单（布林带扩张 + 放量 + MACD 零上金叉），但工程重心放在**数据完整性校验 + GitHub Actions 定时发布**上——把选股结果自动发布成 GitHub Pages 的移动端 HTML 页面。

- 形态：本地 `python strategy.py` 生成 `docs/index.html` + `docs/data.json`；配合 GitHub Actions（`server-pages.yml`/`update.yml`/`akshare-probe.yml`）每周五自动更新发布。
- 技术栈：Python；`pandas/numpy`、`requests`、`jinja2`；数据源三层——**Tushare（日线+校验）** + **腾讯（前复权周线）** + **akshare（历史）**。
- 特色：代码里对「数据完整性/前视/陈旧数据」有大量显式校验与失败即拒绝（禁止发布）的防御，是「宁可不发也不发错」的范式。

---

## 1 功能模块全景

| 模块 | 位置 | 职责 |
|---|---|---|
| 策略计算 | `strategy.py: apply_strategy` | 周线 BOLL/放量/MACD 信号 |
| 数据源（腾讯直连） | `strategy.py: get_all_a_stocks/get_weekly_data` | 腾讯 qt.gtimg.cn / ifzq.gtimg.cn 批量探测+周线 |
| 数据源（AKShare/Tushare） | `qfq_data.py: AkshareMarketData` | 腾讯前复权 + Tushare 当日校验 |
| 数据源（Tushare） | `market_data.py: TushareMarketData` | 全市场日线缓存 + 本地聚合周线 |
| 复权 | `adjustment.py` | 前复权因子加载与价格调整 |
| 发布 | `strategy.py: generate_html/save_data_json` | 移动端 HTML + JSON |
| 服务化 | `server/*` | systemd timer、runner、离线审计 |

---

## 2 数据源来源与采集机制

| 源 | 接口（URL/函数） | 内容 | 校验 |
|---|---|---|---|
| 腾讯 | `https://qt.gtimg.cn/q=...`（80 只/批） | 全 A 股代码+名称（枚举代码段探测） | 过滤 ST/退/PT、价格≤0 |
| 腾讯 | `https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param={symbol},week,,,120,qfq` | 周线前复权 120 根 | 需≥30 根，`has_recent_kline`（末日期 ≤14 天） |
| Tushare | `api.tushare.pro`（`daily`/`trade_cal`/`adj_factor`/`stock_basic`） | 全市场日线（5 年）、复权因子、日历 | `MIN_ROWS_PER_TRADE_DATE=4000`、覆盖比 `0.80` |
| akshare | `qfq_data.py`（腾讯前复权 + Tushare 当日校验） | 前复权历史 | `validate_history` 逐项校验 |

**关键校验/防御机制**（这是本项目最有价值的部分）：
- `has_recent_kline`：K 线末日期超过 14 天即弃（防退市/停牌旧快照造假信号）。
- `MIN_DATA_COVERAGE_RATIO = 0.80`；（`market_data.py`/`qfq_data.py`）日线行数 <4000 直接 `raise MarketDataError(...停止发布结果)`。
- `qfq_data.validate_history`：重复/乱序日期、非有限 OHLCV、负成交量、`high<open/close/low`、`low>open/close/high`、末日期≠参考日、`|末收盘-参考收盘|>0.011` 全报错拒绝。
- **前复权**（`adjustment.py`）：`原价 × 当日因子 / 样本末日因子`，成交量不调整；因子缺失/非正 → 拒绝退回未复权。
- 限流：`REQUEST_INTERVAL_SECONDS=2.6`；「频率超限」重试 5 次退避（`15*(attempt+1)`s），`trade_cal`/`stock_basic` 按小时/低频限流单独退避（`1次/小时` 退避 3605s）。

---

## 3 分析逻辑（周线爆发策略）

`apply_strategy(df)`（df 含 `close`/`vol`，周线）：

1. **BOLL 扩张**（`mid=MA(close,20)`，`upper=mid+2σ`，`lower=mid-2σ`，`σ=rolling std ddof=0`）：
   `upper > REF(upper,1) 且 mid > REF(mid,1) 且 lower < REF(lower,1)`（上轨升、中轨升、下轨降 = 开口扩张）。
2. **放量**：`vol_ratio = vol / REF(vol,1)`，`exist(vol_ratio > 4, 26)`（26 周内存在单周量 > 前周 4 倍）。
3. **MACD 零上金叉**：`dif=EMA(close,12)-EMA(close,26)`，`dea=EMA(dif,9)`，`jc=cross(dif,dea)`，`zero_cond=dea>0`，`exist(jc & zero_cond, 3)`（3 周内 DIF 上穿 DEA 且 DEA>0）。

三者 `&` → `xg` 布尔序列，取末一根信号为真即选中。

**辅助函数**（`strategy.py`）：`ema/ma/std/ref/cross/exist` 均为 pandas 实现（`ewm/rolling/shift`），语义同通达信。

**周线聚合**（`market_data.py: bars`）：日线 `resample("W-FRI")` 聚合（open=first、close=last、high=max、low=min、vol=sum）。

---

## 4 最终产物结构与格式

- **HTML**（`docs/index.html`）：Jinja2 模板，移动端深色主题（`#0a0e27`），股票卡片（名称/代码/价格/涨跌幅/开高低），顶部「数据状态 + 行情距今超 4 天」的 freshness 警告脚本，免责声明；空结果时显示「今日暂无符合策略的股票」。
- **JSON**（`docs/data.json`）：`{update_time, timezone, trade_date, data_quality, strategy, adjustment, data_source, count, stocks[]}`。
- **定时**：`server/stock-picker.timer`（systemd 每周五）+ GitHub Actions `update.yml` 发布 Pages。

---

## 5 坑与注意事项

1. **代码里有两套重复的数据函数**：`strategy.py` 顶部定义了「腾讯直连版」`get_all_a_stocks/get_weekly_data/get_daily_data_for_display`，随后又**重定义同名函数**（用 `MARKET_DATA = AkshareMarketData` 覆盖前者）；`market_data.py` 还有第三套 `TushareMarketData`。实际生效的是后定义的 AKShare/Tushare 版，读代码极易被第一套误导。
2. **Tushare Token 是硬门槛**：`AkshareMarketData.__init__` 缺 `TUSHARE_TOKEN` 直接 `raise MarketDataError`（`qfq_data.py`），README 没强调；GitHub Actions 需配 Secret。
3. **复权/校验失败=拒绝运行**：`qfq_data` 的 `validate_history` 任一字段异常即抛错、`adjustment` 复权因子缺失即拒绝——设计上「宁可不发」，本地首次跑全市场需较长时间拉全 5 年数据。
4. **腾讯接口枚举探测代码段**：`get_all_a_stocks`（第一套）硬编码枚举 `000001-000999`、`300001-302000`、`600000-602000` 等，覆盖不完全（如 001/301/603 之后的新代码段），且批次 80 只/次、`sleep(0.05)`，是全市场兜底的笨办法。
5. **前复权基准是样本末日**：`ratio = factors / factors.iloc[-1]`，历史价随样本窗口末端变化，跨次运行时同一历史日的「前复权价」会因窗口不同而略有漂移（README 未说明）。
6. **周线 `W-FRI` 固定周五**：`resample("W-FRI")`，非完整周被舍去，与首尾数据的周线口径需知悉。

---

## 6 可借鉴的设计要点

1. **「失败即拒绝发布」的数据完整性守卫**：`MIN_ROWS=4000`、覆盖比 0.8、末日期/末收盘与 Tushare 参考对账（`>0.011` 拒绝）、OHLC 合法性与复权因子校验——把「数据对不对」制度化，而不是事后发现假信号。
2. **双源交叉校验**：腾信前复权周线 × Tushare 当日日线做「当日收盘一致性」核对，识别源间口径漂移，是免费数据源下的严谨做法。
3. **限流分 API 退避**：按 API 粒度差异化退避（`trade_cal` 按小时、`daily` 按分钟、`adj_factor` 固定 2.6s），避免粗暴统一 sleep。
4. **「信号日当天收盘 → T+1 成交」的前视防护**：策略只取末一根信号，配合 `has_recent_kline`（防陈旧 K 线当今日），避免把停牌/退市股的历史形态当实时信号。
5. **产物管静态发布**：把选股结果渲染成移动端单页 HTML + 结构化 JSON，用 GitHub Pages + systemd timer 无人值守发布，适合「每日/每周自动看结果」场景。

---

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `strategy.py` | 周线爆发策略 + 双数据源（腾讯/AKShare）+ HTML/JSON 生成 |
| `market_data.py` | Tushare 日线缓存 + 本地聚合周线 + 完整性校验 |
| `qfq_data.py` | AKShare/腾讯前复权 + Tushare 当日校验 + 数据审计 |
| `adjustment.py` | 前复权因子加载与价格调整 |
| `verify_output.py` | 输出结果自检 |
| `server/runner.py` / `stock-picker.timer/.service` | 定时运行与 systemd 服务 |
| `.github/workflows/*.yml` | akshare 探测 / 页面发布 / 定时更新 |
| `tests/test_qfq_data.py` | 前复权数据测试 |