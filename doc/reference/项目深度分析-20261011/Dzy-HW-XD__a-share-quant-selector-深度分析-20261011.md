# a-share-quant-selector（Dzy-HW-XD）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：[github.com/Dzy-HW-XD/a-share-quant-selector](https://github.com/Dzy-HW-XD/a-share-quant-selector) · **分析依据**：本地克隆源码（`refrences/Dzy-HW-XD__a-share-quant-selector`，git commit `9cfbf80` 2026-09-08）· **配套**：`../开源股票分析项目清单-20261010.md`
> **方法**：精读 `strategy/bowl_rebound.py`（碗口反弹）、`strategy/pattern_*`（B1 匹配）、`utils/technical.py`（技术指标）、`utils/akshare_fetcher.py`（取数）、`utils/dingtalk_notifier.py`（推送）与 README/CHANGELOG/B1_PATTERN_MATCH.md。

---

## 0 定位与使用形态

a-share-quant-selector 是一个**小而精的单策略 A 股选股系统**，核心是「碗口反弹策略」+「B1 完美图形相似度匹配」+「钉钉推送 K 线图」。它是 KHunter 的**前身/基础架构**（KHunter 在 README 郑重致谢），体量仅 10 个 py 文件。

- 形态：CLI（`main.py init/update/run/web`）+ Flask Web（`localhost:5000`）+ 钉钉机器人通知（带图）。
- 技术栈：Python 3.8+；`akshare`（唯一行情源 + 腾讯 HTTP 兜底）、`pandas/numpy`、`matplotlib`（K线图）、`flask`、`fastdtw`、`PyYAML`。
- 特色：README 用「TODO 清单」坦诚待办（TODO1 尝试大模型近似、TODO2 更新砖型图显、TODO3 补完美图形库），并标注「本项目使用多Agent协作开发」（Main/Developer/QA/Release）。

---

## 1 功能模块全景

| 模块 | 位置 | 职责 |
|---|---|---|
| 碗口反弹策略 | `strategy/bowl_rebound.py` | 唯一选股策略（分类标记版） |
| B1 完美图形匹配 | `strategy/pattern_matcher.py`、`pattern_feature_extractor.py`、`pattern_library.py`、`pattern_config.py` | 10 案例四维相似度排序 |
| 策略基类/注册 | `strategy/base_strategy.py`、`strategy_registry.py` | BaseStrategy（ABC）+ 注册表 |
| 技术指标 | `utils/technical.py` | MA/EMA/KDJ/REF/EXIST/趋势线（通达信风格） |
| 取数 | `utils/akshare_fetcher.py` | akshare + 腾讯兜底，CSV 落盘 |
| 通知 | `utils/dingtalk_notifier.py` | 钉钉 webhook + 限流 + K线图 |
| Web | `web_server.py` | Flask 管理界面 |
| K 线图 | `utils/kline_chart.py` / `kline_chart_fast.py` | 趋势线+成交量叠加图 |

---

## 2 数据源来源与采集机制

| 源 | 接口（函数） | 内容 | 落盘 |
|---|---|---|---|
| akshare | `ak.stock_zh_a_hist(symbol, period="daily", start_date, adjust="qfq")` | 单股历史日线（前复权） | CSV |
| akshare | `ak.stock_sh_a_spot_em()` / `ak.stock_sz_a_spot_em()` / `ak.stock_zh_a_spot_em()` | 沪深 A 股实时列表/快照 | 代码列表 + 增量更新 |
| akshare | `ak.stock_individual_info_em(symbol)` | 个股总市值（`item=="总市值"`） | 内存（市值过滤兜底） |
| 腾讯（HTTP 兜底） | `utils/akshare_fetcher.py: _fetch_stock_list_http` / `_fetch_market_cap_tencent` | 股票列表、批量流通市值 | CSV |
| 本地 CSV | `utils/csv_manager.py` | 历史数据持久化 | `data/*.csv` |

**取数细节**（`akshare_fetcher.py`）：
- `init_full_data`：默认抓 6 年历史（`years=6`），`max_stocks` 可限只处理前 N 只（快速测试）。
- `daily_update`：增量补当日；`max_retries=3` 重试；`skip_failed=True` 失败跳过继续。
- **模拟数据降级** `_generate_mock_data`：akshare 拿不到时生成带 `market_cap` 列的假数据兜底（`_check_market_cap_realtime` 里最终「默认返回 True 不过滤」是三级降级的终点）。
- **智能更新**：3 点前不更新走本地；3 点后检查当天数据缺失再增量（README 与 KHunter 一致）。

---

## 3 分析逻辑

### 3.1 碗口反弹策略（`bowl_rebound.py`）

**指标定义**（源码 docstring + `technical.py`）：
- **知行短期趋势线** = `EMA(EMA(CLOSE,10),10)`（连续两次 10 日 EMA）。
- **知行多空线** = `(MA(CLOSE,M1)+MA(CLOSE,M2)+MA(CLOSE,M3)+MA(CLOSE,M4))/4`，代码默认 `M1=14/M2=28/M3=57/M4=114`。⚠️ docstring 仍写「MA(5,10,20,30)」，是 CHANGELOG [2024-02-10] 把 `5/10/20/30 → 14/28/57/114` 之后**文档未更新**的遗留。

**选股条件**（`calculate_indicators` + `select_stocks`）：
1. 上升趋势：`短期趋势线 > 多空线`（`trend_above`）；
2. 放量阳线：`V >= REF(V,1)*N` 且 `C > O` 且 `总市值 > CAP`（`key_candle`）；
3. 异动：`EXIST(关键K线, M)` 天内存存在；
4. J 值低位：`J <= J_VAL`（KDJ 9/3/3）；
5. 位置分类（三者任一，优先级从高到低）：回落碗中（`多空线 ≤ close ≤ 短期趋势线`）> 靠近多空线（`±duokong_pct%`）> 靠近短期趋势线（`±short_pct%`）；
6. **最大量是阴量剔除**：回顾期内成交量最大那天若 `close<open` 直接剔除（README 解释：大资金出逃的弱势信号）。

**风险过滤**：ST/*ST、退市关键词、成交量≤0、收盘 NaN、近 30 日 `|J|` 均值 >80。

**市值三级降级**（`_check_market_cap_realtime`）：CSV `market_cap`（合理区间 `1e9 < cap < 1e11`）→ akshare 实时总市值（解析「33.19亿」）→ `收盘价×2e8` 粗略估算 → 默认 True 不过滤。

**参数多源差异（重要发现）**：

| 参数 | 代码 `default_params` | `config/strategy_params.yaml`（运行时实际生效） | README 示例 |
|---|---|---|---|
| N（量比） | 4 | **2** | 2.4 |
| M（回溯天） | 15 | **30** | 20 |
| J_VAL | 30 | **20** | 0 |
| duokong_pct | 3 | **0.87** | 3 |
| short_pct | 2 | **1** | 2 |

→ 实际跑起来用 `config/strategy_params.yaml`，README 与代码默认值都只是参考；这是文档/代码/配置三处未对齐的典型。

### 3.2 B1 完美图形匹配（`pattern_matcher.py`）

对选股结果与 **10 个历史成功案例**做四维相似度，按百分制排序：
- 权重（`strategy_params.yaml` 实际值）：双线结构 0.10、KDJ 状态 0.20、量能特征 0.25、价格形态 0.45（README 写 0.30/0.20/0.25/0.25，亦不同步）。
- 四维计算（`B1_PATTERN_MATCH.md` 详述）：
  1. **双线结构**——`short_vs_bullbear` 比值、`short_slope` 斜率方向（同向至少 0.7 分、反向 ≥0.3 惩罚）、`price_bias_pct` 双线乖离、`is_in_bowl` 碗中一致性（不一致惩罚 0.2）；
  2. **KDJ**——J 值位置（低位/中位/高位）、J 值数值（±30 容差）、金叉状态；
  3. **量能**——近 10 日/前 10 日均量比（±1.5 容差）、量能趋势分类（持续放量/缩量/缩量后放量/平稳）、最大量比（±3 容差）；
  4. **价格形态**——归一化价格曲线 `(close-min)/(max-min)` 的 **FastDTW** 距离 + 最大回撤（±15%）+ 趋势方向。
- 特征提取用**突破日前 25 天**数据（排除突破当天，注释解释了「大阳线会扭曲指标」）。
- 输出：相似度百分比、命中案例名/日期、四维分项、策略分类/价格/J 值；`min_similarity=60`、`top_n_results=25`。

### 3.3 钉钉通知（`dingtalk_notifier.py`）

- 加签认证：HMAC-SHA256（`hmac` + `base64` + `urlencode`，secret 从 `config.yaml` 读）。
- **限流器 `RateLimiter`**：每分钟 ≤20 条、每条间隔 ≥2s、遇限速错误（钉钉 660026）指数退避重试 3 次（1s/4s/8s），附带「锁定时间」延长退避——README 专门写了这段，是踩过钉钉限流坑的产物。
- 消息内容：策略参数回显 + 分类计数（回落碗中/靠近多空线/靠近短期线）+ 每票「分类/价格/J 值/关键 K 线日期/入选理由」+ **K 线图图片**（20 天 K 线 + 蓝色短期趋势线 + 绿色多空线 + 红绿成交量柱）。

---

## 4 最终产物结构与格式

- **选股信号**（`select_stocks` 返回）：
```python
{'date', 'close', 'J', 'volume_ratio', 'market_cap', 'short_term_trend',
 'bull_bear_line', 'reasons': [...], 'category', 'key_candle_date'}
```
- **B1 匹配结果**：`{total_score(百分制), breakdown: {trend_structure/kdj_state/volume_pattern/price_shape}}`。
- **钉钉消息**：普通选股结果 + B1 匹配结果（Top N，含排名/相似度/命中案例/分项/分类/价格/J 值）。
- **Web**：`localhost:5000` 系统概览（股票数/最新数据日期）、股票列表、选股结果、策略配置在线改。
- **CSV 数据**：`data/{code}.csv` 历史日线（含 `market_cap` 列），是全部计算的基础。

---

## 5 坑与注意事项

1. **参数四处不一致（README / 代码默认 / yaml / CHANGELOG）**：N/M/J_VAL/duokong_pct/short_pct 在四个来源取值全不同（见 §3.1 表）。实际以 `config/strategy_params.yaml` 为准，读代码/文档时极易被带偏。
2. **多空线 docstring 过时**：`bowl_rebound.py` 顶部注释写「(MA5+MA10+MA20+MA30)/4」，但实际 M1-M4=14/28/57/114；CHANGELOG 记录过这次改动，文档没跟着改。
3. **市值兜底会「不筛选」**：三级降级最后「默认返回 True（不过滤）」，即 akshare/腾讯都失败时市值条件静默失效；`_generate_mock_data` 模拟数据也说明取数不稳定时系统偏向「放行」而非「漏选」。
4. **B1 权重 README 与 yaml 不一致**：README/B1_PATTERN_MATCH 写双线 30%、KDJ 20%、量能 25%、形态 25%；yaml 实际是 0.10/0.20/0.25/0.45（价格形态权重几乎翻倍）。形态匹配结果对权重很敏感。
5. **倒序数据是全局前提**：`technical.py` 每个函数都先 `iloc[::-1]` 反转到正序算、再反转回来（注释「正确处理倒序数据」），数据方向假设是「最新在前」；若输入正序会静默算错（`MA/EMA/REF/EXIST` 均依赖此假设）。
6. **KDJ 递归 O(n) 且前 n-1 日 RSV=50**：`technical.KDJ` 用 Python for 循环递推（非向量化），全市场跑会慢；RSV 不足期用 50 填充、K/D 初值 50，与通达信首值处理略不同。
7. **单策略定位**：CHANGELOG 显示原本 4 个独立策略被重构合并成「1 个策略 + 3 种分类标记」，扩展新策略要继承 BaseStrategy 手动注册；README TODO2/TODO3 表明砖型图、B2/B3 图形库尚未实现。
8. **钉钉限流是硬约束**：示例格式含每票一张 K 线图，若选股数量多、图片多，20 条/分钟 + 2s 间隔会让一次推送耗时数分钟（README 已内置限流器应对）。
9. **夸张的技术债**：`kline_chart_fast.py` 是 `kline_chart.py` 的「快速版」，`_generate_mock_data` 说明数据稳定性是痛点，整体是「能跑为主」的轻量实现。

---

## 6 可借鉴的设计要点

1. **「策略选股（硬条件）→ 相似度排序（软排序）」两段式**：碗口反弹先做硬筛选，B1 匹配再用「10 个历史成功案例」对结果做形态相似度排序——把「主观案例经验」沉淀成可复现的匹配算法，是从「人肉看图」到「半自动」的巧妙过渡。
2. **形态匹配用「百分比归一化 + DTW」而非绝对价格**：B1_PATTERN_MATCH.md 用 20 元 vs 100 元股票的例子讲清为何用百分比偏离（消除价格绝对值失真），FastDTW 允许时间轴弹性对齐，是形态相似度的经典解法。
3. **突破日前 25 天的窗口提取**：明确「不含突破当天」，避免突破大阳线扭曲指标，是"捕捉蓄势状态"的关键设计。
4. **市值多级降级**：CSV → akshare 实时 → 收盘×股本估算，每级有合理区间校验（`1e9 < cap < 1e11`），是国内免费数据源不稳定下的务实写法。
5. **钉钉限流器独立成类**：`RateLimiter` 把「每分钟 N 条 + 最小间隔 + 指数退避 + 锁定延长」封装，README 单独写一节讲踩坑经过，可复用于任何 IM 推送。
6. **通达信公式函数正倒序自适配**：`MA/EMA/KDJ/REF/EXIST/HHV/LLV` 都做了顺序检测/反转处理，保证倒序数据也能算对——比硬编码顺序更稳。

---

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `strategy/bowl_rebound.py` | 碗口反弹策略（指标/条件/分类/市值降级） |
| `strategy/base_strategy.py` | 策略基类（ABC） |
| `strategy/strategy_registry.py` | 策略注册表 |
| `strategy/pattern_matcher.py` | B1 四维相似度计算引擎 |
| `strategy/pattern_feature_extractor.py` | B1 特征提取（双线/KDJ/量能/形态） |
| `strategy/pattern_library.py` / `pattern_config.py` | 10 案例库 + 配置 |
| `utils/technical.py` | MA/EMA/KDJ/REF/EXIST/知行趋势线（通达信风格） |
| `utils/akshare_fetcher.py` | akshare+腾讯取数、CSV 落盘、市值降级 |
| `utils/dingtalk_notifier.py` | 钉钉 webhook + 限流器 |
| `utils/kline_chart.py` / `kline_chart_fast.py` | K 线+趋势线图生成 |
| `config/strategy_params.yaml` | 碗口反弹 + B1 匹配参数（实际生效值） |
| `main.py` | CLI（init/update/run/web/--b1-match/--category） |
| `web_server.py` | Flask Web |
| `B1_PATTERN_MATCH.md` | B1 匹配算法详解 |