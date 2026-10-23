# short-term-stock-picker（online0001）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：[github.com/online0001/short-term-stock-picker](https://github.com/online0001/short-term-stock-picker) · **分析依据**：本地克隆源码（`refrences/online0001__short-term-stock-picker`，git commit `9135420` 2026-04-22）· **配套**：`../开源股票分析项目清单-20261010.md`
> **方法**：精读 `scripts/pick_stocks.py`（唯一核心脚本）、`SKILL.md`（OpenClaw Skill 元数据）与 README（中英双语）。

---

## 0 定位与使用形态

Short-Term Stock Picker 是一个 **OpenClaw Skill 形态的 A 股短线强势股筛选器**，定位为「供 LLM Agent 调用的股票分析 skill」。核心链路：用 AKShare 拉近 20 交易日涨停股池 → 逐股算均线/量比/换手率 → 打分 → 输出 CSV。

- 形态：`SKILL.md`（skill 元数据 + 触发词）+ `scripts/pick_stocks.py`（一个脚本通吃）+ README（中英双语，SEO 词堆砌「涨停板分析 skill / 打板工具」等）。
- 技术栈：Python 3.7+；`akshare >= 1.18`、`pandas`。
- 声明：仅供教育研究，不构成投资建议。

---

## 1 功能模块全景

| 模块 | 位置 | 职责 |
|---|---|---|
| Skill 元数据 | `SKILL.md` | name/description/触发词/条件速查 |
| 选股脚本 | `scripts/pick_stocks.py` | 涨停池聚合 → 逐股技术分析 → 打分 → CSV |
| 文档 | `README.md` | 双语文档、评分体系说明 |

---

## 2 数据源来源与采集机制

| 源 | 接口 | 内容 | 说明 |
|---|---|---|---|
| AKShare（东财） | `ak.stock_zt_pool_em(date)` | 近 20 交易日的每日涨停股池 | 逐日翻（`trade_dates[:20]`），每 5 日打印进度、`sleep(0.2)` |
| AKShare（新浪） | `ak.stock_zh_a_daily(symbol, start_date, end_date)` | 单股日线（近 40 天） | 用于算均线/量比/换手 |

**交易日生成**（`get_trade_dates`）：从今天往前推，`d.weekday() < 5` 就收进来，直到凑满 25 天——**只排周末、不排节假日**（纯工作日近似）。

**代码→symbol 映射**（`get_stock_hist_data`）：`600/601/603/605/688` 开头 → `sh`，其余 → `sz`。

---

## 3 分析逻辑（选股规则与阈值）

### 3.1 硬性过滤（`analyze_stock`）

1. **ST/退市**：名称含 `ST`/`*ST`/`退` 剔除。
2. **板块**：仅 `600/601/603/605/000/001/300/688` 开头（主板+深主板+创业+科创，不含北交所）。
3. **市值**：`流通市值 = 0` 或 `> 150 亿`（`MARKET_CAP_THRESHOLD = 15000000000` 元）剔除。
4. **历史数据**：不足 20 日剔除。

### 3.2 技术条件

- **均线**：`close ≥ ma5 且 close ≥ ma10 且 close ≥ ma20` 且 `ma5 > ma10 > ma20`（站稳+多头排列，缺一即剔除）。
- **量比**：`vol_ratio = 近5日均量 / 再前5日均量`（`tail(5)` vs `tail(10).iloc[:-5]`）。
- **换手率**：`近3日均换手 × 100`，须在 `0.5~10%` 之间（`turnover > 10 or < 0.5` 剔除）。
- **3 日涨跌**：`price_up_3d = 最新收盘 > 3 日前收盘`。

### 3.3 评分（`综合评分 = 涨停次数×20 + 技术评分 + 量比×5`）

技术评分 `tech_score`：
- 起步 `20 + 10`（均线多头基础分；代码写死 `20 + 10`，README 只列「均线多头 20」）；
- 量比 `≥1.5` +15 / `≥1.2` +10 / 其余 +5；
- `3日上涨 且 量比≥1.2` +10（量价配合）；
- `换手率 2~8%` +5（换手率稳定）。

最终 `total_score = limit_up_count * 20 + tech_score + vol_ratio * 5`，按分数降序排序。

### 3.4 输出（`main`）

- 控制台打印前 50 只；`result.csv`（utf-8-sig）列：`代码/名称/涨停次数(近20日)/流通市值(亿)/最新价/MA5/MA10/MA20/换手率/量比(近5日)/3日涨跌/综合评分/板块/所属行业`。

---

## 4 最终产物结构与格式

- **`result.csv`**：上述 15 列，按综合评分降序。
- **控制台**：条件清单 + 逐股「✅ 代码 名称 - 评分」+ 结果表格。

---

## 5 坑与注意事项

1. **输出路径硬编码到作者本机**：`output_path = '/Users/lulei/.openclaw/workspace/skills/short-term-stock-picker/result.csv'`——别人跑会在自己机器上找 `/Users/lulei/...`，要么权限出错、要么静默写失败。
2. **README 声称的功能未实现**：README/SKILL.md 多次写「资金流向估算 / 近3日主力净流入估算」，但 `analyze_stock` **没有任何资金流计算**——「资金流」只存在于文案，实际评分只用涨停次数+技术+量比。SKILL.md 第 5 条「近3日主力净流入估算」是空头支票。
3. **交易日只是排周末、不排法定节假日**：`get_trade_dates` 把节假日也算作交易日，导致 `stock_zt_pool_em(date)` 对节假日返回空、靠 try/except 静默跳过，实际扫描的交易日数可能不足 20。
4. **评分公式与 README 表不完全对应**：代码 `tech_score = 20 + 10` 起步（README 只写「均线多头 20」未提 +10）；README「换手率稳定 5 分」实际是「换手率 2~8% +5」；量比权重 `×5` 在 README 有但代码里量比未做上限封顶。
5. **量比除零/停牌**：`vol_prev5` 为 0 时 `vol_ratio` 回退为 1；停牌股 `ak.stock_zh_a_daily` 可能返回旧数据（无 staleness 校验），会把停牌/退市股的陈旧 K 线当最新。
6. **`sh`/`sz` 前缀规则缺北交所**：`get_stock_hist_data` 只认 `6xx→sh、其余→sz`，但硬过滤已把 `8` 开头北交所排除，故逻辑自洽但扩展性差。
7. **逐股 `sleep(0.25)` 全市场串行**：涨停池几百只逐股拉新浪日线 + 0.25s 间隔，单次全流程可达数分钟。

---

## 6 可借鉴的设计要点

1. **Skill 形态封装**：`SKILL.md` 用 frontmatter（name/description/credentials/requirements）描述触发词与依赖，把「命令行脚本」包装成「可被 Agent 识别的能力」，description 里密集列出同义触发词（「帮我选短线股 / 找强势股 / 筛选股票」），是 AI 生态里「让工具被准确路由」的做法。
2. **免 Token**：只用 AKShare 免费接口（东财涨停池 + 新浪日线），个人零门槛。
3. **评分公式直观可调**：`综合 = 涨停×20 + 技术 + 量比×5`，权重、阈值都是单行常量，README 列成表格便于 LLM 复述/调参。
4. **多板兼容**：板块段把主板/深市/创业/科创都纳入，代码 `get_stock_hist_data` 有对应前缀映射。

---

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `scripts/pick_stocks.py` | 核心选股脚本（涨停池聚合/过滤/评分/CSV） |
| `SKILL.md` | OpenClaw Skill 元数据与条件速查 |
| `README.md` | 双语文档、评分体系、输出字段 |