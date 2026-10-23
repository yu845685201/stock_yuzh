# stock-analysis（wkingnet/stock-analysis）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：[github.com/wkingnet/stock-analysis](https://github.com/wkingnet/stock-analysis) · **分析依据**：本地克隆源码（`refrences/wkingnet__stock-analysis`，git commit `814f1d4` 2026-01-02）· **配套**：`../开源股票分析项目清单-20261010.md`
> **方法**：精读 `readTDX_lday.py`、`readTDX_cw.py`（数据导出）、`func.py`（取数/前复权）、`func_TDX.py`（通达信公式函数重写）、`CeLue模板.py`（策略模板）、`xuangu.py`（选股）、`huice.py`（回测）与 README。

---

## 0 定位与使用形态

wkingnet/stock-analysis 是一个**「数据导出 + 选股 + 回测衔接」三件套的 Windows 本地工具**，核心卖点是：**完全不依赖任何第三方在线行情 API，所有基础数据都来自本机通达信（TDX）软件导出的离线文件**（README：「力求选择最稳定可靠的数据获取方式……本项目所有数据依靠本地通达信软件导出提供。感谢通达信，真是个好公司！」）。

- 定位：作者是「趋势/投机」派（不做价投、不做超短），需求是「盘中选股 + 策略回测」，但通达信公式有局限（无循环、变量不能二次赋值、只能短周期），故用 Python 复刻「通达信式公式」并扩展到可盘中、可回测。
- 形态：几个独立 .py 脚本，流程串行：`readTDX_cw.py`（财报）→ `readTDX_lday.py`（日线前复权）→ `celue_save.py`（打策略信号）→ `xuangu.py`（选股）/ `huice.py`（rqalpha 回测）/ `plot.py`（K线信号图）。
- 技术栈：Windows + 通达信（开心果版）+ Python 3.8（anaconda）+ pytdx 1.72 + pyecharts 1.9 + rqalpha 4.2.5 + TA-Lib 0.4.19 + pandas 1.2。
- 声明：README 明说「除了选股策略以外，其他都可公开」「个人实际策略不分享」，仓库里的 `CeLue模板.py` 是示例策略模板 + 一套示例（并非作者实盘策略）。

---

## 1 功能模块全景

| 模块 | 位置 | 职责 |
|---|---|---|
| 财务报表导出 | `readTDX_cw.py` | 从通达信服务器下载 `gpcw*.dat/zp` 财务包，解析成 pkl |
| 日线导出+前复权 | `readTDX_lday.py` | 读本机 `vipdoc/sh(sz)/lday/*.day`，前复权转 csv/pickle |
| 数据工具 | `func.py` | 前复权 `make_fq`、盘口在线行情、块文件解析、多线程下载 |
| 通达信公式函数 | `func_TDX.py` | 用 pandas/talib 复刻 REF/MA/SMA/HHV/LLV/COUNT/CROSS/BARSLAST 等 |
| 策略模板 | `CeLue模板.py`（使用时改名 celue.py） | 策略 HS300 信号 + 策略1/2 + 卖策略 |
| 选股 | `xuangu.py` | 多进程两段漏斗选股，输出「已选出股票列表」 |
| 策略信号保存 | `celue_save.py` | 为历史数据打买卖信号，生成 `celue汇总.csv` |
| 回测 | `huice.py` | 用 rqalpha 回读 `celue汇总.csv` 下单回测 |
| 信号可视化 | `plot.py` | pyecharts 画个股 K 线 + 买卖区间（红赚绿亏） |
| 最佳服务器 | `pytdx_best_ip.py` | 测 pytdx 通达信服务器延迟选最优 IP |

---

## 2 数据源来源与采集机制

| 数据 | 来源路径 | 更新方式 |
|---|---|---|
| 日线数据 | 本机通达信 `vipdoc/sh(sz)/lday/*.day`（二进制 .day） | 通达信「盘后数据下载」手动/自动（16 点后） |
| 专业财务 | 通达信 `vipdoc/cw/gpcw????.dat`（二进制） | 服务器 `down.tdx.com.cn:8001/tdxfin/` 对 md5 增量下载 |
| 股本变迁 | 通达信 `T0002/hq_cache/gbbq`（股本变迁） | 通达信运行时自动更新 |
| 板块/行业 | `T0002/hq_cache/block_gn.dat`、`tdxhy.cfg` | 通达信本地文件 |
| 盘中实时行情 | pytdx `TdxHq_API`（TCP 7709，`pytdx_ip/pytdx_port` 可配） | 交易时段在线拉取 |

**前复权（`func.make_fq`）是本项目数据层最核心的原创逻辑**：把如山高堆积的「股本变迁（gbbq）+ 专业财务」结合，等比（复权因子连乘）重算历史价格，README 强调「导出数据自动前复权，可与通达信软件内显示价格完全一致，一分钱都不差（需在通达信设置1勾选『复权采用等比方式』）」——否则多年前价格会变负。

**日线导出流程**（`readTDX_lday.py`）：`check_files_exist` → `update_lday`（只追加当日，`del` 参数强制全量重建）→ `qfq()` 多进程前复权 → 落 csv + pickle（README：csv 性能差，后改 pickle 存盘）。

**盘中实时**（`func.get_tdx_lastestquote` / `update_stockquote`）：盘中把 pytdx 实时快照 `update` 进个股日线 DataFrame（仅内存，不落盘），若当日是权息日则用股本变迁 `make_fq` 现场重算前复权；`xuangu.py` 把当日实时行情**临时 pick 到本地 10 分钟缓存**（`df_today.pkl`，注释：防多次调用被服务器封 IP）。

---

## 3 分析逻辑

### 3.1 通达信公式函数复刻（`func_TDX.py`）

用 pandas/talib 把通达信公式语句逐条翻译成 Python，让策略代码「和通达信公式看起来一致」：

| 函数 | 实现 | 备注 |
|---|---|---|
| `REF(value, day)` | `Series.shift(day)` | 引用 N 周期前 |
| `MA(value, day)` | `talib.SMA(value, day).iat[-1]` | **返回具体值**（唯一标量） |
| `SMA(value, day)` | `talib.SMA(value, day)` | **返回序列** |
| `HHV/LLV(series, day)` | `rolling(day).max()/min()`，day=0 时循环算累进极值 | 注释「rolling 性能极差，慎用」 |
| `COUNT(cond, n)` | 显式循环：对每个 True 位置 `iloc[i:i+n]+1` | 注释「rolling 太慢，弃用」 |
| `EXIST(cond, n)` | `True in cond[-n:]` | N 期内存在 |
| `CROSS(s1, s2)` | `(s1>s2) & (s1.shift()<=s2.shift())` | 上穿 |
| `BARSLAST(cond)` | 循环计数距上次满足的天数 | |
| `BARSLASTCOUNT(cond)` | 循环计数连续满足天数 | |
| `VALUEWHEN(cond, v)` | 条件成立取值并 `ffill` | |

### 3.2 策略模板（`CeLue模板.py`）

模板规定四个函数（用户照猫画虎写自己的 `celue.py`），并内附一套示例策略：

- **策略HS300(df_hs300)**：HS300 大盘择时信号。`当日涨幅 = close/REF(close,1)-1`；信号 = **涨跌幅 ∈ (-1.5, 1.5)**（当日既不大涨也不大跌则允许买入）。选股前先判大盘：HS300 信号为假则当日不买（`xuangu.py` 里仍出选股结果但标红不执行买入）。
- **策略1(df, mode='fast')**：快速初筛（`mode='fast'` 只处理当日）。条件：上市满 500 日（`BARSLAST(close==0)>500` 或 `C.shape[0]<500` 跳过）且 `close>9` 元；**排除当日涨停**：`TJ04_2 = ~((C+0.01) >= ceil((floor(REF(C,1)*1000*板幅)-4)/10)/100)`，科创板 `68`/创业板 `30` 用 1.2 板幅、其余 1.1——即涨停判定按分位取整（`*1000` 后 `floor/ceil`，等价于两位小数比较，注释说「差一分钱也是没封住」的精神）。
- **策略2(df, HS300_信号)**：主策略。须 ≥251 日；条件：
  - TJ01 均线趋势：`MA120>-5` 且 `MA10<60` 且 `MA60<10` 且 `-7<MA250<10`（均线走平/微升中的形态）；
  - TJ02 位置：`C>MA60` 且 `C<MA60*1.1` 且 `C>O`（站上 MA60 且刚突破不超 10%、收阳）；
  - TJ06 回踩结构：`LLV(20)/LLV(200)-1<0.5` 且 `1 < (上穿MA60日MA60值/其后20日最低价) / (C/上穿MA60日MA60值) < 1.5`（用 `BARSLAST` 找上穿日，再单独回填该日 MA60 与 20 日最低）；
  - TJ11 首现过滤：`HS300信号 & 策略1 & TJ01 & TJ02 & TJ06` 在 10 日内 **首次出现**（`COUNT(TJ11_1,10)` 且 `REF(COUNT,1)==0`）。
- **卖策略(df, 策略2)**：三种卖出，取最先触发者（模拟通达信 `AUTOFILTER`）：
  - SELL01：`C<MA60` 且 `C<买入日开盘价`；
  - SELL02：`买入后涨幅<10%` 且 `H<REF(L,1)`（向下跳空缺口）；
  - SELL03：买入 N 天后（N = 流通市值<100 亿取 7 天、否则 14 天）涨幅在 (1%, 3%) 之间卖出。

### 3.3 选股（`xuangu.py`，两段漏斗 + 多进程）

1. `make_stocklist()`：从 `csv_lday` 目录生成代码列表 → 剔除通达信「ST板块」概念（`block_gn.dat`）→ 剔除指定「T1002」等行业（`tdxhy.cfg`）→ **强制剔除科创板**（`68` 开头）。
2. 判 HS300 信号（盘中拉 `000300` 实时更新）→ 信号为假时仍选股但提示「不执行买入」。
3. `run_celue1`（fast 初筛，多进程 `Pool(processes=CPU/1.5)`）→ `run_celue2`（主策略，多进程）→ 输出 `已选出股票列表`（当日买入信号名单，可直接对接下单程序）。
- 性能说明：README 注明「多进程时 df_dict 字典占内存过多反而慢，故改为函数内读单个 pkl」是踩坑后的优化；进程数 = `os.cpu_count()-2`（或 `/1.5`）。

### 3.4 策略信号与回测

- `celue_save.py`：对每只股票历史逐日跑「策略1+策略2+卖策略」，给每个交易日打上 `买入/卖出` 信号，**汇总成一张 `celue汇总.csv`**（列含 date/code/信号）。README 强调这步把回测效率从「循环 4000 股 × 5 年 ≈ 半小时」压到「1 分钟」。
- `huice.py`（回测）：`import rqalpha`，`init` 读入 `celue汇总.csv`（代码转 `XSHG/XSHE` 后缀），`before_trading` 按当日信号用 `order_percent`（总资产 10%）或 `order_target_value`（单票 10 万）下单（各参数在文件头变量里调）；卖出信号对应清仓。回测区间 `2013-01-01~2022-12-31`，初始资金 1000 万。
- `plot.py`：pyecharts 画个股 K 线 + 叠加买卖信号高亮持仓区间（红赚绿亏）。

---

## 4 最终产物结构与格式

- **本地数据文件**：`csv_lday/*.csv`（gbk 编码日线）、`pickle/*.pkl`、`cw/*.pkl`（财报）、`gbbq.csv`（股本变迁）、`index/000300.csv`（指数）。
- **策略信号**：`celue汇总.csv`（date, code, 买卖信号列），是「数据 → 任意回测框架」的中枢交换格式；README 建议「拿 celue汇总.csv 去任意回测框架，只需导入即可」。
- **选股结果**：控制台输出 `已选出股票列表`（`xuangu.py` 末尾的 list），README 明示「只需对已选出股票列表下达买入命令即可」对接自动化交易。
- **回测/图**：rqalpha 产出净值/收益报告存 `rq_result/`；`plot.py` 输出 `pyecharts.html`。

---

## 5 坑与注意事项（来自注释与 README）

1. **数据量与硬盘/性能**：README 明确「通达信导出的日线数据需要 4G 硬盘空间，强烈建议固态硬盘」；csv 性能差已改 pickle 存盘（历史注释记录的优化）。
2. **前复权必须勾选「等比方式」**：README 加粗强调，否则「很多股票多年前的价格是负的」；日线解析前复权与通达信一一对应是硬约束，改复权口径会破坏选股一致性。
3. **通达信公式复刻的性能陷阱**：`HHV/LLV/COUNT` 用 `rolling`+循环，`func_TDX.py` 顶部注释「HHV/LLV/COUNT 使用了 rolling 函数，性能极差，慎用」；`策略2` 里 `TJ06_MA60_DAY` 的 `BARSLAST` 回填是显式 Python 循环，全市场 4000 股 × 多年会很慢（故才需要 `celue汇总.csv` 一次性预计算）。
4. **盘中行情封 IP**：`xuangu.py` 注释「把当日实时行情临时保存到本地，防止多次调用被服务器封 IP」，缓存 10 分钟（`df_today.pkl` 的 `mtime < 600s`）；非交易时段自动删临时文件。
5. **个人策略不公开**：仓库只有模板+示例，作者实盘策略在本地 `celue.py`（不分享）；`xuangu.py`/`celue_save.py` 都 `import CeLue`（用户自建文件），克隆后需自己写策略才能跑通。
6. **通达信服务器 IP 可能变**：`user_config.py` 里 `pytdx_ip='218.6.170.55'` 硬编码，`pytdx_best_ip.py` 就是为了自动测「当前最优通达信服务器 IP」而生（服务器 IP 会封禁/变更）。
7. **财务文件下载损坏处理**：`readTDX_cw.py` 对 zip `BadZipFile/zlib.error` 删文件跳过并提示「服务器端文件错误」；对 `gpcw.txt` 用 md5 比对做增量。
8. **回测依赖 rqalpha 老版本**：README 列出 rqalpha 4.2.5、pyecharts 1.9、TA-Lib 0.4.19（cp37 wheel）——全是 Windows/anaconda 时代的锁版，现代 Python 环境装起来有门槛（README 给了 .whl 手工安装指引）。
9. **盘中选股窗口限制**：`9:00–16:00` 且周一至周五才取实时行情（`huice.py`/`xuangu.py` 里 `time.strftime` + `tm_wday` 判断），其余时间走离线数据；权息日需要现场重算前复权，逻辑较脆弱。

---

## 6 可借鉴的设计要点

1. **「本地数据优先、在线仅补盘中一口价」的抗脆弱架构**：把「数据稳定性」当作头等事（README 原话「完全是把程序主动权交到对方手里」是作者反对的），日线/财报/股本全部本地化，只在盘中拉一口实时价做内存合并——对个人量化的可持续性极有借鉴意义。
2. **复刻「通达信公式语言」到 pandas 的语义层**：`func_TDX.py` 把 `REF/MA/HHV/LLV/CROSS/BARSLAST/COUNT` 做成与通达信同名的 Python 函数，让通达信老手直接迁移公式、结果可对照排查——是「降低迁移成本」的很好示范。
3. **`celue汇总.csv` 作为「策略信号 → 任意回测框架」的中枢**：把重计算（逐日打信号）与轻消费（回测/画图/选股）解耦，一次预计算多处复用；README 用「半小时 vs 1 分钟」的量化对比说明收益。
4. **卖点明确、取舍坦率**：README 花大量篇幅写「量化能不能赚钱」的认知（工具≠圣杯、策略思想才是核心），把项目定位收敛到「数据导入 + 选股」，回测交给 rqalpha，不重复造轮子。
5. **模拟通达信 `AUTOFILTER`**：卖策略用「循环找首个成立信号位」来等价通达信公式的 `AUTOFILTER`（过滤重复信号），并注释说明，是在 pandas 里实现公式语义的一次性解决方案。
6. **涨跌停判定按分位取整**：`TJ04` 用 `floor(REF(C,1)*1000*板幅)` 的两位小数精度比较，是「涨停判定差一分钱也不行」的严谨写法（与 vibe-astock 的思路一致）。

---

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `readTDX_cw.py` | 财务包 md5 增量下载 + gpcw.dat 解析成 pkl |
| `readTDX_lday.py` | 日线 .day 解析 + 前复权 + csv/pickle 落盘 |
| `func.py` | `make_fq` 前复权、盘口实时、块文件解析、多线程下载 |
| `func_TDX.py` | 通达信公式函数复刻（REF/MA/SMA/HHV/LLV/COUNT/CROSS/BARSLAST…） |
| `CeLue模板.py` | 策略模板 + 示例（HS300信号/策略1/策略2/卖策略） |
| `xuangu.py` | 多进程两段漏斗选股主程序 |
| `celue_save.py` | 逐日打买卖信号 → celue汇总.csv |
| `huice.py` | rqalpha 回测（读 celue汇总.csv 下单） |
| `plot.py` | pyecharts K线 + 买卖信号图 |
| `pytdx_best_ip.py` | 通达信服务器延迟测试选 IP |
| `user_config.py` | 路径/服务器/指数配置 |
| `util_docs/专业财务文件字段含义对照表.txt` | 财报 .dat 字段释义 |