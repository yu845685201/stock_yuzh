# tick-stock-panel（TSP）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：https://github.com/shy3130/tick-stock-panel · **分析对象版本**：main 分支（commit `c5ffe38`，2026-10-11）
> **分析依据**：本地克隆源码（`refrences/shy3130__tick-stock-panel`），精读 `backend/app/`（indicators/strategy/backtest/services/data_providers/custom）、`tiers.yaml`、`README.md` 与 `docs/`，结论均来自源码注释与实现。
> **配套文档**：../开源股票分析项目清单-20261010.md、../开源股票分析项目深度分析-20261011.md

---

## 0. 定位与使用形态

自托管、零运维的 A 股量化工作台，Docker 单容器部署（数据全落本地 Parquet + DuckDB，无外部数据库）。后端 Python（FastAPI）+ Polars 计算引擎，前端 React + TypeScript。**不看盘/不荐股/不对标同花顺通达信**（README 明示），定位"本地量化方案样板"。

对外能力全开放：**61 个 API 端点**（Token 六档权限）、**12 个 MCP 工具**（`mcp-server/src/tsp_mcp_server/server.py`）、SSE 事件流。CI 守护 2400+ 测试。作者 `shy3130`，MIT。

| 维度 | 实现 |
|---|---|
| 部署 | Docker 单容器（`docker-compose.yml`、`Dockerfile`） |
| 存储 | 本地 Parquet + DuckDB（`backend/app/parquet.py`、`db_safe.py`） |
| 计算引擎 | Polars（毫秒级全市场扫描） |
| 无外部依赖 | 零外部数据库、数据不落云端 |

---

## 1. 功能模块全景

| 栏目/模块 | 能力 |
|---|---|
| 看板 Dashboard | 情绪分 + 涨跌/成交额榜单 + 概念/行业领涨领跌 + 大盘异动事件流；12 列拖拽吸附网格 |
| 自选 Watchlist | 多分组（M:N）、表格/卡片双视图、换手/量比/RSI 实时指标 |
| 策略 Screener | Polars 扫描全 A，日线/分钟统一池，按声明周期路由执行；25 内置策略 + 自定义信号 + AI 生成策略 |
| 回测 Backtest | 因子/策略/分钟三类；净值/回撤/夏普/胜率/蒙卡回撤 + 因子归因 |
| 因子 Factors | DSL 25 算子 + IC·分层·Newey-West 检验 + 版本管理 |
| 挖掘 Mining | 嵌套样本外多因子组合搜索，显式发布、永不自动上线 |
| 个股分析 Stock Analysis | 日K + 9 类关键价位 + AI 四维分析（技术/基本面/财务/消息面） |
| 财务分析 Financials | 三表 + 关键指标（多源并集 + fuyao 四表）+ AI 解读 |
| 市场环境 Regime | 情绪周期 6 阶段 + 概念/行业主线排名 + 5 档情绪分 |
| 连板梯队 Limit Up Ladder | 连板层级统计 + 概念/行业分布 + 封单监控（可切连跌梯队） |
| 监控中心 Monitor | 四类规则 AND/OR + 弹窗/语音播报/飞书/企微推送 |
| 异动监控 Abnormal | 竞价/盘中/偏移三 tab（交易所异动偏离值口径） |
| 复盘 Recap | 盘后 AI 自动生成八节报告，注入龙虎榜资金动向，可定时、推飞书、下载 Markdown |
| AI 对话助手 | 21 查询工具 + 4 动作工具（写操作弹确认卡） |
| Open API / MCP | 61 端点契约化 + 12 MCP 工具 |

---

## 2. 数据源来源与采集机制

### 2.1 能力路由矩阵（数据源插件化核心）

数据层核心设计：把 6+1 类数据集抽象为"能力"（capability），每个能力**独立路由**到任意源，换源不改口径（`data_providers/capabilities.py`）：

| 能力 id | 内容 | TickFlow 档位门槛 | 默认源 |
|---|---|---|---|
| daily | 历史 K 线 + 实时覆写 | none（免费通道历史日K） | tickflow |
| adj_factor | 除权因子（前复权基准） | starter | tickflow |
| realtime | 全市场实时快照 | starter | tickflow |
| minute | 分钟 K（分时/分钟回测） | pro | tickflow |
| depth5 | 五档盘口（封单） | pro | tickflow |
| financial | 财务指标/三表 | expert | tickflow |
| full_minute | 全市场当日分钟落盘 | expert | tickflow |

每个能力的 `candidates`（候选源）+ `pending`（未就绪）+ `usable`（生效源可用）由 `build_capability_matrix` 一次算出，前端设置页据此置灰/提示。

### 2.2 数据源明细

| 源 | 接口/获取 | 内容 | 限速/档位 |
|---|---|---|---|
| **TickFlow** | `api.tickflow.org`（付费端点）+ free-api 服务器 | 行情/日K/分钟K/五档/复权因子/财务/WS | 五档限速见 `tiers.yaml`（如 none=日K批量 60 次/分×100 标的；expert=quote 300 次/分×500 标的、WS 订阅 100） |
| **fuyao（同花顺官方）** | `fuyao.aicubes.cn` REST，`FUYAO_API_KEY` | 实时快照/日K（10d dump 秒筛）/除权因子（涨跌停自检）/财务四表 | datasets: realtime/daily/adj_factor/financial；分钟未接入自动回退 TickFlow |
| **自定义源** | `data_providers/custom/`（plugin 机制） | 任意数据集声明 | 见 `docs/custom-data-source.md` |
| **同花顺板块** | ths 维度数据 | 概念/行业涨幅轮动矩阵、盘前风向标 | 板块维度轮动 |

### 2.3 采集层的坑（源码注释/CHANGELOG/CONTRIBUTING 记录）

- **"日K热门路径内存"**：指标管线全 Polars 表达式（SQL 窗口无法表达递归 EMA），100% 表达式免逐条 Python。
- **涨跌停价格用"分"整数运算**：`polars_limit_price` 用 `((prev*100+0.5).floor()*numerator+50)//100/100` 的半调四舍五入（`price_limits.py`），逐分精确对齐交易所。
- **制度随时间变**：主板 ST 2026-07-06 起 10%（`MAIN_BOARD_ST_LIMIT_CHANGE_DATE`）；注册制新股上市初期无涨跌幅窗口（主板/创业板/科创板 5 个交易日、北交所 1 日），按 `listing_date` + 板块改革日期判定；无交易日历时用"保守日历天边际"（5 日≈15 天）"宁多标勿漏标"（漏涨停标记代价高于多标）。
- **偏离值用分母对齐**：个股 N 日累计涨跌幅 − 对应指数同期涨跌幅（`deviate_Nd`），板块基准按 SH/STAR/SZ/GEM/BJ 分别减对应指数（科创板减科创50、创业板减创业板综指）。
- **盘中节假日不叠第二次**：缓存日早于今天且周末/休市时，`includes_today=True` 不再叠加"今日涨跌"，否则"把最近交易日涨跌算两遍"（`abnormal_moves.py` 注释）。
- **评分模型对齐看板**：`regime_builder` 与 `market_overview_builder` 共用 `_score(low,high)` 归一化与阈值，避免两处口径漂移。

---

## 3. 分析逻辑

### 3.1 指标流水线（68 列 enriched）

`indicators/pipeline.py`（2380 行）把基础数据换成 68 列指标信号，一次性算表落 enriched Parquet：

- **基础列**：`consecutive_limit_ups/downs`（递推连板数，从历史 cum_sum）、`amplitude`（振幅）、`change_pct`；
- **均线/EMA**：MA5/10/20/60、EMA 系列、MACD(dif/dea/hist)、BOLL(MA20±2σ)、KDJ、ATR14、RSI、量比 `vol_ratio_5d`（成交量/5日均量）；
- **偏离列**：`deviate_3d/10d/30d`（异动监控用）；
- **信号列**：`signal_*` 布尔列不再硬编码，改由自定义信号（`custom_signals.build_expressions`）注入计算——靠 `get_signal_dependencies()` 维护依赖闭包、按需分 Pass（Pass1 均线/EMA/MACD 基础，Pass2 依赖基础列）。

### 3.2 自定义信号（30 个预置种子）

`s`信号` 是策略/回测/监控的统一最小单元，全部为"字段 + 运算符 + 阈值"声明式条件（`strategy/signal_seeds.py`）。预置 30 个（首次写入用户目录后可编辑）：涨停/跌停/炸板/跌停翘板、MA5/10/20 突破跌破、MA5↔MA20 金叉死叉、MA20↔MA60 金叉、MACD 金叉死叉、60 日新高新低、放量（量比≥2）、跌破/突破布林上下轨。其中涨跌停类条件引用 `field:limit_up_price`（由 `compute_limit_signals` 用制度 + 分位整数运算注入）。

**炸板定义**（三条件 AND）：`raw_high > 0` 且 `raw_close < limit_up_price` 且 `raw_high >= limit_up_price`（盘中摸板但收盘未封住）。

### 3.3 市场情绪（5 档 state + 6 阶段 phase，双轨并存）

**5 档情绪分**（`regime_builder.py`）：四维加权 `WEIGHTS = {profit 0.35, speculation 0.25, resilience 0.20, trend 0.20}`：

| 维度 | 子项（权重） | 校准分位 |
|---|---|---|
| 赚钱 profit | 涨家数占比×0.45 + 均涨幅×0.25 + 中位涨幅×0.20 + 强弱差×0.10 | 涨家数 21/75、均涨幅 ±1.2%、强弱差 −13/14 |
| 投机 speculation | 涨停数×0.30 + 封板率×0.40 + 连板高度×0.30 | 涨停 35/97、封板率 57/75%、连板 4/9 |
| 抗跌 resilience | `100 − _score(大跌股占比, 2, 18)` | 大跌日此项暴降 → 总分进 weak |
| 趋势 trend | 指数涨幅×0.50 + MA20 上方占比×0.50 | 指数 ±2.5%、MA20 上方 22/76 |

`score ≥ 45 strong / ≥ 30 lean_strong / ≥ 20 range / ≥ 15 lean_weak / else weak`（阈值 `STATE_*`）。阈值用 A 股 2022-2026 真实 p15/p85 分位数校准。

**6 阶段情绪周期**（`market_phase.py`，纯函数，与 5 档并存）：由连板梯队（height 最高连板 / first_board 首板宽 / ge2·ge3·ge5 宽度 / promo 晋级率 / ladder_completeness 梯队完整度）驱动。阈值标定自 2020-08~2026-08 全市场 1454 交易日的 p10/p60/p90 分位数：

| 阶段 | 判定核心（EMA 平滑后） | 关键阈值 |
|---|---|---|
| 高潮 climax | ge2 ≥ 50 或 first_board ≥ 220 | p90 的 2 倍 / 2.5 倍（历史 <2% 天数） |
| 主升 rally | height≥7 且 ge2≥15 且 promo≥0.23，或 promo≥0.30 且 ge2≥12 且 height≥5 | p60 / p85+ |
| 冰点 ice | height≤4 且 ge2≤6 且 first_board≤24 | p10-p20，**优先于退潮**（长期死寂≠自高位退潮） |
| 退潮 ebb | 5 日前 ge2≥12（自高位回落）且 promo≤0.15 且 ge2 回落；或 promo≤0.13 且封板率≤0.57 | p20 |
| 启动 ignite | ge2 较 5 日前 +3 且 ge2≥8 且 promo≥0.20；或 height 较 5 日前 +1 且 height≥5 且 promo≥0.19 | — |

工程纪律：驱动量 EMA 平滑（α≈1/3，约 5 日）；阶段切换需**连续 2 个交易日同标签**（`_CONFIRM_DAYS=2`）；晋级率池 <10 家记 null（小样本噪声）；正面阶段（主升/高潮/启动）在大盘 5 档 state 为弱/偏弱时**被否决**降为修复（防 2024-01 微盘崩时中字头涨停生态把"主升"加在大盘弱势日）。

### 3.4 异动监控（竞价/盘中/偏移三 tab，交易所口径）

`abnormal_moves.py` 按交易所《交易规则》(2026 修订) 披露阈值计算"接近度"：

| 板块 | 异常波动（3 日偏离） | 严重异常波动 |
|---|---|---|
| 主板 | ±20% | 10 日 +100%/−50%、30 日 +200%/−70% |
| 创业板/科创板 | ±30% | 同上（各板块一致） |
| 北交所 | ±40% | 同上 |

偏离值 = 个股 N 日累计涨跌幅 − 对应指数同期涨跌幅。**接近度 = |实时偏离| / 该方向阈值**：≥1 触发（triggered）、≥0.7 边缘（edge）、≥0.5 观察（watch）。严重异常波动负向阈值显著严于正向（跌方向更早触发）。盘中实时叠加："历史偏离（已完成交易日）+ 今日实时涨跌 − 基准指数今日涨跌"，按板块基准分别减（不再全市场混均值）。

盘中 tab 信号（enriched 最新快照零新增采集）：涨停/炸板/跌停翘板/跌停/60 日新高/新低/放量（量比≥2），行序按优先级（涨停>炸板>翘板>跌停>新高>新低>放量），同级按 |今日涨跌| 降序。

### 3.5 AI 复盘（八节客观模板）

`market_recap.py`：与看板同源 `build_market_overview`（保证口径一致），将精简切片交给 LLM 流式生成（NDJSON meta/delta/error/done）。八节模板：①一句话定调②盘面总览③指数结构④板块主线⑤资金与情绪⑥消息催化⑦后续观察要点⑧风险提示。

**红线**：绝对不输出"进攻/防守/加仓/减仓/仓位/低吸/追高/反包"等交易指令——判断标准是"换成中立财经记者能不能写出来，能写就保留"。温度 0.5，1200-2000 字。复盘可选注入龙虎榜资金动向（fuyao 源）与盘前风向标（fuyao 竞价筛选名单 + 当日实际表现对照），新闻未接时降级为"从量价异动推断催化，不要编造消息"。

### 3.6 回测与因子（研究纪律最重的部分）

**策略回测**（`backtest/engine.py`）：成本模型拆分口径——佣金双边（`fees_pct=0.0002`）+ 印花税仅卖出 + 滑点双边（`slippage_bps=5.0`）；T+1、止损 `stop_loss_pct`、移动止损、移动止盈、`max_hold` 到期退出。退出优先级：历史挂单 > 风控（止损/移动止损/止盈）> signal（卖点）> max_hold > end。

**统计**（`backtest/stats_v2.py`）：因子 IC/IR 用 **Newey-West HAC t 值**（因 h 日前瞻收益有 h−1 阶 MA 自相关，主口径取 NW、滞后 L=h）、BH-FDR q 值、**Deflated Sharpe**（`deflated_sharpe_psr`，含期望最大夏普 `expected_max_sharpe` 的多重检验惩罚）；策略回测含蒙卡回撤、因子归因（胜/败单入场信号日因子对比）。

**因子 DSL**（`factors/dsl.py`）：25 算子（`ts_mean/ts_std/ts_sum/ts_max/ts_min/ts_delay/ts_delta/ts_rank/ts_zscore/ts_corr/ts_cov/ts_quantile/decay_linear` + 截面 `rank/zscore/winsorize` + 算术/比较），编译期错误码体系（E003 常量非数字、E005 前视、E009 截面嵌时序窗口、E013 缺列）。所有 `ts_*` 只向后看。

**挖掘**（`backtest/mining.py`）：嵌套样本外——训练区间因子方向重估 + 相关性去重 + 多因子排名组合搜索，候选入库"显式确认后才发布，永不自动上线"。

---

## 4. 最终产物结构与格式

| 产物 | 结构 | 落盘/schema |
|---|---|---|
| enriched Parquet | 68 列指标信号全市场表 | 本地 Parquet，按月分区原子写（`atomic_parquet_writes` 测试守护） |
| 策略回测报告 | 净值曲线/回撤/夏普/胜率/盈亏比/蒙卡 + 因子归因 + CSV 导出（概要/净值/交易明细/分标的） | `backtest/` 结果 JSON（`stats_v2` JSON-safe 序列化） |
| 因子检验 | IC/IR + 分层收益 + 多空 + Newey-West t + BH-FDR q | Factors 页四 tab |
| 市场环境 | 5 档 state + 6 阶段 phase + 概念/行业主线排名 | regime 表 |
| 异动监控 | 规则表 + 按接近度排序个股（triggered/edge/watch 计数） | `abnormal` API |
| AI 复盘 | Markdown 八节 + meta 摘要 | NDJSON 流 + 历史报告存储（`json_report_store`） |
| 监控通知 | 弹窗/语音播报/飞书/企微 | 触发记录持久化（`alert_store`） |
| AI 对话 | 富卡片（分时/日K 图自动渲染） | 工具足迹逐条可展开核对 |
| Open API | 61 端点契约化（6 Token 档权限） | 见 `docs/open-platform-plan.md` |
| MCP | 12 工具 | `mcp-server/README.md` |

---

## 5. 坑与注意事项（来自源码注释/CHANGELOG/README 边界说明）

1. **明确不做**：不对标同花顺/通达信、不内置"AI 荐股/涨停预测"（README IMPORTANT 块）。
2. **信号单源化**：`signal_*` 内置定义迁移为自定义信号 JSON，`builtin/__init__.py` 仅留向后兼容垫片，引擎不再扫该目录——防止"内置代码 + 用户 JSON"双源漂移。
3. **排名策略"研究专用"**：`factor_rank_research` 标 `research_only: True`，评分由研究生成、`scoring: {}` 空以防止框架把策略分替换掉。
4. **分钟数据档位门槛**：minute/depth5 需 Pro、financial/full_minute 需 Expert，档位不足时对应能力 `tf_available=False` 置灰，连板封单/盘口给缺数据提示（`usable` 门控而非套餐视角）。
5. **均价/成交额口径**：`change_pct/amplitude` 小数制（0.0366=3.66%），`turnover_rate` 已百分数（5.2=5.2%），指数 quote 又是百分数——三套口径，AI 助手 prompt 单独声明转换规则（`custom/assistant/prompt.py`"数据口径"节）。
6. **确认卡闸门**：4 个动作工具须用户点确认才执行，120 秒超时/溢出当"过期"而非"拒绝"反馈给模型（`actions.py`）——拒绝是用户不要，超时只是没人点。
7. **fuyao 股本无上游接口**：指标历史期建议切回 TickFlow 补齐（plugin.yaml 描述原文）。
8. **research 策略不自动上线**：挖掘候选"显式确认后发布，永不自动上线"，防自动 out-of-sample 污染。

---

## 6. 可借鉴的设计要点

1. **能力路由矩阵**：数据源按"能力"（数据集）独立路由，`build_capability_matrix` 一次算候选/待就绪/可用三态——换源不动指标与回测口径，是数据源易变环境下的解耦范式（与 vibe-astock 的"端点注册表 + 缓存层"思路同源但实现更彻底）。
2. **"声明式信号"作为统一最小单元**：信号（字段+运算符+阈值）→ 策略触发器 → 回测 → 监控四者复用同一表达式（`custom_signals.build_expressions`），指标管线零硬编码信号布尔式，改动一处全链路生效。
3. **双轨情绪指标**：5 档情绪分（4 维加权、p15/p85 校准）与 6 阶段情绪周期（连板梯队驱动、p10/p60/p90 标定 + EMA 平滑 + 2 日确认 + 大盘弱档否决）各司其职——前者给回测环境过滤，后者给主线识别，两者阈值集中一处随分位数可改。
4. **交易所口径的异动监控**：偏离值"个股−板块对应指数"、接近度分级（≥1/≥0.7/≥0.5）、严重异常波动负向阈值严于正向——把主观"异动"落到规则可验证。
5. **研究的统计严谨性**：Newey-West 处理 IC 自相关、Deflated Sharpe 处理多重检验、因子归因联系胜败单、挖掘嵌套样本外——面向"别被自己骗"的整套护栏。
6. **写操作确认卡 + 动作/查询分层**：查询工具直通、写操作（建信号/跑回测/改自选/补数据）弹确认卡，超时≠拒绝的语义区分，是 LLM Agent 接入生产系统的低成本安全闸。

---

## 7. 主要源文件索引

| 文件 | 内容 |
|---|---|
| `backend/app/indicators/pipeline.py` | 68 列 enriched 指标流水线（Polars 表达式 + 分 Pass） |
| `backend/app/price_limits.py` | 涨跌停制度/分位整数运算/注册制无涨跌幅窗口 |
| `backend/app/strategy/signal_seeds.py` | 30 个预置信号（声明式条件） |
| `backend/app/strategy/engine.py` / `scoring.py` | 策略执行引擎 + 截面 zscore 评分 |
| `backend/app/strategy/monitor_rules.py` | 四类监控规则（AND/OR、指数/异动校验） |
| `backend/app/services/market_phase.py` | 6 阶段情绪周期（阈值/EMA/2 日确认/否决） |
| `backend/app/services/regime_builder.py` | 5 档情绪分（四维加权） |
| `backend/app/services/abnormal_moves.py` | 异动监控（交易所偏离值/接近度/盘中信号） |
| `backend/app/services/market_recap.py` | AI 大盘复盘（八节模板 + NDJSON 流） |
| `backend/app/backtest/engine.py` | 回测引擎（成本模型/T+1/止损/退出优先级） |
| `backend/app/backtest/stats_v2.py` | Newey-West / BH-FDR / Deflated Sharpe |
| `backend/app/factors/dsl.py` | 因子 DSL（25 算子 + 错误码体系） |
| `backend/app/backtest/mining.py` | 嵌套样本外因子挖掘 |
| `backend/app/data_providers/capabilities.py` | 能力路由矩阵（6+1 能力注册） |
| `backend/app/plugins/fuyao/plugin.yaml` + `provider.py` | 同花顺官方 fuyao 数据源插件 |
| `backend/app/tickflow/`（client/rate_limits/scheduler/tiers） | TickFlow 数据源与限速调度 |
| `backend/app/custom/assistant/prompt.py` + `tools.py` + `actions.py` | AI 助手指令/数据口径/21 查询 + 4 动作确认卡 |
| `mcp-server/src/tsp_mcp_server/server.py` | 12 个 MCP 工具 |
| `tiers.yaml` | TickFlow 五档限速表（能力探测依据） |