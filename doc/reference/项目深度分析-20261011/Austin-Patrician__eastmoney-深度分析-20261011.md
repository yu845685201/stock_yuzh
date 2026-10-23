# VAlpha Terminal（eastmoney 仓库）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：https://github.com/Austin-Patrician/eastmoney · **分析对象版本**：main 分支（commit `e293bf7`，2026-02-06）
> **分析依据**：本地克隆源码（`refrences/Austin-Patrician__eastmoney`），精读 `src/analysis/`、`src/data_sources/`、`src/llm/`、`src/scheduler/`、`app/routers/`、README。仓库名 `eastmoney`，项目名 VAlpha Terminal。
> **配套文档**：../开源股票分析项目清单-20261010.md、../开源股票分析项目深度分析-20261011.md

---

## 0. 定位与使用形态

专注 A 股的智能金融情报终端（Web：FastAPI 后端 + React 19 前端 + Vite 7 + MUI 7 + Tailwind），集成投资组合、基金分析、股票监控、AI 推荐、新闻聚合与市场情绪分析，由 LLM 自动生成盘前策略与盘后复盘报告。本地自托管（Docker Compose 一体化，前端 :9000），非商业许可证。

技术栈：Python 3.10+ / FastAPI / SQLite（`funds.db`）/ 可选 Redis；APScheduler 定时（默认盘前 08:30、盘后 15:30）；LLM 支持 Gemini/OpenAI/DeepSeek 等兼容接口；Tavily 实时网络搜索。

---

## 1. 功能模块全景

| 栏目/模块 | 能力 |
|---|---|
| 投资组合 Portfolios | 多组合管理、收益分析、压力测试、AI 诊断 |
| 基金分析 Funds | 基金池管理，盘前/盘后报告按基金类型分流策略 |
| 股票监控 Stocks | 自选股基本面+技术面综合分析 |
| AI 智能推荐 Recommendations | 量化因子模型初筛 + LLM 深度分析（选股选基） |
| 新闻资讯 | 多源财经新闻聚合 + AI 情绪分析 |
| 商品情报 Commodities | 黄金/白银等贵金属实时分析 |
| 市场情绪 Sentiment | 涨停/炸板池驱动的情绪周期 + 舆论情绪 + 资金流 |
| 邮件通知 | 报告生成、异常提醒推送 |
| 定时任务 | APScheduler 自动化报告生成调度 |

---

## 2. 数据源来源与采集机制

### 2.1 数据源策略（`src/data_sources/`）

`DATA_SOURCE_PROVIDER = hybrid | tushare | akshare` 三选一，`data_source_manager.py` 统一管理，`DATA_SOURCE_CACHE_TTL=60` 秒缓存。

| 源 | 客户端 | 内容 | 鉴权 |
|---|---|---|---|
| **TuShare Pro** | `tushare_client.py` | `daily`(日线)、`stk_factor`(MACD/KDJ/RSI/BOLL,5000 积分)、`fina_indicator`(ROE/毛利率/负债,2000)、`income/balancesheet/cashflow`(5000)、`daily_basic`(PE/PB/市值)、`moneyflow`(大中小单资金,2000)、`moneyflow_hsgt`(北向,免费)、`forecast`(业绩预告) | TUSHARE_API_TOKEN（推荐配置提高稳定性） |
| **AKShare** | `akshare_api.py` | `stock_zt_pool_em`(涨停池)、`stock_zt_pool_zbgc_em`(炸板池)、`stock_zt_pool_previous_em`(昨日涨停)、`stock_zt_pool_dtgc_em`(跌停池)、板块/资金流等 | 免 key |
| **yfinance** | `yfinance_client.py` | 港美股行情/K线（辅助） | 无 |
| **Tavily 搜索** | `web_search.py` | 实时网络搜索（情绪分析用） | TAVILY_API_KEY |

### 2.2 采集层防护

- **熔断器** `circuit_breaker.py`：三态 CLOSED/OPEN/HALF_OPEN，`failure_threshold=5`、`timeout=300` 秒。连续 5 次失败 OPEN（走 fallback），300 秒后 HALF_OPEN 试探，试用成功 CLOSED/失败立即 REOPEN。状态存 cache（TTL 3600）。
- **限流器** `rate_limiter.py`（`src/data_sources/rate_limiter.py` + `factor_store/rate_limiter.py`）。
- **重试** `tushare_call_with_retry`（TuShare 调用统一重试包装）。
- **缓存** `src/cache/cache_manager.py`。

### 2.3 采集层的坑（源码注释）

- **TuShare 返回字符串**：moneyflow 数值列可能是字符串，`_get_stock_moneyflow` 对 `buy_sm_vol...net_mf_amount` 逐列 `pd.to_numeric(errors='coerce').fillna(0)`。
- **akisahre 涨停列名靠猜**：`market_cycle.py` 用 `'连板数' in df_zt.columns`、`'涨跌幅'` 判断，注释"假设列名包含 '连板数'"——akshare 涨跌停池列名不稳。
- **跌停池接口不稳**：`stock_zt_pool_dtgc_em` 可能不存在，用 try/except 忽略（"跌停数据不如涨停好找，可以用实时行情筛选"）。
- **stk_factor 类接口需高积分**：标注 5000/2000 积分门槛，不满足时该因子为 None，`compute_score` 按实际可用权重归一化（`weight_sum`），缺因子不打断。

---

## 3. 分析逻辑

### 3.1 策略模式架构（`src/analysis/`）

每个分析策略继承 `AnalysisStrategy`，实现 `collect_data(mode)` 与 `generate_report(mode, data)`。`StrategyFactory.get_strategy` 按资产类型分流：

| 策略 | 触发条件 |
|---|---|
| `StockStrategy` | type=stock |
| `CommodityStrategy` | 基金名含 黄金/白银/有色/油/石油/贵金属/商品 |
| `EquityStrategy` | 默认股票型/混合型基金 |

`PreMarketAnalyst` / `PostMarketAnalyst`（继承 `BaseAnalyst`）遍历标的，`get_strategy → collect_data(mode='pre'/'post') → generate_report`，产出 Markdown 报告落 `reports/`。

### 3.2 量化因子初筛 + LLM 深化（AI 推荐核心）

推荐链路（`src/analysis/recommendation/`）：**因子引擎算分初筛 → LLM synthesize 解释**。股票引擎三个因子集：

**技术因子**（`stock_engine/factors/technical.py`，"Predict breakouts, don't chase rallies"）：
- **盘整分 consolidation_score**（0-100）：波动率分 `100 − 日收益std×2000`×0.4 + 区间收窄分 `(2−后段/前段区间比)×50`×0.35 + 靠近前高分 `100 − 距20日高差×500`×0.25。
- **量能先行 volume_precursor**：近 10 日均量/前 10 日均量（1.5x=50,2x=75,2.5x=100）×0.6 + 价格稳定分 `100−涨幅×20`×0.4；**仅当量增≥1.3 且价格变动<5% 才给满分口径**（否则降权 0.7）。
- **均线收敛 ma_convergence**：当前 MA5/10/20/60 价差分 `100−价差%×8`×0.4 + 收敛比分（<1 收敛加分，ratio<0.7=100）×0.6。
- RSI/MACD（`normalize=50+macd×20`）/布林位置（0=下轨 100=上轨）。

**基本面因子**（`fundamental.py`，"Quality > Market cap"）：
- 阈值：`ROE_MIN_THRESHOLD=10`、`DEBT_MAX_THRESHOLD=70`、`OCF_MIN_RATIO=0.8`。
- 质量分 = ROE 35%（<10% 只给 30 封顶，10%=50/15%=70/20%+=90）+ 毛利率稳定 20%（std<2=100,std>10=0）+ 现金流质量 25%（OCF/净利≥0.8 给高分）+ 负债健康 20%（>70% 扣分）。
- 增速分 = 营收 CAGR 40% + 净利 CAGR 40%（0%=30/10%=50/20%=70/30%+=90）+ 增长加速 20%（YoY−CAGR）。
- 估值分 = PEG 50%（<0.5=100/0.5-1=90-80/1-1.5=80-60/>2 递减）+ PE 分位 25% + PB 分位 25%（分位越低越便宜分越高）。

**情绪/资金因子**（`sentiment.py`，"Look for accumulation, not already-pumped"）：
- `main_inflow_5d`：5 日主力净流入（大单+特大单买−卖）/日均大单量，归一 `50+main×100`。
- `main_inflow_trend`：前后 5 日净流入加速比，`50+trend_ratio×25`（0-100）。
- `north_inflow_5d`：北向 5 日净流入（`50+total/1000`）。
- `retail_outflow_ratio`：散户卖出占比（逆向指标，>0.55 视为吸筹）；`is_accumulation_signal`：主力流入>0.1 且趋势>55 且散户流出>0.55，三选二。

**短线策略**（`strategies/short_term.py`，7-30 天持有）：技术 40% + 吸筹 25% + 催化 20%（当前恒 50，占位待接入财报日历）+ 风险调整 15%；`MIN_SCORE_THRESHOLD=60` 推荐、`HIGH_SCORE_THRESHOLD=80` 高置信。风险分：RSI 35-65=80/超卖<25=70（机会）/超买=30，布林中区 30-70=80，负债<50=80。

### 3.3 市场情绪周期（`analysis/sentiment/market_cycle.py`）

程序先算核心指标（akshare 涨停/炸板/昨日涨停/跌停池）：
- **封板率 seal_rate** = 涨停数/(涨停+炸板)×100；
- **最高板 market_height** = 涨停池"连板数"最大值 + 连板梯队分布；
- **赚钱效应** = 昨日涨停股今日平均涨幅 `avg_profit` + 红盘率 `profit_rate`。

`determine_cycle_phase` 规则引擎（LLM 可润色）：

| 阶段 | 判定条件 |
|---|---|
| 冰点/退潮 Ice Point | 跌停>20 且 涨停<30 |
| 强分歧 Divergence | 封板率<60 且 炸板>20 |
| 高潮/主升 Climax | 封板率>75 且 涨停>50 |
| 发酵/启动 Fermentation | 涨停>40 且 最高板≥3 |
| 震荡/轮动 Rotation | 其余 |

### 3.4 盘前/盘后报告（LLM prompts）

`src/llm/prompts.py`、`stock_diagnosis_prompt.py`、`recommendation_prompts.py` 定义模板。盘前（08:30）/盘后（15:30）由 APScheduler 触发（`src/scheduler/manager.py`），按标的类型走对应策略，LLM 结合 `collect_data` 收集的行情/资金/新闻 + Tavily 搜索生成报告。

---

## 4. 最终产物结构与格式

| 产物 | 结构 | 落盘/通道 |
|---|---|---|
| 盘前策略报告 | LLM 生成的 Markdown（标的名称/代码 + 策略结论） | `reports/` 目录 + 邮件通知 |
| 盘后复盘报告 | 同上 | `reports/` + 邮件 |
| AI 推荐 | 因子分 + 置信度（high/medium/low）+ 因子拆解 + key_factors（≤5 条）+ LLM 解释 | `/api/recommendations/v2` |
| 情绪分析报告 | 情绪周期阶段 + 封板率/最高板/赚钱效应指标 + LLM 情绪总结 | `/api/sentiment/analyze` |
| 组合报告 | 收益分析 + 压力测试 + 相关矩阵 + AI 诊断 | `/api/portfolios` |
| 基金报告 | 盘前/盘后按类型分流策略 | `reports/` |

推荐结果 schema：`{score, is_recommended, confidence, holding_period, strategy_type, factor_scores{technical/accumulation/catalyst/risk}, key_factors[]}`。

---

## 5. 坑与注意事项（来自代码注释/README 边界说明）

1. **因子积分门槛**：`stk_factor`(5000 分)/`moneyflow`(2000 分)/`fina_indicator`(2000 分)/三表(5000 分)——低积分 token 下因子为 None，评分按可用权重归一化（缺因子不打断，`weight_sum` 兜底）。
2. **akshare 涨停池列名不稳**：`market_cycle.py` 用 `'连板数' in columns` 防御性判断，注释"假设列名包含 '连板数'"。
3. **跌停池接口可能不存在**：`stock_zt_pool_dtgc_em` 用 try/except 忽略，"跌停数据不如涨停好找"。
4. **catalyst 因子是占位**：`_compute_catalyst_score` 恒返回 50（"actual implementation needs earnings calendar"）——短线的"事件催化"20% 权重实际是中性。
5. **TuShare 字符串列**：moneyflow 数值列需 `pd.to_numeric` 转换。
6. **港股历史行情字段**：README（所长楼）提及部分接口错误待修（BlueNote），数据源偶发不稳定。
7. **非商业许可证**：禁商用/销售/付费服务/商业集成。
8. **默认账号**：README/部署建议修改 JWT 认证默认凭证。

---

## 6. 可借鉴的设计要点

1. **策略模式 + 因子引擎 + LLM 三分离**：`AnalysisStrategy.collect_data/generate_report` 把"采数据"与"写报告"解耦；推荐走"确定性因子初筛 → LLM 解释关键因子"——LLM 不参与算分，只解释 `key_factors`。
2. **熔断器 + 限流 + 重试三件套**：CLOSED/OPEN/HALF_OPEN 状态机（阈值 5 次、300 秒试探），对 TuShare 这类限流下游是标准做法。
3. **因子分"按实际可用权重归一化"**：`weight_sum` 只算非 None 因子权重，缺数据因子不把总分拉偏——比固定权重更稳健。
4. **"预测突破不追涨"的因子设计**：盘整分/量能先行/均线收敛三个自研因子，配合"量增≥1.3 且价格变动<5%"的吸筹门控，是短线量价逻辑落到可测代码的范例。
5. **情绪周期规则引擎 + LLM 润色**：程序算封板率/最高板/赚钱效应，规则引擎给 5 阶段粗判，LLM 只做语言润色——数字不交给模型。

---

## 7. 主要源文件索引

| 文件 | 内容 |
|---|---|
| `src/analysis/recommendation/stock_engine/factors/technical.py` | 技术因子（盘整/量能先行/均线收敛/RSI/布林） |
| `src/analysis/recommendation/stock_engine/factors/fundamental.py` | 基本面因子（ROE/毛利率/OCF/负债/CAGR/PEG/分位） |
| `src/analysis/recommendation/stock_engine/factors/sentiment.py` | 情绪资金因子（主力/北向/散户逆向） |
| `src/analysis/recommendation/stock_engine/strategies/short_term.py` | 短线策略（40/25/20/15 权重，60/80 阈值） |
| `src/analysis/recommendation/stock_engine/strategies/long_term.py` | 长线策略 |
| `src/analysis/recommendation/fund_engine/` | 基金选基因子（alpha/momentum/risk/performance/manager） |
| `src/analysis/recommendation/llm_synthesis/explainer.py` | LLM 因子解释 |
| `src/analysis/strategies/`（factory/base/equity/stock/commodity） | 策略模式与工厂分流 |
| `src/analysis/pre_market.py` / `post_market.py` | 盘前/盘后分析编排 |
| `src/analysis/sentiment/market_cycle.py` | 情绪周期（akshare 涨跌停池 + 5 阶段规则） |
| `src/analysis/sentiment/{social_media,news_mining,money_flow}.py` | 舆论/新闻/资金流情绪 |
| `src/analysis/portfolio/{risk_metrics,stress_test,correlation,signals}.py` | 组合风险/压力测试/相关矩阵 |
| `src/analysis/commodities/quantitative.py` + `gold_silver.py` | 贵金属商品分析 |
| `src/analysis/fund/{diagnosis,risk_metrics,comparison,portfolio_analysis}.py` | 基金诊断/风险/对比 |
| `src/data_sources/`（tushare_client/akshare_api/yfinance/circuit_breaker/rate_limiter/data_source_manager） | 数据源与防护 |
| `src/llm/`（prompts/stock_diagnosis_prompt/recommendation_prompts/client/tools） | LLM 提示词与工具 |
| `src/scheduler/manager.py` | APScheduler 定时任务 |
| `src/report_gen.py` | 报告生成入口 |
| `src/services/{email_service,news_service,ai_scenario_service,assistant_service}.py` | 邮件/新闻/AI 场景/助手服务 |