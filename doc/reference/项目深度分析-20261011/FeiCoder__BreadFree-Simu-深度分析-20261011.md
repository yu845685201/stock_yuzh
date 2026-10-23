# BreadFree-Simu 深度分析

> 分析日期：2026-10-11 · 仓库 URL：https://github.com/FeiCoder/BreadFree-Simu · 分析依据：本地克隆源码（refrences/FeiCoder__BreadFree-Simu）· 配套：../开源股票分析项目清单-20261010.md

## 0 定位与架构总览

BreadFree 是**模块化、事件驱动的 A 股量化研究平台**，核心卖点是"把执行引擎与策略逻辑分离"——同一条回测引擎上可插拔经典算法策略（双均线/动量轮动）、基准（买入持有）与**复杂 LLM 多智能体系统**（LangGraph 投委会）。它本身不是投研 Agent 产品，而是**一个允许研究者把 LLM Agent 当"策略"塞进回测的实验框架**（README：simulation/research only）。

架构（README mermaid + 源码）：

```
main.py（CLI + config.yaml，参数优先级 CLI > config > 默认）
   ↓
BacktestEngine（事件循环，逐日 On Bar 喂数据给策略）
   ↓
DataFetcher（AkShare 拉数据 + 缓存本地 CSV + warmup）
   ↓ 策略接口 BaseStrategy（on_bar 回调）
   ├─ RotationStrategy（动量+效率轮动 ETF/股票）
   ├─ AgentStrategy（LangGraph 三智能体：分析师→风控→基金经理）
   ├─ DoubleMAStrategy（金叉死叉）
   ├─ BenchmarkStrategy（买入持有）
   └─ TripleMomentumStrategy / EffiAgentRotation（三重动量 / 效率+Agent 轮动）
   ↓ Broker（成交/持仓/胜率）→ 日志 + Pyecharts 可视化/HTML 报告
```

## 1 数据源来源（逐接口）

| 数据源 | 封装/函数 | 用途 | 鉴权 |
|---|---|---|---|
| **AkShare**（主源） | `data/data_fetcher.py`：`ak.stock_zh_a_hist`（A 股日线，qfq 前复权）、`ak.fund_etf_hist_em`（ETF）、`ak.spot_hist_sge`（上金所黄金，`fetch_sge_gold_daily`） | 日线行情（股票/ETF/黄金），缓存为本地 CSV 加速回测 | 无 |
| **Tushare**（可选） | `data_fetcher._fetch_from_tushare`（`ts.pro_api`，`_get_tushare_code` 补 .SH/.SZ 后缀） | A 股日线备源 | `TUSHARE_TOKEN`（config 或环境变量） |
| **新闻** | `data/news_fetcher.py`（`fetch_and_save_news(symbol, max_pages=20)` → `data/cache/news_{symbol}.json`） | 个股新闻标题+内容+发布时间，供 LLM Agent 做事件驱动分析 | 无（爬虫） |
| **SQLite** | `data/database.py` + `db_models.py` + `init_database.py`（SQLAlchemy） | 股票池/元数据持久化（`get_stock_list` 等） | 本地 |

## 2 分析逻辑与 Agent 机制

### 2.1 AgentStrategy（strategies/agent_strategy.py，LangGraph 三智能体）

`build_graph()` 用 LangGraph `StateGraph(AgentState)` 建**严格串行链**：

```
market_analyst → risk_manager → fund_manager → END
```

`AgentState` 五个字段：`date / market_data / account_status / analyst_view / risk_view / final_decision`（JSON 字符串），三个节点通过 state 共享内存传递。

| 节点 | 角色定位 | 关键 prompt 规则 |
|---|---|---|
| **market_analyst**（分析师） | 激进 A 股分析师，政策敏感型 | ①事件五分类：`Policy_Support`（政策扶持→长期利好）/`Earnings_Surprise`（业绩超预期→短期爆发）/`M_and_A`（并购重组→高波动）/`Scandal`（丑闻立案→**一票否决建议卖出**）/`Noise`（噪音→忽略）；②反射检查（数据完整 5 日 + 预期差：利好出尽/底部确认 + 散户情绪）；③输出事件分析+趋势+交易建议（强烈买入/买入/观望/卖出/强烈卖出）+≤50 字理由 |
| **risk_manager**（风控官） | 保守，保本防回撤 | ①验证分析师是否含政策面分析（缺失要求重分析）；②评估 T+1 流动性风险 + 涨跌停无法及时止损；③波动率检查：当前 vs 近 30 日均值，**>30% 风险上调**；输出风险评估（高/中/低）+ 建议仓位 0.0-1.0（分析师建议"强烈买入"但风险"高"→≤0.3）+ 理由 |
| **fund_manager**（基金经理） | 最终决策权 | ①冲突处理：分析师"买入"vs 风控"卖出"→**强制采用风控**；②输出严格 JSON：`action(BUY/SELL/HOLD)` + `quantity_pct(0-1)` + `reason` + `learning_note`（决策依据记录，用于后续优化） |

决策执行（`on_bar` → `_run_graph`）：算 MA5/MA20、近 30 日收益波动率（`returns.tail(30).std()*100`）、近 5 日收盘、`get_news_context`（近 2 日新闻前 10 条）拼 `market_data`；账户状态拼 `account_status`（`broker.cash` + `positions`）；`ainvoke` 后 `json.loads` 解析决策执行 buy/sell，含**整手边界处理**（`quantity = (max_shares // lot_size) * lot_size`；目标金额不足一手但资金够一手→fallback 1 手；持仓 <1 手→允许全仓清卖）。

### 2.2 其它策略

- **RotationStrategy**（`effi_rotation_strategy.py`）：动量+效率轮动，`lookback_period` 选 `top_n` 资产，`hold_period` 持有，`use_efficiency` 效率分。
- **TripleMomentumStrategy**：`bias_n`/`momentum_day`/`slope_n`/`rebalance_threshold` 三重动量（bias 均线 + 动量回归 + 斜率效率）。
- **EffiAgentRotation**（`effi_agent_strategy.py` + `effi_agent_strategy_hold20d.py` + `effi_agent_strategy_signal.py`）：效率轮动与 Agent 组合的多个变体。
- **DoubleMAStrategy**（`double_ma_strategy.py`/`ma_strategy.py`）：短均线上穿长均线金叉/死叉。

### 2.3 Broker 与成交（engine/broker.py）

`initial_cash=100000.0`、`commission_rate=0.0003`；`buy`/`sell` 加权平均成本法；`closed_trades` 记录已平仓盈亏（`return_pct`）供算胜率；`get_total_equity` 现金+持仓市值。

### 2.4 LLM 客户端（utils/llm_client.py）

`async_hunyuan_chat`：走腾讯混元 OpenAI 兼容端点 `https://api.lkeap.cloud.tencent.com/v1`，默认模型 `deepseek-v3.2`（可换 `deepseek-r1`/`deepseek-v3.1`），`temperature=0.2, top_p=0.3, max_tokens=4096`；`parse_llm_response` 支持 markdown 代码块 + fallback 兜底解析 JSON。`HUNYUAN_API_KEY` 缺失或为占位直接 `RuntimeError`。

## 3 最终产物结构与格式

- **回测结果图**：`--output_file` 输出 PNG（Pyecharts，`utils/plotter.py`/`plot_heatmap.py`，热力图 + 净值曲线）。
- **HTML 报告**：根目录 `backtest_result.html`（交互式图表）。
- **日志**：`logs/`（`utils/logger.py`，mode="all"/"file"），逐日 `[date] Agent Decision: action | Reason`。
- **参数网格搜索**：`scripts/grid_search.sh`（RotationStrategy）与 `tm_grid_search.sh`（TripleMomentum）调参。
- **指标**：`utils/metrics.py`（胜率/收益等，基于 `closed_trades`）。

## 4 工程实现要点（关键代码路径）

| 关注点 | 实现 |
|---|---|
| 参数优先级 | CLI > config.yaml > 默认值（main.py 三段式） |
| 数据缓存 | DataFetcher 拉取后存本地 CSV，回测 warmup 复用，避免重复拉 |
| 策略接口 | `base_strategy.py::BreadFreeStrategy`（`on_bar(date, bars)` 回调 + `set_symbols` + `history` 字典） |
| 事件循环 | `backtest_engine.py::BacktestEngine` 逐日喂 bar 给策略，策略产信号 → Broker 执行 |
| 多策略变体 | `effi_agent_strategy*.py` 三个变体（base/hold20d/signal），用相似骨架做消融 |
| 可视化 | Pyecharts（`plotter.py` 净值曲线、`plot_heatmap.py` 参数热力） |

## 5 坑与注意事项（来自代码注释/实现）

1. **T+1 是"prompt 提示"而非"引擎约束"**（⚠️ 关键发现）：README 声明 BacktestEngine 支持 T+1 模拟，Agent 的 analyst/risk_manager prompt 也反复强调"必须考虑 T+1 交易制度、涨跌停板"，但 `backtest_engine.py`/`broker.py` **grep 不到任何 T+1 硬实现**（无"当日买入不可卖"的持仓冻结逻辑，`broker.buy` 后立即可 `sell`）。即：T+1/涨跌停只存在于 prompt 文本，回测成交层面**并未真正模拟**，LLM 即便"遵守"也只是靠 prompt 自觉。
2. **散户情绪是占位符**（agent_strategy.py 注释）：`sentiment = "散户情绪稳定。"` 硬编码默认值，注释明确说明"self.history 只存 close，无法算平均成交量"——即数据准备层没存 volume 历史，情绪指标名存实亡。
3. **AgentStrategy 只支持单标的**：`set_symbols` 收到多标的只警告并取第一个，`on_bar` 也只处理一个 symbol；LLM 投委会本质是单股择时，不是组合。
4. **新闻依赖本地缓存**：`load_news` 读 `data/cache/news_{symbol}.json`，文件不存在就"暂无新闻"——**回测前必须先跑 `news_fetcher` 爬好新闻**，否则事件驱动分析退化为无事件。
5. **PnL 计算简化**（broker.py 注释自陈）：`avg_price` 不含买入佣金、卖出 PnL 只扣卖出佣金，双边佣金未完全计入；`get_total_equity` 用"最后已知价"兜底缺失价。
6. **LLM JSON 解析脆弱**：`fund_manager_node` 靠 `strip`+剥 ``` 代码块，`json.loads` 失败整次决策静默跳过（except 打日志 continue），弱模型输出会直接丢信号。
7. **整手边界**：`quantity==0` 时 fallback 到 1 手（资金够时），持仓 <1 手允许全清——处理藏了较多分支判断，逻辑分散。
8. **混元 key 硬校验**：`HUNYUAN_API_KEY` 缺失或占位直接 `RuntimeError`（import 时即抛），非 Agent 策略也会被这一步卡住——LLM client 模块级校验。

## 6 可借鉴的设计要点

1. **"策略可插拔 + LLM 也是策略"的实验框架**：把 AgentStrategy 与双均线/轮动做成同一 `on_bar` 接口，让"LLM 投委会"和"经典规则"在同一条回测引擎上直接对比——这是研究 LLM 交易信号是否有效的正确实验口径。
2. **三智能体串行链 + 冲突裁决规则**：分析师→风控→基金经理的投委会 pipeline，且把"分析师与风控冲突时强制听风控"写死进 fund_manager 的 prompt——用确定性规则约束 LLM 冲突场景。
3. **事件驱动分析与预期差**：分析师 prompt 强制做"事件五分类 + 预期差判断（利好出尽/底部确认）"，把"读新闻"升级成"事件影响评估"，是 A 股题材归因的实用 prompt 范式。
4. **反射检查与 learning_note**：每个节点带"反射检查要求"，fund_manager 输出 `learning_note` 记录决策依据用于后续优化——虽未闭环到记忆库，但思路是对的方向。

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `breadfree/strategies/agent_strategy.py` | LangGraph 三智能体策略（分析师/风控/基金经理 + prompts） |
| `breadfree/strategies/base_strategy.py` | 策略接口 `BreadFreeStrategy` |
| `breadfree/strategies/{effi_rotation,double_ma,ma,benchmark,triple_momentum}_strategy.py` | 经典/轮动策略 |
| `breadfree/strategies/effi_agent_strategy*.py` | 效率+Agent 组合变体 |
| `breadfree/engine/backtest_engine.py`、`broker.py` | 事件循环引擎 + Broker 成交 |
| `breadfree/data/data_fetcher.py` | AkShare/Tushare 取数 + 缓存 |
| `breadfree/data/news_fetcher.py` | 新闻爬取缓存 |
| `breadfree/data/{database,db_models}.py` | SQLite 持久化 |
| `breadfree/utils/llm_client.py` | 混元 LLM 客户端（deepseek-v3.2） |
| `breadfree/utils/{metrics,plotter,plot_heatmap,portfolio}.py` | 指标/可视化/组合 |
| `main.py`、`breadfree/config.yaml` | CLI 入口 + 全局配置 |
| `scripts/{grid_search,tm_grid_search}.sh` | 参数网格搜索 |
| `docs/{database_model,Quantitative Trading Agent,zhihu_*}.md` | 设计文档 |