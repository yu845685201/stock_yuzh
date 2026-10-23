# llm-agent-trader 深度分析

> 分析日期：2026-10-11 · 仓库 URL：https://github.com/jason8745/llm-agent-trader · 分析依据：本地克隆源码（refrences/jason8745__llm-agent-trader）· 配套：../开源股票分析项目清单-20261010.md

## 0 定位与架构总览

AI 驱动的**股票交易回测系统**（教学/研究向）：把 LLM 嵌入传统逐日回测循环，让模型每天读技术指标 + 趋势分析，输出 `BUY/SELL/HOLD + confidence + risk_level`，再交给确定性回测引擎模拟成交、算绩效。前端 Next.js 点击"回测"，后端用 **Server-Sent Events** 流式推送每日决策进度。定位是"LLM 智能策略 + 传统技术分析 + 风险管理的回测实验室"，非实盘。

架构（README/design.md 的 mermaid + 源码）：

```
前端 Next.js 15（React 19 + Recharts/LightweightCharts + Radix UI + TanStack Query）
   ↓ POST /api/v1/llm-stream（SSE）
后端 FastAPI
   ├─ LLM 流式回测引擎（协调层：初始化/SSE 进度/协调取数-策略-结果）
   ├─ CustomBacktestEngine（核心计算：交易模拟/绩效）
   └─ LLMSmartStrategy（LLM 策略：逐日信号生成）
   ↓
数据层：Yahoo Finance (yfinance) → 技术指标 → LLM 决策 → SQLite 日志
```

三层职责划分（design.md 明确）：「流式回测引擎是指挥官，回测引擎是执行者，LLMSmartStrategy 是决策大脑」。

## 1 数据源来源（逐接口）

| 数据源 | 封装 | 用途 | 覆盖/鉴权 |
|---|---|---|---|
| **Yahoo Finance** | `utils/stock_data.py` + `utils/fetcher.py`（`yf.Ticker(symbol)`，`handle_yfinance_errors` 装饰器统一异常） | OHLCV 历史 + 当前价 + 个股信息（`StockInfo` 模型） | 美股，无 key |
| **SQLite** | `backend/backend/data/backtest_logs.db` + `db_maintenance` 类 | 回测日志持久化（每日决策/交易信号/绩效） | 本地 |
| **LLM** | `llm/client.py`（Azure OpenAI / Google Gemini 自动检测） | 逐日交易决策推理 | `AZURE_OPENAI_API_KEY` / `GOOGLE_API_KEY` |

LLM client 细节（`client.py`）：`_detect_available_provider()` 按 `.env` 里注释/取消注释来自动判断用 Azure 还是 Gemini（两者都在时优先 Azure，向后兼容）；`temperature=0.1`、`max_tokens=4000`。

## 2 分析逻辑与 Agent 机制

### 2.1 LLMSmartStrategy（llm/strategies/llm_strategy.py，约 3400 行）

核心是 `generate_signals(data) -> List[TradingSignal]`，逐日循环。方法族分四类：

**技术指标**（`utils/indicators.py`）：MA（SMA/EMA）、RSI(14)、Bollinger、MACD、ATR(14)、VMA（成交量均线）、CCI(20)、KDJ(9)、OBV、ADX(14)——`_calculate_all_indicators` 一次性算全。

**趋势与事件**：
- `_analyze_current_trend`（当前趋势）+ `_calculate_trend_consistency`（趋势一致性 0-1）+ `_calculate_trend_strength` + `_calculate_consecutive_move_tendency`（连续移动倾向）；
- `trend_analyzer.py::EnhancedTrendAnalyzer` 多时间框架加权判 `uptrend/downtrend/consolidation`（`uptrend_weight > downtrend_weight * 1.2` 才确认，20% 偏差偏置防噪音），并检测**阶段转换信号强度**（phase transitions）；
- `_detect_trigger_events` + `_determine_event_severity`（触发事件与严重度）。

**自适应"股票人格"分析**（`_analyze_stock_characteristics` → `_classify_stock_personality`）：对每只票跑指标有效性测试——`_test_ma_crossover_effectiveness`/`_test_bollinger_bands_effectiveness`/`_test_macd_effectiveness`/`_analyze_breakout_tendency`/`_calculate_reversal_frequency`/`_calculate_gap_frequency`/`_analyze_support_resistance`，据此动态调技术参数阈值（`_adjust_technical_parameters`）。

**LLM 决策**：`_build_decision_prompt` 把趋势/指标/事件/持仓状态拼成 prompt → `_make_llm_decision` → `_parse_llm_response`（解析 JSON）→ `_create_signal_from_decision`。

### 2.2 策略 Prompt（llm/strategies/prompt/traditional_strategy.md）

一段"传统决策原则"注入 prompt，核心规则：

- **趋势三分**：uptrend（可进/持有/加仓）、downtrend（不进场/立即出场）、consolidation（谨慎观望）。
- **转换信号强度分档**（🔥 最高优先级）：非整理阶段 ≥3% 向上转换→强烈建议进场、2-3%→积极考虑、1-2%→谨慎考虑；向下转换对应→立即出场/谨慎出场/提高警惕。
- **整理阶段特别规则**：优先观望；只有转换信号 ≥5% 且动量 `strong_bullish` 才考虑进场。
- **风控铁律**：downtrend 即使 MACD 金叉也不进；`downtrend + 趋势一致性 <0.5` 不抢反弹；价格低于 20 日均线 + MACD 负 + downtrend 三重偏空不进。
- **持有多重确认出场**：单 MACD 死叉不足出场，需配合明确转 downtrend（避免 uptrend 中过早出场）。
- **输出 JSON**：`{"action":"BUY|SELL|HOLD","confidence":0.0-1.0,"reasoning":"...","risk_level":"low|medium|high","expected_outcome":"..."}`。

### 2.3 回测引擎（backtesting/engine.py::CustomBacktestEngine）

`run_backtest` → `_prepare_technical_indicators` → `_simulate_trading`（逐日调 `strategy.generate_signals`，`_execute_buy_order`/`_execute_sell_order` 模拟成交）→ `_calculate_performance_metrics`。`BacktestConfig.calculate_commission` 按成交额计佣金；`Portfolio` 维护持仓/现金/总市值。绩效指标：`total_return`/`annual_return`/`max_drawdown`/`win_rate`（盈亏配对）/`strategy_win_rate`。

### 2.4 流式与反馈

`api/v1/endpoints/llm_stream.py` 用 SSE 推送每日决策 + 绩效进度；`daily_feedback.py` 提供历史决策回看/改善建议；`backtest_analysis.py` 提供回测分析查询（SQLite 日志）。

## 3 最终产物结构与格式

- **回测报告**（`get_backtest_summary`）：总报酬率/年化报酬率/最大回撤/胜率 + 交易事件表 + 净值曲线（`get_backtest_chart`）。
- **SSE 实时流**：逐日 `progress` 事件（第 N 天、当前价、持仓/持仓盈亏）+ 每日决策 JSON。
- **前端可视化**：Recharts/LightweightCharts 净值曲线 + K 线叠加买卖点、分析面板、LLM 对话界面。
- **SQLite 日志**：`backtest_logs.db` 存每日 `_log_daily_analysis`（决策+理由+指标）、`_log_trading_signal`、事件严重度。

## 4 工程实现要点（关键代码路径）

| 关注点 | 实现 |
|---|---|
| LLM provider 切换 | `.env` 注释/取消注释 key，`_detect_available_provider` 自动判 Azure/Gemini |
| 指标全家桶 | `indicators.py` 10 类指标（含 KDJ/OBV/ADX/CCI/VMA），pandas 向量化 |
| 趋势多时框加权 | `trend_analyzer.py`，20% 偏差偏置 + phase transition 检测 |
| 自适应调参 | `_test_*_effectiveness` 系列回测指标有效性，动态改阈值 |
| 交易模拟 | `Portfolio`/`Trade`/`OrderType`/`TradeStatus` dataclass，`calculate_commission` |
| 异常处理 | `yfinance_utils/handle_yfinance_errors` 装饰器统一 yfinance 异常 |
| 未实现盈亏 | `unrealized_pnl_tracker.py` 跟踪浮动盈亏 |

## 5 坑与注意事项（来自代码/实现）

1. **look-ahead 风险需自行核对**：逐日循环里 LLM 是否只喂"当日及之前"数据，取决于 `_build_decision_prompt` 的切片逻辑，代码未在注释中显式声明防前视规定（未能核实是否有护栏，使用前须人工核对 prompt 数据窗口）。
2. **LLM 输出解析脆弱**：`_parse_llm_response` 依赖模型严格按 JSON 格式返回（prompt 强制 `action/confidence/reasoning/risk_level/expected_outcome`），`temperature=0.1` 是为保稳定，但换弱模型仍可能解析失败落入默认 HOLD。
3. **单策略温度敏感**：整体 `temperature=0.1` 偏保守，是"决策稳定优先"的取舍，未必是最优采样。
4. **义大利/中文混排文案**：报告文本含繁体中文（"總報酬率"）与简体混排，`traditional_strategy.md` 用简体，UI 报告措辞不统一。
5. **`llm_strategy.py` 3400 行单文件**：策略逻辑高度内聚但也高度耦合，`_analyze_stock_characteristics` 到 `_parse_llm_response` 全在一个类里，改一处易牵动其它。
6. **SQLite 单文件日志**：`backend/backend/data/backtest_logs.db` 直接入库（含 db 二进制在源码树），并发/扩展受限，纯本地实验室形态。

## 6 可借鉴的设计要点

1. **"程序做技术分析、LLM 做最终决策"的清晰分工**：指标计算/趋势识别/事件检测全是确定性代码，LLM 只吃结构化 prompt 输出带 `confidence`/`risk_level` 的 JSON 决策——LLM 的角色被收窄到"读已算好的数据下判断"。
2. **策略规则 markdown 化**：交易原则（趋势三分 + 转换信号分档 + 风控铁律 + 输出 schema）写进独立 `traditional_strategy.md` 注入 prompt，规则与代码解耦、易改易审。
3. **股票人格自适应**：先对每只票测"哪个指标历史上有效"，再动态调阈值——比固定参数更贴合个股特性，是"自适应参数"的可参考实现。
4. **转换信号强度分档**：把趋势转换量化成"信号强度百分比"并分档映射到明确的进出场力度，避免 LLM 对"趋势变了"的模糊判断。

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `backend/app/llm/strategies/llm_strategy.py` | LLMSmartStrategy 主策略（~3400 行） |
| `backend/app/llm/strategies/prompt/traditional_strategy.md` | 传统决策原则 prompt + JSON schema |
| `backend/app/backtesting/engine.py` | CustomBacktestEngine 回测引擎与绩效 |
| `backend/app/llm/client.py` | LLM client（Azure/Gemini 自动检测） |
| `backend/app/llm/analysis/trend_analyzer.py` | 多时框趋势 + 阶段转换检测 |
| `backend/app/llm/analysis/enhanced_technical_analyzer.py` | 增强技术分析 |
| `backend/app/utils/indicators.py` | 10 类技术指标 |
| `backend/app/utils/stock_data.py`、`fetcher.py` | yfinance 取数 |
| `backend/app/api/v1/endpoints/llm_stream.py` | SSE 流式回测端点 |
| `backend/app/api/v1/endpoints/{backtest_analysis,daily_feedback}.py` | 回测分析/每日反馈 |
| `backend/app/utils/yfinance_utils/*` | yfinance 异常/模型封装 |
| `design.md`、`README.md` | 系统设计/架构 |