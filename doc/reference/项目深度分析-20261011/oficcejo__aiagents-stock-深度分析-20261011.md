# aiagents-stock 深度分析

> 分析日期：2026-10-11 · 仓库 URL：https://github.com/oficcejo/aiagents-stock · 分析依据：本地克隆源码（refrences/oficcejo__aiagents-stock）· 配套：../开源股票分析项目清单-20261010.md

## 0 定位与架构总览

一个**功能面极广**的 Streamlit 单体 A 股分析系统（约 80 个顶层 py 文件），核心是「六分析师多智能体 + 团队讨论 + 结构化决策」，外围扩展了大量 A 股特色模块（龙虎榜、低价牛、宏观周期、主力资金、新闻流、板块策略、价值/小市值选股、智能监控）并打通了 MiniQMT/TDX 交易与通达信行情通道。形态不是 LangGraph 式图编排，而是**串行调用写法 + 对话式团队会议 + 双路径最终决策**（TypeSafe Jev 结构化决策优先、DeepSeek 文本解析降级）。

多智能体核心流程（`ai_agents.py::StockAnalysisAgents`）：

```
run_multi_agent_analysis() —— 6 个分析师逐一串行调用（每个 time.sleep 模拟）
   ├─ 技术分析师 technical_analyst_agent
   ├─ 基本面分析师 fundamental_analyst_agent（含季报三表期数）
   ├─ 资金面分析师 fund_flow_analyst_agent（akshare 资金流）
   ├─ 风险管理师 risk_management_agent（问财：解禁/减持/事件）
   ├─ 市场情绪分析师 market_sentiment_agent（ARBR/换手，默认关闭）
   └─ 新闻分析师 news_analyst_agent（qstock 新闻，默认关闭）
   ↓
conduct_team_discussion() —— 把各分析师报告拼成一篇「投资决策会议」对话
   ↓
make_final_decision() —— 优先 Jev 结构化，降级 DeepSeek 文本+正则
   ↓
输出统一字段：rating / confidence_level / operation_advice / holding_period /
position_size / entry_range / take_profit / stop_loss / risk_warning / decision_source
```

## 1 数据源来源（逐接口）

| 数据源 | 封装/函数 | 用途 | 鉴权 |
|---|---|---|---|
| **akshare**（腾讯支线） | `data_source_manager.get_stock_hist_data` → `ak.stock_zh_a_hist_tx`（`proxy.finance.qq.com`，注释：避开东财 API 屏蔽） | 历史 K 线（前复权 qfq/hfq） | 无 |
| **akshare**（东财） | `ak.stock_individual_info_em`（基本信息）、`ak.stock_financial_report_sina`（三张表） | 个股信息/财报 | 无 |
| **新浪财经** | `hq.sinajs.cn/list=sh600519`（`data_source_manager.get_realtime_quotes`，解析 32+ 字段 CSV） | 实时行情（name/open/pre_close/price/high/low/volume/amount） | 无（需 Referer） |
| **Tushare** | `data_source_manager`（`pro_api.daily/income/balancesheet/cashflow/stock_basic`，作降级源；单位换算：手→股 ×100、千元→元 ×1000） | 行情/财务降级 | `TUSHARE_TOKEN` |
| **问财 pywencai** | `risk_data_fetcher.py`（`pywencai`，`_get_lifting_ban_data` 限售解禁 / `_get_shareholder_reduction_data` 股东减持 / `_get_important_events_data` 重要事件） | 风险数据 | 需登录态（`login_iwencai.py` 处理） |
| **qstock** | `qstock_news_data.py` | 个股新闻 | 无 |
| **StockAPI**（龙虎榜） | `longhubang_data.py`（`http://lhb-api.ws4.cn/v1/youzi/all`，`code==20000` 才有效，40 次/秒限流） | 龙虎榜游资席位数据 | 可选 `api_key`（普通请求日免 1000 次） |
| **MiniQMT** | `miniqmt_interface.py` | 量化交易下单 | `MINIQMT_CONFIG`（enabled/account_id/host:58610） |
| **TDX 通达信** | `smart_monitor_tdx_data.py` | 通达信行情/自选 | `TDX_CONFIG`（base_url） |
| **You.com** | `config.YDC_API_KEY` | 联网搜索 | `YDC_API_KEY` |

数据源降级纪律：`DataSourceManager` 每个方法「akshare 优先、Tushare 兜底」，K 线重试 3 次指数退避（2s/4s）；`longhubang_data` 重试 3 次 + 0.025s 请求间隔节流。开篇 `patch_requests()` 给所有 requests 注入 User-Agent，专用解决东财服务器 `RemoteDisconnected`。

## 2 分析逻辑与 Agent 机制

### 2.1 六分析师（`deepseek_client.py` 每个方法一个专属 Prompt）

| Agent | 方法 | 数据输入 | 输出 |
|---|---|---|---|
| 技术分析师 | `technical_analysis` | stock_data + indicators（RSI/布林/均线） | 趋势/支撑阻力/信号 |
| 基本面分析师 | `fundamental_analysis` | financial_data + quarterly_data（三表期数） | 财务/估值/季报趋势 |
| 资金面分析师 | `fund_flow_analysis` | 资金流 + indicators | 主力/市场情绪/流动性 |
| 风险管理师 | `risk_management_agent`（内置 11 段 prompt） | 问财风险数据（解禁/减持/事件）+ Beta/52 周高低 | 10 维度风险 + 11 条应对建议 |
| 市场情绪分析师 | `market_sentiment_agent` | ARBR/换手/涨跌停（`market_sentiment_data.py`） | 情绪/资金情绪/反转信号 |
| 新闻分析师 | `news_analyst_agent` | qstock 新闻 | 利好利空/影响评估/事件识别 |

风险管理师是 prompt 最重的一个：固定 11 段（限售解禁/减持/重要事件/市场/个股/流动性/波动/估值/行业/综合评定/风控建议），明确要求"识别日期/规模/股东身份，量化评估，数据缺失本身也是风险"。分析师结果字典统一 `{agent_name, agent_role, analysis, focus_areas, timestamp}`。

### 2.2 团队讨论与最终决策（双路径）

`conduct_team_discussion()` 把参与者报告拼成一篇 `discussion_prompt`，让 LLM"以对话形式模拟投资决策会议"，输出一致性/分歧/权重/时机/共识。

`make_final_decision()` 是核心适配层，降级触发条件明确写在 docstring：

- 优先 **Jev**（TypeSafe System One 模型）：`JevDecisionClient.ask_final_decision(state, include_scores=True)`，一次调用**并行评估** `rating`（Choice 五档）+ `major_risk`（Noul 质押/商誉/立案/暴雷）+ 4 个 Score 维度（技术/基本面/资金/风险，各五档量表），返回类型化答案 + 概率分布 + 校准置信度，**无文本生成、无正则解析**。
- 降级条件：未配 `TYPESAFE_API_KEY`、`JevError`（网络/超时）、评级置信度 `< JEV_MIN_CONFIDENCE`（默认 0.5）、无法算合法现价——任一触发即回落 DeepSeek 文本 + 正则解析链路。
- **价位字段由代码算**（`compute_price_fields`）：`entry_range = 现价 -2% ~ 现价`、`take_profit/target_price = +8%`、`stop_loss = -5%`——注释明确"Jev 不擅长精确算术，不询问模型"。
- `major_risk.noul >= 0.5` 触发风险告警并把仓位建议压到「轻仓」；评级映射五档中文（强烈买入/买入/持有/卖出/强烈卖出）。
- `composite_decision_score` 按 `JEV_SCORE_WEIGHTS = {technical:0.30, fundamental:0.30, fund_flow:0.25, risk:0.15}` 加权合成 0-10 总分，供批量选股排序。

### 2.3 龙虎榜分析集群（`longhubang_*.py`）

`LonghubangAgents` 是多维度分析师集群，含游资行为分析师等角色（`youzi_behavior_analyst`：识别活跃游资席位 5-8 个、操作风格画像、打板/潜伏/接力特征、抱团信号）。`LonghubangScoring.calculate_stock_score` 五维评分：`_calculate_capital_quality`（资本质量）+ `_calculate_net_inflow_score`（净流入）+ `_calculate_sell_pressure_score`（卖压）+ `_calculate_institution_score`（机构）+ `_calculate_bonus_score`（含 `_count_top_youzi` 顶级游资加分），`score_all_stocks` 全局排序。

### 2.4 低价牛策略（`low_price_bull_strategy.py::LowPriceBullStrategy`）

类回测型策略（`initial_capital=1000000.0`）：全仓等权买（`calculate_buy_amount` 满仓 + 个股最大仓位限制 + 100 股/手），**卖出双信号**：①MA5 下穿 MA20；②持股满 `holding_period`（5 天）强制到期卖出。`can_buy` 三重闸：已持有/达最大持股数/当日买入数上限/资金足。

### 2.5 外围模块（同构"data/engine/agents/ui"分包）

宏观分析（`macro_analysis_*`）、宏观周期（`macro_cycle_*`，含 pdf）、主力资金（`main_force_*`）、新闻流（`news_flow_*`，含 Jev 新闻立场 `news_stance_questions` + Webhook 告警）、板块策略（`sector_strategy_*`）、价值股/小市值/业绩增长选股（`value_stock_*/small_cap_*/profit_growth_*`）、持仓管理（`portfolio_*`）、监控调度（`monitor_*/smart_monitor_*`）、通知（`notification_service.py`）。

## 3 最终产物结构与格式

- **最终决策统一字段**（`docs/UNIFIED_ANALYSIS_SPEC.md` 约束，字符串格式兼容 `split("-")`/`re.findall` 老解析）：`rating`（五档）/`confidence_level`（Jev 置信度×10）/`operation_advice`/`holding_period`（中期 2-8 周等）/`position_size`（轻仓/中仓）/`entry_range`/`take_profit`/`stop_loss`/`target_price`/`risk_warning`/`decision_source`（jev|deepseek_fallback），附数值字段（`entry_min`/`stop_loss_value` 等）。
- **龙虎榜报告**：游资画像 + 席位进出 + 综合评分排名 + `get_score_explanation` 评分解释 + PDF 导出（`longhubang_pdf.py`）。
- **低价牛**：持仓汇总 + 交易历史 + 净值曲线（`low_price_bull_ui.py`）。
- **Web UI**：Streamlit（`app.py`，约 126KB 单文件主界面），`Dockerfile国内源版` 4-6 分钟国内镜像构建；`docker run -p 8503:8501`。
- **推送**：Webhook 通知 + 新闻流告警（Jev 置信度 ≥ `JEV_ALERT_MIN_CONFIDENCE`=0.6 才发）。

## 4 工程实现要点（关键代码路径）

| 关注点 | 实现 |
|---|---|
| LLM 引擎优先级 | `llm_client.get_llm_client`：OrcaRouter > NVIDIA NIM > DeepSeek（issue #47） |
| 配置热更新 | `config.reload_from_env()`：Web 保存配置后重载模块级常量，解决"必须重启才生效"（#43） |
| Jev 双实现路径 | `typesafe_decision_client.JevDecisionClient`：构造时探测 `typesafe-sdk`，不可用回退 requests 裸 POST `/systemone`，二者返回结构一致 |
| state 统一截断 | `build_analysis_state`：`STATE_MAX_CHARS=6000`，每分析师至少 400 字上限，靠 `_clip` 防超窗 |
| 问财登录 | `login_iwencai.py` 处理 pywencai 登录态（问财需要 cookie/登录才能稳定查数） |
| 东财反爬 | `utils/akshare_helper.patch_requests` 全局注入 UA + `retry_on_failure` |
| 交易通道 | `miniqmt_interface.py`（MiniQMT 下单）+ `smart_monitor_qmt.py`/`smart_monitor_tdx_data.py` |

## 5 坑与注意事项（来自代码注释/实现）

1. **东财 `RemoteDisconnected`**（`data_source_manager.py` 头注释）：东财服务器频繁断开，必须在 import akshare 前 `patch_requests()` 注入 User-Agent 等请求头——历史数据改走腾讯盟 `stock_zh_a_hist_tx`（`proxy.finance.qq.com`）避开屏蔽。
2. **数据源单位换算三重坑**（代码注释）：腾讯 `amount` 单位是**万元**需 ×10000 转元；Tushare `vol` 单位是**手**需 ×100 转股、`amount` 单位是**千元**需 ×1000 转元；复权类型映射 `{'qfq','hfq','':None}` 各源不同。
3. **腾讯数据源缺 volume 列**：`stock_zh_a_hist_tx` 只返回 open/close/high/low/amount，代码用 `amount / 均价` 估算成交量作为备选（`avg_price=(open+close+high+low)/4`）。
4. **新浪实时字段索引脆弱**：`hq.sinajs.cn` 返回 CSV，字段位置硬编码（fields[0]名称/[1]开/[2]昨收/[3]现价/[8]量/[9]额），`len(fields) >= 32` 才解析——接口字段变会静默拿错值。
5. **Jev 结构性缺陷的对策**：Jev"不擅长精确算术"，价位字段从不下放给模型，由 `compute_price_fields` 按百分比代码算；Jev 置信度 < 0.5 判低质响应降级。
6. **问财（pywencai）依赖登录态**：需 `login_iwencai` 维持登录，否则查数失败或返回空。
7. **龙虎榜第三方 API 易变**：`longhubang_data.py` 里 `base_url` 注释了三个候选（zhongdu.net / ws4.cn / stockapi.com.cn），当前用 `ws4.cn`；`code != 20000` 静默返回 None 而非抛错。
8. **模块极多但耦合单体**：约 80 个顶层 py + `app.py` 126KB 单文件，`time.sleep(1)` 是纯"模拟分析时间"（非真实并发），说明整体是 Streamlit 演示/单体形态，非生产级编排。

## 6 可借鉴的设计要点

1. **"结构化决策优先、文本生成兜底"的双路径**：Jev（System One）用 Choice/Score/Noul 原子问题直接拿类型化答案 + 校准置信度，完全绕开 LLM 文本生成与正则解析的不确定性；文本链路只在 Jev 低质/不可用时兜底——把"决策"和"叙述"彻底分离。
2. **价位字段永不交给模型**：模型出方向与评级，价位/仓位比例由代码按现价×百分比硬算（-2%/+8%/-5%），杜绝模型算错价。
3. **置信度门控**：`JEV_MIN_CONFIDENCE=0.5`（决策降级阈值）与 `JEV_ALERT_MIN_CONFIDENCE=0.6`（告警推送阈值）两级分离——低置信不决策、更低不推送。
4. **可扩展的"data/engine/agents/ui"四分包**：龙虎榜/宏观/板块/新闻流等每个业务都拆成取数、引擎、Agent、UI 四层，虽耦合但边界清晰、易复制新模块。
5. **国内数据源单位换算显式注释**：万元/手/千元的换算写死在注释里，是 A 股多源接入最易出错处。

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `ai_agents.py` | 六分析师 + 团队讨论 + 双路径最终决策 |
| `deepseek_client.py` | DeepSeek 客户端 + 6 个分析师专属 Prompt 方法 + `final_decision` |
| `typesafe_decision_client.py` | Jev 结构化决策（问题模板/state 构建/答案归一化/客户端） |
| `llm_client.py` | LLM 工厂（OrcaRouter/NVIDIA/DeepSeek）+ `compute_price_fields` |
| `config.py` | 全量配置（模型/Jev 阈值/价位百分比/评分权重） |
| `data_source_manager.py` | akshare/tushare 自动切换 + 单位换算 |
| `risk_data_fetcher.py`、`login_iwencai.py` | 问财风险数据（解禁/减持/事件）+ 登录态 |
| `fund_flow_akshare.py`、`qstock_news_data.py`、`market_sentiment_data.py` | 资金流/新闻/情绪数据 |
| `longhubang_*.py`（data/engine/scoring/agents/ui/db/pdf） | 龙虎榜数据采集 + 评分 + AI 集群 + PDF |
| `low_price_bull_*.py`（strategy/selector/service/monitor/ui） | 低价牛策略（MA5/MA20 + 5 日强卖） |
| `macro_analysis_*`、`macro_cycle_*`、`main_force_*` | 宏观分析/周期/主力资金模块 |
| `news_flow_*`、`sector_strategy_*` | 新闻流（含 Jev 立场）+ 板块策略 |
| `value_stock_*`、`small_cap_*`、`profit_growth_*` | 价值/小市值/业绩增长选股 |
| `portfolio_*`、`monitor_*`、`smart_monitor_*` | 持仓管理/监控调度/智能监控（QMT/TDX） |
| `miniqmt_interface.py`、`notification_service.py` | 交易通道/通知 |
| `app.py` | Streamlit 主界面（~126KB） |
| `BUILD_CN.md`、`docs/` | 国内镜像构建与文档 |