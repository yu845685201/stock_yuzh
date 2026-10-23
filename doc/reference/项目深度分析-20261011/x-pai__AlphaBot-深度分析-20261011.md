# AlphaBot 深度分析

> 分析日期：2026-10-11 · 仓库 URL：https://github.com/x-pai/AlphaBot · 分析依据：本地克隆源码（refrences/x-pai__AlphaBot）· 配套：../开源股票分析项目清单-20261010.md

## 0 定位与架构总览

AlphaBot（x-pai）是**面向 A 股的智能股票分析助手平台**：单 Agent 对话助手 + 5 角色路由 + 工具循环（内部工具 + 自定义 Skill + 外部 MCP 工具）+ 账户/交易跨境通道 + 定时自动化报告，配一个**极其详尽的 A 股盘后复盘 Skill**（九段式证据驱动复盘）。后端 FastAPI + Celery + Redis，前端 React，Docker 一键部署，默认账户 admin/admin123。风格上走"陪伴式投教+纪律交易"路线（README 大量"交易者誓言""理性交易者社区"话术）。

架构：

```
前端 React（对话 UI + 市场/专题主页 + Agent 助手）
   ↓ /api/v1/agent
AgentService（单 Agent，SYSTEM_PROMPT + 角色路由 + 工具循环 max 20 loop，两阶段成稿）
   ├─ 5 角色：general/portfolio/alert/research/risk（关键词路由，各自 LLMProfile + tools）
   ├─ 工具三层：ToolRegistry(内部工具) → SkillRegistry(自定义 Skill handler) → McpHostRegistry(外部 MCP)
   ├─ System 注入：持仓/交易记忆 + 未读预警 + 舆情摘要 + 风控提醒 + 定投提醒 + 长期记忆检索
   └─ 通道：web_chat / mcp / feishu / telegram / qq（ChannelMessage → ChannelReply 统一入口）
   ↓
数据层：data_sources（个股：akshare/tushare/tdx/alpha_vantage/hk_stock）
         market_data_sources（市场：xgb/eastmoney/ths/tdxaidata）
   ↓
Celery 定时任务（盘后复盘/午间简报/日报）+ 账户连接（QMT/同花顺下单）
```

## 1 数据源来源（逐接口）

数据源分两套工厂，各按 dataset 绑定来源（`market_data_sources/factory.py`）：

**个股数据 `data_sources/`**：`akshare.py`（A 股免费主源）、`tushare.py`、`tdx.py`（通达信）、`alpha_vantage.py`（全球）、`hk_stock.py`（港股）。

**市场数据 `market_data_sources/`**（服务市场主页评分 + 复盘），按 dataset 路由：

| 数据集 | 默认来源 | 用途 |
|---|---|---|
| pools / universe / surge / quotes / fundflow / trending / members / plate_index / payoff_strong | **xgb** | 池子/股票池/涨停/行情/资金流/人气/板块成分/强势溢价（本地 XGBoost 预测驱动） |
| turnover / payoff_hot / payoff_drawdown | **ths**（同花顺） | 换手/热点溢价/回撤溢价 |
| 全量切换 | **tdxaidata**（通达信官方 TdxAiData，`tdxaidata.py`+`tdxaidata_client.py`） | 可选 `DEFAULT_MARKET_DATA_SOURCE=tdxaidata` 全量替换，强制一致性校验，禁止混合绑定 |

`factory.py` 的 `_resolve_name`/`reconfigure` 实现严格的 dataset→source 绑定校验（tdxaidata 全量切换禁止 mixed bindings，非法配置抛 `MarketDataSourceConfigurationError`）。

**MCP Host**（`core/mcp_host.py` + `config/mcp_servers.yml`）：AlphaBot 本身作 MCP host，接外部 HTTP JSON-RPC MCP server——配置示例 `trendradar`、`stockbuddy`；复盘 skill 里实际依赖 `westock-mcp`（主）、`tdx-connector`（备）。

**ML**：`services/ml_service.py` + `models/stock_analysis_model.pkl`（XGBoost）+ `train_model.py`。

## 2 分析逻辑与 Agent 机制

### 2.1 单 Agent + 角色路由（services/agent_service.py）

`SYSTEM_PROMPT` 明确"涉及持仓/盈亏必须先调工具，不得臆测编造"（`get_my_positions`/`get_my_trades`/`get_portfolio_summary`/`get_orders`/`place_order`/`get_portfolio_health`/`save_investment_note`）。`route_role()` 纯关键词把消息路由到 5 角色，每个角色有独立 `LLMProfile`（`llm_registry.py`：DEFAULT/RISK/RESEARCH）+ `system_hint` + 工具集（`definitions.py::get_role_tool_names`）：

| 角色 | 触发关键词 | profile | 工具侧重 |
|---|---|---|---|
| general | 默认 | DEFAULT | 全量（search_stocks/get_stock_info/get_stock_price_history/get_stock_intraday/get_market_news/get_stock_fundamentals/search_web/save_investment_note…） |
| portfolio | 持仓/盈亏/组合/下单/委托 | DEFAULT | get_my_positions/get_my_trades/get_orders/place_order/cancel_order/get_portfolio_summary/get_portfolio_health |
| alert | 预警/提醒/价格到达 | RISK | set_price_alert/list_my_alerts/delete_alert |
| research | 研究/研报/基本面/估值 | RESEARCH | 研究/技术/基本面类 |
| risk | 风险/回撤/止损/风控 | RISK | 持仓/风险类 |

`account_scoped` 工具（持仓/下单类）会从 `metadata.account_context` 自动补 `account_id`/`provider`（`_apply_tool_runtime_context`）。

### 2.2 工具执行三层（AgentService._run_tool_loop → execute_tool）

工具循环 `AGENT_MAX_TOOL_LOOPS=20`，`tool_choice=auto`；每轮 LLM 返回 tool_calls → `execute_tool` 按优先级转发：

1. `SkillRegistry.get_handler(tool_name)` → Skill handler；
2. `McpHostRegistry.get_tool(tool_name)` → 通过 MCP 协议转发外部工具（`mcp.types.CallToolResult` 转 dict）；
3. 未知工具返回 error。

**两阶段成稿**（`_use_staged_response`）：工具循环跑完后，`_compose_staged_response` 追加 system 指令"进入最终成稿阶段，不要继续调用工具，输出完整可交付答案"再调一次 LLM——解决自动化报告里"我将继续…"的中间态废话。

### 2.3 上下文注入（_collect_extra_system_lines + _build_messages）

`_build_messages` 把 6 类上下文注入 system prompt：当前日期时间、当前账户（`account_context`）、**长期记忆检索**（`MemoryService.search(user_id, user_message, top_k=5)`）+ 交易模式记忆（固定 query `"亏损 追涨 割肉 交易模式"` top_k=3）、未读预警（`AlertService.get_unread_triggers`）、持仓舆情摘要（`NewsDigestService`）、风控提醒（`RiskControlService.check` + `format_warnings_for_prompt`）、定投提醒（`next_dca_date`）。加载 DB 历史会话时**明确跳过 tool/tool_calls 消息**（注释：OpenAI 要求 `tool` 消息紧跟对应 `tool_calls` 的 assistant 消息，历史回放无法保持严格顺序会触发 400）。

### 2.4 A 股盘后复盘 Skill（data/skills/ashare-daily-review/SKILL.md）——最有价值部分

一个"证据驱动"的九段式盘后复盘 prompt，核心铁律：**不编造数据**（缺失标【未获取】）、**三类陈述标签** `【事实】/【推断】/【假设·待验证】`、**消息≠行情原因**（须 price action 验证）、**每个行动建议必含三要素**（触发条件+失效条件+仓位上限）。

数据获取流程（并行批量 + 连接抖动重试一次）：
- A 市场状态：`data_changedist`（涨跌停/分布）、`data_market_overview`、`data_quote`（指数精确点位，sh000001/sz399001 等）、`data_futures`/`data_quote`(外围美股/港股/外汇 `fxCNH` 等，⚠️ 注释明确 `data_forex` 的 quote 模式已失效只当代码字典用)；
- B 资金结构：`data_sector`/`data_hot`/`data_fund_flow`/`data_fund_margin`，强制输出"资金迁徙图 + 四维度资金表（北向/主力/游资/融资盘）"；
- C 主线题材：`data_hot`/`data_stocklist`/`data_sector(search)`；
- D 消息归因：`data_hot(news)`/`data_news`/`data_notice`/`data_calendar`/`data_macro`，输出**交叉验证矩阵**（新闻事件|理论传导|实际盘面|匹配度|背离原因）；
- E 情绪周期：`data_lhb`（龙虎榜 jg机构/yzb一线游资/yyb营业部）+ `tool_ranking(metric=limitup_days)` 拉真实连板梯队画**连板雁阵图**（不以估算）；
- G 盘前思路：**判断先行四件套**（核心判断=情绪周期定位+方向+打法定性 → 依据≤3行 → 打法≤3条且**具名到梯队个股** → 红线=1认错信号+总仓数字），配套"情绪周期×打法映射表"（冰点/退潮=防守，发酵=打晋级）；
- H 全天时间线五模块（今日标签/日内形状/日内轨迹/今日名场面/时段节点表）。

报告结构：顶部全景速览仪表盘 + 九段式主体（顺序不可调）+ 尾部全天时间线，最终交付物为 Markdown，末尾固定免责声明。自检清单列了 7 类常见错误（估算冒充真实、出消息=涨、建议无三要素、条件分支全覆盖、不看情绪阶段打连板、推断当事实、编造持仓）。

## 3 最终产物结构与格式

- **九段式 A 股盘后复盘报告**（Markdown）：全景速览仪表盘 + 市场状态/资金结构/主线题材/消息归因/情绪周期/持仓交易/盘前思路/风险控制/次日验证 + 全天时间线五模块，全篇【事实】【推断】【假设·待验证】标签 + 消息归因矩阵 + 连板雁阵图 + 判断先行四件套。
- **午间简报**（ashare-midday-brief skill）。
- **对话回复**：`{content, session_id, tool_outputs, completion_reason, tool_loop_count, staged_response_used}`。
- **市场/专题主页**：`/?view=market`（市场主线/情绪/热点，市场评分需先配策略权重否则为 0）、`/?view=topic`。
- **推送通道**：飞书（`channel_feishu.py`）/Telegram（`channel_telegram.py` + `telegram_poller.py`）/QQ（`qq_service.py`）；自动化定时生成/发布日报。
- **账户下单**：QMT（`account/connectors/qmt.py`）/同花顺（`ths.py`）真实委托，`place_order` 需用户明确确认。

## 4 工程实现要点（关键代码路径）

| 关注点 | 实现 |
|---|---|
| 角色路由 | `AgentService.route_role()` 关键词 → 5 角色 → `ROLE_CONFIGS`（profile+system_hint+tools） |
| LLM 多租户 | `llm_registry.py::LLMRegistry` + `LLMProfileName`（DEFAULT/RISK/RESEARCH 分 LLM）+ `litellm_service.py`/`openai_service.py`/`ai_service.py` |
| 工具注册 | `core/registries.py::ToolRegistry`（启停开关 ENABLED_AGENT_TOOLS）+ `skills/registry.py::SkillRegistry` + `core/mcp_host.py::McpHostRegistry` |
| MCP | AlphaBot 双角色：既作 MCP **host**（接外部）也作 MCP **server**（`mcp_server.py`，`get_mcp_exposable_tool_specs` 对外暴露工具） |
| 定时任务 | `core/celery_app.py` + `tasks/ai_tasks.py` + `scheduler_service.py` + `automation_service.py` |
| 限流器 | `middleware/rate_limiter.py` + `batch_analysis_limiter.py` |
| 风控 | `risk_control_service.py`（仓位集中/波动/行为偏差 → 注入 prompt 提示） |
| 回测 | `backtest_service.py` |
| ML | `ml_service.py` + `stock_analysis_model.pkl`（XGBoost）+ `utils/train_model.py` |
| 安全 | `webhook_security.py`（webhook 签名校验）+ `app_cipher.py`（加密）+ `mcp_token_service.py` |

## 5 坑与注意事项（来自代码注释/SKILL）

1. **历史 tool/tool_calls 消息必须跳过**（agent_service.py 注释）：OpenAI 要求 `tool` 消息紧跟含 `tool_calls` 的 assistant 消息，DB 回放历史无法保持严格顺序 → 400 错误，故存档时跳过工具调用历史。
2. **`data_forex` 的 quote 模式已失效**（ashare-daily-review SKILL）：实测 quote 模式无论传什么都只返回品种列表，外汇行情必须走 `data_quote`（`fx` 前缀如 `fxCNH`），`data_forex(mode=list)` 仅当代码字典；且目录外代码（如不存在的 fxUSDCNH）会被**静默忽略不报错**。
3. **tdxaidata 全量切换的配置陷阱**（factory.py）：`DEFAULT_MARKET_DATA_SOURCE=tdxaidata` 要求所有 binding 一致用 tdxaidata，混合绑定直接抛配置错误；tdxaidata 不支持的 dataset 也不放行——防"部分数据悄悄用了别的源"。
4. **市场评分默认 0**（README）：首次部署需按 QUICKSTART 配市场策略权重，否则市场主页评分为 0。
5. **复盘里"消息归因"的纪律**：不把"出利好"直接等同"所以涨"，必须检查 price action 是否确认 + 有无更可能驱动，因果标【推断】——对抗"叙事谬误"。
6. **连板雁阵图不许估算**：`limitup_days` 查不到 N 天 M 板的断板反包，须用 `data_finsearch` 从财联社/复盘稿提取现成表述并标媒体来源，或按日回溯自算；数据源不可达就标【未获取】，绝不编层级数量。
7. **多账户隔离**：`account_scoped` 工具靠 `metadata.account_context` 补 account_id/provider，渠道（feishu/telegram/qq）通过 `notify_channel` 上下文传递，跨账户串味是主要风险点。

## 6 可借鉴的设计要点

1. **角色路由 + 立体上下文注入**：关键词路由分角色，且把"持仓/未读预警/舆情/风控/定投/长期记忆"六类上下文实时注入 system prompt——Agent 回答天然带着用户状态，这是"个人投顾 Agent"的正确形态。
2. **A 股复盘 Skill 的证据纪律**：三类标签【事实/推断/假设·待验证】、消息归因交叉验证矩阵、"行动建议必含三要素"、判断先行四件套、情绪周期×打法映射——把"复盘"从"写作文"变成"可证伪的判断流程"，次日还有验证信号清单闭环。
3. **两阶段成稿**（工具循环 → staged response）：把"探索/取数"和"成稿"分离，第二段强制"不要再调工具、直接交付"，显著改善自动化报告质量。
4. **数据源 dataset 级绑定 + 一致性强校验**：市场数据按 dataset 路由到 xgb/ths/eastmoney/tdxaidata，全量切换时强制一致性、拒绝混合——避免"某类数据悄悄换了口径"。
5. **MCP 双向**：既 host 外部 MCP 工具（扩展能力），又 server 化暴露内部工具（`get_mcp_exposable_tool_specs`），工具生态可组合。

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `backend/app/services/agent_service.py` | AgentService（角色路由/工具循环/两阶段成稿/上下文注入） |
| `backend/app/skills/definitions.py`、`registry.py` | 内部工具 spec + Skill 注册 |
| `backend/data/skills/ashare-daily-review/SKILL.md` | A 股九段式盘后复盘（证据驱动） |
| `backend/data/skills/ashare-midday-brief/`、`ashare-daily-review-tdx/` | 午间简报/通达信复盘 |
| `backend/app/services/data_sources/*` | 个股数据源（akshare/tushare/tdx/alpha_vantage/hk_stock） |
| `backend/app/services/market_data_sources/*` | 市场数据源（xgb/eastmoney/ths/tdxaidata + factory） |
| `backend/app/core/mcp_host.py`、`mcp_server.py`、`registries.py` | MCP host/server + ToolRegistry |
| `backend/app/services/account/connectors/{qmt,ths}.py` | 交易连接器 |
| `backend/app/services/{memory_service,alert_service,risk_control_service,news_digest_service}.py` | 记忆/预警/风控/舆情 |
| `backend/app/services/{automation_service,scheduler_service,report_service}.py` | 自动化/调度/报告 |
| `backend/app/tasks/ai_tasks.py`、`core/celery_app.py` | Celery 定时任务 |
| `backend/app/services/ml_service.py`、`train_model.py` | XGBoost 预测 |
| `backend/app/api/routes/{agent,ai,market,channels,stocks}.py` | API 路由 |
| `backend/docs/alphabot_agent.md` | Agent 设计文档 |