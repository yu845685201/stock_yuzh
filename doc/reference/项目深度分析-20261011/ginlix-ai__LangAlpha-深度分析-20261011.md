# LangAlpha 深度分析

> 分析日期：2026-10-11 · 仓库 URL：https://github.com/ginlix-ai/LangAlpha · 分析依据：本地克隆源码（refrences/ginlix-ai__LangAlpha）· 配套：../开源股票分析项目清单-20261010.md

## 0 定位与架构总览

LangAlpha 自称 **"the harness for agentic trading"（智能体交易的外骨骼）**——面向美股（A 股 hosted "coming soon"）的 AI 交易代理平台：Agent 做研究、建 thesis、并在用户设定的权限边界内用**真实券商账户**下单。核心理念是"**A trade is a loop, not a tool call**"（交易是循环不是工具调用）与"**Autonomy needs a boundary you set**"（自主权由你设边界）。与 TradingAgents 系"分析师辩论"范式不同，它是**单 Agent（PTC agent）+ 沙箱代码执行 + skills 库 + 受管控订单**的产品化平台。

架构分层（AGENTS.md + `src/`）：

```
web/（React 19 + Vite + TS） | desktop/（Electron 壳，仅入口 URL） | Slack/Discord/Telegram/iMessage/Feishu 通道
   ↓
src/server/（FastAPI，多 worker，SSE 流，Postgres + Redis）
   ↓
src/ptc_agent/（核心 Agent：LangChain create_agent + 中间件栈 + 子代理编排）
   ├─ PTC 模式：LLM 写 Python → execute_code 在 Daytona/Docker 沙箱里调 MCP 包装函数
   ├─ 角色：Analyst（workspace 内）/ Chief of Staff（跨 workspace）/ Flash（无沙箱快速助手）
   ├─ 5 子代理：research / general-purpose / data-prep / equity-analyst / report-builder
   └─ plugins/ 的 MCP server + skills（30+ 投研技能）
   ↓
src/tools/（web search/fetch、SEC filings、market data、secretary、automation）
src/data_client/（金融数据协议抽象：capability × market × asset_class 路由降级链）
src/llms/（多模型 wrapper + token 计价 + models.json manifest）
```

## 1 数据源来源（逐接口）

数据层是**多 provider 路由链**抽象（`src/data_client/market_data_provider.py`）：每个 symbol 按 `capability(intraday/daily/snapshot) × market × asset_class × interval` 解析出 provider 优先级链，逐个尝试降级。

| 数据源 | 封装 | 用途 | 鉴权 |
|---|---|---|---|
| **Yahoo Finance** | `plugins/yfinance/` 4 个 MCP server（`yf_price`/`yf_market`/`yf_fundamentals`/`yf_analysis`，`SOURCE="yfinance"`） | 行情/基本面/分析（self-host 默认，60 秒刷新、标 delayed） | 无 |
| **FMP**（Financial Modeling Prep） | `financialmodelingprep.com`，Daytona `platform_secrets` 把 API key 注入沙箱，仅 allowlist 主机出站代换 | Self-host 备选行情、Hosted 主数据 | `FMP_API_KEY`（平台统一托管） |
| **自建 market_data bundle** | `plugins/langalpha_market_data/`（`price_data`/`fundamentals`/`options`/`macro` 4 个 MCP server） | Hosted 美股/期权实时行情 + 期权链 + 宏观 | 平台托管 |
| **alternative_data bundle** | `plugins/alternative_data/` | 另类数据 | — |
| **SEC 档案** | `src/tools/sec/` | 财报三表/10-K/10-Q filings | 无 |
| **Web 搜索** | `search_api: tavily`（选项 bocha/serper/exa/parallel） | 联网检索 | 对应 key |
| **爬虫** | `crawler.backend: router`（或 scrapling：HTTP→browser→stealth 分层），circuit breaker + 队列准入 | 网页抓取（含 PDF/YouTube/X） | — |

数据协议抽象要点：`market_data_provider.py` 按 capability 对每个市场/资产类别分链，provider 列表可 per-capability 覆盖 `markets`；`_borrows_width`/`_verdicts` 处理跨 provider 宽度借用与空 cell 判定。

## 2 分析逻辑与 Agent 机制

### 2.1 PTC 模式（Programmatic Tool Calling）——核心差异化

AGENTS.md 明确定义：**默认 LLM 不直接调 MCP 工具**，而是通过 `execute_code` 写出 Python 代码，import 生成的 wrapper 模块，在沙箱里调用 MCP-backed 函数——把数据处理、画图、多步分析合并进一次执行。`ToolFunctionGenerator`（`src/ptc_agent/core/tool_generator.py`）生成上传进沙箱的 wrapper 代码。金融数据 MCP server 以 stdio 子进程运行。

工具也可绑定到 **direct 路径**（模型的单次 JSON tool call，沙箱不参与）——这是 per-call 策略能看见、UI 能渲染的形状，因此 **`trading` 能力组只允许 direct 路径**（`tool_binding.py` 解析每个工具的路径并钳制在其组允许范围内）。

### 2.2 角色（AgentRole in agent/roles.py）

- **Analyst**（默认）：workspace 内的 agent，做研发、出产物（留在 workspace）。
- **Chief of Staff**：`all_workspaces_agent` flag 后的"全 workspace"agent，跑在用户电脑 Home 目录，自己读 folders/past threads 回答已有问题，新工作 `delegate_to_analyst` 交给 workspace analyst。带协调工具 `manage_workspaces`/`delegate_to_analyst`/`agent_output`/`manage_threads`。
- **Flash agent**：无沙箱快速助手（flag off 时的 fallback），只走 external 工具 + direct 绑定的 MCP。

`resolve_turn_route`（`server/services/turn_runtime.py`）按 turn 选角色。

### 2.3 子代理（agent/subagents/builtins.py）

5 内置：`research`（深度研究）、`general-purpose`、`data-prep`（数据准备）、`equity-analyst`（个股分析）、`report-builder`（报告组装），全部默认启用；另有用户自定义（`agent_config.yaml` 的 `subagents.definitions`，可设 mode ptc/flash、tools、skills、max_iterations、sections）。`BackgroundSubagentOrchestrator` 并行调度后台任务。

### 2.4 记忆与上下文（AGENTS.md 详述）

- **长期记忆**：LangGraph `BaseStore` 上的 user + workspace 两级，agent 自写；随 per-thread baseline 冻结在 turn 开始时。
- **Scratchpad**（可选）：per-thread 目录 `.agents/scratchpad/<id8>/` 的 `note/` 做任务 checkpoint（请求、用户决定、方法、发现、委托工作 id、计划剩余），上下文接近 40k token 或 15% 余量时请求写一次。
- **Compaction**：`token_threshold: 120000` 全量压缩，保留最后 10 条；Tier 1 在 turn 间隔 >90 分钟时截断大工具参数。
- **Thread transcripts**：turn 结束时从 checkpoint 渲染成 JSONL 落 `thread_transcripts`。

### 2.5 Governed Orders（受管控订单）

每笔订单必须走"一条被治理的路径"：权限在 **host** 层强制——连接能做什么、哪些订单等用户批准、已批准订单恰好执行一次，都在仓库代码里可读；**任何 prompt 或 agent 写的脚本都不能放宽**，且对每个模型一致。经纪人连接四家：Robinhood / Interactive Brokers / moomoo / Webull（live orders 仅 Robinhood + moomoo），连接时可逐项开关能力（read-only → paper → live）。`trading` 能力组强制 direct 路径，即订单必须是一次可被 per-call 策略审查的单次工具调用。

## 3 最终产物结构与格式

- **Skills 产出**（`plugins/langalpha_research/skills/`，30+）：`initiating-coverage`（sell-side 首覆盖报告，10 页含评级/目标价/thesis/scenarios/peers/风险 + 7 图）、`dcf-model`（含 workbook-patterns/TROUBLESHOOTING）、`comps-analysis`、`earnings-analysis`/`earnings-preview`、`trade-pitch`、`morning-note`、`thesis-tracker`、`3-statements`、`company-profile`、`competitive-analysis`、`catalyst-calendar`、`check-model`/`check-deck`、`idea-generation`、`sector-overview`、`market-watch`、`model-update` 等。
- **交付物 skills**（`plugins/langalpha_deliverables/skills/`）：`html-report`/`pdf`/`pptx`/`docx`/`xlsx`（活公式 Excel 模型）/`interactive-dashboard`（Plotly/Chart.js/Lightweight Charts）/`chart-annotation`/`ui-design`/`inline-widget`。
- **自动化**：schedule brief + price trigger（% 涨跌/价格阈值触发），结果进统一 feed + Slack/Discord/Telegram/iMessage/Feishu。
- **Sources 面板**：每个 turn 的 filing/网页/数据调用/文件来源，含 prompt 与参数，可审计。

## 4 工程实现要点（关键代码路径）

| 关注点 | 实现 |
|---|---|
| 沙箱 | Daytona 云（auto_stop 1h/auto_archive 7d/auto_delete 90d，snapshot 版本化）或 Docker 本地；livefs FUSE 把 server 端文件（memory/profile/automations/transcripts 只读）挂进沙箱 |
| 多 worker | `--workers N`，真相在 Postgres（run ledger + advisory locks），Redis 仅协调，进程内存只是执行上下文 |
| 无 ORM | 原生 psycopg3 async，Alembic 用 raw SQL `op.execute()`；app data 与 LangGraph checkpointer 两个连接池 |
| 第三方 MCP 隔离 | 非自有 stdio MCP 一律 `uvx`/`npx` pinned 版本，绝不从共享 venv 跑（防 SDK major 搁浅） |
| SSE 重连 | 事件缓冲进 Redis，从 LangGraph checkpoint 重放 |
| 模型路由 | `llms/manifest/providers.json` + `models.json`；`LLMService.complete` 统一走 BYOK/OAuth/per-user 偏好 |
| Prompt 安全 | 工具 docstring 是 prompt surface，市场数据/MCP 工具 docstring 被 snapshot 锁（`agent_docstring_lock.json`），改动即挂测试 |

## 5 坑与注意事项（来自代码注释）

1. **yfinance 新闻端点被 Yahoo 停服**（`yf_analysis_mcp_server.py:234` 注释）：yfinance 读新闻走的端点 Yahoo 已停服（返 404，yfinance 转成空列表），但 search 端点还有数据——新闻功能需靠 search。
2. **第三方 MCP 服务器共享环境即死**（AGENTS.md）：任何非自有 stdio MCP 从共享 venv/sandbox system Python 跑，会与环境的 `mcp` pin 耦合，下一 SDK major 就搁浅（`scrapling mcp` 失败类）；必须 `uvx`/`npx` 隔离 + 预握手失败分类诊断（`classify_startup_failure`）。
3. **多 worker 状态陷阱**（AGENTS.md）：请求、SSE consumer、outbox drainer、recovery scanner 可能落到不同进程，禁止引入 request path 会读的 module-level state，本地 registry 不可当 liveness 真相。
4. **沙箱代换保护**（agent_config.yaml）：Daytona `platform_secrets` 只在 allowlist 主机出站 HTTPS 头代换真实 key，沙箱环境变量里只有占位符；已 boot 且环境变量未设则 fail-close。
5. **MCP tools 只存在于 host**（interactive-dashboard SKILL 警告）：`tools/` 模块不拷贝进 Docker 容器，Docker 内 FastAPI 服务代码必须直接 `import yfinance`，且 yfinance 需 `tzdata`（时区错误坑）。
6. **cliched 文档锁**：市场数据工具 docstring 被 snapshot 锁定，重写措辞会导致 unit 套件失败——因为 docstring 就是给模型的 prompt surface。
7. **自我隔离边界**：workspace 间共享 OS 用户、可访问兄弟文件与 MCP 凭据，工具选区是路由约定**不是**安全边界，需要隔离用独立 computer。

## 6 可借鉴的设计要点

1. **PTC 模式**：让 LLM"写代码调用数据"而非"直接 tool call"，一次执行完成取数+计算+画图，且代码可复用、可审计——比逐次 tool call 更适合多步量化分析。
2. **Governed orders（受管控订单）**：交易能力组强制 direct 路径 + host 层权限钳制 + approved 恰好执行一次，把"AI 下单"的安全边界落到仓库可读的代码里，且任何 prompt/脚本都无法放宽。
3. **skills 库分层**：研究技能（dcf/comps/initiating-coverage）与交付技能（html/pdf/xlsx/dashboard）分离，Agent 按需装载 skills 输出"deliverable 而非 answer"。
4. **交易即循环**：thesis 存 workspace，research/sizing/order/watch 持续回灌证据，自动化（schedule + price trigger）让 agent 长期盯盘——比"一问一答"更贴近真实投资过程。
5. **数据协议抽象**：`capability × market × asset_class` 的三维 provider 路由链，同一 symbol 不同能力可挂不同 provider 优先级——多源降级的可配置化。

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `src/ptc_agent/agent/agent.py` | `PTCAgent.create_agent()` 组装工具+中间件+编排 |
| `src/ptc_agent/agent/subagents/builtins.py`、`registry.py` | 5 内置子代理 |
| `src/ptc_agent/agent/roles.py` | Analyst / Chief of Staff 角色 |
| `src/ptc_agent/core/tool_generator.py` | PTC 包装代码生成（ToolFunctionGenerator） |
| `src/ptc_agent/core/mcp_registry.py` | MCP 注册与启动失败分类 |
| `src/data_client/market_data_provider.py` | 数据协议抽象（capability×market 路由链） |
| `src/tools/web/`、`src/tools/sec/`、`src/tools/market_data/` | 搜索/爬虫/SEC/行情工具 |
| `src/server/services/tool_binding.py`、`turn_runtime.py` | 工具路径钳制、turn 路由 |
| `src/llms/` + `manifest/` | 多模型 wrapper 与模型清单 |
| `plugins/yfinance/*_mcp_server.py` | yfinance MCP（价格/行情/基本面/分析） |
| `plugins/langalpha_market_data/*_mcp_server.py` | 自建行情/期权/宏观 MCP |
| `plugins/langalpha_research/skills/*/SKILL.md` | 30+ 投研技能 |
| `plugins/langalpha_deliverables/skills/*/SKILL.md` | 交付技能（报告/Excel/仪表盘/图表） |
| `agent_config.yaml`、`config.yaml` | 行为配置（模型/沙箱/MCP/compaction/search） |
| `AGENTS.md`、`DESIGN.md` | 架构/PTC 模式/设计系统 |