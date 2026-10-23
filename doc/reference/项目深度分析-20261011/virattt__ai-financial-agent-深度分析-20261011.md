# ai-financial-agent 深度分析

> 分析日期：2026-10-11 · 仓库 URL：https://github.com/virattt/ai-financial-agent · 分析依据：本地克隆源码（refrences/virattt__ai-financial-agent）· 配套：../开源股票分析项目清单-20261010.md

## 0 定位与架构总览

Virattt 出品的**金融聊天 Agent 教学模板**（README 明示 "proof of concept"、"educational purposes only"），用 **Vercel AI SDK** + **Financial Datasets API** 搭一个能查行情/财报/筛选股票的对话式投研助手。它是"AI 金融 Agent 最小完整形态"的样板——不是多智能体辩论，而是**单 Agent + 函数工具（function call）+ 生成式 UI**；生产版是其产品化版本 `chat.financialdatasets.ai`。

架构（Next.js App Router）：

```
前端（React 18 + shadcn/ui + Recharts + CodeMirror/ProseMirror 编辑器）
   ↓ POST /api/chat
app/(chat)/api/chat/route.ts —— AI SDK streamText()
   ├─ systemPrompt（regularPrompt）
   ├─ tools: FinancialToolsManager.getTools()（7 个函数工具）
   ├─ maxSteps: 10
   └─ onFinish 落库
   ↓
lib/ai/tools/financial-tools.ts —— 直连 Financial Datasets API（X-API-Key 头）
   ↓
Postgres（Drizzle ORM）+ next-auth 登录 + LangSmith 追踪 + Vercel Blob 文件
```

## 1 数据源来源（逐接口）

单一数据源——**Financial Datasets API**（`https://api.financialdatasets.ai`），一个专为 AI Agent/LLM 设计的金融数据 API（30+ 年美股数据、100% 覆盖；AAPL/GOOGL/MSFT/NVDA/TSLA 免费）。`lib/ai/tools/financial-tools.ts` 内 7 个工具直连：

| 工具 | 端点 | 覆盖 | 鉴权 |
|---|---|---|---|
| `getStockPrices` | `GET /prices/snapshot?ticker=`（快照）+ `GET /prices/?ticker=&start_date=&end_date=&interval=&interval_multiplier=`（历史） | 美股实时快照 + 历史（second/minute/day/week/month/year 粒度） | `X-API-Key` |
| `getIncomeStatements` | `GET /financials/income-statements/?` | 利润表（quarterly/annual/ttm，支持 report_period_lte/gte 时点界） | 同上 |
| `getBalanceSheets` | `GET /financials/balance-sheets/?` | 资产负债表 | 同上 |
| `getCashFlowStatements` | `GET /financials/cash-flow-statements/?` | 现金流量表 | 同上 |
| `getFinancialMetrics` | `GET /financial-metrics/?` | 派生指标（P/E、营业利润等报表外指标） | 同上 |
| `searchStocksByFilters` | `POST /financials/search/`（body 传 filters） | 按财务指标筛选（gt/gte/lt/lte/eq 比较） | 同上 |
| `getNews` | `GET /news/?ticker=&limit=` | 个股新闻/事件 | 同上 |

REST 换数据源只需改 `financial-tools.ts` 几行（README 明确说明），`FINANCIAL_DATASETS_API_KEY` 来自 `.env`。

## 2 分析逻辑与 Agent 机制

### 2.1 单 Agent + 工具调用

`app/(chat)/api/chat/route.ts` 里 `streamText({ tools: financialToolsManager.getTools(), maxSteps: 10, ... })`：模型按需调用 7 个金融工具（AI SDK 的多步工具循环，`maxSteps:10` 限制最大轮数）。`lib/ai/index.ts::customModel` 用 `createOpenAI({ compatibility:'strict' })` + `experimental_wrapLanguageModel` 包 `customMiddleware`（`lib/ai/custom-middleware.ts`，用于 LangSmith 追踪/观察）。

### 2.2 System Prompt（lib/ai/prompts.ts）

`regularPrompt` 要点：友好金融助手、回答简洁、**不要返回 code/markdown/表格/列表等 UI 文本**（因结果由生成式 UI 组件渲染）、取近期财务数据**默认 `ttm` period、用最少 API 请求拿到所需信息**（多处工具支持参数一次取聚合数据）。

### 2.3 去重缓存

`FinancialToolsManager` 内置 `toolCallCache = new Set<string>()`，`shouldExecuteToolCall` 按 `JSON.stringify({toolName, params})` 去重——同一轮里模型重复调用相同工具+参数直接返回 `null` 跳过，省请求配额。

### 2.4 生成式 UI（Generative UI）

数据不是塞进 markdown 文本，而是通过 `dataStream.writeData` 写结构化事件（如 `searchStocksByFilters` 写 `tool-loading` 事件），前端 `components/data-stream-handler.tsx` 渲染成**真实表格组件**：`income-statements-table`、`balance-sheets-table`、`cash-flow-statements-table`、`financial-metrics-table`、`stock-screener-table`、`overview`（股价卡片）等。

### 2.5 文档 Blocks（辅助功能）

`prompts.ts` 的 `blocksPrompt` 是通用"文档编辑"模式（`createDocument`/`updateDocument`），供写代码/报告用，与金融工具无关，是 AI SDK 模板自带能力。

## 3 最终产物结构与格式

- **对话式投研回复**：核心结论用文本，数据用生成式 UI 表格（财报三表、财务指标、股票筛选结果、价格概览）。
- **文档 Blocks**：`createDocument`（代码/长文，CodeMirror/ProseMirror 编辑器实时编辑）+ `updateDocument`（diff 视图 `components/diffview.tsx`）。
- **股票筛选器**：`searchStocksByFilters` 结果渲染 `stock-screener-table`。
- **附件/文件**：Vercel Blob 上传（`/api/files/upload`）。
- **历史与投票**：`/api/history`、`/api/vote`（消息 vote），数据库持久化。
- **追踪**：LangSmith（`LANGCHAIN_TRACING_V2`）记录每个 turn 的工具调用。

## 4 工程实现要点（关键代码路径）

| 关注点 | 实现 |
|---|---|
| AI SDK 集成 | `streamText`（`ai` 4.0.20）+ `@ai-sdk/openai`，`maxSteps:10` |
| 工具定义 | Zod schema 定义参数（`z.object`/`z.enum`），`execute` 内 fetch 直连 |
| 多模型 | `lib/ai/models.ts` 定义模型清单（ChatGPT 各档），前端 `model-selector.tsx` 切换 |
| DB | Drizzle + `@vercel/postgres`，迁移 `lib/db/migrations/*.sql`，`db:generate`/`db:migrate` |
| 认证 | next-auth 5 beta + bcrypt-ts，`/api/auth/[...nextauth]` + `auth.config.ts` |
| 可观测 | `@vercel/otel` + OpenTelemetry + LangSmith |
| 部署 | Vercel 一键 clone（env AUTH_SECRET/OPENAI_API_KEY） |

## 5 坑与注意事项（来自代码注释/实现）

1. **数据源单一 + TikTok 式免费额度**：Financial Datasets API 仅 AAPL/GOOGL/MSFT/NVDA/TSLA 免费，其余需付费 key；换源要改 `financial-tools.ts`。
2. **`getStockPrices` 每次快照+历史两次请求**：`snapshot` 与 `historical` 分开 fetch 再拼 `combinedData`，模型调用会双倍请求；靠 `toolCallCache` 抑制重复，但首次仍是 2 次。
3. **去重缓存无生命周期**：`toolCallCache` 是 manager 实例级 `Set`，跨 turn 若实例复用（取决于构造函数调用位置）可能误拦；代码未做跨 turn 清理逻辑。
4. **教学项目风险**：README 免责声明明确"不用于真实交易"；无风控、无仓位、无回测，纯研究对话。
5. **prompt 约束与生成式 UI 耦合**：`regularPrompt` 要求模型"不要返回 markdown/表格"，因为它假设数据都走生成式 UI——换掉前端渲染会破坏约定。

## 6 可借鉴的设计要点

1. **"函数工具 + 生成式 UI"的最小闭环**：数据不进对话文本，而是函数工具返回结构化数据 → `dataStream.writeData` → 前端渲染真实表格，是 AI 金融产品最正统的呈现范式（对比"markdown 表格塞给模型"）。
2. **工具去重**：`toolCallCache` 按 `{toolName, params}` 去重，径直省掉重复 API 调用——单 Agent 多步循环的实用省钱术。
3. **签名清晰的工具契约**：Zod schema 严格定义参数（period enum、report_period_lte/gte 时点界、filters 比较运算符），把"AI 到底能查什么口径"固化到类型。
4. **数据源对 AI 友好**：Financial Datasets API 提供 `ttm` 周期、`report_period_lte/gte` 时点界、`financial-metrics` 派生指标、`search` 筛选——专为 agent 免二次计算设计的数据契约值得借鉴。

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `app/(chat)/api/chat/route.ts` | chat 端点（streamText + tools + maxSteps） |
| `lib/ai/tools/financial-tools.ts` | 7 个金融工具 + FinancialToolsManager + 去重 |
| `lib/ai/prompts.ts` | regularPrompt / blocksPrompt / codePrompt |
| `lib/ai/index.ts`、`models.ts`、`custom-middleware.ts` | 模型封装 + 中间件 |
| `lib/api/stock-filters.ts` | 股票筛选合法字段枚举 |
| `lib/db/schema.ts`、`queries.ts`、`migrations/` | Drizzle schema + 迁移 |
| `app/(auth)/` | 登录/注册/next-auth |
| `components/{income,balance-sheet,cash-flow,financials,financial-metrics,stock-screener}-*.tsx` | 生成式 UI 表格 |
| `components/data-stream-handler.tsx` | dataStream 事件渲染 |
| `components/overview.tsx`、`code-editor.tsx`、`diffview.tsx` | 价格概览 / 代码块 / diff |