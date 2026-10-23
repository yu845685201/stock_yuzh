# limit-up-review（会炒股的程序员·涨停复盘）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：https://github.com/stock-programmer/limit-up-review · **分析对象版本**：main 分支（commit `0d1606d`，2025-08-10）
> **分析依据**：本地克隆源码（`refrences/stock-programmer__limit-up-review`），精读 `main.py`、`comprehensive_analyzer.py`、`market_data/market_analyzer.py`、`pdf_downloader/cninfo_downloader.py`、`llm_pdf_processor/`、`prompt/`、README、CLAUDE.md。
> **配套文档**：../开源股票分析项目清单-20261010.md、../开源股票分析项目深度分析-20261011.md

---

## 0. 定位与使用形态

专注"当日涨停复盘"单一功能的 Python CLI 工具，从作者私有 A 股投资 AI Agent 分叉开源，同时是作者提出的 **CAO（Co-agent Orchestration，开发/测试/Debug 三 Agent 协同）开发方法的实验田**（见 `prompt/design.py`）。协议严格：禁止商用、须署名。

运行形态：`python3 main.py` 两个交互模式（①市场综合分析 ②涨停深度分析），`TUSHARE_TOKEN` 走环境变量，LLM 走 OpenAI/Gemini/Claude/Ollama 多客户端。产物落 `analysis_output/`（JSON + Excel）与 CSV。无 Web、无调度、无推送——是"AI Agent 编程方法论"的示例工程而非产品。

---

## 1. 功能模块全景

| 模块 | 能力 |
|---|---|
| 市场数据分析（mode 1） | 9 类榜单 CSV（见 §3.1） |
| 涨停深度分析（mode 2） | 逐只涨停股串起"行情事实→公司基本面→财务三表关键指标→近 30 天公告→涨停原因与公告关联→行业热点分布"链条 |
| 财务报告分析 | LLM 分析巨潮财报 PDF，提取结构化 JSON（公司信息/主要财务指标/管理层讨论与分析） |
| 公告采集 | 巨潮资讯网真实 API 下载公告与财报 PDF |
| CAO 开发方法 | 三 Agent 协同（开发/测试 case/Debug 循环）的 prompt 资产 |

---

## 2. 数据源来源与采集机制

### 2.1 数据源明细

| 源 | 客户端/接口 | 内容 | 限制 |
|---|---|---|---|
| **TuShare Pro** | `market_analyzer.py`（`pro.daily`/`stock_basic`/`trade_cal`/`daily_basic`） | 日线行情、股票列表（list_status=L）、交易日历、市值 | `TUSHARE_TOKEN` 环境变量 |
| **巨潮资讯网** | `cninfo_downloader.py`（`www.cninfo.com.cn/new/hisAnnouncement/query` + `static.cninfo.com.cn`） | 公告列表 + 财报/公告 PDF | 需 orgId（股票代码→机构 ID 映射） |
| **LLM** | `llm_client.py`（OpenAI/Gemini/Claude/Ollama） | 财报 PDF 结构化提取 | 多客户端、指数退避重试 |

### 2.2 巨潮真实 API 细节

- **股票→orgId 映射**：拉取 `new/data/szse_stock.json` / `sse_stock.json` 建映射；**API 失败时用 10 只常用股票硬编码映射兜底**（平安银行 9900001915、贵州茅台 9900000657 等）。
- **公告查询**：`hisAnnouncement/query` POST，字段 `stock=代码,orgId`、`seDate=起~止`、`category`（年报 category_ndbg_szsh/中报 category_bndbg_szsh/季报 category_sjdbg_szsh）、`column=szse|sse`。
- **下载**：`static.cninfo.com.cn/adjunctUrl`，30s 超时，文件已存在跳过；`download_pdf` 校验 content-type 含 pdf 且字节 >1000。
- **限速**：`query_announcements` 之间 `time.sleep(1)`、逐只股票 `time.sleep(2)`、多页 `time.sleep(2)`——硬延迟防封。

### 2.3 采集层的坑

- **硬编码映射兜底**：远程 JSON 拿不到时只有 10 只股票可查 orgId，其余股票公告/财报全部失效。
- **tushare daily 没有复权**：区间涨幅用 `close_end/close_start` 裸价算，送转/除权期间涨幅会失真。
- **60 日新高逐日拉取**：`get_new_high_large_cap_stocks` 对近 60 个交易日逐日调 `pro.daily` 拼历史最高价，请求量 ≈60 次/次调用。

---

## 3. 分析逻辑

### 3.1 市场综合分析（9 类榜单，`market_analyzer.py`）

| 榜单 | 筛选口径 |
|---|---|
| 每日涨停股 | `pct_chg >= 9.5` |
| 成交额>4亿且涨幅>5% | `amount > 400000`(千元) 且 `pct_chg > 5.0` |
| 成交额>4亿且涨停 | `amount > 400000` 且 `pct_chg >= 9.5` |
| 近 5/10/20 日涨幅排名 | 交易日历对齐首末日，`(close_end − close_start) / close_start`，取前 30 |
| 当年涨幅排行 | 年初首个交易日→当日收盘，取前 30 |
| 成交额>4亿且跌幅>5% | `amount > 400000` 且 `pct_chg < −5.0` |
| 新高且市值>300亿 | 当日 `high ≥ 近60交易日最高价` 且 `total_mv > 30000000`(万元)，按市值降序前 30 |

### 3.2 涨停深度分析（`comprehensive_analyzer.py`）

逐只涨停股（默认 5 只、上限 20）串链：

1. **行情事实**：收盘价/涨跌幅/成交量/成交额（来自涨停清单行）。
2. **财报**（`_get_financial_info`）：巨潮下载当年年报/中报 → LLM 分析 PDF → `extract_business_info`（主营/行业/产品/经营模式）+ `extract_financial_data`（营收/净利/资产/净资产 + **程序算 ROE=净利/净资产、ROA=净利/资产**）。当年没有回退上一年年报；LLM 失败降级 `_fallback_business_extraction`（正则 `是一家...专注于` / `所属行业`）。
3. **近 30 天公告**（`_get_recent_announcements`，取最近 5 条）：`_classify_announcement_type` 分 5 类（财务/治理/业务/投资/业绩/其他），`_is_positive_announcement` 匹配 **19 个利好关键词**（中标/签约/合作/收购/增资/扩产/新品/专利/政策支持/补贴/奖励/业绩预增/分红/股权激励/重组/资产注入/战略合作/技术突破）。
4. **涨停原因推断**（`_analyze_limit_up_reasons`）：利好公告数>0 → `announcement_correlation=True` + "近期有利好公告发布"；再按公告类型追加"业务拓展或重大合同 / 投资并购活动 / 业绩超预期"；**利好数=0 → "可能为题材炒作或跟风上涨"**。
5. **汇总**：`avg_change_pct`/`total_amount`/`with_positive_news`/`business_correlation`(利好占比%)；`_analyze_industries` 行业分布（按行业计数降序）；`_analyze_announcement_themes` 事件类型分布。

### 3.3 LLM 财报 PDF 分析（`financial_report_analyzer.py`）

提示词定位"专业财务分析师"，要求按**第二节公司简介和主要财务指标 + 第三节管理层讨论与分析**输出严格的 JSON Schema（`公司基本信息`/`主要财务指标`/`管理层讨论与分析`）。数据流：

```
直接分析 PDF（Gemini upload_file / OpenAI base64 / GPT-4V）
   ↓ 失败
PDFTextConverter.extract_target_sections（pdfplumber 提取目标章节）
   ↓ 目标章节为空
extract_text_from_pdf 全文 [:50000]（限长防超 token）
   ↓
LLM 输出 JSON → _parse_llm_response（直接 json.loads / 正则提取 ```json``` 块 / 正则提取 {...}）
```

`llm_client.py` 多客户端细节：Gemini `safety_settings` 全部 `BLOCK_NONE`（防财报内容被安全策略拦截）、官方 SDK 优先 + HTTP 多端点兜底、指数退避重试（max_retries 2-3）；OpenAI `temperature=0.1, max_tokens=4000`；Claude `claude-3-sonnet`；Ollama 本地 `qwen2.5:14b`。

### 3.4 CAO 开发方法（`prompt/`）

`design.py` 明确三 Agent 编排："①开发角色 agent ②设计测试 case 角色 agent ③Debug 角色 agent（对比测试 agent 设计的 case 与开发 agent 的输出），进入 debug 工作，Debug 是循环，终结态是测试 case 通过"。作者自述"之前想用 spec 发现不行，然后自底向上；第一阶段完成，第二阶段需要多 agent 角色"。`debug.py` 记录了真实 bug 修复现场（输出"主营业务 not enough"→ 改提示词让 LLM 提取更完整）。

---

## 4. 最终产物结构与格式

| 产物 | 结构 | 落盘 |
|---|---|---|
| 市场榜单 | 9 张 CSV（`limit_up_stocks_YYYYMMDD.csv` 等） | 当前目录 |
| 涨停深度报告 | JSON（`limit_up_analysis_{date}.json`：limit_up_stocks[]/summary/industry_analysis/announcement_themes） | `analysis_output/` |
| Excel 三页 | ①股票详情（代码/名称/收盘/涨幅/成交额/主营/行业/利好数/可能原因）②汇总统计（总数/平均涨幅/总成交额/利好数/利好关联度%）③行业分布（行业/股票数量） | `analysis_output/limit_up_report_{date}.xlsx` |
| 财报 PDF | 巨潮下载的 PDF 文件（按股票建子目录） | `downloads/cninfo/{code}/` |
| 控制台 | 逐只股票实时打印 + 摘要 | stdout |

---

## 5. 坑与注意事项（来自源码注释/debug.py/README）

1. **涨停判定口径错误**：`pct_chg >= 9.5%` 一刀切——不区分主板 10%/创业板科创板 20%/北交所 30%/ST 5%，会把 20cm 且涨 9.5~10% 的非涨停纳入、漏掉 ST 5% 涨停，且主板根本没有 9.5% 涨停（差一分也封不住）。可对照 vibe-astock 的 `limit_pct(code,name,day)` 分板块分日期判定。
2. **财务数字靠 LLM 提取**：财报营收/净利从 PDF 经 LLM 转 JSON 再 `float(replace(',',''))`，OCR/解析误差无程序校验，主营业务被截断是实测 bug（debug.py 记录）。
3. **JSON 解析脆弱**：`_parse_llm_response` 靠 `re.search(r'(\{.*\})')` 贪心正则兜底，LLM 输出不规范时解析失败返回 None。
4. **硬编码 orgId 兜底**：巨潮映射 API 失败时只有 10 只股票可查，其余公告/财报静默失效。
5. **裸价区间涨幅**：tushare daily 不复权，除权除息期间近 5/10/20 日涨幅会失真。
6. **偶发性注释**：CLAUDE.md 提到依赖 `ghostscript` 等 PDF 处理组件（camelot/tabula 表提取），但实际上线用 LLM 替代了传统 PDF 表提取路径；requirements 里 tabula-py/camelot-py 仍列为依赖。
7. **多方协议约束**：许可证禁商用须署名（LICENSE 详文）。

---

## 6. 可借鉴的设计要点

1. **LLM 读财报 PDF 的降级链**：直接多模态（Gemini upload_file / OpenAI base64）→ pdfplumber 提取目标章节 → 全文截断 50000 字——三条路逐步降级，兼顾精度与鲁棒性；digit 校验（ROE/ROA）仍交还程序算。
2. **结构化提取 Schema 前置**：让 LLM 按三节财报的固定 JSON Schema 输出（公司信息/财务指标/管理层讨论），配合"信息未找到"占位，消费端 `extract_*` 与解析解耦。
3. **涨停原因的"事实-公告关联"朴素推理**：19 个利好关键词 + 5 类公告分型，利好=0 标"题材炒作"——虽粗但把"涨停原因"落成可复核规则，不做过度归因。
4. **CAO 三 Agent 开发法**：开发/测试 case/Debug 对赌循环，终结态=测试通过，是"自底向上、先连通再深化"的 LLM 辅助编程叙事，`prompt/` 目录本身就是方法论文档。

---

## 7. 主要源文件索引

| 文件 | 内容 |
|---|---|
| `main.py` | 主程序（两模式交互 + 9 榜单 CSV + 涨停深度分析入口） |
| `market_data/market_analyzer.py` | 9 类榜单（tushare daily/stock_basic/trade_cal/daily_basic） |
| `comprehensive_analyzer.py` | 涨停深度分析编排（行情→财报→公告→原因→行业分布） |
| `pdf_downloader/cninfo_downloader.py` | 巨潮资讯网公告/财报 PDF 下载（orgId 映射 + 真实 API） |
| `llm_pdf_processor/financial_report_analyzer.py` | LLM 财报 PDF 结构化提取（JSON Schema + 降级链） |
| `llm_pdf_processor/llm_client.py` | 多 LLM 客户端（OpenAI/Gemini/Claude/Ollama + 退避重试） |
| `llm_pdf_processor/pdf_text_converter.py` | PDF 文本/目标章节提取（pdfplumber） |
| `prompt/design.py` / `dev.py` / `debug.py` / `tester.py` | CAO 三 Agent 开发方法的 prompt 资产与实操记录 |
| `CLAUDE.md` | 项目文档（环境/功能/CAO 说明） |