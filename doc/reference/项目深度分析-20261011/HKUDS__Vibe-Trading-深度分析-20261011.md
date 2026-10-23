# Vibe-Trading 深度分析

> **分析日期**：2026-10-11 · **仓库**：[HKUDS/Vibe-Trading](https://github.com/HKUDS/Vibe-Trading) v0.1.16（本地 HEAD `8e43007`，2026-10-10）
> **分析依据**：本地克隆源码（`refrences/HKUDS__Vibe-Trading`），精读 `agent/src/agent/grounding/`（数字 grounding 门）、`agent/src/governance/`、`agent/backtest/loaders/registry.py`、`agent/src/market_data.py`、`agent/src/swarm/`、`agent/src/quantlib/` 与 README/CHANGELOG。所有结论来自源码实现、docstring 与 CHANGELOG，非推断。
> **配套文档**：[开源股票分析项目深度分析-20261011](./开源股票分析项目深度分析-20261011.md)

---

## 0 定位与架构总览

Vibe-Trading 是"一条命令装好的个人交易 Agent"（`pip install vibe-trading-ai`），后端 FastAPI + 前端 React 19，另带 MCP server、CLI、Electron 桌面壳。它不是单一分析框架，而是**全栈交易工作台**：LLM Agent 循环 + 27 个行情数据源 + 9 个市场回测引擎 + 18 家券商（实盘/paper）+ 286+ 函数的 `quantlib` 量化库 + 技能(skills) + 编排出多 Agent（swarm）+ 治理（哈希清单 + 哈希链审计账本）。

最独特的两个自研模块，是全项目灵魂：

1. **Grounding Gate（数字接地门）**：每个面向市场的回答在发布前都要通过一道纯"形状"校验——模型自己靠 `figures` 代码块声明每个数字的角色（`observed/derived/proposed/cited/count`），门只按数字的形状与工具证据核对，核不过的单个数字被**剔除**（`（略※）`/`(omitted※)`）而不是整篇拒绝。
2. **Governance（治理）**：每次运行生成 RunManifest（system prompt/skills/工具注册表/包版本的哈希）、及实盘动作的**哈希链追加式审计账本**（seq + prev_record_hash + fsync + flock，篡改任一条都会让其后所有记录哈希断裂）。

### 模块地图

| 目录 | 职责 |
|---|---|
| `agent/src/agent/` | Agent 循环（`loop.py`）、grounding 门（`grounding/`）、记忆、上下文压缩、skills 加载 |
| `agent/src/tools/` | ~74 个 MCP/本地工具（`backtest_tool`/`market_data_tool`/`quantlib_tool`/`swarm_tool`…） |
| `agent/backtest/loaders/registry.py` | 27 数据源 + 每市场 FALLBACK_CHAINS + 每源价格口径表 |
| `agent/backtest/engines/` | 9 市场回测引擎（A股/美股/港/印/韩/加/阿/英/越 + crypto/futures/…） |
| `agent/src/live/` | 实盘：18 券商、kill switch、order gate、mandate、审计 |
| `agent/src/swarm/` | 多 Agent 编排（worker/presets/task_store/retry） |
| `agent/src/quantlib/` | 量化函数库（options/fixedincome/credit/copula/vaR/eventstudy…） |
| `agent/src/governance/` | RunManifest + 哈希链 Ledger |
| `agent/src/skills/` | 60+ 技能包（ichimoku/harmonic/edgar-sec-filings/liquidation-heatmap…） |
| `agent/src/factors/zoo/` | Alpha Zoo（462 个 alpha 因子） |
| `agent/src/strategy_discovery/` + `strategy_store/` | 策略发现与存储 |

---

## 1 数据源来源

行情统一走 `agent/backtest/loaders/registry.py`：每个市场一条 **FALLBACK_CHAINS**（优先级链），`resolve_loader` 顺链回退；`MARKET_DATA_ORDER_*` 环境变量只能**重排**（必须是默认链的排列），不能增删源（增删会被拒绝，见 CHANGELOG `#1231`/`#1402`）。

### 1.1 每市场数据源链表（FALLBACK_CHAINS）

| 市场 | 链（越前越优先） | 鉴权 |
|---|---|---|
| A股 | tencent → eastmoney → baostock → akshare → tushare → gildata → local | gildata(恒生聚源)、tushare、local 需 key |
| 美股 | yahoo → stooq → sina → eastmoney → yfinance → tiingo → fmp → finnhub → alphavantage → longbridge → akshare → local | fmp/finnhub/alphavantage/longbridge 需 key |
| 港股 | tencent → eastmoney → yahoo → futu → akshare → yfinance → tushare → longbridge → local | futu/tushare/longbridge |
| 印度 | yahoo → yfinance → india_broker → local | — |
| 韩国 | pykrx → yahoo → yfinance → local | pykrx 无鉴权(KRX) |
| 加拿大/.TO | yahoo → yfinance → local | — |
| 阿根廷/.BA | yahoo → yfinance → local | — |
| 英国/.L | yahoo → yfinance → local | — |
| 越南/.VN | yahoo → yfinance → local | — |
| 加密 | okx → binance → ccxt → yfinance → local | 交易所 API |
| 期货 | akshare → local | — |
| 基金 | tushare → akshare → local | — |
| 宏观 | akshare → tushare → local | — |
| 外汇/贵金属 | mt5 → akshare → yfinance → local | MT5 本地终端(Windows) |
| 指数 | yahoo → yfinance → local | — |

### 1.2 价格口径表（PRICE_CALIBER，`#1301`）——每个数据源声明"我给的价是什么意思"

```python
"yahoo": "split_dividend"        # 复权，scaled to adjclose
"yfinance": "split_dividend"     # auto_adjust=True
"eastmoney": "split_dividend"    # fqt=1 前复权
"tencent": "split_dividend_additive"  # fqkline qfq，含分红加回
"tushare": "split_dividend"      # adj_factor
"pykrx": "split"                 # adjusted=True
"sina": "raw"                    # 未复权（公开已知缺口）
"longbridge": "raw"              # 显式 NoAdjust
"alphavantage": "raw"            # TIME_SERIES_DAILY 非 _ADJUSTED
```

关键设计：**标的解析**（`market_data.py:_SOURCE_PATTERNS`）先把代码按正则映射到首选源（`600519.SH→tencent`、`AAPL.US→yahoo`、`GC=F→yahoo`、`005930.KS→pykrx`、`BTC-USDT→okx`、`EURUSD=X→yahoo`），且**修复过一整类"落错市场引擎"事故**（见 §5）。

### 1.3 其它数据：工具层

`agent/src/tools/` 下另有独立工具封装非行情数据：SEC 13F（季度同比）、ETF 穿透（`etf_holdings_tool`，CSI-300 跟踪到 342 只覆盖 98.66%）、PredictIt 预测市场、FRED 宏观、沪深港通、龙虎榜、机构持仓、财务三表、龙虎榜席位、微博/新闻舆情等。

---

## 2 分析逻辑与 Agent 机制

### 2.1 Agent 循环（`agent/src/agent/loop.py`）

- ReAct 式循环：模型 bind 工具（工具清单来自 `tools.py:build_registry()`），工具调用 → 结果 → 继续，直到给出最终答案。
- **无进度即停**（CHANGELOG `#1363`）：同一调用从**第二次失败起**拒绝（不是第一次，给限流/超时留一次诚实重试）；一轮 run 连续 8 次工具调用不增益信息即停止并给出可见的恢复请求。
- **去重账本**：按键 = 参数而非工具名；清空结果会重新开放该工具（修过"数据已取到却被上下文压缩清掉，账本却不让重新取，一次 run 44 次被堵"的 bug，`#1349`）。
- **上下文压缩**（`context_compact.py`）：按消息边界打包、逐 chunk 折叠进迭代模板；按每个模型真实上下文窗口而非固定 40K；超大消息切成带标签片段；压缩会保留模型自己的验证记录。
- **LLM 超时**：`VIBE_TRADING_LLM_TIMEOUT_SECONDS` 默认 300s。

### 2.2 Grounding Gate（数字接地门，`agent/src/agent/grounding/`）——核心机制

**五角色声明**（`figures.py:ROLES`）：模型在回答末尾用 ```figures 代码块，每行声明一个数字 `value | role | note | ref`：
- `observed`（已观测）：必须出现在工具结果里；`derived`（派生）：note 必须是算术本身且至少一个操作数本会话已观测；`proposed`（提议）：须可派生或在已观测价格区间内；`cited`（引用）：note 需带来源；`count`（计数）：不检查。

**形状分类**（`scan_figures`，不读自然语言词，只读"形状"）：每个数字被归为三种之一：
- `exempt`（豁免）：日期/时间/证券代码/行首列表序号/表序号列/代码块内容——不是测量值，跳过；
- `measured`（测量）：带小数点、`%`/`pp`/`bp`、贴货币符，或表里"只有数字没有词"的单元格——这是"伪造价格/指标"的形状；
- `bare`（裸整数）：普通整数（计数、周期、窗口长度），不检查。

**匹配规则**（`FiguresBlock.match`）：精度匹配带 `ROUNDED_BAND=0.005`；`37%` 与 `0.37` 是不同断言；无小数点粗写 `39` 绝不借 `38.68` 的声明。

**拒绝后的三种出路**（`release.py`）：最多 `MAX_GROUNDING_REVISIONS=2` 次修订（第 1 次给逐条纠正提示，第 2 次剔除未核实的数字直接发布）；`MAX_GROUNDING_RECOVERY_ROUNDS=6` 次只读恢复（`search_symbol`→`get_market_data` 各最多 `MAX_SYMBOL_RESOLUTION_ATTEMPTS=2`/`MAX_PRICE_EVIDENCE_ATTEMPTS=3`）。可剔除的 issue 码（`_REDACTABLE_CODES`）：`numeric_claim_conflict / numeric_claim_unavailable / unsourced_symbol_figures / figure_undeclared`。

**剔除实现**（`_cut_flagged`/`_sweep_same_values`）：被剔除数字换成 `（略※）`/`(omitted※)`；还会**扫掉同一数字的其它写法**（`37%` 被删后 `37 个百分点`、代码块里的 `37` 也被删），但**单数字绝不做扫 key**（避免误删表格行号和 `2/3`）；被剔数字按去空格后的 digits 归一化比对；最后正文再验一道，脚注也过同一道门。

**代理 note 修复**（`repair_provenance`）：只有"缺数据来源/货币"这类纯来源缺陷才由程序直接补 "数据说明：xxx 行情来源 yyy，计价货币 zzz"，不花一轮模型。

### 2.3 Swarm（多 Agent 编排，`agent/src/swarm/`）

- `worker.py` / `task_store.py` / `presets.py`：preset 定义任务图（含 `skills` 白名单——已被 `load_skill` **在代码层强制**，不再只靠 prompt）；worker 各自私有消息史、`run_dir` 隔离（`run_dir` 只在工作区内生效、区外给定边界拒绝并点名路径，`#1430`）。
- 每个 task 独立 artifact；worker 每个写摘要的退出路径都存消息日志；retry 带等量抖动 backoff；`--resume` 保留已完成子任务、只重放失败子图；task artifact 隔离防止一个任务的文件污染另一个（`test_swarm_task_artifact_isolation`）。
- 子 agent 结果做**投递校验**：`ok/success` 键的裸工具信封会被否掉（不能把"工具成功"冒充成分析结论，`#1052`）。

### 2.4 量化与估值（`agent/src/quantlib/` + valuation）

- `quantlib`：286+ 个被测试函数、约 20 个模块——期权定价（Heston、barrier 有限差分 Greeks）、固收（ISDA CDS、Key Rate Duration、Vasicek）、信用、copula（Gaussian/Archimedean，对数空间实现）、VaR/CVaR/EVT、微观结构（VPIN/Roll/Amihud/Kyle）、事件研究（Patell z、CAR SE 含协方差）、分组清洗 CV、因子 IC。通过只读 `quantlib_call` 暴露，让 skill"import 数学而不是把公式写在 markdown 里"。
- **估值引擎**（`run_dcf`/`run_comps`/三表）：一条硬规则——**缺一个输入就输出 NOT RUNNABLE**，绝不静默默认值；DCF 拒绝非有限输入（否则给出负数股价）。
- **Alpha Zoo**（`factors/zoo/`）：462 个 alpha，是 84+24+20+4 等多轮"NaN 常数"漏洞扫清除后的产物（见 §5）。

---

## 3 最终产物结构与格式

### 3.1 报告

- Markdown 报告 + 可选 **PDF**（Embed 内嵌 CJK 字体；Scheduled Email PDF 支持可选 AES-256 密码，密码只存在私有渠道配置，`#1709`）。
- Report Audit：`report_audit` 逐点核查已取到值的部分，**没取到值的点不核**（修过"跳过所有无值点后报 PASS = 给一篇零证据报告盖章"的 fail-open，`#1362`）。
- 多语言：9 种语言（英/中/日/韩/阿/西/印尼/德/巴西葡）。

### 3.2 Run Card / 回测产物

- 回测落地 **Run Detail** 四页签：Factor Research（IC 序列/IC 统计/分位组净值/IC 相关矩阵）、Positions（权重饼图=毛构成/净敞口柱=净构成）、Tearsheet（月度热力、年度柱、最大回撤标注）、Research Dashboard（KPI、相对基准净值、滚动 Sharpe、完整成交账本）。
- `positions.csv` 存**实际成交**，`target_positions.csv` 存目标权重（修过"报告宣称 80% 敞口其实只成交到 20%"的错，`#1082`）。
- Run Card 显式记录模型来源 + 训练截止年龄；`structured_metrics` 承载 dict/list 型诊断（每指标 ≤4KB UTF-8 JSON）。

### 3.3 治理产物

- `manifest.json`（RunManifest）：只存哈希，不含原文；`manifest_hash` **排除 timestamp/run_id**——同方法学两次运行哈希相等，才能回答"方法学变没变"。
- 实盘审计 `audit.jsonl`（`chain=True` 时）：`{seq, prev_record_hash, record_hash, payload}`，追加前先 `verify_chain` 校验整条链。

### 3.4 推送通道

Web/CLI + IM 渠道（Telegram/Discord/Slack/飞书/微信/钉钉/QQ/Email/WebSocket），`channels/` 统一管理；Market Watch 监控可通过 outbox 推简报。

---

## 4 工程实现要点

### 关键代码路径

| 关注点 | 路径 |
|---|---|
| grounding 门：数字解析/形状 | `agent/src/agent/grounding/figures.py:scan_figures / parse_figures_block` |
| grounding 门：拒绝/恢复/剔除 | `agent/src/agent/grounding/release.py:redacted_release / correction_prompt / recovery_action` |
| grounding 门：证据账本 | `agent/src/agent/grounding/ledger.py`、`evidence.py` |
| grounding 门：身份锁定 | `agent/src/agent/grounding/identity.py` / `identity_checks.py` |
| Agent 循环 | `agent/src/agent/loop.py` |
| 治理：清单 | `agent/src/governance/manifest.py:RunManifest.build_run_manifest` |
| 治理：哈希链账本 | `agent/src/governance/ledger.py:append_record / verify_chain` |
| 数据源注册 | `agent/backtest/loaders/registry.py:FALLBACK_CHAINS / PRICE_CALIBER` |
| 标的解析 | `agent/src/market_data.py:detect_source` |
| 多 Agent | `agent/src/swarm/{worker,presets,task_store,runtime}.py` |
| 工具注册 | `agent/src/tools/`（`build_registry()`） |
| 实盘 | `agent/src/live/`（`audit.py`、kill switch、order gate、mandate） |
| 回测引擎 | `agent/backtest/engines/`（`ChinaAEngine`、`GlobalEquity`、crypto…） |
| 量化库 | `agent/src/quantlib/` |

### 治理账本关键实现（`ledger.py`）

- `append_record`：POSIX `flock`（Windows `msvcrt`）持锁读尾+追加；每次写后 `fsync`；新建文件时连父目录一起 fsync（新文件目录项本身不可持久）。
- `verify_chain`：逐条重算 `record_hash`（含 prev_record_hash），任何早期记录被改/删都会让其后每一条链断。
- **拒绝在断链上续写**：追加前校验整条链，发现已被篡改抛 `LedgerCorruptionError`，而不是在坏链上再砌一个看似合法的后缀。O(n) 每追加，注释明确"实盘账本是低频（每订单/委托/异常一条，非每 tick），换最快保证"。

---

## 5 坑与注意事项（全部来自 CHANGELOG/代码注释，逐条）

1. **纯 A 股被当永续合约跑**：所有无分支的 A 股源（local/tencent/eastmoney/baostock/mootdx/sina）曾把纯 A 股组合路由到 crypto 引擎——不认印花税/T+1/涨跌停/100 股整手，还收 8 小时一次的资金费，且"跑通了且指标自洽，所以危险"。现纯 A 股组合必达 `ChinaAEngine`（`#1431`）。
2. **Alpha Zoo 的 NaN 常数**：`np.where` 比较遇 NaN 是 `False`，三元落到常量；`dropna()` 又把缺口挡在 IC 之外——多轮扫除后仍有 84+24+20+4 个 alpha 在输入缺口上吐数字被当真实信号。现在守卫在 **registry 层**：声明依赖缺一根 bar 就把该 alpha 当栏输出 mask 成 NaN；warmup 半段 52 个 alpha 仍开放并如实声明（`#1377` 等）。
3. **`pct_change()` 默认前向填充**：缺 close 变 0.0% 假收益，一次全量扫描找到 24 个调用点横跨 10 个文件。
4. **腾讯 fqkline 只给 window 的最后 500 根**：2018–2026 的请求返回 2024-06 起的尾部、退出码 0 无警告。现从 end 向前走、空页重试一次才信、错误形状当失败、bump 缓存版本。
5. **A 股成交量单位 100 倍漂移**：五个源按"手"（board lots）、BaoStock 按"股"，回退切换会静默 100× 缩放。现 loader 声明每市场量单位、provenance 暴露单位、BaoStock 在 loader 边界转手、跨源回归要求结算日 1% 内一致（`#1065`，与 daily_stock_analysis 同款坑）。
6. **Yahoo/FMP/Tiingo 分红当亏损**：served raw OHLC 而链上其余源复权，ex-dividend 缺口被读成真实下跌。现按 adjclose 缩放；sina/longbridge 仍 fully unadjusted，口径差异公开追踪（`#1301`）。
7. **混蛋号分类**：`GC=F`/`XAUUSD` 曾判成 A 股、`BTCUSDT` 走 A 股规则（T+1、禁做空）、`ETH-USDT` 搜成娘娘 Aave(ETH)。现靠 venue 目录/`FIAT_CODES` 精确分类，金属/外汇对是精确断言（`#1280/#1282/#1265`）。
8. **grounding 门放过英文伪造价**：曾匹配 `close` 但漏 `closed`、`last traded at`；中文漏 `成交价/最新价/股价/收报`。现双语词表完整，且断言两语言对同例给**相同**结论（`#1349`）。
9. **grounding 门因行号误删**：曾把表格行号 `1,2,3` 当测量值删成 `(omitted※)`。现表序号列识别为结构（`#1471`），单数字绝不做扫 key。
10. **证券代码写作差异**：`.SS/.SH` 被当不同标的 → 每个上海票永久歧义。现归一化为同一 canonical symbol+venue（v0.1.13）。
11. **实盘 fail-open**：Alpaca 做空按正敞口计价（qty 是模、方向在 side）→ 加杠杆反而"放松"敞口上限；OKX/Futu 把被拒的 API 读成"空仓 status ok"。现 qty 带符号、错误信封 fail-closed。
12. **kill switch 跳过自己的动作**：错误信封被当订单列表迭代 → sweep 标"已触发"却没平仓。现只在真实写过交易所后才 latch、每次 halt episode 独占、重启后 latch 持久化（`#1233/#1244`）。
13. **限价单按市价计价**：MCP 订单门按 live quote 计价数量单，限价两倍市价能绕过 cap 成交两倍名义额。现按 quote 与 limit 的较差者计价（`#1361`）。
14. **回测种出"额外一年"**：为 MA200 把 start_date 前移一年喂 warmup，却把这一年也判了分。现 `warmup_bars`/`evaluation_start_date` 分离数据窗与评估窗（`#1240`）。
15. **混币组合直接加和**：mixed-currency 复合回测求和成一条净值线——现直接拒绝（v0.1.13）。
16. **swarm `run_dir` 路径逃逸**：retry 清理 `shutil.rmtree(run_dir/artifacts/<agent_id>)`，preset 里的 `agent_id` 未经校验，`..` 能删整个 run 目录——现必须是单个安全段且落在该 run artifacts 内（`#1430`）。
17. **测试套件污染真实配置根**：曾把合成 `order_rejected` 追加进用户 `~/.vibe-trading/live/audit.jsonl`（哈希链账本的价值就在于不可伪造）——现 conftest 先重定向 HOME、session 结束断言真实账本字节不变（`#1118`）。
18. **凭证文件不在 .gitignore**：13 个券商配置文件（alpaca.json 等）曾缺忽略，`git add -A` 就能提交 key——现测试从代码推导清单。
19. **Anthropic 恢复路径追加 system 消息**：Anthropic API 拒绝对话中途的 system 消息——现恢复 steering 走 user 消息 + inline `<system>` 标签（`#1112`）。
20. **`ChatLLM.close()` 关掉 LangChain 进程级 HTTPX**：一次标题生成看完图，后续全 "client has been closed"——现只关 Vibe-Trading 自己建的 transport（`#1182`）。
21. **Stooq 反爬页面**：HTTP 200 + HTML 形如"无数据"，还把后续请求都堵死——现识别为不可用 + 一次性警告如何重排（`#1637`）。

---

## 6 可借鉴的设计要点

1. **数字接地门是 LLM×金融的工程标杆**：让模型**声明角色**而非靠 65 条正则语义猜"这数字是什么"；只按"形状+证据"核对；单个数字失败就剔除 `（略※）` 而非整篇拒绝，且被剔数字的等价写法也被扫掉。配合小数逗号/千分位/全角/货币符号的归一化，直击"幻觉价格/指标"。
2. **可回答"方法学变没变"的清单哈希**：RunManifest 哈希 scope 是 composition 而非 instance（timestamp/run_id 排除在哈希外），同方法两次运行哈希相同——复现性与 provenance 兼得。
3. **哈希链审计账本**：seq + prev_record_hash + fsync + flock + 追加前整链校验，篡改传播即可见；拒绝在断链上续写。这是把"治理"代码化的样板。
4. **数据源三省**：每源声明**价格口径**（复权/未复权/前复权加回）、声明**成交量单位**、声明**失败原因**（rejected query ≠ empty）；一条链只能重排不能增删。三者避免"换个回退源数字静默漂一个量级"。
5. **回测把 warmup 与评估窗分离**：`warmup_bars` 只喂指标、不进回报计算——"长回看策略偷偷多判一年"这类点对点错误被结构性地排除。
6. **估值引擎缺输入即 NOT RUNNABLE**：宁可"不可运行"也不静默默认值，且拒绝非有限输入——对应 DCF 返回负数股价的经典翻车。
7. **实盘全程 fail-closed**：kill switch 靠持久化 latch + 独占 halt episode、订单门按较差价计价、错误信封当失败、重启后恢复只按精确 client_order_id 且绝不重发——安全边界每一处都有双重保险。
8. **量化的"数学库与技能分离"**：`quantlib` 用被测试的函数承载公式，skill 只 import 不复制 markdown 公式，避免"文档里的公式与代码漂移"。

## 附：主要源文件索引

| 文件 | 内容 |
|---|---|
| `agent/src/agent/grounding/figures.py` | 数字声明块、形状分类、精度匹配 |
| `agent/src/agent/grounding/release.py` | 拒绝/纠正/恢复/剔除/代理 note 修复 |
| `agent/src/agent/grounding/ledger.py` / `evidence.py` | 会话工具证据账本 |
| `agent/src/agent/grounding/identity.py` | 标的身份锁定与符号归一化 |
| `agent/src/agent/loop.py` | Agent 循环（无进度即停/去重/压缩） |
| `agent/src/agent/context_compact.py` | 上下文压缩 |
| `agent/src/governance/manifest.py` | RunManifest（方法学哈希） |
| `agent/src/governance/ledger.py` | 哈希链审计账本 |
| `agent/backtest/loaders/registry.py` | FALLBACK_CHAINS + PRICE_CALIBER + 源别名 |
| `agent/src/market_data.py` | 标的→首选源映射 + 符号归一 |
| `agent/backtest/engines/` | 9 市场回测引擎 |
| `agent/src/live/` | 实盘/审计/kill switch/order gate/mandate |
| `agent/src/swarm/{worker,presets,task_store}.py` | 多 Agent 编排 |
| `agent/src/quantlib/` | 286+ 量化函数（20 模块） |
| `agent/src/tools/` | ~74 MCP/本地工具 |
| `agent/src/factors/zoo/` | Alpha Zoo（462 alpha） |
| `agent/src/strategy_discovery/` + `strategy_store/` | 策略发现/存储 |
| `CHANGELOG.md` | 210KB 修复记录，坑的富矿 |