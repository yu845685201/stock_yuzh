# ai-berkshire 深度分析

> **分析日期**：2026-10-11 · **仓库**：[xbtlin/ai-berkshire](https://github.com/xbtlin/ai-berkshire)（本地 HEAD `a221a20`，2026-10-09）
> **分析依据**：本地克隆源码（`refrences/xbtlin__ai-berkshire`），精读 `CLAUDE.md`/`AGENTS.md`/`ai_CLAUDE.md`、`skills/`（21 个 skill）、`codex-skills/`、`tools/`（15 个脚本）与 README。所有结论来自源码实现、模板与注释，非推断。
> **配套文档**：[开源股票分析项目深度分析-20261011](./开源股票分析项目深度分析-20261011.md)

---

## 0 定位与架构总览

AI Berkshire 是一个**基于 Claude Code / Codex 的个人价值投资研究框架**——不是可编程的 Python 框架，而是一套 **Skill（斜杠命令）+ 辅助工具脚本 + 报告规范 + 准出流程** 的组合。作者把它总结为"一个人 + Claude Code = 一个投研团队"，全量报告 2370 份、109 家公司、23 个专题。

**核心差异点（README 自述）**：直接问 Claude"拼多多值不值得买"，得到的是"一方面…另一方面…投资有风险请自行判断"的平衡话术，没法拿来做决策。AI Berkshire 逼出的是**带具体价格区间和分层建议的强制结论**，并用四位大师的视角**制造真实的矛盾和张力**。

### 四大师框架

| 角色 | 大师视角 | 职责 |
|---|---|---|
| business-analyst | 段永平 | 商业模式 & 护城河（"好生意"标准：差异化/定价权/可持续优势） |
| financial-analyst | 巴菲特 | 财务报表 & 估值（扣现金 PE、安全边际） |
| industry-researcher | 芒格 | 行业格局 & 竞争（逆向思维） |
| risk-assessor | 李录 | 风险 & 管理层 & 长期确定性（10 年后怎么样） |
| team-lead | 四大师综合 | 汇总研判、输出最终报告 |

### 目录与产物

```
skills/*.md            Claude Code 斜杠命令源（21 个，canonical）
codex-skills/*/SKILL.md  Codex 技能包（大多由 skills/ 自动生成）
codex-prompts/*.md      Codex 斜杠 prompt（兼容层）
tools/*.py              15 个共享校验/取数工具（财务严谨/报告抽检/数据源/回测/筛股）
reports/{公司名}/        研究报告（2370 份）
筛选公司/×池             A股/科创板/美股召回池
实盘记录/               真实交易记录
```

---

## 1 数据源来源

数据源规范写在 `skills/financial-data.md`，CLAUDE.md 与 investment-team.md 里索引：

| 市场 | 主源 + 副源（≥2 源交叉） | 工具 |
|---|---|---|
| 美股 | macrotrends + stockanalysis | `tools/financial_rigor.py` |
| 港股 | aastocks + macrotrends | 同上 |
| A股 | 东方财富 + 巨潮资讯 | `tools/ashare_data.py` |
| 台股 | FinMind + Goodinfo | `tools/twstock_data.py`（FinMind） |
| 雪球 | 舆情/讨论 | `tools/xueqiu_scraper.py` |
| 晨星 | 公允价（DCF 参考） | `tools/morningstar_fair_value.py` |
| 终值 | Gordon 终值模型 | `tools/terminal_value.py` |
| 动量回测 | 历史动量 | `tools/momentum_backtest.py` / `momentum_backtest_v2.py` |
| 筛股 | 召回池筛选 | `tools/stock_screener.py` |

**硬规则**："财务数据必须来自两个独立来源…两源误差 > 1% 须标记"（investment-team.md 任务模板原文）；市值必须手算校验（股价 × 总股本，与报告市值比对）；货币单位必须明确（港币/人民币/美元/新台币），防止混淆。

---

## 2 分析逻辑与"Agent/Skill"机制

### 2.1 `/investment-team`：四角色并行多 Agent（招牌 skill）

这是最"多智能体"的一个，执行流程十步（skill 原文）：

1. **展示团队框架**（4 角色 + team-lead，确认后启动）。
2. **AI 研究偏见评估**：先给公司打"信息丰富度"等级，决定研究策略——
   - A 级（信息充裕）→ 团队重点放**反面检验/非共识视角**，避免"正确的废话"；
   - B 级（信息适中）→ 每个推算数字标置信度、lead 汇总标"数据充分度"；
   - C 级（信息稀缺）→ 转"第一性原理模式"，聚焦商业本质的几个核心问题。
   关键提醒："资料多≠确定性高；AI 能输出的置信度 ≠ 投资的真实确定性。确定性来自商业模式本身"。
3. **WebSearch 权限预检**（见 §5 坑 #1）。
4. **TeamCreate 建团队**（`{公司名}-research`，team-lead）。
5. **TaskCreate 建 4 个任务**（各自 subject/description，内含金融严谨性 Bash 工具调用要求与具体命令模板）。
6. **并行启动 4 个 Agent**（同一消息里 4 次 Task，`run_in_background: true`，`subagent_type: general-purpose`）。
7. **接收报告 + 进度跟踪**（SendMessage 通信，非文件协作）。
8. **shutdown_request 关闭成员**。
9. **汇总最终报告**（八段结构：一句话结论 → 四维评分总表(1-5★) → 核心数据速览 → 各维度摘要 → Bull vs Bear(各 5-7 条) → 巴菲特买入前 Checklist(10 项) → 最终建议(分层操作+价格区间+催化剂) → 总结段落）。
10. **数据抽检准出 + TeamDelete**（见 §2.4）。

**最终建议强制格式**（README"镜子测试"）：

| 策略 | 建议 | 价格区间 |
|---|---|---|
| 激进型 | 当前价位可建仓 20% | $95-105 |
| 稳健型 | 等回购政策明确后建仓 | $85-95 |
| 保守型 | 不符合 10 年确定性标准，观望 | — |

配套纪律："**5 句话说不完整 = 不买，没有例外**"。

### 2.2 其余 20 个 skill（`skills/`）

| Skill | 用途 |
|---|---|
| `investment-research` / `private-company-research` | 单公司深度研究 / 未上市公司研究 |
| `investment-checklist` | 巴菲特买入前 10 项 checklist |
| `industry-research` / `industry-funnel` | 行业研究 / 行业漏斗筛选 |
| `earnings-review` / `earnings-team` | 财报复盘 / 财报四大师团队版 |
| `management-deep-dive` | 管理层深挖 |
| `thesis-tracker` / `thesis-drift` | 投资论点长期维护 / 论点漂移监测 |
| `portfolio-review` | 组合复盘（portfolio-latest.md） |
| `quality-screen` / `income-investment` | 质量筛股 / 收息投资 |
| `news-pulse` / `bottleneck-hunter` / `era-alpha` | 新闻脉冲 / 瓶颈猎人 / 时代 alpha |
| `deep-company-series` | 深研系列 |
| `dyp-ask` / `wechat-article` / `financial-data` | 段永平视角问询 / 公众号文章 / 数据源规范 |

### 2.3 工具层：`financial_rigor.py`（精确十进制引擎）

核心子命令全部用 `decimal`（`Context(prec=28, ROUND_HALF_EVEN)`），零三方依赖：

| 子命令 | 用途 |
|---|---|
| `verify-market-cap` | 市值 = 股价 × 总股本，偏差 >5% 报错 / >1% 警告 / ≤1% 通过 |
| `verify-valuation` | 由 price/eps/bvps/fcf/dividend 精确算 PE/PB/ROE/P-FCF/FCF yield/股息率/PS |
| `cross-validate` | 多源数值比中位数，`--tolerance`（默认 2%）内 ✅ 否则 ❌，输出加权中位共识值 |
| `benford` | Benford 定律首位数分布 + MAD + 卡方，判断造假嫌疑 |
| `three-scenario` | 乐观/中性/悲观三情景，`future_eps = eps×(1+g)^years × target_pe` 得目标价与涨跌幅 |
| `calc` | 精确算术（白名单字符 `0-9.+-*/() eE`，禁用内置函数的 eval） |

### 2.4 数据抽检准出流程（`report_audit.py`）

三步准出/打回：`extract`（MARKDOWN 正则抽取财务数字点，**随机抽样 15%**）→ Claude 从可靠信源取数填 `fetched_value` → `verdict`（比对偏差，输出准出/打回判决）。

### 2.5 报告索引（`reports_index.py`）

自动扫描 `reports/` 生成 `reports/index.json` + `README.md` + README 顶部的 REPORTS-BANNER；`--check` 只读校验（CI 也跑这条），文件名 `-YYYYMMDD` 后缀 + 正文首行 `# 标题` 即可被识别，YAML front-matter 可覆盖。新专题/筛选池目录须登记 `reports/_index/config.json`。

---

## 3 最终产物结构与格式

- **research/团队报告**（多 Agent）：`reports/{公司名}/` 下 `01-商业模式分析-段永平视角.md` … `04-风险管理层评估-李录视角.md` + `最终报告.md`（Team Lead 八段式）；单公司 `{公司名}-research-{YYYYMMDD}.md`。
- **评分体系**：★1-5（不含半星），四维评分总表 + 综合评分。
- **Bull vs Bear**：🟢 看多 5-7 条 / 🔴 看空 5-7 条。
- **分层操作建议**：激进/稳健/保守 → 建议 + 价格区间 + 关键催化剂（加仓/减仓各 3-5 条）。
- **公众号/读者评审**（earnings-team 额外产出）。

---

## 4 工程实现要点

### 关键文件

| 路径 | 内容 |
|---|---|
| `CLAUDE.md` | Claude Code 项目指令（报告结构/命名/原则/推送流程） |
| `AGENTS.md` | Codex 兼容规则（canonical 源、双系统同步、质量规则） |
| `skills/*.md` | 21 个 Claude Code 斜杠命令源 |
| `codex-skills/*/SKILL.md` | Codex 技能包（由 `scripts/sync-codex-skills.py` 生成） |
| `scripts/sync-codex-skills.py` | skills→codex-skills 生成（`--check` 校验不写盘） |
| `scripts/sync-codex-prompts.py` | skills→codex-prompts 生成 |
| `scripts/install-*.{sh,bat}` | 三平台安装脚本（Claude 命令/Codex skills/prompts） |
| `tools/financial_rigor.py` | 精确十进制财务验算（6 子命令） |
| `tools/report_audit.py` | 报告数据抽检准出（extract/verdict） |
| `tools/reports_index.py` | 研究报告索引生成 + CI 校验 |
| `tools/{ashare_data,twstock_data,xueqiu_scraper,stock_screener}.py` | 数据取数/筛股 |
| `tools/{morningstar_fair_value,terminal_value,momentum_backtest*}.py` | 估值/动量辅助 |

### 双平台同步机制（AGENTS.md 规定）

`skills/*.md` 是唯一真源；改 skill 后必须 `python3 scripts/sync-codex-skills.py` 重新生成 `codex-skills/`；手动改生成的 SKILL.md 不被允许（除非同步改 source）。`--check` 模式供 CI 校验"生成产物是否过期"。

---

## 5 坑与注意事项（来自 skill/工具注释，逐条）

1. **后台 Agent 静默退化（最危险失败模式，issue #58）**：`run_in_background: true` 的子 Agent **无法弹出交互式权限确认**。若 `WebSearch` 不在 `.claude/settings.local.json` 的 `permissions.allow` 白名单，子 Agent 的联网搜索被**静默拦截**，退化为仅凭训练知识作答，却仍按框架输出"看起来完整、实则未联网"的伪研究。skill 强制在启动 Agent 前用 Bash `grep -l '"WebSearch"'` 预检，未放行则停下提示用户，不启动。
2. **联网失败禁止伪装**：Agent prompt 模板硬性要求——"若 WebSearch 被拦截/不可用，禁止用训练知识冒充联网结果，必须在报告顶部醒目标注「⚠️ 本报告未能联网，基于训练知识（截止日期 X），置信度降级」"。
3. **Windows GBK 控制台崩溃**：`financial_rigor.py` 输出的 ❌/⚠️/✅ 在 GBK 控制台会抛 `UnicodeEncodeError`，导致"偏差超标"这条最该被看到的告警**反而直接崩溃**。修法：`_force_utf8_stdio()` 用 `stream.reconfigure(encoding='utf-8', errors='replace')`。
4. **负号被抽检正则吃掉**：`report_audit.py` 里所有数字捕获组必须带 `_SIGN`（覆盖 ASCII 正负、U+2212 en-dash/em-dash、全角）——否则 `-1.72%` 被抓成 `1.72`，报告值 vs 信源值符号相反、偏差 200%，产生**假打回**。
5. **推送前必须刷新索引**：`reports_index.py` 不跑就推，CI 的索引校验会失败；commit 中文、`git pull --rebase`、不推中间过程文件（如 data_collection.md）。
6. **市值手算 + 货币单位**：价格 × 总股本 必须与报告市值对齐；港币/人民币/美元/新台币不得混淆。
7. **两源误差 >1% 须标记**：单一来源的可信度不被承认，交叉验证是准出前提。
8. **"资料多≠确定性高"**：信息丰富度评级是"调整研究策略"的信号（A 级反而要专门做反面检验），不是确定性的前提；AI 的置信度≠投资确定性。
9. **评分不含半星**：★1-5 整颗，避免人肉调分的含糊地带。
10. **反偏见纪律**：team-lead 汇总时必须评估"各 Agent 是否受限于资料充裕度、是否与市场共识过度趋同"，最终报告须含"信息丰富度评级"和"AI 研究局限性声明"。

---

## 6 可借鉴的设计要点

1. **"Skill 即研究 SOP"**：把投资研究流程固化成 21 个可复用的斜杠命令（含任务模板、工具调用命令、输出结构、"镜子测试"），让个人也能复现一个团队的工作流——比"直接问 LLM"高一个量级的地方在于**过程可重复、产物可审计**。
2. **四大师视角制造张力**：不是"用巴菲特方法分析一下"，而是四套互相冲突的方法论各自独立出分（段永平 3.7 / 巴菲特 4.4 / 芒格 3.5 / 李录 2.0 这种真实分歧），team-lead 只综合不抹平。
3. **数据严谨性工具化**：`financial_rigor.py` 用 decimal 精确算估值、`cross-validate` 多源比中位数、`benford` 查造假、`three-scenario` 三情景——把"数字不靠模型心算"这一原则落成命令行工具。
4. **准出前 15% 随机抽检**：不是"写完就发"，而是 `extract → 信源取数 → verdict` 的抽样复核闸门——把质量关放进流程而非靠自觉。
5. **对抗"静默退化"的工程意识**：WebSearch 权限预检、联网失败醒目标注、Windows 编码兜底、`_SIGN` 符号位——每一处都是"LLM 工作流里最隐蔽的失败模式"。
6. **双平台（Claude/Codex）同源同步**：`skills/` 单真源 + 生成脚本 + `--check` CI 校验，一个框架同时服务两套 agent 生态且不产生源码分叉。
7. **研究索引自动维护**：报告文件名规范 + `reports_index.py` 自动生成索引与 README，2370 份报告可导航、CI 防漂移。

## 附：主要源文件索引

| 文件 | 内容 |
|---|---|
| `CLAUDE.md` | Claude Code 项目指令（目录/命名/原则/推送） |
| `AGENTS.md` | Codex 兼容规则与 syncing 契约 |
| `skills/investment-team.md` | 四角色并行多 Agent 研究（10 步流程） |
| `skills/financial-data.md` | 数据源规范（两源交叉/误差阈值） |
| `skills/{investment-checklist,earnings-team,industry-funnel,...}.md` | 其余 20 个研究 skill |
| `tools/financial_rigor.py` | 精确十进制财务验算（市值/估值/交叉/Benford/三情景） |
| `tools/report_audit.py` | 报告 15% 抽检准出 |
| `tools/reports_index.py` | 研究报告索引生成 |
| `tools/{ashare_data,twstock_data,xueqiu_scraper,stock_screener}.py` | 取数与筛股 |
| `scripts/sync-codex-skills.py` / `sync-codex-prompts.py` | skills→codex 生成 |
| `scripts/install-*.{sh,bat}` | 本地安装脚本 |
| `reports/_index/` | 索引配置与 front-matter 规范 |