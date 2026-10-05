# 港美股指数与港股个股日K采集技术方案 V1.0（20261004）

## 1. 背景与目标

每日复盘目前的外围市场数据只有**当日快照**（`overseas_market_daily`：close + 涨跌幅两个值，
7 个代码），无 OHLCV 历史。由此带来的复盘缺口：

1. 外围章节算不了 5 日/20 日趋势、60 日位置百分位（A 股指数章有、外围没有）；
2. 节后首个交易日无法量化"假期中外围的累积变化"（如本次国庆：A 股 10-01~10-08 休市，
   港股 10-02 起开市，目前报告口径完全覆盖不到）;
3. 港股核心个股（腾讯/美团/阿里/恒生科技权重等）与 A 股板块存在强联动
   （A-H 溢价、恒生科技对 A 股科技的引导），当前零覆盖。

**目标**：新增外围日K采集——港股 3 指数 + 美股 3 指数 + 港股核心个股池（33 只，
其中 15 只 A-H 两地公司携带 A 股 ts_code 映射，A 股侧复用库内 `his_kline_day` 不重复采集），
落库 OHLCV 历史，盘后增量 + 全量初始化，为报告层"外围趋势/位置百分位/A-H 映射观察"
（第二步，另行实施）供数。

**边界**：本方案只做**采集与数据质量**层；报告层消费增强（外围章节升级、A-H 映射小节）
列为阶段 2，实施前按需细化。分析判定口径零改动。

## 2. 现状与差距

### 2.1 现有外围快照（`overseas_market_daily`）

- config `review_sync.sina_markets` 7 码：美股三大 `gb_*`（10-04 O4 换源）、恒生 `rt_hkHSI`、
  A50 `hf_CHA50CFD`、汇率 `fx_*`×2；sina 唯一来源，仅快照两值。
- 快照的 `trade_date` 以 A 股采集日落库，A 股休市期间无行——**假期外围观察为空白**。

### 2.2 顺带实锤的快照 bug（本次调研发现，P0 一并修）

`sina_source.py` `rt_hk*` 分支（约 133~140 行）把 `fields[2]` 当 close，而 sina
`rt_hkHSI` 实际布局为：

```
[0]代码 [1]名称 [2]今开 [3]昨收 [4]最高 [5]最低 [6]最新/收盘 [7]涨跌额 [8]涨跌幅% … [17]日期
```

证据（库内 4 行 close 逐行等于腾讯日K的**开盘价**）：

| 库内行（A 股日） | 库内 close | 腾讯日K对应市场日 open | 实际收盘 |
|---|---|---|---|
| 20260927 | 24523.50 | 09-25 open 24523.50 | 24510.09 |
| 20260928 | 24554.58 | 09-28 open 24554.58 | 24642.51 |
| 20260929 | 24648.64 | 09-29 open 24648.64 | 24523.57 |
| 20260930 | 24099.73 | 10-02 open 24099.73 | 23972.29 |

（0928 为低开高走日，库值=open≠high，排除"取最高"的可能。）涨跌幅字段 `[8]` 取值正确，
**只有 close 列自 09-27 上线起一直是开盘价**，历次报告外围市场表恒生"收盘"列失真。

## 3. 数据源选型与实测（2026-10-04）

### 3.1 选定：腾讯 ifzq `fqkline/get`（单接口族覆盖全部三类需求）

```
GET https://web.ifzq.gtimg.cn/appstock/app/fqkline/get
    ?param=<code>,day[,<start>,<end>],<count>,qfq
```

- code 前缀：港股指数 `hkHSI / hkHSCEI / hkHSTECH`；港股个股 `hk00700`（5 位代码）；
  美股指数 `usDJI / usIXIC / usINX`。
- 响应：`{code:0, data:{<code>:{day:[[date,open,close,high,low,vol,(第7元素)],…], qt:{实时块}}}}`。
  **day 行布局 `[date, open, close, high, low, volume]`**（close 在第 2 位，勿与 open 颠倒）；
  港股**个股**行可带第 7 元素（公司行动字典：回购/分红），布局可能为空/字典，解析需防御式；
  指数行无第 7 元素。
- `qt` 实时块内嵌，可与 day 末行互证（采集后自洽校验用）。
- **实测结论**（2026-10-04，全部通过）：
  - 7 个代码（3 港指 + 1 港股个股 + 3 美指）日线全通；
  - `count=800` 单请求 800 根（hkHSI 返回 2023-07-04~2026-10-02）——全量 10 年按日期分窗
    4 请求/代码即可；
  - 日期区间形式 `param=hk00700,day,2016-01-01,2016-12-31,640,qfq` 通过，**qfq 历史可回溯**
    （2016 年前复权价正常返回）；
  - usDJI 10-02 收 51176.96，与 O4 三方对拍（ifzq=qt=sina gb_）一致。

### 3.2 已排除源（O4 探路 + 本次确认，不重复踩）

| 源 | 结论 |
|---|---|
| yahoo chart | 非 JSON 响应，不可用 |
| stooq | JS 盾，不可用 |
| 东财 push2his | `secid=100.DJI` 返回 null |
| 新浪 K 线接口 | 港股/美股历史日线无可靠接口（gb_* 仅实时快照） |

安全红线（与 `sina_source.py` 既有红线一致）：正常 UA/Referer、串行限速、
不破解任何技术措施、风控识别即退避冷却、仅自用不分发。ifzq 无官方文档，字段布局
属实测记录，**每次解析变更前先 curl 实测核对**（本方案 §2.2 的字段位次坑即前车之鉴）。

## 4. 采集范围

### 4.1 指数（6 个，config 清单化）

| 代码 | 名称 | 备注 |
|---|---|---|
| hkHSI | 恒生指数 | |
| hkHSCEI | 恒生国企指数 | |
| hkHSTECH | 恒生科技指数 | 对 A 股科技板块引导性最强；2020-07-27 发布，历史短属正常 |
| usDJI / usIXIC / usINX | 道指/纳指/标普500 | 与 gb_* 快照构成双源互备 |

### 4.2 港股个股池（建议初始 33 只，**config 清单化，由用户定稿增删**）

池子分两组：**A-H 两地上市**（config 携带 `a_code` 映射，两地对比用）与**仅港股**。

**A-H 两地上市（15 只，H 股采集 + A 股映射）**：

| H 股代码 | 名称 | a_code（A 股 ts_code） |
|---|---|---|
| hk01211 | 比亚迪股份 | sz.002594 |
| hk03968 | 招商银行 | sh.600036 |
| hk01288 | 农业银行 | sh.601288 |
| hk02359 | 药明康德 | sh.603259 |
| hk02196 | 复星医药 | sh.600196 |
| hk01880 | 中国中免 | sh.601888 |
| hk02333 | 长城汽车 | sh.601633 |
| hk00168 | 青岛啤酒 | sh.600600 |
| hk06030 | 中信证券 | sh.600030 |
| hk00857 | 中国石油 | sh.601857 |
| hk00883 | 中国海洋石油 | sh.600938 |
| hk00386 | 中国石化 | sh.600028 |
| hk02628 | 中国人寿 | sh.601628 |
| hk01766 | 中国中车 | sh.601766 |
| hk00981 | 中芯国际 | sh.688981 |

**仅港股（18 只）**：00700 腾讯、03690 美团、09988 阿里巴巴、01810 小米、
09618 京东、01024 快手、09999 网易、09888 百度、06618 京东健康、00241 阿里健康、
02015 理想、09868 小鹏、09866 蔚来、09961 携程、09626 哔哩哔哩、00175 吉利汽车、
01109 华润置地、00688 中国海外发展。
（注：吉利汽车、华润置地、中国海外发展及恒生科技权重多数无 A 股，归仅港股组。）

config 结构（`review_sync.overseas_kline_stocks`，同 `sina_markets` 风格）：

```yaml
overseas_kline_stocks:
  - {code: hk01211, name: 比亚迪股份, a_code: sz.002594}   # A-H 两地：a_code 为 A 股 ts_code
  - {code: hk00700, name: 腾讯控股}                          # 仅港股：无 a_code
```

**A 股侧不采集、只做映射**：A 股日K在 `his_kline_day` 已有全量（含前复权因子体系），
重复采集进外围表会造成双表冗余与复权/日历口径分裂。`a_code` 仅作为消费层的
join 键——报告层 A-H 对比时，H 侧取 `overseas_kline_day`、A 侧取 `his_kline_day`，
经 `a_code` 关联（见 §5 决策 4 与 §8）。

## 5. 表设计

新表 `overseas_kline_day`（挂 `dao_review_sync.py` ReviewDaoMixin，DDL 风格与
`overseas_market_daily` 一致）：

```sql
CREATE TABLE IF NOT EXISTS overseas_kline_day (
    id BIGSERIAL PRIMARY KEY,
    market_code VARCHAR(20) NOT NULL,   -- hkHSI / usDJI / hk00700 …
    trade_date VARCHAR(8) NOT NULL,     -- 市场自身交易日 YYYYMMDD（非 A 股日历！）
    open  NUMERIC(20, 4),
    high  NUMERIC(20, 4),
    low   NUMERIC(20, 4),
    close NUMERIC(20, 4),
    volume NUMERIC(24, 4),
    adj_type VARCHAR(8) DEFAULT 'raw',  -- raw=指数/不复权；qfq=个股前复权（滚动口径）
    source VARCHAR(16) DEFAULT 'ifzq',
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (market_code, trade_date)
);
```

**关键设计决策**：

1. **`trade_date` 用市场自身交易日历**（港股 10-02 有行、A 股休市）——这是本表与
   `overseas_market_daily`（A 股采集日落库）的本质差异，也是"假期外围观察"能力的前提。
   报告层取数口径：A 股锚定日 D → 取 D 或 D 前该市场最近交易日（与 O4 回补口径一致）。
2. **个股存 qfq（前复权）**：复盘用途（趋势/位置百分位/两地比价）需要连续序列，
   qfq 直接可用；取舍是公司行动后历史值会滚动变化（跨版本重算报告历史段会有差异），
   `adj_type` 列显式标注。未来若需 raw，接口去 qfq 参数重拉即可，表结构不用动。
   指数无复权概念，标 `raw`。
3. **不与 A 股 `his_kline_day` / 指数日K表混表**：日历体系、代码体系、复权体系均不同。
4. **A-H 两地公司的 A 股侧不入本表**：经 config `a_code` 映射，消费层直接 join
   `his_kline_day`（全市场已有、含独立复权体系），本表只存 H 股/外围侧——避免同一
   A 股标的双表双口径。`a_code` 完整性在采集器启动时校验（须存在于 `base_stock_info`，
   缺失则告警不阻塞）。

## 6. 采集器设计

### 6.1 Manager 与 CLI

`src/sync/overseas_kline_manager.py`（复用 `HttpCollectorBase` + `ReviewBlockedError`
风控模式，同 `overseas_manager` 骨架）：

```
CLI sync-overseas-kline              # 盘后增量：全部配置代码尾部追加（每代码 1 请求）
    --init                           # 全量初始化：按年份分窗拉历史（10 年）
    --codes hkHSI,hk00700            # 指定代码（调试/补采）
    --start-date / --end-date        # 区间补采（回补模式）
    --dry-run                        # 试运行：只列目标清单与请求计划
```

- 增量判定：读库内该代码最新 `trade_date`，请求 `count=20` 尾部窗口，upsert 去重
  （UNIQUE 约束兜底），幂等可重跑。
- init：每代码按 10 年 × ~250 交易日 ≈ 2500 根，按 `count=800` 日期分窗 4 请求；
  42 代码 × 4 ≈ 170 请求，串行 1~2s 限速，全量约 5 分钟。
- **采集时点约束**：港股 16:00 收盘（A 股 15:00 收盘后、16:10 前跑编排器会拿到港股
  盘中未收盘数据）；美股为北京时间昨夜收盘（T-1，口径与现有快照一致）。编排器日常
  深夜执行（实测 23:17）无冲突；若提前执行，港股当日行次日增量自动补齐（幂等 upsert）。
- 自洽校验：day 末行 vs qt 实时块 close 不一致时告警不入库（腾讯内部自证）。

### 6.2 编排器接入（`review_sync_orchestrator.py`）

- `REVIEW_SYNC_SOURCES` 增加 `'overseas_kline'`，标签"外围日K"；
- 波次：放入第一波（与 overseas 等并发；本源对外围快照无依赖，且互拍在采集器内部做）；
- `sync-review-all` 缺省纳入，`sync-review-data --sources` 可单独指定。

### 6.3 单元测试

- day 行解析（含港股个股第 7 元素为空/字典两种样本）；
- open/close 位次防颠倒回归（用真实响应样本固化）；
- 增量 upsert 幂等（同日重跑零重复）；
- config `a_code` 映射校验（合法 ts_code 且存在于 `base_stock_info`）。

## 7. 数据质量设计

1. **双源对拍（采集器内置）**：美股 `usDJI/usIXIC/usINX` 当日 close ↔ `gb_*` 快照
   close；港股指数 ↔ 修复后的 `rt_hkHSI`；偏差 > 0.5% 记告警（不阻塞入库，报告点名）。
2. **冻结 tripwire**：任一代码 close 连续 ≥3 个交易日完全相同 → 采集状态标"疑似冻结"
   （复用 O4 `backfill_overseas_frozen.py` 的哨兵思路，进阶为采集器内置）。
3. **P0：修复 `rt_hkHSI` 字段位次**（§2.2）：`sina_source.py` `rt_hk*` 分支
   `close` 取 `fields[6]`（原 `fields[2]`），注释同步更正实测布局；
   回补 `overseas_market_daily` 既有 `rt_hkHSI` 行（量小，逐行 update close）。
   **此项独立于本方案先行实施**（15 分钟级），避免报告恒生收盘继续失真。

## 8. 报告层消费规划（阶段 2，本方案不实施）

供数就绪后的增强方向（实施前按需出细化设计）：

1. **外围市场章节升级**（复盘框架.MD §2.1 增补口径）：指数行增加 5 日累计涨跌、
   20 日趋势方向、60 日位置百分位——与 A 股指数章同口径；
2. **假期外围观察**：A 股休市区间的外围累积变化（节后首日复盘的必看项，
   10-09 首验即用）；
3. **A-H/外围映射观察小节**：恒生科技 vs 科创50/创业板联动、核心 A-H 股两地表现对比、
   隔夜美股风险偏好对 A 股开盘的引导。数据路径：
   - H 侧 `overseas_kline_day`（qfq）+ A 侧 `his_kline_day`（前复权），经 config
     `a_code` 映射 join；**两侧交易日历不同，对齐口径取"A 股锚定日 D → 各自 D 或 D 前
     最近市场交易日"后再比较涨跌幅**（不直接比价格）；
   - **A-H 溢价率**（可选进阶）：`A 价 /（H 价 × 港元兑人民币）− 1`，依赖汇率快照。
     汇率源实测（2026-10-04）：sina `fx_shkdcny` 可用（0.85449，尾部带日期，
     布局同既有 `fx_*` 分支）；腾讯 `hkHSAHP`（AH 溢价指数）日K为空，不可用。
     落地动作：config `sina_markets` 增补一行 `{code: fx_shkdcny, name: 港元兑人民币}`，
     随既有快照采集入库，无需新采集器。
4. skill 必检清单同步增补：外围日K的 qfq 口径、双源对拍告警项。

## 9. 实施计划

| 序 | 项 | 工作量 | 依赖 |
|---|---|---|---|
| 0 | P0 修复 rt_hkHSI 位次 + 回补快照历史行 | 15 分钟 | 无，先行 |
| 1 | 表 + DAO + Manager + CLI + 单测 | ~1 天 | 序 0（对拍依赖修复后快照） |
| 2 | 编排器接入 + config 清单定稿 | 0.5 小时 | 序 1 |
| 3 | `--init` 全量回补（~5 分钟）+ 对拍验收 | 0.5 小时 | 序 1 |
| 4 | 阶段 2 报告层消费（外围趋势/位置百分位/A-H 映射/假期观察） | 0.5~1 天 | 序 3，另行细化 |

**验收标准**：

1. `overseas_kline_day` 6 指数 + 个股池全量在库，抽样 3 代码与腾讯网页版日线对拍一致；
2. 美指当日 close 与 `gb_*` 快照双源一致；恒生与修复后 `rt_hkHSI` 一致；
3. 增量重跑幂等（同日二次执行零新增零变更）；
4. pytest 全绿（含新增解析与位次回归测试）；
5. A 股休市期间外围行可见（如 10-02 港股行）；
6. A-H 映射校验通过：全部 `a_code` 在 `base_stock_info` 存在，抽样 3 组两地日K
   可按锚定日映射对齐（如比亚迪 sz.002594 ↔ hk01211）。

**时间建议**：假期内完成序 0~3，10-09 节后首日复盘即带"假期外围累积变化"（阶段 2
最小子集可突击：仅假期区间统计，不做完整 60 日百分位）。

## 10. 风险与注意事项

1. **ifzq 非官方文档接口**：字段布局以实测为准，解析变更前必先 curl 核对；
   安全红线同既有 sina 采集（串行限速/正常 UA/风控退避/仅自用）。
2. **限速阈值未知**：init 170 请求串行 1~2s 起步；若触发风控（非 0 code/超时聚集），
   退避冷却后断点续跑（幂等设计保证）。
3. **qfq 滚动口径**：公司行动后历史值变化，跨版本报告重算历史段会有差异——
   `adj_type` 显式标注，复盘框架口径注明。
4. **港股个股第 7 元素布局不定**：空/字典/不同键，防御式解析，解析失败的行
   如实跳过并记失败清单（同"数据真实性优先"既有原则）。
5. **港股 16:00 收盘时点**：16:10 前跑编排器港股当日为盘中数据；深夜执行无冲突，
   skill 时点表增补"外围日K：港股 16:10 后、美股 T-1"。
6. **表会存 A 股休市日的行**（市场自身日历），这是设计特性不是脏数据；
   下游消费一律按"A 股锚定日 → D 或 D 前最近市场交易日"映射，禁止直接等值 join。
7. **不做 git 操作**：改动留工作区，入库由用户决定；长回补（如未来扩池）用
   nohup + caffeinate。

---

## 11. 实施记录（2026-10-04 深夜，序 0~3 完成）

**§3.1 勘误（实施中实测发现）**：`fqkline/get` 对美股代码**损坏**——尾部请求恒回 1 根、
日期区间回 0 根（首次 --init 美股只入 1 行即此因）。美股必须走 `usfqkline/get` 同形端点：
end 生效、**start 被忽略**、count = 自 end 向后取的根数（向后分页）。本实现 1100 自然日
窗口（≈760 交易日）< 800 根可整窗覆盖，窗口起点之前多出的行由 manager 按窗口边界过滤。
港股 `fqkline/get` 语义实测：start/end 双边界生效，count 在窗口内从 end 截断。

**序 0（P0）**：`sina_source.py` rt_hk* close 取 `[6]`（原误取 `[2]` 今开）；
4 行历史按 ifzq hkHSI 日线"开盘价匹配"逐行回补真实收盘
（`backend/scripts/fix_rthk_close_backfill.py`，幂等，报告 rthk_close_backfill_report_*）。

**序 1~2 落地物**：
- 表 `overseas_kline_day`（§5 设计原样；DDL 同步 doc/init-V3.sql）+ DAO 四方法
  （dao_review_sync.py：upsert_overseas_klines / fetch_overseas_kline_latest_date /
  fetch_overseas_klines_since / fetch_overseas_kline_recent_closes）
- 源 `src/data_sources/ifzq_source.py`：IfzqSource（按代码路由端点；day 行
  [date,open,close,high,low,vol] 解析、第 7 元素防御式；qt[3]/qt[30] 自洽校验输入）+
  IfzqSourceError（结构变更，区别于风控急停）
- 采集器 `src/sync/overseas_kline_manager.py`：增量尾部 20 根 / 无行代码尾部 800 根种子 /
  --init 10 年分窗 4 请求/代码 / --start-date+--end-date 区间补采 / --dry-run 计划清单；
  **qfq 重述检测**（增量尾部与库内重叠 close 变化 >0.1% → 该代码全历史重拉，防公司行动后
  新旧复权基准拼接跳变）；冻结 tripwire（连续 3 市场日 close 同值标疑似）；a_code 校验
  （base_stock_info 缺失告警不阻塞）；双源对拍内置（usDJI/usIXIC/usINX ↔ gb_*、
  hkHSI ↔ rt_hkHSI 实时，偏差 >0.5% 告警不阻塞）
- CLI `sync-overseas-kline`；编排器波 1 挂载（源键 `overseas_kline`，标签"外围日K"）
- config：`overseas_kline_indices`（6）+ `overseas_kline_stocks`（15 A-H 带 a_code +
  18 仅港股）+ `overseas_kline_interval: 1.5`
- 测试 `backend/test/test_overseas_kline.py` 23 项（真实响应样本固化的位次回归、
  第 7 元素三形态、qt 自洽三场景、幂等、重述触发、分窗过滤、a_code、编排器挂载）

**序 3 验收（§9 六项全过）**：
1. 39 代码全量在库 **80,629 行**（HK 2463~2462 行/20161003 起、HSTECH 1523 行/20200727
   发布日起、美股 2514 行/20161003 起）；--init 4.4 分钟（156 窗口 = 4 请求/代码）；
2. 对拍：道指/纳指/恒生 kline ↔ 快照偏差 **0.0000%**；抽样 hkHSI 23972.29 / hk00700
   421.20 / usDJI 51176.96（10-02 收盘）与腾讯网页一致；
3. 增量重跑幂等（同日二跑 20 根重叠 upsert 零新增）；
4. pytest **471 绿**（新增 23 项 + 波次测试期望序列更新）；
5. A 股休市日外围行在库（10-02 全部 39 代码、10-01 美股 3 代码——港股当日休市，真实分布）；
6. a_code 15/15 在 base_stock_info；A-H 映射锚定日对齐抽样（hk01211↔sz.002594 等 3 组
   同锚定日 20260930 各自 close）。

**skill 时点表已增补**（§10.5）：外围日K = 港股 16:10 后、美股 T-1（.zcode 与
.workbuddy 源件已同步）。**阶段 2（§8 报告层消费）未实施**——原因盘点与细化设计的
问题清单（Q1~Q11）见独立文档
**doc/港美股日K阶段2未实施原因与细化清单-V1.0-20261004.md**；10-09 节后首日复盘可
先行用库内数据做"假期外围累积变化"最小子集。
