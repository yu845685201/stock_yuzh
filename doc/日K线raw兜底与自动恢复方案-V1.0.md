# 日K线 raw 兜底与自动恢复方案 V1.0

- 日期：2026-11-18
- 状态：**已实施**（2026-11-18，改动见下"实施记录"；未 commit）
- 触发背景：2026-11-17 盘后 THS 前复权序列对 280 只股票缺当日 bar（q_factor=0），采集端 merged 以 qfq 为主表、raw 行被直接跳过，当日数据采不进；隔夜 THS 恢复后自愈。

## 实施记录（2026-11-18）

| 步骤 | 内容 | 落点 |
|---|---|---|
| D1 | 台账表 `kline_day_raw_fallback` DDL（init.sql 追加 + 运行时幂等创建，uat 库已应用） | `doc/init.sql`、`dao_kline_day.py` |
| D2 | merge 层兜底（尾部缺口判定 + 除权校验 `_raw_fallback_no_exrights` 返回 (ok, reason)，reason 区分 missing_last/exrights_suspected）+ normalize 标记传递（`_raw_fallback` → `source='TDXAPI_RAWFB'`）+ 台账随批写入 | `sync_manager.py`、`kline_day_pipeline.py`、`dao_kline_day.py` |
| D3 | 恢复阶段 `_recover_raw_fallback`（主同步末尾自动执行；定点行更新只动价格/preclose/change_rate/source，防 DO UPDATE 无条件覆盖把其他字段改 NULL） | `sync_manager.py` |
| D4 | 配置 `sync.kline_day_raw_fallback.{enabled,max_stocks_per_run}` + `RawFallbackBudget`（线程安全一股一额度）+ 报告新增"raw 兜底（V1.0）"节 + result 键 fallback_written/skipped_exrights/recovered | `config.yaml`、`kline_day_pipeline.py`、`sync_manager.py` |
| D5 | 单测 14 个新场景（`test_kline_day_raw_fallback.py`）+ 既有 merge/refetch 测试适配元组返回；全量回归 1229 passed（仅 2 个遗留的 SKILL.md 副本漂移失败，与本次无关）；真实单股增量冒烟通过（报告节/台账初始化/恢复空转均正常） | `test/` |

设计偏离说明：merge 返回值从 `List` 改为 `(merged, raw_skips)` 元组（携带除权跳过明细）；恢复阶段不做 trade_date < 目标日限制（所有 open 记录统一重拉 qfq——主流程同日覆盖时数值为空操作，隔日时完成真正的 qfq 换轨）。

---

## 1. 背景与根因链（已实测确认）

```
THS 前复权 all.js 当日未发布
  → ① 中间件 qfq 序列缺当日 bar（getQfqKlineDay 直出 THS 前复权，web/server.go:173-181）
  → ② 因子 map 取键落空 → q_factor=0（spider-ths.go:32-36，Go map 零值）
  → ③ 采集端 merged 以 qfq 为主表，raw 行日期不在 qfq 主表即 continue 跳过
     （sync_manager.py:_merge_kline_day_sources 448-457 行）
  → 0 行入库，重试无效
```

关键事实：**每股每次同步都同时拉取 qfq + raw 双源**（kline_day_pipeline.py:109-113），raw 行数据在内存里现成，只是被 merge 丢掉。兜底 = 把这份已有数据用起来，**零额外行情请求**。

## 2. 方案三步 → 实现件映射

| 用户方案 | 实现件 | 位置 |
|---|---|---|
| ① q_factor=0 时 raw 兜底 | merge 层兜底逻辑（含除权校验） | `_merge_kline_day_sources` 改造 |
| ② 兜底数据打标记 | 行级标记：`his_kline_day.source='TDXAPI_RAWFB'`（复用现有列，零 schema 变更）＋ 台账表 `kline_day_raw_fallback`（生命周期记录） | DAO 新增 |
| ③ 下次采集专门处理标记数据 | 恢复阶段：每次 sync_kline_day 自动执行，qfq 补齐后覆盖兜底行并销账 | sync_manager 新增 `_recover_raw_fallback` |

**一处修订建议**：触发条件建议用"**qfq 缺目标交易日 ∧ raw 有该日 ∧ 除权校验通过**"，而不是逐股调 `/api/ths-factor` 确认 q_factor=0。理由：qfq 缺日本身就是 THS 未发布的直接证据（两者同源同因），逐股查因子接口是每股一次额外网络请求，收益仅是诊断信息。q_factor 值（能取到时）记入台账 reason 字段，不作为阻塞条件。

## 3. 详细设计

### 3.1 兜底写入（merge 层）

改造 `_merge_kline_day_sources(qfq_list, raw_list, trade_dates)`：

```python
for item in raw_list:
    td = normalize(item['Time'])
    row = merged.get(td)
    if row is None:
        # 【新增】原逻辑直接 continue；改为兜底判定
        if td not in trade_dates:            # 请求区间外（P-11 日期过滤语义），仍跳过
            continue
        if not raw_fallback_enabled:         # config 开关，默认 true
            continue
        if not _no_exrights(raw_item, raw_list):   # 除权校验，见 3.2
            skipped_exrights.append(td)            # 记台账 skip，见 3.3
            continue
        merged[td] = {                             # 构造兜底行（中间件原始单位，走同一 normalize）
            'Time': item['Time'], 'Open': item['Open'], 'High': item['High'],
            'Low': item['Low'], 'Close': item['Close'], 'Last': item['Last'],
            'Volume': item['Volume'], 'Amount': item['Amount'],
            'RawClose': item['Close'],             # raw 兜底行 close == raw_close
            'raw_fallback': True,                  # 标记随行传递到写库
        }
        fallback_dates.append(td)
    else:
        row['Amount'] = item['Amount']             # 原逻辑不变
        row['RawClose'] = item['Close']
```

- 兜底行与正常行走**同一条 normalize 路径**（preclose 链 / change_rate / 单位换算 / 基本面匹配），保证库内口径一致（preclose=前日 close 复制、change_rate=close/preclose-1）。
- init 模式与增量模式统一生效（merge 是共用出口）；北交所官网盘后包路径（bj_kline_manager）不经过此函数，**不在范围内**。

### 3.2 除权校验（唯一风险门）

```python
def _no_exrights(raw_item, raw_list) -> bool:
    # raw 序列内相邻关系：本日 Last（上一收盘）== 前一交易日 raw Close
    # 兼容首元素（raw_list 内无前日）：回落查库 fetch_prev_his_kline_day_close（dao_kline_day.py:165）
    return raw_item['Last'] == prev_raw_close
```

- 原理：除权除息日 raw 的"上一收盘"会被交易所调整为除权基准价，必不等于前一日真实收盘；无除权日两者相等。
- 不相等 → **不兜底、不入库**，台账记 `status='skip_exrights'`，等 THS 前复权恢复后由标准同步/恢复阶段正常入库。
- 方向性加强校验（可选二道保险）：除权只会下调 preclose，`Last > 前日 close` 的情况（preclose 上调）必非除权，也跳过（防 raw 数据异常）。

### 3.3 台账表（生命周期记录）

```sql
CREATE TABLE IF NOT EXISTS kline_day_raw_fallback (
    ts_code        varchar(16)  NOT NULL,
    trade_date     varchar(8)   NOT NULL,           -- 无横线，与库内口径一致
    status         varchar(16)  NOT NULL,           -- 'fallback' | 'skip_exrights' | 'recovered'
    reason         varchar(32),                     -- 'qfq_missing'（q_factor=0 同义，能取到时附值）
    q_factor       numeric,                         -- 兜底时点因子值快照（可空）
    raw_close      numeric,                         -- 兜底写入值快照（审计用）
    fallback_at    timestamptz   DEFAULT now(),
    recovered_at   timestamptz,                     -- qfq 覆盖成功时间
    recover_source varchar(24),                     -- 'main_sync' | 'recovery_stage'
    PRIMARY KEY (ts_code, trade_date)
);
```

- 行级标记：兜底行写库时 `source='TDXAPI_RAWFB'`（现有取值 `TDXAPI`/`TDX_OFFICIAL`，已 grep 确认下游无任何按 `his_kline_day.source` 过滤的逻辑，安全）。恢复后被主 upsert 自然覆盖回 `TDXAPI`。
- 台账写入与 kline 写库同事务/同批次先后提交；PK 幂等，重复兜底（重跑）不产生重复行。

### 3.4 恢复阶段（每次 sync_kline_day 自动执行）

主批循环结束后、报告生成前，调用 `_recover_raw_fallback(trade_dates)`：

1. `SELECT ts_code, trade_date FROM kline_day_raw_fallback WHERE status='fallback' AND trade_date < 本次目标日`（**当日兜底、当日重跑**的股票不需要本阶段——主同步的 qfq tail 已含该日，upsert 覆盖时 source 回 `TDXAPI`，在主流程内直接销账）。
2. 对每股跑 `get_kline_qfq_tail(code, N)`（N 覆盖到最老 fallback 日），取到目标日 qfq → 走同一 normalize → upsert（source=`TDXAPI`）→ 台账 `status='recovered', recovered_at=now(), recover_source='recovery_stage'`。
3. qfq 仍缺该日 → 保留 `fallback` 状态，下次再试（无失败计数、不告警——上游发布节奏不可控）。
4. 并行度与限速沿用现有 `kline_max_workers`/中间件惯例；通常 open 记录为 0~几十条，空转成本 ≈ 1 次查库。

### 3.5 配置与上限

```yaml
sync:
  kline_day_raw_fallback:
    enabled: true                  # 总开关（默认开）
    max_stocks_per_run: 1500       # 单次运行兜底股票数上限，防系统性故障时异常放大
```

超上限时：兜底动作整体让位（记报告告警，不部分执行——避免"半市场 qfq + 半市场 raw"的隐性口径混拼）。

### 3.6 报告与可观测

- `sync_kline_day` 返回值新增键：`fallback_written` / `fallback_skipped_exrights` / `fallback_recovered`（计数 + 明细清单）。
- 日K同步报告（`kline_day_sync_report_*.md`）新增一节：本次兜底/跳过/恢复明细。
- 编排器（review orchestrator）日K门控行数自动包含兜底行——**当日复盘数据完整性不再受 THS 节奏影响**。

## 4. 边界情况清单

| 情形 | 行为 |
|---|---|
| 停牌（raw 也缺该日） | 兜底天然不触发（raw_list 无该日），维持现有"连续缺行判停牌"口径 |
| 真除权日恰逢 THS 未发布 | 除权校验拦截 → skip_exrights，等 THS 恢复（数据不缺、只是晚到） |
| preclose 上调（Last > 前日 close） | 非 except 权特征，视为 raw 异常 → 同样跳过 |
| 请求区间外日期 | 不兜底（尊重 P-11 日期过滤） |
| 台账已有 recovered 记录又再次兜底 | PK upsert 覆盖，fallback_at 刷新，状态机回 `fallback` |
| `--init` 整股重拉 | 同样走 merge → 兜底逻辑天然生效（THS 全历史缺某日的长期缺口也能补） |
| BJ 北交所 | 不经过此 merge，不在范围（其 qfq 问题是另一类，已有官网盘后包路径） |

## 5. 测试计划（backend/test/，monkeypatch 双源 mock）

1. qfq 缺 1 日 + raw 有 + Last 连续 → merged 含该日、raw_fallback=True、source=TDXAPI_RAWFB、台账 fallback
2. Last ≠ 前日 close（模拟除权）→ 不入 merged、台账 skip_exrights
3. Last > 前日 close（preclose 上调）→ 跳过
4. raw 也缺（停牌）→ 无动作
5. 恢复阶段：qfq 已恢复 → upsert（source=TDXAPI）+ recovered_at 置位；qfq 仍缺 → 保持 fallback
6. 当日兜底当日重跑 → 主流程内销账（recover_source='main_sync'）
7. max_stocks_per_run 上限触发 → 整体让位 + 报告告警
8. 既有测试回归：`test/test_review_sync_all.py`、sync/analysis 相关全绿

## 6. 实施顺序与验收标准

| 步骤 | 内容 | 验收 |
|---|---|---|
| D1 | 台账表 DDL + DAO（init SQL 迁移） | 表存在、PK 幂等写入 |
| D2 | merge 层兜底 + source 标记 + 台账写入 | 单测 1-4 过 |
| D3 | 恢复阶段 | 单测 5-6 过 |
| D4 | 配置 + 上限 + 报告键 | 单测 7 过 |
| D5 | 全量回归 + uat 实战验证（monkeypatch 注入 qfq 缺日场景跑一遍真实链路） | 全绿 + 报告含兜底节 |

工作量估计：约 250~350 行（含测试），涉及文件：`sync_manager.py`、`kline_day_pipeline.py`、`dao_kline_day.py`（或新 DAO）、`config.yaml`、init SQL、测试 2 个文件。

## 7. 明确不做的事

- 不逐股调 `/api/ths-factor` 做 q_factor=0 确认（见 §2 修订建议）；
- 不给 `his_kline_day` 加新列（source 列够用）；
- 不处理北交所路径（另一套源，问题性质不同）；
- 不做"有除权时的 raw 强插"（宁可晚到，不可断裂）。
