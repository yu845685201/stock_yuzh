# tdx-api 中间件优化技术方案 V1.1

> 版本：V1.1 | 日期：2026-09-09 | 分支：`feature/opt_0909`（tdx-api 与 stock_yuzh 统一）
> 上游依据：[日K线数据采集技术方案-V3.0-中间件增强.md](./日K线数据采集技术方案-V3.0-中间件增强.md) §3、§7（M-A / M-B / M-B3 / M-C）
> 本文档是 V3.0 中间件增强部分的**施工设计**：所有插入点均按 2026-09-09 当前代码（feature/opt_0909，含 upstream 3 个新提交）重新核对，以**函数名**为锚，行号仅供快速定位、随代码演进失效。
> 已移除范围：复权因子采集、PROD 全量跑数（2026-09-09 用户决策）。
>
> **V1.1 修订（2026-09-09）：确立"只新增、不修改"设计红线**
> - **接口只新增**：既有接口（/api/kline、/api/kline-history、/api/kline-all 系列）的路径、参数、响应结构、行为**一律不动**；缓存能力以**新接口** `/api/kline-qfq` 提供
> - **代码近零修改**：不修改任何既有代码行（不删不改）；新逻辑全部落新文件；对既有文件仅剩 3 处"纯新增行"（const.go 常量 ×1、client.go 分发 case ×1、server.go 路由注册 ×2）
> - `getQfqKlineDay` **完全不动**（含签名），作为新缓存模块的回源函数被调用
> - 唯一例外：`docker-compose.yml` 卷路径修复——那是修既有部署 bug（不修重建必丢数据挂载），与特性设计无关

---

## 1. 背景与目标

V3.0 已论证：中间件对所有日K接口的实现是"服务端向同花顺/通达信**全量拉取**后再截取"，调用方即使只要最近 2 条也要付全量代价（同花顺前复权 2-8s/股、通达信原始 0.5-5s/股），这是日常增量 40-90 分钟的唯一根源。

本方案落地 V3.0 的中间件增强，目标：

| 指标 | 现状 | 目标 |
|------|------|------|
| 日常增量（全市场 5,200+ 股） | 40-90 分钟 | **3-5 分钟**，除权高峰日 ≤8 分钟 |
| 原始日K尾部查询 | 0.5-5s/股（11 批拼接） | **<150ms/股**（单协议请求） |
| 前复权日K查询 | 2-8s/股（THS 全量） | 缓存命中 **10-50ms/股** |
| 缓存失真窗口 | — | XDXR 实时失效 + 周度校准兜底，上限 1 周 |

**任务清单**（对应 V3.0 §7）：

| # | 任务 | 仓库 | 预估 |
|---|------|------|------|
| T1 | XDXR 协议 spike（除权除息事件查询） | tdx-api | 1-2 天（timebox） |
| T2 | M-A `/api/kline-recent` 原始尾部单请求接口 | tdx-api | 0.5 天 |
| T3 | M-B 前复权序列缓存 + XDXR 失效 + `refresh` 旁路 | tdx-api | 2-3 天 |
| T4 | M-B3 周度校准脚本 | tdx-api | 0.5-1 天 |
| T5 | 部署（含 docker-compose 卷路径修复） | tdx-api | 0.5 天 |
| T6 | M-C 采集侧 `use_enhanced_api` 开关切换 + 灰度对账 | stock_yuzh | 1-2 天 |

依赖关系：T1 → T3；T2 独立可先行；T3 → T4 → T5 → T6。**T1 是唯一技术不确定点，先做。**

---

## 2. 现状盘点（2026-09-09 代码核实）

### 2.1 模块结构

- 仓库根 = Go module `github.com/injoyai/tdx`（协议库本体，fork 自 injoyai/tdx，协议改动都在本地，不受上游库升级影响）
- `web/` 是独立 module（`module web`，`replace github.com/injoyai/tdx => ../`），HTTP 服务层
- Docker 多阶段构建，容器 `tdx-stock-web`（当前 Up 8 周，健康）

### 2.2 前复权数据链路（缓存改造的核心现场）

`getQfqKlineDay`（`web/server.go`，函数名锚点）是唯一汇聚点，当前逻辑：

1. `extend.GetTHSDayKline(code, THS_QFQ)` —— 同花顺 `all.js` **全量**前复权（2-8s）
2. `client.GetKlineDayAll(code)` —— TDX **全量**原始（11 批拼接），仅为补 `Amount`（THS 不返回成交额）
3. 合并、按上一条 close 填 `Last`，返回 `protocol.KlineResp`

调用方共 5 处：`handleGetKline`（day/week/month 三分支，`web/server.go`）、`handleGetKlineHistory`（day/week/month 三分支，`web/server_api_extended.go`）、另有两处 `/api/kline-all/ths` 相关 handler（`server_api_extended.go` 内 `getQfqKlineDay` 调用点）。week/month 由日K经 `convertToWeekKline`/`convertToMonthKline` 转换——**日K缓存落地后这两个周期自动受益，无需单独改造**。

### 2.3 协议层（XDXR 要接入的现场）

- `protocol/frame.go`：`Frame{MsgID, Control, Type, Data}` + `Bytes()` 组帧；`Response` + `Decode()` 解帧（含 zlib 解压）——**帧层完备，新增消息类型零改动**
- `protocol/const.go`：生效常量只到 `TypeKline=0x052D`；`KMSG_XDXRINFO=0x000f` 存在于**注释块**（const.go 末尾），需提升为生效常量 `TypeXdxr`
- `client.go` `handlerDealMessage`：按 `f.Type` 的 switch 分发，各类型调用对应 `Model.Decode(f.Data, cache)`；`SendFrame(f, cache...)` 支持随请求缓存上下文（K 线用它传 `KlineCache{Type, Kind}`）
- `protocol/model_kline.go` 是新增协议的**标准范式**：`XxxReq.Bytes()` / `Model.Frame()` 组请求、`Model.Decode()` 解析、复用 `unit.go` 的 `DecodeCode`（code→exchange+number）、`getVolume`（成交量解码）、`Control01`

### 2.4 本地缓存层（M-B 复用对象）

`extend/pull-kline.go` 已实现 per-code SQLite 方案：`data/database/kline/{code}.db`，xorm 引擎、`SetMaxOpenConns(1)`、表结构 `Kline{Code,Date,Open,High,Low,Close,Volume,Amount,InDate}`、按 `tableName` 分表（`DayKline` 等）、增量拉取（末条日期锚定 + `Date >= ?` 删除重插）。**当前库内基本为空**（仅 `sh600000.db`），`/api/tasks/pull-kline` 未全市场跑过——缓存需随 M-B 上线逐步建立（懒加载）或批量预热。

### 2.5 部署层（一个必须先修的坑）

`docker-compose.yml` 卷源路径仍为旧位置 `.../workspace/test/tdx-api/web/data/database`，该目录已随仓库迁移至 `stock_yuzh_base/tdx-api`。**当前容器靠 bind mount 的 inode 存活**（实测容器内 `/app/data/database` 与新位置内容一致），但一旦 `docker compose down` 或重建，挂载源将指向不存在的路径（docker 会自动创建空目录），数据卷实际丢失。→ T5 第一步必须修正卷路径。

---

## 3. 总体设计

```
┌─ stock_yuzh 采集侧（M-C 切换后，use_enhanced_api=true）──────┐
│ 日常增量：qfq 尾部 ← /api/kline-qfq（新接口，缓存）          │
│           amount/raw ← /api/kline-recent（新接口，单请求）   │
│ 开关=false 时：仍走 /api/kline-history + /api/kline-all     │
│ （旧路径，一行代码行为都不变）                               │
└──────────────┬─────────────────────────────────────────────┘
               ▼
┌─ tdx-api 中间件 ────────────────────────────────────────────┐
│ 【新】/api/kline-qfq（缓存优先，V1.1 起唯一缓存入口）        │
│   ├─ 缓存命中 → XDXR 校验（T1）                             │
│   │    ├─ 锚定日以来无 category=1 除权事件 → 尾部追加新 bar  │
│   │    │   （新 bar 价格=原始值，无除权时 qfq≡raw；          │
│   │    │     amount 取原始域真实值）→ 服务                   │
│   │    └─ 有除权事件（事件日≤当日）→ 缓存失效，回源           │
│   ├─ 缓存未命中 / refresh=1 → 回源                           │
│   └─ 回源：调用既有 getQfqKlineDay（原样复用，不改一行）     │
│            → 重建缓存                                       │
│ 【旧】/api/kline、/api/kline-history：完全不变，             │
│        语义=每次全量回源，兼作周度校准的权威对照源           │
│ 三道防线：XDXR 失效（主）→ 调用方跨快照检测（辅，refresh 回源）│
│           → 周度校准（兜底，T4）                             │
└─────────────────────────────────────────────────────────────┘
```

设计要点：

1. **接口只新增（V1.1 红线）**：既有接口零变更——路径、参数、响应结构、行为均不动；缓存能力只经新接口 `/api/kline-qfq`、`/api/kline-recent` 暴露。任何既有调用方（含 web 前端页面）完全不受影响，也不需要感知 `refresh` 参数。
2. **代码近零修改（V1.1 红线）**：`getQfqKlineDay` 及 5 个既有调用点、`convertToWeekKline`/`convertToMonthKline`、协议帧层**一律不改**；新逻辑全部落新文件；对既有文件仅 3 处纯新增行（见 §4 各节）。
3. **两域分表**：qfq 缓存与原始 K 线分表存储（V3.0 硬性要求，防两域混写）。
4. **失效判定保守化**：仅 `category=1`（除权除息）且**事件日 ≤ 当日**触发失效；公告在前、除权在后的"未来事件"不提前失效（避免公告日至除权日间每天无谓全量重拉）。
5. **降级路径**：T1 失败（协议实现不可行）→ 退回"调用方检测 + 周度校准"双防线，失真上限一周，M-A/T2 成果保留。
6. **代价与取舍（明示）**：旧接口调用方（如 web 前端）不享受提速；新旧接口长期并存（旧=权威回源，新=缓存加速），由 stock_yuzh 开关选择路径——这是"只新增"换来的兼容性，可接受。

---

## 4. 详细设计

### 4.1 T1 XDXR 协议 spike（timebox 1-2 天）

**目标**：实现并实证 TDX 协议 `0x000f`（除权除息信息）查询，产出 `Client.GetXdxr(code)`。

**协议规格**（pytdx 逆向，详见 [notes/xdxr_protocol_reference.md](./notes/xdxr_protocol_reference.md)）：

- 请求：`Frame{Control: Control01, Type: 0x000f, Data: {0x01, 0x00, market(1B), code(6B ASCII)}}`
  （对照 pytdx 固定头 `0c1f187600010b000b000f000100` + market + code：`0f00`=功能号，`0100`+market+code=9B 数据域，与帧长 `0b00`=11 吻合）
- 响应（`Decode` 解帧后的 `Data`）：跳 9 字节 → `num uint16 LE` → 每条记录：`market+code(7B)` → 跳 1B → **日期 4B（uint32 LE，YYYYMMDD 整型）** → `category(1B)` → 16B 负载
  - ⚠️ 疑点：`xdxr_protocol_reference.md` 记为"日期时间(9 字节模式)"，pytdx 源码 `get_xdxr_info.py` 实际为 4 字节整型日期（`date_raw // 10000` 取年）。**spike 第一个验证点**，以实测为准并回写笔记
- 负载按 category：`1=除权除息` → `fenhong/peigujia/songzhuangu/peigu`（`<ffff`）；`11/12=扩缩股/缩股` → suogu；`13/14=权证` → xingquanjia/fenshu；`2-10` → 股本四元组（`<IIII`，经 `getVolume` 解码）

**实施项**：

| # | 改动 | 位置（函数名锚点） |
|---|------|------|
| 1 | 新增 `TypeXdxr = 0x000f` 常量（**纯新增行**，可直接定义在新文件 model_xdxr.go 内，const.go 一行都不碰） | `protocol/model_xdxr.go`（首选）或 `protocol/const.go` |
| 2 | 新增 `protocol/model_xdxr.go`（新文件）：`XdxrEvent{Date, Category, Fenhong, Peigujia, Songzhuangu, Peigu, ...}`、`XdxrResp`、`xdxr.Frame(code)`（复用 `DecodeCode`）、`xdxr.Decode(bs)` | 参照 `model_kline.go` 范式 |
| 3 | 分发 switch 新增 `case protocol.TypeXdxr`（**纯新增 1 行**，既有 case 不动） | `client.go` `handlerDealMessage` |
| 4 | 新增 `Client.GetXdxr(code string) (*protocol.XdxrResp, error)`（新增函数，不改既有函数） | `client.go`（参照 `GetKlineDay` 的 SendFrame 调用方式，XDXR 无需 cache 参数） |

**验证清单**（全部通过才算 spike 成功）：

1. `sz.000001`、`sh.600519`、`sz.300750` 三股：Go 实现输出与 pytdx `get_xdxr_info` 逐条对照一致（日期、category、分红/送转数值）
2. 抽 2 只近期有分红的股票，category=1 事件的 `fenhong`（每股分红）与公开分红公告一致
3. 无除权事件的股票（如次新股）返回 `num=0` 不报错
4. 单次请求耗时 <200ms（预期 ~50ms）
5. 事件日期边界：确认返回的事件日期 = 除权除息日（非公告日）

**产出**：spike 结论回写 `doc/notes/xdxr_protocol_reference.md`（修正日期字节数等实测差异）；Go 代码 + 单测（`protocol/frame_test.go` 同款）。

**降级**：若协议实现受阻（响应解析对不上/服务器拒绝），timebox 到点即停，M-B 改为"周度校准 + 调用方检测"双防线设计，本方案其余部分不受影响。

### 4.2 T2 M-A `/api/kline-recent`（0.5 天，可与 T1 并行）

```
GET /api/kline-recent?code=000001&type=day&limit=N
```

- **实现**：新 handler `handleGetKlineRecent`（`web/server_api_extended.go`），核心一行 `client.GetKlineDay(code, 0, N)`——单次协议请求取最近 N 条**原始**（不复权）K 线，不走 800 条批拼接。`GetKlineDay(code, start, count)` 签名已实证（`client.go`，分钟线 handler 在用同款调用）
- **参数**：`code`（6 位，必传）、`type`（首期仅支持 `day`，其他周期 400）、`limit`（默认 2，最大 800）
- **响应**：复用 `successResponse(w, protocol.KlineResp)` 结构，含真实 `Amount`（原始域），响应体 <1KB
- **路由注册**：`web/server.go` 路由注册段（`http.HandleFunc("/api/kline-history", ...)` 附近）加一行
- **验收**：`curl` 实测 sz.000001 limit=2 响应 <150ms，数据与 `/api/kline-all` 尾部 2 条逐字段一致（open/high/low/close/volume/amount）
- 此接口独立有用，先行上线即可降低原始域采集成本（不等 T3）

### 4.3 T3 M-B 前复权缓存 + XDXR 失效（2-3 天，核心）

**新增接口（V1.1：缓存能力的唯一入口，既有接口不动）**：

```
GET /api/kline-qfq?code=000001&type=day|week|month&limit=N&refresh=1
```

- `type=day` 为缓存本体；`week`/`month` 由缓存日序列经既有 `convertToWeekKline`/`convertToMonthKline`（纯函数，直接复用）转换
- `refresh=1`：绕过缓存回源并重建缓存（供调用方检测命中后重拉、周度校准修复使用）
- 响应结构与 `/api/kline-history` 同构（`protocol.KlineResp`），调用方解析代码可复用

**代码组织（新逻辑全部在新文件）**：

| 文件 | 内容 | 对既有代码触碰 |
|------|------|----------------|
| `web/qfq_cache.go`（**新**） | 缓存读写（`DayKlineQfq`/`QfqMeta`/`XdxrEvent` 三表）、`hasEffectiveXdxr` 失效判定、尾部追加、`handleGetKlineQfq` handler、per-code 互斥锁 | 无 |
| `web/server.go` | 路由注册 `http.HandleFunc("/api/kline-qfq", handleGetKlineQfq)` | **纯新增 1 行** |
| `getQfqKlineDay`（既有） | **一行不改**，作为回源函数被 `qfq_cache.go` 调用 | 无 |

**存储设计**（复用 per-code SQLite 方案，`extend/pull-kline.go` 范式）：

| 表 | 位置 | 内容 |
|----|------|------|
| `DayKlineQfq` | `data/database/kline/{code}.db` | 前复权全量序列（与原始域 `DayKline` **分表**） |
| `QfqMeta` | 同上 | 每股一行：`Code`、`AnchorDate`（缓存锚定日=最后一条 bar 日期）、`BuiltAt`（回源时间）、`LastXdxrCheck`、`CacheVersion`（缓存逻辑版本，见 §8.3） |
| `XdxrEvent` | 同上 | 该股除权事件增量缓存：`Date`、`Category`、负载字段、`InDate`；每次 `GetXdxr` 后 upsert |

**服务流程**（新函数，伪代码）：

```go
// web/qfq_cache.go（新文件）
func getQfqKlineDayCached(code string, refresh bool) (*protocol.KlineResp, error) {
    if !refresh {
        if cached, meta, ok := loadQfqCache(code); ok {        // DayKlineQfq + QfqMeta
            events := xdxrSince(code, meta.AnchorDate)         // Client.GetXdxr + XdxrEvent 增量缓存
            if !hasEffectiveXdxr(events, today) {              // 无 category=1 且 事件日≤当日
                tail := client.GetKlineDay(code, 0, tailN)     // 原始尾部（T2 同款调用）
                newBars := barsAfter(tail, meta.AnchorDate)
                if len(newBars) > 0 {
                    appendQfqBars(code, newBars)               // 无除权时 qfq≡raw；amount=原始真实值
                    meta.AnchorDate = newBars 末条日期
                }
                return buildResp(cached + newBars), nil        // Last 按上一条 close 填
            }
            // 有除权事件 → 落入回源
        }
    }
    resp, err := getQfqKlineDay(code)                          // 既有函数原样调用：THS 全量 + TDX amount 合并
    if err != nil {
        return nil, err
    }
    rebuildQfqCache(code, resp)                                // 覆盖写 DayKlineQfq + 重锚 QfqMeta
    return resp, nil
}
```

**关键规则**：

1. **追加 bar 的价格口径**：XDXR 确认无除权 ⇒ 复权因子恒 1 ⇒ 新 bar 直接取原始值（V3.0 实测依据：近期因子恒为 1）；`Amount` 取原始域真实值（与现行"TDX 补 amount"口径一致）；`Last` = 缓存末条 close
2. **缓存与调用方检测的协同**：stock_yuzh 侧的跨快照除权检测（`_detect_kline_day_refetch`）命中后，全量重拉**必须带 `refresh=1`**（走新接口），否则从失效缓存取回同样错位的数据（V3.0 已强调，T6 落地时验证）
3. **并发安全**：per-code 互斥锁（`map[code]*sync.Mutex` + 清理）保护"读缓存→XDXR→追加"临界区；xorm 沿用 `SetMaxOpenConns(1)`；SQLite 单文件写串行
4. **缓存建立**：懒加载——每股首次访问未命中即回源建缓存；另提供批量预热（复用 `/api/tasks/pull-kline` 任务框架加一个 qfq 预热任务类型，或夜间脚本逐股调 `/api/kline-qfq?refresh=1`），上线首夜执行，避免首个交易日白天全市场回源
5. **内存**：磁盘缓存无 3GB 压力（V3.0 估算），热点内存层不做（M-D 按需）
6. **ETF 等特殊品种**：`adjustQuotePrice` 的三位小数修正只作用于 quote；K 线 `Price`（厘）口径不变，缓存不加品种特判

**单测/集成测试**：

- 追加逻辑：构造缓存 + mock 尾部，断言新 bar 字段（价格=原始、amount=真实、Last 衔接）
- 失效判定：`hasEffectiveXdxr` 表驱动测试（事件日=当日/昨日/明日、category=1/5、多事件混合）
- 集成：sz.000001 建缓存 → 次日增量 → 与旧接口 `/api/kline-history`（权威回源）逐字段一致

### 4.4 T4 M-B3 周度校准脚本（0.5-1 天）

- **位置**：`tdx-api/scripts/weekly_qfq_calibrate.py`（该目录已有 `run_api_checks.py` 同款 Python 脚本范式）
- **流程**（V1.1 后更简单：旧接口天然是权威对照源，无需绕路）：遍历全市场代码（`/api/codes`）→ ① `GET /api/kline-qfq?code=X`（新接口，吃缓存）→ ② `GET /api/kline?code=X`（**旧接口，每次全量回源，权威值**）→ 比对①② → 发现不一致 → ③ `GET /api/kline-qfq?code=X&refresh=1` 强制回源重建缓存（自动修正）
- **输出**：Markdown 报告（不一致股票、差异行数、首尾差异日期、修正动作），供人工复核告警
- **调度**：周末窗口 cron（预计 40-90 分钟）；与 stock_yuzh 的日常增量错开

### 4.5 T5 部署（0.5 天）

1. **修 `docker-compose.yml` 卷路径**：`.../workspace/test/tdx-api/web/data/database` → `.../workspace/stock_yuzh_base/tdx-api/web/data/database`（当前容器靠 inode 存活，down 即丢挂载，**必须先修再重建**）
2. `docker compose build && docker compose up -d`（多阶段构建；`web/` 独立 module + `replace ../`，库源码改动会被打入镜像）
3. 验收：`/api/health` 健康；`/api/kline-recent` 实测；抽 3 股 `/api/kline` 与旧镜像结果一致；容器内 `/app/data/database` 挂载正确（写文件宿主机可见）
4. 回滚预案：镜像 tag 留存上一版，`docker compose down && 旧镜像 up` 即回退（缓存部署对两条采集路径同时生效，代码级回退以镜像为准）

### 4.6 T6 M-C 采集侧切换（stock_yuzh，1-2 天）

- `config.yaml` 已有设计位 `kline_day.use_enhanced_api`（V3.0 §4），`TdxApiSource` 四方法（`get_kline_qfq_full/raw_full/qfq_tail/raw_tail`，M1a 已交付）中 tail 方法切换到新路径：qfq 尾部走 **`/api/kline-qfq?limit=N`**，raw 尾部走 `/api/kline-recent?limit=N`；调用方检测命中后的重拉带 `refresh=1`
- **灰度**：100 股双路径对账（同一批股票 enhanced 与 legacy 各采一次，逐字段比对）→ 全市场
- **验收**：全市场日常增量墙钟 ≤8 分钟（目标 3-5 分钟）；双路径对账零差异（除权股允许 legacy 路径因无缓存而数值一致但耗时更高）
- 开关默认 `false` 上线，验证通过后翻 `true`；`false` 随时回退 V2.0 直连路径（旧接口行为未变，回退完全等价于现状）

---

## 5. 接口契约

### 5.1 新增 `GET /api/kline-recent`

| 项 | 内容 |
|----|------|
| 参数 | `code`（6 位，必传）、`type=day`（首期唯一）、`limit`（默认 2，≤800） |
| 响应 | 与 `/api/kline-history` 同构：`{code:0, data:{count, list:[{time,open,high,low,close,last,volume,amount}]}}` |
| 语义 | **原始不复权**尾部 N 条，`amount` 为真实成交额（厘） |
| 性能 | <150ms/次 |
| 错误 | code 空/非法 → 400；TDX 源异常 → 错误提示（不降级 THS，与现有约定一致） |

### 5.2 新增 `GET /api/kline-qfq`（T3 交付，缓存能力唯一入口）

| 项 | 内容 |
|----|------|
| 参数 | `code`（6 位，必传）、`type=day\|week\|month`（默认 day）、`limit`（可选，截尾部）、`refresh=1`（可选，绕过缓存回源并重建） |
| 响应 | 与 `/api/kline-history` 同构（`protocol.KlineResp`） |
| 语义 | 前复权日/周/月 K 线，**缓存优先**；week/month 由缓存日序列转换 |
| 性能 | 缓存命中 10-50ms；未命中/refresh 回源 2-8s（THS 全量，同旧接口） |

### 5.3 既有接口：零变更（V1.1 红线）

| 接口 | 状态 |
|------|------|
| `/api/kline` | **完全不变**（每次全量回源；兼作周度校准权威对照源） |
| `/api/kline-history` | **完全不变** |
| `/api/kline-all`、`/api/kline-all/tdx`、`/api/kline-all/ths` | **完全不变** |
| 其余全部既有接口 | **完全不变** |

---

## 6. 测试与验收总表

| 阶段 | 验收项 | 标准 |
|------|--------|------|
| T1 | pytdx 对照（3 股） | 事件逐条一致 |
| T2 | kline-recent 性能 + 正确性 | <150ms；与 kline-all 尾部一致 |
| T3 | 缓存命中正确性 | 新接口缓存增量服务结果 vs 旧接口（`/api/kline`，权威回源）逐字段一致（抽 20 股） |
| T3 | 失效正确性 | 构造/实测除权股，除权日触发回源且历史序列重算 |
| T4 | 校准脚本 | 全市场跑通出报告；不一致项已自动修正 |
| T5 | 部署 | 健康检查 + 卷挂载正确 + 3 股回归 |
| T6 | 双路径对账 | 100 股零差异 → 全市场增量 ≤8 分钟 |

---

## 7. 风险与应对

| 风险 | 等级 | 应对 |
|------|------|------|
| XDXR 协议实现受阻（唯一技术不确定点） | 高 | timebox 1-2 天；降级为"调用方检测 + 周度校准"双防线，失真上限 1 周 |
| 缓存与同花顺源漂移（非除权的数据修订） | 中 | 周度校准兜底 + 调用方 raw 不可变性校验 |
| 除权高峰日回源集中（200-300 股 × 2-8s） | 中 | 天然分散在当日增量内，12 并发下 ≤8 分钟可达成；保持现有重试退避 |
| THS 源限流/封禁 | 中 | 缓存命中后 THS 调用量降至原来的 ~1%（仅除权股回源），风险反降 |
| docker-compose 卷路径失效导致数据丢失 | **高（已确认存在）** | T5 第一步修复，重建前必查 |
| 缓存并发写竞争（同 code 并发请求） | 低 | per-code 互斥锁 + SQLite 单连接 |
| upstream 持续合入新提交 | 低 | 本方案全部锚点按函数名定位；"只新增"红线使冲突面最小化（仅 3 处新增行）；T3 开工前再 fetch 一次 upstream 评估 |
| 新旧接口并存的一致性维护 | 低 | 旧接口语义固定=全量回源，恰好是缓存的权威对照源；两者差异即校准信号，设计上自洽 |

---

## 8. 上游同步（upstream merge）影响分析与保障机制

tdx-api 会持续从 `upstream/main`（oficcejo/tdx-api）合入更新。本节回答：上游更新会不会影响新接口？上游的修复/优化会不会在新接口中失效？

### 8.1 合并冲突面：极小且全部显式

| 我们的改动 | 冲突可能性 | 处置 |
|------------|------------|------|
| 新文件（model_xdxr.go、qfq_cache.go、校准脚本） | 零冲突 | — |
| `client.go` 分发 switch +1 case | 上游同改 switch 时报冲突 | 两个 case 并存即可，机械解决 |
| 路由注册 +2 行 | 上游同段加路由时报冲突 | 并存即可，机械解决 |
| `TypeXdxr` 常量 | **放新文件 model_xdxr.go**，`const.go` 一行不碰 | 零冲突 |

冲突全部经 git 显式暴露，不存在"合并后逻辑悄悄丢失"的路径。

### 8.2 上游修复/优化：**自动继承**的部分（调用关系红利）

新接口对既有代码是**调用**而非复制，上游在以下位置的修复自动对新接口生效：

| 上游变更位置 | 受益的新逻辑 |
|--------------|--------------|
| 协议层（`frame.go`、`model_kline.go` Decode、成交量/小数修复，如已合入的"Amount 为 0""ETF 三位小数"） | `/api/kline-recent`、缓存尾部追加（直接调 `GetKlineDay`） |
| `getQfqKlineDay`、`extend.GetTHSDayKline`（THS 解析、amount 合并） | `/api/kline-qfq` 回源路径（原样调用） |
| `convertToWeekKline`/`convertToMonthKline` | `/api/kline-qfq` 的 week/month 分支（纯函数复用） |

**另两类上游变更以编译错误显式暴露（好事，不会静默）**：`getQfqKlineDay` 签名变化 → 编译失败；上游自带 XDXR 实现 → 常量/case 重复定义 → 编译失败，取其一即可。

### 8.3 不会自动生效、需机制保障的两点

| # | 风险 | 机制 |
|---|------|------|
| 1 | **"旧接口=权威对照源"假设被破坏**：若上游给 `/api/kline` 也加缓存或改语义，周度校准退化为"缓存比缓存"，**静默失灵** | 列入 §8.4 合并后必查项：确认旧接口行为未变（响应耗时特征 + 与 THS 源抽样比对） |
| 2 | **存量缓存数据的版本漂移**：上游修改价格解析/复权相关逻辑后，磁盘旧缓存按老逻辑构建，不会自动刷新 | `QfqMeta.CacheVersion` 字段：相关逻辑变更时 bump，版本不符 → 首次访问自动全量重建；周度校准兜底（失真上限 1 周） |

### 8.4 保障机制（随 T3 一并交付）

1. **关注清单**：merge upstream 前 `git diff main...upstream/main --stat`，凡涉及 `protocol/`、`client.go`、`extend/spider-ths.go`、`web/server.go`、`web/server_api_extended.go` 的提交逐条过一遍（对照 §8.2/§8.3 分类处置）
2. **合并后回归**：`scripts/run_api_checks.py` + 新接口验收集（kline-recent <150ms、kline-qfq 缓存命中 vs 旧接口一致、XDXR 三股对照）全绿才算合并完成
3. **缓存版本号**：`CacheVersion` 常量定义在 `qfq_cache.go`，§8.3-2 类变更时 +1
4. **周度校准**作为一切漏检的最终兜底

## 9. 排期建议

| 日序 | 内容 |
|------|------|
| D1-D2 | T1 XDXR spike（出结论即回写协议笔记） |
| D2 | T2 M-A（与 T1 并行，当天可上线） |
| D3-D5 | T3 M-B 缓存主体 + 单测/集成测试 |
| D6 | T4 校准脚本 + T5 部署（含卷路径修复）+ 首夜批量预热 |
| D7-D8 | T6 采集侧切换 + 100 股对账 + 全市场灰度 + ≤8 分钟验收 |

完成后跟进项（不在本期）：M-B2 原始序列缓存（区间模式提速至分钟级）、M-D 聚合接口/内存热层、XDXR category=5/6 作为 float_share 刷新信号（V3.0 §5.6 增强项）。
