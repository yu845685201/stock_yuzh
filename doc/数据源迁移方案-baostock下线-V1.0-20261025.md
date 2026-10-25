# 数据源迁移方案——baostock 下线 V1.0（20261025）

## 1. 背景与决策

baostock 为既有禁用清单数据源（接口非常慢），此前仅因存量三类采集未迁移而保留。
2026-10-25 用户拍板**彻底移除**，三类数据全部切换至 tdx-api 本地中间件（认可来源
优先级第 1 位）。四项执行决策：彻底移除 / 股本活跃股全量重导（闸门前置）/ 列表
tdx-api 自推导 / 一次性实施。

## 2. 三类数据换源结果

| 数据 | 原路径（baostock） | 新路径（tdx-api） | 性能 | 验收 |
|---|---|---|---|---|
| 交易日历 → base_trade_calendar | query_trade_dates，分钟级 | `/api/index/all` 沪深指数日K并集（真值口径） | 1.5s 全量 | 与存量双向零差异 |
| 股本 → base_fundamentals_info | query_profit_data 每股每季 1 请求，全量重刷 ≈55h+ | `/api/xdxr` 每股 1 请求全历史事件 | **13 分钟全量** | 闸门 PASS |
| 股票列表 → base_stock_info | query_stock_basic 全量，分钟级 | `/api/codes` 快照 + 日K首日推导 | 0.4s diff | 首次 diff 人工过目后 apply |

### 2.1 交易日历（P1）——指数 K 线真值口径

- 实测发现中间件 `/api/workday/range` 的推断回退对**历史**节假日同样生效
  （`workday.go Is()`：不在 K 线缓存的工作日一律视为交易日，节假日表仅
  2025/2026），历年法定节假日被误判为交易日（~16-24 天/年）。
- 最终口径：≤指数最新 bar 用 sh000001+sz399001 日K日期并集（K线有 bar=真实
  开市）；尾部（K线未覆盖且 ≤2026-12-31）用 workday 节假日表推断；2026 之后
  不落行（等指数K线到达后下次同步修正）。`end_year` 限当年+1。
- **数据质量修正**：识别出 baostock 日历漏标的 1991 年深交所周六开市 45 个
  交易日（K线有真实成交为证），新口径已正确标记为交易日。
- 入口：`sync-trade-calendar --start-year --end-year`（CLI 不变）。

### 2.2 股本（P2）——XDXR 事件序列

- 源：`/api/xdxr`（TDX 0x000F，6 位纯数字代码，单位**万股**，单请求 ~50ms，
  退市股全历史可得，实测 20/20）。
- 行生成（`xdxr_share_manager.xdxr_events_to_rows` 纯函数）：
  - category 1（除权除息）股本字段全 0 弃用；category 2-10 股本变动事件生效；
  - `stat_date = disclosure_date = 事件日`（股本生效日；下游
    `match_fundamentals`/`fetch_total_shares_asof` 均为"≤日期取最近一条"语义，
    事件日比 baostock 财报披露日更精确且无前视）；
  - total/float = 后总股本/盘后流通 ×1e4；未变动一侧延续前值；
  - 基线行 = 首事件前值 @ list_date（缺失回退首事件日；前值缺失回退后值）。
- **一致性校验闸门**（`scripts/verify_xdxr_vs_baostock.py`，投产前置）：
  抽样 300 活跃 + 20 退市，探针=报告期末（stat_date）两侧同瞬间对比。
  首轮（探针=披露日）FAIL 暴露的是合法时点差异（报告期末→披露日之间的
  送转/增发，XDXR 反映事件后真实股本，属换源收益）；修正探针语义后
  **PASS：total 99.87% / float 98.99%**（达标线 99%/98%）。
- **全量重导**：6167 只（type='1' 含退市，一次性完成），5913 只有事件行
  （135,359 行）、254 只无事件（北交所为主）保留原 baostock 行冻结。
  备份：`base_fundamentals_info_bak_20261025` 表 + CSV，可随时回滚。
- **下游验收**：日K join（turnover_rate 链路）5 只样本正常，且 XDXR 已反映
  宁德时代增发后 46.27 亿股（存量 baostock 快照仍为 45.63 亿旧值）；估值
  asof、解禁 ratio、ths_finance 最新总股本全部正常。
- **运行时加固**（迁移中实际发生 TDX 连接断死）：`get_xdxr_strict` 区分
  业务错误与空事件（`TdxApiBusinessError`）；管理器连续 30 次业务失败熔断
  中止（断点保留，处置 `docker restart tdx-stock-web` 后续跑）。
- 日常增量：编排器一键模式将 `shares_xdxr` 置于**独占前置波**（用户确认每天
  全刷可接受，2026-10-25）——必须先于日K同步完成，除权/增发当日生效的股本
  在日K入库读股本映射前落库，事件股当日 turnover_rate 即为精确值；独立 CLI
  `sync-shares-xdxr`（支持 --codes/--no-resume/--dry-run/--manifest-path）；
  断点 `tmp/xdxr_share_manifest.json`。

### 2.3 股票列表（P3）——快照维护

- 源：`/api/codes`（中间件每日 09:00 自 TDX 0x0450 刷新本地 SQLite）。
  **快照仅覆盖沪深**（TDX sh/sz 服务器不含北交所），bj.* 不参与缺席统计
  （否则误判退市），北交所维护沿用 bj_kline_manager 既有链路。
- `stock_list_manager` 维护逻辑（type='1' 范围）：
  - 新股 INSERT，list_date=日K首根日期（IPO 精确；未开盘无K线则留空，
    后续每日运行自动回填——"宁缺毋滥"）；
  - 改名 UPDATE（带 XD/XR/DR/C 临时除权/上市标记不判定；TDX 名称 4 汉字
    截断（旧名以新名为前缀）不判定；全角/空格先归一化）；
  - 退市：连续 5 日缺席快照且最后日K停更>90 天（防长期停牌误杀）→
    'D' + delist_date=末K日期；缺席计数持久化 `tmp/stock_list_absence.json`；
  - 指数/ETF/可转债存量行保留冻结（etf_scale 等独立采集，无新增消费方）。
- 快照过滤：A 股代码段白名单（排除 038 深市优先股，实测快照混入康佳KJP1）；
  302/689（创业板扩容/存托凭证）为正常股票段。
- 入口：`sync-stocks`（默认仅 diff 报告，`--apply` 落库）。首次 diff
  （新股 2/改名 0/退市 0）人工过目后已 apply。

## 3. baostock 下线清单（2026-10-25）

- 删除：`data_sources/baostock_source.py`、`thread_safe_baostock.py`、
  `sync/fundamentals_manager.py`、`concurrent_fundamentals_manager.py`、
  `fundamentals_rebuild_manager.py`、`fundamentals_common.py`、CLI
  `sync-fundamentals`/`rebuild-fundamentals`、`test/manual_check_baostock.py`、
  `examples/sync_fundamentals_example.py`、scripts 下 fundamentals_launchd/
  resume_fundamentals/check_progress/log_progress_hourly 四脚本；
- launchd：`com.yuzh.fundamentals-rebuild`、`com.stockyz.fundamentals-rebuild`
  两个任务 bootout + plist 删除；
- config：`data_sources.baostock` 段与 `sync.fundamentals_rebuild_*` 键移除，
  新增 `sync.shares_xdxr_interval`（0.1s）、`sync.stock_list_delist_*`；
- requirements.txt 移除 baostock 依赖。
- 回滚：git 历史可回溯代码；股本数据回滚 =
  `INSERT INTO base_fundamentals_info SELECT * FROM base_fundamentals_info_bak_20261025`。

## 4. 已知边界与运维注意

- **北交所股本**：XDXR 无数据（254 只，保留 baostock 存量行冻结；新股无行，
  turnover_rate 留空）。
- **未来日历**：workday 节假日表仅到 2026；跨年后（2027-01）需在官方日历
  可得或指数K线覆盖后重跑 `sync-trade-calendar`。
- **中间件单点**：tdx-stock-web 容器承载全部行情/股本/日历源；TDX 上游断连
  时股本采集熔断自保（manifest 断点），`docker restart tdx-stock-web` 恢复。
- **XD/XR 名称**：除息季行情名带临时标记，改名判定自动跳过，除权完成后
  库名保持静态名。

## 5. 验收记录（2026-10-25）

- 日历：与存量双向零差异（≤今日）；国庆/周末/未来官方段全部正确。
- 股本闸门：total 99.87% / float 98.99% / 退市可得 20/20。
- 全量重导：6167 只、0 失败、13 分钟（断点续跑 770s）。
- 下游：turnover_rate join / 估值 asof / 解禁 ratio / ths_finance 最新股本
  四处抽样全部正常。
- 股票列表：首次 diff 人工过目（新股 2、伪改名 0、退市 0）后 apply。
- 测试：全量 pytest **1100 passed, 4 skipped**（含新增 test_xdxr_share.py
  8 例、test_stock_list_manager.py 6 例）。
