# 日K线采集全量重建 + Phase 3 实施计划（需求 1-7）

分支策略：需求 1-4 在当前分支 `feature/data_coll_simple` 完成；全部跑数验证通过后 commit+push（需求 5）；从该分支拉 `feature/data_coll_opt_260905`（需求 6）实施 Phase 3（需求 7）。

---

## 阶段 0：准备（当前分支）

1. **引入 pytest**：requirements.txt 加 pytest，建 `backend/test/` 目录（现状为零测试）
2. **100 股测试集**：大盘银行/ST/次新/近期除权/长期停牌各若干，写入配置供灰度共用
3. **基线快照**：脚本记录 his_kline_day（1,665 万行）行数/日期分布/amount=0 占比、base_fundamentals_info 覆盖、100 股价格抽样 → `doc/reports/`，作为重建前后对账锚点

## 阶段 1：F 线基本面修复（需求 1-2）

**F1a 表迁移（UAT 执行）**：§5.3 SQL（stat_date 列、DROP NOT NULL、唯一键改 `(ts_code, stat_date)`、idx_bfi_pub）+ his_kline_day 加 `raw_close NUMERIC(20,4)` + 修正 init-V1.sql/数据模型.MD 的 DDL 滞后（4 缺失列、disclosure_date 实为 VARCHAR(8)）。验收：行数守恒 225,444。

**F1b 采集改造**：`baostock_source.get_financial_data` 动态定位 pubDate/statDate，删除 `_get_disclosure_date()`，pubDate 缺失留白；`connection.upsert_fundamentals_data`（:713）列加 stat_date、冲突键改 `(ts_code, stat_date)`。

**F1c 自测**：pytest 单测（pubDate 映射→平安银行 2025Q3=2025-10-25、缺失留白、同日双报告共存）+ 100 股灰度重刷（~10 分钟）+ 方案 §5.5 清单（前视消除、行数守恒、turnover 重算）。

**F2 重刷 runner**：新增 `src/sync/fundamentals_rebuild_manager.py` + CLI `rebuild-fundamentals`。复用 `quarter_calculator`（2007Q1 下限）与 `BaostockSource`（`_execute_query_with_retry` 自带断线重连）；任务全集 = `base_stock_info WHERE type='1'`（含退市 5,537 只）× 上市以来全部报告期 ≈ 24-26 万次；**串行单会话**（baostock 约束）+ manifest 文件断点（表已清空，DB 不能作断点）+ 失败重试 3 次跳过 + 幂等 upsert。

**F2 执行**：TRUNCATE base_fundamentals_info → **后台启动（9-10 小时）** → 期间并行阶段 2 开发 → 完成 G1（§5.5 全量复验）。

**F3 匹配微调**：`_build_fundamentals_map` 排序键 `(disclosure_date, stat_date)`（同日双报告取报告期更新者）、无 pubDate 不入 map + 单测。

## 阶段 2：M 线日K重构（与 F2 后台运行并行）

**M1a 数据源封装**：`TdxApiSource` 增 `get_kline_qfq_full`（/api/kline）、`get_kline_raw_full`（/api/kline-all）、`get_kline_qfq_tail`（/api/kline-history?limit=N）、`get_kline_raw_tail`（/api/kline-all?limit=N）；兼容 `data.List`/`data.list` 两种响应键；厘→元、手→股换算。单测。

**M1b-预备**：`connection.py` 新增 `replace_his_kline_day(ts_code, records)`——DELETE+execute_values 单事务（沿用 `get_connection()` 手动事务范式）；`upsert_his_kline_day` 增加 raw_close、adjust_flag 列。

**M1b 三模式重构**：init=双全量合并（qfq 主表，amount 按 trade_date 对齐、缺失留空）→ normalize → 整股原子替换；range=双全量拉取+内存过滤+区间 upsert；daily=尾部 limit=5+检测+命中转单股全刷。**补 per-stock try/except**（现状单股异常中止全场，必修）。CLI 与签名不变。

**M1c 检测校验**：Last 跨快照检测（阈值 max(0.01, close×0.2%)，config 可配）+ raw 不可变校验（fresh raw_close vs 库存）+ 命中触发全刷。

**M1d 报告扩展**：检测命中数、全刷清单、两源缺失统计、耗时分布（扩展 `_write_kline_day_report`）。

**M1e 自测**：单测（合并/阈值/normalize）+ 100 股三模式试点 + 与基线快照对账 + 构造"模拟 Last 错位"验证自愈 + 幂等重跑验证。

## 阶段 3：清空 + 全量跑数（需求 3-4，仅 UAT）

1. TRUNCATE `base_fundamentals_info`、`his_kline_day`（保留表结构）
2. 后台启动 F2 runner（9-10h）；期间持续监控，问题主动修复（限速调整、断点续跑）
3. F2 完成 → **G1**：§5.5 五项清单全量复验
4. 后台启动 M2：`sync-kline-day --init` 全市场 5,200+ 股、8-12 并发、CSV 双落地保持（约 1-2h）；监控同花顺限流（异常率升高则降 worker）
5. **G3 全局对账**：行数守恒（对基线 1,665 万）、amount 非零率、除权日 preclose 衔接抽验、涨跌幅异常率对比基线 → 收尾报告

## 阶段 4：提交推送（需求 5，当前分支）

- **提交**：backend/src 改动、backend/test 新测试、backend/config/config.yaml、backend/requirements.txt、doc/（两份方案、实施计划、各阶段报告）
- **排除**：backend/test.py、backend/test_baostock.py（既有实验脚本）、data-log.log、dump.rdb
- 中文简短 commit（风格随仓库历史）→ `push origin feature/data_coll_simple`

## 阶段 5：新分支 + Phase 3（需求 6-7）

1. `git checkout -b feature/data_coll_opt_260905` + `push -u origin`
2. **XDXR spike（timebox 1-2 天）**：库内 0x000f 无任何封装（常量在注释块中），请求体格式需参考 pytdx 自行实现消息收发（Frame/Decode/分发 case/Client.GetXdxr 五步）；不可行则 M-B 降级为"周度校准+调用方检测"双防线
3. **M-A**：`/api/kline-recent` handler（`client.GetKlineDay(code,0,N)` 签名已实证）+ server.go 路由注册 + 响应对齐
4. **M-B**：在 `getQfqKlineDay`（server.go:168，三条 qfq 路径共同汇聚点）插入缓存——per-code SQLite（复用已挂载的 data/database/kline 卷，qfq 分表）+ LRU + XDXR 失效（仅事件日≤当天）或降级模式 + `refresh=1` 旁路
5. **M-B3** 周度校准脚本（绕过缓存全市场比对）
6. Docker 多阶段构建重建镜像 + compose 部署（数据卷持久化）
7. **M-C**：stock_yuzh 侧 `use_enhanced_api=true` 双路径 + 100 股对账 → 全市场切换，验证日常增量 ≤8 分钟
8. 完成后新分支 commit+push；tdx-api 仓库改动在其自身 git 单独分支本地提交（是否 push 到 oficcejo/tdx-api 远程由你届时决定）

## 长任务与风险控制

- **F2 约 9-10 小时**：manifest 断点续传，即使会话中断一条命令即可续跑；后台运行期间持续监控主动修复
- 同花顺限流：worker 降档 + 退避重试，必要时分夜
- 中间件缓存对两条调用路径同时生效：故障时以镜像回退为准；`use_enhanced_api` 开关常留
- 全部跑数幂等，失败重跑不产生脏数据

## 明确不在本次范围

PROD 环境执行（G3 通过后另行安排）、M-B2 原始缓存、M-D 聚合接口、1 分钟线模块同类缺陷修复。