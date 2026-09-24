# 自动化执行记录：补齐 2026-09-23 日K线（his_kline_day / uat）

## 2026-09-24 08:53 首次执行 —— 已补齐（成功）

**结论**：源端已就绪，执行了补齐，09-23 最终 **5186 条**（09-21=5188、09-22=5187），已补齐。

**关键步骤与结果**：
- 抽样探测 30 只（max=20260922 且 L）：命中率 **30/30 = 100%** → 执行采集。
- 采集 4 轮：全量 5537 只（7m38s，写库 14510 条）→ 241 只定向（661 条）→ 21 只（7 条）→ 7 只（15 条）。
- 09-23 条数：3480 → 5186。
- 剩余落后 14 只，逐只核验源端后确认为**长期停牌股**（源端 qfq 直接跳过 09-23），属数据源侧正常缺失，非采集问题。

**当次遇到的两个意外（已固化进 skill `stock-kline-day-backfill`）**：
1. **中间件整体宕机**：首次采集 5537 只全部 `Connection refused`，白跑 10 分钟。根因是 **Docker Desktop 未运行**，容器 `tdx-stock-web` 停止。修复：`open -a Docker` → 容器自动拉起 → 等 `health=healthy`（daemon ~6s、health ~30s）。
   - 注意：抽样探测时 8080 还是正常的，约 1 分钟后开跑就已宕机。**采集启动后必须立刻 `grep -c "Connection refused"` 扫日志**。
2. **qfq 接口日期字段是 `Time`**（`2026-09-23T15:00:00+08:00`），不是 `date`/`trade_date`。首版探测脚本按 `date` 解析，得到"有记录但日期全空"，一度误判命中率 0%。

**环境要点**（下次沿用）：
- 解释器 `/Users/yuzh/.pyenv/versions/3.12.4/bin/python3`；`PYTHONPATH=/Users/yuzh/.workbuddy/tmp/stock_deps`；`NO_PROXY/no_proxy=127.0.0.1,localhost`。
- 命令一律加 `--no-csv`；未改 config.yaml；未做 CSV 导出。

**未决**：无。09-23 已补齐，无需再重试。
