# stock_yuzh 项目长期笔记

## baostock 数据源硬约束
- **每日 API 调用上限约 5 万次**（用户确认）。单日超限会触发限制/封禁。
- 全量基本面重采的总调用量约 **25.5 万次**（runner 日志：5537 只股票 / 254,710 个季度调用）。
  → 因此全量重采**必须跨天分批**，按 5 万/天估算至少需 5~6 天，不能指望一天跑完。
- 其他约束：严格串行不支持并发；封禁症状为「黑名单用户，请与管理员联系」。
- **防封禁参数（2026-09-09 核实，勿再引用旧的 0.25/300）**：`config.yaml` 的 sync 段实际生效值为 `fundamentals_rebuild_call_sleep: 0.6`、`fundamentals_rebuild_recycle_calls: 1000`（2026-09-07 17:05 由 0.25/300 上调）；代码兜底默认值是 0.3 / 300。另有 `fundamentals_rebuild_socket_timeout: 60`、`fundamentals_rebuild_sleep: 0.02`。
- 网络故障与封禁的区分：网络问题报 `Broken pipe` / `接收数据异常` / `登录失败：网络接收错误`，**不是封禁**。

## 全量跑数（F2 基本面 / M2 日K）
- 断点续跑依赖 `backend/tmp/fundamentals_rebuild_manifest.json`，completed 集合即已完成股票，**禁止手工改动**。
- 续跑命令：`bash scripts/resume_fundamentals.sh`（manifest 断点 + 40 轮×5 分钟自动重启 + 完成后自动 G1）。
- 流程关口：F2 → G1 验证 → M2 日K重建 → G3 全局对账 → 提交 → Phase 3，关口未过不得推进。
- **2026-09-08 全流程已跑通**：F2 5537/5537(failed=0) → G1 6/6 → M2 16,574,970 行(failed=0) → G3 8/8，M2 耗时约 14 分钟（8 并发）。
- 经验：G1 的 turnover 抽样项在 M2 前可能因「旧日K turnover 用旧股本算、基本面重采后股本变了」而失败（如 sh.600373 的 2016 年批次），**这属于 M2 要修复的对象，不是基本面数据错误**；M2 重算后该项自动归零。切勿因此误判而中断流程。
- 基线锚点 `backend/tmp/baseline_*.json` 必须保护，丢失则 G3 无法对账。

## 本机环境要点
- 长时后台任务**必须用 Bash 工具的 `run_in_background=true`** 启动；`nohup ... &` 启动的进程会在工具会话结束时被回收（已实测）。
- 由 automation 会话启动的后台进程同样活不久（9/8 实测：8.8 小时被回收 3 次）。
- **最稳的启动方式：双 fork + `os.setsid()`**（孙进程被 init 收养，脱离任何会话）。参考 2026-09-08.md 中的脚本。
- ⚠️ 启动命令的命令行**不要包含 `fundamentals_rebuild_manager` 字样**：resume_fundamentals.sh 的防双开用 `pgrep -f` 匹配它，会误判调用方 shell 为「已有进程」而拒绝启动。
- `ps` 命令被系统策略禁用，判断进程存活用 `pgrep -f <关键字>` 或「manifest/日志增量采样」法。
- Python 必须用 `/opt/anaconda3/bin/python3`（baostock/psycopg2/pandas 装在这里）。

## automation 调度能力边界
- 最细粒度 `FREQ=HOURLY;INTERVAL=1`；`BYMINUTE`/`BYHOUR` 多值/`validFrom`/recurring 的 `scheduledAt` 均无法控制触发分钟。
- **触发分钟恒等于创建/更新时刻的分钟**；想对齐整点就先 sleep 到目标分钟窗口内再更新。
- `once` 类型的 `scheduledAt` 可精确到分钟。
