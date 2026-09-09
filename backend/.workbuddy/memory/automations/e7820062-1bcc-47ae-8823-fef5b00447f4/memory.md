# 基本面全量重采（F2）续跑 — 执行记录

## 2026-09-08 00:05 — 启动成功

- 启动方式：`Bash(run_in_background=true)` 执行 `bash scripts/resume_fundamentals.sh >> /tmp/fundamentals_supervised.log 2>&1`，PATH 前置 `/opt/anaconda3/bin`。禁止 `nohup ... &`（进程会被回收）。
- 启动前断点：completed=3662 / 5537，failed=0（manifest 时间戳 2026-09-07T16:41:07）。
- 启动后约 3.5 分钟验证：进程存活（pgrep 命中），日志出现「任务全集：5537 只股票，254710 个季度调用；manifest 已完成 3662，跳过」。
- baostock 登录恢复正常（9/7 的「黑名单用户/登录失败」为当日 5 万次额度耗尽所致，跨日自动恢复）。
- 进度已推进：completed 3662 → 3669，failed=0；本轮（9/8）无任何 error/失败日志。
- 实测速率约 2 只/分钟，剩余约 1875 只，估算 10~16 小时（略慢于 8~12 小时的预估，需后续观察）。

## 关键经验

- 用 `pgrep -f fundamentals_rebuild_manager` 查进程，本机 `ps` 被系统策略禁用。
- 脚本自带 40 轮重启 + 300 秒退避的断点续跑机制，异常中断后重启即可，不会丢进度。
- 每日 5 万次 baostock 调用额度在零点刷新；额度耗尽表现为登录报「黑名单用户」。
- 跑完自动触发 G1 验证。
