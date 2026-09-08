#!/bin/bash
# =============================================================
# launchd 托管包装器：让 F2 基本面重采脱离工具会话生命周期
# - 启动前等待已有采集进程退出，杜绝双开竞争 baostock 会话
# - 采集全部完成后自动卸载自身 launchd 任务，避免空转重启
# =============================================================
BACKEND_DIR="/Users/yuzh/develop/ai/claude/claude-code/workspace/stock_yuzh/backend"
PLIST_LABEL="com.yuzh.fundamentals-rebuild"

cd "$BACKEND_DIR" || exit 1
export PATH=/opt/anaconda3/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin

# 等待已有采集进程退出（防双开）
while pgrep -f "fundamentals_rebuild_manager" > /dev/null; do
    echo "$(date '+%F %T') 检测到已有采集进程，等待 30s..." >> /tmp/fundamentals_launchd.log
    sleep 30
done

echo "$(date '+%F %T') === launchd 拉起采集监督循环 ===" >> /tmp/fundamentals_launchd.log
bash scripts/resume_fundamentals.sh >> /tmp/fundamentals_supervised.log 2>&1

# 监督循环退出后判断是否已全量完成，完成则卸载自己
STATUS=$(python3 -c "
import json
from src.config.config_manager import ConfigManager
from src.sync.fundamentals_rebuild_manager import FundamentalsRebuildManager
d = FundamentalsRebuildManager(ConfigManager())._load_manifest()
print('DONE' if (len(d['completed']) >= 5537 and not d['failed']) else 'CONTINUE')
" 2>/dev/null)

if [ "$STATUS" = "DONE" ]; then
    echo "$(date '+%F %T') F2 已完成，卸载 launchd 任务" >> /tmp/fundamentals_launchd.log
    launchctl bootout "gui/$(id -u)/$PLIST_LABEL" 2>/dev/null
fi
