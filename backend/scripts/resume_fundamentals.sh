#!/bin/bash
# =============================================================
# 基本面信息全量重刷 / 断点续跑（监督循环版）
# 用法: bash scripts/resume_fundamentals.sh
# 特性: manifest 断点续跑、防封禁（300次调用回收会话+0.25s间隔）、
#       网络探针自愈、异常自动重启（40轮×5分钟）、完成后自动跑 G1 验证
# 预计耗时: 首次全量约 12-16 小时（baostock 串行硬约束）；中断后重跑只补剩余
# =============================================================
cd "$(dirname "$0")/.."   # 进入 backend 目录

# 防双开：双进程会竞争 baostock 会话并再次触发封禁
if pgrep -f "fundamentals_rebuild_manager" > /dev/null; then
    echo "✗ 已有基本面重刷进程在运行。如需重启先执行: pkill -f fundamentals_rebuild_manager"
    exit 1
fi

MAX_ROUNDS=40
for i in $(seq 1 $MAX_ROUNDS); do
    echo "=== 第 $i/$MAX_ROUNDS 轮启动 $(date '+%F %T') ==="
    python3 -c "
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
from src.config.config_manager import ConfigManager
from src.sync.fundamentals_rebuild_manager import FundamentalsRebuildManager
m = FundamentalsRebuildManager(ConfigManager())
r = m.execute(resume=True, save_to_csv=True, save_to_db=True)
print('=== ROUND RESULT ===', r.get('success'), r.get('blocked'), r['stats'])
" || true

    STATUS=$(python3 -c "
import json
from src.config.config_manager import ConfigManager
from src.sync.fundamentals_rebuild_manager import FundamentalsRebuildManager
d = FundamentalsRebuildManager(ConfigManager())._load_manifest()
done = len(d['completed']) >= 5537 and not d['failed']
print(('DONE' if done else 'CONTINUE') + ' completed=%d failed=%d' % (len(d['completed']), len(d['failed'])))" 2>/dev/null)
    echo "状态: $STATUS"
    case "$STATUS" in DONE*) echo "✓ 全部股票重刷完成"; break;; esac
    [ $i -lt $MAX_ROUNDS ] && { echo "300 秒后自动重启（断点续跑）..."; sleep 300; }
done

echo ""
echo "=== 运行 G1 验证 ==="
python3 scripts/verify_g1.py
echo ""
echo "完成 G1 后，执行日K重建: bash scripts/rebuild_kline_day.sh"
