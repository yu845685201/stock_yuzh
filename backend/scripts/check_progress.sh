#!/bin/bash
# =============================================================
# 一键查看跑数进度（基本面 + 日K）
# 用法: bash scripts/check_progress.sh
# =============================================================
echo "=== 采集进程 ==="
ps -eo pid,etime,command | grep "[p]ython3 -c" | cut -c1-60 | head -3 || echo "（无运行中进程）"

echo ""
echo "=== 基本面重刷状态 ==="
cd "$(dirname "$0")/.."
python3 -c "
import json
try:
    d = json.load(open('tmp/fundamentals_rebuild_manifest.json'))
    print('manifest: completed %d / 5537, failed %d' % (len(d['completed']), len(d['failed'])))
    print('判定:', '已完成' if len(d['completed']) >= 5537 and not d['failed'] else '进行中')
except FileNotFoundError:
    print('manifest 不存在（尚未开始）')"
[ -f tmp/fundamentals_rebuild_DONE ] && echo "DONE 标记存在（重刷已完成）"
for f in /tmp/fundamentals_supervised.log /tmp/fundamentals_rebuild_full.log /tmp/fundamentals_rebuild_resume*.log /tmp/fundamentals_rebuild_final.log; do
    [ -f "$f" ] && echo "--- $f (最后写入: $(stat -f '%Sm' "$f"))" && grep -E "进度|FINAL|封禁|急停" "$f" 2>/dev/null | tail -2
done

echo ""
echo "=== 数据库现状（UAT） ==="
PGPASSWORD=yuzh1234 psql -h 127.0.0.1 -U postgres -d stock_analysis_uat -t -c "
SELECT 'base_fundamentals_info 行数: ' || COUNT(*) FROM base_fundamentals_info
UNION ALL SELECT 'his_kline_day 行数: ' || COUNT(*) FROM his_kline_day
UNION ALL SELECT 'his_kline_day amount非零: ' || COUNT(*) FROM his_kline_day WHERE amount > 0
UNION ALL SELECT 'his_kline_day raw_close非空: ' || COUNT(*) FROM his_kline_day WHERE raw_close IS NOT NULL;"

echo ""
echo "=== 日K重建日志（如有） ==="
ls -lt /tmp/kline_rebuild_*.log 2>/dev/null | head -2 || echo "（尚未启动过日K重建）"
