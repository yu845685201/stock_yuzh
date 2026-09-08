#!/bin/bash
# 每小时进度记录（由计划任务调用；也可手动执行）
# 输出追加到: backend/tmp/progress_history_20260907.log
cd "$(dirname "$0")/.."
TS=$(date '+%F %T')
OUT=tmp/progress_history_20260907.log
MANIFEST=tmp/fundamentals_rebuild_manifest.json

MF=$(python3 -c "
import json
try:
    d = json.load(open('tmp/fundamentals_rebuild_manifest.json'))
    print('completed=%d/5537 failed=%d' % (len(d['completed']), len(d['failed'])))
except Exception as e:
    print('manifest读取失败:', e)" 2>/dev/null)
DONE=$([ -f tmp/fundamentals_rebuild_DONE ] && echo "YES" || echo "NO")
PROC=$(ps -eo command 2>/dev/null | grep -c "[p]ython3 -c")
DBF=$(PGPASSWORD=yuzh1234 psql -h 127.0.0.1 -U postgres -d stock_analysis_uat -t -A -c "SELECT COUNT(*) FROM base_fundamentals_info" 2>/dev/null)
DBK=$(PGPASSWORD=yuzh1234 psql -h 127.0.0.1 -U postgres -d stock_analysis_uat -t -A -c "SELECT COUNT(*) FROM his_kline_day" 2>/dev/null)
DBA=$(PGPASSWORD=yuzh1234 psql -h 127.0.0.1 -U postgres -d stock_analysis_uat -t -A -c "SELECT COUNT(*) FROM his_kline_day WHERE amount>0" 2>/dev/null)
DBR=$(PGPASSWORD=yuzh1234 psql -h 127.0.0.1 -U postgres -d stock_analysis_uat -t -A -c "SELECT COUNT(*) FROM his_kline_day WHERE raw_close IS NOT NULL" 2>/dev/null)
LAST=$(tail -1 /tmp/fundamentals_supervised.log 2>/dev/null | cut -c1-120)

{
  echo "[$TS] F2进度: $MF | DONE标记: $DONE | 采集进程数: $PROC"
  echo "        基本面行数: $DBF | 日K行数: $DBK | 日K amount>0: $DBA | raw_close非空: $DBR"
  echo "        supervised尾部: $LAST"
} >> "$OUT"
echo "进度已记录到 $OUT"
tail -3 "$OUT"
