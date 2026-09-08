#!/bin/bash
# =============================================================
# 日K线全量重建（清空 + init 全市场双源重建）
# 用法:
#   bash scripts/rebuild_kline_day.sh                 # 清空表后全量重建（标准流程）
#   bash scripts/rebuild_kline_day.sh --no-truncate   # 不清空，续跑/补失败股票（幂等）
# 前置: 基本面全量重刷已完成且 G1 验证通过（日K normalize 依赖基本面匹配）
# 预计耗时: 约 30 分钟（8 并发）；中断后重跑同一命令即可（整股替换幂等）
# =============================================================
cd "$(dirname "$0")/.."   # 进入 backend 目录

SKIP_TRUNCATE=0
[ "$1" == "--no-truncate" ] && SKIP_TRUNCATE=1

# 中间件健康检查（日K数据源）
if ! curl -sf http://127.0.0.1:8080/api/health > /dev/null; then
    echo "✗ tdx-api 中间件不可用（127.0.0.1:8080）。先启动: cd <tdx-api仓库> && docker compose up -d"
    exit 1
fi
echo "✓ tdx-api 中间件正常"

if [ $SKIP_TRUNCATE -eq 0 ]; then
    echo "清空 his_kline_day ..."
    PGPASSWORD=yuzh1234 psql -h 127.0.0.1 -U postgres -d stock_analysis_uat -c "TRUNCATE TABLE his_kline_day;"
else
    echo "跳过清空（续跑/补采模式）"
fi

LOG=/tmp/kline_rebuild_$(date +%Y%m%d_%H%M%S).log
echo "启动全市场日K双源重建，日志: $LOG"
python3 -c "
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
from src.config.config_manager import ConfigManager
from src.sync.sync_manager import SyncManager
m = SyncManager(ConfigManager())
r = m.sync_kline_day(init_mode=True, ts_codes=None, save_to_csv=True, save_to_db=True)
print('=== FINAL ===', 'success:', r['success'], 'records:', r['records'], 'db_rows:', r['db_rows'],
      'failed_stocks:', r.get('failed_stocks'), 'detection:', r.get('detection'),
      'source_missing:', r.get('source_missing'))
" 2>&1 | tee "$LOG"

echo ""
echo "=== 运行 G3 全局对账 ==="
python3 scripts/verify_g3.py
echo ""
echo "G3 通过后: git add backend/src backend/test backend/scripts backend/config/config.yaml backend/requirements.txt doc/ && git commit && git push origin feature/data_coll_simple"
