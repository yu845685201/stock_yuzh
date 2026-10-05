#!/bin/bash
# 财报PDF通宵自动续跑看守（2026-10-01）
# - 轮询 sync-financial-pdf 进程；退出且未完成时自动断点续跑（manifest 幂等）
# - 短时退出（<20min，疑似风控急停）按 3min→6min→…封顶2h 退避；长跑退出 3min 后续跑
# - 本轮"下载 0 份"且无风控急停字样 → 判定采集完成，看守退出
cd /Users/yuzh/develop/os/os_yuzh/04_工具/stock_yuzh_base/stock_yuzh || exit 1
LOG=logs/financial_pdf_overnight.log
COOL=600
echo "$(date '+%F %T') 看守启动" >> "$LOG"
while true; do
  if pgrep -f "run_cli.py sync-financial-pdf" > /dev/null; then
    sleep 300
    continue
  fi
  RUNLOG="logs/financial_pdf_overnight_run_$(date +%m%d_%H%M).log"
  before=$(ls data/financial_pdf 2>/dev/null | wc -l | tr -d ' ')
  start=$(date +%s)
  echo "$(date '+%F %T') 启动续跑（已有 $before 只）" >> "$LOG"
  NO_PROXY=127.0.0.1,localhost .venv/bin/python backend/run_cli.py sync-financial-pdf --init >> "$RUNLOG" 2>&1
  rc=$?
  after=$(ls data/financial_pdf 2>/dev/null | wc -l | tr -d ' ')
  dur=$(( $(date +%s) - start ))
  downloaded=$(grep -o "下载 [0-9]*" "$RUNLOG" | tail -1 | grep -o "[0-9]*$")
  blocked=$(grep -c "风控急停" "$RUNLOG")
  echo "$(date '+%F %T') 本轮退出 rc=$rc 耗时=${dur}s 目录 $before->$after 下载=${downloaded:-?} 风控=${blocked}" >> "$LOG"
  if [ "${downloaded:-1}" = "0" ] && [ "$blocked" = "0" ]; then
    echo "$(date '+%F %T') 本轮下载 0 份且无风控，判定采集完成，看守退出" >> "$LOG"
    break
  fi
  if [ "$dur" -lt 1200 ]; then
    COOL=$(( COOL * 2 )); [ "$COOL" -gt 7200 ] && COOL=7200
  else
    COOL=600
  fi
  echo "$(date '+%F %T') 冷却 ${COOL}s 后续跑" >> "$LOG"
  sleep "$COOL"
done
