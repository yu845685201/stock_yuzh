"""
情绪指标日频表回填（开发项 B，方案 4.2）

- 表 sentiment_metrics_daily：字段与 review/cross_section.sentiment_metrics 返回键一一对应
  （ladder/promotion 存 JSONB），另加涨跌平家数（同一截面顺手计算）
- **口径红线：与报告层逐字节同口径**——复用 annotate_panel / suppress_first_day /
  cross_section_at / sentiment_metrics 同一实现，禁止另写判定
- 分年块控制内存（年块约 135 万行，实测峰值 ~2GB）；
  **块首前推 62 交易日红线**（连板 lookback=60 + 滚动窗口需求；第三轮实测踩坑：
  误用块尾前推会把跨块连板截断——20240930 空间板 12 板误判 2 板）
- 上市首日抑制与报告层同源（load_stock_universe 的 list_date + N 前缀兜底）
- 幂等：逐日 upsert，可中断重跑；与每日 sync-review-all 不冲突（建议错峰）
"""

import json
import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd

from ..analysis.data_reader import DataReader
from ..config.config_manager import ConfigManager
from ..database.connection import DatabaseConnection
from ..review.cross_section import (annotate_panel, cross_section_at,
                                    sentiment_metrics, suppress_first_day)
from .review_common import write_review_report

logger = logging.getLogger(__name__)

#: 面板业务列（与 aggregator.aggregate 完全一致）
PANEL_COLS = ['stock_name', 'open', 'high', 'low', 'close', 'preclose',
              'change_rate', 'volume', 'amount', 'is_st', 'raw_close']
#: 块首前推交易日数（连板 lookback=60 + 余量）
PANEL_LOOKBACK_DAYS = 62


class SentimentMetricsManager:
    """情绪指标日频回填：分年块加载面板 -> 逐日截面 -> sentiment_metrics -> upsert"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        self.reader = DataReader(config_manager)

    # ---------- 主流程 ----------

    def execute(self, start_date: Optional[str] = None, end_date: Optional[str] = None,
                dry_run: bool = False) -> Dict[str, Any]:
        """回填 [start_date, end_date]（yyyyMMdd/iso 均可；缺省 = 最近 10 个交易日增量补齐）。

        缺省增量为日常维护设计：每日 sync-review-all 后跑一次即可让表跟上
        （幂等 upsert；已存在日期按 upsert 覆盖，无重复）。全量历史用显式区间。
        """
        started = datetime.now()
        end = _parse_date(end_date) or self._latest_kline_date()
        if end is None:
            raise ValueError('无法确定回填终点（his_kline_day 为空且未显式给 --end）')
        if start_date:
            start = _parse_date(start_date)
            if start is None or start > end:
                raise ValueError(f'回填区间非法: {start_date!r} ~ {end_date!r}')
        else:
            start = end - timedelta(days=20)  # 自然日窗口，实际按交易日截取（≈10 个交易日）

        stats: Dict[str, Any] = {
            'range': f'{start.isoformat()}~{end.isoformat()}', 'days_total': 0,
            'days_done': 0, 'days_no_kline': 0, 'rows': 0, 'chunks': [],
            'last_completed_date': None,
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [],
                                  'blocked': False, 'dry_run': dry_run}
        try:
            for year in range(start.year, end.year + 1):
                chunk_start = max(start, date(year, 1, 1))
                chunk_end = min(end, date(year, 12, 31))
                self._backfill_chunk(chunk_start, chunk_end, dry_run, stats)
            result['success'] = stats['days_done'] + stats['days_no_kline'] == stats['days_total']
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
        return result

    # ---------- 分年块 ----------

    def _backfill_chunk(self, chunk_start: date, chunk_end: date,
                        dry_run: bool, stats: Dict[str, Any]) -> None:
        """单年块：块首前推 62 交易日加载面板，逐日截面计算并 upsert。"""
        # 日历多取 400 自然日（覆盖 62 交易日回推），锚定块内第一个交易日
        calendar = self.db.fetch_trade_calendar(
            (chunk_start - timedelta(days=400)).isoformat(), chunk_end.isoformat())
        dates = [_to_yyyymmdd(r.get('calendar_date')) for r in calendar]
        dates = [d for d in dates if d]
        anchor = _to_yyyymmdd(chunk_start)
        end_key = _to_yyyymmdd(chunk_end)
        anchor_idx = next((i for i, d in enumerate(dates) if d >= anchor), None)
        if anchor_idx is None:
            return
        chunk_days = [d for d in dates[anchor_idx:] if d <= end_key]
        if not chunk_days:
            return
        stats['days_total'] += len(chunk_days)
        panel_start = dates[max(0, anchor_idx - PANEL_LOOKBACK_DAYS)]
        logger.info(f'情绪回填块 {chunk_start}~{chunk_end}：{len(chunk_days)} 个交易日，'
                    f'面板起点 {panel_start}（块首前推 {min(PANEL_LOOKBACK_DAYS, anchor_idx)} 交易日）')

        panel = self.reader.load_kline(PANEL_COLS, chunk_days[-1], start_date=panel_start)
        if panel.empty:
            stats['days_no_kline'] += len(chunk_days)
            logger.warning(f'情绪回填块 {chunk_start}~{chunk_end} 面板为空（his_kline_day 无数据）')
            return
        panel = annotate_panel(panel)
        list_dates = {s.ts_code: s.list_date for s in self.reader.load_stock_universe()}
        panel = suppress_first_day(panel, list_dates)

        for i, t in enumerate(chunk_days):
            prev_t = dates[anchor_idx + i - 1] if anchor_idx + i >= 1 else None
            cs_today = cross_section_at(panel, t)
            if cs_today.empty:
                stats['days_no_kline'] += 1
                continue
            cs_prev = cross_section_at(panel, prev_t) if prev_t else cs_today.iloc[0:0]
            row = self._metrics_row(t, cs_today, cs_prev)
            if not dry_run:
                self.db.upsert_sentiment_metrics([row])
            stats['days_done'] += 1
            stats['rows'] += 1
            stats['last_completed_date'] = t
        stats['chunks'].append(f'{chunk_start.year}')

    # ---------- 行构造 ----------

    @staticmethod
    def _metrics_row(t: str, cs_today: pd.DataFrame,
                     cs_prev: pd.DataFrame) -> Dict[str, Any]:
        """截面 -> 表行（与 sentiment_metrics 返回键一一对应 + 涨跌平家数）"""
        m = sentiment_metrics(cs_today, cs_prev)
        change = pd.to_numeric(cs_today['change_rate'], errors='coerce')
        traded = cs_today['_traded'].fillna(False).astype(bool) \
            if '_traded' in cs_today.columns else change.notna()
        up = int((traded & change.gt(0)).sum())
        down = int((traded & change.lt(0)).sum())
        flat = int((traded & change.eq(0)).sum())
        return {
            'trade_date': t,
            'stock_count': int(cs_today['ts_code'].nunique()),
            'up_count': up, 'down_count': down, 'flat_count': flat,
            'limit_up_count': m['limit_up_count'],
            'limit_down_count': m['limit_down_count'],
            'burst_count': m['burst_count'],
            'burst_rate': m['burst_rate'],
            'max_streak': m['max_streak'],
            'ladder': json.dumps(m['ladder'], ensure_ascii=False),
            'premium_mean': m['premium_mean'],
            'premium_median': m['premium_median'],
            'premium_count': m['premium_count'],
            'premium_continued': m['premium_continued'],
            'promotion': json.dumps(m['promotion'], ensure_ascii=False),
        }

    # ---------- 工具 ----------

    def _latest_kline_date(self) -> Optional[date]:
        rows = self.db.execute_query("SELECT MAX(trade_date) AS d FROM his_kline_day")
        if rows and rows[0].get('d'):
            return _parse_date(str(rows[0]['d']))
        return None

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 区间：{stats['range']}（分块：{'/'.join(stats['chunks']) or '—'}）",
            f"- 交易日 {stats['days_total']}：回填 {stats['days_done']}，"
            f"无K线 {stats['days_no_kline']}（末行 {stats['last_completed_date']}）",
            "- 口径：与报告层逐字节同口径（annotate_panel/suppress_first_day/"
            "cross_section_at/sentiment_metrics 同一实现）；块首前推 62 交易日",
            "- 幂等：逐日 upsert，可中断重跑；缺省（无区间）为最近 10 个交易日增量补齐",
        ]
        if result['errors']:
            lines.append(f"- 错误：{result['errors']}")
        status = '成功' if result['success'] else '失败'
        return write_review_report('情绪指标日频回填报告', 'sentiment_backfill_report',
                                   status, lines, result.get('duration'))


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    s = str(value).replace('-', '')[:8]
    try:
        return datetime.strptime(s, '%Y%m%d').date()
    except ValueError:
        return None


def _to_yyyymmdd(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).replace('-', '')[:8]
    return s if len(s) == 8 and s.isdigit() else None
