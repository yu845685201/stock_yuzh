"""
个股 PE/PB 自算（方案 4.7，M2；本地计算，无网络请求）

口径（写入表注释与报告说明）：
- 总市值 = 当日收盘价 × 总股本（his_kline_day 当日 total_share 快照优先，
  回退 base_fundamentals_info as-of 已披露 total_share）
- PE(TTM) = 总市值 ÷ 近四季归母净利之和（累计口径换算单季：
  TTM = 最新累计 + 上年年报累计 − 上年同期累计；最新期即年报时 = 该年报累计）
- PB(LF) = 总市值 ÷ 最新一期净资产（net_assets = THS 每股净资产 × 总股本 折算入库）
- 数据真实性原则：任一输入缺失 → 对应字段空 + insufficient_note，不硬造

历史回填（开发项 A）：execute(start_date, end_date) 按交易日历逐日跑单日 as-of 口径；
前置依赖开发项 K（披露日回补）——否则 as-of 过滤把历史财报全部筛空（X-6）。
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ..config.config_manager import ConfigManager
from ..database.connection import DatabaseConnection
from ..utils.financial_ttm import ttm_frame as _ttm_frame_impl
from .review_common import write_review_report

logger = logging.getLogger(__name__)


class ValuationCalcManager:
    """PE/PB 自算：SQL 取数 -> pandas 向量化 -> stock_valuation_daily"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()

    def execute(self, trade_date: Optional[str] = None, dry_run: bool = False,
                start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
        """单日模式（trade_date 缺省当日）或历史回填模式（start_date/end_date 同给）。

        单日取数统一走 as-of 方法（对当日 as-of=当日，行为与旧"最新时点"口径等价）。
        """
        started = datetime.now()
        if start_date or end_date:
            if not (start_date and end_date):
                raise ValueError('--start-date 与 --end-date 必须同时给出（回填模式）')
            return self._execute_backfill(start_date, end_date, dry_run=dry_run, started=started)
        trade_date = trade_date or datetime.now().strftime('%Y%m%d')
        stats: Dict[str, Any] = {
            'trade_date': trade_date, 'stocks': 0, 'mv_ok': 0, 'pe_ok': 0, 'pb_ok': 0,
            'insufficient': 0, 'rows': 0,
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
        try:
            snapshot = self.db.fetch_day_kline_snapshot(trade_date)
            if not snapshot:
                result['errors'].append(f'当日 {trade_date} 日K快照为空——先跑 sync-kline-day')
                return result
            rows = self.compute(snapshot, self.db.fetch_financial_reports_asof(trade_date, 8),
                                self.db.fetch_total_shares_asof(trade_date))
            stats['stocks'] = len(rows)
            stats['mv_ok'] = sum(1 for r in rows if r['total_mv'] is not None)
            stats['pe_ok'] = sum(1 for r in rows if r['pe_ttm'] is not None)
            stats['pb_ok'] = sum(1 for r in rows if r['pb'] is not None)
            stats['insufficient'] = sum(1 for r in rows if r['insufficient_note'])
            stats['rows'] = len(rows)
            if not dry_run and rows:
                self.db.upsert_stock_valuations(rows)
            result['success'] = True
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
        return result

    # ---------- 历史回填模式（开发项 A） ----------

    def _execute_backfill(self, start_date: str, end_date: str, dry_run: bool,
                          started: datetime) -> Dict[str, Any]:
        """按 base_trade_calendar 交易日循环单日 as-of 口径逐日回填。

        - 断点续跑（幂等可中断重跑）：该日 stock_valuation_daily 行数 >= 当日K线行数 × 0.8
          视为已完成，跳过（停牌因素留 20% 容差）；
        - 当日无日K快照（非交易日/未同步）跳过并计数，不视为失败；
        - 进度：每 50 日输出一行日志。
        """
        start = _parse_date(start_date)
        end = _parse_date(end_date)
        if start is None or end is None or start > end:
            raise ValueError(f'回填区间非法: {start_date!r} ~ {end_date!r}')
        stats: Dict[str, Any] = {
            'start_date': start.strftime('%Y%m%d'), 'end_date': end.strftime('%Y%m%d'),
            'days_total': 0, 'days_done': 0, 'days_skipped_done': 0, 'days_no_kline': 0,
            'stocks': 0, 'mv_ok': 0, 'pe_ok': 0, 'pb_ok': 0,
            'insufficient': 0, 'rows': 0, 'last_completed_date': None,
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [],
                                  'blocked': False, 'backfill': True}
        try:
            calendar = self.db.fetch_trade_calendar(start.isoformat(), end.isoformat())
            trade_days = [_to_yyyymmdd(r.get('calendar_date')) for r in calendar]
            trade_days = [d for d in trade_days if d]
            stats['days_total'] = len(trade_days)
            if not trade_days:
                result['errors'].append(f'区间 {stats["start_date"]}~{stats["end_date"]} '
                                        f'交易日历为空——检查 base_trade_calendar')
                return result
            for idx, t in enumerate(trade_days, start=1):
                snapshot = self.db.fetch_day_kline_snapshot(t)
                if not snapshot:
                    stats['days_no_kline'] += 1
                    continue
                existing = 0 if dry_run else self.db.fetch_valuation_row_count(t)
                if existing >= len(snapshot) * 0.8:
                    stats['days_skipped_done'] += 1
                    stats['last_completed_date'] = t
                    continue
                rows = self.compute(snapshot, self.db.fetch_financial_reports_asof(t, 8),
                                    self.db.fetch_total_shares_asof(t))
                if not dry_run and rows:
                    self.db.upsert_stock_valuations(rows)
                stats['days_done'] += 1
                stats['stocks'] += len(rows)
                stats['mv_ok'] += sum(1 for r in rows if r['total_mv'] is not None)
                stats['pe_ok'] += sum(1 for r in rows if r['pe_ttm'] is not None)
                stats['pb_ok'] += sum(1 for r in rows if r['pb'] is not None)
                stats['insufficient'] += sum(1 for r in rows if r['insufficient_note'])
                stats['rows'] += len(rows)
                stats['last_completed_date'] = t
                if idx % 50 == 0:
                    elapsed = (datetime.now() - started).total_seconds()
                    logger.info(f'估值回填进度 {idx}/{stats["days_total"]} 日'
                                f'（已完成 {stats["days_done"]}，跳过 {stats["days_skipped_done"]}，'
                                f'无K线 {stats["days_no_kline"]}；已用 {elapsed / 60:.1f} 分钟）')
            result['success'] = True
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_backfill_report(result)
        return result

    # ---------- 计算核心（拆分便于单测） ----------

    def compute(self, snapshot: List[Dict[str, Any]], reports: List[Dict[str, Any]],
                fallback_shares: Dict[str, float]) -> List[Dict[str, Any]]:
        """向量化计算（snapshot/report 缺行安全）"""
        kline = pd.DataFrame(snapshot)
        if kline.empty:
            return []
        for col in ('close', 'change_rate', 'total_share'):
            if col not in kline.columns:
                kline[col] = None
        kline['close_f'] = pd.to_numeric(kline['close'], errors='coerce')

        # 总股本：当日快照优先，缺失回退最新披露
        def _share(row) -> Optional[float]:
            v = row.get('total_share')
            try:
                v = float(v) if v is not None else None
            except (TypeError, ValueError):
                v = None
            if v and v > 0:
                return v
            return fallback_shares.get(row['ts_code'])

        kline['share'] = kline.apply(_share, axis=1)

        reports_df = self._ttm_frame(reports)
        df = kline.merge(reports_df, on='ts_code', how='left') if not reports_df.empty \
            else kline.assign(ttm_profit=None, latest_net_assets=None, latest_stat_date=None)

        total_mv = df['close_f'] * df['share']
        pe_ttm = _safe_div(total_mv, df['ttm_profit'])
        pb = _safe_div(total_mv, df['latest_net_assets'])

        notes: List[Optional[str]] = []
        for mv, pe, pbv, share in zip(total_mv, pe_ttm, pb, df['share']):
            reasons = []
            if share is None or (isinstance(mv, float) and pd.isna(mv)):
                reasons.append('缺股本')
            if pe is None or (isinstance(pe, float) and pd.isna(pe)):
                reasons.append('缺财报TTM')
            if pbv is None or (isinstance(pbv, float) and pd.isna(pbv)):
                reasons.append('缺净资产')
            notes.append('；'.join(reasons) or None)

        out = []
        for i, row in df.iterrows():
            mv = total_mv.iloc[i] if i in total_mv.index else None
            pe = pe_ttm.iloc[i] if i in pe_ttm.index else None
            pbv = pb.iloc[i] if i in pb.index else None
            out.append({
                'ts_code': row['ts_code'],
                'trade_date': row.get('trade_date'),
                'total_mv': _round_or_none(mv, 4),
                'pe_ttm': _round_or_none(pe, 4),
                'pb': _round_or_none(pbv, 4),
                'insufficient_note': notes[i] if i < len(notes) else '缺财报TTM',
            })
        return out

    @staticmethod
    def _ttm_frame(reports: List[Dict[str, Any]]) -> pd.DataFrame:
        """每只股票最近 N 期累计净利 -> (TTM 归母净利, 最新净资产, 最新报告期)

        实现已抽公共至 ``utils.financial_ttm.ttm_frame``（P0-4 方案 §3.3.2，
        原则 P6：报告层估值 calc 与本处同口径，禁两处实现）——本方法为 import
        委托，行为与列名不变（test/test_valuation_calc.py 保护）。
        """
        return _ttm_frame_impl(reports)

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 数据日期：{stats['trade_date']}；口径：总市值=收盘×总股本；"
            f"PE(TTM)=市值÷近四季归母净利；PB(LF)=市值÷最新净资产",
            f"- 全市场 {stats['stocks']} 只：市值可算 {stats['mv_ok']}，PE 可算 {stats['pe_ok']}，"
            f"PB 可算 {stats['pb_ok']}，输入不足 {stats['insufficient']}（字段留空+note，不硬造）",
        ]
        status = '成功' if result['success'] else '失败'
        return write_review_report('个股PE/PB自算报告', 'stock_valuation_calc_report',
                                   status, lines, result.get('duration'))

    def _write_backfill_report(self, result: Dict[str, Any]) -> Optional[str]:
        """历史回填汇总报告（含口径说明，方案 4.1 要求写入报告）"""
        stats = result['stats']
        lines = [
            f"- 区间：{stats['start_date']}~{stats['end_date']}（交易日 "
            f"{stats['days_total']}：回填 {stats['days_done']}、断点跳过 "
            f"{stats['days_skipped_done']}、无K线 {stats['days_no_kline']}）",
            f"- 累计：{stats['rows']} 行（市值可算 {stats['mv_ok']}，PE 可算 {stats['pe_ok']}，"
            f"PB 可算 {stats['pb_ok']}，输入不足 {stats['insufficient']}）",
            "- 口径：总市值=当日收盘（前复权）× as-of 已披露总股本；前复权价×现股本在除权日"
            "附近与真实市值有偏差，分位统计用途下可接受（同一股票时间序列自洽）",
            "- 口径：PE(TTM) 分子分母均点时（as-of 股本 ÷ as-of 已披露近四季净利）；"
            "8 期窗口不足的股票 TTM 留空不硬造",
            "- 口径：取数过滤 disclosure_date <= 数据日（杜绝前视偏差）；前置依赖开发项 K 披露日回补",
        ]
        if result['errors']:
            lines.append(f"- 错误：{result['errors']}")
        status = '成功' if result['success'] else '失败'
        return write_review_report('个股PE/PB历史回填报告', 'stock_valuation_backfill_report',
                                   status, lines, result.get('duration'))


def _parse_date(value: Optional[str]):
    """yyyyMMdd / yyyy-mm-dd -> date；非法返回 None"""
    from datetime import date as _date
    if not value:
        return None
    s = str(value).replace('-', '')[:8]
    try:
        return datetime.strptime(s, '%Y%m%d').date()
    except ValueError:
        return None


def _to_yyyymmdd(value: Any) -> Optional[str]:
    """交易日历 calendar_date（date/str）-> yyyyMMdd"""
    if value is None:
        return None
    s = str(value).replace('-', '')[:8]
    return s if len(s) == 8 and s.isdigit() else None


def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    out = a / b
    out = out.replace([float('inf'), float('-inf')], None)
    return out.where(b.notna() & (b != 0))


def _round_or_none(value: Any, digits: int) -> Optional[float]:
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(f):
        return None
    return round(f, digits)
