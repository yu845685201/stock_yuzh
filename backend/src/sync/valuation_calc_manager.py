"""
个股 PE/PB 自算（方案 4.7，M2；本地计算，无网络请求）

口径（写入表注释与报告说明）：
- 总市值 = 当日收盘价 × 总股本（his_kline_day 当日 total_share 快照优先，
  回退 base_fundamentals_info 最新披露 total_share）
- PE(TTM) = 总市值 ÷ 近四季归母净利之和（累计口径换算单季：
  TTM = 最新累计 + 上年年报累计 − 上年同期累计；最新期即年报时 = 该年报累计）
- PB(LF) = 总市值 ÷ 最新一期净资产（net_assets = THS 每股净资产 × 总股本 折算入库）
- 数据真实性原则：任一输入缺失 → 对应字段空 + insufficient_note，不硬造
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ..config.config_manager import ConfigManager
from ..database.connection import DatabaseConnection
from .review_common import write_review_report

logger = logging.getLogger(__name__)


class ValuationCalcManager:
    """PE/PB 自算：SQL 取数 -> pandas 向量化 -> stock_valuation_daily"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()

    def execute(self, trade_date: Optional[str] = None, dry_run: bool = False) -> Dict[str, Any]:
        started = datetime.now()
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
            rows = self.compute(snapshot, self.db.fetch_recent_financial_reports(8),
                                self.db.fetch_latest_total_shares())
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

        累计口径单季换算：TTM = cum(L) + cum(A_prev) - cum(S_prev)；
        L 为年报（1231）时 TTM = cum(L)。净资产取最新期 net_assets。
        """
        if not reports:
            return pd.DataFrame(columns=['ts_code', 'ttm_profit', 'latest_net_assets',
                                         'latest_stat_date'])
        df = pd.DataFrame(reports)
        df['stat_date'] = df['stat_date'].astype(str)
        df['net_profit'] = pd.to_numeric(df['net_profit'], errors='coerce')
        df['net_assets'] = pd.to_numeric(df.get('net_assets'), errors='coerce')
        df = df.sort_values(['ts_code', 'stat_date'], ascending=[True, False])

        ttm_rows = []
        for ts_code, g in df.groupby('ts_code'):
            rows = g.reset_index(drop=True)
            if rows.empty or pd.isna(rows.loc[0, 'net_profit']):
                continue
            latest = rows.loc[0]
            latest_cum = latest['net_profit']
            latest_md = latest['stat_date'][4:8]
            ttm = None
            if latest_md == '1231':
                ttm = latest_cum
            else:
                # 上年年报
                prev_annual = rows[(rows['stat_date'].str.endswith('1231'))
                                   & (rows['stat_date'] < latest['stat_date'])]
                # 上年同期
                prev_year = str(int(latest['stat_date'][:4]) - 1)
                prev_same = rows[rows['stat_date'] == f'{prev_year}{latest_md}']
                if not prev_annual.empty and not prev_same.empty \
                        and not pd.isna(prev_annual.iloc[0]['net_profit']) \
                        and not pd.isna(prev_same.iloc[0]['net_profit']):
                    ttm = latest_cum + prev_annual.iloc[0]['net_profit'] \
                        - prev_same.iloc[0]['net_profit']
            ttm_rows.append({
                'ts_code': ts_code,
                'ttm_profit': ttm,
                'latest_net_assets': latest['net_assets'],
                'latest_stat_date': latest['stat_date'],
            })
        return pd.DataFrame(ttm_rows)

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
