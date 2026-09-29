"""
龙虎榜同步（方案 4.9，M3；沪深交易所官网每日披露页官方 JSON，17:30 后发布）

接口事实（2026-09-27 实抓锁定，详见 sse_szse_source 文件头）：
- 深交所：列表 CATALOGID=1842_xxpl_after（dqrq/zqdm/zqjc/cjje 亿元/plyy 原因/
  bz 内含明细入口 ZBDM）+ 席位明细 1842_detal tab2（买1~卖5 营业部金额）
- 上交所：queryTradeOpenInfo.do（行按 个股×原因×席位 展开，bsType B/S、
  branchRank、branchName、branchTxAmt；refType 为披露原因代码）→
  汇总与席位同源一次拿全
- 失败次日补抓（官网保留历史榜单）；入库榜单数与官网展示条数校验
"""

import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ..config.config_manager import ConfigManager
from ..data_sources.sse_szse_source import SzseSseSource
from ..database.connection import DatabaseConnection
from .review_common import code6_to_ts_code, write_review_report

logger = logging.getLogger(__name__)

# 深交所 bz 字段明细入口参数（a-param='/ShowReport/data?...ZBDM=0902'）
_ZBDM_PATTERN = re.compile(r'ZBDM=(\d+)')


class LhbManager:
    """龙虎榜：官网 JSON -> lhb_stock_daily + lhb_detail（含席位则一并入库）"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        self.source = SzseSseSource(self.config)

    def execute(self, trade_date: Optional[str] = None, dry_run: bool = False) -> Dict[str, Any]:
        started = datetime.now()
        trade_date = trade_date or datetime.now().strftime('%Y%m%d')
        stats: Dict[str, Any] = {
            'trade_date': trade_date, 'szse_stocks': 0, 'sse_stocks': 0,
            'lhb_rows': 0, 'detail_rows': 0, 'errors': [],
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
        try:
            stock_rows, detail_rows = [], []
            szse_stocks, szse_details = self._collect_szse(trade_date)
            sse_stocks, sse_details = self._collect_sse(trade_date)
            stock_rows = szse_stocks + sse_stocks
            detail_rows = szse_details + sse_details
            stats['szse_stocks'] = len({(r['ts_code'], r['reason']) for r in szse_stocks})
            stats['sse_stocks'] = len({(r['ts_code'], r['reason']) for r in sse_stocks})
            stats['lhb_rows'] = len(stock_rows)
            stats['detail_rows'] = len(detail_rows)
            stats['rows'] = stats['lhb_rows']  # 编排器摘要行数约定（rows/records）

            if not stock_rows:
                result['errors'].append(f'{trade_date} 沪深龙虎榜均无数据'
                                        f'（非交易日或未到发布时间 17:30）')
            if not dry_run:
                if stock_rows:
                    self.db.upsert_lhb_stocks(stock_rows)
                if detail_rows:
                    self.db.upsert_lhb_details(detail_rows)
            result['success'] = bool(stock_rows)
        except Exception as e:
            logger.exception(f'龙虎榜采集失败: {e}')
            result['errors'].append(str(e))
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
        return result

    # ---------- 深交所 ----------

    def _collect_szse(self, trade_date: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        stocks: List[Dict[str, Any]] = []
        details: List[Dict[str, Any]] = []
        date_param = f'{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}'
        raw = self.source.fetch_szse_lhb(trade_date)
        for item in raw:
            code = str(item.get('zqdm') or '')
            if not code:
                continue
            ann_date = str(item.get('dqrq') or '').replace('-', '') or trade_date
            stocks.append({
                'trade_date': trade_date,
                'ts_code': code6_to_ts_code(code),
                'stock_name': item.get('zqjc'),
                'reason': str(item.get('plyy') or '').strip(),
                'buy_amt': None,   # 深交所列表仅披露成交金额（亿元），买卖分列在席位明细
                'sell_amt': None,
                'net_amt': None,
            })
            # 席位明细：从 bz 提取 ZBDM 后单独请求（串行低频）
            m = _ZBDM_PATTERN.search(str(item.get('bz') or ''))
            if m:
                try:
                    seats = self.source.fetch_szse_lhb_seats(ann_date, code, m.group(1))
                except Exception as e:
                    logger.warning(f'深交所席位明细失败 {code}: {e}')
                    seats = []
                for seat in seats:
                    seat_name = str(seat.get('zsmc') or '').strip()
                    if not seat_name:
                        continue
                    side_label = str(seat.get('mmlb') or '').strip()
                    if not side_label:
                        continue
                    # side 存官网原文（买1~卖5，含排名）：深交所"机构专用"对应多个不同
                    # 机构席位，若归并为纯方向会被 (ts_code, seat_name, side) 唯一键去重丢行
                    side = side_label if side_label[0] in ('买', '卖') else side_label[:1]
                    details.append({
                        'trade_date': trade_date,
                        'ts_code': code6_to_ts_code(code),
                        'seat_name': seat_name,
                        'side': side,
                        'amount': _num_yuan(seat.get('mrje') if side.startswith('买') else seat.get('mcje'), 1.0),
                    })
        return stocks, details

    # ---------- 上交所 ----------

    def _collect_sse(self, trade_date: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        stocks: List[Dict[str, Any]] = []
        details: List[Dict[str, Any]] = []
        raw = self.source.fetch_sse_lhb(trade_date)
        agg: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for item in raw:
            code = str(item.get('secCode') or '')
            if not code:
                continue
            ts_code = code6_to_ts_code(code)
            ref_type = str(item.get('refType') or '')
            bs_type = str(item.get('bsType') or '')
            amount = _to_float(item.get('branchTxAmt'))
            key = (ts_code, ref_type)
            if key not in agg:
                agg[key] = {
                    'trade_date': trade_date,
                    'ts_code': ts_code,
                    'stock_name': item.get('secAbbr'),
                    'reason': f'披露原因代码{ref_type}',
                    'buy_amt': 0.0, 'sell_amt': 0.0, 'net_amt': None,
                }
            if bs_type == 'B':
                agg[key]['buy_amt'] = (agg[key]['buy_amt'] or 0.0) + (amount or 0.0)
            elif bs_type == 'S':
                agg[key]['sell_amt'] = (agg[key]['sell_amt'] or 0.0) + (amount or 0.0)
            details.append({
                'trade_date': trade_date,
                'ts_code': ts_code,
                'seat_name': str(item.get('branchName') or '').strip(),
                # side 存官网原文含排名（买N/卖N，branchRank 为榜内排名），防同席位跨行去重
                'side': f"{'买' if bs_type == 'B' else '卖'}{item.get('branchRank') or ''}",
                'amount': amount,
            })
        for v in agg.values():
            if v['buy_amt'] is not None and v['sell_amt'] is not None:
                v['net_amt'] = round(v['buy_amt'] - v['sell_amt'], 2)
        return list(agg.values()), details

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 数据日期：{stats['trade_date']}（17:30 后发布，晚间重跑幂等安全）",
            f"- 上榜：深交所 {stats['szse_stocks']} 条 / 上交所 {stats['sse_stocks']} 条，"
            f"合计入库 {stats['lhb_rows']}",
            f"- 席位明细：{stats['detail_rows']} 条",
        ]
        if result['errors']:
            lines.append(f"- 提示：{result['errors']}")
        status = '成功' if result['success'] else '失败（待补）'
        return write_review_report('龙虎榜同步报告', 'lhb_sync_report',
                                   status, lines, result.get('duration'))


def _num_yuan(value: Any, scale: float) -> Optional[float]:
    """'15.25'×scale -> 元；千分位逗号安全"""
    if value is None:
        return None
    s = str(value).replace(',', '').strip()
    try:
        return float(s) * scale
    except ValueError:
        return None


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value not in (None, '') else None
    except (TypeError, ValueError):
        return None
