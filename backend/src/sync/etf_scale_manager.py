"""P3 ETF 份额采集（量价资金方案 V1.1 §5.2）：沪深两所官方 API → etf_scale_daily。

- 上交所：query.sse.com.cn/commonQuery.do
  sqlId=COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L&STAT_DATE=YYYY-MM-DD
  （**按日查历史**，实测回溯 2020-01；920 只全量单次拉取）；
- 深交所：szse.cn/api/report/ShowReport/data?CATALOGID=1945&PAGENO=n
  （749 只，**仅当日快照**，历史靠每日增量积累）；
- 份额单位两所一致 = **万份**（已用 159915≈206 亿份 / 510300≈241 亿份 /
  510050≈69 亿份定标验证）；份额日变化 = 申赎方向代理。
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ..data_sources.http_collector_base import HttpCollectorBase, ReviewBlockedError
from ..database.connection import DatabaseConnection
from ..config.config_manager import ConfigManager
from ..sync.review_common import mark_blocked, write_review_report

logger = logging.getLogger(__name__)

_SSE_URL = 'https://query.sse.com.cn/commonQuery.do'
_SSE_SQL_ID = 'COMMON_SSE_ZQPZ_ETFZL_XXPL_ETFGM_SEARCH_L'
_SZSE_URL = 'https://www.szse.cn/api/report/ShowReport/data'

_SSE_PAGE_PARAMS = {
    'isPagination': 'true',
    'pageHelp.pageSize': '2000',
    'pageHelp.pageNo': '1',
    'pageHelp.beginPage': '1',
    'pageHelp.cacheSize': '1',
    'pageHelp.endPage': '1',
}


def parse_sse_rows(payload: Dict[str, Any], stat_date: str) -> List[Dict[str, Any]]:
    """上交所响应 → etf_scale_daily 行（纯函数；TOT_VOL 万份）"""
    out: List[Dict[str, Any]] = []
    for r in payload.get('result') or []:
        code = str(r.get('SEC_CODE') or '').strip()
        if not code:
            continue
        out.append({
            'trade_date': stat_date.replace('-', ''),
            'exchange': 'sse',
            'fund_code': code,
            'fund_name': (str(r.get('SEC_NAME') or '').strip() or None),
            'etf_type': (str(r.get('ETF_TYPE') or '').strip() or None),
            'shares_wan': _to_f(r.get('TOT_VOL')),
            'source': 'sse_commonquery',
        })
    return out


def parse_szse_rows(rows: List[Dict[str, Any]], stat_date: str) -> List[Dict[str, Any]]:
    """深交所 ShowReport 行 → etf_scale_daily 行（dqgm 万份；HTML 包裹字段剥壳）"""
    out: List[Dict[str, Any]] = []
    for r in rows:
        code_m = re.search(r'>(\d{6})<', str(r.get('sys_key') or ''))
        if not code_m:
            continue
        name_m = re.search(r"name=([^&']+)", str(r.get('kzjcurl') or ''))
        out.append({
            'trade_date': stat_date.replace('-', ''),
            'exchange': 'szse',
            'fund_code': code_m.group(1),
            'fund_name': name_m.group(1) if name_m else None,
            'etf_type': None,
            'shares_wan': _to_f(r.get('dqgm')),
            'source': 'szse_1945',
        })
    return out


def _to_f(v: Any) -> Optional[float]:
    if v is None:
        return None
    t = str(v).strip().replace(',', '')
    try:
        return float(t)
    except ValueError:
        return None


class EtfScaleManager:
    """ETF 份额日度（沪深两所）→ etf_scale_daily"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        review = self.config.get('review_sync', {}) or {}
        ec = review.get('etf_scale', {}) or {}
        self.backfill_days = int(ec.get('backfill_days', 250))
        self.type_filter = [str(x) for x in (ec.get('stock_type_filter') or [])]
        self.sse = HttpCollectorBase(
            self.config, 'sse_etf', interval_seconds=1.0,
            referer='https://www.sse.com.cn/')
        self.szse = HttpCollectorBase(
            self.config, 'szse_etf', interval_seconds=1.0,
            referer='https://www.szse.cn/')

    # ---------- 上交所（STAT_DATE 幂等，可回补历史） ----------

    def fetch_sse(self, stat_date: str) -> List[Dict[str, Any]]:
        params = dict(_SSE_PAGE_PARAMS)
        params['sqlId'] = _SSE_SQL_ID
        params['STAT_DATE'] = f'{stat_date[:4]}-{stat_date[4:6]}-{stat_date[6:]}'
        payload = self.sse.get_json(_SSE_URL, params=params)
        rows = parse_sse_rows(payload, stat_date)
        if self.type_filter:
            rows = [r for r in rows if r['etf_type'] in self.type_filter]
        return rows

    # ---------- 深交所（当日快照，PAGENO 翻页） ----------

    def fetch_szse(self, stat_date: str) -> List[Dict[str, Any]]:
        all_rows: List[Dict[str, Any]] = []
        page = 1
        while page <= 100:  # 防失控上限（749 只 / 每页 10 ≈ 75 页）
            payload = self.szse.get_json(
                _SZSE_URL, params={'SHOWTYPE': 'JSON', 'CATALOGID': '1945',
                                   'TABKEY': 'tab1', 'PAGENO': page,
                                   f'random{page}': '0.1'})
            data = payload[0] if isinstance(payload, list) and payload else None
            if not data:
                break
            rows = parse_szse_rows(data.get('data') or [], stat_date)
            all_rows.extend(rows)
            total = int(data.get('metadata', {}).get('recordcount') or 0)
            fetched = page * int(data.get('metadata', {}).get('pagesize') or 10)
            if not rows or fetched >= total:
                break
            page += 1
        return all_rows

    # ---------- 主流程 ----------

    def execute(self, start_date: Optional[str] = None,
                end_date: Optional[str] = None,
                write_report: bool = True) -> Dict[str, Any]:
        """上交所按区间逐日（STAT_DATE 幂等）；深交所仅抓最新快照（当日/最近交易日）"""
        started = datetime.now()
        end = (datetime.strptime(end_date, '%Y%m%d').date() if end_date
               else datetime.now().date())
        if start_date:
            start = datetime.strptime(start_date, '%Y%m%d').date()
        else:
            # P0-3 首跑修正：DatabaseConnection 无 _query，用 execute_query（dict 行）
            rows = self.db.execute_query(
                "SELECT MAX(trade_date) AS max_date FROM etf_scale_daily "
                "WHERE exchange='sse'")
            last = (rows[0]['max_date'] if rows and rows[0]['max_date']
                    else None)
            start = ((datetime.strptime(str(last), '%Y%m%d').date() + timedelta(days=1))
                     if last else end - timedelta(days=self.backfill_days))
        stats: Dict[str, Any] = {
            'start': start.strftime('%Y%m%d'), 'end': end.strftime('%Y%m%d'),
            'sse_days': 0, 'sse_rows': 0, 'szse_rows': 0, 'errors': [],
        }
        result: Dict[str, Any] = {'success': True, 'stats': stats, 'errors': stats['errors']}
        try:
            # 上交所逐日回补/增量（限流方案 §1：明确被拒即停止后续请求——
            # 2026-11-19 实测曾逐日吞风控、以 1s 间隔继续打满剩余日期）
            day = start
            while day <= end:
                ymd = day.strftime('%Y%m%d')
                try:
                    rows = self.fetch_sse(ymd)
                    if rows:
                        self.db.upsert_etf_scale(rows)
                        stats['sse_days'] += 1
                        stats['sse_rows'] += len(rows)
                except ReviewBlockedError as e:
                    mark_blocked(result, e, f'官网风控急停（sse {ymd} 起 {stats["sse_days"]} 日'
                                            f'已入库，余下区间待补）: {e}')
                    break
                except Exception as e:
                    stats['errors'].append(f'sse {ymd}: {e}')
                day += timedelta(days=1)
            if not result.get('blocked'):
                # 深交所最新快照（仅当日；历史靠日常增量积累）
                try:
                    szse_rows = self.fetch_szse(end.strftime('%Y%m%d'))
                    if szse_rows:
                        self.db.upsert_etf_scale(szse_rows)
                    stats['szse_rows'] = len(szse_rows)
                except ReviewBlockedError as e:
                    mark_blocked(result, e, f'官网风控急停（szse 快照，已入库数据保留）: {e}')
                except Exception as e:
                    stats['errors'].append(f'szse: {e}')
            # F2（方案 V1.0 §F2）：自报数据日（etf_scale_daily 当日 max），供编排层数据日校验
            try:
                r = self.db.execute_query('SELECT MAX(trade_date) AS mx FROM etf_scale_daily')
                stats['data_date_max'] = str(r[0]['mx']) if r and r[0].get('mx') else None
            except Exception:
                stats['data_date_max'] = None
        except ReviewBlockedError as e:
            mark_blocked(result, e, f'风控急停: {e}')
        finally:
            stats['duration'] = round((datetime.now() - started).total_seconds(), 1)
            if result.get('blocked'):
                result['success'] = False  # 急停轮不视为成功（已入库分区保留，余下待补）
            if write_report:
                lines = [
                    f"- 区间：{stats['start']} ~ {stats['end']}；上交所 {stats['sse_days']} 日"
                    f" {stats['sse_rows']} 行；深交所快照 {stats['szse_rows']} 行",
                    '- 口径：份额单位万份；上交所 STAT_DATE 幂等可回补；深交所仅当日'
                    '快照（历史靠每日增量积累）；份额日变化=申赎方向代理',
                ]
                lines.extend(f"- 错误：{e}" for e in stats['errors'][:5])
                result['report_path'] = write_review_report(
                    'ETF 份额同步报告', 'etf_scale_sync_report',
                    '成功' if result['success'] else '失败', lines,
                    duration_seconds=stats['duration'])
        return result
