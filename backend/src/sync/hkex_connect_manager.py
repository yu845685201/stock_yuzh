"""P3 互联互通资金采集（量价资金方案 V1.1 §5.1）：港交所每日统计 JSON →
hkex_connect_daily（北向总额 + 南向净额，一表四行/日）。

源：https://www.hkex.com.hk/eng/csm/DailyStat/data_tab_daily_YYYYMMDDe.js
（页面隐藏字段 hidCSMDailyStatJsonPath 揭示的静态 JSON，按交易日一文件；
实测 2026-10-05：Northbound 有 Total Turnover/Trade Count/ETF Turnover，
Buy/Sell 分项与 DQB 停披露（占位）；Southbound 有完整 Buy/Sell Turnover）。

口径红线：
- 北向仅成交总额（买+卖），**禁止解读为净流入/流出**；
- 文件滚动保留约 2 个月（实测 0401 可得 / 0315 起 404）——每日增量必须坚持，
  漏采即永久丢失；
- 南向净买入 = Buy − Sell（真净额）；A 股休市日 Southbound=0 为真实状态
  （港股通关闭），不是缺数据。
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ..data_sources.http_collector_base import (HttpCollectorBase,
                                                ReviewBlockedError)
from ..data_sources.hsgt_detail_source import (HsgtDetailSource,
                                               parse_hold_row)
from ..database.connection import DatabaseConnection
from ..config.config_manager import ConfigManager
from ..sync.review_common import write_review_report

logger = logging.getLogger(__name__)

_HKEX_URL = ('https://www.hkex.com.hk/eng/csm/DailyStat/'
             'data_tab_daily_{yyyymmdd}e.js')

_CHANNELS = ('north_sse', 'north_szse', 'south_sse', 'south_szse')
# 文件 market 段名 → channel
_MARKET_MAP = {
    'SSE Northbound': 'north_sse',
    'SZSE Northbound': 'north_szse',
    'SSE Southbound': 'south_sse',
    'SZSE Southbound': 'south_szse',
}


def _num(text: str) -> Optional[float]:
    """'101,257.88' → 101257.88；'-' / 占位 → None"""
    t = str(text).strip().replace(',', '')
    if not t or t in ('-', '999,999,999', '999999999'):
        return None
    try:
        return float(t)
    except ValueError:
        return None


def parse_daily_stat(raw: str) -> List[Dict[str, Any]]:
    """港交所日度统计 JS 文本 → 4 行 channel 记录（纯函数，单测覆盖）。

    文件格式：``tabData = [ ... ]``；每段 market ∈ _MARKET_MAP，content[0].table
    的 schema/tr 为成交表（北向 4 列：Total/Count/DQB/ETF；南向 7 列含 Buy/Sell）。
    北向 buy/sell 恒 None（停披露不硬造）；南向按 schema 列序取 Buy/Sell。
    """
    m = re.search(r'tabData\s*=\s*(\[.*\])\s*;?\s*$', raw, re.S)
    if not m:
        raise ValueError('响应不含 tabData（页面结构变更或非交易日 404 文本）')
    import json
    data = json.loads(m.group(1))
    date_of_file = None
    rows: Dict[str, Dict[str, Any]] = {}
    for seg in data:
        market = str(seg.get('market') or '')
        channel = _MARKET_MAP.get(market)
        if not channel:
            continue
        date_of_file = str(seg.get('date') or '').replace('-', '') or date_of_file
        tables = seg.get('content') or []
        if not tables:
            continue
        table = tables[0].get('table') or {}
        schema = (table.get('schema') or [[]])[0]
        trs = table.get('tr') or []
        # 成交表：每列一个 tr（td[0] 为该列值）
        cols = {name: (_num(trs[i]['td'][0][0]) if i < len(trs) and trs[i]['td'] else None)
                for i, name in enumerate(schema)}
        rows[channel] = {
            'channel': channel,
            'total_turnover': cols.get('Total Turnover'),
            'buy_turnover': cols.get('Buy Turnover'),
            'sell_turnover': cols.get('Sell Turnover'),
            'etf_turnover': cols.get('ETF Turnover'),
            'trade_count': cols.get('Total Trade Count'),
        }
    out: List[Dict[str, Any]] = []
    for ch in _CHANNELS:
        row = rows.get(ch) or {'channel': ch, 'total_turnover': None,
                               'buy_turnover': None, 'sell_turnover': None,
                               'etf_turnover': None, 'trade_count': None}
        row['trade_date'] = date_of_file
        row['source'] = 'hkex_dailystat'
        out.append(row)
    return out


class HkexConnectManager:
    """港交所互联互通日度数据（北向总额 + 南向净额）→ hkex_connect_daily"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        review = self.config.get('review_sync', {}) or {}
        hc = review.get('hkex_connect', {}) or {}
        self.backfill_days = int(hc.get('backfill_days', 60))
        self.collector = HttpCollectorBase(
            self.config, 'hkex_connect', interval_seconds=1.0,
            referer='https://www.hkex.com.hk/')

    # ---------- 单日抓取 ----------

    def _fetch_day(self, yyyymmdd: str) -> Optional[List[Dict[str, Any]]]:
        """单日文件抓取与解析；404（非交易日/超出保留期）返回 None"""
        url = _HKEX_URL.format(yyyymmdd=yyyymmdd)
        # allow_status 放行 404：非交易日/超滚动保留期的文件不存在属常态（返回 None）
        text = self.collector.get_text(url, allow_status=(404,))
        if 'tabData' not in text:
            logger.info(f'hkex {yyyymmdd}: 无文件（非交易日或超保留期）')
            return None
        rows = parse_daily_stat(text)
        # 用文件内日期（文件生成日的港股交易日）而非请求日
        fdate = rows[0].get('trade_date') if rows else None
        if not fdate:
            return None
        for r in rows:
            r['trade_date'] = fdate
        return rows

    def execute(self, start_date: Optional[str] = None,
                end_date: Optional[str] = None,
                write_report: bool = True) -> Dict[str, Any]:
        """区间采集（缺省 = 自最近库内数据日起增量；空库回补 backfill_days）"""
        started = datetime.now()
        end = (datetime.strptime(end_date, '%Y%m%d').date() if end_date
               else datetime.now().date())
        if start_date:
            start = datetime.strptime(start_date, '%Y%m%d').date()
        else:
            # P0-3 首跑修正：DatabaseConnection 无 _query，用 execute_query（dict 行）
            latest = self.db.execute_query(
                "SELECT MAX(trade_date) AS max_date FROM hkex_connect_daily")
            last = (latest[0]['max_date'] if latest and latest[0]['max_date']
                    else None)
            if last:
                start = (datetime.strptime(str(last), '%Y%m%d').date()
                         + timedelta(days=1))
            else:
                start = end - timedelta(days=self.backfill_days)
        stats: Dict[str, Any] = {
            'start': start.strftime('%Y%m%d'), 'end': end.strftime('%Y%m%d'),
            'days_fetched': 0, 'days_empty': 0, 'rows': 0,
            'dates': [],
        }
        result: Dict[str, Any] = {'success': True, 'stats': stats, 'errors': []}
        try:
            day = start
            while day <= end:
                ymd = day.strftime('%Y%m%d')
                rows = self._fetch_day(ymd)
                if rows:
                    # 该文件实际日期可能 ≠ 请求日（节假日跳日）——按文件日期入库
                    self.db.upsert_hkex_connect(rows)
                    stats['days_fetched'] += 1
                    stats['rows'] += len(rows)
                    stats['dates'].append(rows[0]['trade_date'])
                else:
                    stats['days_empty'] += 1
                day += timedelta(days=1)
        except ReviewBlockedError as e:
            result['success'] = False
            result['errors'].append(f'风控急停: {e}')
        except Exception as e:
            logger.exception('hkex_connect 采集失败')
            result['success'] = False
            result['errors'].append(str(e))
        finally:
            # F2（方案 V1.0 §F2）：自报数据日（hkex_connect_daily 当日 max）
            try:
                r = self.db.execute_query('SELECT MAX(trade_date) AS mx FROM hkex_connect_daily')
                stats['data_date_max'] = str(r[0]['mx']) if r and r[0].get('mx') else None
            except Exception:
                stats['data_date_max'] = None
            stats['duration'] = round((datetime.now() - started).total_seconds(), 1)
            result['report_path'] = self._write_report(result) if write_report else None
        return result

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 区间：{stats['start']} ~ {stats['end']}；抓到 {stats['days_fetched']} 个交易日"
            f" / 空日 {stats['days_empty']}；入库 {stats['rows']} 行",
            '- 口径：北向仅成交总额（买+卖，停披露分项），禁止解读净额；'
            '南向净买入=Buy−Sell；A 股休市日南向 0=港股通关闭（真实状态）',
        ]
        if result['errors']:
            lines.append(f"- 错误：{result['errors']}")
        return write_review_report(
            '互联互通（北向+南向）同步报告', 'hkex_connect_sync_report',
            '成功' if result['success'] else '失败', lines,
            duration_seconds=stats.get('duration'))


# ---------------------------------------------------------------------------
# P3 北向个股明细（第三阶段-3，方案 §5.1 第 6 项；东财 datacenter 直连，
# 不依赖 akshare）。与上方 HkexConnectManager（港交所总额口径）同文件不同类：
# 总额仍在每日发布（买+卖），个股明细 2024-08-19 起停止每日披露（真实口径
# 变化，见 hsgt_detail_source 文件头）——两层口径报告层分工标注。
# ---------------------------------------------------------------------------


class NorthFlowDetailManager:
    """北向个股明细：东财 → north_flow_detail_daily（个股全量历史 + 日增量重拉）。"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        review = self.config.get('review_sync', {}) or {}
        self.nf_cfg = review.get('north_flow_detail') or {}
        self.source = HsgtDetailSource(self.config)

    # ---------- 单只/批量拉取 ----------

    def pull_stock(self, code6: str, dry_run: bool = False) -> Dict[str, Any]:
        """单只股票全量历史拉取入库（沪/深通道按 (trade_date, ts_code) 聚合不双计）。"""
        raw = self.source.fetch_stock_history(code6)
        rows = [r for r in (parse_hold_row(x, code6) for x in raw) if r]
        merged = self._merge_channels(rows)
        if merged and not dry_run:
            # 行数报处理行数——execute_batch rowcount 只反映末条语句（ON CONFLICT
            # 语义），不可作入库量；实际入库以查库为准
            self.db.upsert_north_flow_detail(merged)
        return {'code': code6, 'raw_rows': len(raw), 'merged_rows': len(merged),
                'latest_date': max((r['trade_date'] for r in merged), default=None)}

    def execute(self, trade_date: Optional[str] = None, dry_run: bool = False,
                write_report: bool = True,
                symbols: Optional[List[str]] = None) -> Dict[str, Any]:
        """日增量：对目标股票重拉（发现新发布日即入库）。

        默认目标 = 上次明细中**有持仓的股票**（库内 distinct ts_code，封顶
        review_sync.north_flow_detail.max_symbols 默认 300）；symbols 显式
        指定时按指定列表。披露停止后（2024-08-19 起）重拉自然 no_data——
        如实记录，不当故障。
        """
        started = datetime.now()
        targets = symbols or self._active_holder_codes()
        stats: Dict[str, Any] = {'requested': len(targets), 'pulled': 0,
                                 'new_rows': 0, 'failed': [],
                                 'max_date_before': self.db.fetch_north_flow_detail_max_date()}
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': []}
        try:
            for code6 in targets:
                try:
                    r = self.pull_stock(code6, dry_run=dry_run)
                except Exception as e:
                    logger.warning('北向明细 %s 拉取失败: %r', code6, e)
                    stats['failed'].append(code6)
                    continue
                stats['pulled'] += 1
                stats['new_rows'] += r['merged_rows']
            result['success'] = stats['pulled'] > 0
            if not targets:
                result['errors'].append('无目标股票（库内无明细且未指定 symbols）')
        except Exception as e:
            logger.exception('北向明细采集失败: %r', e)
            result['errors'].append(str(e))
        finally:
            stats['duration'] = round((datetime.now() - started).total_seconds(), 1)
            stats['max_date_after'] = self.db.fetch_north_flow_detail_max_date()
            result['report_path'] = (self._write_report(result)
                                     if write_report else None)
        return result

    # ---------- 历史回填 ----------

    def backfill_range(self, start_date: str, end_date: str,
                       symbols: Optional[List[str]] = None,
                       dry_run: bool = False,
                       suppress_report: bool = True) -> Dict[str, Any]:
        """按股票列表回填全量历史（断点只信查库：已有 (trade_date, ts_code) 跳过）。

        默认 symbols = 全部在市股票（base_stock_info list_status='L'）——一次性
        重活（约 5800 次请求 × 1s 间隔 ≈ 100 分钟量级，仿两融回填的夜间批处理
        模式）；可传 symbols 做子集回填。
        """
        started = datetime.now()
        codes = symbols or self._all_listed_codes()
        have = set(self.db.fetch_north_flow_detail_dates(start_date, end_date))
        stats: Dict[str, Any] = {
            'range': f'{start_date}~{end_date}', 'symbols_total': len(codes),
            'symbols_done': 0, 'symbols_failed': [], 'rows': 0,
            'have_dates': len(have), 'last_completed_code': None,
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats,
                                  'errors': [], 'backfill': True}
        for code6 in codes:
            try:
                r = self.pull_stock(code6, dry_run=dry_run)
            except Exception as e:
                logger.warning('北向明细 %s 回填失败: %r', code6, e)
                stats['symbols_failed'].append(code6)
                continue
            if r['merged_rows']:
                stats['rows'] += r['merged_rows']
            stats['symbols_done'] += 1
            stats['last_completed_code'] = code6
        result['success'] = not stats['symbols_failed']
        stats['duration'] = round((datetime.now() - started).total_seconds(), 1)
        result['report_path'] = (None if suppress_report
                                 else self._write_backfill_report(result))
        return result

    # ---------- 报告层查询 ----------

    def fetch_top_as_of(self, trade_date: str, limit: int = 10
                        ) -> List[Dict[str, Any]]:
        """某日北向持股市值 Top N（报告层活跃度锚；含 ts_code 供名称回查）"""
        return self.db.fetch_north_flow_detail_top(trade_date, limit=limit)

    def fetch_max_date(self) -> Optional[str]:
        return self.db.fetch_north_flow_detail_max_date()

    # ---------- 内部 ----------

    def _merge_channels(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """沪/深双通道聚合：同 (trade_date, ts_code) 持仓与市值求和、占比取大。

        明细行带 channel/stock_name（不入库列），聚合后剥掉——入库列契约见
        dao.upsert_north_flow_detail。
        """
        merged: Dict[str, Dict[str, Any]] = {}
        for r in rows:
            key = (r['trade_date'], r['ts_code'])
            cur = merged.get(key)
            if cur is None:
                cur = {'trade_date': r['trade_date'], 'ts_code': r['ts_code'],
                       'hold_shares': 0.0, 'hold_market_value': 0.0,
                       'hold_ratio': None, 'change_shares': 0.0,
                       'change_amount': 0.0}
                merged[key] = cur
            cur['hold_shares'] += r.get('hold_shares') or 0.0
            cur['hold_market_value'] += r.get('hold_market_value') or 0.0
            cur['change_shares'] += r.get('change_shares') or 0.0
            cur['change_amount'] += r.get('change_amount') or 0.0
            ratio = r.get('hold_ratio')
            if ratio is not None:
                cur['hold_ratio'] = max(cur['hold_ratio'] or 0.0, ratio)
        return list(merged.values())

    def _active_holder_codes(self) -> List[str]:
        """库内已有持仓的股票代码（6 位；日增量重拉目标）"""
        rows = self.db.execute_query(
            "SELECT DISTINCT ts_code FROM north_flow_detail_daily "
            "ORDER BY ts_code")
        codes = [str(r['ts_code']).split('.')[-1] for r in rows]
        cap = int(self.nf_cfg.get('max_symbols', 300) or 300)
        return codes[:cap]

    def _all_listed_codes(self) -> List[str]:
        rows = self.db.execute_query(
            "SELECT stock_code FROM base_stock_info WHERE list_status = 'L' "
            "ORDER BY stock_code")
        return [str(r['stock_code']) for r in rows]

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 目标股票 {stats['requested']} 只，实拉 {stats['pulled']} 只，"
            f"累计明细行 {stats['new_rows']}",
            f"- 库内最新数据日：{stats.get('max_date_before')} → "
            f"{stats.get('max_date_after')}",
            '- 口径：个股明细披露止于 2024-08-16（交易所停止每日披露，真实口径'
            '变化）；此后日增量重拉自然无新数据——非故障',
            '- 双通道聚合：沪/深股通同股持仓求和、占比取大，不双计',
        ]
        if stats['failed']:
            lines.append(f"- 失败股票（可重跑续补）: {stats['failed'][:20]}")
        if result['errors']:
            lines.append(f"- 提示：{result['errors']}")
        status = '成功' if result['success'] else '失败/无目标（待查）'
        return write_review_report('北向个股明细同步报告',
                                   'north_flow_detail_sync_report', status,
                                   lines,
                                   duration_seconds=stats.get('duration'))

    def _write_backfill_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 区间：{stats['range']}；股票 {stats['symbols_total']} 只："
            f"完成 {stats['symbols_done']}、失败 {len(stats['symbols_failed'])}",
            f"- 累计明细行：{stats['rows']}（末完成股票 {stats['last_completed_code']}）",
            f"- 区间内已有数据日 {stats['have_dates']} 个（断点参考）",
            "- 断点续跑口径：只信查库（已有 (trade_date, ts_code) upsert 幂等）",
        ]
        if stats['symbols_failed']:
            lines.append(f"- 失败股票清单（可重跑续补）: "
                         f"{stats['symbols_failed'][:30]}")
        status = '成功' if result['success'] else '部分失败（可重跑续补）'
        return write_review_report('北向个股明细历史回填报告',
                                   'north_flow_detail_backfill_report', status,
                                   lines, duration_seconds=stats.get('duration'))
