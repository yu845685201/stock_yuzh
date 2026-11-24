"""
融资融券同步（方案 4.11，M3；沪深交易所官网每日披露，T-1 日数据）

接口事实（2026-09-27 实抓锁定，详见 sse_szse_source 文件头）：
- 深交所：1837_xxpl tab2（txtDate=当日；融资买入额/融资余额亿元、融券卖出量/余量万股、
  融券余额万元；无融资偿还额列 → 留空）
- 上交所：RZRQ_MX_INFO（全市场单日 ~2000 条；rzye/rzmre/rzche 元、rqyl 融券余量；
  无融券余额金额列 → 留空）
- 官网保留历史，次日并抓；复盘报告两融章节必须标注"数据日期 T-1"
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ..config.config_manager import ConfigManager
from ..data_sources.http_collector_base import ReviewBlockedError
from ..data_sources.sse_szse_source import SzseSseSource
from ..database.connection import DatabaseConnection
from .review_common import (code6_to_ts_code, expand_trade_days, mark_blocked,
                            write_review_report)

logger = logging.getLogger(__name__)


class MarginManager:
    """两融：官网分页明细 -> margin_trade_daily"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        self.source = SzseSseSource(self.config)
        review = self.config.get('review_sync', {}) or {}
        self.lookback_days = int(review.get('margin_lookback_days', 5))

    def execute(self, trade_date: Optional[str] = None, dry_run: bool = False,
                write_report: bool = True,
                markets: Optional[set] = None) -> Dict[str, Any]:
        """拉取 trade_date（缺省 T-1）全市场两融明细。

        交易所发布滞后时逐日回看（默认最多 5 天），沪深独立回补到各自最近有数据日，
        报告如实标注每所数据日期（复盘两融章节标注"数据日期 T-N"）。

        markets（限流方案 20261120 §4.2）：限定采集的交易所子集（{'szse'/'sse'}，
        缺省双所）。沪深各自解析成功即各自入库——单所被拒不再丢弃已抓另一所
        （2026-11-19 实测：深市 2,119 行已抓、沪市 400 整批入库为零）；
        补采以"目标市场+日期"为单位，只补缺失侧。
        """
        started = datetime.now()
        if trade_date:
            target = datetime.strptime(trade_date, '%Y%m%d').date()
        else:
            target = datetime.now().date() - timedelta(days=1)
        date_str = target.strftime('%Y%m%d')
        want_szse = markets is None or 'szse' in markets
        want_sse = markets is None or 'sse' in markets
        stats: Dict[str, Any] = {
            'data_date': date_str, 'szse_date': None, 'sse_date': None,
            'szse_rows': 0, 'sse_rows': 0, 'rows': 0, 'missing': [],
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
        blocked: Dict[str, Exception] = {}
        rows: List[Dict[str, Any]] = []
        try:
            # 深交所（单位换算：亿元×1e8；融券余额万元×1e4）——发布滞后时逐日回看
            if want_szse:
                for offset in range(self.lookback_days):
                    day = (target - timedelta(days=offset)).strftime('%Y%m%d')
                    try:
                        batch = self._szse_rows(day)
                    except ReviewBlockedError as e:
                        blocked['szse'] = e
                        logger.warning(f'两融深市风控急停 {day}: {e}')
                        break
                    if batch:
                        rows.extend(batch)
                        stats['szse_date'] = day
                        stats['szse_rows'] = len(batch)
                        if not dry_run:
                            # 单所解析成功即入库（§4.2：沪深可分别成功、分别待补）
                            self.db.upsert_margin_trades(batch)
                        break

            # 上交所（元原样；无融券余额金额列 → rqye 留空）
            if want_sse:
                for offset in range(self.lookback_days):
                    day = (target - timedelta(days=offset)).strftime('%Y%m%d')
                    try:
                        batch = self._sse_rows(day)
                    except ReviewBlockedError as e:
                        blocked['sse'] = e
                        logger.warning(f'两融沪市风控急停 {day}: {e}')
                        break
                    if batch:
                        rows.extend(batch)
                        stats['sse_date'] = day
                        stats['sse_rows'] = len(batch)
                        if not dry_run:
                            self.db.upsert_margin_trades(batch)
                        break
            stats['rows'] = len(rows)

            # 行数下限校验（两融全市场 >3000 标的，方案 5.4；单所模式不判总量）
            if 0 < len(rows) < 3000 and not blocked:
                logger.warning(f'两融仅 {len(rows)} 行，低于历史日均 3000，'
                               f'疑似数据不全（仍入库，报告标注）')
                stats['missing'].append(f'行数 {len(rows)} < 3000')

            if blocked:
                # 单/双所被拒：已入库侧保留，被拒侧待补（§4.2 部分完成如实呈现）
                ok_parts, miss_parts = [], []
                for m, label in (('szse', '深市'), ('sse', '沪市')):
                    if m in blocked:
                        miss_parts.append(f'{label}风控急停待补')
                    elif stats.get(f'{m}_rows'):
                        ok_parts.append(f'{label}已入库 {stats[f"{m}_rows"]} 行')
                detail = '；'.join(ok_parts + miss_parts) or '风控急停'
                first_err = blocked.get('szse') or blocked.get('sse')
                mark_blocked(result, first_err,
                             f'官网风控急停（{detail}，稍后重跑补）: {first_err}')
            elif not rows:
                result['errors'].append(
                    f'{date_str} 起回看 {self.lookback_days} 日沪深两融均无数据（非交易日或未发布）')
            result['success'] = bool(rows) and not blocked
        except Exception as e:
            logger.exception(f'两融采集失败: {e}')
            result['errors'].append(str(e))
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            # X-19：回填模式逐日报告抑制，结束后由 backfill_range 汇总
            result['report_path'] = self._write_report(result) if write_report else None
        return result

    # ---------- 历史区间回填（开发项 C） ----------

    def backfill_range(self, start_date: str, end_date: str, dry_run: bool = False,
                       suppress_report: bool = True) -> Dict[str, Any]:
        """按交易日历展开 [start_date, end_date] 逐日回填（开发项 C，20230101 起实测 34h 量级）。

        - **断点续跑只信查库（X-14 红线）**：margin_trade_daily 当日行数 > 0 跳过。
          不得信任单次调用的 stats——margin 内置"发布滞后逐日回看"，显式 --trade-date d
          而官网 d 未就绪时会静默采到 d-1（行数计入本次调用，误导"当日已成功"）；
        - 数据日期核对：逐日记录 stats 的 szse_date/sse_date 实际数据日期，与请求日
          不一致者汇总进报告（回看机制正常行为，如实标注）；
        - 单日失败不阻塞（失败清单可重跑续补）；风控急停中止并记续跑点；
        - 游标：margin 无 sync_cursor，回补不影响增量语义；
        - 报告（X-19）：suppress_report=True 时逐日不落盘，结束写一份汇总；
        - 限速：沿用现有 sse_szse_interval（1.5s）——实测 165s/日，深市官网 pagesize=20
          固定、提速空间有限，不建议调整（方案 §六 风险表）。
        """
        started = datetime.now()
        days = expand_trade_days(self.db, start_date, end_date)
        stats: Dict[str, Any] = {
            'range': f'{start_date}~{end_date}', 'days_total': len(days),
            'days_done': 0, 'days_skipped': 0, 'rows': 0,
            'days_failed': [], 'date_notes': [], 'last_completed_date': None,
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [],
                                  'blocked': False, 'backfill': True}
        for d in days:
            # 断点续跑按交易所拆分（限流方案 §4.2：以目标市场+日期为补采单位，
            # 已完整分区跳过；单所缺失只补单所）。X-14 语义不变：只信查库。
            if not dry_run:
                counts = self.db.fetch_margin_row_counts_by_exchange(d)
                missing = {m for m in ('szse', 'sse') if not counts.get(m)}
                if not missing:
                    stats['days_skipped'] += 1
                    continue
            else:
                missing = None
            r = self.execute(trade_date=d, dry_run=dry_run, write_report=False,
                             markets=missing)
            if r.get('blocked'):
                result['blocked'] = True
                result['errors'].append(f'风控急停于 {d}，已完成 {stats["days_done"]} 日，'
                                        f'续跑点 --start-date {d}')
                break
            if r['success']:
                stats['days_done'] += 1
                stats['rows'] += r['stats']['rows']
                stats['last_completed_date'] = d
                # X-14 数据日期核对：回看采到非请求日时如实记录
                szse_d, sse_d = r['stats'].get('szse_date'), r['stats'].get('sse_date')
                if szse_d != d or sse_d != d:
                    stats['date_notes'].append(
                        {'requested': d, 'szse_date': szse_d, 'sse_date': sse_d})
            else:
                stats['days_failed'].append({'date': d, 'errors': r['errors'][:2]})
        result['success'] = not result['blocked'] and not stats['days_failed']
        result['duration'] = (datetime.now() - started).total_seconds()
        result['report_path'] = self._write_backfill_report(result)
        return result

    def _write_backfill_report(self, result: Dict[str, Any]) -> Optional[str]:
        """回填汇总报告（X-19：整个区间只写一份）"""
        stats = result['stats']
        lines = [
            f"- 区间：{stats['range']}（交易日 {stats['days_total']}：回填 {stats['days_done']}、"
            f"跳过 {stats['days_skipped']}、失败 {len(stats['days_failed'])}）",
            f"- 累计入库：{stats['rows']} 行（末完成日 {stats['last_completed_date']}）",
            "- 断点续跑口径（X-14）：只信查库（margin_trade_daily 当日行数>0 跳过），"
            "不信任单次调用 stats——回看机制会静默采到 d-1",
        ]
        if stats['date_notes']:
            shown = stats['date_notes'][:20]
            lines.append(f"- 数据日期核对（请求日≠数据日，回看行为；共 {len(stats['date_notes'])} 日，"
                         f"至多展示 20）: {shown}")
        if result['blocked']:
            lines.append(f"- ⚠ 风控急停：{result['errors']}")
        if stats['days_failed']:
            lines.append(f"- 失败日期清单（可重跑续补）: "
                         f"{[f['date'] for f in stats['days_failed'][:30]]}")
        status = '风控急停（部分完成，可续跑）' if result['blocked'] else \
            ('成功' if result['success'] else '部分失败（失败清单见上，可重跑续补）')
        return write_review_report('两融历史回填报告', 'margin_backfill_report',
                                   status, lines, result.get('duration'))

    # ---------- 每所行构造（回看复用） ----------

    def _szse_rows(self, day: str) -> List[Dict[str, Any]]:
        rows = []
        for item in self.source.fetch_szse_margin(day):
            code = str(item.get('zqdm') or '')
            if not code:
                continue
            rzmre = _num(item.get('jrrzmr'), 1e8)
            rzye = _num(item.get('jrrzye'), 1e8)
            rqye = _num(item.get('jrrjye'), 1e4)
            if rzye is None and rzmre is None:
                continue  # 脏行剔除
            rows.append({
                'trade_date': day,
                'ts_code': code6_to_ts_code(code),
                'rzye': rzye,
                'rzmre': rzmre,
                'rzche': None,   # 深交所明细无融资偿还额列
                'rqye': rqye,
            })
        return rows

    def _sse_rows(self, day: str) -> List[Dict[str, Any]]:
        rows = []
        for item in self.source.fetch_sse_margin(day):
            code = str(item.get('stockCode') or '')
            if not code:
                continue
            rzye = _num(item.get('rzye'))
            rzmre = _num(item.get('rzmre'))
            if rzye is None and rzmre is None:
                continue
            rows.append({
                'trade_date': day,
                'ts_code': code6_to_ts_code(code),
                'rzye': rzye,
                'rzmre': rzmre,
                'rzche': _num(item.get('rzche')),
                'rqye': None,
            })
        return rows

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 数据日期：深交所 {stats.get('szse_date') or '无'} / "
            f"上交所 {stats.get('sse_date') or '无'}（T-{stats.get('lookback', '')} 口径按实际标注；"
            f"交易所次日盘前发布）",
            f"- 深交所 {stats['szse_rows']} 行 / 上交所 {stats['sse_rows']} 行，合计 {stats['rows']}",
            f"- 单位：元（深交所原始亿元、融券余额万元已换算；偿还额深交所无该列）",
        ]
        if stats['missing']:
            lines.append(f"- 校验提示：{stats['missing']}")
        if result['errors']:
            lines.append(f"- 提示：{result['errors']}")
        status = '成功' if result['success'] else '失败（待补）'
        return write_review_report('融资融券同步报告', 'margin_trade_sync_report',
                                   status, lines, result.get('duration'))


def _num(value: Any, scale: float = 1.0) -> Optional[float]:
    if value is None:
        return None
    s = str(value).replace(',', '').strip()
    if s in ('', '--', '-'):
        return None
    try:
        return float(s) * scale
    except ValueError:
        return None
