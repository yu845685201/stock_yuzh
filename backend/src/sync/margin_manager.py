"""
融资融券同步（方案 4.11，M3；盘后采集数据源整合详细技术方案 V1.0 §4.1 三源改造）

源路径（2026-11-20 实测锁定，详见 sse_szse_source / cninfo_source 文件头）：
- 沪市 queryMargin.do：tabType=mxtype、detailsDate=D、pageHelp 分页
  （pageNo=beginPage=endPage=n 全带前缀）；行带 opDate 独立日期证据；
  11-20 为 2,000+19 两页合计约 1.1s。rqyl 是余量，不作金额。
- 深市 1837_xxpl XLSX 整表导出：一份 ~145KB / 2,119 行（替代旧 JSON 106 页）；
  文件无日期列——用带日期的巨潮同日代码集和可比金额验证导出归属；
  该文件余额/买入额/融券余额列已是元（原 JSON 的亿元/万元换算仅用于旧路径）。
- 巨潮 data20/marginTrading/detailList：单次全市场 4,491 条带 TRADEDATE
  （沪深 4,138 条，北交所记录进范围统计不落沪深表）；官网某市场失败时按市场
  独立入库，沪市 rqye 缺列时不清空巨潮已验证金额（upsert COALESCE 语义）。

日期语义（§2.1）：显式 trade_date 直采该业务日不回看；缺省取
"发布日（今天）的上一交易日"（两融 T 日数据 T+1 盘前发布）——不再退回自然日-1；
历史回填断点续跑只信查库（X-14 红线不变）。
"""

import logging
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

from ..config.config_manager import ConfigManager
from ..data_sources.cninfo_source import CninfoBlockedError, CninfoSource
from ..data_sources.http_collector_base import ReviewBlockedError
from ..data_sources.sse_szse_source import SzseSseSource
from ..database.connection import DatabaseConnection
from .review_common import (code6_to_ts_code, expand_trade_days,
                            latest_closed_trade_date, mark_blocked,
                            prev_trade_date, write_review_report)

logger = logging.getLogger(__name__)


def _dec(value: Any) -> Optional[Decimal]:
    """任意数值输入 → Decimal（元；'1,234.56'/'1234'；无效/空返回 None）"""
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    s = str(value).replace(',', '').strip()
    if s in ('', '--', '-'):
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


class MarginManager:
    """两融：沪市分页明细 + 深市 XLSX 整表 + 巨潮校验/补缺 -> margin_trade_daily"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        self.source = SzseSseSource(self.config)
        # 巨潮两融校验/补缺源（§4.1；复用公告源的域门与限速配置）
        cninfo_cfg = dict(self.config.get('data_sources', {}).get('cninfo', {}) or {})
        cninfo_cfg.setdefault('query_base_url', 'http://www.cninfo.com.cn')
        self.cninfo = CninfoSource(cninfo_cfg)
        review = self.config.get('review_sync', {}) or {}
        self.lookback_days = int(review.get('margin_lookback_days', 5))
        # 深市路径开关：XLSX 整表（默认）；false 回退旧 JSON 翻页
        self.szse_xlsx_enabled = bool(review.get('margin_szse_xlsx_enabled', True))
        # 巨潮可用性降级：官网成功时巨潮仅作对账（true，默认）；false 跳过巨潮
        self.cninfo_verify_enabled = bool(review.get('margin_cninfo_verify_enabled', True))

    def execute(self, trade_date: Optional[str] = None, dry_run: bool = False,
                write_report: bool = True, markets: Optional[set] = None,
                explicit_date: bool = False) -> Dict[str, Any]:
        """拉取 trade_date（缺省 = 发布日的上一交易日）全市场两融明细。

        日期语义（§2.1）：显式业务日直采不回看（explicit_date=True 由编排/单源
        CLI 传入）；manager 独立调用缺省保留逐日回看（默认最多 5 天，发布滞后兜底）。
        深市 XLSX 无日期列，巨潮同日代码集验证归属；巨潮不可用时深市保留待核
        （无日期导出缺独立日期证据不入账）。单所失败不丢另一所已验数据。
        """
        started = datetime.now()
        if trade_date:
            target = datetime.strptime(trade_date, '%Y%m%d').date()
        else:
            # 两融 T-1 业务日：发布日（今天）的上一交易日（日历驱动，非自然日差）
            today_compact = datetime.now().strftime('%Y%m%d')
            prev = prev_trade_date(self.db, today_compact)
            if prev:
                target = datetime.strptime(prev, '%Y%m%d').date()
            else:
                target = datetime.now().date() - timedelta(days=1)
        date_str = target.strftime('%Y%m%d')
        # 显式业务日直采（§2.1：单源 --trade-date / 队列领到的都是已解析业务日，
        # 回看会静默采到别的日期——X-14）；独立调用缺省保留回看兜底。
        # lookback_days 属性同样兼容绕过 __init__ 的测试桩
        lookback = 1 if explicit_date else int(getattr(self, 'lookback_days', 5))
        want_szse = markets is None or 'szse' in markets
        want_sse = markets is None or 'sse' in markets
        stats: Dict[str, Any] = {
            'data_date': date_str, 'szse_date': None, 'sse_date': None,
            'szse_rows': 0, 'sse_rows': 0, 'rows': 0, 'missing': [],
            'szse_source': None, 'sse_source': None,
            'cninfo_verify': None, 'cninfo_diffs': [],
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
        blocked: Dict[str, Exception] = {}
        rows: List[Dict[str, Any]] = []
        # 巨潮当日记录（校验源/补缺源；官网全成时仅对账）——一次拉取复用（§3.1）
        # getattr 兜底：绕过 __init__ 的测试桩回退关闭态（无巨潮、旧 JSON 路径）
        cn_rows: Optional[Dict[str, List[Dict[str, Any]]]] = None
        cn_error: Optional[str] = None
        if getattr(self, 'cninfo_verify_enabled', False):
            try:
                cn_rows = self._cninfo_by_market(date_str)
            except (CninfoBlockedError, ReviewBlockedError) as e:
                cn_error = str(e)
                logger.warning(f'巨潮两融 {date_str} 不可用（校验/补缺降级）: {e}')
            except Exception as e:
                cn_error = str(e)
                logger.warning(f'巨潮两融 {date_str} 拉取失败（校验/补缺降级）: {e}')
        try:
            # 深市：XLSX 整表 + 巨潮同日代码集验证归属；官网失败用巨潮补缺
            if want_szse:
                got_szse = False
                for offset in range(lookback):
                    day = (target - timedelta(days=offset)).strftime('%Y%m%d')
                    try:
                        batch, src, verify = self._szse_collect(day, cn_rows)
                    except ReviewBlockedError as e:
                        blocked['szse'] = e
                        logger.warning(f'两融深市风控急停 {day}: {e}')
                        break
                    if batch:
                        got_szse = True
                        rows.extend(batch)
                        stats['szse_date'] = day
                        stats['szse_rows'] = len(batch)
                        stats['szse_source'] = src
                        if verify:
                            stats['cninfo_verify'] = verify
                        if not dry_run:
                            self.db.upsert_margin_trades(batch)
                        break
                if not got_szse and not blocked.get('szse') \
                        and cn_rows and cn_rows.get('szse'):
                    # XLSX 全 lookback 无数据（未发布）但巨潮有带日期深市记录 → 补缺入库
                    if not dry_run:
                        self.db.upsert_margin_trades(cn_rows['szse'])
                    rows.extend(cn_rows['szse'])
                    stats['szse_date'] = date_str
                    stats['szse_rows'] = len(cn_rows['szse'])
                    stats['szse_source'] = 'cninfo'

            # 上交所：queryMargin.do（行带 opDate 独立证据）；失败用巨潮补缺
            if want_sse:
                got_sse = False
                for offset in range(lookback):
                    day = (target - timedelta(days=offset)).strftime('%Y%m%d')
                    try:
                        batch = self._sse_rows(day)
                    except ReviewBlockedError as e:
                        blocked['sse'] = e
                        logger.warning(f'两融沪市风控急停 {day}: {e}')
                        break
                    if batch:
                        got_sse = True
                        rows.extend(batch)
                        stats['sse_date'] = day
                        stats['sse_rows'] = len(batch)
                        stats['sse_source'] = 'sse'
                        if not dry_run:
                            self.db.upsert_margin_trades(batch)
                        break
                if not got_sse and not blocked.get('sse') \
                        and cn_rows and cn_rows.get('sse'):
                    # 官网全 lookback 无数据但巨潮有带日期沪市记录 → 补缺入库
                    if not dry_run:
                        self.db.upsert_margin_trades(cn_rows['sse'])
                    rows.extend(cn_rows['sse'])
                    stats['sse_date'] = date_str
                    stats['sse_rows'] = len(cn_rows['sse'])
                    stats['sse_source'] = 'cninfo'
            stats['rows'] = len(rows)
            if cn_error:
                stats['missing'].append(f'巨潮校验源不可用: {cn_error[:120]}')

            # 行数下限校验（两融全市场 >3000 标的；单所模式不判总量）
            if 0 < len(rows) < 3000 and not blocked:
                logger.warning(f'两融仅 {len(rows)} 行，低于历史日均 3000，'
                               f'疑似数据不全（仍入库，报告标注）')
                stats['missing'].append(f'行数 {len(rows)} < 3000')

            if blocked:
                ok_parts, miss_parts = [], []
                for m, label in (('szse', '深市'), ('sse', '沪市')):
                    if m in blocked:
                        miss_parts.append(f'{label}风控急停待补')
                    elif stats.get(f'{m}_rows'):
                        ok_parts.append(f'{label}已入库 {stats[f"{m}_rows"]} 行'
                                        f"（{stats.get(f'{m}_source') or '官网'}）")
                detail = '；'.join(ok_parts + miss_parts) or '风控急停'
                first_err = blocked.get('szse') or blocked.get('sse')
                mark_blocked(result, first_err,
                             f'官网风控急停（{detail}，稍后重跑补）: {first_err}')
            elif not rows:
                result['errors'].append(
                    f'{date_str} 起回看 {lookback} 日沪深两融均无数据（非交易日或未发布）')
            result['success'] = bool(rows) and not blocked
        except Exception as e:
            logger.exception(f'两融采集失败: {e}')
            result['errors'].append(str(e))
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result) if write_report else None
        return result

    # ---------- 历史区间回填（开发项 C） ----------

    def backfill_range(self, start_date: str, end_date: str, dry_run: bool = False,
                       suppress_report: bool = True) -> Dict[str, Any]:
        """按交易日历展开 [start_date, end_date] 逐日回填（开发项 C，20230101 起实测 34h 量级）。

        - **断点续跑只信查库（X-14 红线）**：margin_trade_daily 当日行数 > 0 跳过。
          不得信任单次调用的 stats；
        - **逐日直采（§2.1）**：显式业务日不回看——d 未发布/无数据当天记失败可续补，
          不再静默采到 d-1（原回看行为已从根上移除，date_notes 仅保留巨潮/XLSX 归属
          验证路径下的实际来源差异）；
        - 单日失败不阻塞（失败清单可重跑续补）；风控急停中止并记续跑点；
        - 报告（X-19）：suppress_report=True 时逐日不落盘，结束写一份汇总。
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
                             markets=missing, explicit_date=True)
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

    # ---------- 每所行构造（§4.1 三源路径） ----------

    def _szse_collect(self, day: str,
                      cn_rows: Optional[Dict[str, List[Dict[str, Any]]]],
                      ) -> tuple:
        """深市当日两融行（XLSX 优先 + 巨潮归属验证；巨潮互斥差异时用巨潮行）。

        Returns:
            (rows, source, verify)：source ∈ 'szse_xlsx'/'szse_json'/'cninfo'；
            verify ∈ None | 'all_equal' | 'code_mismatch_used_cninfo'
            | 'cn_unavailable_kept_unverified'(未入库返回空行时)。

        Raises:
            ReviewBlockedError: 官网被拒（巨潮补缺由调用方处理）。
        """
        if getattr(self, 'szse_xlsx_enabled', False):
            xlsx_items = self.source.fetch_szse_margin_xlsx(day)
            if not xlsx_items:
                return [], '', None
            return self._verify_szse_against_cninfo(xlsx_items, day, cn_rows,
                                                    source='szse_xlsx')
        # 旧 JSON 翻页路径（配置回退；jrrzmr 亿元 ×1e8 / jrrjye 万元 ×1e4 仅此口径）。
        # 经 _szse_rows 行构造层——单测桩按该层 mock
        rows = self._szse_rows(day)
        return rows, 'szse_json', None

    def _verify_szse_against_cninfo(self, xlsx_items: List[Dict[str, Any]],
                                    day: str,
                                    cn_rows: Optional[Dict[str, List[Dict[str, Any]]]],
                                    source: str) -> tuple:
        """XLSX（无日期列）归属验证：与巨潮同日深市代码集与可比金额对照（§4.1）。

        - 巨潮不可用：无独立日期证据 → 保留待核（不入库，返回空行）；
        - 代码集一致：逐股金额对账（差异记录），XLSX 入库；
        - 代码集不一致：以带日期的巨潮深市行为准入库（互斥差异隔离）。
        """
        rows = self._szse_rows_from_xlsx(xlsx_items, day)
        if cn_rows is None or not cn_rows.get('szse'):
            logger.warning(f'深市两融 {day} XLSX 已解析 {len(rows)} 行，但巨潮同日记录'
                           f'不可用——无日期导出缺独立日期证据，保留待核不入账')
            return [], '', 'kept_unverified_no_cninfo'
        cn_by_code = {r['ts_code'].split('.')[1]: r for r in cn_rows['szse']}
        xlsx_codes = {r['ts_code'].split('.')[1] for r in rows}
        diffs: List[str] = []
        if xlsx_codes == set(cn_by_code.keys()):
            for r in rows:
                cn = cn_by_code.get(r['ts_code'].split('.')[1])
                if not cn:
                    continue
                for f in ('rzye', 'rzmre', 'rqye'):
                    a, b = r.get(f), cn.get(f)
                    if a is not None and b is not None and a != b:
                        diffs.append(f'{r["ts_code"]}.{f}: xlsx={a} cn={b}')
            if diffs:
                logger.warning(f'深市两融 {day} 双源金额差异 {len(diffs)} 处（先隔离复查，'
                               f'已按 XLSX 入库并记录）: {diffs[:5]}')
            return rows, source, ('all_equal' if not diffs else f'diffs={len(diffs)}')
        only_xlsx = sorted(xlsx_codes - set(cn_by_code.keys()))[:5]
        only_cn = sorted(set(cn_by_code.keys()) - xlsx_codes)[:5]
        logger.warning(f'深市两融 {day} XLSX 与巨潮代码集不一致'
                       f'（xlsx独有{len(only_xlsx)}: {only_xlsx}；'
                       f'巨潮独有{len(only_cn)}: {only_cn}）——'
                       f'改用带日期巨潮行入库，差异隔离复查')
        return (list(cn_rows['szse']), 'cninfo',
                'code_mismatch_used_cninfo')

    def _szse_rows_from_xlsx(self, items: List[Dict[str, Any]],
                             day: str) -> List[Dict[str, Any]]:
        """深市 XLSX 行 → margin_trade_daily 行（金额已是元；rzche 该文件无列）"""
        rows = []
        for item in items:
            code = str(item.get('zqdm') or '')
            if not code:
                continue
            rows.append({
                'trade_date': day,
                'ts_code': code6_to_ts_code(code),
                'rzye': item.get('rzye'),
                'rzmre': item.get('rzmre'),
                'rzche': None,
                'rqye': item.get('rqye'),
            })
        return rows

    def _szse_rows_from_json(self, items: List[Dict[str, Any]],
                             day: str) -> List[Dict[str, Any]]:
        """旧 JSON 翻页行 → margin_trade_daily 行（亿元/万元换算仅此口径）"""
        rows = []
        for item in items:
            code = str(item.get('zqdm') or '')
            if not code:
                continue
            rzmre = _dec_scale(item.get('jrrzmr'), Decimal('1e8'))
            rzye = _dec_scale(item.get('jrrzye'), Decimal('1e8'))
            rqye = _dec_scale(item.get('jrrjye'), Decimal('1e4'))
            if rzye is None and rzmre is None:
                continue
            rows.append({
                'trade_date': day,
                'ts_code': code6_to_ts_code(code),
                'rzye': rzye,
                'rzmre': rzmre,
                'rzche': None,
                'rqye': rqye,
            })
        return rows

    def _cninfo_by_market(self, day: str) -> Dict[str, List[Dict[str, Any]]]:
        """巨潮当日全市场明细按市场分组（沪深落表；北交所计入范围统计不落沪深表）。

        2026-11-26 实测：F010V 取值 '深圳'(2119)/'上海'(2019)/'北京'(353)，
        合计 4,491 与文档一致；F009N=两融合计（可比记录 F001N+F008N=F009N）。
        """
        detail = self.cninfo.fetch_margin_detail(day)
        market_map = {'深圳': 'szse', '上海': 'sse', '北京': 'bse',
                      'sz': 'szse', 'sse': 'sse', 'sh': 'sse', 'bj': 'bse'}
        grouped: Dict[str, List[Dict[str, Any]]] = {'szse': [], 'sse': [], 'bse': []}
        unknown: List[str] = []
        for rec in detail['records']:
            code = str(rec.get('SECCODE') or '').strip()
            market_raw = str(rec.get('F010V') or '').strip().lower()
            market = market_map.get(market_raw) or market_map.get(
                str(rec.get('F010V') or '').strip())
            if not code or len(code) != 6:
                continue
            row = {
                'trade_date': day,
                'ts_code': code6_to_ts_code(code),
                'rzye': _dec(rec.get('F001N')),
                'rzmre': _dec(rec.get('F002N')),
                'rzche': _dec(rec.get('F003N')),
                'rqye': _dec(rec.get('F008N')),
            }
            # F009N 两融合计可比校验（文档 §4.1；F009N 缺失跳过）
            hj = _dec(rec.get('F009N'))
            if hj is not None and row['rzye'] is not None \
                    and row['rqye'] is not None \
                    and row['rzye'] + row['rqye'] != hj:
                logger.warning(f'巨潮两融 {code} 合计不符: F001N+F008N='
                               f'{row["rzye"] + row["rqye"]} ≠ F009N={hj}（保留行，差异复查）')
            if market == 'szse':
                grouped['szse'].append(row)
            elif market == 'sse':
                grouped['sse'].append(row)
            elif market == 'bse':
                grouped['bse'].append(row)
            elif market_raw:
                unknown.append(market_raw)
        if unknown:
            logger.warning(f'巨潮两融 {day} 未识别市场 {len(unknown)} 行'
                           f'（F010V={sorted(set(unknown))[:5]}），记录不落沪深表')
        grouped['_meta'] = [{'total': detail['total'], 'trade_date': detail['trade_date']}]  # type: ignore[list-item]
        return grouped

    def _szse_rows(self, day: str) -> List[Dict[str, Any]]:
        """旧 JSON 翻页路径行构造（szse_xlsx_enabled=false 回退用）"""
        return self._szse_rows_from_json(self.source.fetch_szse_margin(day), day)

    def _sse_rows(self, day: str) -> List[Dict[str, Any]]:
        """沪市行构造（queryMargin.do；rzye/rzmre/rzche 元；rqyl 余量不取）"""
        rows = []
        for item in self.source.fetch_sse_margin(day):
            code = str(item.get('stockCode') or '')
            if not code:
                continue
            rzye = _dec(item.get('rzye'))
            rzmre = _dec(item.get('rzmre'))
            if rzye is None and rzmre is None:
                continue
            rows.append({
                'trade_date': day,
                'ts_code': code6_to_ts_code(code),
                'rzye': rzye,
                'rzmre': rzmre,
                'rzche': _dec(item.get('rzche')),
                'rqye': None,      # 沪市官方明细无融券余额金额列（rqyl 为余量）
            })
        return rows

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 数据日期：深交所 {stats.get('szse_date') or '无'}（来源 {stats.get('szse_source') or '-'}）/ "
            f"上交所 {stats.get('sse_date') or '无'}（来源 {stats.get('sse_source') or '-'}）"
            f"（两融 T-1 业务日，T+1 盘前发布）",
            f"- 深交所 {stats['szse_rows']} 行 / 上交所 {stats['sse_rows']} 行，合计 {stats['rows']}",
            "- 单位：元（Decimal 全程；深市 XLSX 金额列为元口径，旧 JSON 亿元/万元换算仅回退路径）",
        ]
        if stats.get('cninfo_verify'):
            lines.append(f"- 巨潮同日归属验证：{stats['cninfo_verify']}")
        if stats['missing']:
            lines.append(f"- 校验提示：{stats['missing']}")
        if result['errors']:
            lines.append(f"- 提示：{result['errors']}")
        status = '成功' if result['success'] else '失败（待补）'
        return write_review_report('融资融券同步报告', 'margin_trade_sync_report',
                                   status, lines, result.get('duration'))


def _dec_scale(value: Any, scale: Decimal) -> Optional[Decimal]:
    """旧 JSON 路径量纲换算（亿元 ×1e8 / 万元 ×1e4）；无效返回 None"""
    v = _dec(value)
    return v * scale if v is not None else None
