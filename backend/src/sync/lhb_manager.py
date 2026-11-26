"""
龙虎榜同步（方案 4.9/4.10，M3；沪深交易所官网每日披露页官方 JSON，17:30 后发布）

接口事实（2026-09-27 首抓 / 2026-10-02 上交所切换汇总接口，详见 sse_szse_source 文件头）：
- 深交所：列表 CATALOGID=1842_xxpl_after（dqrq/zqdm/zqjc/cjje 亿元/plyy 原因/
  bz 内含明细入口 ZBDM）+ 席位明细 1842_detal tab2（买1~卖5 营业部金额）
- 上交所：queryAllTradeOpenDate.do 汇总接口（1 请求全部上榜行，个股×原因；
  席位打包在 branchNameB/S + branchTxAmtB/S 逗号分隔字段，T12 实测一一对应；
  旧 queryTradeOpenInfo.do + refType 空传只返回 refType=1 单分类——X-1 漏采
  约 96% 的存量 bug，已废弃）
- 金额口径（V1.2）：buy/sell/net = 该上榜原因的前五席位合计（沪市由打包字段 Σ，
  深市由席位明细按榜单行聚合）——**禁止跨上榜原因求和**（多原因股混加当日榜与
  三日累计榜会使金额失真至 8 倍以上）；多原因股按行展示
- X-18 先删后插：重抓日期先删沪市旧格式行（'披露原因代码{N}'）再 upsert 新行
- 失败次日补抓（官网保留历史榜单）；入库榜单数与官网展示条数校验
"""

import hashlib
import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ..config.config_manager import ConfigManager
from ..data_sources.cninfo_source import CninfoBlockedError, CninfoSource
from ..data_sources.http_collector_base import ReviewBlockedError
from ..data_sources.sse_szse_source import SSE_REF_TYPE_NAMES, SzseSseSource
from ..database.connection import DatabaseConnection
from ..database.dao_collection_retry import domain_for_source
from .review_common import (clean_stock_name, code6_to_ts_code, expand_trade_days,
                            mark_blocked, write_review_report)

logger = logging.getLogger(__name__)

# 深交所 bz 字段明细入口参数（a-param='/ShowReport/data?...ZBDM=0902'）
_ZBDM_PATTERN = re.compile(r'ZBDM=(\d+)')

# 科创板原因映射（§5.1：refType 按科创板字典解释；未知码回落保持兼容）
SSE_KCB_REF_TYPE_NAMES = dict(SSE_REF_TYPE_NAMES)   # 科创板沿用官网披露类型字典，未知原文回落


def _event_id(trade_date: str, exchange: str, board: str, ts_code: str,
              reason_code: str, period_start: Optional[str],
              period_end: Optional[str]) -> str:
    """事件归一身份 -> event_id（§5.2：披露日+交易所+板块+代码+原因规则+区间）"""
    key = (f'{trade_date}|{exchange}|{board}|{ts_code}|{reason_code}|'
           f'{period_start or ""}~{period_end or ""}')
    return hashlib.sha1(key.encode('utf-8')).hexdigest()[:32]


class LhbManager:
    """龙虎榜：官网 JSON -> lhb_stock_daily + lhb_detail（含席位则一并入库）；
    批次 B 事件层（§5.1/§5.2）：三源归并 -> lhb_event + lhb_event_seat（事务替换）
    """

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        self.db.ensure_lhb_event_tables()
        self.source = SzseSseSource(self.config)
        cninfo_cfg = dict(self.config.get('data_sources', {}).get('cninfo', {}) or {})
        cninfo_cfg.setdefault('query_base_url', 'http://www.cninfo.com.cn')
        self.cninfo = CninfoSource(cninfo_cfg)
        review = self.config.get('review_sync', {}) or {}
        self.events_enabled = bool(review.get('lhb_events_enabled', True))

    def execute(self, trade_date: Optional[str] = None, dry_run: bool = False,
                write_report: bool = True) -> Dict[str, Any]:
        started = datetime.now()
        trade_date = trade_date or datetime.now().strftime('%Y%m%d')
        stats: Dict[str, Any] = {
            'trade_date': trade_date, 'szse_stocks': 0, 'sse_stocks': 0,
            'lhb_rows': 0, 'detail_rows': 0, 'errors': [], 'seat_gaps': [],
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
        try:
            szse_blocked: Optional[ReviewBlockedError] = None
            sse_blocked: Optional[ReviewBlockedError] = None
            szse_stocks: List[Dict[str, Any]] = []
            szse_details: List[Dict[str, Any]] = []
            szse_seat_gaps: List[Dict[str, str]] = []
            sse_stocks: List[Dict[str, Any]] = []
            sse_details: List[Dict[str, Any]] = []
            # 深市（限流方案 §1/§3 P0：明确被拒即停止后续席位请求并按行保存缺口）
            try:
                szse_stocks, szse_details, szse_seat_gaps = self._collect_szse(trade_date)
            except ReviewBlockedError as e:
                szse_blocked = e
                logger.warning(f'龙虎榜深市风控急停 {trade_date}: {e}')
            # 沪市与深市不同主机，独立尝试（§4.2 沪深可分别成功、分别待补）
            try:
                sse_stocks, sse_details = self._collect_sse(trade_date)
            except ReviewBlockedError as e:
                sse_blocked = e
                logger.warning(f'龙虎榜沪市风控急停 {trade_date}: {e}')
            stats['szse_stocks'] = len({(r['ts_code'], r['reason']) for r in szse_stocks})
            stats['sse_stocks'] = len({(r['ts_code'], r['reason']) for r in sse_stocks})
            stock_rows = szse_stocks + sse_stocks
            detail_rows = szse_details + sse_details
            stats['lhb_rows'] = len(stock_rows)
            stats['detail_rows'] = len(detail_rows)
            stats['rows'] = stats['lhb_rows']  # 编排器摘要行数约定（rows/records）

            if not dry_run:
                # 沪深分别入库（限流方案 §4.2）：单所被拒不再丢弃已抓另一所
                if szse_stocks:
                    self.db.upsert_lhb_stocks(szse_stocks)
                if szse_details:
                    self.db.upsert_lhb_details(szse_details)
                if sse_stocks:
                    # X-18 先删后插：沪市旧格式行（'披露原因代码{N}'）与新文案映射在唯一键
                    # (trade_date, ts_code, reason) 下会并存致同股重复上榜；重抓日期先删
                    stats['sse_legacy_deleted'] = self.db.delete_sse_lhb_legacy(trade_date)
                    self.db.upsert_lhb_stocks(sse_stocks)
                if sse_details:
                    self.db.upsert_lhb_details(sse_details)
            if not stock_rows and szse_blocked is None and sse_blocked is None:
                result['errors'].append(f'{trade_date} 沪深龙虎榜均无数据'
                                        f'（非交易日或未到发布时间 17:30）')
            # 席位缺口即时登记（限流方案 §3/§4.3："榜单已取，席位待补"——
            # 补采只抓缺口行，不再被"当天有行就跳过"挡住）
            if szse_seat_gaps:
                stats['seat_gaps'] = szse_seat_gaps
                if not dry_run:
                    self._register_seat_gaps(trade_date, szse_seat_gaps)
            elif not dry_run:
                # 本轮席位齐全：结清该日历史席位缺口待办（幂等，仅未结任务生效）
                self._complete_seat_gaps(trade_date)
            if szse_blocked is not None or sse_blocked is not None:
                parts = []
                for m, label, e in (('szse', '深市', szse_blocked), ('sse', '沪市', sse_blocked)):
                    if e is None:
                        continue
                    got = stats.get(f'{m}_stocks')
                    parts.append(f'{label}风控急停待补' if not got
                                 else f'{label}榜单已入库 {got} 股、余量待补')
                    mark_blocked(result, e, f'官网风控急停（{label}）: {e}')
            result['success'] = bool(stock_rows) \
                and szse_blocked is None and sse_blocked is None
            # §5.1/§5.2 批次 B 事件层：三源归并写 lhb_event/lhb_event_seat
            # （事件+席位同事务整日替换；旧表并行投影不动，读端切换独立进行）
            if self.events_enabled and not dry_run:
                try:
                    ev_stats = self._collect_and_store_events(trade_date)
                    stats['events'] = ev_stats
                except ReviewBlockedError as e:
                    logger.warning(f'龙虎榜事件层风控急停 {trade_date}: {e}')
                    stats['events'] = {'error': str(e)[:200]}
                except Exception as e:
                    logger.warning(f'龙虎榜事件层失败 {trade_date}: {e}')
                    stats['events'] = {'error': str(e)[:200]}
        except Exception as e:
            logger.exception(f'龙虎榜采集失败: {e}')
            result['errors'].append(str(e))
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            # X-19：回填模式逐日报告抑制（write_report=False），结束后由 backfill 汇总
            result['report_path'] = self._write_report(result) if write_report else None
        return result

    # ---------- 历史区间回填（开发项 C） ----------

    def backfill_range(self, start_date: str, end_date: str, dry_run: bool = False,
                       suppress_report: bool = True) -> Dict[str, Any]:
        """按交易日历展开 [start_date, end_date] 逐日回填（开发项 C）。

        - 断点续跑只信查库：lhb_stock_daily 当日行数 > 0 跳过（幂等，可中断重跑）；
        - 单日失败不阻塞（记失败清单，结束后汇总，失败日期可重跑续补）；
        - 风控急停（ReviewBlockedError）中止整个回补，记录续跑点；
        - 游标：lhb 无 sync_cursor，回补不影响增量语义；
        - 报告（X-19）：suppress_report=True 时逐日不落盘，结束写一份汇总；
        - 限速（X-5）：回补期上交所间隔默认提至 3.0s（config
          review_sync.lhb_backfill_interval 可覆盖），不影响日常增量。
        """
        started = datetime.now()
        days = expand_trade_days(self.db, start_date, end_date)
        review_cfg = (self.config.get('review_sync') or {}) or {}
        interval = float(review_cfg.get('lhb_backfill_interval', 3.0))
        if self.source.request_interval < interval:
            logger.info(f'龙虎榜回补限速提升: {self.source.request_interval}s -> {interval}s（X-5）')
            self.source.request_interval = interval
        stats: Dict[str, Any] = {
            'range': f'{start_date}~{end_date}', 'days_total': len(days),
            'days_done': 0, 'days_skipped': 0, 'lhb_rows': 0, 'detail_rows': 0,
            'seat_gap_days_fixed': 0, 'days_failed': [], 'last_completed_date': None,
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [],
                                  'blocked': False, 'backfill': True}
        for d in days:
            if not dry_run and self.db.fetch_lhb_row_count(d) > 0:
                # 榜单已取：仅当存在未结席位缺口时补席位（限流方案 §4.3——
                # 修正"当天有行就跳过"导致席位缺口永不回补的旧判断）
                scope = self.db.scope_retry_missing_scope('lhb', d, 'seats') or {}
                gaps = scope.get('seats') or []
                if not gaps:
                    stats['days_skipped'] += 1
                    continue
                r = self.backfill_seats(d, gaps, suppress_report=True)
                if r.get('blocked'):
                    result['blocked'] = True
                    result['errors'].append(f'席位补采风控急停于 {d}，'
                                            f'已找回 {stats["seat_gap_days_fixed"]} 日，'
                                            f'续跑点 --start-date {d}')
                    break
                if r.get('success'):
                    stats['seat_gap_days_fixed'] += 1
                    stats['detail_rows'] += r['stats']['detail_rows']
                else:
                    stats['days_failed'].append({'date': d, 'errors': r['errors'][:2]})
                continue
            r = self.execute(trade_date=d, dry_run=dry_run, write_report=False)
            if r.get('blocked'):
                result['blocked'] = True
                result['errors'].append(f'风控急停于 {d}，已完成 {stats["days_done"]} 日，'
                                        f'续跑点 --start-date {d}')
                break
            if r['success']:
                stats['days_done'] += 1
                stats['lhb_rows'] += r['stats']['lhb_rows']
                stats['detail_rows'] += r['stats']['detail_rows']
                stats['last_completed_date'] = d
            else:
                stats['days_failed'].append({'date': d, 'errors': r['errors'][:2]})
        result['success'] = not result['blocked'] and not stats['days_failed']
        result['duration'] = (datetime.now() - started).total_seconds()
        result['report_path'] = self._write_backfill_report(result)
        return result

    # ---------- 席位缺口（SCOPE_RETRY 子项，限流方案 20261120 §3/§4.3） ----------

    def _register_seat_gaps(self, trade_date: str, gaps: List[Dict[str, str]]) -> None:
        """席位缺口即时登记为子项补采待办（限流方案 §2：真实缺口一经确认即独立提交）"""
        try:
            self.db.register_retry(
                dataset='lhb', source='lhb', target_date=trade_date, market='CN',
                action='SCOPE_RETRY', reason='SEAT_GAP', sub_key='seats',
                missing_scope={'seats': [
                    {'ann_date': g.get('ann_date'), 'code': g.get('code'),
                     'zb_dm': g.get('zb_dm'), 'ts_code': g.get('ts_code'),
                     'reason': g.get('reason')} for g in gaps]},
                domain=domain_for_source('lhb'),
                last_error=f'{trade_date} 席位缺口 {len(gaps)} 行')
        except Exception as e:
            logger.warning(f'席位缺口登记失败（报告仍记录 {len(gaps)} 行）: {e}')

    def _complete_seat_gaps(self, trade_date: str) -> None:
        """本轮席位齐全时结清该日历史席位缺口待办（幂等，仅未结任务生效）"""
        try:
            self.db.complete_scope_retry('lhb', trade_date, 'seats')
        except Exception as e:
            logger.debug(f'席位缺口结清检查失败（忽略）: {e}')

    def backfill_seats(self, trade_date: str, gaps: List[Dict[str, str]],
                       dry_run: bool = False,
                       suppress_report: bool = True) -> Dict[str, Any]:
        """只补席位缺口（SCOPE_RETRY 子项补采；榜单已验收，不重抓——限流方案 §4.3）。

        缺口键 =（ann_date, code, zb_dm, ts_code, reason）；全部找回时结清该日
        席位待办；明确被拒即停（已找回部分保留入库），余下缺口留在待办。
        """
        started = datetime.now()
        stats: Dict[str, Any] = {'trade_date': trade_date, 'gaps_total': len(gaps),
                                 'recovered': 0, 'detail_rows': 0, 'still_failed': [],
                                 'errors': []}
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [],
                                  'blocked': False, 'backfill': True}
        rows: List[Dict[str, Any]] = []
        try:
            for g in gaps:
                try:
                    seats = self.source.fetch_szse_lhb_seats(
                        g.get('ann_date') or trade_date, g.get('code') or '',
                        g.get('zb_dm') or '')
                except ReviewBlockedError as e:
                    mark_blocked(result, e, f'席位补采风控急停（已找回 '
                                            f'{stats["recovered"]}/{stats["gaps_total"]}）: {e}')
                    break
                except Exception as e:
                    stats['still_failed'].append(f'{g.get("ts_code")}:{g.get("reason")}')
                    stats['errors'].append(str(e))
                    continue
                rows.extend(self._parse_szse_seats(seats, trade_date,
                                                   g.get('ts_code') or ''))
                stats['recovered'] += 1
            if rows and not dry_run:
                self.db.upsert_lhb_details(rows)
                stats['detail_rows'] = len(rows)
            result['success'] = (not result.get('blocked') and not stats['still_failed']
                                 and stats['recovered'] == stats['gaps_total'])
            if result['success'] and not dry_run:
                self.db.complete_scope_retry('lhb', trade_date, 'seats')
        except Exception as e:
            logger.exception(f'席位缺口补采失败 {trade_date}: {e}')
            result['errors'].append(str(e))
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = (None if suppress_report
                                     else self._write_backfill_report(result))
        return result

    def _write_backfill_report(self, result: Dict[str, Any]) -> Optional[str]:
        """回填汇总报告（X-19：整个区间只写一份）"""
        stats = result['stats']
        lines = [
            f"- 区间：{stats['range']}（交易日 {stats['days_total']}：回填 {stats['days_done']}、"
            f"跳过 {stats['days_skipped']}、失败 {len(stats['days_failed'])}）",
            f"- 累计入库：榜单 {stats['lhb_rows']} 行 / 席位明细 {stats['detail_rows']} 行"
            f"（末完成日 {stats['last_completed_date']}）",
            "- 断点续跑口径：只信查库（lhb_stock_daily 当日行数>0 跳过），可中断重跑",
        ]
        if result['blocked']:
            lines.append(f"- ⚠ 风控急停：{result['errors']}")
        if stats['days_failed']:
            lines.append(f"- 失败日期清单（可重跑续补）: "
                         f"{[f['date'] for f in stats['days_failed'][:30]]}")
        status = '风控急停（部分完成，可续跑）' if result['blocked'] else \
            ('成功' if result['success'] else '部分失败（失败清单见上，可重跑续补）')
        return write_review_report('龙虎榜历史回填报告', 'lhb_backfill_report',
                                   status, lines, result.get('duration'))

    # ---------- 深交所 ----------

    def _collect_szse(self, trade_date: str
                      ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]],
                                 List[Dict[str, str]]]:
        """深交所榜单行 + 席位明细 + 席位缺口（限流方案 20261120 §3 P0）。

        金额口径（X-7/V1.2）：每行 buy/sell/net 用**当次抓到的该行席位**聚合
        （买 Σ、卖 Σ、差为净；席位 3~8 个不满额属正常，T19）——按榜单行粒度，
        禁止按股票跨原因求和。席位抓取失败时金额留空（不硬造）。

        席位缺口口径：明确被拒（ReviewBlockedError）→ **停止后续席位请求**，
        当行及余下行全部记缺口；单行普通失败 → 记缺口继续。缺口按
        （ann_date, code, zb_dm, ts_code, reason）保存，补采只抓缺口行。
        """
        stocks: List[Dict[str, Any]] = []
        details: List[Dict[str, Any]] = []
        seat_gaps: List[Dict[str, str]] = []

        def _gap(ann_date: str, code: str, zb_dm: str, ts_code: str, reason: str) -> Dict[str, str]:
            return {'ann_date': ann_date, 'code': code, 'zb_dm': zb_dm,
                    'ts_code': ts_code, 'reason': reason}

        raw = self.source.fetch_szse_lhb(trade_date)
        seats_stopped = False
        for item in raw:
            code = str(item.get('zqdm') or '')
            if not code:
                continue
            ann_date = str(item.get('dqrq') or '').replace('-', '') or trade_date
            ts_code = code6_to_ts_code(code)
            reason = str(item.get('plyy') or '').strip()
            # 席位明细：从 bz 提取 ZBDM 后单独请求（串行低频）
            row_details: List[Dict[str, Any]] = []
            m = _ZBDM_PATTERN.search(str(item.get('bz') or ''))
            if m:
                if seats_stopped:
                    seat_gaps.append(_gap(ann_date, code, m.group(1), ts_code, reason))
                else:
                    try:
                        seats = self.source.fetch_szse_lhb_seats(ann_date, code, m.group(1))
                    except ReviewBlockedError as e:
                        # 明确被拒：停止后续席位请求（限流方案 §1），余下行全部记缺口
                        logger.warning(f'深交所席位明细被拒（停止后续席位请求）{code}: {e}')
                        seats_stopped = True
                        seat_gaps.append(_gap(ann_date, code, m.group(1), ts_code, reason))
                    except Exception as e:
                        logger.warning(f'深交所席位明细失败 {code}: {e}')
                        seat_gaps.append(_gap(ann_date, code, m.group(1), ts_code, reason))
                    else:
                        row_details = self._parse_szse_seats(seats, trade_date, ts_code)
                details.extend(row_details)
            buy_amt = _sum_side(row_details, '买')
            sell_amt = _sum_side(row_details, '卖')
            net_amt = round(buy_amt - sell_amt, 2) \
                if buy_amt is not None and sell_amt is not None else None
            stocks.append({
                'trade_date': trade_date,
                'ts_code': ts_code,
                'stock_name': clean_stock_name(item.get('zqjc')),
                'reason': reason,
                'buy_amt': buy_amt,
                'sell_amt': sell_amt,
                'net_amt': net_amt,
                # 列表披露成交金额（亿元）——个股当日总成交额口径，与沪市 secTxAmount 同列
                'sec_tx_amount': _num_yuan(item.get('cjje'), 1e8),
            })
        return stocks, details, seat_gaps

    @staticmethod
    def _parse_szse_seats(seats: List[Dict[str, Any]], trade_date: str,
                          ts_code: str) -> List[Dict[str, Any]]:
        """深交所席位原始行 → lhb_detail 行（与 _collect_sse 打包解析同唯一键口径）"""
        parsed: List[Dict[str, Any]] = []
        for seat in seats or []:
            seat_name = str(seat.get('zsmc') or '').strip()
            if not seat_name:
                continue
            side_label = str(seat.get('mmlb') or '').strip()
            if not side_label:
                continue
            # side 存官网原文（买1~卖5，含排名）：深交所"机构专用"对应多个不同
            # 机构席位，若归并为纯方向会被 (ts_code, seat_name, side) 唯一键去重丢行
            side = side_label if side_label[0] in ('买', '卖') else side_label[:1]
            amount = _num_yuan(seat.get('mrje') if side.startswith('买')
                               else seat.get('mcje'), 1.0)
            parsed.append({
                'trade_date': trade_date,
                'ts_code': ts_code,
                'seat_name': seat_name,
                'side': side,
                'amount': amount,
            })
        return parsed

    # ---------- 上交所 ----------

    def _collect_sse(self, trade_date: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """上交所汇总接口（X-1 修复）：每行一个 secCode×refType 榜单记录，
        买卖各 5 席位打包在 branchNameB/S + branchTxAmtB/S 逗号分隔字段（T12）。"""
        stocks: List[Dict[str, Any]] = []
        details: List[Dict[str, Any]] = []
        raw = self.source.fetch_sse_lhb_summary(trade_date)
        for item in raw:
            code = str(item.get('secCode') or '')
            if not code:
                continue
            ts_code = code6_to_ts_code(code)
            ref_type = str(item.get('refType') or '')
            reason = SSE_REF_TYPE_NAMES.get(ref_type, f'披露原因代码{ref_type}')
            buy_names = _split_packed_names(item.get('branchNameB'))
            buy_amts = _split_packed_amounts(item.get('branchTxAmtB'))
            sell_names = _split_packed_names(item.get('branchNameS'))
            sell_amts = _split_packed_amounts(item.get('branchTxAmtS'))
            buy_vals = [a for a in buy_amts if a is not None]
            sell_vals = [a for a in sell_amts if a is not None]
            buy_amt = round(sum(buy_vals), 2) if buy_vals else None
            sell_amt = round(sum(sell_vals), 2) if sell_vals else None
            net_amt = round(buy_amt - sell_amt, 2) \
                if buy_amt is not None and sell_amt is not None else None
            stocks.append({
                'trade_date': trade_date,
                'ts_code': ts_code,
                'stock_name': clean_stock_name(item.get('secAbbr')),
                'reason': reason,
                'buy_amt': buy_amt,
                'sell_amt': sell_amt,
                'net_amt': net_amt,
                'sec_tx_amount': _to_float(item.get('secTxAmount')),
            })
            # 席位明细：第 i 段为买{i+1}/卖{i+1}（与 lhb_detail 唯一键兼容）
            for i, (name, amount) in enumerate(zip(buy_names, buy_amts)):
                if not name:
                    continue
                details.append({'trade_date': trade_date, 'ts_code': ts_code,
                                'seat_name': name, 'side': f'买{i + 1}', 'amount': amount})
            for i, (name, amount) in enumerate(zip(sell_names, sell_amts)):
                if not name:
                    continue
                details.append({'trade_date': trade_date, 'ts_code': ts_code,
                                'seat_name': name, 'side': f'卖{i + 1}', 'amount': amount})
        return stocks, details

    # ---------- 事件层（§5.1 三源归并；§5.2 事务替换） ----------

    def _collect_and_store_events(self, trade_date: str) -> Dict[str, Any]:
        """官方事件优先归并三源，整日事务替换事件与席位。

        - 沪主板 queryAllTradeOpenDate（元、打包席位、abnormalStart/End）；
        - 科创板 queryKCBTradeInfo（万元→元、unnormalStart/End）；
        - 深市 XLSX（原因原文、无区间列——period 留空不以披露日伪造）；
        - 巨潮 getMarketStatisticsData 对账 + 席位补充（沪市与官网事件按
          (ts_code×事件数) 对账；深市单原因股直接按该股当日唯一事件补席位，
          多原因股留待核不猜配对）。
        """
        events: List[Dict[str, Any]] = []
        # ---- 沪主板（汇总接口，元） ----
        sse_main = 0
        sse_fund = 0
        for item in self.source.fetch_sse_lhb_summary(trade_date):
            code = str(item.get('secCode') or '')
            if not code:
                continue
            ts_code = code6_to_ts_code(code)
            ref_type = str(item.get('refType') or '')
            security_type = 'fund' if code.startswith('5') else 'stock'
            reason_code = f'sse_main.r{ref_type}'
            reason_text = SSE_REF_TYPE_NAMES.get(ref_type, f'披露原因代码{ref_type}')
            period_start = _compact_date(item.get('abnormalStart'))
            period_end = _compact_date(item.get('abnormalEnd'))
            buy_names = _split_packed_names(item.get('branchNameB'))
            buy_amts = _split_packed_amounts(item.get('branchTxAmtB'))
            sell_names = _split_packed_names(item.get('branchNameS'))
            sell_amts = _split_packed_amounts(item.get('branchTxAmtS'))
            seats = _packed_seats(buy_names, buy_amts, sell_names, sell_amts,
                                  scale=1.0, source='sse_main')
            buy_vals = [a for a in buy_amts if a is not None]
            sell_vals = [a for a in sell_amts if a is not None]
            buy_amt = round(sum(buy_vals), 2) if buy_vals else None
            sell_amt = round(sum(sell_vals), 2) if sell_vals else None
            events.append({
                'event_id': _event_id(trade_date, 'sse', 'main', ts_code,
                                      reason_code, period_start, period_end),
                'trade_date': trade_date, 'exchange': 'sse', 'board': 'main',
                'ts_code': ts_code, 'security_type': security_type,
                'reason_code': reason_code, 'reason_text': reason_text,
                'period_start': period_start, 'period_end': period_end,
                'sec_tx_amount': _to_float(item.get('secTxAmount')),
                'buy_amt': buy_amt, 'sell_amt': sell_amt,
                'net_amt': (round(buy_amt - sell_amt, 2)
                            if buy_amt is not None and sell_amt is not None else None),
                'source_keys': {'sse': item.get('fileDate') or trade_date},
                'seats': seats,
            })
            if security_type == 'fund':
                sse_fund += 1
            else:
                sse_main += 1
        # ---- 科创板（万元→元） ----
        kcb_n = 0
        for item in self.source.fetch_sse_kcb_lhb(trade_date):
            code = str(item.get('secCode') or '')
            if not code:
                continue
            ts_code = code6_to_ts_code(code)
            ref_type = str(item.get('refType') or '')
            reason_code = f'sse_kcb.r{ref_type}'
            reason_text = SSE_KCB_REF_TYPE_NAMES.get(ref_type, f'披露原因代码{ref_type}')
            period_start = _compact_date(item.get('unnormalStartDate'))
            period_end = _compact_date(item.get('unnormalEndDate'))
            buy_names = _split_packed_names(item.get('deptNameB'))
            buy_amts = _split_packed_amounts(item.get('deptTxAmtB'))
            sell_names = _split_packed_names(item.get('deptNameS'))
            sell_amts = _split_packed_amounts(item.get('deptTxAmtS'))
            seats = _packed_seats(buy_names, buy_amts, sell_names, sell_amts,
                                  scale=1e4, source='sse_kcb')   # 万元→元
            buy_vals = [a for a in buy_amts if a is not None]
            sell_vals = [a for a in sell_amts if a is not None]
            buy_amt = round(sum(v * 1e4 for v in buy_vals), 2) if buy_vals else None
            sell_amt = round(sum(v * 1e4 for v in sell_vals), 2) if sell_vals else None
            region = _to_float(item.get('regionTxAmt'))
            events.append({
                'event_id': _event_id(trade_date, 'sse', 'kcb', ts_code,
                                      reason_code, period_start, period_end),
                'trade_date': trade_date, 'exchange': 'sse', 'board': 'kcb',
                'ts_code': ts_code, 'security_type': 'stock',
                'reason_code': reason_code, 'reason_text': reason_text,
                'period_start': period_start, 'period_end': period_end,
                'sec_tx_amount': (round(region * 1e4, 2)
                                  if region is not None else None),
                'buy_amt': buy_amt, 'sell_amt': sell_amt,
                'net_amt': (round(buy_amt - sell_amt, 2)
                            if buy_amt is not None and sell_amt is not None else None),
                'source_keys': {'sse_kcb': item.get('fileDate') or trade_date},
                'seats': seats,
            })
            kcb_n += 1
        # ---- 深市 XLSX（原因原文；无区间） ----
        szse_n = 0
        szse_by_code: Dict[str, int] = {}
        for item in self.source.fetch_szse_lhb_xlsx(trade_date):
            code = str(item.get('zqdm') or '')
            if not code:
                continue
            ts_code = code6_to_ts_code(code)
            reason_text = str(item.get('plyy') or '').strip()
            reason_code = f'szse_main.{reason_text[:48]}'
            events.append({
                'event_id': _event_id(trade_date, 'szse', 'main', ts_code,
                                      reason_code, None, None),
                'trade_date': trade_date, 'exchange': 'szse', 'board': 'main',
                'ts_code': ts_code, 'security_type': 'stock',
                'reason_code': reason_code, 'reason_text': reason_text,
                'period_start': None, 'period_end': None,
                'sec_tx_amount': (float(item['cjje'])
                                  if item.get('cjje') is not None else None),
                'buy_amt': None, 'sell_amt': None, 'net_amt': None,
                'source_keys': {'szse_xlsx': item.get('ann_date') or trade_date},
                'seats': [],
            })
            szse_n += 1
            szse_by_code[ts_code] = szse_by_code.get(ts_code, 0) + 1
        # ---- 巨潮对账 + 席位补充 ----
        recon: Dict[str, Any] = {}
        try:
            cn = self.cninfo.fetch_lhb_seats(trade_date)
            cn_events = cn['events']
            cn_by_code: Dict[str, List[Dict[str, Any]]] = {}
            for ev in cn_events:
                code = str(ev.get('secCode') or '')
                if not code:
                    continue
                cn_by_code.setdefault(code, []).append(ev)
            cn_sse_stock = sum(1 for c in cn_by_code if c.startswith('6')
                               for _ in cn_by_code[c])
            recon = {
                'cninfo_total': cn['total'],
                'cn_sse_stock_events': cn_sse_stock,
                'official_sse_stock_events': sse_main + kcb_n,
                'sse_stock_match': (cn_sse_stock == sse_main + kcb_n),
            }
            # 深市单原因股：按该股当日唯一巨潮事件补席位（多原因留待核不猜配对）
            seat_filled = 0
            seat_pending = 0
            for ev in events:
                if ev['exchange'] != 'szse' or ev.get('seats'):
                    continue
                code6 = ev['ts_code'].split('.')[1]
                cn_rows = cn_by_code.get(code6) or []
                if len(cn_rows) == 1 and szse_by_code.get(ev['ts_code'], 0) == 1:
                    ev['seats'] = _cninfo_seats(cn_rows[0], source='cninfo')
                    ev['buy_amt'] = ev['sell_amt'] = None   # 席位后汇总由读端计算
                    seat_filled += 1
                else:
                    seat_pending += 1
            recon['szse_seat_filled'] = seat_filled
            recon['szse_seat_pending'] = seat_pending
        except (CninfoBlockedError, Exception) as e:   # noqa: B014 巨潮为增强路径，不拖垮官方事件
            recon = {'error': f'巨潮不可用: {str(e)[:120]}'}
        # ---- 整日事务替换 ----
        with self.db.get_connection() as conn:
            try:
                self.db.replace_lhb_events_tx(conn, events, trade_date)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
        return {'events': len(events), 'sse_main': sse_main, 'sse_fund': sse_fund,
                'sse_kcb': kcb_n, 'szse_main': szse_n, 'recon': recon}

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 数据日期：{stats['trade_date']}（17:30 后发布，晚间重跑幂等安全）",
            f"- 上榜：深交所 {stats['szse_stocks']} 条 / 上交所 {stats['sse_stocks']} 条，"
            f"合计入库 {stats['lhb_rows']}",
            f"- 席位明细：{stats['detail_rows']} 条",
            "- 金额口径：该上榜原因的前五席位合计（非全市场买卖总额）；"
            "多原因上榜股按行展示，禁止跨原因求和",
        ]
        if stats.get('sse_legacy_deleted') is not None:
            lines.append(f"- 沪市旧格式行清理（X-18 先删后插）: {stats['sse_legacy_deleted']} 行")
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


def _compact_date(value: Any) -> Optional[str]:
    """任意日期输入 -> yyyyMMdd；空返回 None"""
    s = str(value or '').strip().replace('-', '')
    return s[:8] if len(s) == 8 and s.isdigit() else None


def _packed_seats(buy_names: List[str], buy_amts: List[Optional[float]],
                  sell_names: List[str], sell_amts: List[Optional[float]],
                  scale: float, source: str) -> List[Dict[str, Any]]:
    """官网打包席位 -> 席位行（§5.2 (event_id,side,rank)；名称不足或金额缺失
    保留已匹配位置——原始席位数组数量与排名不一致时不 zip 截断、不压缩空位）"""
    seats: List[Dict[str, Any]] = []
    for rank, name in enumerate(buy_names, start=1):
        if not name:
            continue
        amt = buy_amts[rank - 1] if rank - 1 < len(buy_amts) else None
        seats.append({'event_id': None, 'side': 'B', 'seat_rank': rank,
                      'seat_name': name,
                      'buy_amount': (round(amt * scale, 2) if amt is not None else None),
                      'sell_amount': None, 'amount_unit': 'yuan', 'source': source})
    for rank, name in enumerate(sell_names, start=1):
        if not name:
            continue
        amt = sell_amts[rank - 1] if rank - 1 < len(sell_amts) else None
        seats.append({'event_id': None, 'side': 'S', 'seat_rank': rank,
                      'seat_name': name,
                      'buy_amount': None,
                      'sell_amount': (round(amt * scale, 2) if amt is not None else None),
                      'amount_unit': 'yuan', 'source': source})
    return seats


def _cninfo_seats(ev: Dict[str, Any], source: str) -> List[Dict[str, Any]]:
    """巨潮事件 detail -> 席位行（每位置买方/卖方成对展开，元，千分位清洗）"""
    from decimal import Decimal

    def _d(v: Any) -> Optional[Decimal]:
        if v is None:
            return None
        s = str(v).replace(',', '').strip()
        try:
            return Decimal(s)
        except Exception:
            return None

    seats: List[Dict[str, Any]] = []
    for rank, pos in enumerate(ev.get('detail') or [], start=1):
        bname = str(pos.get('buyOrgName') or '').strip()
        sname = str(pos.get('sellOrgName') or '').strip()
        if bname:
            seats.append({'event_id': None, 'side': 'B', 'seat_rank': rank,
                          'seat_name': bname, 'buy_amount': _d(pos.get('buyOrgBuyTotal')),
                          'sell_amount': _d(pos.get('buyOrgSellTotal')),
                          'amount_unit': 'yuan', 'source': source})
        if sname:
            seats.append({'event_id': None, 'side': 'S', 'seat_rank': rank,
                          'seat_name': sname, 'buy_amount': _d(pos.get('sellOrgBuyTotal')),
                          'sell_amount': _d(pos.get('sellOrgSellTotal')),
                          'amount_unit': 'yuan', 'source': source})
    return seats


def _split_packed_names(value: Any) -> List[str]:
    """汇总接口打包席位名串 '名1,名2,…' -> 列表（空串/None -> []）"""
    if value is None or not str(value).strip():
        return []
    return [p.strip() for p in str(value).split(',')]


def _split_packed_amounts(value: Any) -> List[Optional[float]]:
    """打包席位金额串 -> 列表（与席位名一一对应，T12）。

    正常响应金额不含千分位逗号（作者实测 5 元素一一对应）；含千分位的残段
    （float 解析失败）并入下一段重试，仍不可解析的尾段落 None（不硬造）。
    """
    if value is None or not str(value).strip():
        return []
    out: List[Optional[float]] = []
    acc = ''
    for raw_tok in str(value).split(','):
        tok = raw_tok.strip()
        if acc:
            cand = f'{acc},{tok}'
        elif not tok:
            out.append(None)
            continue
        else:
            cand = tok
        try:
            out.append(float(cand))
            acc = ''
        except ValueError:
            acc = cand
    if acc:
        out.append(None)
    return out


def _sum_side(row_details: List[Dict[str, Any]], direction: str) -> Optional[float]:
    """按榜单行聚合席位金额（买/卖 Σ；无席位行返回 None 不硬造）"""
    vals = [d['amount'] for d in row_details
            if d['side'].startswith(direction) and d.get('amount') is not None]
    return round(sum(vals), 2) if vals else None
