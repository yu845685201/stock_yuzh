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

import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ..config.config_manager import ConfigManager
from ..data_sources.http_collector_base import ReviewBlockedError
from ..data_sources.sse_szse_source import SSE_REF_TYPE_NAMES, SzseSseSource
from ..database.connection import DatabaseConnection
from .review_common import (clean_stock_name, code6_to_ts_code, expand_trade_days,
                            write_review_report)

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

    def execute(self, trade_date: Optional[str] = None, dry_run: bool = False,
                write_report: bool = True) -> Dict[str, Any]:
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
                # X-18 先删后插：沪市旧格式行（'披露原因代码{N}'）与新文案映射在唯一键
                # (trade_date, ts_code, reason) 下会并存致同股重复上榜；重抓日期先删
                if sse_stocks:
                    stats['sse_legacy_deleted'] = self.db.delete_sse_lhb_legacy(trade_date)
                if stock_rows:
                    self.db.upsert_lhb_stocks(stock_rows)
                if detail_rows:
                    self.db.upsert_lhb_details(detail_rows)
            result['success'] = bool(stock_rows)
        except ReviewBlockedError as e:
            result['blocked'] = True
            result['errors'].append(f'官网风控急停（本日未入库，稍后重跑补）: {e}')
            logger.warning(f'龙虎榜风控急停 {trade_date}: {e}')
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
            'days_failed': [], 'last_completed_date': None,
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [],
                                  'blocked': False, 'backfill': True}
        for d in days:
            if not dry_run and self.db.fetch_lhb_row_count(d) > 0:
                stats['days_skipped'] += 1
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

    def _collect_szse(self, trade_date: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """深交所榜单行 + 席位明细。

        金额口径（X-7/V1.2）：每行 buy/sell/net 用**当次抓到的该行席位**聚合
        （买 Σ、卖 Σ、差为净；席位 3~8 个不满额属正常，T19）——按榜单行粒度，
        禁止按股票跨原因求和。席位抓取失败时金额留空（不硬造）。
        """
        stocks: List[Dict[str, Any]] = []
        details: List[Dict[str, Any]] = []
        raw = self.source.fetch_szse_lhb(trade_date)
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
                    amount = _num_yuan(seat.get('mrje') if side.startswith('买')
                                       else seat.get('mcje'), 1.0)
                    row_details.append({
                        'trade_date': trade_date,
                        'ts_code': ts_code,
                        'seat_name': seat_name,
                        'side': side,
                        'amount': amount,
                    })
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
        return stocks, details

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
