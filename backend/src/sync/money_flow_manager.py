"""
主力资金流近似 —— 分笔自算（方案 4.10，M3；口径自定义项）

- 范围：每日异动股子集（当日涨停 ∪ 跌停 ∪ |涨跌幅|≥7% ∪ 成交额 Top50，合并去重；
  include_burst 开启时另含炸板——开发项 F 封板指标对炸板股全覆盖）
- 来源：tdx-api /api/minute-trade-all（当日不带 date、历史带 date=yyyyMMdd，
  历史接口可取昨日及之前 → 当日失败次日用历史路径补）
- 近似口径（写入 calib_note 与报告口径说明）：
  * 单笔金额 = Price(厘->元) × Volume(手) × 100
  * 方向：Status=0 买盘主动、1 卖盘主动；其余（2 中性/5 盘后定价等）按价格归向——
    与前一笔成交价比较，价升归买、价降归卖、平价归前一笔方向（首笔中性盘剔除），阈值参数化
  * 大单阈值：单笔金额 ≥ review_sync.money_flow.big_amount_threshold（默认 50 万元）
  * 输出：大单买入额/卖出额/净流入=买-卖/净流入占比=净流入÷当日成交额（成交额取日K，百分点）
- 灰度：gray_limit 子集上限起步（默认 200），并发 ≤3 + 串行间隔；
  超时率 > throttle_on_timeout_pct 自动降速（间隔 ×2）；单股失败不阻塞（记失败清单）

分笔封板指标（开发项 F，方案 4.6）：同一批分笔零额外请求派生 first_seal_time /
last_seal_time / open_count / sealed_ratio -> limit_seal_stats_daily。
口径红线：limit_price = 分笔 max（涨停/炸板）/ min（跌停）——绕开复权与浮点双重陷阱
（禁止改用理论价口径）；Time 为 ISO8601 带时区，按 09:25~15:00 截断（北交所盘后笔）。
历史日期抓取重试覆盖：预热空响应（X-4，合法空数据重试即得全量）+ 上游断连
（X-9，'closed network connection'，等待 30~60s 自愈）。
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ..analysis.limit_up_rules import board_of, limit_pct
from ..config.config_manager import ConfigManager
from ..data_sources.tdx_api_source import TdxApiSource
from ..database.connection import DatabaseConnection
from .review_common import (code6_to_ts_code, expand_trade_days,
                            ts_code_to_code6, write_review_report)

logger = logging.getLogger(__name__)

_PRICE_SCALE = 1000.0  # 分笔价格厘 -> 元
_THROTTLE_FACTOR = 2.0
#: 封板时段过滤（T22：北交所 15:00 后存在盘后笔，不截断会稀释 sealed_ratio）
_SEAL_SESSION_START = '092500'
_SEAL_SESSION_END = '150000'


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _parse_tick_time(value: Any) -> Optional[str]:
    """分笔时间戳 -> hhmmss（ISO8601 带时区如 2026-09-30T09:25:00+08:00，T22；
    兼容裸 HH:MM:SS）；解析失败返回 None"""
    s = str(value or '').strip()
    if not s:
        return None
    if 'T' in s:
        s = s.split('T', 1)[1]
    if '+' in s:
        s = s.split('+', 1)[0]
    if s.endswith('Z'):
        s = s[:-1]
    s = s.replace(':', '')
    if len(s) >= 6 and s[:6].isdigit():
        return s[:6]
    return None


def compute_seal_stats(trades: List[Dict[str, Any]], side: str = 'up') -> Optional[Dict[str, Any]]:
    """封板指标计算（开发项 F，纯函数便于单测）。

    Args:
        trades: 分笔原始行（Price 厘、Time、Volume、Status）
        side: 'up' = 涨停/炸板（limit_price=分笔 max）；'down' = 跌停（min）

    Returns:
        {limit_price, first_seal_time, last_seal_time, open_count, sealed_ratio}
        或 None（时段内无有效分笔）。sealed = 分笔价 == limit_price（同一分笔源内
        同价浮点一致，等值比较安全；绝不引入理论涨停价）。
    """
    ticks: List[Tuple[str, float]] = []
    for t in trades or []:
        if not isinstance(t, dict):
            continue
        price = _to_float(t.get('Price'))
        hhmmss = _parse_tick_time(t.get('Time'))
        if price is None or hhmmss is None:
            continue
        if not (_SEAL_SESSION_START <= hhmmss <= _SEAL_SESSION_END):
            continue  # 盘后/集合竞价外的笔不计（T22 时段过滤）
        ticks.append((hhmmss, price / _PRICE_SCALE))
    if not ticks:
        return None
    ticks.sort(key=lambda x: x[0])
    prices = [p for _, p in ticks]
    limit_price = max(prices) if side == 'up' else min(prices)
    sealed_flags = [p == limit_price for p in prices]

    first_seal = next((t for t, s in zip(ticks, sealed_flags) if s), None)
    last_seal = None
    for i, s in enumerate(sealed_flags):
        if s and (i == 0 or not sealed_flags[i - 1]):
            last_seal = ticks[i][0]  # 最后一段连续 sealed 的首笔时刻
    open_count = 0
    for i in range(1, len(sealed_flags)):
        if sealed_flags[i - 1] and not sealed_flags[i]:
            open_count += 1
    sealed_ratio = round(sum(sealed_flags) / len(sealed_flags), 4)
    return {
        'limit_price': round(limit_price, 4),
        'first_seal_time': first_seal[0] if first_seal else None,
        'last_seal_time': last_seal,
        'open_count': open_count,
        'sealed_ratio': sealed_ratio,
    }


class MoneyFlowManager:
    """分笔资金流：异动子集 -> 分笔抓取 -> 方向归并分桶 -> money_flow_daily"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        tdx_config = dict(self.config.get('data_sources', {}).get('tdx_api', {}) or {})
        self.source = TdxApiSource(tdx_config)
        review = self.config.get('review_sync', {}) or {}
        mf = review.get('money_flow', {}) or {}
        self.concurrent_workers = max(1, int(mf.get('concurrent_workers', 3)))
        self.request_interval = float(mf.get('request_interval', 0.15))
        self.big_amount_threshold = float(mf.get('big_amount_threshold', 500000))
        # P2 全市场扩面（量价资金方案 V1.1：用户决策不灰度直接开）+ 超大单阈值
        self.full_market = bool(mf.get('full_market', False))
        self.super_big_threshold = float(mf.get('super_big_threshold', 1000000))
        self.top_amount_n = int(mf.get('top_amount_n', 50))
        self.pct_threshold = float(mf.get('pct_threshold', 0.07))
        self.gray_limit = int(mf.get('gray_limit', 200))
        self.throttle_on_timeout_pct = float(mf.get('throttle_on_timeout_pct', 0.05))
        # F：日常子集是否纳入炸板（触板未封）——保证封板指标对炸板股全覆盖（约 +20 只/日）
        self.include_burst = bool(mf.get('include_burst', False))
        self._rate_lock = threading.Lock()
        self._last_request_ts = 0.0
        self._interval_holder = [self.request_interval]  # 自动降速共享（分批提交间生效）

    # ---------- 子集计算 ----------

    def select_abnormal_stocks(self, trade_date: str, seal_only: bool = False,
                               full_market: Optional[bool] = None
                               ) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        """当日异动子集（涨停∪跌停∪|涨跌幅|≥阈值∪成交额TopN，合并去重）。

        full_market=True（P2 全市场扩面，config review_sync.money_flow.full_market）
        时返回全部有成交行（停牌/无成交排除——分笔可得性与子集口径前提一致）。

        seal_only=True（F 封板回填）：仅涨停∪跌停∪炸板（强制含炸板），不带
        涨跌幅/成交额扩充——回补请求量按 ~90 只/日估算的前提。

        返回 (子集行 [{ts_code, stock_code, stock_name, amount, close, high, low, change_rate, is_st}], 统计)
        """
        use_full = self.full_market if full_market is None else bool(full_market)

        snapshot = self.db.fetch_day_kline_snapshot(trade_date)
        if not snapshot:
            raise ReviewSyncFailure(f'当日 {trade_date} 全市场日K为空——先跑 sync-kline-day')

        limit_hits: List[Dict[str, Any]] = []
        burst_hits: List[Dict[str, Any]] = []
        pct_hits: List[Dict[str, Any]] = []
        for row in snapshot:
            change_rate = _to_float(row.get('change_rate'))
            close, high, low = _to_float(row.get('close')), _to_float(row.get('high')), _to_float(row.get('low'))
            if change_rate is None or close is None:
                continue
            stock_code = str(row.get('stock_code') or '')
            is_st = bool(row.get('is_st'))
            pct = limit_pct(board_of(stock_code), trade_date, is_st)
            # 涨停/跌停：change_rate 阈值法（制度上限 -0.10 浮点余量）+ close 触及极值护栏
            if pct is not None and high is not None and low is not None:
                threshold = pct - 0.10
                if change_rate >= threshold and close >= high:
                    limit_hits.append(row)
                    continue
                if change_rate <= -threshold and close <= low:
                    limit_hits.append(row)
                    continue
                # F：炸板（触板未封住）——high 触及涨停阈值、收盘未封高（始终计算，
                # 是否并入日常子集由 include_burst 决定；seal_only 回填路径必含）
                preclose = _to_float(row.get('preclose'))
                if high is not None and preclose and preclose > 0:
                    high_gain = (high - preclose) / preclose * 100.0
                    if high_gain >= threshold:
                        burst_hits.append(row)
                        continue
            if abs(change_rate) >= self.pct_threshold * 100:
                pct_hits.append(row)

        # 成交额 TopN（amount 非空优先）
        with_amount = [( _to_float(r.get('amount')) or 0.0, r) for r in snapshot
                       if (_to_float(r.get('amount')) or 0.0) > 0]
        with_amount.sort(key=lambda x: x[0], reverse=True)
        top_amount = [r for _, r in with_amount[:self.top_amount_n]]

        merged: Dict[str, Dict[str, Any]] = {}
        if use_full and not seal_only:
            # P2 全市场：全部有成交行（灰度上限与异动扩充均不生效）
            for row in snapshot:
                ts_code = row.get('ts_code')
                if ts_code and (_to_float(row.get('amount')) or 0.0) > 0:
                    merged[ts_code] = row
            stats = {
                'snapshot_total': len(snapshot), 'full_market': len(merged),
                'selected': len(merged),
            }
            return list(merged.values()), stats
        if seal_only:
            base = limit_hits + burst_hits      # 涨停∪跌停∪炸板（F 回填子集）
        elif self.include_burst:
            base = limit_hits + burst_hits + pct_hits + top_amount
        else:
            base = limit_hits + pct_hits + top_amount
        for row in base:
            ts_code = row.get('ts_code')
            if ts_code and ts_code not in merged:
                merged[ts_code] = row

        stocks = list(merged.values())
        if len(stocks) > self.gray_limit and not seal_only:
            # 灰度期压量：按 异动优先级（涨跌停 > 炸板 > 大涨跌 > Top成交额）已天然有序合并，
            # 超量时保留排序在前者（成交额 Top 靠后剔除）
            stocks = stocks[:self.gray_limit]
        stats = {
            'snapshot_total': len(snapshot),
            'limit_hits': len(limit_hits), 'burst_hits': len(burst_hits),
            'pct_hits': len(pct_hits),
            'top_amount': len(top_amount), 'selected': len(stocks),
        }
        return stocks, stats

    # ---------- 主流程 ----------

    def execute(self, trade_date: Optional[str] = None, codes: Optional[List[str]] = None,
                dry_run: bool = False) -> Dict[str, Any]:
        started = datetime.now()
        trade_date = trade_date or datetime.now().strftime('%Y%m%d')
        stats: Dict[str, Any] = {
            'trade_date': trade_date, 'subset': {}, 'stocks_total': 0, 'stocks_ok': 0,
            'stocks_failed': 0, 'failed': [], 'rows': 0,
            'timeout_throttle': False, 'total_trades': 0,
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
        try:
            if codes:
                stocks = []
                for c in codes:
                    code6 = ts_code_to_code6(c)
                    stocks.append({'ts_code': code6_to_ts_code(code6), 'stock_code': code6,
                                   'stock_name': '', 'amount': None})
                stats['subset'] = {'selected': len(stocks), 'forced_codes': True}
            else:
                stocks, stats['subset'] = self.select_abnormal_stocks(trade_date)
            stats['stocks_total'] = len(stocks)
            if dry_run:
                result['success'] = True
                return result
            if not self.source.connect():
                result['errors'].append('tdx-api 连接失败')
                return result

            existing = self.db.fetch_money_flow_codes(trade_date)
            if self.full_market:
                # P2 全市场：不跳过已有行（旧行可能缺分桶列，重算幂等覆盖）
                pending = stocks
            else:
                pending = [s for s in stocks if s['ts_code'] not in existing or codes]
            logger.info(f"资金流 {trade_date}：子集 {len(stocks)} 只，待抓 {len(pending)} 只（已入库跳过）")

            done_count = 0
            failed_timeouts = 0
            rows: List[Dict[str, Any]] = []
            seal_rows: List[Dict[str, Any]] = []
            stats['seal_rows'] = 0
            # 分批提交：超时率超阈值后对后续批次生效（间隔 ×2）
            chunk_size = max(self.concurrent_workers * 4, 8)
            for chunk_start in range(0, len(pending), chunk_size):
                chunk = pending[chunk_start:chunk_start + chunk_size]
                with ThreadPoolExecutor(max_workers=self.concurrent_workers) as executor:
                    futures = {executor.submit(self._process_stock, s, trade_date): s
                               for s in chunk}
                    for future in as_completed(futures):
                        stock = futures[future]
                        try:
                            row, seal_row, timed_out = future.result()
                            if timed_out:
                                failed_timeouts += 1
                            if row is not None:
                                rows.append(row)
                                stats['stocks_ok'] += 1
                                stats['total_trades'] += row['stock_count']
                            else:
                                stats['stocks_failed'] += 1
                                stats['failed'].append(stock['ts_code'])
                            if seal_row is not None:
                                seal_rows.append(seal_row)
                                stats['seal_rows'] = stats.get('seal_rows', 0) + 1
                        except Exception as e:
                            stats['stocks_failed'] += 1
                            stats['failed'].append(stock['ts_code'])
                            logger.exception(f"{stock.get('ts_code')} 资金流处理异常: {e}")
                        done_count += 1
                        if done_count % 25 == 0:
                            logger.info(f"资金流进度 {done_count}/{len(pending)}，成功 {stats['stocks_ok']}")
                if (not stats['timeout_throttle'] and done_count >= 10
                        and failed_timeouts / done_count > self.throttle_on_timeout_pct):
                    self._interval_holder[0] *= _THROTTLE_FACTOR
                    stats['timeout_throttle'] = True
                    logger.warning(f"分笔超时率超 {self.throttle_on_timeout_pct:.0%}，"
                                   f"请求间隔降速至 {self._interval_holder[0]:.2f}s")
            if rows:
                self.db.upsert_money_flows(rows)
                stats['rows'] = len(rows)
            if seal_rows:
                self.db.upsert_seal_stats(seal_rows)
            result['success'] = stats['stocks_failed'] == 0 and stats['stocks_ok'] > 0
            if not pending:
                result['success'] = True
        except ReviewSyncFailure as e:
            result['errors'].append(str(e))
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
        return result

    # ---------- 封板指标历史回填（开发项 F，方案 4.6 第 5 条） ----------

    def backfill_seal_stats(self, start_date: str, end_date: str, dry_run: bool = False) -> Dict[str, Any]:
        """逐日回填封板指标：子集 = 涨停 ∪ 炸板 ∪ 跌停（炸板股涨幅可 <7%，
        不在 money_flow 现行子集内，需独立选取）。

        - 断点续跑：limit_seal_stats_daily 已有该股行则跳过（逐股粒度，可中断重跑）；
        - 逐股取分笔（含 X-4/X-9 重试）→ 计算 → upsert money_flow + seal 两表；
        - 回补量：约 250 交易日 × ~90 只/日 ≈ 2.2 万次请求，分晚执行。
        """
        started = datetime.now()
        days = expand_trade_days(self.db, start_date, end_date)
        stats: Dict[str, Any] = {
            'range': f'{start_date}~{end_date}', 'days_total': len(days),
            'days_done': 0, 'days_no_subset': 0, 'stocks_ok': 0,
            'stocks_failed': 0, 'seal_rows': 0, 'money_rows': 0,
            'days_failed': [], 'last_completed_date': None,
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [],
                                  'blocked': False, 'backfill': True}
        try:
            if not self.source.connect():
                result['errors'].append('tdx-api 连接失败')
                return result
            for d in days:
                try:
                    subset, _ = self.select_abnormal_stocks(d, seal_only=True)
                except ReviewSyncFailure:
                    stats['days_no_subset'] += 1  # 当日无日K
                    continue
                existing = set() if dry_run else self.db.fetch_seal_stats_codes(d)
                pending = [s for s in subset if s['ts_code'] not in existing]
                if not pending:
                    stats['days_no_subset'] += 1 if not subset else 0
                    continue
                rows, seal_rows = [], []
                for stock in pending:
                    try:
                        row, seal_row, _ = self._process_stock(stock, d)
                    except Exception as e:
                        stats['stocks_failed'] += 1
                        logger.warning(f"{stock.get('ts_code')} 封板回填失败: {e}")
                        continue
                    if row is not None:
                        rows.append(row)
                        stats['stocks_ok'] += 1
                    else:
                        stats['stocks_failed'] += 1
                    if seal_row is not None:
                        seal_rows.append(seal_row)
                if not dry_run:
                    if rows:
                        self.db.upsert_money_flows(rows)
                    if seal_rows:
                        self.db.upsert_seal_stats(seal_rows)
                stats['seal_rows'] += len(seal_rows)
                stats['money_rows'] += len(rows)
                stats['days_done'] += 1
                stats['last_completed_date'] = d
                logger.info(f"封板回填 {d}：子集 {len(pending)}，成功 {stats['stocks_ok']}，"
                            f"seal {len(seal_rows)} 行")
            result['success'] = stats['stocks_failed'] == 0
        except ReviewSyncFailure as e:
            result['errors'].append(str(e))
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_backfill_report(result)
        return result

    def _write_backfill_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 区间：{stats['range']}（交易日 {stats['days_total']}：回填 {stats['days_done']}、"
            f"无子集/已完成 {stats['days_no_subset']}、失败 {len(stats['days_failed'])}）",
            f"- 累计：seal {stats['seal_rows']} 行 / money_flow {stats['money_rows']} 行；"
            f"个股成功 {stats['stocks_ok']}、失败 {stats['stocks_failed']}"
            f"（末完成日 {stats['last_completed_date']}）",
            "- 口径：limit_price=分笔 max/min（绕开复权与浮点陷阱）；时段 09:25~15:00 截断（T22）；"
            "历史抓取含 X-4 预热重试与 X-9 断连等待",
            "- 断点续跑：limit_seal_stats_daily 逐股粒度跳过，可中断重跑",
        ]
        if result['errors']:
            lines.append(f"- 错误：{result['errors']}")
        status = '成功' if result['success'] else '部分失败（失败日可重跑续补）'
        return write_review_report('封板指标历史回填报告', 'seal_stats_backfill_report',
                                   status, lines, result.get('duration'))

    # ---------- 单股处理 ----------

    def _rate_limit(self) -> None:
        """全局串行间隔（并发 worker 共享节拍；间隔可被超时降速调大）"""
        with self._rate_lock:
            now = time.time()
            wait = self._interval_holder[0] - (now - self._last_request_ts)
            if wait > 0:
                time.sleep(wait)
            self._last_request_ts = time.time()

    def _process_stock(self, stock: Dict[str, Any],
                       trade_date: str) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], bool]:
        """抓分笔 + 分桶计算 + 封板指标；返回 (money_flow行|None, seal行|None, 是否超时)"""
        code6 = stock.get('stock_code') or ts_code_to_code6(stock['ts_code'])
        self._rate_limit()
        trades, timed_out = self._fetch_trades(code6, trade_date)
        if not trades:
            return None, None, timed_out
        total_amt = _to_float(stock.get('amount'))
        row = self.compute_money_flow(trades, total_amt)
        row.update({
            'trade_date': trade_date,
            'ts_code': stock['ts_code'],
            'calib_note': '分笔分桶:全市场' if self.full_market else '分笔分桶:异动子集',
        })
        seal_row = self._seal_row_for(stock, trades, trade_date)
        return row, seal_row, timed_out

    def _seal_row_for(self, stock: Dict[str, Any],
                      trades: List[Dict[str, Any]], trade_date: str) -> Optional[Dict[str, Any]]:
        """封板指标行构造（开发项 F）：side 由日K 判定——close<=low 为跌停（min），
        其余（涨停/炸板）取 max；limit_pct 不可判定的板块跳过"""
        code6 = stock.get('stock_code') or ts_code_to_code6(stock['ts_code'])
        is_st = bool(stock.get('is_st'))
        if limit_pct(board_of(code6), trade_date, is_st) is None:
            return None
        close, low = _to_float(stock.get('close')), _to_float(stock.get('low'))
        side = 'down' if (close is not None and low is not None and close <= low) else 'up'
        stats = compute_seal_stats(trades, side=side)
        if stats is None:
            return None
        stats.update({'trade_date': trade_date, 'ts_code': stock['ts_code'],
                      'source': 'tdx_tick'})
        return stats

    def _fetch_trades(self, code6: str, trade_date: str) -> Tuple[List[Dict[str, Any]], bool]:
        """当日不带 date（1800/次聚合）；历史带 date（2000/次，可取昨日及之前）。

        历史分支暂时性错误重试（F 回补前置，方案 4.6 第 6 条）：
        - X-4 预热：历史日期首次请求表现为**合法空响应**（Go 侧 2s 超时预热失败），
          重试即得全量——现行 3 次通用重试只覆盖异常，不覆盖空数据，故在此显式重试；
        - X-9 断连：tdx 中间件上游长连接空闲断开（'closed network connection'），
          约 1 分钟自愈——等待 30s 后重试；
        - 重试安全性：子集来自当日日K（必有成交），"有成交但 0 分笔"实际不存在，
          休市日/停牌股在子集选择时已排除；
        - 现有 timed_out 降速语义保留（重试后仍超时才计入降速统计）。
        """
        today = datetime.now().strftime('%Y%m%d')
        if trade_date == today:
            try:
                return self.source.get_minute_trade_history(code6), False
            except Exception as e:
                if 'timed out' in str(e).lower() or 'timeout' in str(e).lower():
                    return [], True
                logger.warning(f'{code6} 分笔抓取失败: {e}')
                return [], False
        # 历史分支（含 X-4/X-9 重试）
        for attempt in range(3):
            try:
                trades = self.source.get_minute_trade_history(code6, trade_date)
            except Exception as e:
                msg = str(e).lower()
                if 'timed out' in msg or 'timeout' in msg:
                    return [], True
                if 'closed network connection' in msg:
                    logger.warning(f'{code6} tdx 上游断连（X-9），等待 30s 后重试'
                                   f'（第 {attempt + 1} 次）')
                    time.sleep(30)
                    continue
                logger.warning(f'{code6} 历史分笔抓取失败: {e}')
                return [], False
            if trades:
                return trades, False
            logger.info(f'{code6} {trade_date} 历史分笔空响应（X-4 预热），重试'
                        f'（第 {attempt + 1} 次）')
            time.sleep(2 + attempt * 3)
        logger.warning(f'{code6} {trade_date} 历史分笔重试后仍为空，判定失败')
        return [], False

    def compute_money_flow(self, trades: List[Dict[str, Any]],
                           total_amt: Optional[float]) -> Dict[str, Any]:
        """分桶计算（纯函数，便于单测）。

        Status=0 买盘主动 / 1 卖盘主动 / 其余中性盘按价归向（tick rule，平价继承前一笔方向，
        首笔中性盘剔除）。大单 = 单笔金额 ≥ big_amount_threshold。
        """
        big_buy = 0.0
        big_sell = 0.0
        sb_buy = 0.0
        sb_sell = 0.0
        mid_buy = 0.0
        mid_sell = 0.0
        small_buy = 0.0
        small_sell = 0.0
        total_trades = 0
        prev_price: Optional[float] = None
        prev_dir = 0  # 0 买 / 1 卖；中性盘平价继承
        for t in trades:
            if not isinstance(t, dict):
                continue
            price = _to_float(t.get('Price'))
            volume = _to_float(t.get('Volume'))
            if price is None or volume is None:
                continue
            price = price / _PRICE_SCALE
            amount = price * volume * 100.0  # 手 -> 股
            status = int(t.get('Status') or 0)
            if status == 0:
                direction = 0
            elif status == 1:
                direction = 1
            else:
                if prev_price is None:
                    prev_price = price
                    continue  # 首笔中性盘剔除
                if price > prev_price:
                    direction = 0
                elif price < prev_price:
                    direction = 1
                else:
                    direction = prev_dir
            prev_price = price
            prev_dir = direction
            total_trades += 1
            if amount >= self.big_amount_threshold:
                if direction == 0:
                    big_buy += amount
                else:
                    big_sell += amount
                # 超大单为大单的子集（≥super_big_threshold），big_* 口径不变
                if amount >= self.super_big_threshold:
                    if direction == 0:
                        sb_buy += amount
                    else:
                        sb_sell += amount
            elif amount >= 100000.0:
                if direction == 0:
                    mid_buy += amount
                else:
                    mid_sell += amount
            else:
                if direction == 0:
                    small_buy += amount
                else:
                    small_sell += amount

        big_net = big_buy - big_sell
        if total_amt is None or total_amt <= 0:
            big_net_pct = None  # 日K成交额缺失不硬造占比
        else:
            big_net_pct = round(big_net / total_amt * 100, 6)
        return {
            'total_amt': total_amt,
            'big_buy_amt': round(big_buy, 4),
            'big_sell_amt': round(big_sell, 4),
            'big_net_amt': round(big_net, 4),
            'big_net_pct': big_net_pct,
            'super_big_buy_amt': round(sb_buy, 4),
            'super_big_sell_amt': round(sb_sell, 4),
            'super_big_net_amt': round(sb_buy - sb_sell, 4),
            'mid_buy_amt': round(mid_buy, 4),
            'mid_sell_amt': round(mid_sell, 4),
            'mid_net_amt': round(mid_buy - mid_sell, 4),
            'small_buy_amt': round(small_buy, 4),
            'small_sell_amt': round(small_sell, 4),
            'small_net_amt': round(small_buy - small_sell, 4),
            'stock_count': total_trades,
        }

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        sub = stats['subset']
        lines = [
            f"- 数据日期：{stats['trade_date']}；口径：近似口径:分笔分桶（方案 4.10）",
            f"- 子集：快照 {sub.get('snapshot_total', 0)} 只 -> 涨跌停 {sub.get('limit_hits', 0)}，"
            f"炸板 {sub.get('burst_hits', 0)}，"
            f"|涨跌幅|≥{self.pct_threshold * 100:.0f}% {sub.get('pct_hits', 0)}，"
            f"成交额Top{self.top_amount_n} {sub.get('top_amount', 0)} -> 选中 {sub.get('selected', 0)} 只"
            f"（灰度上限 {self.gray_limit}）",
            f"- 抓取：成功 {stats['stocks_ok']}，失败 {stats['stocks_failed']}，"
            f"分笔总数 {stats['total_trades']}；封板指标 {stats.get('seal_rows', 0)} 行",
            f"- 大单阈值：{self.big_amount_threshold:.0f} 元；自动降速触发：{'是' if stats['timeout_throttle'] else '否'}",
        ]
        if stats['failed']:
            lines.append(f"- 失败清单（次日历史路径补抓）：{stats['failed'][:50]}")
        status = '成功' if result['success'] else ('部分失败' if stats['stocks_ok'] else '失败')
        return write_review_report('分笔资金流同步报告', 'money_flow_sync_report',
                                   status, lines, result.get('duration'))


class ReviewSyncFailure(RuntimeError):
    """manager 级失败（如当日日K未同步）"""
