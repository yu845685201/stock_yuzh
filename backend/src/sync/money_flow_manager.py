"""
主力资金流近似 —— 分笔自算（方案 4.10，M3；口径自定义项）

- 范围：每日异动股子集（当日涨停 ∪ 跌停 ∪ |涨跌幅|≥7% ∪ 成交额 Top50，合并去重），
  子集由当日全市场日K现算（涨停/跌停阈值口径复用 analysis.limit_up_rules）
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
from .review_common import code6_to_ts_code, ts_code_to_code6, write_review_report

logger = logging.getLogger(__name__)

_PRICE_SCALE = 1000.0  # 分笔价格厘 -> 元
_THROTTLE_FACTOR = 2.0


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


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
        self.top_amount_n = int(mf.get('top_amount_n', 50))
        self.pct_threshold = float(mf.get('pct_threshold', 0.07))
        self.gray_limit = int(mf.get('gray_limit', 200))
        self.throttle_on_timeout_pct = float(mf.get('throttle_on_timeout_pct', 0.05))
        self._rate_lock = threading.Lock()
        self._last_request_ts = 0.0
        self._interval_holder = [self.request_interval]  # 自动降速共享（分批提交间生效）

    # ---------- 子集计算 ----------

    def select_abnormal_stocks(self, trade_date: str) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
        """当日异动子集（涨停∪跌停∪|涨跌幅|≥阈值∪成交额TopN，合并去重）。

        返回 (子集行 [{ts_code, stock_code, stock_name, amount, close, high, low, change_rate, is_st}], 统计)
        """
        snapshot = self.db.fetch_day_kline_snapshot(trade_date)
        if not snapshot:
            raise ReviewSyncFailure(f'当日 {trade_date} 全市场日K为空——先跑 sync-kline-day')

        limit_hits: List[Dict[str, Any]] = []
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
            if abs(change_rate) >= self.pct_threshold * 100:
                pct_hits.append(row)

        # 成交额 TopN（amount 非空优先）
        with_amount = [( _to_float(r.get('amount')) or 0.0, r) for r in snapshot
                       if (_to_float(r.get('amount')) or 0.0) > 0]
        with_amount.sort(key=lambda x: x[0], reverse=True)
        top_amount = [r for _, r in with_amount[:self.top_amount_n]]

        merged: Dict[str, Dict[str, Any]] = {}
        for row in limit_hits + pct_hits + top_amount:
            ts_code = row.get('ts_code')
            if ts_code and ts_code not in merged:
                merged[ts_code] = row

        stocks = list(merged.values())
        if len(stocks) > self.gray_limit:
            # 灰度期压量：按 异动优先级（涨跌停 > 大涨跌 > Top成交额）已天然有序合并，
            # 超量时保留排序在前者（成交额 Top 靠后剔除）
            stocks = stocks[:self.gray_limit]
        stats = {
            'snapshot_total': len(snapshot),
            'limit_hits': len(limit_hits), 'pct_hits': len(pct_hits),
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
            pending = [s for s in stocks if s['ts_code'] not in existing or codes]
            logger.info(f"资金流 {trade_date}：子集 {len(stocks)} 只，待抓 {len(pending)} 只（已入库跳过）")

            done_count = 0
            failed_timeouts = 0
            rows: List[Dict[str, Any]] = []
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
                            row, timed_out = future.result()
                            if timed_out:
                                failed_timeouts += 1
                            if row is not None:
                                rows.append(row)
                                stats['stocks_ok'] += 1
                                stats['total_trades'] += row['stock_count']
                            else:
                                stats['stocks_failed'] += 1
                                stats['failed'].append(stock['ts_code'])
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
            result['success'] = stats['stocks_failed'] == 0 and stats['stocks_ok'] > 0
            if not pending:
                result['success'] = True
        except ReviewSyncFailure as e:
            result['errors'].append(str(e))
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
        return result

    # ---------- 单股处理 ----------

    def _rate_limit(self) -> None:
        """全局串行间隔（并发 worker 共享节拍；间隔可被超时降速调大）"""
        with self._rate_lock:
            now = time.time()
            wait = self._interval_holder[0] - (now - self._last_request_ts)
            if wait > 0:
                time.sleep(wait)
            self._last_request_ts = time.time()

    def _process_stock(self, stock: Dict[str, Any], trade_date: str) -> Tuple[Optional[Dict[str, Any]], bool]:
        """抓分笔 + 分桶计算；返回 (row|None, 是否超时)"""
        code6 = stock.get('stock_code') or ts_code_to_code6(stock['ts_code'])
        self._rate_limit()
        trades, timed_out = self._fetch_trades(code6, trade_date)
        if not trades:
            return None, timed_out
        total_amt = _to_float(stock.get('amount'))
        row = self.compute_money_flow(trades, total_amt)
        row.update({
            'trade_date': trade_date,
            'ts_code': stock['ts_code'],
            'calib_note': '近似口径:分笔分桶',
        })
        return row, timed_out

    def _fetch_trades(self, code6: str, trade_date: str) -> Tuple[List[Dict[str, Any]], bool]:
        """当日不带 date（1800/次聚合）；历史带 date（2000/次，可取昨日及之前）"""
        today = datetime.now().strftime('%Y%m%d')
        try:
            if trade_date == today:
                return self.source.get_minute_trade_history(code6), False
            return self.source.get_minute_trade_history(code6, trade_date), False
        except Exception as e:
            if 'timed out' in str(e).lower() or 'timeout' in str(e).lower():
                return [], True
            logger.warning(f'{code6} 分笔抓取失败: {e}')
            return [], False

    def compute_money_flow(self, trades: List[Dict[str, Any]],
                           total_amt: Optional[float]) -> Dict[str, Any]:
        """分桶计算（纯函数，便于单测）。

        Status=0 买盘主动 / 1 卖盘主动 / 其余中性盘按价归向（tick rule，平价继承前一笔方向，
        首笔中性盘剔除）。大单 = 单笔金额 ≥ big_amount_threshold。
        """
        big_buy = 0.0
        big_sell = 0.0
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
            'stock_count': total_trades,
        }

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        sub = stats['subset']
        lines = [
            f"- 数据日期：{stats['trade_date']}；口径：近似口径:分笔分桶（方案 4.10）",
            f"- 子集：快照 {sub.get('snapshot_total', 0)} 只 -> 涨跌停 {sub.get('limit_hits', 0)}，"
            f"|涨跌幅|≥{self.pct_threshold * 100:.0f}% {sub.get('pct_hits', 0)}，"
            f"成交额Top{self.top_amount_n} {sub.get('top_amount', 0)} -> 选中 {sub.get('selected', 0)} 只"
            f"（灰度上限 {self.gray_limit}）",
            f"- 抓取：成功 {stats['stocks_ok']}，失败 {stats['stocks_failed']}，"
            f"分笔总数 {stats['total_trades']}",
            f"- 大单阈值：{self.big_amount_threshold:.0f} 元；自动降速触发：{'是' if stats['timeout_throttle'] else '否'}",
        ]
        if stats['failed']:
            lines.append(f"- 失败清单（次日历史路径补抓）：{stats['failed'][:50]}")
        status = '成功' if result['success'] else ('部分失败' if stats['stocks_ok'] else '失败')
        return write_review_report('分笔资金流同步报告', 'money_flow_sync_report',
                                   status, lines, result.get('duration'))


class ReviewSyncFailure(RuntimeError):
    """manager 级失败（如当日日K未同步）"""
