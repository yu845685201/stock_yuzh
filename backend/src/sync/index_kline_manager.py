"""
指数日K线同步（方案 4.1，M1；tdx-api /api/index(/all) 唯一来源，无兜底）

- 指数清单来自 config review_sync.indices（北证50 实施首日实测，失败当日剔除并记录不阻塞）
- 首次全量 /api/index/all（6 请求）；每日增量 /api/index?limit=5（缺口 ≤5 个交易日自动补齐）
- 单位口径与 his_kline_day 一致：中间件价格/成交额为厘（÷1000），成交量手（×100）
- preclose 由前一条 close 补齐（最旧一条查库补），change_rate 自算（close/preclose-1，百分点）
- UpCount/DownCount 存 up_count/down_count（涨跌家数参考；精确家数由全市场日K自算）
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..config.config_manager import ConfigManager
from ..data_sources.tdx_api_source import TdxApiSource
from ..database.connection import DatabaseConnection
from ..utils.kline_normalize import normalize_date_str
from .review_common import code6_to_ts_code, write_review_report

logger = logging.getLogger(__name__)

_PRICE_SCALE = 1000.0   # 厘 -> 元
_VOLUME_SCALE = 100.0   # 手 -> 股

# config exchange -> tdx-api 代码前缀（/api/index 要求 sh000001 形式，2026-09-27 实测）
_EXCHANGE_PREFIX = {'sse': 'sh', 'szse': 'sz', 'bse': 'bj'}


def _tdx_code(idx: Dict[str, str]) -> str:
    """指数清单项 -> tdx-api 代码（sse/szse/bse 前缀 + 6 位代码）"""
    prefix = _EXCHANGE_PREFIX.get(str(idx.get('exchange') or '').lower(), '')
    return f'{prefix}{idx["code"]}'


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


class IndexKlineManager:
    """指数日K：尾部增量/全量 -> 单位换算 -> preclose 链 -> upsert trade_index_info"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        tdx_config = dict(self.config.get('data_sources', {}).get('tdx_api', {}) or {})
        self.source = TdxApiSource(tdx_config)
        self.indices: List[Dict[str, str]] = list(
            (self.config.get('review_sync', {}) or {}).get('indices', []) or [])
        self.tail_limit = 5
        self.interval = float((self.config.get('review_sync', {}) or {}).get('index_interval', 0.2))

    def execute(self, init_mode: bool = False, codes: Optional[List[str]] = None,
                dry_run: bool = False) -> Dict[str, Any]:
        started = datetime.now()
        indices = self._select_indices(codes)
        stats: Dict[str, Any] = {
            'indices_total': len(indices), 'indices_ok': 0, 'rows': 0,
            'failed': [], 'skipped': [],
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
        try:
            if not indices:
                result['errors'].append('指数清单为空（config review_sync.indices）')
                return result
            if not self.source.connect():
                result['errors'].append('tdx-api 连接失败')
                return result

            for idx in indices:
                code, name = idx['code'], idx.get('name', code6_to_ts_code(idx['code']))
                tdx_code = _tdx_code(idx)
                try:
                    if self.interval > 0:
                        import time
                        time.sleep(self.interval)
                    klines = (self.source.get_index_kline_all(tdx_code) if init_mode
                              else self.source.get_index_kline_tail(tdx_code, self.tail_limit))
                    if not klines:
                        # 北证50 实测不可取等场景：当日剔除并记录，不阻塞其余指数
                        stats['skipped'].append(f'{name}({code}) 无数据')
                        continue
                    rows = self._build_rows(idx, klines)
                    if dry_run:
                        stats['rows'] += len(rows)
                        stats['indices_ok'] += 1
                        continue
                    inserted = self.db.upsert_index_klines(rows)
                    stats['rows'] += len(rows)
                    stats['indices_ok'] += 1
                    logger.info(f'指数 {name}({code}) upsert {inserted} 行')
                except Exception as e:
                    logger.exception(f'指数 {name}({code}) 同步失败: {e}')
                    stats['failed'].append(f'{name}({code}): {e}')
            result['success'] = not stats['failed'] and not stats['skipped'] and stats['indices_ok'] > 0
            if stats['skipped'] and stats['indices_ok'] > 0:
                result['success'] = True  # 部分指数缺失不算整体失败（报告标注）
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
        return result

    # ---------- 行构造 ----------

    def _build_rows(self, idx: Dict[str, str], klines: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        code, ts_code, name = idx['code'], idx.get('ts_code') or code6_to_ts_code(idx['code']), idx.get('name', '')
        # 日期升序组织（tdx 返回新→旧）
        items: List[Dict[str, Any]] = []
        for k in klines:
            if not isinstance(k, dict):
                continue
            trade_date = normalize_date_str(k.get('Time') or k.get('time'))
            if not trade_date:
                continue
            items.append((trade_date, k))
        items.sort(key=lambda x: x[0])

        # 最旧一条的 preclose 查库补齐（尾部增量场景）
        prev_close = None
        if items:
            oldest = items[0][0]
            prev_close = self.db.fetch_index_kline_prev_close(ts_code, oldest)

        rows: List[Dict[str, Any]] = []
        for trade_date, k in items:
            close = _to_float(k.get('Close')) or _to_float(k.get('close'))
            close = close / _PRICE_SCALE if close else None
            if close is None:
                continue
            if prev_close is None:
                preclose = close  # 全历史首日无昨收，preclose=close（涨跌幅 0）
            else:
                preclose = prev_close
            change_rate = round((close / preclose - 1) * 100, 6) if preclose else None
            rows.append({
                'ts_code': ts_code,
                'index_code': code,
                'index_name': name,
                'trade_date': trade_date,
                'open': self._scale_price(k.get('Open') or k.get('open')),
                'high': self._scale_price(k.get('High') or k.get('high')),
                'low': self._scale_price(k.get('Low') or k.get('low')),
                'close': close,
                'preclose': round(preclose, 4),
                'volume': self._scale_volume(k.get('Volume') or k.get('volume')),
                'amount': self._scale_price(k.get('Amount') or k.get('amount')),
                'change_rate': change_rate,
                'up_count': k.get('UpCount') or k.get('up_count'),
                'down_count': k.get('DownCount') or k.get('down_count'),
            })
            prev_close = close
        return rows

    @staticmethod
    def _scale_price(value: Any) -> Optional[float]:
        v = _to_float(value)
        return v / _PRICE_SCALE if v else None

    @staticmethod
    def _scale_volume(value: Any) -> Optional[float]:
        v = _to_float(value)
        return v * _VOLUME_SCALE if v else None

    def _select_indices(self, codes: Optional[List[str]]) -> List[Dict[str, str]]:
        if not codes:
            return self.indices
        wanted = set(c.strip() for c in codes if c.strip())
        return [i for i in self.indices if i['code'] in wanted or i.get('ts_code') in wanted]

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 指数：{stats['indices_total']} 个（成功 {stats['indices_ok']}，"
            f"剔除 {len(stats['skipped'])}，失败 {len(stats['failed'])}）",
            f"- 落库行数：{stats['rows']}",
        ]
        if stats['skipped']:
            lines.append(f"- 剔除清单：{stats['skipped']}（次日增量自动补齐）")
        if stats['failed']:
            lines.append(f"- 失败清单：{stats['failed']}")
        status = '成功' if result['success'] else '存在失败'
        return write_review_report('指数日K同步报告', 'index_kline_sync_report',
                                   status, lines, result.get('duration'))
