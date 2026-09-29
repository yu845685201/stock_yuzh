"""
复权因子同步（方案 4.2，M1；tdx-api 新接口 /api/ths-factor，/api/xdxr 作事件校验）

- 全量初始化：5400 股 × 1 请求，manifest 分批跨天（tmp/adjust_factor_manifest.json，急停可续）
- 日常增量：读取日K pipeline 落盘的除权触发清单 tmp/adjust_trigger_YYYYMMDD.json
  （kline_day 检测命中 qfq_rebase 的股票，每日几十只），仅对这些股重拉因子
- 因子跳变日交叉校验（可选）：/api/xdxr 事件日应含 Category=1
- 整轮失败不阻塞复盘（因子缺口的直接后果是历史前复权校验精度下降）
"""

import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..config.config_manager import ConfigManager
from ..data_sources.tdx_api_source import TdxApiSource
from ..database.connection import DatabaseConnection
from .review_common import (
    load_manifest,
    save_manifest,
    tmp_dir,
    ts_code_to_code6,
    write_review_report,
)

logger = logging.getLogger(__name__)

DEFAULT_MANIFEST = tmp_dir() / 'adjust_factor_manifest.json'


def adjust_trigger_path(trade_date: str) -> Any:
    """除权触发清单路径（kline_day pipeline 写，本 manager 读）"""
    return tmp_dir() / f'adjust_trigger_{trade_date}.json'


def read_adjust_trigger(trade_date: str) -> List[str]:
    """读取当日除权触发清单（ts_code 列表；文件缺失/损坏返回空——增量不硬抓）"""
    path = adjust_trigger_path(trade_date)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding='utf-8'))
        return [str(c) for c in (data.get('ts_codes') or [])]
    except Exception as e:
        logger.warning(f'除权触发清单读取失败，忽略: {e}')
        return []


class AdjustFactorManager:
    """复权因子：全量 manifest 断点 + 除权触发清单增量"""

    def __init__(self, config_manager: ConfigManager, manifest_path: Optional[Any] = None):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        tdx_config = dict(self.config.get('data_sources', {}).get('tdx_api', {}) or {})
        self.source = TdxApiSource(tdx_config)
        self.interval = float((self.config.get('review_sync', {}) or {}).get('factor_interval', 0.5))
        self.xdxr_check = bool((self.config.get('review_sync', {}) or {}).get('xdxr_check', False))
        self.manifest_path = manifest_path or DEFAULT_MANIFEST

    def execute(self, init_mode: bool = False, trade_date: Optional[str] = None,
                codes: Optional[List[str]] = None, dry_run: bool = False) -> Dict[str, Any]:
        """执行采集。

        Args:
            init_mode: 全量初始化（全市场，manifest 断点）
            trade_date: 增量模式读取该日除权触发清单（yyyyMMdd，缺省当日）
            codes: 指定股票（优先级最高，兼容 ts_code/6 位）
            dry_run: 只统计目标不请求
        """
        started = datetime.now()
        if codes:
            target = [ts_code_to_code6(c) for c in codes]
            mode = f'指定 {len(target)} 只'
        elif init_mode:
            target = self._all_stock_codes()
            mode = '全量初始化'
        else:
            trade_date = trade_date or datetime.now().strftime('%Y%m%d')
            # 触发清单存的是 ts_code（sh.600577 格式），统一归一为 6 位裸代码
            # （全量/增量路径格式不一致曾致增量全部报"代码长度错误"，见问题记录 P-09）
            target = [ts_code_to_code6(c) for c in read_adjust_trigger(trade_date)]
            mode = f'增量（{trade_date} 除权触发清单 {len(target)} 只）'

        stats: Dict[str, Any] = {
            'mode': mode, 'targets': len(target), 'rows': 0, 'stocks_ok': 0,
            'stocks_empty': 0, 'failed': [], 'xdxr_mismatch': 0,
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
        manifest = load_manifest(self.manifest_path)

        try:
            if not self.source.connect():
                result['errors'].append('tdx-api 连接失败')
                return result
            if dry_run:
                stats['rows'] = 0
                result['success'] = True
                return result

            for i, code in enumerate(target, 1):
                if init_mode:
                    done = manifest.get('stocks', {}).get(code, {}).get('done')
                    if done:
                        stats['stocks_ok'] += 1
                        continue
                try:
                    n, mismatch = self._sync_one(code)
                    stats['rows'] += n
                    if n > 0:
                        stats['stocks_ok'] += 1
                    else:
                        # 空结果多为 THS 瞬时拦截：不记 done，manifest 标 failed 供重试轮找回
                        stats['stocks_empty'] += 1
                        stats['failed'].append(code)
                        if init_mode:
                            manifest.setdefault('stocks', {})[code] = {
                                'done': False, 'error': 'ths_empty',
                                'updated_at': datetime.now().isoformat(timespec='seconds')}
                            save_manifest(self.manifest_path, manifest)
                    stats['xdxr_mismatch'] += mismatch
                    if init_mode and n > 0:
                        manifest.setdefault('stocks', {})[code] = {
                            'done': True, 'rows': n, 'updated_at': datetime.now().isoformat(timespec='seconds')}
                        save_manifest(self.manifest_path, manifest)
                except Exception as e:
                    logger.exception(f'{code} 复权因子采集失败: {e}')
                    stats['failed'].append(code)
                    if init_mode:
                        manifest.setdefault('stocks', {}).setdefault(code, {})['error'] = str(e)
                        save_manifest(self.manifest_path, manifest)
                if init_mode and i % 50 == 0:
                    self._log_progress(stats, started)
            result['success'] = not stats['failed']
            # 全量模式末轮：对 manifest 中失败/空结果股票自动重试一轮（THS 拦截多为瞬时）
            if init_mode and stats['failed']:
                retry_list = list(dict.fromkeys(stats['failed']))
                logger.info(f'末轮重试 {len(retry_list)} 只（THS 拦截/异常找回）')
                recovered = 0
                for code in retry_list:
                    try:
                        n, _ = self._sync_one(code)
                        if n > 0:
                            recovered += 1
                            stats['rows'] += n
                            manifest.setdefault('stocks', {})[code] = {
                                'done': True, 'rows': n,
                                'updated_at': datetime.now().isoformat(timespec='seconds')}
                            save_manifest(self.manifest_path, manifest)
                        if code in stats['failed']:
                            stats['failed'] = [c for c in stats['failed'] if c != code]
                    except Exception as e:
                        logger.warning(f'重试 {code} 仍失败: {e}')
                logger.info(f'末轮重试找回 {recovered}/{len(retry_list)} 只')
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
        return result

    def _sync_one(self, code: str) -> tuple:
        """单股：拉因子全历史 -> upsert；xdxr 交叉校验（跳变日应含 Category=1）

        跳变判定用相对阈值：q_factor 相邻变化 >0.2% 才算真跳变（分红送转最小影响
        远超此值；全量实测 1e-9 绝对阈值会把 THS 价格浮点噪声误判为跳变，产生
        千万级假阳性，2026-09-28 校准）。
        """
        factors = self.source.get_ths_factor(code)
        rows = []
        prev_q = None
        jump_dates: List[str] = []
        for f in factors:
            if not isinstance(f, dict):
                continue
            trade_date = str(f.get('date') or '')
            if len(trade_date) != 8 or not trade_date.isdigit():
                continue
            q = _to_float(f.get('q_factor'))
            h = _to_float(f.get('h_factor'))
            if q is None:
                continue
            if prev_q is not None and prev_q > 0 and abs(q - prev_q) / prev_q > 0.002:
                jump_dates.append(trade_date)
            prev_q = q
            rows.append({'ts_code': _ts(code), 'trade_date': trade_date,
                         'qfq_factor': q, 'hfq_factor': h})
        if rows and self.interval > 0:
            time.sleep(self.interval)
        if rows:
            self.db.upsert_adjust_factors(rows)

        mismatch = 0
        if jump_dates and self.xdxr_check:
            # 校验默认关闭（xdxr_check=false）：因子由 THS qfq 与 TDX raw 双源比值逐日算出，
            # 两源历史口径差异导致段内因子存在 >0.2% 的逐日漂移，逐跳变日对齐事件日会产生
            # 90%+ 噪声误报（实测 9 只股 9476/21865）。方案 3.2 的验收方式是抽样人工比对，
            # 此处仅保留开关供诊断使用。
            events = self.source.get_xdxr(code)
            event_dates = {str(e.get('date') or '')[:8] for e in events if isinstance(e, dict)}
            mismatch = sum(1 for d in jump_dates if d not in event_dates)
        return len(rows), mismatch

    def _all_stock_codes(self) -> List[str]:
        stocks = self.db.fetch_stock_basic()
        return sorted({ts_code_to_code6(s['ts_code']) for s in stocks if s.get('ts_code')})

    @staticmethod
    def _log_progress(stats: Dict[str, Any], started: datetime) -> None:
        done = stats['stocks_ok'] + stats['stocks_empty'] + len(stats['failed'])
        elapsed = (datetime.now() - started).total_seconds()
        eta_min = elapsed / done * (stats['targets'] - done) / 60 if done else 0
        logger.info(f"复权因子进度 {done}/{stats['targets']}，ETA {eta_min:.0f} 分钟")

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 模式：{stats['mode']}",
            f"- 目标 {stats['targets']} 只，成功 {stats['stocks_ok']}（空 {stats['stocks_empty']}），"
            f"失败 {len(stats['failed'])}",
            f"- 落库行数：{stats['rows']}",
            f"- xdxr 交叉校验不一致（因子跳变日无 Category=1 事件）：{stats['xdxr_mismatch']} 处",
            f"- manifest：{self.manifest_path}",
        ]
        if stats['failed']:
            lines.append(f"- 失败清单：{stats['failed'][:50]}")
        status = '成功' if result['success'] else '存在失败'
        return write_review_report('复权因子同步报告', 'adjust_factor_sync_report',
                                   status, lines, result.get('duration'))


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ts(code6: str) -> str:
    from .review_common import code6_to_ts_code
    return code6_to_ts_code(code6)
