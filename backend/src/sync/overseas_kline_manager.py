"""
外围日K采集（港美股指数 + 港股个股，技术方案 V1.0 20261004 §6）

- 源：腾讯 ifzq fqkline 单接口族（ifzq_source.py），串行限速，同 overseas_manager 骨架
- 范围：config review_sync.overseas_kline_indices（6 指数）+ overseas_kline_stocks
  （33 港股个股，A-H 两地公司带 a_code 供消费层映射，A 股侧复用 his_kline_day 不采集）
- 口径：trade_date 为市场自身交易日（非 A 股日历）；指数 adj_type=raw、个股 qfq（滚动）；
  A 股休市日外围行正常入库（假期外围观察的前提），下游按"A 股锚定日 → D 或 D 前
  最近市场交易日"映射
- 增量：读库内最新市场日，尾部 20 根窗口 upsert，幂等可重跑；代码无行时尾部 800 根
  种子并提示 --init 补全
- init：默认近 10 年按 ~1100 自然日分窗（≈760 交易日 < 单请求 800 根上限，
  4 请求/代码）
- 数据质量（方案 §7）：
  1) 自洽校验 day 末行 vs qt 现值（同日期才比，不一致告警不入库）；
  2) 双源对拍：美指/恒生 kline 末行 close ↔ sina gb_*/rt_hkHSI 实时（偏差 >0.5% 告警，
     不阻塞；sina 侧异常不影响本采集）；
  3) 冻结 tripwire：close 连续 ≥3 个市场日完全相同 → 标"疑似冻结"；
  4) qfq 重述检测：增量尾部与库内重叠日期 close 变化（公司行动后 qfq 基准滚动）→
     该代码全历史重拉，保证表内复权基准自洽
- a_code 完整性启动校验（须存在于 base_stock_info，缺失告警不阻塞）
"""

import logging
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from ..config.config_manager import ConfigManager
from ..data_sources.http_collector_base import ReviewBlockedError
from ..data_sources.ifzq_source import IfzqSource, IfzqSourceError
from ..data_sources.sina_source import SinaSource
from ..database.connection import DatabaseConnection
from .review_common import write_review_report

logger = logging.getLogger(__name__)

# 港股个股代码形如 hk00700（5 位数字）；hkHSI/hkHSCEI 等为指数
_STOCK_CODE_PATTERN = re.compile(r'^hk\d{5}$')

# init 分窗自然日跨度：≈760 交易日（按美股 252/年），余量低于单请求 800 根上限
# （10 年 → 4 窗，与方案 §6.1"4 请求/代码"一致）
_WINDOW_CALENDAR_DAYS = 1100

# 增量尾部窗口根数（方案 §6.1：count=20，1 请求/代码）
_INCREMENTAL_COUNT = 20
# 代码无行时的种子根数（尾部 3 年余量，完整历史提示 --init）
_SEED_COUNT = 800
# 分窗/种子请求的 count（窗口跨度已保证交易日数不超过此值）
_WINDOW_COUNT = 800

# 对拍容差（方案 §7.1：偏差 > 0.5% 记告警）
_CROSS_CHECK_TOL = 0.005
# qfq 重述判定容差（增量尾部重叠 close 与库内偏差超过即视为复权基准滚动/源修订）
_RESTATE_TOL = 0.001
# 冻结 tripwire：连续相同 close 的市场日数
_FROZEN_STREAK = 3

# kline 代码 ↔ sina 快照代码（双源对拍；仅 4 个有快照对应，hkHSCEI/hkHSTECH 无）
_CROSS_CHECK_PAIRS = [
    ('usDJI', 'gb_dji'), ('usIXIC', 'gb_ixic'), ('usINX', 'gb_$inx'),
    ('hkHSI', 'rt_hkHSI'),
]


def _fmt_d(v: str) -> str:
    """yyyyMMdd / yyyy-mm-dd → yyyy-mm-dd（ifzq param 形式）"""
    s = str(v).replace('-', '').strip()
    if len(s) != 8:
        raise ValueError(f'非法日期: {v}（应为 yyyyMMdd）')
    return f'{s[:4]}-{s[4:6]}-{s[6:8]}'


class OverseasKlineManager:
    """外围日K：腾讯 ifzq -> overseas_kline_day（市场自身交易日历）"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        self.source = IfzqSource(self.config)
        review = self.config.get('review_sync', {}) or {}
        self.indices: List[Dict[str, str]] = list(review.get('overseas_kline_indices', []) or [])
        self.stocks: List[Dict[str, str]] = list(review.get('overseas_kline_stocks', []) or [])
        self.init_years = int(review.get('overseas_kline_init_years', 10))
        # 代码注册表：code -> {name, a_code, is_stock, adj_type}（重复代码后到者忽略）
        self.registry: Dict[str, Dict[str, Any]] = {}
        for item in self.indices + self.stocks:
            code = (item.get('code') or '').strip()
            if not code or code in self.registry:
                continue
            is_stock = bool(_STOCK_CODE_PATTERN.match(code))
            self.registry[code] = {
                'name': item.get('name') or code,
                'a_code': (item.get('a_code') or '').strip() or None,
                'is_stock': is_stock,
                'adj_type': 'qfq' if is_stock else 'raw',
            }

    # ---------- 入口 ----------

    def execute(self, init_mode: bool = False, codes: Optional[List[str]] = None,
                start_date: Optional[str] = None, end_date: Optional[str] = None,
                dry_run: bool = False) -> Dict[str, Any]:
        """执行采集。

        - 缺省：增量（尾部 20 根/代码）
        - init_mode：全量初始化（近 init_years 年分窗）
        - start_date+end_date 同给：区间补采（回补模式）
        """
        started = datetime.now()
        mode = '区间补采' if (start_date and end_date) else ('全量初始化' if init_mode else '增量')
        stats: Dict[str, Any] = {
            'mode': mode, 'codes_total': 0, 'codes_ok': 0, 'rows': 0,
            'skipped_rows': 0, 'windows': 0, 'failed': [],
            'frozen_suspect': [], 'qfq_refresh': [], 'selfcheck_failed': [],
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [],
                                  'warnings': [], 'blocked': False, 'report_path': None}
        try:
            targets = self._resolve_targets(codes)
            stats['codes_total'] = len(targets)
            if not targets:
                result['errors'].append('采集清单为空（config review_sync.overseas_kline_'
                                        'indices / overseas_kline_stocks）')
                return result
            result['warnings'].extend(self._validate_a_codes())

            windows: Optional[List[Tuple[str, str]]] = None
            if start_date and end_date:
                windows = self._windows(date.fromisoformat(_fmt_d(start_date)),
                                        date.fromisoformat(_fmt_d(end_date)))
            elif init_mode:
                windows = self._init_windows()

            if dry_run:
                result['plan'] = self._dry_run_plan(targets, windows)
                result['success'] = True
                return result

            latest_before: Dict[str, Dict[str, Any]] = {}
            for code in targets:
                info = self.registry[code]
                try:
                    last, rows = self._collect_code(code, info, windows, stats)
                    if rows:
                        self.db.upsert_overseas_klines(rows)
                        stats['rows'] += len(rows)
                        latest_before[code] = last
                    stats['codes_ok'] += 1  # 窗口内无行（如 HSTECH 发布前）不算失败
                    self._frozen_tripwire(code, stats)
                except ReviewBlockedError:
                    result['blocked'] = True
                    result['errors'].append(f'{code} 风控急停（已完成代码已入库，重跑续）')
                    break
                except (IfzqSourceError, ValueError) as e:
                    stats['failed'].append(code)
                    result['errors'].append(f'{code}: {e}')
            # 双源对拍（best-effort，告警不阻塞）
            if latest_before:
                result['warnings'].extend(self._cross_check_with_sina(latest_before))
            result['success'] = stats['codes_ok'] > 0 and not result['blocked']
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            if not dry_run:
                result['report_path'] = self._write_report(result)
        return result

    # ---------- 采集 ----------

    def _collect_code(self, code: str, info: Dict[str, Any],
                      windows: Optional[List[Tuple[str, str]]],
                      stats: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        """单代码采集：init/区间走分窗，增量走尾部窗口；返回 (末行信息, 待入库行)。

        增量模式先做 qfq 重述检测：尾部与库内重叠日期 close 变化 → 全历史重拉
        （公司行动后 qfq 基准滚动，避免表内新旧基准拼接出人为跳变）。
        """
        if windows is None:
            latest = self.db.fetch_overseas_kline_latest_date(code)
            count = _INCREMENTAL_COUNT if latest else _SEED_COUNT
            if not latest:
                logger.info(f'[overseas_kline] {code} 库内无行，尾部 {count} 根种子'
                            f'（完整历史请执行 --init）')
            parsed = self._fetch_and_selfcheck(code, None, None, count, stats)
            restated = self._detect_restatement(code, parsed['rows'])
            if restated:
                logger.warning(f'[overseas_kline] {code} 检测到 qfq 重述 {restated}，全历史重拉')
                stats['qfq_refresh'].append(code)
                rows = self._fetch_windows(code, self._init_windows(), stats)
            else:
                rows = parsed['rows']
        else:
            rows = self._fetch_windows(code, windows, stats)
        for r in rows:
            r['adj_type'] = info['adj_type']  # 指数 raw / 个股 qfq（滚动口径）
        last = {'trade_date': rows[-1]['trade_date'], 'close': rows[-1]['close']} \
            if rows else None
        return last, rows

    def _fetch_windows(self, code: str, windows: List[Tuple[str, str]],
                       stats: Dict[str, Any]) -> List[Dict[str, Any]]:
        """按日期分窗逐窗拉取并合并（升序拼接）。

        美股端点（usfqkline）start 不生效、自 end 向后取 count 根——窗口跨度
        ~1100 自然日（≈760 交易日）< 800 根可整窗覆盖，多出的窗口起点之前行
        按窗口边界过滤（保持窗口互不重叠）。
        """
        all_rows: List[Dict[str, Any]] = []
        for start, end in windows:
            parsed = self._fetch_and_selfcheck(code, start, end, _WINDOW_COUNT, stats)
            stats['windows'] += 1
            for r in parsed['rows']:
                if start <= f'{r["trade_date"][:4]}-{r["trade_date"][4:6]}-{r["trade_date"][6:8]}' <= end:
                    all_rows.append(r)
        return all_rows

    def _init_windows(self) -> List[Tuple[str, str]]:
        end_d = date.today()
        start_d = end_d - timedelta(days=int(self.init_years * 365.25) + 1)
        return self._windows(start_d, end_d)

    def _fetch_and_selfcheck(self, code: str, start: Optional[str], end: Optional[str],
                             count: int, stats: Dict[str, Any]) -> Dict[str, Any]:
        """单窗口请求 + 自洽校验（方案 §6.1：day 末行 vs qt 不一致告警不入库）"""
        parsed = self.source.fetch_daily(code, start=start, end=end, count=count)
        stats['skipped_rows'] += parsed['skipped_rows']
        if parsed['skipped_rows']:
            logger.warning(f'[overseas_kline] {code} {start or "尾部"}~{end or ""} '
                           f'解析跳过 {parsed["skipped_rows"]} 行（如实不入库）')
        alarm = IfzqSource.self_check(parsed)
        if alarm:
            stats['selfcheck_failed'].append(code)
            raise IfzqSourceError(alarm)
        return parsed

    def _detect_restatement(self, code: str, rows: List[Dict[str, Any]]) -> List[str]:
        """增量尾部与库内重叠日期的 close 变化检测（返回重述日期清单）"""
        if not rows:
            return []
        db_closes = self.db.fetch_overseas_klines_since(code, rows[0]['trade_date'])
        if not db_closes:
            return []
        restated = []
        for r in rows:
            db_close = db_closes.get(r['trade_date'])
            if db_close is None or r['close'] is None:
                continue
            if abs(r['close'] - db_close) / db_close > _RESTATE_TOL:
                restated.append(r['trade_date'])
        return restated

    def _frozen_tripwire(self, code: str, stats: Dict[str, Any]) -> None:
        """冻结 tripwire：库内最近 3 个市场日 close 完全相同 → 标疑似冻结（告警不阻塞）"""
        closes = self.db.fetch_overseas_kline_recent_closes(code, _FROZEN_STREAK)
        if len(closes) == _FROZEN_STREAK and len(set(closes)) == 1:
            stats['frozen_suspect'].append(code)
            logger.warning(f'[overseas_kline] {code} close 连续 {_FROZEN_STREAK} 个市场日相同'
                           f'（{closes[0]}），疑似冻结')

    # ---------- 清单与校验 ----------

    def _resolve_targets(self, codes: Optional[List[str]]) -> List[str]:
        if not codes:
            return list(self.registry)
        unknown = [c for c in codes if c not in self.registry]
        if unknown:
            raise ValueError(f'未知代码: {unknown}（config 清单共 {len(self.registry)} 个）')
        return [c for c in self.registry if c in codes]

    def _validate_a_codes(self) -> List[str]:
        """a_code 完整性校验：须存在于 base_stock_info（告警不阻塞，方案 §5 决策 4）"""
        a_map = {info['a_code']: code for code, info in self.registry.items()
                 if info.get('a_code')}
        if not a_map:
            return []
        rows = self.db.execute_query(
            'SELECT ts_code FROM base_stock_info WHERE ts_code = ANY(%s)',
            (list(a_map),))
        found = {r['ts_code'] for r in rows}
        warnings = [f'{a_map[c]} 的 a_code={c} 不在 base_stock_info，A-H 映射将缺 A 侧'
                    for c in a_map if c not in found]
        for w in warnings:
            logger.warning(f'[overseas_kline] {w}')
        return warnings

    @staticmethod
    def _windows(start_d: date, end_d: date) -> List[Tuple[str, str]]:
        """[start, end] 按 _WINDOW_CALENDAR_DAYS 切窗（10 年 → 4 窗，与方案一致）"""
        windows = []
        cur = start_d
        while cur <= end_d:
            w_end = min(cur + timedelta(days=_WINDOW_CALENDAR_DAYS - 1), end_d)
            windows.append((cur.isoformat(), w_end.isoformat()))
            cur = w_end + timedelta(days=1)
        return windows

    def _dry_run_plan(self, targets: List[str],
                      windows: Optional[List[Tuple[str, str]]]) -> Dict[str, Any]:
        """试运行计划：目标清单（类型/复权/映射）+ 请求计划（窗口与请求数）"""
        items = []
        for code in targets:
            info = self.registry[code]
            latest = self.db.fetch_overseas_kline_latest_date(code)
            items.append({'code': code, 'name': info['name'],
                          'type': '个股' if info['is_stock'] else '指数',
                          'adj_type': info['adj_type'],
                          'a_code': info.get('a_code'), 'latest_in_db': latest})
        if windows is None:
            plan = {'mode': '增量', 'requests': len(targets),
                    'per_code': '尾部 20 根（无行代码尾部 800 根种子）'}
        else:
            plan = {'mode': '分窗', 'windows': [f'{s}~{e}' for s, e in windows],
                    'requests': len(targets) * len(windows)}
        return {'targets': items, 'plan': plan}

    # ---------- 双源对拍 ----------

    def _cross_check_with_sina(self, latest_before: Dict[str, Dict[str, Any]]) -> List[str]:
        """kline 末行 close ↔ sina 实时报价（gb_*/rt_hkHSI），偏差 >0.5% 告警。

        best-effort：sina 侧任何异常只记告警，不影响本采集结果（方案 §7.1 不阻塞）。
        """
        pairs = [(k, s) for k, s in _CROSS_CHECK_PAIRS if k in latest_before]
        if not pairs:
            return []
        try:
            sina = SinaSource(self.config)
            markets = [{'code': s, 'name': k} for k, s in pairs]
            quotes = {q['market_code']: q for q in sina.fetch_markets(markets)}
        except Exception as e:  # noqa: BLE001 —— 对拍失败不影响采集
            return [f'双源对拍未执行（sina 取数失败，不影响入库）: {e}']
        warnings: List[str] = []
        for ifzq_code, sina_code in pairs:
            last = latest_before.get(ifzq_code) or {}
            quote = quotes.get(sina_code) or {}
            k_close, s_close = last.get('close'), quote.get('close')
            if not k_close or not s_close:
                continue
            deviation = abs(s_close - k_close) / k_close
            if deviation > _CROSS_CHECK_TOL:
                warnings.append(
                    f'对拍偏差 {deviation:.2%}: {ifzq_code} {last["trade_date"]} close={k_close}'
                    f' vs {sina_code} 实时={s_close}（>{_CROSS_CHECK_TOL:.1%}，请点名核查）')
        return warnings

    # ---------- 报告 ----------

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f'- 模式：{stats["mode"]}；代码 {stats["codes_total"]} 个'
            f'（成功 {stats["codes_ok"]}，失败 {len(stats["failed"])}）',
            f'- 入库行数：{stats["rows"]}（解析跳过 {stats["skipped_rows"]} 行，'
            f'请求窗口 {stats["windows"]} 个）',
        ]
        if stats['failed']:
            lines.append(f'- 失败清单: {stats["failed"]}')
        if stats['selfcheck_failed']:
            lines.append(f'- 自洽校验失败（本轮未入库）: {stats["selfcheck_failed"]}')
        if stats['qfq_refresh']:
            lines.append(f'- qfq 重述全历史重拉: {stats["qfq_refresh"]}')
        if stats['frozen_suspect']:
            lines.append(f'- ⚠ 疑似冻结（close 连续 {_FROZEN_STREAK} 日同值）: '
                         f'{stats["frozen_suspect"]}')
        for w in result.get('warnings', []):
            lines.append(f'- ⚠ {w}')
        for e in result['errors']:
            lines.append(f'- 错误：{e}')
        status = '成功' if result['success'] else ('风控急停' if result['blocked'] else '失败')
        return write_review_report('外围日K同步报告', 'overseas_kline_sync_report',
                                   status, lines, result.get('duration'))
