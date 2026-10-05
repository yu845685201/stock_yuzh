"""
北交所日K manager（P0-2 方案 B，复盘报告层与 P0 修复技术方案 §2.3/§2.5/§2.6）

职责（对给定交易日，逐日）：
1. 取通达信官网盘后包（TdxOfficialSource.fetch_daily_package，每交易日 1 请求）；
2. 过滤 market='bj' → 白名单校验（validate_bj_stock_code，P0-4）→ 组装 his_kline_day
   行 upsert（is_st 由 DAO 派生自动覆盖，P0-1 单一出口）；
3. 北交所标的清单补录 base_stock_info（来源 cod，close>0 且过白名单的条目；
   list_date=NULL——cod 无上市日，北交所股「上市首日不判定」退化为不可判，模块按
   数据不足处理）；
4. 沪深对拍：包内沪深行 vs 库内当日 raw_close（diff>0.01 元或库内缺失计入差异清单），
   报告落 doc/reports/tdx_official_audit_YYYYMMDD.md，差异率>0.5% 标异常；
5. 僵尸码审计：北交所近窗行跑 detect_zombie_bj_codes（P0-4），疑似码计入报告告警。

日K行口径（§2.5）：close = raw_close = 包内原始价（北交所暂无复权因子，因子按 1）；
preclose = 包内 prev_close；change_rate = 当日内比值（不受复权影响，天然正确）；
volume 股 / amount 元直接入库；adjust_flag = 3（不复权）；source = 'TDX_OFFICIAL'；
turnover_rate 依赖 base_fundamentals_info.float_share，缺股本留空（数据真实性原则）。

已知口径局限（数据字典同步声明）：北交所历史不复权，除权事件处序列断裂，由对拍
审计监控；preclose<=0（新股上市首日）change_rate 留空。
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..data_sources.tdx_official_source import BSE_PACKAGE_START, TdxOfficialSource
from ..database.connection import DatabaseConnection
from .review_common import (
    ReviewSyncError,
    detect_zombie_bj_codes,
    validate_bj_stock_code,
    write_review_report,
)

logger = logging.getLogger(__name__)

SOURCE_TAG = 'TDX_OFFICIAL'
AUDIT_DIFF_TOLERANCE = 0.01     # 对拍差异阈值（元）
AUDIT_DIFF_RATE_ALERT = 0.005   # 对拍差异率告警阈值（0.5%）
ZOMBIE_LOOKBACK_DAYS = 15       # 僵尸码审计回看交易日数
ZOMBIE_MIN_CONSECUTIVE = 5      # 僵尸码连续定格日数阈值（P0-4）

# bj cod 中已知非股票段（2026-09-30 实测 20260929 包 386 条：899 指数 2 条、
# 910 指数 6 条、821 可转债 24 条、810 其他 3 条）——显式跳过并计数，不静默不抛错；
# 未知段仍抛错（宁抛错不静默，未来新号段需人工确认口径）
_BJ_NON_STOCK_PREFIXES = ('810', '821', '899', '910')

# 沪深对拍只对股票段（cod 同时含指数 000x/999x、债券、基金等非股票条目，
# 逐条对拍会产生海量假"缺失"；股票段口径与 code6_to_ts_code 一致）
_AUDIT_STOCK_PREFIXES = {
    'sh': ('60', '68'),
    'sz': ('00', '30'),
}

_BSTOCK_UPsert_COLUMNS = (
    'ts_code', 'stock_code', 'stock_name', 'market_code', 'market_name',
    'exchange_code', 'list_status', 'list_date', 'type',
)


class BjKlineManager:
    """北交所日K同步 + 全市场对拍审计（每日随 sync-bj-kline / 编排器执行）"""

    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        review = self.config.get('review_sync', {}) or {}
        bj_cfg = review.get('bj_kline', {}) or {}
        self.enabled = bool(bj_cfg.get('enabled', True))
        self.source = TdxOfficialSource(self.config)
        self.db = DatabaseConnection(config_manager)

    # ------------------------------------------------------------------ 入口

    def execute(self, trade_date: Optional[str] = None, days_back: int = 0,
                dry_run: bool = False) -> Dict[str, Any]:
        """同步北交所日K（可逐日回补）+ 沪深对拍 + 僵尸码审计。

        Args:
            trade_date: 起始交易日 yyyyMMdd（缺省当日；当日包未发布则该日记"未发布"）
            days_back: 自起始日再向前回补的交易日数（回补历史用）
            dry_run: 试运行——只取包打印计划，不写库不出报告
        """
        started = datetime.now()
        result: Dict[str, Any] = {
            'success': False, 'stats': {'rows': 0, 'days': 0, 'unpublished': 0,
                                        'diff_stocks': 0, 'zombie_stocks': 0},
            'errors': [], 'blocked': False, 'report_path': None,
        }
        if not self.enabled:
            result['skipped_disabled'] = True
            result['errors'].append('配置停用（review_sync.bj_kline.enabled=false）')
            return result

        dates = self._resolve_trade_dates(trade_date, days_back)
        if not dates:
            result['errors'].append(f'未解析到交易日（trade_date={trade_date}）')
            return result
        result['dates'] = dates

        if dry_run:
            result['success'] = True
            result['stats']['days'] = len(dates)
            result['detail'] = f'dry-run：目标交易日 {dates}'
            return result

        total_rows = 0
        unpublished = 0
        audit_rows: List[Dict[str, Any]] = []   # 每日对拍差异
        for ymd in dates:
            try:
                package = self.source.fetch_daily_package(ymd)
            except ValueError as e:
                # 404（非交易日/当日未发布）：记跳过，不视为失败
                logger.info(f'{ymd} 盘后包未发布，跳过: {e}')
                unpublished += 1
                continue
            except Exception as e:  # 网络/风控/格式残缺：单日失败不阻塞其余日
                result['errors'].append(f'{ymd} 取包失败: {e}')
                continue

            rows = self._build_kline_rows(package, ymd)
            upserted = self.db.upsert_his_kline_day(rows)
            total_rows += upserted
            self._upsert_bj_stock_list(package, ymd)

            diffs = []
            try:
                diffs = self._audit_cross_check(package, ymd)
            except Exception as e:  # 对拍属辅助动作，失败不阻塞入库主流程
                logger.warning(f'{ymd} 沪深对拍失败（不阻塞）: {e}')
            result['stats']['diff_stocks'] += len(diffs)
            audit_rows.extend(diffs)

        result['stats']['rows'] = total_rows
        result['stats']['days'] = len(dates) - unpublished
        result['stats']['unpublished'] = unpublished

        zombies = self._audit_zombie_codes()
        result['stats']['zombie_stocks'] = len(zombies)

        ok_days = result['stats']['days'] > 0
        result['success'] = ok_days and not result['errors']
        result['report_path'] = self._write_audit_report(
            dates[-1], result, audit_rows, zombies,
            duration=(datetime.now() - started).total_seconds())
        return result

    # ------------------------------------------------------------------ 交易日

    def _resolve_trade_dates(self, trade_date: Optional[str],
                             days_back: int) -> List[str]:
        """起始日 + 回看 N 个交易日（base_trade_calendar 口径，升序；
        下限钳到 BSE_PACKAGE_START——此前包无北交所文件，无需请求）。"""
        base = str(trade_date).replace('-', '') if trade_date else \
            datetime.now().strftime('%Y%m%d')
        if not (len(base) == 8 and base.isdigit()):
            raise ReviewSyncError(f'非法 trade_date: {trade_date}，应为 yyyyMMdd')
        rows = self.db.fetch_all(
            """
            SELECT calendar_date FROM base_trade_calendar
            WHERE is_trading_day = 1 AND calendar_date <= %s
              AND calendar_date >= %s
            ORDER BY calendar_date DESC LIMIT %s
            """,
            (_fmt(base), _fmt(BSE_PACKAGE_START), int(days_back) + 1),
        )
        if not rows:
            return []
        return [_plain(r['calendar_date']) for r in reversed(rows)]

    # ------------------------------------------------------------------ 组装入库

    def _build_kline_rows(self, package: List[Dict[str, Any]], ymd: str) -> List[Dict[str, Any]]:
        """包行 → his_kline_day 行（仅北交所股票；is_st 由 DAO 派生，不在此设置）。"""
        float_share_map = self._load_float_share_map()
        rows: List[Dict[str, Any]] = []
        for item in package:
            if item.get('market') != 'bj':
                continue
            code = self._classify_bj_code(item['code'])
            if not code:
                continue
            preclose = float(item['prev_close'])
            close = float(item['close'])
            volume = int(item['volume'])
            rows.append({
                'ts_code': f'bj.{code}',
                'stock_code': code,
                'stock_name': item.get('name') or '',
                'trade_date': ymd,
                'open': float(item['open']),
                'high': float(item['high']),
                'low': float(item['low']),
                'close': close,
                'raw_close': close,          # 北交所不复权，close = raw_close
                'preclose': preclose,
                'change_rate': round((close - preclose) / preclose * 100, 6)
                               if preclose > 0 else None,
                'volume': volume,
                'amount': float(item['amount']),
                'adjust_flag': 3,            # 不复权
                'turnover_rate': self._calc_turnover(volume, float_share_map.get(f'bj.{code}')),
                'source': SOURCE_TAG,
            })
        if not rows:
            raise ReviewSyncError(f'{ymd} 盘后包无有效北交所行（白名单过滤后为空）')
        return rows

    @staticmethod
    def _calc_turnover(volume: int, float_share: Optional[float]) -> Optional[float]:
        """换手率（%）；缺股本留空（数据真实性原则，§2.5）"""
        if not float_share or float_share <= 0:
            return None
        return round(volume / float(float_share) * 100, 6)

    def _load_float_share_map(self) -> Dict[str, float]:
        """北交所 ts_code → 最新流通股本（base_fundamentals_info）"""
        try:
            rows = self.db.fetch_all(
                """
                SELECT DISTINCT ON (ts_code) ts_code, float_share
                FROM base_fundamentals_info
                WHERE ts_code LIKE 'bj.%' AND float_share IS NOT NULL AND float_share > 0
                ORDER BY ts_code, disclosure_date DESC NULLS LAST
                """)
            return {r['ts_code']: float(r['float_share']) for r in rows}
        except Exception as e:
            logger.warning(f'加载北交所流通股本失败（换手率将留空）: {e}')
            return {}

    def _upsert_bj_stock_list(self, package: List[Dict[str, Any]], ymd: str) -> int:
        """北交所标的清单补录 base_stock_info（cod 口径；list_date 不回填不改写）。"""
        stocks = []
        for item in package:
            if item.get('market') != 'bj':
                continue
            code = self._classify_bj_code(item['code'])
            if not code:
                continue
            stocks.append((
                f'bj.{code}', code, item.get('name') or '',
                'bj', '北京证券交易所', 'BSE', 'L', None, 1,
            ))
        if not stocks:
            return 0
        sql = f"""
            INSERT INTO base_stock_info ({', '.join(_BSTOCK_UPsert_COLUMNS)})
            VALUES %s
            ON CONFLICT (ts_code) DO UPDATE SET
                stock_name = EXCLUDED.stock_name,
                market_code = EXCLUDED.market_code,
                market_name = EXCLUDED.market_name,
                exchange_code = EXCLUDED.exchange_code,
                list_status = EXCLUDED.list_status,
                update_time = CURRENT_TIMESTAMP
        """
        # list_date 故意不在 DO UPDATE 中：cod 无上市日，不改写库内既有值
        n = self.db.execute_values(sql, stocks, page_size=1000)
        logger.info(f'{ymd} 北交所标的清单补录 {n} 只')
        return n

    # ------------------------------------------------------------------ 对拍与审计

    def _classify_bj_code(self, code: str) -> Optional[str]:
        """bj cod 条目分流：股票段过白名单（越界抛错），已知非股票段返回 None 跳过。

        - 非股票段（899/910 指数、821 可转债、810 其他）→ None（计数上报，不静默）；
        - 股票段（43x/83x/87x/920x）→ 归一化 6 位代码；
        - 未知段（如 921+）→ ValueError（P0-4 宁抛错不静默）。
        """
        code6 = str(code).strip()
        if code6[:3] in _BJ_NON_STOCK_PREFIXES:
            self.non_stock_skipped = getattr(self, 'non_stock_skipped', 0) + 1
            return None
        return validate_bj_stock_code(code6)

    def _audit_cross_check(self, package: List[Dict[str, Any]], ymd: str) -> List[Dict[str, Any]]:
        """沪深对拍：包内股票行 vs 库内当日 raw_close。

        规则（§2.6）：raw_close 差异 > 0.01 元、或包内有价而库内缺失 → 计入差异清单。
        只对股票段（sh 60/68、sz 00/30）逐条比对——cod 同时含指数/债券/基金等
        非股票条目，全量逐条对拍会产生海量假"缺失"。库内当日无任何行视为
        「日K未同步」，不逐股罗列缺失。键为 (市场, 代码)——sh 包内上证指数
        000001 与 sz 平安银行 000001 同码，裸键会撞车。
        """
        db_closes = {
            (str(r['ts_code']).split('.')[0], str(r['stock_code'])): r['raw_close']
            for r in self.db.fetch_all(
                "SELECT ts_code, stock_code, raw_close FROM his_kline_day "
                "WHERE trade_date = %s", (ymd,))
        }
        if not db_closes:
            logger.info(f'{ymd} 库内无沪深日K（未同步？），对拍仅记录"库内缺失"总数')
        diffs: List[Dict[str, Any]] = []
        for item in package:
            market = item.get('market')
            if market not in _AUDIT_STOCK_PREFIXES:
                continue
            code = item['code']
            if not code.startswith(_AUDIT_STOCK_PREFIXES[market]):
                continue  # 指数/债券/基金等非股票条目
            pkg_close = float(item['close'])
            db_close = db_closes.get((market, code))
            if db_close is None:
                if db_closes:
                    # 库内缺失：多为停牌缺行/当日未上市（批量回补时未上市股会逐日重复计入）
                    diffs.append({'date': ymd, 'kind': 'missing', 'code': code,
                                  'name': item.get('name') or '', 'market': market,
                                  'pkg_close': pkg_close, 'db_close': None, 'diff': None})
                continue
            db_close = float(db_close)
            if abs(pkg_close - db_close) > AUDIT_DIFF_TOLERANCE:
                diffs.append({'date': ymd, 'kind': 'price_diff', 'code': code,
                              'name': item.get('name') or '', 'market': market,
                              'pkg_close': pkg_close, 'db_close': db_close,
                              'diff': round(pkg_close - db_close, 4)})
        return diffs

    def _audit_zombie_codes(self) -> List[str]:
        """僵尸码审计（P0-4）：北交所近窗行跑连续定格检测"""
        try:
            rows = self.db.fetch_all(
                """
                SELECT stock_code, trade_date, volume, close, preclose
                FROM his_kline_day
                WHERE ts_code LIKE 'bj.%%'
                  AND trade_date >= (
                      SELECT MIN(d) FROM (
                          SELECT DISTINCT trade_date AS d FROM his_kline_day
                          WHERE ts_code LIKE 'bj.%%'
                          ORDER BY d DESC LIMIT %s
                      ) recent)
                """, (ZOMBIE_LOOKBACK_DAYS,))
            return detect_zombie_bj_codes(
                [{**r, 'volume': float(r['volume'] or 0)} for r in rows],
                min_consecutive_days=ZOMBIE_MIN_CONSECUTIVE)
        except Exception as e:
            logger.warning(f'僵尸码审计失败（不阻塞主流程）: {e}')
            return []

    # ------------------------------------------------------------------ 报告

    def _write_audit_report(self, last_date: str, result: Dict[str, Any],
                            diffs: List[Dict[str, Any]], zombies: List[str],
                            duration: float) -> Optional[str]:
        """对拍审计报告 → doc/reports/tdx_official_audit_YYYYMMDD.md（§2.6）

        差异率口径：**价格差异**（raw_close 差 >0.01 元）按天计差异率并以此判异常；
        库内缺失（停牌缺行/当日未上市）单列——批量回补时未上市股会逐日重复计入，
        混入总差异率会结构性虚高（2026-09-30 尾段回补实测 203 条差异全为缺失形态）。
        """
        stats = result['stats']
        price_diffs = [d for d in diffs if d.get('kind') == 'price_diff']
        missing = [d for d in diffs if d.get('kind') != 'price_diff']
        lines = [
            f'- 数据日期：{last_date}；同步交易日 {stats["days"]} 天，'
            f'北交所入库 {stats["rows"]} 行，包未发布跳过 {stats["unpublished"]} 天',
            f'- 对拍差异：价格差异 {len(price_diffs)} 条 / 库内缺失 {len(missing)} 条'
            f'（停牌缺行、未上市新股属已知口径）；僵尸码疑似：{len(zombies)}',
            '',
        ]

        # 逐日价格差异率（§2.6 阈值按"当日"口径）
        per_day: Dict[str, List[Dict[str, Any]]] = {}
        for d in price_diffs:
            per_day.setdefault(d.get('date') or last_date, []).append(d)
        alert_days: List[str] = []
        if per_day:
            lines += ['## 逐日价格差异率', '',
                      '| 交易日 | 价格差异数 | 库内当日行数 | 差异率 |', '|---|---|---|---|']
            for ymd in sorted(per_day):
                try:
                    n = int(self.db.fetch_one(
                        "SELECT COUNT(*) n FROM his_kline_day WHERE trade_date = %s",
                        (ymd,))['n'])
                except Exception:
                    n = 0
                rate = len(per_day[ymd]) / n if n else 0.0
                if rate > AUDIT_DIFF_RATE_ALERT:
                    alert_days.append(ymd)
                lines.append(f'| {ymd} | {len(per_day[ymd])} | {n} | {rate:.4%} |')
            lines.append('')

        if price_diffs:
            lines += ['## 价格差异明细（前 50 条）', '',
                      '| 交易日 | 市场 | 代码 | 名称 | 包 close | 库 raw_close | 差异 |',
                      '|---|---|---|---|---|---|---|']
            for d in price_diffs[:50]:
                lines.append(f"| {d.get('date', '-')} | {d['market']} | {d['code']} | {d['name']} "
                             f"| {d['pkg_close']} | {d['db_close']} | {d['diff']} |")
            lines.append('')
        if missing:
            lines += ['## 库内缺失（已知口径，不计差异率）', '',
                      '| 交易日 | 市场 | 代码 | 名称 | 包 close |', '|---|---|---|---|---|']
            for d in missing[:30]:
                lines.append(f"| {d.get('date', '-')} | {d['market']} | {d['code']} "
                             f"| {d['name']} | {d['pkg_close']} |")
            if len(missing) > 30:
                lines.append(f'…（其余 {len(missing) - 30} 条略）')
            lines.append('')
        if zombies:
            lines.append('## 疑似僵尸码（P0-4，连续 ≥5 日无成交且价格定格）')
            lines.append('')
            lines.append('、'.join(zombies))
            lines.append('')
        for err in result['errors'][:10]:
            lines.append(f'- 错误：{err}')

        if alert_days:
            status = (f'异常（{",".join(alert_days)} 价格差异率 > {AUDIT_DIFF_RATE_ALERT:.1%}，'
                      '人工介入）')
        else:
            status = '完成'
        lines.append(f'- 异常判定：{status}（阈值 {AUDIT_DIFF_RATE_ALERT:.1%}，'
                     '按价格差异/库内当日行数逐日计算）')

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_lines = [f'# 通达信盘后包对拍审计 {ts}', '', f'- 状态：{status}']
        report_lines.extend(lines)
        report_lines.append(f'- 耗时：{duration / 60:.1f} 分钟' if duration >= 60
                            else f'- 耗时：{duration:.0f} 秒')
        report_lines.append('')
        return _write_audit_markdown(report_lines, last_date)


def _write_audit_markdown(lines: List[str], last_date: str) -> str:
    """审计报告固定路径 doc/reports/tdx_official_audit_YYYYMMDD.md（§2.6）"""
    from .report_writer import resolve_repo_root
    path = resolve_repo_root() / 'doc' / 'reports' / f'tdx_official_audit_{last_date}.md'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n'.join(lines), encoding='utf-8')
    return str(path)


def _fmt(ymd: str) -> str:
    return f'{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}'


def _plain(v: Any) -> str:
    return str(v).replace('-', '')
