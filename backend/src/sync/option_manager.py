"""ETF 期权同步（第三阶段-2，方案 §5.1 第 5 项；上交所官方接口直连，不依赖 akshare）。

三张表（T 型报价 + 希腊字母 + IV 的口径落地）：
- ``option_daily``：逐合约风险指标（IV/Delta/Gamma/Theta/Vega/Rho），
  来源 SSE 期权风险指标（sqlId SSE_ZQPZ_YSP_GGQQZSXT_YSHQ_QQFXZB_DATE_L）；
- ``option_underlying_daily``：逐标的每日统计（合约数/成交额/认购认沽量/PC率/
  未平仓），来源 SSE 每日统计（COMMON_SSE_ZQPZ_YSP_QQ_SJTJ_MRTJ_CX）；
- ``option_contract_master``：合约主数据（标的/购沽/行权价/到期日），来源 SSE
  合约主数据（SSE_ZQPZ_YSP_GGQQZSXT_XXPL_DRHY_SEARCH_L），first_seen 保首见日、
  last_seen 滚动刷新——历史合约（已到期）靠日行情表冗余列回溯，不重复拉。

采集纪律（方案 §5.3 全项适用）：
1. **两态异常契约**：日期无数据（非交易日/未发布）记 no_data；接口异常记 error；
2. **限流**：每请求间隔 review_sync.option.interval_seconds（默认 1.0s）；
3. **幂等续跑**：option_daily/option_underlying_daily 按
   (trade_date, 合约/标的) upsert；断点查库（当日已有行数 > 0 跳过）；
4. **不进 sync-review-all**：独立命令 + 用户显式触发；
5. 行数下限校验：SSE ETF 期权全市场单日 ≈ 400~700 合约，低于 100 记 missing
   提示（数据不全仍入库，报告标注）。

口径：交易日期 = 请求的 trade_date（SSE 数据发布日期即交易日，T 日盘后发布）；
报告层消费 option_underlying_daily（PC 率/OI 作期权情绪锚，衔接 ETF 规模章）。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ..config.config_manager import ConfigManager
from ..data_sources.domain_gate import get_gate
from ..data_sources.http_collector_base import ReviewBlockedError
from ..data_sources.sse_option_source import (SSE_QUERY_URL, SseOptionSource,
                                              parse_contract_row, parse_risk_row,
                                              parse_stats_row)
from ..database.connection import DatabaseConnection
from .review_common import (domain_cooldown_early_exit, expand_trade_days,
                            latest_closed_trade_date, mark_blocked,
                            write_review_report)

logger = logging.getLogger(__name__)

#: 行数下限（SSE ETF 期权单日合约数；低于记 missing 提示）
_MIN_CONTRACTS = 100


class OptionManager:
    """ETF 期权：SSE → option_daily / option_underlying_daily / option_contract_master。"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        self.source = SseOptionSource(self.config)
        self.gate = get_gate(self.config)

    # ---------- 单日执行 ----------

    def execute(self, trade_date: Optional[str] = None, dry_run: bool = False,
                write_report: bool = True,
                sub_datasets: Optional[set] = None) -> Dict[str, Any]:
        """拉取某交易日期权风险指标 + 每日统计 + 合约主数据（§4.2 按缺失子表补齐）。

        日期语义（§2.1）：显式 trade_date 直采该业务日；缺省取最近已收盘交易日
        （T 日盘后发布——不再退回自然日-1）。sub_datasets ∈ {'risk','stats','master'}，
        None=全部；局部到数时先保存已验子表（断点续跑按子表粒度）。
        """
        started = datetime.now()
        if trade_date:
            target = str(trade_date)
        else:
            latest = latest_closed_trade_date(self.db)
            target = str(latest).replace('-', '')
        want = sub_datasets or {'risk', 'stats', 'master'}
        stats: Dict[str, Any] = {'trade_date': target, 'risk_rows': 0,
                                 'stats_rows': 0, 'master_rows': 0,
                                 'missing': []}
        result: Dict[str, Any] = {'success': False, 'stats': stats,
                                  'errors': [], 'blocked': False}
        # 域冷却继承预检（限流方案 §2）：冷却中本轮不外发，登记待补等到期领取
        if domain_cooldown_early_exit(result, self.gate, SSE_QUERY_URL, '期权'):
            result['duration'] = (datetime.now() - started).total_seconds()
            return result
        try:
            # 合约主数据先取（风险指标行需要 underlying/strike/expiry 富化——
            # SSE 风险指标接口只给行情字段，主数据走合约主数据接口；
            # master 按真实观察日缓存，first_seen/last_seen 保护历史观察边界）
            master_rows: List[Dict[str, Any]] = []
            master_by_code: Dict[str, Dict[str, Any]] = {}
            if 'master' in want or 'risk' in want:
                master_rows = [r for r in (parse_contract_row(x) for x in
                                           self.source.fetch_contract_master())
                               if r]
                stats['master_rows'] = len(master_rows)
                master_by_code = {r['contract_code']: r for r in master_rows}

            risk_rows: List[Dict[str, Any]] = []
            if 'risk' in want:
                risk_rows = [r for r in (parse_risk_row(x) for x in
                                         self.source.fetch_risk_indicators(target))
                             if r]
                # §4.2：按返回行 TRADE_DATE 校验整批（错日响应不入账）
                wrong_date = [r for r in risk_rows if r['trade_date'] != target]
                if wrong_date:
                    actual = sorted({r['trade_date'] for r in wrong_date})
                    stats['missing'].append(
                        f'风险指标含错日响应 {len(wrong_date)} 行（{actual[:2]}），'
                        f'错日行已剔除不入账')
                    risk_rows = [r for r in risk_rows if r['trade_date'] == target]
                for r in risk_rows:
                    m = master_by_code.get(r['contract_code'])
                    if m:
                        r['underlying_code'] = m.get('underlying_code')
                        r['call_put'] = m.get('call_put')
                        r['strike'] = m.get('strike')
                        r['expiry_date'] = m.get('expiry_date')
                # §4.2：旧 DAO 占位符需要的行字段一律显式提供——历史已到期合约
                # 不在当前挂牌清单（无法富化）时补 None，不让整批写库异常
                for r in risk_rows:
                    r.setdefault('strike', None)
                    r.setdefault('expiry_date', None)
                    r.setdefault('underlying_code', None)
                    r.setdefault('call_put', None)
                    r.setdefault('contract_symbol', None)
                stats['risk_rows'] = len(risk_rows)
                if 0 < len(risk_rows) < _MIN_CONTRACTS:
                    stats['missing'].append(
                        f'风险指标仅 {len(risk_rows)} 行（预期 >{_MIN_CONTRACTS}）')
                if not risk_rows:
                    result['errors'].append(
                        f'{target} 期权风险指标无数据（非交易日或未发布）')

            stat_rows: List[Dict[str, Any]] = []
            if 'stats' in want:
                stat_rows = [r for r in (parse_stats_row(x) for x in
                                         self.source.fetch_daily_stats(target))
                             if r]
                # §4.2：stats 同样按行内 TRADE_DATE 校验整批（历史请求传 tradeDate）
                wrong_date = [r for r in stat_rows if r['trade_date'] != target]
                if wrong_date:
                    actual = sorted({r['trade_date'] for r in wrong_date})
                    stats['missing'].append(
                        f'逐标的统计含错日响应 {len(wrong_date)} 行（{actual[:2]}），'
                        f'错日行已剔除不入账')
                    stat_rows = [r for r in stat_rows if r['trade_date'] == target]
                stats['stats_rows'] = len(stat_rows)
                if not stat_rows and 'risk' not in want:
                    result['errors'].append(
                        f'{target} 期权逐标的统计无数据（非交易日或未发布）')
                elif stat_rows:
                    # 交叉验证：risk 行数 = 统计表合约数量合计（§4.2 按标的核对）
                    if risk_rows:
                        declared = sum(int(r.get('contract_count') or 0)
                                       for r in stat_rows)
                        if declared and declared != len(risk_rows):
                            stats['missing'].append(
                                f'交叉验证差异：风险表 {len(risk_rows)} 行 ≠ '
                                f'统计表合约数合计 {declared}')

            if not dry_run:
                if risk_rows:
                    self.db.upsert_option_daily(risk_rows)
                if stat_rows:
                    self.db.upsert_option_underlying_daily(stat_rows)
                if master_rows and 'master' in want:
                    self._upsert_master(master_rows, target)
            # 成功语义按子表：请求的两个业务子表各自有数即成功（§2.2 按子项验收）
            wanted_biz = want & {'risk', 'stats'}
            ok_parts = []
            if 'risk' in wanted_biz:
                ok_parts.append(bool(risk_rows))
            if 'stats' in wanted_biz:
                ok_parts.append(bool(stat_rows))
            result['success'] = all(ok_parts) if ok_parts else bool(risk_rows)
        except ReviewBlockedError as e:
            # 限流方案 §1：明确被拒按风控急停记账（期权已入交易所域覆盖范围）
            mark_blocked(result, e, f'官网风控急停（本日未入库，稍后重跑补）: {e}')
            logger.warning('期权风控急停 %s: %r', target, e)
        except Exception as e:
            logger.exception('期权采集失败: %r', e)
            result['errors'].append(str(e))
        finally:
            # F2（方案 V1.0 §F2）：自报数据日（option_daily 当日 max；与 stats['trade_date']
            # 请求日不同口径——实际入库日以库内 max 为准）
            try:
                r = self.db.execute_query('SELECT MAX(trade_date) AS mx FROM option_daily')
                stats['data_date_max'] = str(r[0]['mx']) if r and r[0].get('mx') else None
            except Exception:
                stats['data_date_max'] = None
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = (self._write_report(result)
                                     if write_report else None)
        return result

    # ---------- 历史回填（§4.2：按缺失子表补齐） ----------

    def backfill_range(self, start_date: str, end_date: str,
                       dry_run: bool = False) -> Dict[str, Any]:
        """按交易日历展开逐日回填。

        断点只信查库且**按子表粒度**（§4.2：风险表已有行跳过仅指 risk 子表；
        原实现 risk 有行即整日跳过，统计表缺口永不补——11-20 场景"已有风险表时
        单补标的统计"）。当日子表行数判断：risk>0 且 stats>0 才算完整。
        """
        started = datetime.now()
        days = expand_trade_days(self.db, start_date, end_date)
        stats: Dict[str, Any] = {
            'range': f'{start_date}~{end_date}', 'days_total': len(days),
            'days_done': 0, 'days_skipped': 0, 'risk_rows': 0, 'stats_rows': 0,
            'days_failed': [], 'last_completed_date': None,
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats,
                                  'errors': [], 'backfill': True}
        for d in days:
            if not dry_run:
                want: Optional[set] = set()
                if self.db.fetch_option_row_count(d) == 0:
                    want.add('risk')
                if self.db.fetch_option_stats_row_count(d) == 0:
                    want.add('stats')
                if not want:
                    stats['days_skipped'] += 1
                    continue
                want.add('master')
            else:
                want = None
            r = self.execute(trade_date=d, dry_run=dry_run, write_report=False,
                             sub_datasets=want)
            if r['success']:
                stats['days_done'] += 1
                stats['risk_rows'] += r['stats']['risk_rows']
                stats['stats_rows'] += r['stats']['stats_rows']
                stats['last_completed_date'] = d
            else:
                stats['days_failed'].append({'date': d, 'errors': r['errors'][:2]})
        result['success'] = not stats['days_failed']
        result['duration'] = (datetime.now() - started).total_seconds()
        result['report_path'] = self._write_backfill_report(result)
        return result

    # ---------- 报告层查询 ----------

    def fetch_underlying_as_of(self, trade_date: str) -> List[Dict[str, Any]]:
        """某日逐标的期权统计（报告层期权情绪锚）"""
        return self.db.fetch_option_underlying_as_of(trade_date)

    def fetch_max_date(self) -> Optional[str]:
        rows = self.db.execute_query(
            "SELECT MAX(trade_date) AS d FROM option_daily")
        return str(rows[0]['d']) if rows and rows[0]['d'] else None

    # ---------- 内部 ----------

    def _upsert_master(self, rows: List[Dict[str, Any]], seen_date: str) -> None:
        """合约主数据 upsert（first_seen 仅插入时生效，last_seen 滚动刷新）。"""
        payload = []
        for r in rows:
            item = dict(r)
            item['first_seen'] = seen_date
            item['last_seen'] = seen_date
            payload.append(item)
        self.db.upsert_option_contracts(payload)

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 数据日期：{stats['trade_date']}（T 日盘后发布）",
            f"- 风险指标（逐合约 IV/希腊字母）：{stats['risk_rows']} 行",
            f"- 每日统计（逐标的成交/PC率/OI）：{stats['stats_rows']} 行",
            f"- 合约主数据：{stats['master_rows']} 行（last_seen 刷新）",
        ]
        if stats['missing']:
            lines.append(f"- 校验提示：{stats['missing']}")
        if stats.get('date_notes'):
            lines.append(f"- 数据日期核对（请求日≠数据日，SSE 统计接口仅返回"
                         f"最新交易日）: {stats['date_notes']}")
        if result['errors']:
            lines.append(f"- 提示：{result['errors']}")
        status = '成功' if result['success'] else '失败/无数据（待补）'
        return write_review_report('ETF 期权同步报告', 'option_daily_sync_report',
                                   status, lines, result.get('duration'))

    def _write_backfill_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 区间：{stats['range']}（交易日 {stats['days_total']}：回填 "
            f"{stats['days_done']}、跳过 {stats['days_skipped']}、失败 "
            f"{len(stats['days_failed'])}）",
            f"- 累计风险指标入库：{stats['risk_rows']} 行"
            f"（末完成日 {stats['last_completed_date']}）",
            "- 断点续跑口径：只信查库（当日 option_daily 行数 > 0 跳过）",
        ]
        if stats['days_failed']:
            lines.append(f"- 失败日期清单（可重跑续补）: "
                         f"{[f['date'] for f in stats['days_failed'][:30]]}")
        status = '成功' if result['success'] else '部分失败（可重跑续补）'
        return write_review_report('ETF 期权历史回填报告',
                                   'option_backfill_report', status, lines,
                                   result.get('duration'))
