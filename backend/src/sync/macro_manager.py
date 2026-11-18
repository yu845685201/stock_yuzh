"""宏观指标同步（第三阶段-1，方案 §5.1 第 3 项；东财 + 金十直连，不依赖 akshare）。

表：``macro_indicator_daily``（trade_date × indicator_code 一行一值）。
指标族与注册表见 ``data_sources/macro_source.py``（LPR/SHIBOR/PMI/M1/M2/社融/
CPI/PPI；中债曲线因站点反爬未接入——如实缺口，README 登记）。

采集纪律（方案 §5.3 全项适用）：
1. **两态异常契约**：指标「确实没有数据」（空 result）与「接口异常」分流——
   execute 返回 per-indicator 状态 ok/no_data/error，禁混为「无数据」静默跳过；
2. **限流**：每请求间隔 review_sync.macro.interval_seconds（默认 1.5s）串行；
3. **幂等续跑**：upsert ON CONFLICT(trade_date, indicator_code)，重复跑安全；
   断点判定查库（已有指标日期集合），不信任单次 stats；
4. **列契约集中化**：指标元数据（中文名/单位/频率）集中在
   macro_source.INDICATOR_META，报告层只读该注册表渲染；
5. **不进 sync-review-all**：宏观是低频背景锚，独立命令 + 用户显式触发
   （风控风险不拖累主链路；日/周跑一次足矣）。

口径：发布日 = 指标自身日期列（非采集日）；月频指标整段拉全量后按发布日
过滤，禁逐日轮询。
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..config.config_manager import ConfigManager
from ..data_sources.macro_source import (DEFAULT_INDICATORS, INDICATOR_META,
                                         MacroSource)
from ..database.connection import DatabaseConnection
from .review_common import write_review_report

logger = logging.getLogger(__name__)


class MacroManager:
    """宏观指标：东财/金十 → macro_indicator_daily。"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        review = self.config.get('review_sync', {}) or {}
        macro_cfg = review.get('macro') or {}
        self.indicators = [str(c) for c in
                           (macro_cfg.get('indicators') or DEFAULT_INDICATORS)]
        self.source = MacroSource(self.config)

    # ---------- 单次执行 ----------

    def execute(self, dry_run: bool = False, write_report: bool = True,
                indicators: Optional[List[str]] = None) -> Dict[str, Any]:
        """拉取全部（或指定）指标并入库。

        Returns:
            ``{success, stats: {code: {status, rows, latest_date, error?}}, ...}``
            status ∈ ok（有新行入库）/ no_data（接口正常但无数据）/ error（异常）。
        """
        started = datetime.now()
        codes = list(indicators or self.indicators)
        stats: Dict[str, Any] = {}
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': []}
        try:
            for code in codes:
                if code not in INDICATOR_META:
                    stats[code] = {'status': 'error', 'rows': 0,
                                   'error': f'未注册指标 code: {code}'}
                    continue
                rows = self.source.fetch_indicator(code)
                if not rows:
                    # 两态契约：空数据 ≠ 异常（源无数据时 result 为空）
                    stats[code] = {'status': 'no_data', 'rows': 0, 'error': None}
                    continue
                latest = max(r['trade_date'] for r in rows)
                if not dry_run:
                    self.db.upsert_macro_indicators(self._to_rows(code, rows))
                # 行数报「拉取处理行数」——execute_batch 的 rowcount 只反映末条
                # 语句（ON CONFLICT 语义），不可作入库量（以查库为准）
                stats[code] = {'status': 'ok', 'rows': len(rows),
                               'latest_date': latest, 'error': None}
            ok = [c for c in codes if stats[c]['status'] == 'ok']
            result['success'] = bool(ok)
            if not ok:
                result['errors'].append('全部指标无数据或失败（见 stats）')
        except Exception as e:
            logger.exception('宏观指标采集失败: %r', e)
            result['errors'].append(str(e))
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = (self._write_report(result)
                                     if write_report else None)
        return result

    # ---------- 历史回填 ----------

    def backfill_range(self, start_date: str, end_date: str,
                       dry_run: bool = False) -> Dict[str, Any]:
        """区间回填（低频指标整段拉全量后按区间过滤入库）。

        断点续跑只信查库：区间内已有 (trade_date, indicator_code) 的行跳过
        （幂等 upsert 本身也安全，此处跳过是为省请求）。
        """
        started = datetime.now()
        result: Dict[str, Any] = {'success': False, 'stats': {}, 'errors': []}
        try:
            have = self.db.fetch_macro_indicator_dates(start_date, end_date)
            total_rows = 0
            for code in self.indicators:
                rows = self.source.fetch_indicator(code)
                if not rows:
                    result['stats'][code] = {'status': 'no_data', 'rows': 0}
                    continue
                have_dates = set(have.get(code) or [])
                fresh = [r for r in rows
                         if start_date <= r['trade_date'] <= end_date
                         and r['trade_date'] not in have_dates]
                if not fresh:
                    result['stats'][code] = {'status': 'skipped', 'rows': 0,
                                             'reason': '区间内已有或源无该区间数据'}
                    continue
                if not dry_run:
                    self.db.upsert_macro_indicators(self._to_rows(code, fresh))
                total_rows += len(fresh)
                result['stats'][code] = {'status': 'ok', 'rows': len(fresh)}
            result['success'] = total_rows > 0
            result['stats']['_total_rows'] = total_rows
        except Exception as e:
            logger.exception('宏观回填失败: %r', e)
            result['errors'].append(str(e))
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
        return result

    # ---------- 报告层查询 ----------

    def fetch_latest_snapshot(self) -> Dict[str, Dict[str, Any]]:
        """每指标最新值快照（报告层宏观锚；缺失指标跳过不硬造）"""
        out: Dict[str, Dict[str, Any]] = {}
        for code in self.indicators:
            row = self.db.fetch_macro_latest(code)
            if row is None:
                continue
            name, unit, freq = INDICATOR_META.get(code, (code, '', ''))
            out[code] = {'name': name, 'unit': unit, 'freq': freq,
                         'trade_date': row['trade_date'], 'value': row['value']}
        return out

    # ---------- 内部 ----------

    def _to_rows(self, code: str, rows: List[Dict[str, Any]]
                 ) -> List[Dict[str, Any]]:
        """行转库行。多指标源（fetch_lpr/fetch_pmi）一次返回两个 indicator_code 的
        混合行——以行内自带 indicator_code 为准：原实现整批盖当前循环 code，会让
        LPR_1Y/LPR_5Y、PMI_MFG/PMI_NONMFG 相互串值（PK 覆盖后写错数值，风险排查
        报告 A#15 高危项）。
        """
        out: List[Dict[str, Any]] = []
        for r in rows:
            eff_code = r.get('indicator_code') or code
            name, unit, freq = INDICATOR_META.get(eff_code, (eff_code, '', ''))
            out.append({'trade_date': r['trade_date'], 'indicator_code': eff_code,
                        'value': r['value'], 'unit': unit or None,
                        'period': freq or None, 'source': 'eastmoney/jin10'})
        return out

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = ['- 指标状态（ok=有新行 / no_data=源无数据 / error=异常）:']
        for code in self.indicators:
            st = stats.get(code)
            if not st:
                continue
            line = (f"  - {code}（{INDICATOR_META.get(code, ('',))[0]}）: "
                    f"{st['status']}")
            if st.get('latest_date'):
                line += f"，最新发布日 {st['latest_date']}"
            if st.get('rows'):
                line += f"，入库 {st['rows']} 行"
            if st.get('error'):
                line += f"，{st['error']}"
            lines.append(line)
        lines.append('- 口径：发布日 = 指标自身日期列；低频指标整段拉取按发布日过滤，'
                     '禁逐日轮询')
        lines.append('- 已知缺口：中债国债收益率曲线（chinabond 反爬未接入）')
        status = '成功' if result['success'] else '失败/无数据（待查）'
        return write_review_report('宏观指标同步报告', 'macro_indicator_sync_report',
                                   status, lines, result.get('duration'))
