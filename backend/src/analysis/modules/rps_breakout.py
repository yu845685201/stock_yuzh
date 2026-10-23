"""S4 ``rps_breakout`` RPS 突破策略（P1-12 §4.5 主体；P1-7 §4.6 C 延项落地）。

判定：截面相对强弱 RPS20 ≥ ``rps_min``（默认 90）且 非 ST。RPS = 各股
窗口内前复权收盘涨幅的全市场同日截面百分位（0~100，越大越强，qstock 口径），
由 env ``a_share_rps`` 一次查询装配（``{ts_code: {20: RPS, 60: RPS}}``，
窗口 [20, 60] 两档起步）——截面模块逐股调用，排名必须走 env（模块不自算
全市场排名，纯函数红线）。

与 R 系列的职责分离：独立策略截面，不进 daily_review——按需跑
（``strategy_scan`` 链或 ``--modules`` 临时链）。

依赖：链 YAML 声明 env: a_share_rps。

返回值：``RPS20=xx/RPS60=yy`` / ``否`` / ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List, Optional

import pandas as pd

from ..candidate_rules import BUY_RULES
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class RpsBreakout(AnalysisModule):
    """策略截面：RPS 突破（截面相对强弱前列）。"""

    name: ClassVar[str] = 'RPS突破'
    name_en: ClassVar[str] = 'rps_breakout'
    description: ClassVar[str] = (
        "策略截面（P1-12，独立按需跑，不进 daily_review）：截面 RPS20 ≥ rps_min(90) "
        "且 非 ST。RPS = 窗口前复权收盘涨幅的全市场同日截面百分位（env a_share_rps "
        "装配，qstock 口径，窗口 [20,60] 两档）。"
        "返回理由串（如「RPS20=95/RPS60=88」）或 否 / 数据不足"
    )
    required_columns: ClassVar[List[str]] = ['is_st']
    default_params: ClassVar[Dict[str, object]] = {'rps_min': 90.0}
    hit_rule: ClassVar[str] = 'reason'

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        rps_map = ctx.upstream.get('rps_map') or {}
        node = rps_map.get(ctx.stock.ts_code) or {}
        rps20 = node.get(20)
        rps60 = node.get(60)
        if rps20 is None:
            return ModuleResult.insufficient(
                'RPS 数据缺失（env a_share_rps 未装配或该股历史不足窗口）')

        if df is None or df.empty:
            return ModuleResult.insufficient('无K线数据')
        anchor = df.sort_values('trade_date').iloc[-1]
        if ctx.as_of and str(anchor['trade_date']) != str(ctx.as_of):
            return ModuleResult.insufficient(f'锚定日{ctx.as_of}无K线')
        if bool(anchor.get('is_st')):
            return ModuleResult('否')

        rps_min = float(self.params.get('rps_min') or 90.0)
        if float(rps20) < rps_min:
            return ModuleResult('否')
        label = f'RPS20={rps20:g}' + (f'/RPS60={rps60:g}' if rps60 is not None else '')
        return ModuleResult(label, {'rps20': rps20, 'rps60': rps60})
