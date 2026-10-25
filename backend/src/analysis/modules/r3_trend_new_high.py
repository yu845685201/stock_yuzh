"""R3 ``r3_trend_new_high`` 趋势新高候选（daily_review 链 R 列；2026-10-18 自独立候选链合入）。

判定（candidate_rules.is_r3 同口径）：60 日新高 且 放量 ≥ r3_volume_min 倍
且 当日涨幅 ≥ r3_change_min% 且 非 ST。**P1-7 反高开低走（2026-10-21）**：
开盘涨幅 > r3_max_open_gain% 且 收盘 < 开盘 → 剔除（防「高开诱多长阴」，
open/preclose 比值口径不受复权基准影响）。新高/放量读前置模块
「60日新高新低」「放量倍数」，涨幅读锚定日行。R1/R2 命中股不重复入选。

依赖：链内须先配 new_high_low、volume_surge、r1_ladder_relay、r2_mainline_follow。

返回值：``新高/{涨幅}%/{放量}x`` / ``否`` / ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List, Optional

import pandas as pd

from ..candidate_rules import BUY_RULES, is_hit, is_r3
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class R3TrendNewHigh(AnalysisModule):
    """转天候选 R3：趋势新高（新高 × 放量 × 当日强势）。"""

    name: ClassVar[str] = 'R3趋势新高'
    name_en: ClassVar[str] = 'r3_trend_new_high'
    description: ClassVar[str] = (
        "转天买入候选 R3 趋势新高：60 日新高（前置模块）且 放量 ≥ r3_volume_min(2.0) 倍"
        " 且 当日涨幅 ≥ r3_change_min(3.0)% 且 非 ST；反高开低走（开盘涨幅 > "
        "r3_max_open_gain(5)% 且 收盘<开盘 → 剔除，P1-7）；R1/R2 命中股不重复入选。"
        "判定谓词与报告层第六章预案共用（candidate_rules）。"
        "返回理由串（如「新高/+7.2%/3.1x」）或 否 / 数据不足"
    )
    required_columns: ClassVar[List[str]] = ['change_rate', 'is_st', 'open',
                                             'preclose', 'close']
    default_params: ClassVar[Dict[str, object]] = {
        'r3_volume_min': BUY_RULES['r3_volume_min'],
        'r3_change_min': BUY_RULES['r3_change_min'],
        'r3_max_open_gain': BUY_RULES['r3_max_open_gain'],
    }
    # 命中统计口径（方案 §2.5）：理由串类，非「否 / 数据不足」即命中
    hit_rule: ClassVar[str] = 'reason'

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        for pre in ('R1梯队接力', 'R2主线跟随'):
            res = ctx.upstream.get(pre)
            if res is None:
                return ModuleResult.insufficient(f'缺前置模块 {pre}')
            if is_hit(res.value):
                return ModuleResult('否')

        nh = ctx.upstream.get('60日新高新低')
        vs = ctx.upstream.get('放量倍数')
        if nh is None or vs is None:
            return ModuleResult.insufficient('缺前置模块 new_high_low/volume_surge')
        if nh.value == '数据不足' or vs.value == '数据不足':
            return ModuleResult.insufficient('新高/放量数据不足')

        if df is None or df.empty:
            return ModuleResult.insufficient('无K线数据')
        anchor = df.sort_values('trade_date').iloc[-1]
        if ctx.as_of and str(anchor['trade_date']) != str(ctx.as_of):
            return ModuleResult.insufficient(f'锚定日{ctx.as_of}无K线')
        change_rate = pd.to_numeric(pd.Series([anchor.get('change_rate')]),
                                    errors='coerce').iloc[0]
        if pd.isna(change_rate):
            return ModuleResult.insufficient('锚定日缺 change_rate')
        is_st = bool(anchor.get('is_st'))

        # 反高开低走输入（P1-7）：开盘涨幅 = (open-preclose)/preclose；
        # 缺失不过滤（不硬造）
        open_gain: Optional[float] = None
        close_lt_open = False
        try:
            open_v = float(anchor.get('open'))
            preclose_v = float(anchor.get('preclose'))
            close_v = float(anchor.get('close'))
            if preclose_v > 0:
                open_gain = (open_v - preclose_v) / preclose_v * 100.0
                close_lt_open = close_v < open_v
        except (TypeError, ValueError):
            open_gain, close_lt_open = None, False

        try:
            volume_surge = float(vs.value)
        except (TypeError, ValueError):
            return ModuleResult.insufficient('放量倍数值非法')

        if not is_r3(nh.value == '新高', is_st, volume_surge,
                     float(change_rate), self.params,
                     open_gain=open_gain, close_lt_open=close_lt_open):
            return ModuleResult('否')
        # P4-1（B3）：链侧 VETO 入口——与报告层两入口同源（env 装配 cand_veto_map）
        if (ctx.upstream.get('cand_veto_map') or {}).get(ctx.stock.ts_code):
            return ModuleResult('否')
        label = f'新高/{float(change_rate):+.1f}%/{volume_surge:.1f}x'
        return ModuleResult(label, {'change_rate': float(change_rate),
                                    'volume_surge': volume_surge,
                                    'open_gain': (round(open_gain, 2)
                                                  if open_gain is not None else None)})
