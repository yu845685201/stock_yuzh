"""S1 ``ma_cross_volume`` 均线金叉放量策略（P1-7 §4.6 C，Sequoia-X 策略移植）。

判定：MA5 上穿 MA20（金叉，前一日 MA5≤MA20 且当日 MA5>MA20）且 放量倍数
≥ ``vol_min``（默认 2.0，读前置模块「放量倍数」）且 非 ST。

与 R 系列的职责分离：本模块是**独立策略截面**，不进 daily_review 链——
按需跑（``--modules`` 临时链或 ``strategy_scan`` 链）。

依赖：链内须先配 volume_surge（放量倍数前置）；MA 走 indicators 库
（通达信口径，与链指标列同一实现，原则 P6）。

返回值：``金叉/放量{x}x`` / ``否`` / ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..indicators import MA
from ..candidate_rules import BUY_RULES
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class MaCrossVolume(AnalysisModule):
    """策略截面：MA5/20 金叉 + 放量。"""

    name: ClassVar[str] = '均线金叉放量'
    name_en: ClassVar[str] = 'ma_cross_volume'
    description: ClassVar[str] = (
        "策略截面（P1-7 C，独立按需跑，不进 daily_review）：MA5 上穿 MA20 金叉"
        "（前一日 MA5≤MA20 且当日 MA5>MA20）且 放量 ≥ vol_min(2.0) 倍 且 非 ST。"
        "MA 走 indicators 库（通达信口径）；放量读前置模块。"
        "返回理由串（如「金叉/放量3.1x」）或 否 / 数据不足"
    )
    required_columns: ClassVar[List[str]] = ['close', 'is_st']
    default_params: ClassVar[Dict[str, object]] = {
        'fast': 5, 'slow': 20, 'vol_min': 2.0,
    }
    max_lookback: ClassVar[int] = 25   # slow(20) + 缓冲
    # 命中统计口径（方案 §2.5）：理由串类
    hit_rule: ClassVar[str] = 'reason'

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        vs_res = ctx.upstream.get('放量倍数')
        if vs_res is None:
            return ModuleResult.insufficient('缺前置模块 volume_surge')
        if vs_res.value == '数据不足':
            return ModuleResult.insufficient('放量数据不足')
        try:
            volume_surge = float(vs_res.value)
        except (TypeError, ValueError):
            return ModuleResult.insufficient('放量倍数值非法')

        if df is None or len(df) < self.max_lookback:
            return ModuleResult.insufficient('K线行数不足')
        ordered = df.sort_values('trade_date')
        anchor = ordered.iloc[-1]
        if ctx.as_of and str(anchor['trade_date']) != str(ctx.as_of):
            return ModuleResult.insufficient(f'锚定日{ctx.as_of}无K线')
        if bool(anchor.get('is_st')):
            return ModuleResult('否')

        close = pd.to_numeric(ordered['close'], errors='coerce')
        fast = int(self.params.get('fast') or 5)
        slow = int(self.params.get('slow') or 20)
        ma_f = MA(close, fast)
        ma_s = MA(close, slow)
        f_now, f_prev = ma_f.iloc[-1], ma_f.iloc[-2]
        s_now, s_prev = ma_s.iloc[-1], ma_s.iloc[-2]
        if pd.isna(f_now) or pd.isna(s_now) or pd.isna(f_prev) or pd.isna(s_prev):
            return ModuleResult.insufficient('MA warmup 不足')
        cross = (f_prev <= s_prev) and (f_now > s_now)
        if not cross:
            return ModuleResult('否')
        if volume_surge < float(self.params.get('vol_min') or 2.0):
            return ModuleResult('否')
        label = f'金叉/放量{volume_surge:.1f}x'
        return ModuleResult(label, {'volume_surge': volume_surge,
                                    'ma_fast': round(float(f_now), 2),
                                    'ma_slow': round(float(s_now), 2)})
