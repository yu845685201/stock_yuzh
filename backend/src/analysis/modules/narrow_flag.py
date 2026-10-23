"""S3 ``narrow_flag`` 高窄旗形策略（P1-7 §4.6 C，Sequoia-X 策略移植）。

形态（旗杆 + 窄旗面，源项目口径）：
- **旗杆**：前 ``pole_days``（默认 10）日窗口（不含锚定日）存在单日涨幅
  ≥ ``pole_pct``%（默认 8%）的强势启动日（取窗口内最后一根）；
- **旗面**：启动日之后至锚定日的回调整理 ≥ ``flag_days``（默认 3）日，且
  整理期自旗杆高点回撤 ≤ ``flag_pullback``%（默认 6）——「窄」= 旗面收敛；
- 锚定日收盘**收复**旗杆高点（突破旗面）且 非 ST。

口径注记：形态判据是结构化近似（非形态识别引擎），阈值集中于 params；
旗杆高点以启动日收盘近似（close 口径，前复权下不受复权基准影响）。

返回值：``旗形/杆{涨幅}%/面{n}日`` / ``否`` / ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..candidate_rules import BUY_RULES
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class NarrowFlag(AnalysisModule):
    """策略截面：高窄旗形（旗杆 + 窄旗面 + 收复）。"""

    name: ClassVar[str] = '高窄旗形'
    name_en: ClassVar[str] = 'narrow_flag'
    description: ClassVar[str] = (
        "策略截面（P1-7 C，独立按需跑，不进 daily_review）：旗杆（窗口内单日涨幅 ≥ "
        "pole_pct(8)%）+ 窄旗面（整理 ≥ flag_days(3) 日且回撤 ≤ flag_pullback(6)%）"
        "+ 锚定日收盘收复旗杆高点；非 ST。结构化近似口径，阈值集中于 params。"
        "返回理由串（如「旗形/杆9.9%/面4日」）或 否 / 数据不足"
    )
    required_columns: ClassVar[List[str]] = ['close', 'change_rate', 'is_st']
    default_params: ClassVar[Dict[str, object]] = {
        'pole_days': 10, 'pole_pct': 8.0,
        'flag_days': 3, 'flag_pullback': 6.0,
    }
    max_lookback: ClassVar[int] = 20
    hit_rule: ClassVar[str] = 'reason'

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        if df is None or len(df) < 12:
            return ModuleResult.insufficient('K线行数不足')
        ordered = df.sort_values('trade_date')
        anchor = ordered.iloc[-1]
        if ctx.as_of and str(anchor['trade_date']) != str(ctx.as_of):
            return ModuleResult.insufficient(f'锚定日{ctx.as_of}无K线')
        if bool(anchor.get('is_st')):
            return ModuleResult('否')

        change = pd.to_numeric(ordered['change_rate'], errors='coerce')
        close = pd.to_numeric(ordered['close'], errors='coerce')
        pole_days = int(self.params.get('pole_days') or 10)
        pole_pct = float(self.params.get('pole_pct') or 8.0)
        flag_days = int(self.params.get('flag_days') or 3)
        pullback_cap = float(self.params.get('flag_pullback') or 6.0)

        # 旗杆：回看窗口内（不含锚定日）最后一根达标阳线
        window = change.iloc[-(pole_days + 1):-1]
        pole_idx = None
        for i in range(len(window) - 1, -1, -1):
            if pd.notna(window.iloc[i]) and window.iloc[i] >= pole_pct:
                pole_idx = len(ordered) - 2 - (len(window) - 1 - i)
                break
        if pole_idx is None:
            return ModuleResult('否')

        pole_gain = float(change.iloc[pole_idx])
        pole_high = float(close.iloc[pole_idx])      # 旗杆高点（收盘近似）
        flag = close.iloc[pole_idx + 1:]
        if len(flag) < flag_days:
            return ModuleResult('否')
        flag_low = float(flag.min())
        pullback = (pole_high - flag_low) / pole_high * 100.0
        if pullback > pullback_cap:
            return ModuleResult('否')                # 旗面太宽（非窄旗）
        close_now = float(close.iloc[-1])
        if close_now <= pole_high:
            return ModuleResult('否')                # 未收复旗杆高点

        label = f'旗形/杆{pole_gain:.1f}%/面{len(flag)}日'
        return ModuleResult(label, {'pole_gain': pole_gain,
                                    'flag_days': len(flag),
                                    'pullback_pct': round(pullback, 2),
                                    'pole_high': round(pole_high, 2)})
