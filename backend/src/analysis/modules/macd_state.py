"""M22 ``macd_state`` MACD 状态（P0-1，方案 §3.1.3；通达信口径，indicators 库）。

取值（锚定日，按优先级）：
- ``金叉且柱放大`` / ``金叉``：DIF 上穿 DEA（CROSS(DIF,DEA)），柱（hist）较上一日
  放大（hist[-1] > hist[-2]）时带「且柱放大」；
- ``死叉且柱缩小`` / ``死叉``：DIF 下穿 DEA，柱较上一日缩小时带「且柱缩小」；
- ``零轴上多头`` / ``零轴下空头``：无交叉时按 DIF 相对零轴位置（DIF≥0 记多头）；
- ``数据不足``：warmup 不足 / 缺列 / 含 NaN。

口径：hist=(DIF-DEA)×2（乘 2 是通达信柱口径，附录 B）；warmup 130（EMA 系，
indicators.WARMUP）。命中统计：状态串不计命中（§2.5 原则——状态非事件，分布由
报告层 indicator_panel 呈现）。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..indicators import macd as tdx_macd
from ..indicators.tdx_core import CROSS
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class MacdState(AnalysisModule):
    """MACD 状态（金叉/死叉/柱变化/零轴上下）。"""

    name: ClassVar[str] = 'MACD状态'
    name_en: ClassVar[str] = 'macd_state'
    description: ClassVar[str] = (
        "MACD 状态（通达信口径，hist=(DIF-DEA)×2）：金叉/死叉（DIF 与 DEA 交叉）"
        "及柱较上日放大/缩小；无交叉时按 DIF 相对零轴记零轴上多头/零轴下空头。"
        "warmup 130；warmup 不足或含 NaN 返回数据不足"
    )
    required_columns: ClassVar[List[str]] = ['close']
    max_lookback: ClassVar[int] = 130

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        if df is None or df.empty:
            return ModuleResult.insufficient('无K线数据')
        # 引擎加载已按 (ts_code, trade_date) 排序（load_kline ORDER BY）；
        # 单调则免排序（每模块每次调用省一次 sort，全市场 5820 只 × 7 模块）
        ordered = df if df['trade_date'].is_monotonic_increasing \
            else df.sort_values('trade_date')
        if ctx.as_of and str(ordered.iloc[-1]['trade_date']) != str(ctx.as_of):
            return ModuleResult.insufficient(
                f"锚定日{ctx.as_of}无K线(最新为{ordered.iloc[-1]['trade_date']})")
        close = pd.to_numeric(ordered['close'], errors='coerce')
        if close.isna().any() or len(close) < 2:
            return ModuleResult.insufficient('close 含空值或行数不足')
        if len(close) < self.max_lookback:
            return ModuleResult.insufficient(f'warmup 不足({self.max_lookback} 日)')

        r = tdx_macd(close)
        dif, dea, hist = r.dif, r.dea, r.hist
        if pd.isna(dif.iloc[-1]) or pd.isna(dea.iloc[-1]) or pd.isna(hist.iloc[-1]):
            return ModuleResult.insufficient('warmup 不足(130 日)')

        cross = CROSS(dif, dea)
        up = bool(cross.iloc[-1])
        down = bool(CROSS(dea, dif).iloc[-1])
        hist_now, hist_prev = float(hist.iloc[-1]), float(hist.iloc[-2])
        dif_now = float(dif.iloc[-1])

        if up:
            label = '金叉且柱放大' if hist_now > hist_prev else '金叉'
        elif down:
            label = '死叉且柱缩小' if hist_now < hist_prev else '死叉'
        elif dif_now >= 0:
            label = '零轴上多头'
        else:
            label = '零轴下空头'
        return ModuleResult(label, {
            'dif': round(dif_now, 4), 'dea': round(float(dea.iloc[-1]), 4),
            'hist': round(hist_now, 4), 'hist_prev': round(hist_prev, 4),
        })
