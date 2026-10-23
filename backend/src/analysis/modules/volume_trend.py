"""``volume_trend`` 量能趋势档位（量价资金方案 V1.1 §3.1①）。

纯 K 线计算：今日量 vs 前 5 日均量（r5）与 vs 前 20 日均量（r20）双档：

- ``连续放量``：r5 ≥ surge_ratio 且 最近 3 个交易日中 ≥2 日 r5 ≥ 1.2（趋势确认）；
- ``放量``：r5 ≥ surge_ratio；
- ``温和``：shrink_ratio ≤ r5 < surge_ratio；
- ``缩量``：r5 < shrink_ratio。

与 volume_surge（单点倍数）口径互补不替代。基数剔除停牌无量行（volume=0），
与 volume_surge 同口径。锚定日无成交、有效基准不足 window 天返回 ``数据不足``。

返回值：``连续放量`` / ``放量`` / ``温和`` / ``缩量`` / ``数据不足``。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..module_base import AnalysisModule, ChainContext, ModuleResult

#: 趋势确认阈值（近 3 日中 ≥2 日 r5 达到该值 → 连续放量）
_TREND_RATIO = 1.2


class VolumeTrend(AnalysisModule):
    """量能趋势档位（连续放量/放量/温和/缩量）。"""

    name: ClassVar[str] = '量能趋势'
    name_en: ClassVar[str] = 'volume_trend'
    description: ClassVar[str] = (
        "量能趋势档位：今日量 vs 前 5 日均量比值 r5 分档——r5≥surge_ratio(1.5) 且近 3 日中 "
        "≥2 日 r5≥1.2 记「连续放量」；r5≥1.5 记「放量」；0.7≤r5<1.5 记「温和」；r5<0.7 记"
        "「缩量」。纯 K 线计算，与 volume_surge 单点倍数互补；基数剔除停牌无量行，锚定日"
        "无成交或基准不足 window(20) 天返回「数据不足」，不降窗口"
    )
    required_columns: ClassVar[List[str]] = ['volume']
    max_lookback: ClassVar[int] = 21
    default_params: ClassVar[Dict[str, object]] = {
        'surge_ratio': 1.5, 'shrink_ratio': 0.7, 'window': 20,
    }
    # 命中统计分桶（方案 §2.5）：放量态两档分别计数；温和/缩量为非异动态不计
    hit_buckets: ClassVar[List[str]] = ['连续放量', '放量']

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        if df is None or df.empty:
            return ModuleResult.insufficient('无K线数据')
        ordered = df.sort_values('trade_date')
        anchor = ordered.iloc[-1]
        if ctx.as_of and str(anchor['trade_date']) != str(ctx.as_of):
            return ModuleResult.insufficient(f'锚定日{ctx.as_of}无K线')
        try:
            surge = float(self.param('surge_ratio'))
            shrink = float(self.param('shrink_ratio'))
            window = int(self.param('window'))
        except (TypeError, ValueError):
            return ModuleResult.insufficient('参数非法')
        if shrink >= surge or window < 5:
            return ModuleResult.insufficient('参数非法(surge>shrink, window≥5)')

        vol = pd.to_numeric(ordered['volume'], errors='coerce')
        anchor_vol = vol.iloc[-1]
        if pd.isna(anchor_vol) or float(anchor_vol) <= 0:
            return ModuleResult.insufficient('锚定日无成交(停牌标记行)')

        # 基数剔除无量行（停牌标记 volume=0），与 volume_surge 同口径
        hist = vol.iloc[:-1]
        hist = hist[hist > 0]
        base5 = hist.tail(5)
        base20 = hist.tail(window)
        if len(base5) < 5 or len(base20) < window:
            return ModuleResult.insufficient(
                f'有效基准不足(5日{len(base5)}/20日{len(base20)})')
        r5 = float(anchor_vol) / float(base5.mean())
        r20 = float(anchor_vol) / float(base20.mean())

        # 近 3 日趋势：倒数第 1~3 个交易日各自的 r5（当日量/其前 5 日均量）
        recent_days = 0
        all_vol = vol[vol > 0].reset_index(drop=True)
        for k in range(1, 3 + 1):
            idx = len(all_vol) - k
            if idx < 5:
                break
            r_k = float(all_vol.iloc[idx]) / float(all_vol.iloc[idx - 5:idx].mean())
            if r_k >= _TREND_RATIO:
                recent_days += 1

        if r5 >= surge and recent_days >= 2:
            label = '连续放量'
        elif r5 >= surge:
            label = '放量'
        elif r5 >= shrink:
            label = '温和'
        else:
            label = '缩量'
        detail: Dict[str, object] = {'r5': round(r5, 4), 'r20': round(r20, 4),
                                     'recent_surge_days': recent_days}
        return ModuleResult(label, detail)
