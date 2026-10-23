"""M25 ``boll_pos`` BOLL 位置%（P0-1，方案 §3.1.3；通达信口径，indicators 库）。

取值：锚定日收盘在布林带中的位置（%B 口径）= (close-LOWER)/(UPPER-LOWER)*100
（1 位小数）。
- =50 为中轨；>50 上半区，<50 下半区；可超出 [0,100]（收盘在带外）；
- UPPER-LOWER=0（一字板/无波动）→ ``数据不足``。

口径：MID=MA(C,20)，UPPER/LOWER=MID±STD(C,20)*2（STD 为 ddof=0 总体标准差，
附录 B）。warmup 60。带宽 (UPPER-LOWER)/MID 进 detail，供「收口/张口」判读。
命中统计：连续比率，显示可算样本数（§2.5）。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..indicators import boll as tdx_boll
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class BollPos(AnalysisModule):
    """BOLL 位置%（收盘在布林带中的相对位置）。"""

    name: ClassVar[str] = 'BOLL位置%'
    name_en: ClassVar[str] = 'boll_pos'
    description: ClassVar[str] = (
        "锚定日收盘在布林带中的位置（%B 口径）= (close-LOWER)/(UPPER-LOWER)*100"
        "（1 位小数，50 为中轨，可超出 [0,100]）；MID=MA(C,20)、UPPER/LOWER=MID±"
        "STD(C,20)*2（ddof=0）。带宽进 detail；带宽为 0（一字板）返回数据不足"
    )
    required_columns: ClassVar[List[str]] = ['close']
    max_lookback: ClassVar[int] = 60
    default_params: ClassVar[Dict[str, object]] = {'n': 20, 'p': 2}
    # 命中统计口径（方案 §2.5）：连续比率，显示可算样本数
    hit_rule: ClassVar[str] = 'sample'

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
        if close.isna().any():
            return ModuleResult.insufficient('close 含空值')
        if len(close) < self.max_lookback:
            return ModuleResult.insufficient(f'warmup 不足({self.max_lookback} 日)')

        r = tdx_boll(close, int(self.param('n')), float(self.param('p')))
        c_now = float(close.iloc[-1])
        mid, upper, lower = (float(r.mid.iloc[-1]), float(r.upper.iloc[-1]),
                             float(r.lower.iloc[-1]))
        if pd.isna(mid) or pd.isna(upper) or pd.isna(lower):
            return ModuleResult.insufficient('warmup 不足(60 日)')
        width = upper - lower
        if width <= 0:
            return ModuleResult.insufficient('带宽为 0（一字板/无波动）')
        pos = (c_now - lower) / width * 100
        return ModuleResult(round(pos, 1), {
            'close': c_now, 'mid': round(mid, 4),
            'upper': round(upper, 4), 'lower': round(lower, 4),
            'bandwidth': round(width / mid * 100, 2) if mid else None,
        })
