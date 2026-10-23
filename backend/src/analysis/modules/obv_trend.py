"""M27 ``obv_trend`` OBV 趋势（P0-1，方案 §3.1.3；通达信口径，indicators 库）。

取值（价量 5 日方向组合，OBV 取 5 日变化方向）：
- ``价升量增`` / ``价升量缩``：close 5 日上涨 × OBV 5 日升/降；
- ``价跌量减`` / ``价跌量增``：close 5 日下跌 × OBV 5 日降/升；
- ``背离待察``：价升量缩**且**收盘创 20 日新高（价创新高但能量潮未跟进，顶背离信号）；
- 两向皆平 → ``否``；warmup 不足或含 NaN → ``数据不足``。

口径：OBV=SUM(IF(C>REF(C,1),VOL,IF(C<REF(C,1),-VOL,0)),0)/10000（万手单位、
平盘计 0，附录 B）。warmup 130。命中统计：状态串不计命中（§2.5 原则）。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..indicators import obv as tdx_obv
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class ObvTrend(AnalysisModule):
    """OBV 趋势（价量方向组合 + 顶背离待察）。"""

    name: ClassVar[str] = 'OBV趋势'
    name_en: ClassVar[str] = 'obv_trend'
    description: ClassVar[str] = (
        "价量 5 日方向组合：close 5 日涨/跌 × OBV 5 日升/降，记价升量增/价升量缩/"
        "价跌量减/价跌量增；价升量缩且收盘创 20 日新高记背离待察（顶背离信号）。"
        "OBV 为万手单位、平盘计 0（通达信口径）。warmup 130"
    )
    required_columns: ClassVar[List[str]] = ['close', 'volume']
    max_lookback: ClassVar[int] = 130
    default_params: ClassVar[Dict[str, object]] = {'window': 5, 'high_window': 20}

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
        vol = pd.to_numeric(ordered['volume'], errors='coerce')
        if close.isna().any() or vol.isna().any():
            return ModuleResult.insufficient('close/volume 含空值')
        if len(close) < self.max_lookback:
            return ModuleResult.insufficient(f'warmup 不足({self.max_lookback} 日)')

        n = int(self.param('window'))
        hw = int(self.param('high_window'))
        if len(close) <= n:
            return ModuleResult.insufficient(f'行数不足({n}+1)')
        obv = tdx_obv(close, vol)
        if pd.isna(obv.iloc[-1]) or pd.isna(obv.iloc[-1 - n]):
            return ModuleResult.insufficient('warmup 不足(130 日)')

        d_price = float(close.iloc[-1]) - float(close.iloc[-1 - n])
        d_obv = float(obv.iloc[-1]) - float(obv.iloc[-1 - n])
        if d_price == 0 and d_obv == 0:
            return ModuleResult('否', {'d_price': 0.0, 'd_obv': 0.0})

        price_up = d_price > 0
        obv_up = d_obv > 0
        if price_up and obv_up:
            label = '价升量增'
        elif price_up and not obv_up:
            # 顶背离待察：价升量缩 + 收盘创 high_window 日新高
            if len(close) > hw and float(close.iloc[-1]) >= float(
                    close.iloc[-hw:].max()):
                label = '背离待察'
            else:
                label = '价升量缩'
        elif not price_up and not obv_up:
            label = '价跌量减'
        else:
            label = '价跌量增'
        return ModuleResult(label, {
            'd_price': round(d_price, 4), 'd_obv': round(d_obv, 4),
            'price_dir': 'up' if price_up else 'down',
            'obv_dir': 'up' if obv_up else 'down',
        })
