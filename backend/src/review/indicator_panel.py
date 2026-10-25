"""报告层技术指标面板（P0-1 §3.1.7，2026-10-18 增量 C2）。

设计红线——**与 daily_review 链指标列同一实现两入口**（原则 P6）：本面板不重造
判定逻辑，直接按 ts_code 分组调用 `analysis.modules.ind_*.py` 七模块（+ volume_surge /
new_high_low 两个交叉榜用模块），与链逐股调用同一代码路径；对拍见
`test/analysis/test_review_indicator_parity.py`（链 CSV vs 本面板，逐值一致）。

窗口：与链扩展后窗口一致——`INDICATOR_WINDOW_DAYS=522`（链 max 模块回看
= 缠论 520 + 引擎 buffer 2）。窗口不一致则 MACD/RSI/OBV 最新值与链不可比
（ewm 播种差异——2026-10-25 前曾停在对齐失败的 133d，对拍 731 处失配即此
根因，见技术方案附录 C.6）；有 test_review_indicator_parity.py 常量闸
防再次漂移。

成本：逐股调用与链同量级（520d 窗口约 +60~90s，2026-10-25 实测口径），
`review generate` ~2 分钟——独立性红线（不依赖链运行）+ 逐值对拍可用的
代价，agg_cache（P3-2）下同日复用。
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from ..analysis.data_reader import DataReader
from ..analysis.module_base import ChainContext, StockInfo
from ..analysis.modules.atr_pct import AtrPct
from ..analysis.modules.boll_pos import BollPos
from ..analysis.modules.kdj_state import KdjState
from ..analysis.modules.ma_arrangement import MaArrangement
from ..analysis.modules.macd_state import MacdState
from ..analysis.modules.new_high_low import NewHighLow
from ..analysis.modules.obv_trend import ObvTrend
from ..analysis.modules.rsi_state import RsiState
from ..analysis.modules.volume_surge import VolumeSurge

#: 指标面板窗口（交易日）：与链 daily_review 窗口公式逐字节对齐——
#: 链 module_days = max(effective_lookback) = 缠论 520，引擎取 nth_back(module_days + 2)
INDICATOR_WINDOW_DAYS = 522

#: 面板复算的模块（顺序即输出列序；后两个供交叉榜使用）
_PANEL_MODULES = [
    MacdState(), KdjState(), RsiState(), BollPos(), AtrPct(), ObvTrend(),
    MaArrangement(), VolumeSurge(), NewHighLow(),
]

#: 面板加载列（七指标 + 交叉榜 + 展示所需）
PANEL_COLUMNS = ['stock_name', 'open', 'high', 'low', 'close', 'volume', 'amount']


def load_indicator_panel(reader: DataReader, as_of: str,
                         start: str) -> pd.DataFrame:
    """加载指标面板（133 交易日窗口，与链扩展后窗口一致）。"""
    return reader.load_kline(PANEL_COLUMNS, as_of, start_date=start)


def compute_indicator_states(panel: pd.DataFrame, as_of: str) -> pd.DataFrame:
    """按 ts_code 分组调用模块（与链同一代码路径），输出锚定日状态宽表。

    Returns:
        DataFrame，列：ts_code + 各模块中文名（MACD状态/KDJ状态/RSI状态/
        BOLL位置%/ATR波动%/OBV趋势/均线排列/放量倍数/60日新高新低）+ 展示字段
        （股票名称/成交额）。
    """
    if panel is None or panel.empty:
        return pd.DataFrame()
    rows: List[Dict[str, Any]] = []
    for ts_code, g in panel.groupby('ts_code', sort=False):
        g = g.sort_values('trade_date')
        ctx = ChainContext(as_of=as_of,
                           stock=StockInfo(ts_code=str(ts_code), stock_code='',
                                           stock_name=''))
        row: Dict[str, Any] = {'ts_code': str(ts_code)}
        for mod in _PANEL_MODULES:
            row[mod.name] = mod.analyze(g, ctx).value
        # 展示用字段（成交额/名称取锚定日行）
        anchor = g.iloc[-1]
        row['股票名称'] = anchor.get('stock_name', '')
        row['成交额'] = anchor.get('amount')
        rows.append(row)
    return pd.DataFrame(rows)


def _count(col: pd.Series, pred) -> int:
    return int(col.astype(str).map(pred).sum())


def _quantiles(col: pd.Series) -> Dict[str, Any]:
    v = pd.to_numeric(col, errors='coerce').dropna()
    if v.empty:
        return {'sample': 0}
    q = v.quantile([0.25, 0.5, 0.75])
    return {'sample': int(len(v)),
            'p25': round(float(q.loc[0.25]), 2),
            'p50': round(float(q.loc[0.5]), 2),
            'p75': round(float(q.loc[0.75]), 2)}


def _top_rows(df: pd.DataFrame, top_n: int) -> List[Dict[str, Any]]:
    """按成交额降序取 top_n（成交额不可算的排后）。"""
    if df.empty:
        return []
    d = df.copy()
    d['_amt'] = pd.to_numeric(d['成交额'], errors='coerce').fillna(-1)
    d = d.sort_values('_amt', ascending=False).head(top_n)
    out = []
    for _, r in d.iterrows():
        out.append({
            'code': str(r['ts_code']).split('.')[-1],
            'name': str(r.get('股票名称') or ''),
            'reason': str(r.get('_reason') or ''),
            'amount': (None if pd.isna(r['_amt']) or r['_amt'] < 0
                       else float(r['_amt'])),
        })
    return out


def summarize_indicator_states(states: pd.DataFrame, volume_surge_min: float = 2.0,
                               top_n: int = 10) -> Dict[str, Any]:
    """状态宽表 → 报告层聚合（分布 + 分位 + 交叉榜）。纯函数，可单测。

    口径：分布按模块中文列精确匹配（档位）或子串匹配（KDJ/RSI 的组合标记）；
    BOLL/ATR 为连续比率给分位与样本数；交叉榜 = MACD 金叉 × 放量≥阈值、
    RSI 超卖 × 60日新低（均按成交额降序 top_n）。
    """
    if states is None or states.empty:
        return {}
    macd = states['MACD状态']
    kdj = states['KDJ状态']
    rsi = states['RSI状态']
    obv = states['OBV趋势']
    ma = states['均线排列']

    # 交叉榜：MACD 金叉 × 放量达标（阈值与 config review.volume_surge_min 同源）
    surge = pd.to_numeric(states['放量倍数'], errors='coerce')
    m1 = macd.astype(str).str.startswith('金叉') & (surge >= volume_surge_min)
    t1 = states[m1].copy()
    t1['_reason'] = t1['MACD状态'].astype(str) + ' / 放量' + \
        surge[m1].map(lambda v: f'{float(v):.1f}x' if pd.notna(v) else '—')
    # 交叉榜：RSI 超卖 × 60日新低
    m2 = rsi.astype(str).str.contains('超卖', regex=False) & \
        (states['60日新高新低'].astype(str) == '新低')
    t2 = states[m2].copy()
    t2['_reason'] = 'RSI超卖 / 60日新低'

    return {
        'macd_dist': {
            '金叉': _count(macd, lambda v: v.startswith('金叉')),
            '死叉': _count(macd, lambda v: v.startswith('死叉')),
            '零轴上多头': _count(macd, lambda v: v == '零轴上多头'),
            '零轴下空头': _count(macd, lambda v: v == '零轴下空头'),
        },
        'kdj_dist': {
            '超买': _count(kdj, lambda v: '超买' in v),
            '超卖': _count(kdj, lambda v: '超卖' in v),
            '金叉': _count(kdj, lambda v: '金叉' in v),
            '死叉': _count(kdj, lambda v: '死叉' in v),
        },
        'rsi_dist': {
            '超买': _count(rsi, lambda v: '超买' in v),
            '超卖': _count(rsi, lambda v: '超卖' in v),
        },
        'obv_dist': {k: _count(obv, lambda v, k=k: v == k)
                     for k in ('价升量增', '价升量缩', '价跌量减', '价跌量增', '背离待察')},
        'ma_dist': {k: _count(ma, lambda v, k=k: v == k)
                    for k in ('多头排列(MA5>10>20>60)', '空头排列(MA5<10<20<60)',
                              '纠缠', '收盘上穿MA60')},
        'boll_pos': _quantiles(states['BOLL位置%']),
        'atr_pct': _quantiles(states['ATR波动%']),
        'top_macd_surge': _top_rows(t1, top_n),
        'top_rsi_new_low': _top_rows(t2, top_n),
        'sample': int(len(states)),
    }
