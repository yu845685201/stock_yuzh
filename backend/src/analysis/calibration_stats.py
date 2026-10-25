"""校准统计核心（V4.0 A3/A4）：纯函数层，DataFrame 进、stats dict 出。

零 IO 零写库——读数与报告落盘在 scripts/run_breadth_calibration.py /
run_a50_trigger_calibration.py（瘦 CLI），产物 data/calibration/（不入库，
同 chan_validation 先例）。

- A3/X4：涨跌家数比（breadth = up_count/down_count）阈值校准——分布、
  分桶次日结果、条件增量（core 内/外）、阈值选项表。
- A4/X3：A50 触发准入回测——as-of 对齐（≤D−1，防把 D 自身 bar 的盘后信息
  当盘前可得）、同向率、触发可用性、A5 准入门三门读数。

口径纪律：数据缺失不硬造（缺日剔除并计数）；日K 回测为「全天收」代理，
同向率是乐观界（D−1 bar 含 D−1 日盘后/夜盘信息）。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

#: A5 准入门阈值（V4.0 §2.X3 写死；gate3 为前向人工登记不在此计算）
GATE1_SAME_DIRECTION_PCT = 60.0
GATE2_CROSS_FREQ_PCT = 10.0
A50_MIN_BARS = 250

#: breadth 分桶（含 <1 与极端档；inf 档单列）
_BREADTH_BUCKETS = ((0.0, 1.0), (1.0, 1.5), (1.5, 2.0), (2.0, 2.5),
                    (2.5, 4.0), (4.0, float('inf')))


def _bucket_label(v: float) -> str:
    if v == float('inf'):
        return 'inf档'
    for lo, hi in _BREADTH_BUCKETS:
        if lo <= v < hi:
            return f'[{lo:g},{hi:g})' if hi != float('inf') else '[4,+∞)'
    return '其它'


def _fnum(v: Any) -> Optional[float]:
    try:
        f = float(v)
        return f if np.isfinite(f) else None
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------- A3/X4 breadth

def breadth_series(m: pd.DataFrame) -> pd.Series:
    """涨跌家数比序列（index=trade_date 升序）。

    down==0 → inf（普涨无下跌的极端档）；up/down 缺失或 down<0 → NaN（剔除）。
    """
    mm = m.sort_values('trade_date') if 'trade_date' in m.columns else m
    up = pd.to_numeric(mm['up_count'], errors='coerce')
    down = pd.to_numeric(mm['down_count'], errors='coerce')
    keys = (mm['trade_date'] if 'trade_date' in mm.columns
            else pd.Series(mm.index, index=mm.index))
    out: Dict[Any, float] = {}
    for key, u, d in zip(keys, up, down):
        if pd.isna(u) or pd.isna(d) or d < 0:
            out[key] = float('nan')
        elif d == 0:
            out[key] = float('inf')
        else:
            out[key] = float(u) / float(d)
    return pd.Series(out, index=pd.Index(list(out.keys()), name='trade_date'))


def breadth_distribution(br: pd.Series) -> Dict[str, Any]:
    """分位表（有限值）+ 阈值占比（inf 计入 ≥ 档——极端宽度本身就是信号）。"""
    v = br.dropna()
    finite = v[v != float('inf')]
    return {
        'sample_days': int(len(v)),
        'inf_days': int((v == float('inf')).sum()),
        'quantiles': {f'P{p}': round(float(finite.quantile(p / 100)), 3)
                      for p in (50, 75, 90, 95, 99)},
        'above': {f'>={t}': {'days': int((v >= t).sum()),
                             'pct': round(float((v >= t).mean()) * 100, 1)}
                  for t in (1.5, 2.0, 2.5)},
    }


def _next_day_frame(m: pd.DataFrame, sh: pd.DataFrame) -> Dict[str, Any]:
    """逐日拼次日结果帧（A3 各函数共用）：breadth + 次日溢价/涨停家数变化 +
    次日上证开盘 gap 与收盘涨幅（按 trade_date merge，缺日剔除并计数）。"""
    mm = m.sort_values('trade_date').reset_index(drop=True)
    br = breadth_series(mm).reset_index(drop=True)
    sh_idx = (sh.sort_values('trade_date').set_index('trade_date')
              if 'trade_date' in sh.columns else sh)
    rows: List[Dict[str, Any]] = []
    dropped_no_next = 0
    dropped_no_sh = 0
    for i in range(len(mm)):
        b = br.iloc[i]
        if pd.isna(b):
            continue
        if i + 1 >= len(mm):
            dropped_no_next += 1
            continue
        next_date = str(mm['trade_date'].iloc[i + 1])
        if next_date not in sh_idx.index:
            dropped_no_sh += 1
            continue
        s = sh_idx.loc[next_date]
        try:
            gap = (float(s['open']) / float(s['preclose']) - 1) * 100
            cchg = (float(s['close']) / float(s['preclose']) - 1) * 100
        except (TypeError, ValueError, ZeroDivisionError):
            dropped_no_sh += 1
            continue
        rows.append({
            'trade_date': str(mm['trade_date'].iloc[i]),
            'breadth': float(b), 'bucket': _bucket_label(b),
            'max_streak': _fnum(mm['max_streak'].iloc[i])
            if 'max_streak' in mm.columns else None,
            'premium_mean': _fnum(mm['premium_mean'].iloc[i])
            if 'premium_mean' in mm.columns else None,
            'next_premium': _fnum(mm['premium_mean'].iloc[i + 1])
            if 'premium_mean' in mm.columns else None,
            'next_lu_chg': ((_fnum(mm['limit_up_count'].iloc[i + 1]) or 0)
                            - (_fnum(mm['limit_up_count'].iloc[i]) or 0))
            if 'limit_up_count' in mm.columns else None,
            'sh_open_gap': round(gap, 3), 'sh_close_chg': round(cchg, 3),
        })
    return {'rows': pd.DataFrame(rows), 'dropped_no_next': dropped_no_next,
            'dropped_no_sh': dropped_no_sh}


def _summarize(d: pd.DataFrame) -> Dict[str, Any]:
    if d.empty:
        return {'n': 0, 'next_premium_mean': None, 'sh_open_gap_median': None,
                'sh_close_chg_median': None}
    return {'n': int(len(d)),
            'next_premium_mean': (round(float(d['next_premium'].mean()), 2)
                                  if d['next_premium'].notna().any() else None),
            'sh_open_gap_median': round(float(d['sh_open_gap'].median()), 2),
            'sh_close_chg_median': round(float(d['sh_close_chg'].median()), 2)}


def conditional_next_day(m: pd.DataFrame, sh: pd.DataFrame) -> Dict[str, Any]:
    """分桶 × 次日结果：各桶样本数/次日溢价均值/涨停家数变化中位/上证 gap 中位。"""
    nd = _next_day_frame(m, sh)
    df = nd['rows']
    buckets = []
    if not df.empty:
        for label, g in df.groupby('bucket', sort=False):
            s = _summarize(g)
            s['bucket'] = label
            s['next_lu_chg_median'] = (round(float(g['next_lu_chg'].median()), 1)
                                       if g['next_lu_chg'].notna().any() else None)
            buckets.append(s)
    return {'rows_count': int(len(df)), 'dropped_no_next': nd['dropped_no_next'],
            'dropped_no_sh': nd['dropped_no_sh'], 'buckets': buckets}


def incremental_power(m: pd.DataFrame, sh: pd.DataFrame,
                      core_streak: float = 5.0,
                      core_premium: float = 4.0) -> Dict[str, Any]:
    """条件增量（第二证据的存在意义）：

    (a) core（高度≥5 且 溢价≥4，即高潮主判据已命中）日内 breadth≥2 vs <2 的
        次日差异——宽度是否在主判据之上仍有增量；
    (b) core 未命中日中 breadth 越各阈值档的次日表现 vs 未命中总体基线——
        极端宽度能否「单独」预告次日兑现/退潮。
    """
    nd = _next_day_frame(m, sh)
    df = nd['rows']
    if df.empty or 'max_streak' not in df.columns:
        return {'core_days': 0, 'in_core': {}, 'out_core': {}}
    core = (df['max_streak'] >= core_streak) & (df['premium_mean'] >= core_premium)
    in_core, out_core = df[core], df[~core]
    wide = in_core[in_core['breadth'] >= 2.0] if not in_core.empty else in_core
    narrow = in_core[in_core['breadth'] < 2.0] if not in_core.empty else in_core
    return {
        'core_days': int(core.sum()),
        'in_core': {'ge2': _summarize(wide), 'lt2': _summarize(narrow)},
        'out_core': {**{f'ge{t:g}': _summarize(out_core[out_core['breadth'] >= t])
                        for t in (1.5, 2.0, 2.5)},
                     'baseline': _summarize(out_core)},
    }


def threshold_options(m: pd.DataFrame, sh: pd.DataFrame,
                      thresholds=(1.5, 2.0, 2.5, 'P90'),
                      core_streak: float = 5.0,
                      core_premium: float = 4.0) -> List[Dict[str, Any]]:
    """阈值选项表：命中天数/占比、命中日次日表现、与高潮主判据（core）交叠度。"""
    nd = _next_day_frame(m, sh)
    df = nd['rows']
    if df.empty:
        return []
    finite = df.loc[df['breadth'] != float('inf'), 'breadth']
    p90 = float(finite.quantile(0.9)) if len(finite) else None
    core = ((df['max_streak'] >= core_streak)
            & (df['premium_mean'] >= core_premium))
    out = []
    for t in thresholds:
        thr = p90 if t == 'P90' else float(t)
        hit = df[df['breadth'] >= thr]
        s = _summarize(hit)
        out.append({'threshold': t, 'thr_value': round(thr, 3),
                    'days': s['n'],
                    'pct': round(s['n'] / len(df) * 100, 1),
                    'next_sh_close_median': s['sh_close_chg_median'],
                    'next_premium_mean': s['next_premium_mean'],
                    'overlap_core_pct': (round(float(core[hit.index].mean()) * 100, 1)
                                         if s['n'] else None)})
    return out


# ---------------------------------------------------------------- A4/X3 A50

def align_overseas(sh: pd.DataFrame, ov: pd.DataFrame) -> pd.DataFrame:
    """逐 A 股交易日 D 对齐海外锚的「盘前可得」变动。

    c(D) = ov 最后一根 trade_date ≤ D−1（日历）的收盘——D 自身 bar 于 D+1 凌晨
    （A50/纳指）或 D 日盘后（恒指）才收，对 D 的开盘不可用；前向填充覆盖 ov
    休市（如周一用周五 bar，周末缺口因此进入周一读数）。
    move = c(D)/c(D 上一个 A 股交易日) − 1（%）；首个可比日无前值 → 剔除。
    """
    sh = sh.sort_values('trade_date').reset_index(drop=True)
    ov = ov.sort_values('trade_date').reset_index(drop=True)
    ov_dates = ov['trade_date'].astype(str).tolist()
    ov_close = pd.to_numeric(ov['close'], errors='coerce').tolist()

    def close_asof(day: str) -> Optional[float]:
        key = (pd.Timestamp(day) - pd.Timedelta(days=1)).strftime('%Y%m%d')
        c = None
        for dt, cl in zip(ov_dates, ov_close):
            if dt <= key and cl is not None and not pd.isna(cl) and cl > 0:
                c = float(cl)
            elif dt > key:
                break
        return c

    rows: List[Dict[str, Any]] = []
    prev: Optional[float] = None
    for _, r in sh.iterrows():
        d = str(r['trade_date'])
        c = close_asof(d)
        if c is None:
            continue
        try:
            gap = (float(r['open']) / float(r['preclose']) - 1) * 100
        except (TypeError, ValueError, ZeroDivisionError):
            gap = None
        if prev is not None:
            rows.append({'trade_date': d, 'ov_move': round((c / prev - 1) * 100, 3),
                         'open_gap': None if gap is None else round(gap, 3)})
        prev = c
    return pd.DataFrame(rows)


def consistency(merged: pd.DataFrame) -> Dict[str, Any]:
    """同向率 = P(sign(ov_move) == sign(上证 open_gap))；任一侧恰为 0 剔除并计数。"""
    df = merged.dropna(subset=['ov_move', 'open_gap']) if not merged.empty else merged
    both_nz = df[(df['ov_move'] != 0) & (df['open_gap'] != 0)]
    rate = (float(((both_nz['ov_move'] > 0) == (both_nz['open_gap'] > 0)).mean())
            if len(both_nz) else None)
    return {'n': int(len(both_nz)),
            'agree_rate': round(rate * 100, 1) if rate is not None else None,
            'zero_excluded': int(len(df) - len(both_nz))}


def trigger_availability(merged: pd.DataFrame,
                         thresholds=(1.0, 1.5, 2.0),
                         weekday: int = 0) -> List[Dict[str, Any]]:
    """触发可用性（默认周一子样本=周末缝）：|move| 越各阈的频度与 P1/P3 开盘
    方向命中率（P1=≤−thr 且当日低开；P3=≥+thr 且当日高开）。"""
    if merged.empty:
        return []
    df = merged.copy()
    df['weekday'] = df['trade_date'].map(lambda d: pd.Timestamp(d).weekday())
    sub = df[df['weekday'] == weekday].dropna(subset=['ov_move', 'open_gap'])
    out = []
    for t in thresholds:
        hit = sub[sub['ov_move'].abs() >= t]
        p1 = hit[hit['ov_move'] <= -t]
        p3 = hit[hit['ov_move'] >= t]
        out.append({'threshold': t, 'anchor_days': int(len(sub)),
                    'cross_days': int(len(hit)),
                    'cross_pct': (round(len(hit) / len(sub) * 100, 1)
                                  if len(sub) else None),
                    'p1_n': int(len(p1)),
                    'p1_open_down': int((p1['open_gap'] < 0).sum()) if len(p1) else 0,
                    'p3_n': int(len(p3)),
                    'p3_open_up': int((p3['open_gap'] > 0).sum()) if len(p3) else 0})
    return out


def a50_gate_reads(cons_mon: Dict[str, Any], avail_15: Optional[Dict[str, Any]],
                   bars: int) -> Dict[str, Any]:
    """A5 准入门三门读数（V4.0 §2.X3）：① 周一子样本同向率 ≥60%；
    ② 周一子样本越 ±1.5% 频度 ≥10%；③ 前向案例人工登记（恒 pending）。
    样本不足（<A50_MIN_BARS）时不给 pass 结论。"""
    sufficient = bars >= A50_MIN_BARS
    g1 = cons_mon.get('agree_rate')
    g2 = (avail_15 or {}).get('cross_pct') if avail_15 else None
    return {
        'sample_sufficient': sufficient,
        'gate1_same_direction': {'value': g1, 'threshold': GATE1_SAME_DIRECTION_PCT,
                                 'pass': (g1 is not None and sufficient
                                          and g1 >= GATE1_SAME_DIRECTION_PCT)},
        'gate2_cross_freq': {'value': g2, 'threshold': GATE2_CROSS_FREQ_PCT,
                             'pass': (g2 is not None and sufficient
                                      and g2 >= GATE2_CROSS_FREQ_PCT)},
        'gate3_forward_cases': {'value': None, 'pass': None,
                                'note': '前向观察人工登记（≥2 例方向一致），'
                                        'Phase 1 观察锚 note 积累至观察期满'},
    }
