"""因子评估最小闭环（P1-6，方案 §4.3）：RankIC + 前缀不变性自检。

不引入回测引擎（评估报告结论），只做两件事：

① **RankIC**：对 R1-R4 候选信号与前瞻收益算逐截面 Spearman 秩相关——
   信号源为 ``candidate_rules`` 谓词 + ``cross_section`` 向量化面板**库内重算**
   （不逐日跑链：660 交易日 × 全市场链运行约 3 小时不可接受，本引擎一次大面板
   装载 + 向量化判定 + 逐日查表，方案估算 ~11 分钟）；
② **前缀不变性自检**：全量面板 vs 截断到末日前一日的面板重算信号，要求
   ≤末日前一日的信号逐值一致（证明信号只依赖 ≤当日数据，无前视）。

口径红线：
- 前瞻收益 = T+h / T 的**收盘价比值**（前复权 close 比值口径，不受复权基准影响）；
- 每日有效样本 < ``min_names``（默认 50）该日不计 IC（不硬造）；
- 禁 inf/NaN 进 IC（``np.isfinite`` 守卫）；输出 CSV + meta（含逐日 IC 序列）；
- 谓词唯一定义在 ``candidate_rules``（本引擎列式查表后逐行调用，禁另写一套）。
"""

from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from ..analysis.candidate_rules import is_r1, is_r2, is_r3, is_r4
from ..review.cross_section import annotate_panel, suppress_first_day

logger = logging.getLogger(__name__)

#: 默认样本期（方案 §4.3：2024-01 ~ 2026-09，约 660 交易日）
DEFAULT_START = '20240101'
DEFAULT_END = '20260930'

#: 面板加载列（annotate_panel 需要全集）
_PANEL_COLS = ['stock_name', 'open', 'high', 'low', 'close', 'preclose',
               'change_rate', 'volume', 'amount', 'is_st']

#: 自检信号（已知有短期动量：连板数越高短期超额越稳——IC 应显著不为 0，
#: 若为 0 反而是实现错误的自检信号，方案 §4.3 验收）
SELF_CHECK_SIGNAL = '连板数'

#: 默认信号与前瞻 horizon
DEFAULT_SIGNALS = ['r1', 'r2', 'r3', 'r4']
DEFAULT_HORIZONS = [1, 5, 20]


def _finite(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


# ------------------------------------------------------------------ RankIC 核心

def rankic(signal: pd.Series, fwd_return: pd.Series,
           min_names: int = 50) -> Dict[str, Any]:
    """逐截面 Spearman 秩相关。

    Args:
        signal: 信号值（R1-R4 布尔 / 连板数等），以 ``(trade_date, ts_code)``
            为 MultiIndex 的 Series（长表）。
        fwd_return: 前瞻收益（小数，如 0.05 = +5%），同索引对齐。
        min_names: 每日有效样本下限，不足该日不计（不硬造）。

    Returns:
        ``{rank_ic, ic_mean, ic_std, icir, n_days, n_names, per_day_ic}``；
        ``rank_ic`` 为最新截面 IC；``icir`` = ic_mean/ic_std（std≤0 → None，
        禁 inf）。
    """
    if not isinstance(signal.index, pd.MultiIndex) or signal.index.nlevels != 2:
        raise ValueError('signal/fwd_return 须以 (trade_date, ts_code) 为 MultiIndex')
    df = pd.concat({'sig': signal, 'fwd': fwd_return}, axis=1).dropna()
    df = df[np.isfinite(df['sig'].astype(float)) & np.isfinite(df['fwd'].astype(float))]
    if df.empty:
        return {'rank_ic': None, 'ic_mean': None, 'ic_std': None, 'icir': None,
                'n_days': 0, 'n_names': 0, 'per_day_ic': pd.Series(dtype=float)}

    ics: Dict[str, float] = {}
    last_names = 0
    for trade_date, g in df.groupby(level=0, sort=True):
        if len(g) < min_names:
            continue
        pair = pd.DataFrame({'s': pd.to_numeric(g['sig'], errors='coerce'),
                             'f': pd.to_numeric(g['fwd'], errors='coerce')}).dropna()
        if len(pair) < min_names:
            continue
        # Spearman = 秩的 Pearson（布尔信号自动按平均秩处理并列）
        ic = pair['s'].rank().corr(pair['f'].rank())
        if ic is not None and math.isfinite(ic):
            ics[str(trade_date)] = float(ic)
            last_names = int(len(pair))
    if not ics:
        return {'rank_ic': None, 'ic_mean': None, 'ic_std': None, 'icir': None,
                'n_days': 0, 'n_names': 0, 'per_day_ic': pd.Series(dtype=float)}
    series = pd.Series(ics).sort_index()
    ic_mean = float(series.mean())
    ic_std = float(series.std(ddof=1)) if len(series) > 1 else None
    # ICIR = mean/std：std 为 None/≤0/近零（浮点残渣）时无定义 → None（禁 inf/爆炸）
    icir = (ic_mean / ic_std) if (ic_std is not None and ic_std > 1e-12) else None
    return {
        'rank_ic': float(series.iloc[-1]),
        'ic_mean': round(ic_mean, 6),
        'ic_std': round(ic_std, 6) if ic_std is not None else None,
        'icir': round(icir, 4) if icir is not None else None,
        'n_days': int(len(series)),
        'n_names': int(last_names),
        'per_day_ic': series,
    }


def prefix_invariance(signal_full: pd.Series,
                      signal_truncated: pd.Series) -> Dict[str, Any]:
    """前缀不变性对比：交集上逐值相等（无前视自检）。

    Returns:
        ``{ok, n_compared, n_diff, first_diff}``；索引无交集 → ``ok=False``。
    """
    common = signal_full.index.intersection(signal_truncated.index)
    if len(common) == 0:
        return {'ok': False, 'n_compared': 0, 'n_diff': -1,
                'first_diff': '索引无交集（无法对比）'}
    a = signal_full.loc[common]
    b = signal_truncated.loc[common]
    if a.dtype == bool or b.dtype == bool:
        diff = a.astype(str) != b.astype(str)
    else:
        diff = ~((a == b) | (a.isna() & b.isna()))
    n_diff = int(diff.sum())
    first = None
    if n_diff:
        idx = diff[diff].index[0]
        first = f'{idx}: {a.loc[idx]} vs {b.loc[idx]}'
    return {'ok': n_diff == 0, 'n_compared': int(len(common)),
            'n_diff': n_diff, 'first_diff': first}


# ------------------------------------------------------------------ 信号与收益面

def _nth_trading_day_forward(reader, as_of: str, n: int) -> Optional[str]:
    """锚定日往后第 ``n`` 个交易日（reader 只有 back 版，此处对称补 forward）。"""
    if n <= 0:
        return as_of
    rows = reader._query(
        """
        SELECT calendar_date FROM base_trade_calendar
        WHERE is_trading_day = 1 AND calendar_date >= %s
        ORDER BY calendar_date ASC LIMIT %s
        """,
        (as_of.replace('-', ''), n + 1))
    if len(rows) < n + 1:
        return None
    return str(rows[-1][0]).replace('-', '')


def forward_returns(reader, start: str, end: str,
                    horizons: Sequence[int]) -> Dict[int, pd.Series]:
    """前瞻收益：T+h / T 收盘价比值（前复权 close 比值，不受复权基准影响）。

    返回 ``{h: Series(MultiIndex trade_date, ts_code)}``；价格窗口自动向后延
    max(horizons) 个交易日（样本期外的延伸只用于前瞻，不进 IC 样本）。
    """
    max_h = max(horizons)
    end_buf = _nth_trading_day_forward(reader, end, max_h) or end
    close_df = reader.load_kline(['close'], end_buf, start_date=start)
    if close_df.empty:
        return {}
    close = pd.to_numeric(close_df['close'], errors='coerce')
    pivot = (pd.DataFrame({'trade_date': close_df['trade_date'].astype(str),
                           'ts_code': close_df['ts_code'].astype(str),
                           'close': close})
             .pivot(index='trade_date', columns='ts_code', values='close')
             .sort_index())
    out: Dict[int, pd.Series] = {}
    for h in horizons:
        fwd = pivot.shift(-h) / pivot - 1.0
        long = fwd.stack(dropna=True)
        long.index = long.index.set_names(['trade_date', 'ts_code'])
        out[int(h)] = long[np.isfinite(long)]
    return out


def _day_limit_up_codes(panel: pd.DataFrame, trade_date: str) -> set:
    day = panel[panel['trade_date'].astype(str) == trade_date]
    return set(day.loc[day['_limit_up'], 'ts_code'].astype(str))


def _max_cluster(concepts: Optional[Sequence[str]], top: set,
                  counts: Dict[str, int]) -> Optional[int]:
    """个股概念与聚集集合交集中的最大涨停家数（R2 聚集强度输入）。

    无交集/计数缺失 → None（is_r2 的 cluster_count=None 不过该附加条件，
    不硬造）。
    """
    if not concepts or not top or not counts:
        return None
    hit = [counts[c] for c in concepts if c in top and c in counts]
    return max(hit) if hit else None


def _day_maps(reader, trade_date: str, config_manager,
              limit_up_codes: set) -> Dict[str, Any]:
    """单日 env 查表（R1 开板 / R2 概念与聚集 / R4 机构净买）——复用 env_assembly
    同函数（与链/报告层同口径，原则 P6）。"""
    from ..analysis.env_assembly import (_concept_cluster_counts,
                                         _institution_net_map, _seal_open_map,
                                         _theme_map, _top_concepts)
    review_cfg = (config_manager.load_config().get('review') or {})
    blacklist = [str(x) for x in (review_cfg.get('theme_exclude_concepts') or [])]
    theme_map = _theme_map(reader, trade_date)
    return {
        'seal': _seal_open_map(reader, trade_date),
        'theme': theme_map,
        'top_concepts': _top_concepts(theme_map, limit_up_codes, blacklist),
        'cluster_counts': _concept_cluster_counts(theme_map, limit_up_codes,
                                                  blacklist),
        'institution': _institution_net_map(reader, trade_date),
    }


def _lhb_dates(reader, start: str, end: str) -> set:
    """样本期内有龙虎榜明细的日期集（实测 lhb_detail 仅稀疏月份有量：
    201901/202301/202401/202609——无行日期 R4 记 NaN 出局，禁静默全 False）。"""
    try:
        rows = reader._query(
            "SELECT DISTINCT trade_date FROM lhb_detail "
            "WHERE trade_date BETWEEN %s AND %s", (start, end))
    except Exception as e:  # pragma: no cover - 库异常兜底
        logger.warning('LHB 日期集读取失败: %r', e)
        return set()
    return {str(r[0]).replace('-', '') for r in rows}


def build_signals(reader, start: str, end: str, config_manager,
                  signals: Optional[Sequence[str]] = None,
                  progress_every: int = 20) -> pd.DataFrame:
    """库内重算信号面板：一次大面板装载 + annotate + 逐日查表 → 长表。

    返回列：请求的信号（``r1/r2/r3/r4`` 布尔）+ ``连板数``（自检信号），
    索引 ``(trade_date, ts_code)``，日期范围 = 样本期。
    """
    signals = list(signals or DEFAULT_SIGNALS)
    want_all = list(signals) + [SELF_CHECK_SIGNAL]
    # 滚动指标（60 日新高/放量基数）需要 start 之前的历史——与报告层面板同口径
    # （nth_trading_day_back(start, 62)）；信号只取 [start, end] 截面
    panel_start = reader.nth_trading_day_back(start, 62) or start
    df = reader.load_kline(_PANEL_COLS, end, start_date=panel_start)
    if df.empty:
        return pd.DataFrame()
    panel = annotate_panel(df)
    list_dates = {s.ts_code: s.list_date for s in reader.load_stock_universe()}
    panel = suppress_first_day(panel, list_dates)

    dates = sorted(d for d in panel['trade_date'].astype(str).unique()
                   if start <= d <= end)
    need_maps = any(s in ('r1', 'r2', 'r4') for s in want_all)

    frames: List[pd.DataFrame] = []
    r2_gap_days: List[str] = []
    r4_gap_days: List[str] = []
    lhb_dates = _lhb_dates(reader, start, end) if 'r4' in want_all else set()
    for i, d in enumerate(dates):
        day = panel[panel['trade_date'].astype(str) == d]
        if day.empty:
            continue
        codes = day['ts_code'].astype(str)
        out = pd.DataFrame(index=pd.MultiIndex.from_arrays(
            [[d] * len(day), codes.values], names=['trade_date', 'ts_code']))
        out[SELF_CHECK_SIGNAL] = day['_streak'].astype(int).values
        if need_maps:
            maps = _day_maps(reader, d, config_manager, _day_limit_up_codes(panel, d))
            # 纯列表查表（dict.get 保 None）——禁 Series.map：pandas 会把 None
            # 转 NaN，而 is_r4 的 float(NaN)<=0 为 False 会漏过守卫把缺失当正值
            seal_vals = [maps['seal'].get(c) for c in codes]
            inst_vals = [maps['institution'].get(c) for c in codes]
            theme_vals = [maps['theme'].get(c) for c in codes]
            top = set(maps['top_concepts'] or [])
        is_st = day['is_st'].astype(bool)
        if 'r1' in want_all:
            out['r1'] = [is_r1(int(s), bool(st), oc)
                         for s, st, oc in zip(day['_streak'], is_st, seal_vals)]
        if 'r2' in want_all:
            if not maps.get('theme'):
                # 概念成员时点不可得（实测 sina_gn 仅近期有量）→ 当日 R2 不可算，
                # 记 NaN 出局（禁静默全 False 造成「无信号」假象）
                out['r2'] = np.nan
                r2_gap_days.append(d)
            else:
                # 聚集强度（P1-7 A）：命中聚集概念内当日涨停家数 < r2_cluster_min
                # 不通过；取该股概念与聚集集合交集中家数最大者
                counts = maps.get('cluster_counts') or {}
                out['r2'] = [is_r2(int(s), bool(st), c or [], top,
                                   cluster_count=_max_cluster(c or [], top, counts))
                             for s, st, c in zip(day['_streak'], is_st, theme_vals)]
        if 'r3' in want_all:
            out['r3'] = [is_r3(bool(nh), bool(st), _finite(vs), _finite(cr))
                         for nh, st, vs, cr in zip(day['_new_high'], is_st,
                                                   day['_volume_surge'],
                                                   day['change_rate'])]
        if 'r4' in want_all:
            if d not in lhb_dates:
                # 当日无龙虎榜明细（采集缺口，非「无机构席位」）→ R4 不可算
                out['r4'] = np.nan
                r4_gap_days.append(d)
            else:
                names = day['stock_name'].astype(str)
                out['r4'] = [is_r4(v, 'ST' in n.upper())
                             for v, n in zip(inst_vals, names)]
        frames.append(out)
        if progress_every and (i + 1) % progress_every == 0:
            logger.info('信号重算进度 %d/%d（%s）', i + 1, len(dates), d)
    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames).sort_index()
    if r2_gap_days:
        logger.warning('R2 概念成员时点不可得 %d/%d 日（sina_gn 仅近期有量），'
                       '这些日期 R2 记 NaN 出局', len(r2_gap_days), len(dates))
    if r4_gap_days:
        logger.warning('R4 龙虎榜明细缺失 %d/%d 日（lhb_detail 稀疏采集），'
                       '这些日期 R4 记 NaN 出局', len(r4_gap_days), len(dates))
    return result


def evaluate(signals: Sequence[str], horizons: Sequence[int], start: str, end: str,
             reader, config_manager, min_names: int = 50,
             prefix_check: bool = True) -> Dict[str, Any]:
    """RankIC 评估引擎：信号面板 × 前瞻收益 → 逐 (signal, horizon) IC。

    含自检信号（连板数）与可选前缀不变性对比（全量 vs 截断到 end 前一日重算）。
    """
    signals = list(signals)
    signal_df = build_signals(reader, start, end, config_manager,
                              signals=signals + [SELF_CHECK_SIGNAL])
    if signal_df.empty:
        return {'ok': False, 'reason': '信号面板为空（样本期内无数据）'}
    fwd = forward_returns(reader, start, end, horizons)
    if not fwd:
        return {'ok': False, 'reason': '前瞻收益为空（价格窗口不足）'}

    results: Dict[str, Any] = {'ok': True, 'as_of_range': [start, end],
                               'min_names': min_names, 'rankic': {},
                               'per_day_ic': {}, 'coverage': {}}
    all_days = signal_df.index.get_level_values(0).nunique()
    for sig in signals + [SELF_CHECK_SIGNAL]:
        col = signal_df[sig] if sig in signal_df.columns else None
        if col is None:
            results['rankic'][sig] = {'error': f'信号列缺失: {sig}'}
            continue
        computable = col.dropna().index.get_level_values(0).nunique() \
            if col.isna().any() else all_days
        results['coverage'][sig] = {'total_days': int(all_days),
                                    'computable_days': int(computable)}
        s = col
        if s.dtype != bool:
            s = pd.to_numeric(s, errors='coerce')
        for h in horizons:
            f = fwd[int(h)]
            aligned = pd.concat({'sig': s, 'fwd': f}, axis=1).dropna()
            r = rankic(aligned['sig'], aligned['fwd'], min_names=min_names)
            per_day = r.pop('per_day_ic')
            results['rankic'][f'{sig}@T+{h}'] = r
            results['per_day_ic'][f'{sig}@T+{h}'] = per_day

    if prefix_check:
        prev = reader.nth_trading_day_back(end, 1)
        if prev and prev >= start:
            trunc = build_signals(reader, start, prev, config_manager,
                                  signals=signals + [SELF_CHECK_SIGNAL])
            checks: Dict[str, Any] = {}
            for sig in signals + [SELF_CHECK_SIGNAL]:
                if sig in signal_df.columns and sig in trunc.columns:
                    a, b = signal_df[sig], trunc[sig]
                    common = a.index.intersection(b.index)
                    if len(common):
                        checks[sig] = prefix_invariance(a.loc[common], b.loc[common])
            results['prefix_invariance'] = checks
            results['prefix_invariance_ok'] = bool(checks) and all(
                c.get('ok') for c in checks.values())
        else:
            results['prefix_invariance_ok'] = None
            results['prefix_invariance_reason'] = '样本期过短，无前一交易日可截断'
    return results
