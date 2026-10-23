"""情绪判据校准工具（开发项 B，方案 4.2；只读——不写库、不改配置）。

输入：sentiment_metrics_daily（需先完成回填）+ trade_index_info（上证指数次日涨跌）
     + base_trade_calendar（次日交易日映射）
产出：data/reports/sentiment_calibration_*.md 阈值提案报告——**定稿由用户完成**
     （config review.sentiment_rules 与复盘框架.MD §3 由用户人工修改，工具不自动改）。

评估内容（方案 §4.2）：
1. 现行规则逐条：命中天数、命中后次日溢价均值/上涨概率、未命中对照；
2. 网格搜索建议值（climax/ice/ebb 按方案网格）；命中数 ≥30 才采纳，样本不足标"不可校准"
   （第三轮 T15：climax/ebb 单年样本不足，须全 5 年区间；ebb 放宽 -0.5 档兜底）；
3. 四态覆盖率检查（回暖/震荡兜底态占比，防规则过严导致全部落兜底）；
4. **历史分位标定**（P1-10 §4.4，2026-10-22 增量）：对核心读数 max_streak /
   premium_mean / burst_rate 输出 p10/p60/p90 分位——报告层「分位标定四态」
   的并列列即用此分位框架（规则四态保持基线不替换，回归红线）。

判据口径与 aggregator._sentiment_cycle 一致（高潮→冰点→退潮→回暖/震荡，先命中先定位）。
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ..config.config_manager import ConfigManager
from ..database.connection import DatabaseConnection
from ..sync.review_common import write_review_report

logger = logging.getLogger(__name__)

#: 采纳线：命中样本 < 30 天的网格组合标注"不可校准"（方案 §4.2）
MIN_SAMPLES = 30
#: 方案网格（§4.2；ebb 已按第五轮 T15 放宽 -0.5 档）
GRID = {
    'climax': {'max_streak_gte': [5, 6, 7], 'premium_mean_gte': [3.0, 4.0, 5.0]},
    'ice': {'max_streak_lte': [2, 3], 'burst_rate_gte': [0.4, 0.5, 0.6]},
    'ebb': {'premium_mean_lte': [-1.0, -0.5, 0.0, 1.0]},
}
_STATE_ORDER = ('高潮', '冰点', '退潮', '回暖/震荡')


# ---------- 数据装载 ----------

def load_days(db: DatabaseConnection, start: str, end: str,
              index_ts_code: str) -> pd.DataFrame:
    """表内逐日特征 + 次日结果（次日溢价/涨停家数/上证涨跌幅），末端外推 30 自然日取次日。"""
    rows = db.execute_query(
        """
        SELECT trade_date, limit_up_count, burst_rate, max_streak, premium_mean
        FROM sentiment_metrics_daily WHERE trade_date >= %s AND trade_date <= %s
        ORDER BY trade_date
        """, (start, end))
    if not rows:
        return pd.DataFrame()
    end_ext = _shift_days(end, 30)
    cal = db.execute_query(
        """
        SELECT calendar_date FROM base_trade_calendar
        WHERE is_trading_day = 1 AND calendar_date >= %s AND calendar_date <= %s
        ORDER BY calendar_date
        """, (start, end_ext))
    idx_rows = db.execute_query(
        "SELECT trade_date, change_rate FROM trade_index_info "
        "WHERE ts_code = %s AND trade_date >= %s AND trade_date <= %s",
        (index_ts_code, start, end_ext))
    trading = [_plain(str(r['calendar_date'])) for r in cal]
    next_map = {trading[i]: trading[i + 1] for i in range(len(trading) - 1)}
    idx_chg = {str(r['trade_date']): _f(r['change_rate']) for r in idx_rows}

    m = {}
    for r in rows:
        m[str(r['trade_date'])] = {
            'limit_up_count': _f(r['limit_up_count'], int),
            'burst_rate': _f(r['burst_rate']),
            'max_streak': _f(r['max_streak'], int),
            'premium_mean': _f(r['premium_mean']),
        }
    out = []
    for t, feat in m.items():
        nxt = next_map.get(t)
        if nxt is None or nxt not in m:
            continue  # 次日无表行（区间末端/数据缺失）——无结果不成样本
        out.append({
            'trade_date': t,
            **feat,
            'next_premium_mean': m[nxt]['premium_mean'],
            'next_limit_up_count': m[nxt]['limit_up_count'],
            'next_index_chg': idx_chg.get(nxt),
        })
    return pd.DataFrame(out)


# ---------- 判定（与 _sentiment_cycle 同序同义） ----------

def hit_climax(df: pd.DataFrame, max_streak_gte: float, premium_mean_gte: float) -> pd.Series:
    return (df['max_streak'] >= max_streak_gte) & (df['premium_mean'] >= premium_mean_gte)


def hit_ice(df: pd.DataFrame, max_streak_lte: float, burst_rate_gte: float) -> pd.Series:
    return (df['max_streak'] <= max_streak_lte) | (df['burst_rate'] >= burst_rate_gte)


def hit_ebb(df: pd.DataFrame, premium_mean_lte: float) -> pd.Series:
    return df['premium_mean'] <= premium_mean_lte


def state_series(df: pd.DataFrame, rules: Dict[str, Any]) -> pd.Series:
    """四态定位（先命中先定位：高潮→冰点→退潮→回暖/震荡兜底）"""
    climax = rules.get('climax') or {}
    ice = rules.get('ice') or {}
    ebb = rules.get('ebb') or {}
    states = pd.Series('回暖/震荡', index=df.index, dtype=object)
    states[hit_ebb(df, float(ebb.get('premium_mean_lte', -999)))] = '退潮'
    states[hit_ice(df, float(ice.get('max_streak_lte', -999)),
                   float(ice.get('burst_rate_gte', 999)))] = '冰点'
    states[hit_climax(df, float(climax.get('max_streak_gte', 999)),
                      float(climax.get('premium_mean_gte', 999)))] = '高潮'
    return states


# ---------- 统计 ----------

def outcome_stats(df: pd.DataFrame, mask: pd.Series) -> Dict[str, Any]:
    """命中子集的次日结果（溢价均值/上涨概率/涨停家数均值）；空集安全"""
    sub = df[mask]
    n = int(len(sub))
    if n == 0:
        return {'n': 0, 'next_premium_mean': None, 'up_prob': None, 'next_limit_up_mean': None}
    prem = sub['next_premium_mean'].dropna()
    chg = sub['next_index_chg'].dropna()
    lum = sub['next_limit_up_count'].dropna()
    return {
        'n': n,
        'next_premium_mean': round(float(prem.mean()), 2) if len(prem) else None,
        'up_prob': round(float((chg > 0).mean()), 3) if len(chg) else None,
        'next_limit_up_mean': round(float(lum.mean()), 1) if len(lum) else None,
    }


def grid_lines(df: pd.DataFrame, rule_name: str, combos: List[Tuple[Dict[str, float], str]]
               ) -> List[List[str]]:
    """单个规则的网格搜索结果表（含采纳标注）"""
    rows = []
    for params, desc in combos:
        if rule_name == 'climax':
            mask = hit_climax(df, params['max_streak_gte'], params['premium_mean_gte'])
        elif rule_name == 'ice':
            mask = hit_ice(df, params['max_streak_lte'], params['burst_rate_gte'])
        else:
            mask = hit_ebb(df, params['premium_mean_lte'])
        st = outcome_stats(df, mask)
        verdict = (f"建议 {desc}" if st['n'] >= MIN_SAMPLES
                   else f"不可校准（样本 {st['n']} < {MIN_SAMPLES}）")
        rows.append([
            desc, str(st['n']),
            '—' if st['next_premium_mean'] is None else f"{st['next_premium_mean']:+.2f}%",
            '—' if st['up_prob'] is None else f"{st['up_prob']:.0%}",
            '—' if st['next_limit_up_mean'] is None else f"{st['next_limit_up_mean']:.0f}",
            verdict,
        ])
    return rows


# ---------- 历史分位标定（P1-10 §4.4 增量） ----------

#: 分位标定读数（与 aggregator._sentiment_cycle 同口径字段）
_QUANTILE_READINGS = ('max_streak', 'premium_mean', 'burst_rate')

#: 默认分位档（config review.sentiment_rules.quantile 可覆盖）
DEFAULT_QUANTILES = {'p_low': 10, 'p_mid': 60, 'p_high': 90}


def quantile_calibration(df: pd.DataFrame,
                         quantiles: Optional[Dict[str, float]] = None
                         ) -> Dict[str, Dict[str, Optional[float]]]:
    """核心读数的历史分位标定（P1-10 §4.4）。

    Returns:
        ``{读数: {'p10': v, 'p60': v, 'p90': v, 'n': 样本数}}``；读数为空 →
        各档 None（不硬造）。
    """
    q = {**DEFAULT_QUANTILES, **(quantiles or {})}
    out: Dict[str, Dict[str, Optional[float]]] = {}
    for col in _QUANTILE_READINGS:
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors='coerce').dropna()
        if s.empty:
            out[col] = {'p10': None, 'p60': None, 'p90': None, 'n': 0}
            continue
        out[col] = {
            'p10': round(float(s.quantile(q['p_low'] / 100.0)), 3),
            'p60': round(float(s.quantile(q['p_mid'] / 100.0)), 3),
            'p90': round(float(s.quantile(q['p_high'] / 100.0)), 3),
            'n': int(len(s)),
        }
    return out


def quantile_state(m: Dict[str, Any],
                   calib: Dict[str, Dict[str, Optional[float]]]) -> Tuple[str, List[str]]:
    """分位标定四态（报告层并列列；与规则四态同序同义，判定基准换成分位）。

    - 高潮：max_streak ≥ p90 且 premium_mean ≥ p90；
    - 冰点：max_streak ≤ p10 或 burst_rate ≥ p90；
    - 退潮：premium_mean ≤ p10；
    - 兜底：回暖/震荡（分位数据不足时同归兜底并标注）。
    """
    def _val(col: str, key: str) -> Optional[float]:
        node = calib.get(col) or {}
        v = node.get(key)
        return float(v) if v is not None else None

    ms = m.get('max_streak') or 0
    pm = m.get('premium_mean')
    br = m.get('burst_rate')
    basis: List[str] = []
    if not calib:
        return '回暖/震荡', ['分位标定数据不足']
    p90_ms, p90_pm = _val('max_streak', 'p90'), _val('premium_mean', 'p90')
    p10_ms, p90_br = _val('max_streak', 'p10'), _val('burst_rate', 'p90')
    p10_pm = _val('premium_mean', 'p10')
    if p90_ms is not None and p90_pm is not None and pm is not None \
            and ms >= p90_ms and pm >= p90_pm:
        return '高潮', [f"高度{ms}≥p90({p90_ms})", f"溢价{pm}≥p90({p90_pm})"]
    if p10_ms is not None and ms <= p10_ms:
        basis.append(f"高度{ms}≤p10({p10_ms})")
    if p90_br is not None and br is not None and br >= p90_br:
        basis.append(f"炸板率{br}≥p90({p90_br})")
    if basis:
        return '冰点', basis
    if p10_pm is not None and pm is not None and pm <= p10_pm:
        return '退潮', [f"溢价均值{pm}≤p10({p10_pm})"]
    return '回暖/震荡', ['未达分位极值档']


# ---------- 主流程 ----------

def run_calibration(config_manager: ConfigManager, start: str, end: str) -> Dict[str, Any]:
    """跑校准并产出提案报告（只读；返回 stats 与报告路径）"""
    db = DatabaseConnection(config_manager)
    config = config_manager.load_config()
    rules = dict((config.get('review') or {}).get('sentiment_rules') or {})
    indices = (config.get('review_sync') or {}).get('indices') or []
    index_ts_code = next((i.get('ts_code') for i in indices if '上证' in (i.get('name') or '')),
                         'sh.000001')

    df = load_days(db, _plain(start), _plain(end), index_ts_code)
    if df.empty:
        raise ValueError('sentiment_metrics_daily 区间内无数据——先跑 review backfill-sentiment')

    lines: List[str] = [
        f'- 样本区间：{_plain(start)}~{_plain(end)}，成对样本 {len(df)} 天'
        f'（次日结果可得的交易日；上证指数 {index_ts_code}）',
        '- 判定口径：与报告层 _sentiment_cycle 同序同义（高潮→冰点→退潮→回暖/震荡）',
        '- 采纳规则：命中样本 ≥30 天才给出建议值，否则标"不可校准"（防小样本过拟合）',
        '',
    ]

    # 1. 现行规则逐条
    lines.append('## 一、现行规则评估（config review.sentiment_rules）')
    lines.append('')
    lines.append('| 规则 | 判据 | 命中天数 | 次日溢价均值 | 次日上证上涨概率 | 次日涨停家数均值 |')
    lines.append('|---|---|---|---|---|---|')
    rules_desc = {
        'climax': ('高潮', hit_climax(df, float(rules.get('climax', {}).get('max_streak_gte', 999)),
                                      float(rules.get('climax', {}).get('premium_mean_gte', 999))),
                   f"高度≥{rules.get('climax', {}).get('max_streak_gte')} 且 "
                   f"溢价≥{rules.get('climax', {}).get('premium_mean_gte')}%"),
        'ice': ('冰点', hit_ice(df, float(rules.get('ice', {}).get('max_streak_lte', -1)),
                                float(rules.get('ice', {}).get('burst_rate_gte', 999))),
                f"高度≤{rules.get('ice', {}).get('max_streak_lte')} 或 "
                f"炸板率≥{rules.get('ice', {}).get('burst_rate_gte')}"),
        'ebb': ('退潮', hit_ebb(df, float(rules.get('ebb', {}).get('premium_mean_lte', 999))),
                f"溢价均值≤{rules.get('ebb', {}).get('premium_mean_lte')}%"),
    }
    for name in ('climax', 'ice', 'ebb'):
        label, mask, desc = rules_desc[name]
        st = outcome_stats(df, mask)
        comp = outcome_stats(df, ~mask)
        lines.append(f"| {label} | {desc} | {st['n']} | {_fmt_pct(st['next_premium_mean'])} "
                     f"（未命中对照 {_fmt_pct(comp['next_premium_mean'])}）| "
                     f"{_fmt_prob(st['up_prob'])}（对照 {_fmt_prob(comp['up_prob'])}）| "
                     f"{_fmt_num(st['next_limit_up_mean'])} |")
    lines.append('')

    # 2. 四态覆盖率
    states = state_series(df, rules)
    counts = states.value_counts()
    total = int(len(states))
    lines.append('## 二、四态覆盖率（防规则过严全部落兜底）')
    lines.append('')
    lines.append('| 状态 | 天数 | 占比 |')
    lines.append('|---|---|---|')
    for s in _STATE_ORDER:
        n = int(counts.get(s, 0))
        lines.append(f'| {s} | {n} | {n / total:.1%} |')
    fallback = int(counts.get('回暖/震荡', 0))
    if fallback / total > 0.6:
        lines.append('')
        lines.append(f'> ⚠ 兜底态（回暖/震荡）占比 {fallback / total:.0%} 偏高——'
                     f'现行判据可能过严，建议参考下方网格放宽 ice/ebb。')
    lines.append('')

    # 3. 网格搜索
    lines.append('## 三、网格搜索建议值（阈值提案，定稿由用户完成）')
    lines.append('')
    for rule_name, title in (('climax', '高潮 climax：max_streak × premium_mean'),
                             ('ice', '冰点 ice：max_streak × burst_rate'),
                             ('ebb', '退潮 ebb：premium_mean')):
        combos: List[Tuple[Dict[str, float], str]] = []
        grid = GRID[rule_name]
        if rule_name == 'climax':
            combos = [({'max_streak_gte': a, 'premium_mean_gte': b}, f'高度≥{a} 且 溢价≥{b}%')
                      for a in grid['max_streak_gte'] for b in grid['premium_mean_gte']]
        elif rule_name == 'ice':
            combos = [({'max_streak_lte': a, 'burst_rate_gte': b}, f'高度≤{a} 或 炸板率≥{b}')
                      for a in grid['max_streak_lte'] for b in grid['burst_rate_gte']]
        else:
            combos = [({'premium_mean_lte': a}, f'溢价均值≤{a}%') for a in grid['premium_mean_lte']]
        lines.append(f'### {title}')
        lines.append('')
        lines.append('| 阈值组合 | 命中天数 | 次日溢价均值 | 次日上涨概率 | 次日涨停家数均值 | 结论 |')
        lines.append('|---|---|---|---|---|---|')
        lines.extend('| ' + ' | '.join(str(c) for c in row) + ' |'
                     for row in grid_lines(df, rule_name, combos))
        lines.append('')
    # 4. 历史分位标定（P1-10 增量）
    lines.append('## 四、历史分位标定（P1-10：报告层分位四态的基准）')
    lines.append('')
    calib = quantile_calibration(df)
    lines.append('| 读数 | p10 | p60 | p90 | 样本天数 |')
    lines.append('|---|---|---|---|---|')
    for col in _QUANTILE_READINGS:
        node = calib.get(col)
        if not node:
            continue
        lines.append(f"| {col} | {node['p10']} | {node['p60']} | {node['p90']} "
                     f"| {node['n']} |")
    lines.append('')
    lines.append('> 用法：报告层「分位标定四态」列 = 上述分位为动态阈值的同序判定'
                 '（高潮=高度/溢价双超 p90；冰点=高度≤p10 或 炸板率≥p90；'
                 '退潮=溢价≤p10）。分位随样本滚动，**规则四态仍是基线列不替换**；'
                 '两列不一致时（规则高潮 vs 分位未达极值）点名「判据与分位背离」，'
                 '属校准观察项非故障。')
    lines.append('')
    lines.append('## 五、定稿提示')
    lines.append('')
    lines.append('- 本报告只产出提案；定稿动作（修改 config `review.sentiment_rules` 与 '
                 '复盘框架.MD §3）由用户完成，工具不自动改配置。')
    lines.append('- 建议优先采纳命中数充足且与未命中对照区分度大的组合'
                 '（次日溢价均值差、上涨概率差显著）。')
    lines.append('- ebb 极端退潮日 5 年累计可能仍不足 30 天（T15），由"不可校准"标注兜底，'
                 '维持现行阈值并如实记录即可。')

    report_path = _write_calibration_report(
        f'情绪判据校准提案（{_plain(start)}~{_plain(end)}）', lines)
    return {'samples': len(df), 'report_path': report_path}


def _write_calibration_report(title: str, lines: List[str]) -> Optional[str]:
    """校准报告落盘（统一骨架；正文含 md 表格原样写入）"""
    return write_review_report(title, 'sentiment_calibration', '提案产出（只读，未改配置）',
                               lines, None)


# ---------- 小工具 ----------

def _f(v, cast=float):
    if v is None:
        return None
    try:
        return cast(v)
    except (TypeError, ValueError):
        return None


def _plain(v: Any) -> str:
    return str(v).replace('-', '')[:8]


def _shift_days(yyyymmdd: str, days: int) -> str:
    from datetime import datetime as _dt, timedelta as _td
    base = _dt.strptime(_plain(yyyymmdd), '%Y%m%d')
    return (base + _td(days=days)).strftime('%Y%m%d')


def _fmt_pct(v):
    return '—' if v is None else f'{v:+.2f}%'


def _fmt_prob(v):
    return '—' if v is None else f'{v:.0%}'


def _fmt_num(v):
    return '—' if v is None else f'{v:.1f}'
