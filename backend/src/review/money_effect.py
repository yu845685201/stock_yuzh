"""赚钱效应/亏钱效应聚合（复盘闭环与赚钱效应四期优化方案 §2，M-四期）。

职责：基于报告层 60d 面板截面（annotate_panel 产物，判定与链模块同源）计算
五组指标——调研（开源复盘系统对比 2026-10-07）收敛出的复盘核心：

- **反馈矩阵**：昨日涨停股按昨日板位分组 × 今日五分类（晋级涨停/收红/小跌/
  跌超5%/跌停），"复盘先看死在哪"（vibe-astock 口径）；停牌缺行单独计数不入矩阵；
- **昨日炸板今日修复**（prev_broken_recovery）：昨日炸板股今日均值/中位数/翻红率/回封数；
- **连板溢价**（consec_premium）：昨日 2 板及以上今日均值/中位数/翻红率；
- **大面统计**（loss_effect）：全市场跌超5%/7%家数、最惨跌幅、跌停数 + 昨日涨停股大面；
- **涨跌幅分档分布**：全市场累计分档家数（涨≥2/4/6/8/10%、跌≤−2/−4/−6/−8/−10%）。

五分类互斥判定优先级：晋级涨停 > 跌停 > 跌超5% > 小跌 > 收红（按跌幅从深到浅，
跌停与跌超5% 数值上重叠时分桶互斥，跌停家数另在全市场口径独立出现）。
全部统计只计锚定日有成交行；昨日样本缺失（停牌）如实单列，禁止混入。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import pandas as pd

#: 五分类桶序（渲染列序：从差到好——"复盘先看死在哪"）
CATEGORIES = ('跌停', '跌超5%', '小跌', '收红', '晋级涨停')

#: 昨日板位分组（首板/2板/3板/4板+）
_BIG_BUCKET = 4

#: 分档分布阈值（累计口径，go-stock Up2~Up10 / Down2~Down10）
_DIST_UP = (2.0, 4.0, 6.0, 8.0, 10.0)
_DIST_DOWN = (-2.0, -4.0, -6.0, -8.0, -10.0)

#: 大面统计阈值
LOSS_DOWN5 = -5.0
LOSS_DOWN7 = -7.0


def money_effect(cs_today: pd.DataFrame, cs_prev: pd.DataFrame) -> Dict[str, Any]:
    """赚钱效应/亏钱效应五组指标（截面重算，§5.2 独立重算设计）。

    Args:
        cs_today: 锚定日截面（annotate_panel + cross_section_at 产物）。
        cs_prev: 前一交易日截面（可为空——历史首日）。

    Returns:
        {feedback, burst_recovery, consec_premium, loss, change_dist}；
        昨日截面为空时 feedback/burst_recovery/consec_premium 为 None，
        loss/change_dist 仍按今日全市场给出。
    """
    change = pd.to_numeric(cs_today.get('change_rate'), errors='coerce') \
        if not cs_today.empty else pd.Series(dtype='float64')
    today_map: Dict[str, Dict[str, Any]] = {}
    if not cs_today.empty:
        lu = cs_today['_limit_up'].fillna(False).astype(bool)
        ld = cs_today['_limit_down'].fillna(False).astype(bool)
        names = cs_today.get('stock_name')  # 列缺失（旧合成截面）→ best.name 置空
        for i, (ts_code, chg, up, dn) in enumerate(
                zip(cs_today['ts_code'], change, lu, ld)):
            _nm = names.iloc[i] if names is not None and i < len(names) else None
            today_map[str(ts_code)] = {'chg': None if pd.isna(chg) else float(chg),
                                       'up': bool(up), 'down': bool(dn),
                                       'name': str(_nm) if pd.notna(_nm) else ''}

    prev_map: Dict[str, int] = {}
    prev_burst_codes: List[str] = []
    if not cs_prev.empty:
        prev_up = cs_prev[cs_prev['_limit_up'].fillna(False).astype(bool)]
        for _, r in prev_up.iterrows():
            streak = r.get('_streak')
            prev_map[str(r['ts_code'])] = int(streak) if pd.notna(streak) else 1
        prev_burst = cs_prev[cs_prev['_burst'].fillna(False).astype(bool)]
        prev_burst_codes = [str(c) for c in prev_burst['ts_code']]

    return {
        'feedback': _feedback_matrix(prev_map, today_map),
        'burst_recovery': _burst_recovery(prev_burst_codes, today_map),
        'consec_premium': _consec_premium(prev_map, today_map),
        'loss': _loss_effect(cs_today, prev_map, today_map),
        'change_dist': _change_dist(cs_today),
    }


# ------------------------------------------------------------------ 反馈矩阵

def _outcome(chg: Optional[float], up: bool, down: bool) -> Optional[str]:
    """今日五分类（互斥，优先级从深到浅）；无涨跌幅数据返回 None。"""
    if up:
        return '晋级涨停'
    if down:
        return '跌停'
    if chg is None:
        return None
    if chg <= LOSS_DOWN5:
        return '跌超5%'
    if chg <= 0:
        return '小跌'
    return '收红'


def _bucket_of(streak: int) -> str:
    if streak >= _BIG_BUCKET:
        return '4板+'
    return {1: '首板', 2: '2板', 3: '3板'}.get(streak, f'{streak}板')


def _feedback_matrix(prev_map: Dict[str, int],
                     today_map: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """昨日涨停股板位分组 × 今日五分类 + 每组今日均值/中位数/晋级率。"""
    if not prev_map:
        return None
    buckets: Dict[str, Dict[str, Any]] = {}
    suspended_total = 0
    for ts_code, streak in prev_map.items():
        t = today_map.get(ts_code)
        if t is None:  # 昨日涨停今日无行 = 停牌/缺行，不入矩阵（如实单列）
            suspended_total += 1
            continue
        key = _bucket_of(streak)
        b = buckets.setdefault(key, {'count': 0, 'cats': {c: 0 for c in CATEGORIES},
                                     'chgs': []})
        b['count'] += 1
        cat = _outcome(t['chg'], t['up'], t['down'])
        if cat is not None:
            b['cats'][cat] += 1
        b['chgs'].append(t['chg'])

    rows = []
    for key in ('首板', '2板', '3板', '4板+'):
        b = buckets.get(key)
        if not b:
            continue
        rows.append(_feedback_row(key, b))
    total_chgs = [c for b in buckets.values() for c in b['chgs']]
    if buckets:
        total = {'count': sum(b['count'] for b in buckets.values()),
                 'cats': {c: sum(b['cats'][c] for b in buckets.values())
                          for c in CATEGORIES},
                 'chgs': total_chgs}
        rows.append(_feedback_row('合计', total))
    return {'rows': rows, 'suspended': suspended_total}


def _feedback_row(key: str, b: Dict[str, Any]) -> Dict[str, Any]:
    chgs = [c for c in b['chgs'] if c is not None]
    count = b['count']
    return {'bucket': key, 'count': count,
            'cats': {c: b['cats'][c] for c in CATEGORIES},
            'mean': round(sum(chgs) / len(chgs), 2) if chgs else None,
            'median': round(float(pd.Series(chgs).median()), 2) if chgs else None,
            'limit_rate': round(b['cats']['晋级涨停'] / count, 4) if count else None}


# ------------------------------------------------------------------ 修复与溢价

def _burst_recovery(prev_burst_codes: List[str],
                    today_map: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """昨日炸板股今日修复：样本/均值/中位数/翻红率/回封数。"""
    if not prev_burst_codes:
        return None
    chgs, red, relimit = [], 0, 0
    for code in prev_burst_codes:
        t = today_map.get(code)
        if t is None:
            continue
        if t['chg'] is not None:
            chgs.append(t['chg'])
            if t['chg'] > 0:
                red += 1
        if t['up']:
            relimit += 1
    if not chgs:
        return {'count': len(prev_burst_codes), 'mean': None, 'median': None,
                'red_rate': None, 'relimit': relimit, 'traded': 0}
    return {'count': len(prev_burst_codes), 'traded': len(chgs),
            'mean': round(sum(chgs) / len(chgs), 2),
            'median': round(float(pd.Series(chgs).median()), 2),
            'red_rate': round(red / len(chgs), 4), 'relimit': relimit}


def _consec_premium(prev_map: Dict[str, int],
                    today_map: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """连板溢价：昨日 2 板及以上今日均值/中位数/翻红率（样本描述，不单独判断承接）。"""
    codes = [c for c, s in prev_map.items() if s >= 2]
    if not codes:
        return None
    chgs, red = [], 0
    for code in codes:
        t = today_map.get(code)
        if t is None or t['chg'] is None:
            continue
        chgs.append(t['chg'])
        if t['chg'] > 0:
            red += 1
    if not chgs:
        return {'count': len(codes), 'traded': 0, 'mean': None, 'median': None,
                'red_rate': None, 'best': None}
    # V4.3 B3：cohort 内最高个股（均值/中位裂口 ≥3pct 时 digest 点名成因——
    # 少数大涨拉高均值）；name 缺失置空（不硬造）
    best = None
    for code in codes:
        t = today_map.get(code)
        if t is None or t['chg'] is None:
            continue
        if best is None or t['chg'] > best['chg']:
            best = {'code': code, 'name': t.get('name') or '', 'chg': t['chg']}
    return {'count': len(codes), 'traded': len(chgs),
            'mean': round(sum(chgs) / len(chgs), 2),
            'median': round(float(pd.Series(chgs).median()), 2),
            'red_rate': round(red / len(chgs), 4),
            'best': best}


# ------------------------------------------------------------------ 大面与分档

def _loss_effect(cs_today: pd.DataFrame, prev_map: Dict[str, int],
                 today_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """大面统计：全市场跌超5%/7%、最惨跌幅、跌停数 + 昨日涨停股大面。"""
    chgs = [c for c in (v['chg'] for v in today_map.values()) if c is not None]
    down5 = sum(1 for c in chgs if c <= LOSS_DOWN5)
    down7 = sum(1 for c in chgs if c <= LOSS_DOWN7)
    limit_down = sum(1 for v in today_map.values() if v['down'])
    worst = min(chgs) if chgs else None
    prev_chgs = [today_map[c]['chg'] for c in prev_map
                 if c in today_map and today_map[c]['chg'] is not None]
    prev_down5 = sum(1 for c in prev_chgs if c <= LOSS_DOWN5)
    return {'traded': len(chgs), 'down5': down5, 'down7': down7,
            'limit_down': limit_down,
            'worst': round(worst, 2) if worst is not None else None,
            'prev_up_count': len(prev_chgs), 'prev_up_down5': prev_down5,
            'prev_up_down5_rate': round(prev_down5 / len(prev_chgs), 4)
            if prev_chgs else None}


def _change_dist(cs_today: pd.DataFrame) -> Optional[List[Dict[str, Any]]]:
    """全市场涨跌幅累计分档家数（剔除停牌无成交行；昨日截面缺失时仍可给出）。"""
    if cs_today.empty:
        return None
    change = pd.to_numeric(cs_today['change_rate'], errors='coerce')
    traded = cs_today['_traded'].fillna(False).astype(bool) \
        if '_traded' in cs_today.columns else change.notna()
    vals = change[traded].dropna()
    if vals.empty:
        return None
    out = []
    for t in _DIST_UP:
        out.append({'bucket': f'涨≥{t:.0f}%', 'count': int((vals >= t).sum())})
    for t in _DIST_DOWN:
        out.append({'bucket': f'跌≤{t:.0f}%', 'count': int((vals <= t).sum())})
    return out
