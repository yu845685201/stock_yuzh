# -*- coding: utf-8 -*-
"""样本 cohort × 次日截面收益评价（每日复盘报告优化技术方案 §4.1，2026-11-24）。

``evaluate_cohort``：冻结的 ts_codes 对 N 日截面**显式左连接**，逐股记录
涨跌幅或互斥缺失原因（缺行 / 无成交 / 涨跌幅无效）——禁止把缺行静默剔出
分母；``median_precise``：偶数样本取中间两项均值（内部保留精度）；
``market_sample``：N 日全市场已交易有效样本（市场基准，成员哈希作版本
引用）；``excess_stats``：三组（候选 / 全市场 / 同类首板）同一段
T 收盘→N 收盘收益的中位数与两条超额（百分点）。

空值规范（不转零）：候选有效数为零 → 两条超额均 None；某一基准无有效
样本 → 仅对应超额 None。收益取 N 日截面 ``change_rate``（qfq 前复权，
收盘对收盘可比——除权日由复权基准滚动，两日未调整收盘不直接相除的
红线由数据侧口径保证）。展示层舍入两位小数；中位数与超额在存储时各
舍入一次（读者可用表中中位数复算超额，与发布数字自洽）。
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from .cohorts import member_hash

#: 缺失原因桶（互斥，判定顺序即优先级）
MISSING_REASONS = ('缺行', '无成交', '涨跌幅无效')


def _today_map(cs_n) -> Dict[str, Dict[str, Any]]:
    """N 日截面 → {ts_code: {'chg': float|None, 'traded': bool}}。"""
    if cs_n is None or cs_n.empty:
        return {}
    traded = cs_n['_traded'].fillna(False).astype(bool) \
        if '_traded' in cs_n.columns else cs_n['change_rate'].notna()
    change = cs_n['change_rate']
    out: Dict[str, Dict[str, Any]] = {}
    for ts, chg, tr in zip(cs_n['ts_code'], change, traded):
        out[str(ts)] = {'chg': (None if chg != chg or chg is None else float(chg)),
                        'traded': bool(tr)}
    return out


def evaluate_cohort(ts_codes: Iterable[str], cs_n) -> Dict[str, Any]:
    """cohort 成员 × N 日截面显式左连接（§4.1：剥离停牌/缺行/无效并保留原因数）。

    Returns:
        {'n_total', 'n_valid', 'changes', 'invalid': {原因: 数},
         'member_status': {ts_code: 'ok'|原因}}——changes 供中位数/翻红复算。
    """
    codes = [str(c) for c in ts_codes]
    today = _today_map(cs_n)
    invalid = {r: 0 for r in MISSING_REASONS}
    member_status: Dict[str, str] = {}
    changes: List[float] = []
    for ts in codes:
        t = today.get(ts)
        if t is None:
            invalid['缺行'] += 1
            member_status[ts] = '缺行'
        elif not t['traded']:
            invalid['无成交'] += 1
            member_status[ts] = '无成交'
        elif t['chg'] is None:
            invalid['涨跌幅无效'] += 1
            member_status[ts] = '涨跌幅无效'
        else:
            member_status[ts] = 'ok'
            changes.append(t['chg'])
    return {'n_total': len(codes), 'n_valid': len(changes),
            'changes': changes, 'invalid': invalid,
            'member_status': member_status}


def median_precise(values: Iterable[float]) -> Optional[float]:
    """中位数（偶数样本取中间两项均值；空 → None；不提前舍入）。"""
    s = sorted(float(v) for v in values)
    if not s:
        return None
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def red_count(changes: Iterable[float]) -> int:
    """翻红数（严格大于零，§4.3 E2 验点 1 口径）。"""
    return sum(1 for c in changes if c > 0)


def market_sample(cs_n) -> Dict[str, Any]:
    """N 日全市场已交易有效样本（市场基准；成员哈希作版本引用，§4.1）。

    Returns:
        {'n', 'changes', 'member_hash'}——与 candidate/peer cohort 同一收益
        区间（N 日截面 change_rate）。
    """
    today = _today_map(cs_n)
    changes: List[float] = []
    codes: List[str] = []
    for ts, t in today.items():
        if t['traded'] and t['chg'] is not None:
            changes.append(t['chg'])
            codes.append(ts)
    return {'n': len(changes), 'changes': changes,
            'member_hash': member_hash(codes)}


def _median2(values: Iterable[float]) -> Optional[float]:
    """中位数存储口径（round 2——与既有 verifier._median 一致，读者可复算）。"""
    m = median_precise(values)
    return round(m, 2) if m is not None else None


def excess_stats(cand: Dict[str, Any], market: Dict[str, Any],
                 peer: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """三组同区间中位数 + 两条超额（百分点；D1）。

    Args:
        cand: evaluate_cohort 产物（R2 发布成员）。
        market: market_sample 产物（N 日全市场）。
        peer: evaluate_cohort 产物（T 全部可确认首板）；None = 昨日首板
            cohort 未冻结（旧预测 payload），该基准整组降级。

    空值规范：候选 n_valid=0 → 两条超额均 None（不转零）；某基准无有效
    样本 → 仅对应超额 None。
    """
    cand_median = _median2(cand.get('changes') or [])
    mkt_median = _median2(market.get('changes') or [])
    out: Dict[str, Any] = {
        'candidate': {'n_total': cand.get('n_total'), 'n_valid': cand.get('n_valid'),
                      'invalid': cand.get('invalid'), 'median': cand_median},
        'market': {'n': market.get('n'), 'median': mkt_median,
                   'member_hash': market.get('member_hash')},
        'peer': None, 'excess_vs_market': None, 'excess_vs_peer': None,
    }
    if peer is not None:
        peer_median = _median2(peer.get('changes') or [])
        out['peer'] = {'n_total': peer.get('n_total'), 'n_valid': peer.get('n_valid'),
                       'invalid': peer.get('invalid'), 'median': peer_median}
        if cand_median is not None and peer_median is not None:
            out['excess_vs_peer'] = round(cand_median - peer_median, 2)
    if cand_median is not None and mkt_median is not None:
        out['excess_vs_market'] = round(cand_median - mkt_median, 2)
    return out
