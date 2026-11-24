# -*- coding: utf-8 -*-
"""行业逐日收益与五日复利（每日复盘报告优化技术方案 §3.2，2026-11-24）。

旧口径问题：``_industry_ranking`` 对五日窗口内全部有效个股日涨跌幅直接求
平均——729 的单位是「股日」而非「股票」；不同交易日缺行不齐时，记录较多的
一天权重更高（2026-11-23 传媒 −2.47% 案例）。

新口径（B1/B2）：

1. 逐日取当时可确认的行业成员（``membership_by_session``），剔除停牌（无当日
   行/无成交）与收益无效记录，计算当日有效成员的**等权平均收益** ``r[d]``；
2. 五个市场交易日按复利累计：
   ``五日收益(%) = 100 × [∏(1 + r[d]/100) − 1]``（中间过程保留精度，
   展示层再舍入）；任一整日该行业无有效成员 → 五日收益为 None（数据不足，
   不入榜）；部分缺行但当日仍有有效成员 → 按可得样本计算并标覆盖；
3. 每行输出五个日收益、每日有效家数、窗口去重股票数、有效股日数；
4. 榜单按**未舍入**收益降序，同值按行业名稳定排序，空收益不入榜；
   「新进前十」只允许比较同一周期、同一公式的今昨榜（消费方用 T 榜 vs
   T−1 榜），日榜对五日榜的差异须单列「跨周期排名差」。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

try:  # pandas 可用（聚合链路固定依赖）
    import pandas as pd
except ImportError:  # pragma: no cover - 环境兜底
    pd = None

#: 公式版本（消费方写入 meta / 渲染注记）
SECTOR_METRICS_VERSION = 'v1-20261124'


def industry_returns(panel: 'pd.DataFrame',
                     membership_by_session: Dict[str, Dict[str, str]],
                     sessions: List[str],
                     source_map: Optional[Dict[str, str]] = None
                     ) -> List[Dict[str, Any]]:
    """逐日等权行业收益 + 窗口复利（纯函数；B1）。

    Args:
        panel: 日K面板（列：trade_date、ts_code、change_rate、volume、amount）。
        membership_by_session: {交易日: {ts_code: 行业名}}——逐日当时可确认成员。
        sessions: 窗口交易日（升序）；空或无有效行返回 []。
        source_map: {ts_code: 来源}（'sw_2021'/'sina_hy'）——纯 sina_hy 兜底
            成分的行业为概念板（is_concept=True，渲染层打标签）。

    Returns:
        每行业一行（未舍入 return_5d_pct 降序，同值按行业名升序，None 不入榜）::

            {'industry', 'daily': [{'date', 'ret_pct', 'n'}],   # 每日等权收益(%)
             'return_5d_pct',                                    # 复利（None=整日缺）
             'unique_stocks', 'stock_days',                      # 去重股票数 / 股日数
             'daily_counts': [n],                                # 每日有效家数
             'days_valid', 'is_concept', 'formula_version'}
    """
    if not sessions or panel is None or panel.empty:
        return []
    valid = _valid_day_rows(panel, sessions)
    if valid.empty:
        return []
    # 成员关系展开为 (trade_date, ts_code, industry) 后与有效行连接
    memb_rows = [(d, ts, ind)
                 for d in sessions
                 for ts, ind in (membership_by_session.get(d) or {}).items()]
    if not memb_rows:
        return []
    memb = pd.DataFrame(memb_rows, columns=['trade_date', 'ts_code', 'industry'])
    merged = valid.merge(memb, on=['trade_date', 'ts_code'], how='inner')
    if merged.empty:
        return []

    out: List[Dict[str, Any]] = []
    for industry, g in merged.groupby('industry'):
        daily: List[Optional[Dict[str, Any]]] = []
        by_date = {str(d): sub for d, sub in g.groupby('trade_date')}
        for d in sessions:
            sub = by_date.get(str(d))
            if sub is None or sub.empty:
                daily.append(None)
                continue
            daily.append({'date': str(d),
                          'ret_pct': round(float(sub['_chg'].mean()), 4),
                          'n': int(len(sub))})
        daily_vals = [it for it in daily if it is not None]
        days_valid = len(daily_vals)
        # 复利按未舍入逐日均值（展示值 daily.ret_pct 仅供对账复算）
        ret_5d: Optional[float] = None
        if days_valid == len(sessions):
            prod = 1.0
            for d in sessions:
                prod *= (1.0 + float(by_date[str(d)]['_chg'].mean()) / 100.0)
            ret_5d = 100.0 * (prod - 1.0)
        is_concept = False
        if source_map:
            srcs = g['ts_code'].map(lambda t: source_map.get(str(t)))
            is_concept = not bool((srcs == 'sw_2021').any())
        out.append({
            'industry': str(industry),
            'daily': daily,
            'return_5d_pct': ret_5d,
            'unique_stocks': int(g['ts_code'].nunique()),
            'stock_days': int(len(g)),
            'daily_counts': [(it['n'] if it is not None else 0) for it in daily],
            'days_valid': days_valid,
            'is_concept': is_concept,
            'formula_version': SECTOR_METRICS_VERSION,
        })
    # 未舍入收益降序；None 排除于榜尾之外（不入榜语义由消费方截断处理）
    ranked = sorted((r for r in out if r['return_5d_pct'] is not None),
                    key=lambda r: (-r['return_5d_pct'], r['industry']))
    # 整日缺失/部分覆盖的行业保留在榜尾供「数据不足」展示（不入正式排名）
    missing = sorted((r for r in out if r['return_5d_pct'] is None),
                     key=lambda r: r['industry'])
    return ranked + missing


def _valid_day_rows(panel: 'pd.DataFrame',
                    sessions: List[str]) -> 'pd.DataFrame':
    """窗口内有效个股日行：有当日记录、有成交（volume/amount > 0）、收益有效。"""
    df = panel[panel['trade_date'].isin(set(sessions))]
    if df.empty:
        return df
    chg = pd.to_numeric(df['change_rate'], errors='coerce')
    vol = pd.to_numeric(df['volume'], errors='coerce').fillna(0.0)
    amt = pd.to_numeric(df['amount'], errors='coerce').fillna(0.0)
    ok = chg.notna() & ((vol > 0) | (amt > 0))
    return df.loc[ok].assign(_chg=chg[ok])


# ------------------------------------------------------------------ §4.2 题材交集

def theme_industry_links(cs_today: 'pd.DataFrame',
                        cs_prev: 'pd.DataFrame',
                        industry_map: Dict[str, str],
                        concept_map_today: Dict[str, List[str]],
                        concept_map_prev: Dict[str, List[str]],
                        clusters: List[Dict[str, Any]],
                        top_industries: List[str],
                        exclude: Optional[List[str]] = None
                        ) -> Optional[Dict[str, Any]]:
    """行业×题材成分交集与题材承接（§4.2，代码口径；聚合层计算）。

    展示范围沿用主线候选旧口径：行业前十（5 日榜有效榜）×概念前五（涨停
    聚集、黑名单剔除）；交集按 ``ts_code`` 集合运算——替代 sector_mainline
    旧的名称包含匹配（"军工"≠"国防军工"两套体系的误配从根上消除）。

    每对行业×题材：交集成员数、交集涨停数、各自有效样本数、重合领涨股
    （交集内当日涨停者，单独列名）。每个题材（D2 承接行）：

    - ``tradable_n`` 全体可交易成分数（当日有行且有成交）、``unknown_n``
      未知数（无行/停牌标记——涨停状态不可判定，不入涨停占比分母）；
    - ``limit_up_n`` 已确认涨停数，``limit_rate = limit_up_n / tradable_n``
      （可交易且涨停状态可判定的成分数）；``new_first_board_n`` 新增首板数
      （当日涨停 ∧ 日历相邻前一交易日有行且未涨停）；
    - 昨日成员按**昨日的成分关系与涨停名单冻结**（concept_map_prev ∩
      昨日涨停名单——跨日改标签不改昨日分母），今日用同一组股票评价
      晋级数、有效数、缺失数和涨跌幅中位数。

    跨题材汇总按代码并集去重（``union``），共享领涨股（≥2 个前列题材中
    当日涨停，如真视通同时带重组、军工标签）单独列名、只占一个代码。
    """
    blacklist = [str(x) for x in (exclude or [])]
    top_concepts = [c['concept'] for c in (clusters or [])[:5]
                    if not any(b in c['concept'] for b in blacklist)]
    top_industries = [str(i) for i in (top_industries or [])[:10]]
    if not top_concepts or cs_today is None or cs_today.empty:
        return None

    # ---- 截面索引 ----
    names: Dict[str, str] = {}
    today: Dict[str, Dict[str, Any]] = {}
    traded = cs_today['_traded'].fillna(False).astype(bool) \
        if '_traded' in cs_today.columns else cs_today['change_rate'].notna()
    lu = cs_today['_limit_up'].fillna(False).astype(bool)
    nm_col = cs_today['stock_name'] if 'stock_name' in cs_today.columns else None
    for i, (ts, chg, tr, up, streak) in enumerate(
            zip(cs_today['ts_code'], cs_today['change_rate'], traded, lu,
                cs_today['_streak'])):
        ts = str(ts)
        today[ts] = {
            'chg': (None if chg is None or chg != chg else float(chg)),
            'traded': bool(tr), 'up': bool(up),
            'streak': int(streak) if streak == streak else 0}
        if nm_col is not None and i < len(nm_col):
            names[ts] = str(nm_col.iloc[i]) if nm_col.iloc[i] == nm_col.iloc[i] \
                else ts
    prev_up: set = set()
    prev_has: set = set()
    if cs_prev is not None and not cs_prev.empty:
        pu = cs_prev['_limit_up'].fillna(False).astype(bool)
        for ts, up in zip(cs_prev['ts_code'], pu):
            prev_has.add(str(ts))
            if up:
                prev_up.add(str(ts))
    today_up_set = {ts for ts, t in today.items() if t['up']}

    # ---- 成员索引 ----
    industry_members: Dict[str, set] = {}
    for ts, ind in (industry_map or {}).items():
        if ind in top_industries:
            industry_members.setdefault(ind, set()).add(str(ts))
    concept_today_members: Dict[str, set] = {}
    for ts, concepts in (concept_map_today or {}).items():
        for c in concepts:
            if c in top_concepts:
                concept_today_members.setdefault(c, set()).add(str(ts))
    concept_prev_members: Dict[str, set] = {}
    for ts, concepts in (concept_map_prev or {}).items():
        for c in concepts:
            if c in top_concepts:
                concept_prev_members.setdefault(c, set()).add(str(ts))

    def _valid_n(members: set) -> int:
        return sum(1 for ts in members
                   if today.get(ts, {}).get('traded')
                   and today[ts]['chg'] is not None)

    # ---- 行业×题材交集（每对一行，仅非零交集；按领涨活跃度降序）----
    pairs: List[Dict[str, Any]] = []
    for ind in top_industries:
        im = industry_members.get(ind) or set()
        for con in top_concepts:
            inter = im & (concept_today_members.get(con) or set())
            if not inter:
                continue
            inter_up = inter & today_up_set
            pairs.append({
                'industry': ind, 'concept': con,
                'n_intersection': len(inter),
                'n_intersection_limit_up': len(inter_up),
                'industry_valid_n': _valid_n(im),
                'concept_valid_n': _valid_n(concept_today_members.get(con) or set()),
                'shared_leaders': sorted(
                    names.get(ts, ts) for ts in inter_up),
            })
    pairs.sort(key=lambda p: (-p['n_intersection_limit_up'],
                             -p['n_intersection'], p['industry'], p['concept']))

    # ---- 题材承接（每题材一行；昨日分母按昨日成分冻结）----
    themes: List[Dict[str, Any]] = []
    for con in top_concepts:
        members = concept_today_members.get(con) or set()
        tradable = {ts for ts in members if today.get(ts, {}).get('traded')}
        unknown_n = len(members) - len(tradable)
        up_members = members & today_up_set
        # 新增首板：当日涨停 ∧ 日历相邻前一交易日有行且未涨停
        new_fb = {ts for ts in up_members
                  if ts in prev_has and ts not in prev_up}
        # 昨日冻结：昨日成分关系 ∩ 昨日涨停名单（跨日改标签不改分母）
        prev_frozen = (concept_prev_members.get(con) or set()) & prev_up
        promoted = sum(1 for ts in prev_frozen
                       if today.get(ts, {}).get('up'))
        valid_n = missing_n = invalid_n = 0
        chgs: List[float] = []
        for ts in prev_frozen:
            t = today.get(ts)
            if t is None:
                missing_n += 1
            elif t['chg'] is None:
                invalid_n += 1     # 有行但涨跌幅无效（可比收益不可得）
            else:
                valid_n += 1
                chgs.append(t['chg'])
        median = None
        if chgs:
            s = sorted(chgs)
            mid = len(s) // 2
            median = round(s[mid] if len(s) % 2 else
                           (s[mid - 1] + s[mid]) / 2, 2)
        themes.append({
            'concept': con, 'members_n': len(members),
            'tradable_n': len(tradable), 'unknown_n': unknown_n,
            'limit_up_n': len(up_members),
            'limit_rate': round(len(up_members) / len(tradable), 4)
            if tradable else None,
            'new_first_board_n': len(new_fb),
            'prev_up_frozen_n': len(prev_frozen), 'promoted': promoted,
            'prev_up_valid_n': valid_n, 'prev_up_missing_n': missing_n,
            'prev_up_invalid_n': invalid_n,
            'prev_up_median': median,
        })

    # ---- 跨题材并集去重（共享领涨股只占一个代码）----
    per_theme_up = {con: (concept_today_members.get(con) or set()) & today_up_set
                    for con in top_concepts}
    union_up: set = set()
    for s in per_theme_up.values():
        union_up |= s
    shared: List[Dict[str, Any]] = []
    for ts in sorted(union_up):
        hit = [con for con, s in per_theme_up.items() if ts in s]
        if len(hit) >= 2:
            shared.append({'ts_code': ts, 'name': names.get(ts, ts),
                           'concepts': hit})
    union = {'n_sum': sum(len(s) for s in per_theme_up.values()),
             'n_union': len(union_up), 'shared': shared}

    # ---- 映射覆盖（成分映射不足时直接标注）----
    with_ind = sum(1 for ts in today_up_set if (industry_map or {}).get(ts))
    with_con = sum(1 for ts in today_up_set
                   if (concept_map_today or {}).get(ts))
    mapping = {'limit_up_total': len(today_up_set),
               'with_industry': with_ind, 'with_concept': with_con}

    return {'industries': top_industries, 'concepts': top_concepts,
            'pairs': pairs, 'themes': themes, 'union': union,
            'mapping_coverage': mapping}
