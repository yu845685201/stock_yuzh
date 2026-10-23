"""「〇、昨日核验（预期 vs 实际）」章节（四期方案 §1.4）。

渲染 agg['verification']：情景核验（触发档+低开/高开兑现）→ 验点核验表（V1/V1.5
记录、V2 三态、V3 记录）→ 候选池战绩（超额=池中位−全市场中位）→ 滚动战绩（近 30
核验日）。昨日无预测记录 → "⏳ 无昨日预期记录"（核验体系 2026-10-07 起逐日积累）。
本章节置顶：复盘先对账，再看今日事实。
"""

from __future__ import annotations

from typing import Any, Dict, List

_POOLS = ('R1', 'R2', 'R3', 'R4')
_POOL_NAMES = {'R1': 'R1 梯队接力', 'R2': 'R2 主线跟随',
               'R3': 'R3 趋势新高', 'R4': 'R4 机构席位'}


def build(verification: Dict[str, Any]) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = [
        {'type': 'heading', 'level': 2, 'text': '〇、昨日核验（预期 vs 实际）'},
    ]
    if not verification.get('available', True):
        blocks.append({'type': 'note',
                       'text': f"⏳ 待补：昨日核验（{verification.get('reason', '无昨日预测记录')}）"})
        return blocks

    data = verification.get('data') or {}
    blocks.extend(_scenario_blocks(data.get('scenario') or {}))
    blocks.extend(_verify_point_blocks(data.get('verify_points') or []))
    blocks.extend(_candidate_blocks(data.get('candidates') or {}))
    blocks.extend(_rolling_blocks(data.get('rolling')))
    blocks.append({'type': 'note',
                   'text': '三态规范：成立 / 不成立 / 数据不足——数据不足 ≠ 判错，不污染战绩；'
                           '竞价与盘中"过程判据"无盘前数据，按设计只核验结果端；'
                           'V1/V1.5/V3 为观察记录，情景兑现与 V2 晋级计入滚动战绩'})
    return blocks


def _scenario_blocks(sc: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not sc or sc.get('status') == '数据不足':
        reason = (sc or {}).get('reason') or '昨日预测无情景锚'
        return [{'type': 'heading', 'level': 3, 'text': '情景核验'},
                {'type': 'para', 'text': f'数据不足：{reason}'}]
    trig = sc.get('triggered') or '—'
    rows = []
    for m in sc.get('moves') or []:
        accum = (f"{m['accum']:+.2f}%" if m.get('accum') is not None else '—')
        last = (f"{m['last_close']:,.0f}" if m.get('last_close') is not None else '—')
        base = (f"{m['base_close']:,.0f}" if m.get('base_close') is not None else '—')
        rows.append([m['name'], base, last, accum])
    blocks = [
        {'type': 'heading', 'level': 3, 'text': '情景核验'},
        {'type': 'table',
         'headers': [f"锚（较基准 ±{sc.get('gap_pct', 1.5):.1f}%）", '基准收盘', '最新收盘', '累计变动'],
         'rows': rows},
    ]
    text = f'实际触发档：{trig}'
    if sc.get('expectation'):
        open_gap = (f"{sc['open_gap']:+.2f}%" if sc.get('open_gap') is not None else '—')
        text += (f"（预期{sc['expectation']}）；今日上证开盘 {open_gap} —— "
                 f"{sc['expectation']}预期{sc.get('fulfilled') or '数据不足'}")
    blocks.append({'type': 'para', 'text': text})
    return blocks


def _verify_point_blocks(points: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not points:
        return []
    rows = [[p.get('key'), p.get('base'), p.get('actual'), p.get('verdict')]
            for p in points]
    return [
        {'type': 'heading', 'level': 3, 'text': '验点核验'},
        {'type': 'table', 'headers': ['验点', '昨日基准', '今日实际', '判定'],
         'rows': rows},
    ]


def _candidate_blocks(cand: Dict[str, Any]) -> List[Dict[str, Any]]:
    pools = cand.get('pools') or {}
    if not any((pools.get(k) or {}).get('count') for k in _POOLS):
        return []
    rows = []
    for key in _POOLS:
        st = pools.get(key) or {}
        if not st.get('count'):
            continue
        rows.append([
            _POOL_NAMES[key], st['count'], st.get('traded', 0),
            f"{st['mean']:+.2f}%" if st.get('mean') is not None else '—',
            f"{st['median']:+.2f}%" if st.get('median') is not None else '—',
            f"{st['excess_median']:+.2f}pct" if st.get('excess_median') is not None else '—',
            st.get('limit_up_n', 0), st.get('burst_n', 0), st.get('down_n', 0),
        ])
    mkt_median = cand.get('market_median')
    mkt_rate = cand.get('market_limit_rate')
    base_text = ('基准发生率口径：全市场已交易中位涨跌幅 '
                 + (f'{mkt_median:+.2f}%' if mkt_median is not None else '—')
                 + (f'、涨停率 {mkt_rate:.1%}' if mkt_rate is not None else '')
                 + '；超额≈0 说明判的都是本来就会发生的事')
    blocks = [
        {'type': 'heading', 'level': 3, 'text': '候选池战绩（昨日观察池今日表现）'},
        {'type': 'table',
         'headers': ['池', '只数', '有行情', '均值', '中位数', '超额(vs全市场中位)',
                     '涨停', '炸板', '跌停'],
         'rows': rows},
        {'type': 'note', 'text': base_text},
    ]
    # 极值点名（有行情行的最好/最差各 1，证据计数降序次之）
    for key in _POOLS:
        st = pools.get(key) or {}
        recs = [r for r in (st.get('rows') or []) if r.get('chg') is not None]
        if len(recs) < 2:
            continue
        recs.sort(key=lambda r: -r['chg'])
        best, worst = recs[0], recs[-1]
        if best['chg'] > 0 or worst['chg'] < 0:
            blocks.append({'type': 'para',
                           'text': f'{key} 极值：{best["name"]} {best["chg"]:+.2f}%'
                                   f'（{"涨停" if best.get("limit_up") else best.get("label") or "—"}）'
                                   f' ~ {worst["name"]} {worst["chg"]:+.2f}%'
                                   f'（{"跌停" if worst.get("limit_down") else "断板" if worst.get("burst") else "—"}）'})
    return blocks


def _rolling_blocks(rolling: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    if not rolling:
        return []
    items: List[str] = []
    for key, st in (rolling.get('pools') or {}).items():
        if not st.get('days'):
            continue
        text = f'{_POOL_NAMES.get(key, key)}：{st["days"]} 个核验日'
        if st.get('avg_excess') is not None:
            text += f'，平均超额 {st["avg_excess"]:+.2f}pct'
        if st.get('win_rate') is not None:
            text += f'，池中位跑赢全市场中位 {st["win_rate"]:.0%} 的日子'
        if st.get('limit_rate') is not None and st.get('mkt_limit_rate') is not None:
            text += (f'，晋级率 {st["limit_rate"]:.0%} vs 全市场涨停率 '
                     f'{st["mkt_limit_rate"]:.1%}')
        items.append(text)
    for trig, st in (rolling.get('scenario') or {}).items():
        label = {'P1': 'P1 低开预期', 'P3': 'P3 高开预期'}.get(trig, trig)
        items.append(f'情景兑现：{label} 触发 {st["n"]} 次，兑现 {st["ok"]} 次')
    if rolling.get('v2'):
        v2 = rolling['v2']
        items.append(f'V2 高度股晋级判定：观察 {v2["n"]} 次，晋级 {v2["ok"]} 次')
    if not items:
        return []
    return [
        {'type': 'heading', 'level': 3,
         'text': f'滚动战绩（近 {rolling.get("days", "—")} 个核验日，含当日）'},
        {'type': 'bullets', 'items': items},
        {'type': 'note',
         'text': '战绩口径：候选池超额 = 池中位 − 全市场已交易中位；'
                 '长期 ≈0 的规则只描述了基准，无选股增益——据此校准判据定稿'},
    ]
