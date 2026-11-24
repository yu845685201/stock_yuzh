"""「〇、昨日核验（预期 vs 实际）」章节（四期方案 §1.4）。

渲染 agg['verification']：情景核验（触发档+低开/高开兑现）→ 验点核验表（V1/V1.5
记录、V2 三态、V3 记录）→ 候选池战绩（超额=池中位−全市场中位）→ R2 同类基准
（§4.1 D1：候选 × 全市场 × 前一日首板三组中位与两条超额）→ 三项次日回填
（§4.3 E2：涨停延续 / 二板晋级 / 量价延续）→ 滚动战绩（近 30 核验日）。
昨日无预测记录 → "⏳ 无昨日预期记录"（核验体系 2026-10-07 起逐日积累）。
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
    blocks.extend(_cohort_benchmark_blocks(data.get('cohorts') or {}))
    blocks.extend(_backfill_blocks(data.get('backfills') or {}))
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
        # 新观测业务日（防前视语义：仅业务日严格早于 as_of 的收盘可用；
        # 与基准同业务日 → 无新观测，— 表示无开盘前可知新报价）
        new_date = m.get('new_date') or '—'
        note = '' if m.get('has_new') else (f"（{m['reason']}）" if m.get('reason') else '')
        rows.append([m['name'], base, last, f'{new_date}{note}', accum])
    blocks = [
        {'type': 'heading', 'level': 3, 'text': '情景核验'},
        {'type': 'table',
         'headers': [f"锚（较基准 ±{sc.get('gap_pct', 1.5):.1f}%）", '基准收盘',
                     '最新收盘', '新观测业务日', '累计变动'],
         'rows': rows},
        {'type': 'note',
         'text': '口径（2026-11-24 修订）：新观测仅取 A 股当日开盘前已完成的海外'
                 '交易时段收盘（业务日严格早于锚定日；同日港美收盘一律剔除）；'
                 '同一旧报价重复出现记「无新观测」而非默认 P2。'},
    ]
    text = f'判档：{trig}（{sc.get("completeness") or "—"}）'
    if sc.get('scenario_reason'):
        text += f"——{sc['scenario_reason']}"
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


def _cohort_benchmark_blocks(cohorts: Dict[str, Any]) -> List[Dict[str, Any]]:
    """R2 同类基准（§4.1 D1）：候选 × 全市场 × 前一日全部可确认首板。

    三组同一段 T 收盘→N 收盘收益取中位数；超额 = 候选中位 − 基准中位
    （百分点）。入组数、有效数、统计天数并列；候选有效数为零 → 两条超额
    留空（不转零）；某基准无有效样本 → 仅对应超额留空。
    """
    r2 = cohorts.get('R2')
    if not r2:
        return []
    cand = r2.get('candidate') or {}
    market = r2.get('market') or {}
    peer = r2.get('peer')
    blocks: List[Dict[str, Any]] = [
        {'type': 'heading', 'level': 3,
         'text': f"R2 同类基准（T={r2.get('anchor_date') or '—'} 收盘 → 今日收盘同区间）"},
    ]
    if not cand.get('n_total'):
        blocks.append({'type': 'para',
                       'text': '昨日 R2 正式发布 0 只（池未开放）——同类基准不适用，'
                               '两条超额留空（空池语义，不转零）。'})
        return blocks
    rows = [[_POOL_NAMES['R2'], cand.get('n_total'), cand.get('n_valid'),
             f"{cand['median']:+.2f}%" if cand.get('median') is not None else '—',
             '—']]
    rows.append(['全市场（今日已交易）', '—', market.get('n'),
                 f"{market['median']:+.2f}%" if market.get('median') is not None else '—',
                 f"{r2.get('excess_vs_market'):+.2f}pct"
                 if r2.get('excess_vs_market') is not None else '—'])
    if peer is not None:
        rows.append(['昨日全部可确认首板', peer.get('n_total'), peer.get('n_valid'),
                     f"{peer['median']:+.2f}%" if peer.get('median') is not None else '—',
                     f"{r2.get('excess_vs_peer'):+.2f}pct"
                     if r2.get('excess_vs_peer') is not None else '—'])
    blocks.append({'type': 'table',
                   'headers': ['组', '入组', '有效', '中位数', '超额（百分点）'],
                   'rows': rows})
    inv = cand.get('invalid') or {}
    inv_bits = '、'.join(f'{k} {v}' for k, v in inv.items() if v) or '无'
    note = (f"口径：三组同一段 T 收盘→N 收盘收益（qfq 收盘对收盘）分别取中位数，"
            f"偶数样本取中间两项均值——收盘到收盘衡量信号后的市场表现，"
            f"可交易收益须另列入场时间、成交条件与费用（本表未计）；"
            f"候选无效明细：{inv_bits}；"
            f"全市场成员哈希 {market.get('member_hash') or '—'}（版本引用）")
    if peer is not None and r2.get('peer_unknown_n'):
        note += (f"；昨日首板 cohort 身份未知 {r2['peer_unknown_n']} 只"
                 '（停牌缺行不入可确认名单）')
    blocks.append({'type': 'note', 'text': note})
    return blocks


def _backfill_blocks(backfills: Dict[str, Any]) -> List[Dict[str, Any]]:
    """三项次日回填核验（§4.3 E2）：昨日首页冻结的三项跟踪问题今日回填。"""
    if not backfills:
        return []
    blocks: List[Dict[str, Any]] = [
        {'type': 'heading', 'level': 3, 'text': '三项次日回填（昨日首页冻结基准）'},
    ]
    rows = []
    lu = backfills.get('limit_up')
    if lu:
        inv = lu.get('invalid') or {}
        rows.append([
            '涨停后的强势是否普遍延续',
            f"{lu.get('anchor_date', '—')} 涨停 {lu.get('n_total', 0)} 只",
            f"中位 {lu['median']:+.2f}%、翻红 {lu.get('red_n', 0)} / "
            f"有效 {lu.get('n_valid', 0)}"
            + (f"（{ '、'.join(f'{k} {v}' for k, v in inv.items() if v)}）"
               if any(inv.values()) else ''),
        ])
    tb = backfills.get('two_board')
    if tb:
        rate = tb.get('promotion_rate')
        rows.append([
            '连板队伍有没有新的承接',
            f"{tb.get('anchor_date', '—')} 可确认二板 {tb.get('n_total', 0)} 只",
            f"二进三 {tb.get('promoted', 0)} / 可判定 {tb.get('judgeable', 0)}"
            + (f"（晋级率 {rate:.0%}）" if rate is not None else '')
            + (f"；不可判定 {tb.get('unjudgeable', 0)} 只" if tb.get('unjudgeable') else '')
            + (f"；晋级组中位 {tb['median']:+.2f}%"
               if tb.get('median') is not None else ''),
        ])
    vp = backfills.get('volume_price')
    if vp:
        anchor = vp.get('anchor') or {}
        chg_txt = f"{vp['chg_pct']:+.2f}%" if vp.get('chg_pct') is not None else '—'
        base_txt = ''
        if anchor.get('chg_pct') is not None or anchor.get('ratio_5d') is not None:
            base_txt = (f"（昨日基准 {anchor.get('chg_pct', 0):+.2f}%、"
                        f"{anchor.get('ratio_5d', 0):.2f} 倍）")
        today_txt = (f"{vp.get('ratio_5d'):.2f} 倍" if vp.get('ratio_5d') is not None
                     else '待核')
        if vp.get('status') != 'ok':
            today_txt += f"——待核：{vp.get('reason') or '窗口缺日'}"
        rows.append([
            '普涨是否仍有成交配合',
            f"中证全指（{anchor.get('anchor_date') or '—'} 锚）",
            f"今日 {chg_txt}，成交额 / 前五日均额 {today_txt}{base_txt}"
            f"；交易日 {vp.get('anchor_date', '—')}",
        ])
    if rows:
        blocks.append({'type': 'table',
                       'headers': ['跟踪的问题', '冻结的基准', '今日回填'],
                       'rows': rows})
        blocks.append({'type': 'note',
                       'text': '口径：涨停延续组固定分组不缩小（缺行保留在分母说明中，'
                               '翻红严格大于零）；二板晋级按日历相邻 T、N 同一代码'
                               '确认连续封板，分母为零时晋级率为空；量能倍数按交易日历'
                               '应到日期核对六个端点，窗口缺日或均额为零记待核，'
                               '盘中占位行不能充当收盘。'})
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
    # §4.1（2026-11-24）：R2 同类基准滚动——两条基准各自的有效统计天数
    bench = rolling.get('r2_benchmarks')
    if bench:
        for key, label in (('market', '全市场'), ('peer', '昨日首板')):
            st = bench.get(key)
            if not st:
                continue
            items.append(
                f'R2 对{label}基准：{st["days"]} 个有效交易日（平均入组 '
                f'{st.get("avg_valid_members", "—")} 只），平均超额 '
                f'{st["avg_excess"]:+.2f}pct、跑赢 {st["win_rate"]:.0%} 的日子')
        if bench.get('empty_pool_days'):
            items.append(f'R2 空池日 {bench["empty_pool_days"]} 天'
                         '（池未开放，不计两条基准的分母）')
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
