"""维度2 市场情绪章节（§5.3）：连板梯队 / 空间板高度 / 炸板率 / 昨日涨停今日溢价 /
反馈矩阵（四期）/ 晋级率 / 炸板修复与连板溢价（四期）/ 大面与分档（四期）/
情绪周期定位（含走向，四期）。全部由 cross_section 重算（§5.2 独立重算设计）。"""

from __future__ import annotations

from typing import Any, Dict, List


def build(sentiment: Dict[str, Any], money_effect: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    blocks: List[Dict[str, Any]] = [
        {'type': 'heading', 'level': 2,
         'text': '二、市场情绪（口径说明见复盘框架.MD §2.2）'},
    ]
    if not sentiment.get('available', True):
        blocks.append({'type': 'note',
                       'text': f'⏳ 待补：情绪数据（{sentiment.get("reason", "截面为空")}）'})
        return blocks

    m = sentiment['data']

    # 连板梯队 + 空间板高度
    ladder = m.get('ladder') or {}
    blocks.append({'type': 'heading', 'level': 3,
                   'text': f'连板梯队（空间板高度 {m["max_streak"]} 板）'})
    if ladder:
        blocks.append({'type': 'table', 'headers': ['高度', '家数'],
                       'rows': [[f'{h} 板', n] for h, n in sorted(ladder.items(), reverse=True)]})
    else:
        blocks.append({'type': 'para', 'text': '当日无 2 板及以上连板股'})
    gap = m.get('ladder_gap')
    if gap and gap.get('gaps'):
        blocks.append({'type': 'note',
                       'text': f'梯队断层：{gap["max"]} 板与 {gap["min"]} 板之间缺 '
                               f'{"、".join(str(g) for g in gap["gaps"])} 板'
                               '——高标"悬空"，一旦断板没有下一梯队承接（情绪断层典型信号）'})
    elif gap:
        blocks.append({'type': 'note', 'text': f'梯队完整：{gap["min"]}~{gap["max"]} 板无缺失档位'})

    # 炸板率
    burst_rate = m.get('burst_rate')
    blocks.append({'type': 'heading', 'level': 3, 'text': '炸板率'})
    if burst_rate is not None:
        blocks.append({'type': 'para',
                       'text': f'炸板 {m["burst_count"]} 家 ÷（涨停 {m["limit_up_count"]} + 炸板 '
                               f'{m["burst_count"]}）= {burst_rate:.1%}'})
    else:
        blocks.append({'type': 'para', 'text': '涨停与炸板均为 0，无法计算'})

    # 封板时段与封板质量（四期 §3）
    blocks.extend(_seal_blocks(m.get('seal_dist')))

    # 昨日涨停今日溢价
    blocks.append({'type': 'heading', 'level': 3, 'text': '昨日涨停今日溢价'})
    if m.get('premium_mean') is not None:
        blocks.append({'type': 'para',
                       'text': f'昨日涨停股 {m["premium_count"]} 只：今日溢价均值 '
                               f'{m["premium_mean"]:+.2f}% / 中位数 {m["premium_median"]:+.2f}%；'
                               f'其中今日续涨停 {m["premium_continued"]} 只'})
    else:
        blocks.append({'type': 'para', 'text': '昨日无涨停股或溢价不可得'})

    # 昨日强势股反馈（反馈矩阵，四期 §2——"复盘先看死在哪"）
    blocks.extend(_feedback_blocks(money_effect))

    # 晋级率
    promotion = m.get('promotion') or {}
    blocks.append({'type': 'heading', 'level': 3, 'text': '晋级率'})
    if promotion:
        blocks.append({'type': 'table',
                       'headers': ['晋级档位', '今日家数', '昨日家数', '晋级率'],
                       'rows': [[f'{n - 1}→{n} 板', v['today'], v['prev'],
                                 f"{v['rate']:.0%}" if v.get('rate') is not None else '—（昨日基数为 0）']
                                for n, v in sorted(promotion.items(), reverse=True)]})
    else:
        blocks.append({'type': 'para', 'text': '昨日无 (N-1) 板基数，无法计算晋级率'})

    # 炸板修复与连板溢价（四期 §2）
    blocks.extend(_recovery_blocks(money_effect))

    # 亏钱效应（大面）与涨跌幅分档分布（四期 §2）
    blocks.extend(_loss_blocks(money_effect))

    # 历史分位（开发项 B：sentiment_metrics_daily 回填后自动出现，样本不足/表缺失时不显示）
    blocks.extend(_percentile_blocks(m))

    # 情绪周期定位
    cycle = m.get('cycle') or {}
    blocks.append({'type': 'heading', 'level': 3, 'text': '情绪周期定位'})
    blocks.append({'type': 'para',
                   'text': f"定位：{cycle.get('position', '待校准')}"
                           + (f"（判据：{'；'.join(cycle.get('basis') or [])}）" if cycle.get('basis') else '')})
    # 分位标定四态（P1-10 §4.4 并列列：规则四态保持基线不替换）
    q_pos = cycle.get('quantile_position')
    if q_pos:
        q_text = (f"分位标定定位：{q_pos}"
                  + (f"（判据：{'；'.join(cycle.get('quantile_basis') or [])}）"
                     if cycle.get('quantile_basis') else ''))
        if cycle.get('quantile_divergence'):
            q_text += ' ⚠与规则四态背离（校准观察项，非故障）'
        blocks.append({'type': 'para', 'text': q_text})
    if cycle.get('trial'):
        blocks.append({'type': 'note',
                       'text': '⚠ 判据试用版：冰点/回暖/高潮/退潮阈值为占位值，待复盘框架.MD 用户校准后定稿'})

    # 情绪周期走向（四期 §3：10 日窗口相对读数）
    blocks.extend(_cycle_trend_blocks(m.get('cycle_trend')))
    return blocks


def _seal_blocks(seal: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """封板时段分布（seal_dist；数据缺失整块不出现，整表无行标待补）。"""
    blocks: List[Dict[str, Any]] = [{'type': 'heading', 'level': 3, 'text': '封板质量（首封时段）'}]
    if not seal:
        blocks.append({'type': 'para', 'text': '⏳ 待补：封板分笔统计无数据（limit_seal_stats_daily 无当日行）'})
        return blocks
    blocks.append({'type': 'table',
                   'headers': ['首封时段', *[b['bucket'] for b in seal['buckets']]],
                   'rows': [[f"涨停 {seal['limit_up_count']} 家（覆盖 {seal['covered']}）",
                             *[b['count'] for b in seal['buckets']]]]})
    qual = []
    if seal.get('never_broken_rate') is not None:
        qual.append(f'一次未炸 {seal["never_broken_rate"]:.0%}')
    if seal.get('late_seal_rate') is not None:
        qual.append(f'14:00 后首封 {seal["late_seal_rate"]:.0%}')
    if qual:
        blocks.append({'type': 'para', 'text': '；'.join(qual)})
    blocks.append({'type': 'note',
                   'text': '口径：首封时间五时段分布（tdx 分笔口径）；"涨停 40 家"说不出成色，'
                           '首封越早、一次未炸占比越高封板质量越好'})
    return blocks


def cycle_low_text(trend: Dict[str, Any] = None) -> str:
    """周期走向「窗口最低点」短语单一出处（T2 去歧义，digest §二同源引用）：
    days_since_low = 锚定日距 10 日窗口内 score 最低日的交易日数——
    N=0 即今日为最低点；None 缺数据渲染 '—'。"""
    dsl = (trend or {}).get('days_since_low')
    if dsl is None:
        return '—'
    if dsl == 0:
        return '今日即窗口最低点'
    return f'距窗口 score 最低点已 {dsl} 个交易日'


def _cycle_trend_blocks(trend: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """情绪周期走向（cycle_trend；样本不足时如实标注）。"""
    if not trend:
        return []
    text = (f"10 日窗口 score {trend['score']:.2f}（窗口内分位 {trend['window_pct']}%），"
            f"走向{trend['direction']}（窗口首尾差 {trend['delta']:+.2f}），"
            f"{cycle_low_text(trend)}")
    if not trend.get('complete'):
        text += f"；窗口仅 {trend['window']} 天，读数不完整"
    return [
        {'type': 'para', 'text': f'周期走向：{text}'},
        {'type': 'note',
         'text': '口径：涨停家数/最高连板 0-1 归一 + 炸板率取反后三项均值为 score——'
                 '这是十日窗口内的相对读数，没有绝对含义；"距窗口 score 最低点已 N 日"'
                 '与"正在往上走"是两件事，别混为一谈'},
    ]


#: 分位注记的四个指标（开发项 B）：展示口径与百分位含义
_PCT_METRICS = (
    ('涨停家数', 'limit_up_count', lambda v: f'{int(v)}'),
    ('炸板率', 'burst_rate', lambda v: f'{v:.1%}'),
    ('空间板高度', 'max_streak', lambda v: f'{int(v)} 板'),
    ('溢价均值', 'premium_mean', lambda v: f'{v:+.2f}%'),
)


def _feedback_blocks(money_effect: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """昨日强势股反馈矩阵（money_effect.feedback；缺数据整块不出现）。"""
    fb = ((money_effect or {}).get('data') or {}).get('feedback')
    if not fb or not fb.get('rows'):
        return []
    cats = ('跌停', '跌超5%', '小跌', '收红', '晋级涨停')
    rows = [[r['bucket'], r['count'],
             *(r['cats'].get(c, 0) for c in cats),
             f"{r['mean']:+.2f}%" if r.get('mean') is not None else '—',
             f"{r['median']:+.2f}%" if r.get('median') is not None else '—',
             f"{r['limit_rate']:.0%}" if r.get('limit_rate') is not None else '—']
            for r in fb['rows']]
    blocks = [
        {'type': 'heading', 'level': 3, 'text': '昨日强势股反馈（反馈矩阵）'},
        {'type': 'table',
         'headers': ['昨日板位', '样本', *cats, '今日均值', '中位数', '晋级率'],
         'rows': rows},
    ]
    if fb.get('suspended'):
        blocks.append({'type': 'note',
                       'text': f'另有 {fb["suspended"]} 只昨日涨停股今日无行情行（停牌/缺行），'
                               '未计入矩阵；矩阵分类互斥，跌停不计入跌超5%列'})
    blocks.append({'type': 'note',
                   'text': '口径：昨日涨停股按昨日板位分组的今日表现（固定样本反馈，'
                           '不代表全市场收益）；均值与中位数背离时以中位数描述多数体感'})
    return blocks


def _recovery_blocks(money_effect: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """昨日炸板股今日修复 + 连板溢价（money_effect.burst_recovery/consec_premium）。"""
    data = ((money_effect or {}).get('data') or {})
    br, cp = data.get('burst_recovery'), data.get('consec_premium')
    blocks: List[Dict[str, Any]] = []
    if br and br.get('count'):
        lines = [f'昨日炸板 {br["count"]} 只']
        if br.get('traded'):
            lines.append(f'今日均值 {br["mean"]:+.2f}% / 中位数 {br["median"]:+.2f}%')
            if br.get('red_rate') is not None:
                lines.append(f'翻红 {br["red_rate"]:.0%}')
            lines.append(f'回封涨停 {br.get("relimit") or 0} 只')
        else:
            lines.append('今日无行情行')
        blocks.append({'type': 'heading', 'level': 3, 'text': '昨日炸板股今日修复'})
        blocks.append({'type': 'para', 'text': '：'.join([lines[0], '，'.join(lines[1:])])})
    if cp and cp.get('count'):
        lines = [f'昨日 2 板及以上 {cp["count"]} 只']
        if cp.get('traded'):
            lines.append(f'今日均值 {cp["mean"]:+.2f}% / 中位数 {cp["median"]:+.2f}%'
                         + (f' / 翻红 {cp["red_rate"]:.0%}' if cp.get('red_rate') is not None else ''))
        else:
            lines.append('今日无行情行')
        blocks.append({'type': 'heading', 'level': 3, 'text': '连板溢价（昨日 2 板及以上）'})
        blocks.append({'type': 'para', 'text': '：'.join([lines[0], '，'.join(lines[1:])])})
        blocks.append({'type': 'note',
                       'text': '口径：只描述样本表现，不单独判断承接；与上方反馈矩阵 2 板行同源'})
    return blocks


def _loss_blocks(money_effect: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """亏钱效应（大面）+ 涨跌幅分档分布（money_effect.loss/change_dist）。"""
    data = ((money_effect or {}).get('data') or {})
    loss, dist = data.get('loss'), data.get('change_dist')
    blocks: List[Dict[str, Any]] = []
    if loss and loss.get('traded'):
        text = (f'今日有成交 {loss["traded"]} 只：跌超5% {loss["down5"]} 家 / '
                f'跌超7% {loss["down7"]} 家 / 跌停 {loss["limit_down"]} 家'
                + (f'，最惨 {loss["worst"]:+.2f}%' if loss.get('worst') is not None else ''))
        if loss.get('prev_up_count'):
            rate = (f'（占 {loss["prev_up_down5_rate"]:.0%}）'
                    if loss.get('prev_up_down5_rate') is not None else '')
            text += f'；昨日涨停股今日跌超5% {loss["prev_up_down5"]} 家{rate}'
        blocks.append({'type': 'heading', 'level': 3, 'text': '亏钱效应（大面）'})
        blocks.append({'type': 'para', 'text': text})
        blocks.append({'type': 'note',
                       'text': '口径：判退潮先看大面多不多，炸板率只说明板没封住，'
                               '说明不了封不住之后有多疼；跌超5% 家数含跌停'})
    if dist:
        buckets = [d['bucket'] for d in dist]
        counts = [d['count'] for d in dist]
        blocks.append({'type': 'heading', 'level': 3, 'text': '涨跌幅分档分布（全市场，累计口径）'})
        blocks.append({'type': 'table', 'headers': buckets, 'rows': [counts]})
        blocks.append({'type': 'note',
                       'text': '口径：累计分档家数（涨≥N% / 跌≤N%），剔除停牌无成交行；'
                               '相邻档差值即该档区间家数'})
    return blocks


def _percentile_blocks(m: Dict[str, Any]) -> List[Dict[str, Any]]:
    """"过去 250 交易日分位"注记（数据来自 sentiment_metrics_daily，缺则整块不出现）"""
    pct = m.get('percentiles') or {}
    rows = []
    for label, key, fmt in _PCT_METRICS:
        e = pct.get(key) or {}
        if e.get('pct') is None or e.get('value') is None:
            continue
        rows.append([label, fmt(e['value']), f"{e['pct']:.0f}%"])
    if not rows:
        return []
    return [
        {'type': 'heading', 'level': 3, 'text': '历史分位（过去 250 交易日）'},
        {'type': 'table', 'headers': ['指标', '当日值', '一年分位'], 'rows': rows},
        {'type': 'note',
         'text': '分位口径：当日值在截至当日（不含）的最近 250 个交易日历史分布中的位置，'
                 '50% 为中位水平；样本不足 30 天不展示'},
    ]
