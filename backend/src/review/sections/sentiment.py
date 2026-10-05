"""维度2 市场情绪章节（§5.3）：连板梯队 / 空间板高度 / 炸板率 / 昨日涨停今日溢价 /
晋级率 / 情绪周期定位。全部由 cross_section 重算（§5.2 独立重算设计）。"""

from __future__ import annotations

from typing import Any, Dict, List


def build(sentiment: Dict[str, Any]) -> List[Dict[str, Any]]:
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

    # 炸板率
    burst_rate = m.get('burst_rate')
    blocks.append({'type': 'heading', 'level': 3, 'text': '炸板率'})
    if burst_rate is not None:
        blocks.append({'type': 'para',
                       'text': f'炸板 {m["burst_count"]} 家 ÷（涨停 {m["limit_up_count"]} + 炸板 '
                               f'{m["burst_count"]}）= {burst_rate:.1%}'})
    else:
        blocks.append({'type': 'para', 'text': '涨停与炸板均为 0，无法计算'})

    # 昨日涨停今日溢价
    blocks.append({'type': 'heading', 'level': 3, 'text': '昨日涨停今日溢价'})
    if m.get('premium_mean') is not None:
        blocks.append({'type': 'para',
                       'text': f'昨日涨停股 {m["premium_count"]} 只：今日溢价均值 '
                               f'{m["premium_mean"]:+.2f}% / 中位数 {m["premium_median"]:+.2f}%；'
                               f'其中今日续涨停 {m["premium_continued"]} 只'})
    else:
        blocks.append({'type': 'para', 'text': '昨日无涨停股或溢价不可得'})

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

    # 历史分位（开发项 B：sentiment_metrics_daily 回填后自动出现，样本不足/表缺失时不显示）
    blocks.extend(_percentile_blocks(m))

    # 情绪周期定位
    cycle = m.get('cycle') or {}
    blocks.append({'type': 'heading', 'level': 3, 'text': '情绪周期定位'})
    blocks.append({'type': 'para',
                   'text': f"定位：{cycle.get('position', '待校准')}"
                           + (f"（判据：{'；'.join(cycle.get('basis') or [])}）" if cycle.get('basis') else '')})
    if cycle.get('trial'):
        blocks.append({'type': 'note',
                       'text': '⚠ 判据试用版：冰点/回暖/高潮/退潮阈值为占位值，待复盘框架.MD 用户校准后定稿'})
    return blocks


#: 分位注记的四个指标（开发项 B）：展示口径与百分位含义
_PCT_METRICS = (
    ('涨停家数', 'limit_up_count', lambda v: f'{int(v)}'),
    ('炸板率', 'burst_rate', lambda v: f'{v:.1%}'),
    ('空间板高度', 'max_streak', lambda v: f'{int(v)} 板'),
    ('溢价均值', 'premium_mean', lambda v: f'{v:+.2f}%'),
)


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
