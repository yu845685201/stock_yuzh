"""维度6 子节：缠论背驰候选（左侧，观察池；2026-10-24 引入）。

数据来自聚合器 ``agg['chan']``（520d 面板独立复算，与链 chan_divergence_buy
模块同一实现两入口）。呈现锚定日收盘确认的一买/二买：观察池非推荐；
与 R 系列右侧候选互斥不混排、不参与 R 池计数交叉校验。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

DEFAULT_RULES = {
    'max_rows': 10,   # 表格 ≤10 行惯例；全量在链 CSV 缠论列
    'enabled': True,
}

#: 固定历史统计注记（验证报告 data/chan_validation/report.md，2015-01~2026-09 全A费后；
#: 2026-10-25 起按 P0-7 缺日门控保留样本统计，同口径随机基线重放
#: missing_sensitivity_gated_baseline_20261025_144205.md）
HIST_NOTE = (
    '历史统计（2015-01~2026-09 全A验证，60 日费后，按输入完整性门控保留样本统计，'
    '2026-10-25 修订）：一买均值 +6.79%、胜率 53.8%；二买均值 +5.95%、胜率 53.6%'
    '（对同口径随机基线超额 +4.29pp / +3.19pp）。'
    '历史统计非预测；级联下跌中一买约半数失败，本表为观察池非推荐，'
    '候选池+人工确认使用（验证报告 data/chan_validation/report.md §3.6）。'
)


def build(agg: Dict[str, Any], review_cfg: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """缠论背驰候选 blocks；数据缺失/未启用时降级（缺数据标注规范）。"""
    rules = dict(DEFAULT_RULES)
    rules.update((review_cfg or {}).get('chan_candidates') or {})
    if not rules.get('enabled', True):
        return []
    blocks: List[Dict[str, Any]] = [
        {'type': 'heading', 'level': 3, 'text': '缠论背驰候选（左侧，观察池）'},
    ]
    chan = agg.get('chan')
    available = bool(chan) and getattr(chan, 'available', True)
    data = (getattr(chan, 'data', None) or {}) if chan is not None else {}
    if not available:
        blocks.append({'type': 'note',
                       'text': f"⏳ 待补：缠论面板数据缺失"
                               f"（{getattr(chan, 'reason', '未知原因') if chan is not None else '未聚合'}）"})
        return blocks
    items = data.get('items') or []
    if not items:
        blocks.append({'type': 'para',
                       'text': '今日无一买/二买信号确认（正常状态：全市场年均约 480+250 个，'
                               '单日 0~16 个常见；冰点/退潮期密度上升）'})
        blocks.append({'type': 'note', 'text': HIST_NOTE})
        return blocks

    max_rows = int(rules.get('max_rows', 10))
    rows = []
    for it in items[:max_rows]:
        # P0-7 数据标记：引擎缺日标记 + 链存疑清单命中（前复权断裂等）
        marks = []
        if it.get('data_flag'):
            marks.append(str(it['data_flag']))
        if it.get('suspect_reason'):
            marks.append('链存疑')
        # V4.3 C1：收盘距止损%（锚定日收盘口径，正值=收盘在止损上方；
        # 操作口径=次日开盘，生成时未知）
        cl = it.get('close')
        dist = (f"{(cl - it['stop']) / cl * 100:+.1f}"
                if cl is not None and it.get('stop') is not None and cl else '—')
        rows.append([
            it.get('code', '—'), it.get('name') or '—', it.get('kind', '—'),
            f"{it.get('trigger_low', '—')}（止损参考 {it.get('stop', '—')}）"
            if it.get('stop') is not None else str(it.get('trigger_low', '—')),
            dist,
            f"[{it.get('zd', '—')},{it.get('zg', '—')}]",
            f"{it['divergence']:.2f}" if it.get('divergence') is not None else '—',
            '；'.join(marks) if marks else '—',
        ])
    # V4.4 T4：口径短注前置（与 digest 同构；完整口径在表后 note）
    blocks.append({'type': 'note',
                   'text': '收盘距止损% = 锚定日收盘口径；操作口径 = 次日开盘'
                           '（生成时未知）'})
    blocks.append({'type': 'table',
                   'headers': ['代码', '名称', '信号', '触发低点(止损)',
                               '收盘距止损%', '参照中枢[ZD,ZG]', '背驰强度',
                               '数据标记'],
                   'rows': rows})
    more = len(items) - len(rows)
    tail = f'；全量 {len(items)} 个见 daily_review CSV「缠论背驰买点」列' if more > 0 else ''
    refire_n = sum(1 for it in items if it.get('refire'))
    if refire_n:
        tail += f'；{refire_n} 个为同中枢重发（此前已有信号，事件语义正常）'
    blocks.append({'type': 'note',
                   'text': '信号为锚定日**收盘确认**，操作口径 = 次日开盘（与验证回测撮合一致）；'
                           '背驰强度 = |Σhist₂|/|Σhist₁|，越小背驰越强；'
                           '收盘距止损% = 锚定日收盘相对触发低点（正值=收盘在止损上方，'
                           '收盘口径——操作口径为次日开盘，生成时未知）' + tail})
    blocks.append({'type': 'note', 'text': HIST_NOTE})
    blocks.append({'type': 'note', 'text': _data_quality_note(items, data)})
    parity = data.get('parity')
    if parity is not None and not parity.get('match', True):
        blocks.append({'type': 'note',
                       'text': f"⚠ 缠论对拍不一致：报告复现 {parity.get('report_n')} vs 链 CSV "
                               f"{parity.get('chain_n')}，差异 {parity.get('diff')}"})
    return blocks


def _data_quality_note(items: List[Dict[str, Any]],
                       data: Dict[str, Any]) -> str:
    """P0-7 数据质量脚注：缺日/断裂标记统计 + 候选×存疑清单交叉结果。

    命名含「缠论候选×存疑」供 check_digest_numbers 结构校验（P2-5）锚定。
    """
    flagged = sum(1 for it in items if it.get('data_flag') or it.get('suspect_reason'))
    parts = [f'数据质量：本日 {len(items)} 只候选中 {flagged} 只带缺日/断裂标记'
             f'（缺日>2% 已剔除不判定）']
    overlap = data.get('suspect_overlap')
    if isinstance(overlap, dict) and overlap.get('available'):
        parts.append(f"缠论候选×存疑清单交叉：命中 {overlap.get('hit_count', 0)} 只"
                     f"（全链存疑股票 {overlap.get('suspect_total', 0)} 只）")
    elif isinstance(overlap, dict):
        parts.append(f"缠论候选×存疑清单交叉未执行（{overlap.get('note', '不可得')}）")
    return '；'.join(parts)
