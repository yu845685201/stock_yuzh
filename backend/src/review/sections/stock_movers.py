"""维度4 个股异动章节（§5.3）：涨停/跌停/炸板明细、放量大涨 TopN、
60 日新高新低列表。明细表为主，行数上限由 config review.top_n 控制。"""

from __future__ import annotations

from typing import Any, Dict, List


def build(movers: Dict[str, Any], lhb: Dict[str, Any] = None,
          money_flow: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """维度4 章节块；lhb/money_flow 为开发项 D 的两/三个独立聚合结果。"""
    blocks: List[Dict[str, Any]] = [
        {'type': 'heading', 'level': 2,
         'text': '四、个股异动（口径说明见复盘框架.MD §2.4）'},
    ]
    if not movers.get('available', True):
        blocks.append({'type': 'note',
                       'text': f'⏳ 待补：个股异动（{movers.get("reason", "截面为空")}）'})
        return blocks

    data = movers['data']

    blocks.append({'type': 'heading', 'level': 3,
                   'text': f"涨停明细（{data['limit_up_count']} 家，按连板数降序）"})
    if data['limit_up']:
        blocks.append(_detail_table(data['limit_up']))
        if any(r.get('theme') for r in data['limit_up']):
            blocks.append({'type': 'note',
                           'text': '题材候选为推断口径（概念映射 × 当日热度排序：概念内涨停≥2 家'
                                   '或成分等权涨跌幅≥2%），非编辑部人工归因；多概念股存在噪声，'
                                   '宽基属性概念已按黑名单剔除'})
    else:
        blocks.append({'type': 'para', 'text': '今日无涨停'})

    blocks.append({'type': 'heading', 'level': 3,
                   'text': f"跌停明细（{data['limit_down_count']} 家）"})
    if data['limit_down']:
        blocks.append(_detail_table(data['limit_down']))
    else:
        blocks.append({'type': 'para', 'text': '今日无跌停'})

    blocks.append({'type': 'heading', 'level': 3,
                   'text': f"炸板明细（{data['burst_count']} 家，盘中触板未封住）"})
    if data['burst']:
        blocks.append(_detail_table(data['burst']))
    else:
        blocks.append({'type': 'para', 'text': '今日无炸板'})

    blocks.append({'type': 'heading', 'level': 3, 'text': '放量大涨 Top N'})
    if data['surge_up']:
        blocks.append(_detail_table(data['surge_up']))
    else:
        blocks.append({'type': 'para', 'text': '今日无「大涨 ∧ 放量倍数达标」个股'})

    blocks.append({'type': 'heading', 'level': 3, 'text': '60 日新高 / 新低'})
    blocks.append({'type': 'table',
                   'headers': ['60 日新高（按成交额）', '60 日新低（按成交额）'],
                   'rows': _pair_rows(data.get('new_high') or [], data.get('new_low') or [])})

    # D2 龙虎榜子块（开发项 D；依赖 J 修复后的行粒度金额）
    blocks.extend(_lhb_blocks(lhb))
    # D3 分笔资金流子块（开发项 D）
    blocks.extend(_money_flow_blocks(money_flow))

    cross = data.get('cross_check')
    if cross and cross.get('ok') is False:
        blocks.append({'type': 'note',
                       'text': f"⚠ 交叉校验不一致：链 CSV 涨停 {cross['csv_limit_up']} 家 vs "
                               f"报告重算 {cross['recomputed_limit_up']} 家（详见报告尾部校验说明）"})
    return blocks


def _lhb_blocks(lhb: Dict[str, Any]) -> List[Dict[str, Any]]:
    """D2 龙虎榜：净额 Top10（行粒度）+ 机构净买 Top5"""
    if lhb is None:
        return []
    if not lhb.get('available', True):
        return [{'type': 'note',
                 'text': f"⏳ 待补：龙虎榜（{lhb.get('reason', '当日无数据')}）"}]
    data = lhb['data']
    blocks: List[Dict[str, Any]] = [{'type': 'heading', 'level': 3, 'text': '龙虎榜净买额 Top 10'}]
    if data.get('top'):
        blocks.append({'type': 'table',
                       'headers': ['代码', '名称', '上榜原因', '买入(亿)', '卖出(亿)', '净额(亿)', '成交额(亿)'],
                       'rows': [[r['ts_code'], r['name'], r['reason'],
                                 _yi(r['buy']), _yi(r['sell']), _yi(r['net']),
                                 _yi(r['sec_tx_amount'])] for r in data['top']]})
    else:
        blocks.append({'type': 'para', 'text': '当日无榜单行'})
    if data.get('institution'):
        blocks.append({'type': 'table',
                       'headers': ['代码', '名称', '机构净买(亿)', '机构买入(亿)', '机构卖出(亿)'],
                       'rows': [[r['ts_code'], r.get('name') or '—',
                                 _yi(r['net']), _yi(r['buy']), _yi(r['sell'])]
                                for r in data['institution']]})
    blocks.append({'type': 'note',
                   'text': '口径：金额为该上榜原因的前五席位合计（非全市场买卖总额）；'
                           '多原因上榜股按行展示、禁止跨原因求和；机构净买=席位名含'
                           '"机构专用"的归并近似口径；官网 17:30 后发布'})
    return blocks


def _money_flow_blocks(money_flow: Dict[str, Any]) -> List[Dict[str, Any]]:
    """D3 分笔资金流：大单净流入/净流出 Top10"""
    if money_flow is None:
        return []
    if not money_flow.get('available', True):
        return [{'type': 'note',
                 'text': f"⏳ 待补：分笔资金流（{money_flow.get('reason', '当日无数据')}）"}]
    data = money_flow['data']
    blocks: List[Dict[str, Any]] = [{'type': 'heading', 'level': 3, 'text': '大单净流入 / 净流出 Top 10'}]
    rows = []
    for r in data.get('inflow_top') or []:
        rows.append([r['ts_code'], r.get('name') or '—', _wan(r['big_net']), _yi(r['total_amt'])])
    for r in data.get('outflow_top') or []:
        rows.append([r['ts_code'], r.get('name') or '—', _wan(r['big_net']), _yi(r['total_amt'])])
    if rows:
        blocks.append({'type': 'table',
                       'headers': ['代码', '名称', '大单净额(万)（前 10 净流入，后 10 净流出）', '成交额(万)'],
                       'rows': rows})
    blocks.append({'type': 'note',
                   'text': '口径：异动子集近似口径（涨停∪跌停∪|涨跌幅|≥7%∪成交额Top50，'
                           '单笔≥50 万分桶），非全市场'})
    return blocks


def _detail_table(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """明细表：D2/D3 交叉标记列（榜 / 大单净流入(万)）、E 题材候选列、
    F 封板列（首封/开板），各自有值才展示"""
    has_lhb = any(r.get('lhb') for r in rows)
    has_mf = any(r.get('big_net_wan') is not None for r in rows)
    has_theme = any(r.get('theme') for r in rows)
    has_seal = any(r.get('seal_time') for r in rows)
    headers = ['代码', '名称', '涨幅%', '连板', '放量倍数', '成交额(万)', 'ST']
    if has_lhb:
        headers.append('榜')
    if has_mf:
        headers.append('大单净流入(万)')
    if has_theme:
        headers.append('题材候选')
    if has_seal:
        headers.extend(['首封', '开板'])
    table_rows = []
    for r in rows:
        row = [r['code'], r['name'], r['change_rate'], r['streak'],
               r['volume_surge'], _wan(r['amount']), r['is_st']]
        if has_lhb:
            row.append(r.get('lhb'))
        if has_mf:
            row.append(r.get('big_net_wan'))
        if has_theme:
            row.append(r.get('theme') or '—')
        if has_seal:
            row.extend([r.get('seal_time') or '—', r.get('open_count') if r.get('open_count') is not None else '—'])
        table_rows.append(row)
    return {'type': 'table', 'headers': headers, 'rows': table_rows}


def _pair_rows(highs: List[Dict[str, Any]], lows: List[Dict[str, Any]]) -> List[List[str]]:
    size = max(len(highs), len(lows))
    out = []
    for i in range(size):
        h = highs[i] if i < len(highs) else None
        l = lows[i] if i < len(lows) else None
        out.append([
            f"{h['code']} {h['name']}" if h else '',
            f"{l['code']} {l['name']}" if l else '',
        ])
    return out or [['', '']]


def _wan(amount):
    return round(amount / 1e4) if amount else None


def _yi(amount):
    return round(amount / 1e8, 2) if amount else None
