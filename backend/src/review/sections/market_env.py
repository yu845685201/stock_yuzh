"""维度1 大盘环境章节（§5.3）：指数涨跌与位置 / 两市成交额 / 上涨下跌家数 /
涨停跌停对比 / 外围市场。纯函数：读聚合结果 → 章节块结构。"""

from __future__ import annotations

from typing import Any, Dict, List


def build(env: Dict[str, Any], sentiment: Dict[str, Any],
          margin: Dict[str, Any] = None,
          volume_price: Dict[str, Any] = None,
          capital_anchors: Dict[str, Any] = None,
          news: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """维度1 章节块。env/market_env 与 sentiment/sentiment 的聚合结果；margin 为 D1 两融；
    volume_price 为量价四象限双锚、capital_anchors 为市场级资金锚（P1/P3，量价资金方案 V1.1）；
    news 为市场要闻（四期方案 §4）。"""
    blocks: List[Dict[str, Any]] = [
        {'type': 'heading', 'level': 2,
         'text': '一、大盘环境（口径说明见复盘框架.MD §2.1）'},
    ]
    if not env.get('available', True):
        blocks.append({'type': 'note',
                       'text': f'⏳ 待补：大盘环境数据（{env.get("reason", "表空")}）'})
        return blocks

    data = env['data']

    # 指数涨跌与位置
    indices = data.get('indices') or []
    if indices:
        blocks.append({'type': 'heading', 'level': 3, 'text': '指数涨跌与位置'})
        blocks.append({'type': 'table',
                       'headers': ['指数', '涨跌幅%', '收盘', '60日位置百分位'],
                       'rows': [[i['name'], i['change_rate'], i['close'],
                                 _pct(i['position_pct'])] for i in indices]})
        blocks.append({'type': 'note',
                       'text': '位置百分位 = (收盘−60日最低)/(60日最高−60日最低)，0=年内窗口底部、100=顶部'})
    else:
        blocks.append({'type': 'note', 'text': '⏳ 待补：指数行情（trade_index_info 为空或未跑 sync-index-kline）'})

    # 两市成交额
    turnover = data.get('turnover') or {}
    if turnover:
        mkt_names = {'sh': '沪市', 'sz': '深市', 'bj': '北交所'}
        rows = [[mkt_names.get(m, m), _yi(v.get('today')), _yi(v.get('prev'))]
                for m, v in turnover.get('by_market', {}).items()]
        delta = turnover.get('delta')
        delta_pct = turnover.get('delta_pct')
        suffix = ''
        if delta is not None:
            direction = '增加' if delta >= 0 else '减少'
            pct = f'{delta_pct:+.2f}%' if delta_pct is not None else ''
            suffix = f'（较昨{direction} {abs(delta) / 1e8:,.0f} 亿，{pct}）'
        blocks.append({'type': 'heading', 'level': 3, 'text': f'两市成交额{suffix}'})
        blocks.append({'type': 'table', 'headers': ['市场', '今日(亿)', '昨日(亿)'],
                       'rows': rows})
    else:
        blocks.append({'type': 'note', 'text': '⏳ 待补：成交额统计（当日面板为空）'})

    # 上涨/下跌/平盘
    ad = data.get('advance_decline') or {}
    if ad:
        blocks.append({'type': 'heading', 'level': 3, 'text': '涨跌家数'})
        blocks.append({'type': 'para',
                       'text': f"上涨 {ad['up']} 家 / 下跌 {ad['down']} 家 / 平盘 {ad['flat']} 家"
                               f"（有成交 {ad['total']} 家；剔除停牌无成交行）"})

    # D1 两融子块（开发项 D；表空 → 待补）
    if margin is not None:
        if margin.get('available', True):
            md = margin['data']
            blocks.append({'type': 'heading', 'level': 3, 'text': '两融余额'})
            latest_yi = round(md['latest_rzye'] / 1e8) if md.get('latest_rzye') else None
            delta_yi = round(md['delta'] / 1e8, 1) if md.get('delta') is not None else None
            delta_txt = (f"较上一数据日{'增加' if delta_yi >= 0 else '减少'} {abs(delta_yi):,.1f} 亿"
                         if delta_yi is not None else '—')
            blocks.append({'type': 'para',
                           'text': f"融资余额（沪深合计）{latest_yi:,.0f} 亿"
                                   f"（{md.get('latest_date', '—')}），{delta_txt}"
                                   + (f"；6 日趋势 {md['trend']}" if md.get('trend') else '')})
            blocks.append({'type': 'note',
                           'text': '口径：数据日期 T-1（交易所次日盘前发布）；融资余额为'
                                   '沪深两所合计；融券余额仅深市有值（沪市官网无该列），本报告不展示'})
            if md.get('partial_dates'):
                blocks.append({'type': 'note',
                               'text': f"⚠ 以下数据日因一所单边缺数据（未发布/停更）未计入趋势对比："
                                       f"{md['partial_dates']}"})
        else:
            blocks.append({'type': 'note',
                           'text': f"⏳ 待补：两融数据（{margin.get('reason', '表空')}）"})

    # 涨停/跌停对比（复用情绪维度重算结果，口径同链模块）
    if sentiment.get('available', True):
        m = sentiment['data']
        # 量价四象限（双锚，P1）
    vp = (volume_price or {})
    if vp.get('available'):
        anchors = (vp.get('data') or {}).get('anchors') or []
        if anchors:
            blocks.append({'type': 'heading', 'level': 3, 'text': '量价状态（四象限，双锚）'})
            blocks.append({'type': 'table',
                           'headers': ['锚', '成交额(亿)', 'vs 5日均额', '当日%', '量能档', '四象限判定', '5日均额趋势'],
                           'rows': [[a['name'], f"{a['amount_yi']:,.0f}",
                                     f"{a['r5']:.2f}" if a.get('r5') is not None else '—',
                                     f"{a['chg']:+.2f}", a.get('volume_state') or '—',
                                     f"{a.get('quad') or '—'}（{a.get('verdict') or '—'}）",
                                     a.get('trend_5d') or '—'] for a in anchors]})
            blocks.append({'type': 'note',
                           'text': '口径：成交额比值 r5 分档（放量≥1.1/缩量≤0.9）×当日涨跌（±0.3% 算平）→'
                                   '放量涨=健康 / 缩量涨=背离 / 放量跌=恐慌 / 缩量跌=阴跌；'
                                   '双锚独立判定，矛盾时点名分歧；阈值判据初版待校准'})

    # 市场级资金锚（P3）
    ca = (capital_anchors or {})
    if ca.get('available'):
        cdata = ca.get('data') or {}
        if cdata.get('hkex_date') or cdata.get('etf'):
            blocks.append({'type': 'heading', 'level': 3, 'text': '资金锚（互联互通/ETF 份额）'})
            rows = []
            if cdata.get('hkex_date'):
                rows.append(['北向成交总额', f"{cdata['north_total_yi']:,.1f} 亿",
                             '外资活跃度（买+卖合计，停披露分项，禁止解读净额）'])
                rows.append(['南向净买入', f"{cdata['south_net_yi']:+,.1f} 亿港元",
                             '内地资金买港股（Buy−Sell 真净额；0=港股通关闭）'])
            etf = cdata.get('etf') or {}
            if etf.get('chg_yi_fen') is not None:
                rows.append(['头部宽基ETF份额变动', f"{etf['chg_yi_fen']:+.2f} 亿份",
                             f"沪深300/上证50/科创50/创业板等合计（{etf.get('date')} vs {etf.get('prev_date')}）"])
            elif etf.get('note'):
                rows.append(['头部宽基ETF份额变动', '—', etf['note']])
            if rows:
                blocks.append({'type': 'table',
                               'headers': ['锚', '当日值', '口径'],
                               'rows': rows})

        # ---------- 第三阶段新锚（2026-10-23） ----------
        # 宏观指标锚（低频背景；各指标按自身发布日展示，禁混同日）
        macro = cdata.get('macro') or {}
        macro_items = macro.get('items') or []
        if macro_items:
            blocks.append({'type': 'heading', 'level': 3,
                           'text': '宏观锚（利率/货币量/物价/景气）'})
            rows = [[m['name'],
                     ('—' if m['value'] is None
                      else f"{m['value']:g}{m['unit'] or ''}"),
                     m['trade_date']]
                    for m in macro_items]
            blocks.append({'type': 'table',
                           'headers': ['指标', '最新值', '发布日'],
                           'rows': rows})
            blocks.append({'type': 'note',
                           'text': '口径：各指标按自身发布日展示（月频指标最新发布日'
                                   '可能早于锚定日，属正常）；PPI 源为金十免费'
                                   ' tier（滞后约一年），解读以发布日为准。'
                                   '采集：sync-macro-indicator（低频，日/周一次）'})
        elif cdata.get('hkex_date') or cdata.get('etf'):
            blocks.append({'type': 'para',
                           'text': '宏观锚：未采集（sync-macro-indicator 拉取'
                                   ' LPR/SHIBOR/PMI/M1/M2/社融/CPI/PPI）'})

        # ETF 期权情绪锚（逐标的 PC 率/OI；SSE 口径）
        option = cdata.get('option') or {}
        opt_items = option.get('items') or []
        if opt_items:
            blocks.append({'type': 'heading', 'level': 3,
                           'text': f"ETF 期权情绪（{option.get('trade_date')}）"})
            rows = []
            for o in opt_items:
                pc = o.get('put_call_ratio')
                rows.append([f"{o['underlying_name']}({o['underlying_code']})",
                             f"{o['contract_count']} 只" if o.get('contract_count') is not None else '—',
                             f"{pc:.1f}%" if pc is not None else '—',
                             f"{o['oi_total'] / 1e4:,.0f} 万张" if o.get('oi_total') is not None else '—'])
            blocks.append({'type': 'table',
                           'headers': ['期权标的', '挂牌合约', '认沽/认购成交比', '未平仓合约'],
                           'rows': rows})
            note = ('口径：上交所官方每日统计（T 日盘后）；PC 率=认沽'
                    '成交量÷认购成交量×100（>100 偏防御）；未平仓为'
                    '全市场合计。逐合约 IV/希腊字母见 option_daily 表。'
                    '采集：sync-option-daily')
            if option.get('is_latest_fallback'):
                note += (f"；⚠ 锚定日 {option.get('as_of')} 无统计"
                         f"（SSE 统计接口仅返回最新交易日），本表为库内最新"
                         f"数据日 {option.get('trade_date')}")
            blocks.append({'type': 'note', 'text': note})

        # 北向个股持仓 Top N（明细披露止于 2024-08-16，data_date 显式标注）
        north = cdata.get('north_detail') or {}
        north_items = north.get('items') or []
        if north_items:
            blocks.append({'type': 'heading', 'level': 3,
                           'text': f"北向个股持仓 Top{len(north_items)}"
                                   f"（数据日 {north.get('data_date')}）"})
            rows = [[f"{n['name']}({n['ts_code']})",
                     f"{n['hold_market_value_yi']:,.1f} 亿" if n.get('hold_market_value_yi') is not None else '—',
                     f"{n['hold_ratio']:.2f}%" if n.get('hold_ratio') is not None else '—']
                    for n in north_items]
            blocks.append({'type': 'table',
                           'headers': ['股票', '北向持股市值', '占A股比例'],
                           'rows': rows})
            note = ('口径：东财沪深港通个股持股（沪/深双通道聚合）；'
                    '**交易所 2024-08-19 起停止披露个股每日明细**，'
                    f"当前数据止于 {north.get('max_date')}")
            if north.get('is_latest_fallback'):
                note += '，本表为库内最新数据日（非锚定日）'
            blocks.append({'type': 'note', 'text': note})

    blocks.append({'type': 'heading', 'level': 3, 'text': '涨停/跌停对比'})
    if m.get('limit_up_count') is not None:
        blocks.append({'type': 'para',
                       'text': f"涨停 {m['limit_up_count']} 家 / 跌停 {m['limit_down_count']} 家"
                               f" / 炸板 {m['burst_count']} 家"
                               + (f"（炸板率 {m['burst_rate']:.1%}）" if m.get('burst_rate') is not None else '')})

    # 大小盘风格（I-1 + N2 2026-10-04：沪深300 vs 中证1000 vs 中证2000；正差值=大盘占优）
    spread = data.get('style_spread')
    if spread:
        parts = [f"沪深300 {spread['big']:+.2f}% vs 中证1000 {spread['small']:+.2f}%"]
        if spread.get('small_2000') is not None:
            parts.append(f"中证2000 {spread['small_2000']:+.2f}%")
        parts.append(f"差值(300−1000) {spread['spread']:+.2f} pct")
        if spread.get('spread_2000') is not None:
            parts.append(f"(300−2000) {spread['spread_2000']:+.2f} pct")
        verdict = ('大盘占优' if spread['spread'] >= 0 else '小盘占优')
        blocks.append({'type': 'para',
                       'text': '大小盘风格：' + '，'.join(parts) + f'（{verdict}）'})

    # ---------- 外围（阶段 2 V2.0 §4：日K指数 / 汇率与其他 / 假期观察 / A-H 映射） ----------

    # ① 外围指数（日K）——K 线空时降级回快照全量两值表（B1）
    idx = data.get('overseas_indices') or []
    if idx:
        blocks.append({'type': 'heading', 'level': 3,
                       'text': f"外围指数（{idx[0]['date']}）"})
        blocks.append({'type': 'table',
                       'headers': ['指数', '收盘', '当日%', '5日%', '20日%', '60日位置'],
                       'rows': [[i['name'], i['close'], _signed(i['change_pct']),
                                 _signed(i['chg_5d']), _signed(i['chg_20d']),
                                 _pct(i['position_pct'])] for i in idx]})
        blocks.append({'type': 'note',
                       'text': '口径：overseas_kline_day（腾讯 ifzq，qfq 前复权，市场自身交易日历）；'
                               '60日位置 = (收盘−60日最低)/(60日最高−60日最低)，与 A 股指数章同构；'
                               '当日/5日/20日为该市场日历累计'})
    else:
        fallback = data.get('overseas_all') or []
        if fallback:
            blocks.append({'type': 'heading', 'level': 3,
                           'text': f"外围市场（{fallback[0].get('date', '最新可得')}）"
                                   f"（快照口径，日K缺失）"})
            blocks.append({'type': 'table', 'headers': ['市场', '收盘', '涨跌幅%'],
                           'rows': [[o['name'], o['close'], o['change_pct']]
                                    for o in fallback]})

    # ② 汇率与其他（快照补充项：A50/汇率，含 fx_shkdcny）
    overseas = data.get('overseas') or []
    if overseas:
        blocks.append({'type': 'heading', 'level': 3,
                       'text': f"汇率与其他（{overseas[0].get('date', '最新可得')}）"})
        blocks.append({'type': 'table', 'headers': ['市场', '收盘', '涨跌幅%'],
                       'rows': [[o['name'], o['close'], o['change_pct']] for o in overseas]})
        blocks.append({'type': 'note',
                       'text': '口径：overseas_market_daily 快照最新可得；A50 期货涨跌幅为'
                               '本地昨收自算（快照无昨结字段）；汇率为采集时点最新值'})

    # ③ 假期外围观察（B4/B5 未触发时为 None）
    hol = data.get('holiday_overseas')
    if hol and hol.get('rows'):
        blocks.append({'type': 'heading', 'level': 3,
                       'text': f"假期外围观察（{hol['prev_date']} 后休市区间，"
                               f"截至 {hol.get('last_date') or '—'}）"})
        blocks.append({'type': 'table',
                       'headers': ['指数', '区间累积%', '区间交易日'],
                       'rows': [[r['name'], _signed(r['accum_pct']), r['days'] or 0]
                                for r in hol['rows']]})
        blocks.append({'type': 'note',
                       'text': '口径：区间（前一 A 股交易日, 锚定日）各市场自身交易日首尾收盘比'
                               '（qfq）；锚定日当日变化计入"外围指数"当日%，不重复计入；'
                               '"截至"随采集推进自动更新，节后首日为完整区间'})

    # ④ A-H/外围映射（Q5 五日窗口 / Q8 对齐 / Q9 溢价参考口径）
    ah = data.get('ah_mapping') or {}
    ah_rows = ah.get('rows') or []
    if ah_rows:
        blocks.append({'type': 'heading', 'level': 3,
                       'text': f"A-H/外围映射（5 日累计，锚定 {data.get('as_of', '—')}）"})
        blocks.append({'type': 'table',
                       'headers': ['H股', 'A侧5日%', 'H侧5日%', '差值', 'AH溢价%'],
                       'rows': [[r['h_name'], _signed(r['chg_a_5d']), _signed(r['chg_h_5d']),
                                 '未对齐' if r['unaligned'] else _signed(r['diff']),
                                 None if r['unaligned'] else _signed(r['premium'], nd=1)]
                                for r in ah_rows]})
        ranked = sorted((r for r in ah_rows
                         if not r['unaligned'] and r.get('diff') is not None),
                        key=lambda r: abs(r['diff']), reverse=True)[:3]
        if ranked:
            naming = '、'.join(f"{r['h_name']} {r['diff']:+.2f}pct" for r in ranked)
            blocks.append({'type': 'para', 'text': f'A−H 差值最大：{naming}'})
        note = ('口径：两侧各自市场日历 5 日累计（当日收盘错一个交易时段，故用 5 日窗口）；'
                '对齐 = 锚定日与该股最近行相差 ≤3 个市场交易日，未对齐行不参与比较与点名；'
                '溢价率 = A 收盘 /（H 收盘 × 港元兑人民币快照）− 1，为参考口径'
                '（A 15:00 / H 16:00 / 汇率采集时点三者错位，两侧复权基准独立滚动），'
                '只作观察不作结论')
        if ah.get('fx') is None:
            note += '；汇率待补（fx_shkdcny 未采集），溢价整列缺'
        blocks.append({'type': 'note', 'text': note})

    # ⑤ 市场要闻（四期方案 §4；news_telegraph 当日快讯，importance 优先）
    blocks.extend(_news_blocks(news))
    return blocks


def _news_blocks(news: Dict[str, Any] = None) -> List[Dict[str, Any]]:
    """市场要闻小节（缺数据整块不出现；归因=市场叙事口径注记）。"""
    data = (news or {}).get('data') or {}
    items = data.get('items') or []
    if not items:
        return []
    blocks = [{'type': 'heading', 'level': 3, 'text': '市场要闻（新浪 7x24，重要优先）'}]
    for it in items[:10]:
        star = '★ ' if it.get('importance') else ''
        time_part = f"[{it.get('time', '')[11:16]}] " if len(str(it.get('time', ''))) >= 16 else ''
        blocks.append({'type': 'bullets',
                       'items': [f'{star}{time_part}{it.get("title")}']})
    blocks.append({'type': 'note',
                   'text': '口径：新浪 7x24 快讯当日条目（importance=焦点优先，时间倒序）；'
                           '新闻归因是市场叙事，不是核验过的因果；财联社签名接口按'
                           '合规红线不采'})
    return blocks


def _pct(v):
    return f'{v:.0f}%' if v is not None else None


def _signed(v, nd=2):
    """带号两位（溢价一位）小数格式化；None → None（渲染 '—'）"""
    return f'{v:+.{nd}f}' if v is not None else None


def _yi(v):
    return round(v / 1e8) if v else None
