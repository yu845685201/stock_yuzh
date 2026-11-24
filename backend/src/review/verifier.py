"""复盘核验器（复盘闭环与赚钱效应四期优化方案 §1）。

两件事：

- ``build_predictions``：从聚合结果抽取机器可读预期（情绪定位 / V1·V1.5·V2·V3
  验点基准 / 盘前情景锚（含基准可得约定）/ R1~R4 候选池），generator 生成报告
  后落库 ``review_prediction_daily``；
- ``verify_predictions``：用今日截面核验昨日预期（aggregator 聚合时产出
  ``out['verification']``，generator 落库 ``review_verification_daily``），
  附滚动战绩（近 30 个核验日）。

情景核验（2026-11-24 §3.1 修订）：取数与判档分离至 ``review.scenario``——
新观测只接受海外业务日**严格早于** as_of 的收盘（同业务日港美收盘一律剔除，
防前视），判档先完整度后方向（P1/P2/P3/分歧/部分证据/待核/无新观测），
仅 P1/P3 进入方向命中率分母。

三态规范：**成立 / 不成立 / 数据不足**；信息性观察项判 "记录"（不计入战绩）。
竞价与盘中"过程判据"（决策树 confirm/veto 的竞价分支）无盘前数据，按设计只核验
结果端；数据不足 ≠ 判错，不污染战绩。基准发生率口径：候选池超额 = 池中位涨跌幅 −
全市场已交易中位涨跌幅；"≈0 说明判的都是本来就会发生的事"。

预期落库由 generator 用 DatabaseConnection 写（DataReader 只读）；核验的读路径
走 reader._query（SELECT payload FROM review_prediction_daily）。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .candidate_publication import published_flat, publication_summary
from .scenario import (DIRECTION_EXPECTATIONS, DIRECTION_STATUSES,
                       build_anchor_obs, classify_scenario, read_anchor_quote)

logger = logging.getLogger(__name__)

#: 情景锚（与 sections/tomorrow_plan._OVERSEAS_ANCHORS 一致）
SCENARIO_ANCHORS = ('恒生指数', '纳斯达克')

#: 池键与顺序
POOL_KEYS = ('R1', 'R2', 'R3', 'R4', 'CHAN')

#: eps 阈值（"变了才算变"）：溢价均值 ±0.5pct、开盘平开带 ±0.3%
EPS_PREMIUM = 0.5
EPS_OPEN_FLAT = 0.3
#: 滚动战绩窗口（核验日数）
ROLLING_DAYS = 30


# ------------------------------------------------------------------ 预期抽取

def build_predictions(agg: Dict[str, Any], review_cfg: Dict[str, Any]) -> Dict[str, Any]:
    """从聚合结果抽取机器可读预期（落库 payload，version=1）。"""
    sent = (agg.get('sentiment') or {}).get('data') or {}
    me = (agg.get('market_env') or {}).get('data') or {}
    vp = (agg.get('volume_price') or {}).get('data') or {}
    movers = (agg.get('movers') or {}).get('data') or {}
    cand = agg.get('candidates') or {}

    tp_rules = ((review_cfg.get('tomorrow_plan')) or {})
    verify_points: Dict[str, Any] = {}
    turnover = me.get('turnover') or {}
    if turnover.get('today_total'):
        verify_points['V1'] = {
            'base_yi': round(float(turnover['today_total']) / 1e8, 0),
            'band_pct': float(tp_rules.get('volume_band_pct') or 20)}
    anchors = vp.get('anchors') or []
    if anchors:
        verify_points['V1.5'] = {'quads': [{'name': a.get('name'), 'quad': a.get('quad')}
                                           for a in anchors]}
    limit_up = movers.get('limit_up') or []
    if limit_up and (limit_up[0].get('streak') or 0) >= 2:
        top = limit_up[0]
        verify_points['V2'] = {'code': top.get('code'), 'name': top.get('name'),
                               'streak': int(top.get('streak') or 0)}
    if sent.get('premium_mean') is not None:
        verify_points['V3'] = {'premium_mean': float(sent['premium_mean']),
                               'limit_up_count': int(sent.get('limit_up_count') or 0)}

    scenario_anchors: Dict[str, Any] = {}
    for e in me.get('overseas_indices') or []:
        if e.get('name') in SCENARIO_ANCHORS and e.get('close') is not None:
            scenario_anchors[e['name']] = {
                'close': float(e['close']),
                'date': e.get('date'),
                # 基准可得性约定（§3.1 冻结时点）：T 日报告冻结 T 日港美收盘作
                # N 日盘前基准——对 N 日 09:15 截止必然可知（盘后采集，业务日
                # 早于 N）；核验侧只接受业务日严格早于 as_of 的新观测。
                'base_convention': 'T日收盘（对N日盘前可知）',
            }
    # 样本 cohort 冻结（§4.1/§4.3 2026-11-24 第二轮）：R2 发布名单（同类基准
    # 候选组）+ T 全部可确认首板（同类基准分母，日历连续性核验）——N 日核验
    # 按代码对截面显式左连接计算三组中位数与两条超额；空池/缺 cohort 时
    # 该组降级（不硬造研究样本充当正式名单）。
    frozen = agg.get('cohorts') or {}
    cohorts_out: Dict[str, Any] = {}
    if frozen.get('r2_published') is not None:
        cohorts_out['R2'] = frozen['r2_published']
    if frozen.get('first_board') is not None:
        cohorts_out['first_board'] = frozen['first_board']
    # 三项次日回填（§4.3）：T 涨停名单 / T 可确认二板名单 / 量价锚——
    # N 日收盘后在「昨日核验」回填（首页冻结三项跟踪问题）
    backfills_out: Dict[str, Any] = {}
    if frozen.get('limit_up') is not None:
        backfills_out['limit_up'] = frozen['limit_up']
    if frozen.get('two_board') is not None:
        backfills_out['two_board'] = frozen['two_board']
    anchors = agg.get('backfill_anchors') or {}
    if anchors.get('volume_price'):
        backfills_out['volume_price'] = anchors['volume_price']
    cycle = sent.get('cycle') or {}
    # 候选池（§3.3 2026-11-24）：预测落库读**发布名单**（published）——
    # 正文关闭的池不再进入预测 payload（11-23 三只 R2 原始命中案例归处：
    # 题材跟踪单列，不作为次日核验的正式池成员）；发布批次摘要随行，
    # 各阶段计数（raw/eligible/published）供回放对账。旧缓存无 publication
    # 键时回退聚合全量（向后兼容，不改变既有历史 payload 语义）。
    pub = agg.get('candidate_publication')
    if pub is not None:
        pools: Dict[str, List[Dict[str, Any]]] = {}
        flat = {r['pool']: [] for r in published_flat(pub)}
        for r in published_flat(pub):
            flat[r['pool']].append(r)
        for key in POOL_KEYS:
            pools[key] = flat.get(key) or []
        chan_pool = [{'ts_code': r.get('ts_code'), 'code': r.get('code'),
                      'name': r.get('name'), 'label': r.get('label'),
                      'evidence_count': None}
                     for r in (pub.get('chan') or {}).get('published') or []]
        pools['CHAN'] = chan_pool
        publication_meta = publication_summary(pub)
    else:
        pools = {}
        for key in POOL_KEYS:
            pools[key] = [{'ts_code': r.get('ts_code'), 'code': r.get('code'),
                           'name': r.get('name'), 'label': r.get('label'),
                           'evidence_count': r.get('evidence_count')}
                          for r in (cand.get(key) or [])]
        # 缠论池（2026-10-24）：agg['chan'] 为 AggregationResult（dataclass），
        # 与 R 池 dict 形态不同，单独抽取；日度核验为短窗代理（验证口径是 20/60 日持有）
        chan = agg.get('chan')
        chan_items = list((getattr(chan, 'data', None) or {}).get('items') or []) \
            if chan is not None and getattr(chan, 'available', False) else []
        pools['CHAN'] = [{'ts_code': r.get('ts_code'), 'code': r.get('code'),
                          'name': r.get('name'), 'label': r.get('label'),
                          'evidence_count': None}
                         for r in chan_items]
        publication_meta = None
    return {
        'sentiment': {'position': cycle.get('position'),
                      'max_streak': sent.get('max_streak'),
                      'limit_up_count': sent.get('limit_up_count'),
                      'burst_rate': sent.get('burst_rate'),
                      'premium_mean': sent.get('premium_mean')},
        'verify_points': verify_points,
        'scenarios': {'gap_pct': float(tp_rules.get('overseas_gap_pct') or 1.5),
                      'anchors': scenario_anchors},
        'candidates': pools,
        'candidate_publication': publication_meta,
        'cohorts': cohorts_out,
        'backfills': backfills_out,
        'version': 1,
    }


# ------------------------------------------------------------------ 核验

def verify_predictions(payload: Dict[str, Any], prev_date: str,
                       cs_today, metrics: Dict[str, Any], reader,
                       overseas_index_codes: Dict[str, str]) -> Dict[str, Any]:
    """核验昨日预期（纯读；写库由 generator 完成）。

    Args:
        payload: 昨日预测载荷（review_prediction_daily.payload）。
        prev_date: 预测锚定日。
        cs_today: 锚定日截面（annotate_panel 产物）。
        metrics: 今日情绪指标（sentiment_metrics 返回值）。
        reader: DataReader（_query 只读）。
        overseas_index_codes: 外围指数名 → market_code 映射。
    """
    verify_points = _verify_points(payload.get('verify_points') or {}, cs_today, metrics)
    scenario = _verify_scenario(payload.get('scenarios') or {}, as_of=_as_of_of(cs_today),
                                reader=reader,
                                name2code=overseas_index_codes)
    candidates = _verify_candidates(payload.get('candidates') or {}, cs_today)
    as_of = _as_of_of(cs_today)
    cohorts = _verify_cohorts(payload.get('cohorts') or {}, cs_today)
    backfills = _verify_backfills(payload.get('backfills') or {}, cs_today,
                                  reader, as_of)
    return {'prev_date': prev_date, 'scenario': scenario,
            'verify_points': verify_points, 'candidates': candidates,
            'cohorts': cohorts, 'backfills': backfills}


def _as_of_of(cs_today) -> str:
    """截面锚定日（cs_today 非空时首行 trade_date）。"""
    if cs_today is None or cs_today.empty:
        return ''
    return str(cs_today['trade_date'].iloc[0])


def _verify_cohorts(cohort_payload: Dict[str, Any],
                   cs_today) -> Dict[str, Any]:
    """同类基准核验（§4.1 D1）：R2 发布 cohort × 全市场 × T 可确认首板。

    三组同一段 T 收盘→N 收盘收益（N 日截面 change_rate，qfq 收盘对收盘可比），
    分别取中位数，超额 = 候选中位 − 基准中位（百分点）。空值规范：
    候选有效数为零 → 两条超额均 None；某基准无有效样本 → 仅对应超额 None
    （不转零）。昨日预测无 cohort（体系建立前/旧 payload）→ 空 dict（该组
    降级，不影响其余核验）。
    """
    from .return_metrics import evaluate_cohort, excess_stats, market_sample
    out: Dict[str, Any] = {}
    r2 = cohort_payload.get('R2')
    if r2 is None:
        return out
    cand = evaluate_cohort(r2.get('ts_codes') or [], cs_today)
    market = market_sample(cs_today)
    peer = None
    peer_payload = cohort_payload.get('first_board')
    if peer_payload is not None:
        peer = evaluate_cohort(peer_payload.get('ts_codes') or [], cs_today)
    stats = excess_stats(cand, market, peer)
    stats['anchor_date'] = r2.get('anchor_date')
    stats['selection_rule'] = r2.get('selection_rule')
    stats['member_hash'] = r2.get('member_hash')
    if peer_payload is not None:
        stats['peer_rule'] = peer_payload.get('selection_rule')
        stats['peer_unknown_n'] = peer_payload.get('n_unknown')
    out['R2'] = stats
    return out


def _verify_backfills(backfill_payload: Dict[str, Any], cs_today,
                      reader, as_of: str) -> Dict[str, Any]:
    """三项次日回填核验（§4.3 E2；昨日预测 backfills → 今日截面/行情）。

    涨停延续 / 二板晋级按 N 日截面回填（固定分组不缩小，缺行单列）；
    量价延续按交易日历应到日期核对六个端点（盘中占位行不能充当收盘）。
    昨日预测无 backfills（旧 payload）→ 空 dict。
    """
    from .backfills import verify_backfills
    return verify_backfills(backfill_payload, cs_today, reader, as_of)


def _verify_scenario(sc: Dict[str, Any], as_of: str, reader,
                     name2code: Dict[str, str]) -> Dict[str, Any]:
    """情景核验（2026-11-24 口径修订 §3.1，防前视）。

    取数与判档分离：新观测由 ``scenario.read_anchor_quote`` 读取——海外业务日
    **严格早于** as_of（A 股开盘前已完整结束的交易时段收盘；恒指/纳指 as_of
    当日行一律剔除，2026-11-23 同日港美收盘前视从此不可能）；判档交给
    ``scenario.classify_scenario``（先完整度后方向：P1/P2/P3/分歧/部分证据/
    待核/无新观测）。

    方向兑现保留既有实际行为：上证开盘缺口先取两位小数再按正负判高低（零
    缺口 → P1/P3 均为「不成立」）；仅 P1/P3 具备方向核验资格并计入滚动命中
    率分母，分歧/部分证据单列，待核/无新观测不计（数据不足 ≠ 判错）。
    """
    gap = float(sc.get('gap_pct') or 1.5)
    anchors = sc.get('anchors') or {}
    if not anchors or not as_of:
        return {'status': '数据不足', 'reason': '昨日预测无情景锚',
                'gap_pct': gap, 'moves': [], 'triggered': None,
                'completeness': '无锚', 'direction_eligible': False,
                'scenario_reason': '昨日预测无情景锚', 'expectation': None,
                'open_gap': None, 'fulfilled': None}
    obs: Dict[str, Dict[str, Any]] = {}
    for name in SCENARIO_ANCHORS:
        base = anchors.get(name) or {}
        quote = read_anchor_quote(reader, name2code.get(name), as_of,
                                  base_date=base.get('date'))
        obs[name] = build_anchor_obs(name, base, quote)
    result = classify_scenario(obs['恒生指数'], obs['纳斯达克'], gap)
    triggered = result['triggered']
    # A 股开盘反应（上证 open vs 昨收）——读取保持既有行为
    open_gap = None
    try:
        rows = reader._query(
            "SELECT open, preclose FROM trade_index_info "
            "WHERE index_code = '000001' AND trade_date = %s", (as_of,))
    except Exception as e:
        logger.warning(f'情景核验上证开盘读取失败: {e}')
        rows = []
    if rows and rows[0][0] and rows[0][1]:
        open_gap = round((float(rows[0][0]) / float(rows[0][1]) - 1) * 100, 2)
    expectation = DIRECTION_EXPECTATIONS.get(triggered)
    if expectation is None or open_gap is None:
        fulfilled = None  # 非方向档 / 数据缺失 → 不计战绩
    elif expectation == '低开':
        fulfilled = '成立' if open_gap < 0 else '不成立'
    else:
        fulfilled = '成立' if open_gap > 0 else '不成立'
    return {'status': '记录', 'gap_pct': gap, 'moves': result['moves'],
            'triggered': triggered,
            'completeness': result['completeness'],
            'direction_eligible': triggered in DIRECTION_STATUSES,
            'scenario_reason': result['reason'],
            'expectation': expectation,
            'open_gap': open_gap, 'fulfilled': fulfilled}


def _verify_points(preds: Dict[str, Any], cs_today, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
    """验点核验：V1/V1.5/V3 为信息性记录；V2（高度股晋级）出三态。"""
    pts: List[Dict[str, Any]] = []
    today_yi = None
    if cs_today is not None and not cs_today.empty:
        today_yi = float(cs_today['amount'].sum()) / 1e8
    v1 = preds.get('V1')
    if v1 and v1.get('base_yi') and today_yi:
        base = float(v1['base_yi'])
        band = float(v1.get('band_pct') or 20) / 100
        ratio = today_yi / base
        state = ('放量' if ratio >= 1 + band
                 else '缩量' if ratio <= 1 - band else '温和')
        pts.append({'key': 'V1 量能',
                    'base': f"{base:.0f} 亿 ±{v1.get('band_pct', 20):.0f}%",
                    'actual': f'{today_yi:.0f} 亿（{ratio:.2f}，{state}）',
                    'verdict': '记录'})
    v15 = preds.get('V1.5') or {}
    quads = v15.get('quads') or []
    if quads:
        pts.append({'key': 'V1.5 量价联动',
                    'base': '；'.join(f"{q['name']} {q.get('quad') or '—'}" for q in quads),
                    'actual': '今日象限见量价四象限（对照迁移）', 'verdict': '记录'})
    v2 = preds.get('V2')
    if v2 and v2.get('code'):
        row = None
        if cs_today is not None and not cs_today.empty:
            hit = cs_today[cs_today['stock_code'].astype(str) == str(v2['code'])]
            if not hit.empty:
                row = hit.iloc[0]
        if row is None:
            verdict, actual = '数据不足', '今日无行情行（停牌/缺行）'
        else:
            up = bool(row['_limit_up'])
            chg = row['change_rate']
            chg_s = f'{float(chg):+.2f}%' if chg is not None and _notna(chg) else '—'
            down = bool(row.get('_limit_down'))
            if up:
                verdict = '成立'
            else:
                verdict = '不成立'
            actual = f"今日 {chg_s}" + ('，跌停' if down and not up else
                                        '，晋级涨停' if up else '，断板')
        pts.append({'key': 'V2 梯队',
                    'base': f"高度股 {v2.get('name')}（{v2.get('streak')} 板）晋级",
                    'actual': actual, 'verdict': verdict})
    v3 = preds.get('V3')
    if v3 and v3.get('premium_mean') is not None and metrics.get('premium_mean') is not None:
        base = float(v3['premium_mean'])
        today = float(metrics['premium_mean'])
        diff = today - base
        direction = ('上升' if diff > EPS_PREMIUM
                     else '下降' if diff < -EPS_PREMIUM else '持平')
        pts.append({'key': 'V3 溢价',
                    'base': f'均值基准 {base:+.2f}%',
                    'actual': f'今日 {today:+.2f}%（{direction}，差 {diff:+.2f}pct）',
                    'verdict': '记录'})
    return pts


def _verify_candidates(pools: Dict[str, Any], cs_today) -> Dict[str, Any]:
    """候选池核验：每池今日均值/中位/胜率/晋级/炸板/跌停 + 超额（vs 全市场中位）。"""
    today: Dict[str, Dict[str, Any]] = {}
    chgs_all: List[float] = []
    limit_up_total = 0
    if cs_today is not None and not cs_today.empty:
        change = cs_today['change_rate']
        traded = cs_today['_traded'].fillna(False).astype(bool) \
            if '_traded' in cs_today.columns else change.notna()
        lu = cs_today['_limit_up'].fillna(False).astype(bool)
        bu = cs_today['_burst'].fillna(False).astype(bool)
        ld = cs_today['_limit_down'].fillna(False).astype(bool)
        for code, chg, tr, up, burst, down in zip(cs_today['stock_code'], change,
                                                  traded, lu, bu, ld):
            chg_f = float(chg) if tr and chg is not None and _notna(chg) else None
            if chg_f is not None:
                chgs_all.append(chg_f)
            if up:
                limit_up_total += 1
            today[str(code)] = {'chg': chg_f, 'up': bool(up), 'burst': bool(burst),
                                'down': bool(down)}
    market_median = _median(chgs_all)
    market_limit_rate = round(limit_up_total / len(chgs_all), 4) if chgs_all else None

    out: Dict[str, Any] = {'market_median': market_median,
                           'market_limit_rate': market_limit_rate, 'pools': {}}
    for key in POOL_KEYS:
        rows = pools.get(key) or []
        recs = []
        for r in rows:
            t = today.get(str(r.get('code')))
            recs.append({'code': r.get('code'), 'name': r.get('name'),
                         'label': r.get('label'),
                         'evidence_count': r.get('evidence_count'),
                         'chg': t['chg'] if t else None,
                         'limit_up': bool(t['up']) if t else None,
                         'burst': bool(t['burst']) if t else None,
                         'limit_down': bool(t['down']) if t else None})
        traded = [r for r in recs if r['chg'] is not None]
        chgs = [r['chg'] for r in traded]
        median = _median(chgs)
        out['pools'][key] = {
            'count': len(rows), 'traded': len(traded),
            'mean': round(sum(chgs) / len(chgs), 2) if chgs else None,
            'median': median,
            'win_rate': round(sum(1 for c in chgs if c > 0) / len(chgs), 4)
            if chgs else None,
            'limit_up_n': sum(1 for r in traded if r['limit_up']),
            'burst_n': sum(1 for r in traded if r['burst']),
            'down_n': sum(1 for r in traded if r['limit_down']),
            'excess_median': (round(median - market_median, 2)
                              if median is not None and market_median is not None else None),
            'rows': recs,
        }
    return out


def rolling_stats(as_of: str, current: Dict[str, Any], reader) -> Optional[Dict[str, Any]]:
    """滚动战绩（近 30 个核验日，含当日核验 current）：候选池超额/胜率、
    情景兑现率、V2 晋级率。历史读取失败返回 None（报告不失败）。"""
    payloads: List[Dict[str, Any]] = []
    try:
        rows = reader._query(
            "SELECT payload FROM review_verification_daily WHERE trade_date < %s "
            "ORDER BY trade_date DESC LIMIT %s", (as_of, ROLLING_DAYS))
        payloads = [r[0] for r in rows if r[0]]
    except Exception as e:
        logger.warning(f'滚动战绩历史读取失败: {e}')
    # current（当日核验）由这里单独补位；历史查询用 < 严格小于，
    # 否则当日核验已落库时（同日重跑）会被计入两次（C8）
    if current:
        payloads.insert(0, current)
    if len(payloads) < 2:
        return None

    pool_acc = {key: {'excess': [], 'win': 0, 'days': 0, 'limit_rate': [],
                      'mkt_limit_rate': []} for key in POOL_KEYS}
    scen = {'P1': {'n': 0, 'ok': 0}, 'P3': {'n': 0, 'ok': 0}}
    v2 = {'n': 0, 'ok': 0}
    # §4.1（2026-11-24）：R2 同类基准滚动——两条基准分别累计有效交易日数/
    # 有效成员数/平均超额/胜率，空池日单列（n_total=0 不计基准分母）
    bench = {'market': {'days': 0, 'excess': [], 'win': 0, 'valid_members': 0},
             'peer': {'days': 0, 'excess': [], 'win': 0, 'valid_members': 0},
             'empty_pool_days': 0}
    for p in payloads:
        cand = p.get('candidates') or {}
        for key in POOL_KEYS:
            st = (cand.get('pools') or {}).get(key) or {}
            if st.get('traded'):
                acc = pool_acc[key]
                acc['days'] += 1
                if st.get('excess_median') is not None:
                    acc['excess'].append(float(st['excess_median']))
                    if st['excess_median'] > 0:
                        acc['win'] += 1
                if st.get('limit_up_n') is not None and st['traded']:
                    acc['limit_rate'].append(st['limit_up_n'] / st['traded'])
                    if cand.get('market_limit_rate') is not None:
                        acc['mkt_limit_rate'].append(float(cand['market_limit_rate']))
        sc = p.get('scenario') or {}
        trig = sc.get('triggered')
        if sc.get('fulfilled') and trig in scen:
            scen[trig]['n'] += 1
            if sc['fulfilled'] == '成立':
                scen[trig]['ok'] += 1
        for pt in p.get('verify_points') or []:
            if str(pt.get('key', '')).startswith('V2') and pt.get('verdict') in ('成立', '不成立'):
                v2['n'] += 1
                if pt['verdict'] == '成立':
                    v2['ok'] += 1
        # §4.1：R2 同类基准累计（空池日 n_total=0 单列；两条基准各按可算日累计）
        r2c = (p.get('cohorts') or {}).get('R2')
        if r2c is not None:
            n_total = (r2c.get('candidate') or {}).get('n_total')
            if not n_total:
                bench['empty_pool_days'] += 1
            else:
                for key, ex_key in (('market', 'excess_vs_market'),
                                    ('peer', 'excess_vs_peer')):
                    if r2c.get(ex_key) is not None:
                        acc = bench[key]
                        acc['days'] += 1
                        acc['excess'].append(float(r2c[ex_key]))
                        if r2c[ex_key] > 0:
                            acc['win'] += 1
                        acc['valid_members'] += int(
                            (r2c.get('candidate') or {}).get('n_valid') or 0)

    pools_out = {}
    for key in POOL_KEYS:
        acc = pool_acc[key]
        if not acc['days']:
            continue
        pools_out[key] = {
            'days': acc['days'],
            'avg_excess': round(sum(acc['excess']) / len(acc['excess']), 2)
            if acc['excess'] else None,
            'win_rate': round(acc['win'] / len(acc['excess']), 2)
            if acc['excess'] else None,
            'limit_rate': round(sum(acc['limit_rate']) / len(acc['limit_rate']), 4)
            if acc['limit_rate'] else None,
            'mkt_limit_rate': round(sum(acc['mkt_limit_rate'])
                                    / len(acc['mkt_limit_rate']), 4)
            if acc['mkt_limit_rate'] else None,
        }
    # §4.1：两条基准的滚动汇总（有效交易日数/有效成员数/平均超额/胜率/空池日）
    bench_out: Dict[str, Any] = {'empty_pool_days': bench['empty_pool_days']}
    for key in ('market', 'peer'):
        acc = bench[key]
        if acc['days']:
            bench_out[key] = {
                'days': acc['days'],
                'avg_excess': round(sum(acc['excess']) / len(acc['excess']), 2),
                'win_rate': round(acc['win'] / len(acc['excess']), 2),
                'avg_valid_members': round(acc['valid_members'] / acc['days'], 1),
            }
    if not any(k in bench_out for k in ('market', 'peer')) \
            and not bench_out['empty_pool_days']:
        bench_out = None  # 滚动期内无任何 R2 同类基准数据（体系建立前）
    return {'days': len(payloads), 'pools': pools_out,
            'scenario': {k: v for k, v in scen.items() if v['n']},
            'v2': v2 if v2['n'] else None,
            'r2_benchmarks': bench_out}


# ------------------------------------------------------------------ 工具

def _median(values: List[float]) -> Optional[float]:
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return round(s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2, 2)


def _notna(v: Any) -> bool:
    return v == v  # NaN != NaN
