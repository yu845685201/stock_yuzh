# -*- coding: utf-8 -*-
"""候选发布名单测试（每日复盘报告优化技术方案 §3.3，测试矩阵 C 组）。

C1：11-23 场景（情绪仅开 R1 + 弱档关闭 R1 → 正式右侧候选 0 只；三只 R2
    原始命中归题材跟踪；四只 CHAN 左侧独立）；另测开放且超过展示上限的
    排序/截断；正文 = published、摘要是子集、核验逐股等于首发。
C2：摘要候选段只能由 published 生成——关闭池的股票不得以候选身份出现在
    正文/摘要/预测 payload 任何一处（三端同源断言）。
"""
from src.review.candidate_publication import (build_candidate_publication,
                                              published_flat,
                                              publication_summary)
from src.review.sections import tomorrow_plan
from src.review.verifier import build_predictions


# ------------------------------------------------------------------ 夹具

def _r2_raw(n=3):
    """R2 原始命中（11-23 案例三只：高德红外/慈文传媒/埃斯顿形态）"""
    names = [('sz.002414', '002414', '高德红外'),
             ('sz.002343', '002343', '慈文传媒'),
             ('sz.002747', '002747', '埃斯顿')]
    return [{'ts_code': t, 'code': c, 'name': nm, 'streak': 1,
             'concepts': ['军工', '重组'], 'hit_concept': '军工',
             'cluster_count': 14, 'amount': 5e8, 'big_net': 1e7,
             'evidence': ['资金', '聚集'], 'evidence_count': 2,
             'score': 60.0, 'veto': [], 'label': '首板/军工(聚集14家)'}
            for t, c, nm in names[:n]]


def _cand(r2=None, r1=None, rules=None):
    return {'available': True, 'rules': rules or {'top_n': 5},
            'R1': r1 or [], 'R2': r2 or [], 'R3': [], 'R4': [],
            'R1_filtered': [], 'vetoed': [], 'vetoed_count': 0,
            'adx_gate': {}}


def _sentiment(position):
    return {'data': {'cycle': {'position': position},
                     'limit_up_count': 153, 'max_streak': 5}}


_ADX_FULL = {'gate': 'full', 'market_weak': False}
_ADX_WEAK = {'gate': 'full', 'market_weak': True, 'ma60': 3950.0, 'close': 3900.0}
_ADX_CLOSED = {'gate': 'closed', 'market_weak': False}


# ------------------------------------------------------------------ C1

def test_c1_20261123_all_closed_zero_published():
    """C1（11-23 复现）：情绪高潮仅开 R1 + 弱档否决 R1 → 正式右侧候选
    0 只；三只 R2 原始命中归题材跟踪（不附交易条件）。"""
    cand = _cand(_r2_raw(3))
    cand['adx_gate'] = _ADX_WEAK
    pub = build_candidate_publication(cand, _sentiment('高潮'), {})
    assert pub['open_types'] == []
    for ctype in ('R1', 'R2', 'R3', 'R4'):
        assert pub['pools'][ctype]['n_published'] == 0
        assert not pub['pools'][ctype]['open']
    # 三只 R2 原始命中 → 题材跟踪
    assert len(pub['theme_watch']) == 3
    assert {w['name'] for w in pub['theme_watch']} == {'高德红外', '慈文传媒', '埃斯顿'}
    assert all('非正式候选' in w['watch_reason'] for w in pub['theme_watch'])
    # flat 发布名单为空（预测落库/次日核验无正式成员）
    assert published_flat(pub) == []


def test_c1_chan_independent_freeze():
    """C1：CHAN 左侧名单独立冻结——右侧全关时仍按左侧定义展示。"""
    chan_items = [{'ts_code': f'sh.60000{i}', 'code': f'60000{i}',
                   'name': f'缠{i}', 'label': '一买'} for i in (1, 2, 3, 4)]
    cand = _cand(_r2_raw(3))
    cand['adx_gate'] = _ADX_WEAK
    pub = build_candidate_publication(cand, _sentiment('高潮'), {},
                                      chan_items=chan_items)
    assert pub['chan']['n_published'] == 4
    assert [c['name'] for c in pub['chan']['published']] == ['缠1', '缠2', '缠3', '缠4']


def test_c1_open_pool_rank_and_truncation():
    """C1：开放池排序（五维评分降序、证据计数兜底）+ top_n 截断（截断
    逐股落点记录，禁静默缩池）；正文候选表逐项等于 published。"""
    pool = []
    for i in range(7):
        pool.append({'ts_code': f'sz.30000{i}', 'code': f'30000{i}',
                     'name': f'股{i}', 'streak': 1, 'concepts': ['AI'],
                     'amount': 5e8, 'evidence': ['资金'],
                     'evidence_count': 1, 'score': 100.0 - i * 10,
                     'veto': [], 'label': '首板/AI'})
    # 并列 score 的两只：证据计数决定次序
    pool[5]['score'] = pool[4]['score'] = 55.0
    pool[5]['evidence_count'] = 3
    pool[4]['evidence_count'] = 1
    cand = _cand(pool)
    cand['adx_gate'] = _ADX_FULL
    pub = build_candidate_publication(cand, _sentiment('回暖/震荡'), {})
    st = pub['pools']['R2']
    assert st['open'] and st['n_eligible'] == 7
    assert st['n_published'] == 5
    assert [r['name'] for r in st['published']] == ['股0', '股1', '股2', '股3', '股5']
    assert len(st['truncated']) == 2
    assert all('排序截断' in t['reason'] for t in st['truncated'])
    # 正文渲染 = published（§3.3：读者看到的名单与次日评价的名单一致）
    agg = {'candidates': cand, 'sentiment': _sentiment('回暖/震荡'),
           'candidate_publication': pub,
           'verification': {}, 'market_env': {'data': {}}, 'movers': {'data': {}}}
    blocks = tomorrow_plan._buy_candidates(agg, {})
    tables = [b for b in blocks if b.get('type') == 'table']
    cand_table = next(t for t in tables if t['headers'][0] == '代码')
    assert [row[1] for row in cand_table['rows']] == \
        [r['name'] for r in st['published']]


def test_c1_body_text_matches_publication():
    """C1：11-23 场景正文渲染——「正式右侧候选 0 只」+ 题材跟踪段单列。"""
    cand = _cand(_r2_raw(3))
    cand['adx_gate'] = _ADX_WEAK
    pub = build_candidate_publication(cand, _sentiment('高潮'), {})
    agg = {'candidates': cand, 'sentiment': _sentiment('高潮'),
           'candidate_publication': pub,
           'verification': {}, 'market_env': {'data': {}}, 'movers': {'data': {}}}
    blocks = tomorrow_plan._buy_candidates(agg, {})
    texts = [str(b.get('text', '')) for b in blocks]
    heads = [str(b.get('text', '')) for b in blocks if b.get('type') == 'heading']
    assert any('正式右侧候选 0 只' in t for t in texts)
    assert any('题材跟踪（3 只，非正式候选）' in h for h in heads)
    # 三只题材样本出现在题材跟踪表（唯一出处），无买入决策树随行
    table = next(b for b in blocks if b.get('type') == 'table')
    assert {row[1] for row in table['rows']} == {'高德红外', '慈文传媒', '埃斯顿'}
    bullets = [i for b in blocks if b.get('type') == 'bullets'
               for i in b.get('items') or []]
    assert not any('确认买入' in i for i in bullets)   # 无交易条件


# ------------------------------------------------------------------ C2 三端同源

def _full_agg(position='高潮', weak=True):
    cand = _cand(_r2_raw(3))
    cand['adx_gate'] = _ADX_WEAK if weak else _ADX_FULL
    return {
        'as_of': '20261123', 'prev_date': '20261120',
        'candidates': cand, 'sentiment': _sentiment(position),
        'candidate_publication': build_candidate_publication(
            cand, _sentiment(position), {}),
        'market_env': {'data': {'turnover': {}, 'overseas_indices': []}},
        'volume_price': {'data': {}}, 'movers': {'data': {}},
        'verification': {},
    }


def test_c2_predictions_read_published_not_raw():
    """C2/C1：预测落库读发布名单——正文关闭的池（R2 3 只原始命中）不进入
    payload candidates；发布批次摘要随行供回放对账。"""
    agg = _full_agg('高潮', weak=True)
    pred = build_predictions(agg, {'tomorrow_plan': {}})
    assert pred['candidates']['R2'] == []            # 关闭池不落库为正式成员
    assert pred['candidates']['R1'] == []
    meta = pred['candidate_publication']
    assert meta['pools']['R2'] == {'open': False, 'n_raw': 3, 'n_eligible': 0,
                                   'n_published': 0,
                                   'close_reason': '高潮仅开放 R1',
                                   'n_truncated': 0}
    assert meta['theme_watch_n'] == 3


def test_c2_predictions_published_when_open():
    """C1：开放日预测落库 = published（排序截取后），研究样本计数随行。"""
    pool = [{'ts_code': f'sz.30000{i}', 'code': f'30000{i}', 'name': f'股{i}',
             'evidence_count': 1, 'score': 90.0 - i, 'veto': []}
            for i in range(7)]
    cand = _cand(pool)
    cand['adx_gate'] = _ADX_FULL
    agg = _full_agg('回暖/震荡', weak=False)
    agg['candidates'] = cand
    agg['candidate_publication'] = build_candidate_publication(
        cand, _sentiment('回暖/震荡'), {})
    pred = build_predictions(agg, {'tomorrow_plan': {}})
    assert [r['name'] for r in pred['candidates']['R2']] == \
        [f'股{i}' for i in range(5)]                  # published（非 raw 7 只）
    assert pred['candidate_publication']['pools']['R2']['n_raw'] == 7
    assert pred['candidate_publication']['pools']['R2']['n_published'] == 5


def test_c2_digest_candidates_subset_of_published():
    """C2：摘要候选段由 published 生成（结构化渲染，代码集合 ⊆ published）；
    关闭池的股票只出现在题材跟踪段，不进入候选表。"""
    from src.review.digest import _render_candidate_digest
    agg = _full_agg('高潮', weak=True)
    html = _render_candidate_digest(agg)
    assert '正式右侧候选 <strong>0 只</strong>' in html
    assert '题材跟踪（3 只，非正式候选）' in html
    # 三只题材样本仅在题材跟踪表——不作为候选出现
    for name in ('高德红外', '慈文传媒', '埃斯顿'):
        assert html.count(name) == 1
    # 开放日：摘要候选表 ⊆ published
    pool = [{'ts_code': f'sz.30000{i}', 'code': f'30000{i}', 'name': f'股{i}',
             'label': '首板/AI', 'evidence': ['资金'], 'evidence_count': 1,
             'score': 90.0 - i, 'veto': []}
            for i in range(3)]
    cand = _cand(pool)
    cand['adx_gate'] = _ADX_FULL
    agg2 = _full_agg('回暖/震荡', weak=False)
    agg2['candidates'] = cand
    agg2['candidate_publication'] = build_candidate_publication(
        cand, _sentiment('回暖/震荡'), {})
    html2 = _render_candidate_digest(agg2)
    published_names = {r['name'] for r in agg2['candidate_publication']
                       ['pools']['R2']['published']}
    shown = {n for n in published_names if n in html2}
    assert shown == published_names                     # 摘要=发布名单（全集）
    assert '题材跟踪' not in html2                      # R2 开放 → 无题材跟踪段


def test_c2_close_reasons_by_scenario():
    """开放状态枚举：冰点/退潮/异常定位/ADX closed 各自的关闭原因与文案。"""
    for position, expect_bit in (('冰点', '冰点'), ('退潮', '退潮'),
                                 ('异常态', '异常')):
        cand = _cand(_r2_raw())
        cand['adx_gate'] = _ADX_FULL
        pub = build_candidate_publication(cand, _sentiment(position), {})
        assert pub['open_types'] == []
        assert expect_bit in pub['close_reason']
    cand = _cand(_r2_raw())
    cand['adx_gate'] = _ADX_CLOSED
    pub = build_candidate_publication(cand, _sentiment('回暖/震荡'), {})
    assert pub['open_types'] == []
    assert 'closed' in pub['close_reason']
    assert pub['pools']['R2']['close_reason'] == '指数 ADX 闸门 closed：全关'
    # ADX half：R3/R4 关闭但 R2 开放
    cand['adx_gate'] = {'gate': 'half', 'market_weak': False}
    pub = build_candidate_publication(cand, _sentiment('回暖/震荡'), {})
    assert pub['open_types'] == ['R1', 'R2']
    assert not pub['pools']['R3']['open']
    assert pub['pools']['R3']['close_reason'] == 'ADX 半开放（仅 R1/R2）'


def test_c2_summary_counts_replay_consistent():
    """发布批次摘要计数与各阶段名单一致（回放对账底表）。"""
    cand = _cand(_r2_raw(3))
    cand['adx_gate'] = _ADX_WEAK
    pub = build_candidate_publication(cand, _sentiment('高潮'), {})
    s = publication_summary(pub)
    assert s['position'] == '高潮'
    assert s['open_types'] == []
    assert s['pools']['R2']['n_raw'] == 3
    assert s['theme_watch_n'] == 3
    assert s['vetoed_count'] == 0
    assert s['top_n'] == 5
