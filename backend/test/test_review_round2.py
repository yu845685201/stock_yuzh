# -*- coding: utf-8 -*-
"""报告优化第二轮测试（每日复盘报告优化技术方案 §4/§6，2026-11-24）。

- D1（§4.1）：R2 同类基准三组中位与两条超额（11-23 样例回归 +6.45/−1.10）；
  空池与缺基准的空值语义（不转零）；首板/二板身份的交易日历连续性核验
  （停牌缺行记未知）；滚动两条基准统计。
- D2（§4.2）：行业×题材代码交集；题材承接昨日分母按昨日成分冻结
  （跨日改标签不改分母）；共享领涨股并集去重（真视通只占一个代码）。
- E1（§4.3）：覆盖五层漏斗 E/O/V/A/U、缺行单主因拆分、意外入库、
  双分母占比；目标清单缺失降级（仅面板自身可证计数）。
- E2（§4.3）：涨停延续固定分组不缩小（翻红严格大于零）；二板晋级
  分母为零时比率为空；量价锚应到日期检查（窗口缺日/占位行 → 待核）。
"""
import json

import pandas as pd

from src.review.cohorts import (first_board_cohort, freeze_cohort,
                                 limit_up_cohort, member_hash,
                                 two_board_cohort)
from src.review.backfills import (freeze_backfill_anchors,
                                  verify_limit_up_continuation,
                                  verify_two_board_promotion,
                                  verify_volume_price)
from src.review.coverage import coverage_facts
from src.review.generator import generator_lede
from src.review.return_metrics import (evaluate_cohort, market_sample,
                                       median_precise, excess_stats)
from src.review.verifier import _verify_cohorts, build_predictions, rolling_stats
from src.analysis.sector_metrics import theme_industry_links


# ------------------------------------------------------------------ 夹具

def _cs(rows, date='20261123'):
    """rows: [(code_suffix, change_rate, limit_up, traded, streak, name)] → 最小截面。

    change_rate 为 None → 涨跌幅无效；traded=False → 停牌标记行。
    """
    df = pd.DataFrame(rows, columns=['suffix', 'change_rate', '_limit_up',
                                     '_traded', '_streak', 'stock_name'])
    df.insert(0, 'ts_code', 'sh.' + df['suffix'].astype(str))
    df.insert(1, 'trade_date', date)
    df['stock_code'] = df['suffix']
    df['_limit_up'] = df['_limit_up'].astype(bool)
    df['_traded'] = df['_traded'].astype(bool)
    df['_streak'] = df['_streak'].astype(int)
    df['_limit_down'] = False
    df['_burst'] = False
    df['volume'] = df['_traded'].map({True: 1000, False: 0})
    df['amount'] = 1e8
    return df


class _BackfillReader:
    """base_trade_calendar + trade_index_info 替身（backfills 两个查询）。"""

    def __init__(self, calendar, index_rows, verify_rows=None):
        self.calendar = calendar          # ['20261117', ...]（升序）
        self.index_rows = index_rows      # [(date, amount, chg, name)]
        self.verify_rows = verify_rows or []

    def _query(self, sql, params=None):
        s = str(sql)
        if 'base_trade_calendar' in s:
            return [(d,) for d in reversed(self.calendar)]  # SQL ORDER BY DESC
        if 'review_verification_daily' in s:
            return self.verify_rows
        if 'trade_index_info' in s:
            want = {_plain_date(d) for d in (params[1] if params else [])}
            return [r for r in self.index_rows if _plain_date(r[0]) in want]
        return []


def _plain_date(d):
    return str(d).replace('-', '')


# ------------------------------------------------------------------ D1 同类基准

def test_d1_excess_recalc_11_23_sample():
    """D1 回归（11-23 样例数字）：R2 两只中位 +8.86%、全市场 +2.41%、
    昨日首板 26 只中位 +9.96% → 两条超额 **+6.45 / −1.10 个百分点**。"""
    rows = []
    # 40 只填充股 @2.41（市场分母主体）
    for i in range(40):
        rows.append((f'F{i:03d}', 2.41, False, True, 0, f'填充{i}'))
    # 26 只昨日可确认首板：13 只 9.90 + 13 只 10.02 → 中位 9.96
    for i in range(13):
        rows.append((f'B{i:03d}', 9.90, True, True, 1, f'首板{i}'))
    for i in range(13):
        rows.append((f'B{i + 13:03d}', 10.02, True, True, 1, f'首板{i + 13}'))
    # R2 两只：5.00 / 12.72 → 中位 8.86
    rows.append(('600002', 5.00, True, True, 1, '慈文传媒'))
    rows.append(('600007', 12.72, True, True, 1, '高德红外'))
    cs = _cs(rows)
    payload = {
        'R2': freeze_cohort('R2_published', '20261120', 'R2 发布名单',
                            ['sh.600002', 'sh.600007']),
        'first_board': freeze_cohort('first_board', '20261120', 'T 可确认首板',
                                     [f'sh.B{i:03d}' for i in range(26)]),
    }
    out = _verify_cohorts(payload, cs)
    r2 = out['R2']
    assert r2['candidate']['n_total'] == 2 and r2['candidate']['n_valid'] == 2
    assert r2['candidate']['median'] == 8.86
    assert r2['market']['n'] == 68 and r2['market']['median'] == 2.41
    assert r2['peer']['n_total'] == 26 and r2['peer']['median'] == 9.96
    assert r2['excess_vs_market'] == 6.45
    assert r2['excess_vs_peer'] == -1.10


def test_d1_median_precise_even_samples():
    """中位数：偶数样本取中间两项均值（内部保留精度），空 → None。"""
    assert median_precise([5.00, 12.72]) == 8.86
    assert median_precise([3.0]) == 3.0
    assert median_precise([]) is None
    assert median_precise([1.0, 2.0, 100.0, 4.0]) == 3.0


def test_d1_null_semantics_empty_pool_and_missing_benchmark():
    """D1 空值规范：空池（n_total=0）→ 两条超额均 None；同类基准无有效
    样本 → 仅 excess_vs_peer None（不转零）。"""
    rows = [('F001', 2.0, False, True, 0, '甲'), ('F002', -1.0, False, True, 0, '乙')]
    cs = _cs(rows)
    empty = freeze_cohort('R2_published', '20261120', 'R2 发布名单', [])
    peer = freeze_cohort('first_board', '20261120', 'T 可确认首板', ['sh.X999'])
    out = _verify_cohorts({'R2': empty, 'first_board': peer}, cs)['R2']
    assert out['candidate']['n_total'] == 0 and out['candidate']['n_valid'] == 0
    assert out['excess_vs_market'] is None and out['excess_vs_peer'] is None
    # 同类基准全部缺行（X999 无行）→ 仅对首板的超额留空，对全市场仍可算：
    # 市场中位 = (2.0 + −1.0)/2 = 0.5 → 超额 2.0 − 0.5 = 1.5
    r2c = freeze_cohort('R2_published', '20261120', 'R2', ['sh.F001'])
    out2 = _verify_cohorts({'R2': r2c, 'first_board': peer}, cs)['R2']
    assert out2['peer']['n_valid'] == 0
    assert out2['excess_vs_peer'] is None
    assert out2['excess_vs_market'] == 1.5


def test_d1_evaluate_cohort_missing_reasons():
    """cohort × N 日截面左连接：缺行/无成交/涨跌幅无效互斥分桶并保留数量。"""
    rows = [
        ('A001', 1.0, True, True, 1, '甲'),      # ok
        ('A002', None, False, True, 0, '乙'),     # 涨跌幅无效（有成交）
        ('A003', 0.0, False, False, 0, '丙'),     # 无成交（停牌标记行）
        # A004 无行 → 缺行
    ]
    cs = _cs(rows)
    ev = evaluate_cohort(['sh.A001', 'sh.A002', 'sh.A003', 'sh.A004'], cs)
    assert ev['n_total'] == 4 and ev['n_valid'] == 1
    assert ev['invalid'] == {'缺行': 1, '无成交': 1, '涨跌幅无效': 1}
    assert ev['member_status'] == {'sh.A001': 'ok', 'sh.A002': '涨跌幅无效',
                                   'sh.A003': '无成交', 'sh.A004': '缺行'}


def test_d1_first_board_calendar_continuity():
    """首板身份按日历相邻核验：A 前一交易日有行且未涨停 → 可确认；
    B 前一交易日无行（停牌缺采集）→ 未知；C 前日涨停 → ≥2 板排除。"""
    cs_today = _cs([('A', 10.0, True, True, 1, '甲'),
                    ('B', 10.0, True, True, 1, '乙'),
                    ('C', 10.0, True, True, 2, '丙'),
                    ('D', 10.0, True, True, 2, '丁'),
                    ('E', 10.0, True, True, 2, '戊')])
    cs_prev = _cs([('A', 1.0, False, True, 0, '甲'),
                   ('C', 10.0, True, True, 1, '丙'),
                   ('D', 10.0, True, True, 1, '丁')], date='20261120')
    fb = first_board_cohort(cs_today, cs_prev, '20261123')
    assert fb['ts_codes'] == ['sh.A']
    # 首板身份未知：B 与 E（前一日无行——B 可能复牌续板、E 可能复牌连板）
    assert fb['n_unknown'] == 2 and set(fb['unknown']) == {'sh.B', 'sh.E'}
    # 二板：C 与 D 两根涨停行均日历相邻（T-1 涨停 + T 涨停）→ 可确认；
    # E streak=2 但前一日无行（复牌连板）→ 未知，不入可判定分母
    tb = two_board_cohort(cs_today, cs_prev, '20261123')
    assert tb['ts_codes'] == ['sh.C', 'sh.D']
    assert tb['n_unknown'] == 1 and tb['unknown'] == ['sh.E']


def test_d1_r2_cohort_from_publication_and_hash():
    """R2 cohort 由发布名单冻结（空池语义）；member_hash 可复算一致。"""
    from src.review.candidate_publication import build_candidate_publication
    pub = build_candidate_publication(
        {'available': True, 'rules': {'top_n': 5},
         'R1': [], 'R2': [{'ts_code': 'sh.600002', 'code': '600002',
                           'name': '慈文传媒'}], 'R3': [], 'R4': [],
         'R1_filtered': [], 'vetoed': [], 'vetoed_count': 0,
         'adx_gate': {'gate': 'full', 'market_weak': False}},
        {'data': {'cycle': {'position': '回暖/震荡'}}}, {})
    from src.review.cohorts import r2_published_cohort
    r2 = r2_published_cohort(pub, '20261123')
    assert r2['ts_codes'] == ['sh.600002']
    assert r2['member_hash'] == member_hash(['sh.600002'])
    assert r2['n_total'] == 1


def test_d1_rolling_benchmarks_two_baselines():
    """滚动：两条基准各按可算日累计（天数/平均超额/胜率/平均有效成员数），
    空池日单列不计分母。"""
    def _v(ex_mkt, ex_peer, n_total=2, n_valid=2):
        return {'candidates': {}, 'cohorts': {'R2': {
            'candidate': {'n_total': n_total, 'n_valid': n_valid},
            'excess_vs_market': ex_mkt, 'excess_vs_peer': ex_peer}}}
    hist = [_v(6.45, -1.10), _v(-2.0, None),
            _v(None, 0.5, n_total=0, n_valid=0)]
    current = _v(1.0, 2.0)
    reader = _BackfillReader([], [], verify_rows=[(p,) for p in hist])
    rolling = rolling_stats('20261124', current, reader)
    bench = rolling['r2_benchmarks']
    # 市场基准：current(1.0) + hist0(6.45) + hist1(-2.0) = 3 个有效日；
    # 空池日（hist2 n_total=0）不计任何基准分母
    assert bench['market']['days'] == 3
    assert bench['market']['avg_excess'] == round((1.0 + 6.45 - 2.0) / 3, 2)
    # 同类基准：current(2.0) + hist0(-1.10) = 2 个有效日（hist1 peer=None
    # 不计；hist2 空池不计）
    assert bench['peer']['days'] == 2
    assert bench['peer']['win_rate'] == 0.5
    assert bench['peer']['avg_valid_members'] == 2.0
    assert bench['empty_pool_days'] == 1


def test_d1_build_predictions_freezes_cohorts_and_backfills():
    """预测 payload 冻结 cohorts（R2/first_board）与 backfills（涨停/二板/量价锚）。"""
    vp_anchor = {'code': '000985', 'anchor_date': '20261123',
                 'chg_pct': 1.43, 'ratio_5d': 1.10, 'status': 'ok',
                 'window_dates': [], 'window_amounts': []}
    agg = {
        'sentiment': {'data': {'cycle': {'position': '回暖/震荡'}}},
        'market_env': {'data': {}}, 'volume_price': {'data': {}},
        'movers': {'data': {}}, 'candidates': {'R1': [], 'R2': [], 'R3': [],
                                                'R4': []},
        'cohorts': {
            'limit_up': freeze_cohort('limit_up', '20261123', 'T 涨停',
                                       ['sh.A', 'sh.B']),
            'first_board': freeze_cohort('first_board', '20261123', 'T 首板',
                                        ['sh.A']),
            'two_board': freeze_cohort('two_board', '20261123', 'T 二板', []),
            'r2_published': freeze_cohort('R2_published', '20261123', 'R2', []),
        },
        'backfill_anchors': {'volume_price': vp_anchor},
    }
    pred = build_predictions(agg, {})
    assert pred['cohorts']['R2']['n_total'] == 0
    assert pred['cohorts']['first_board']['ts_codes'] == ['sh.A']
    assert pred['backfills']['limit_up']['n_total'] == 2
    assert pred['backfills']['two_board']['n_total'] == 0
    assert pred['backfills']['volume_price']['ratio_5d'] == 1.10
    assert pred['backfills']['volume_price']['anchor_date'] == '20261123'


# ------------------------------------------------------------------ D2 题材交集

def _d2_fixture():
    """11-23 样例形态：专精特新 14 首板（昨日 1 晋级 0）、重组 2/2、
    军工 1/1；真视通同时带重组、军工标签。"""
    rows = []
    # 专精特新 14 只今日涨停（全部首板：前日行未涨停）
    for i in range(14):
        rows.append((f'Z{i:03d}', 10.0, True, True, 1, f'专精{i}'))
    # 专精特新昨日涨停 1 只今日断板（Z_prev，不在今日成分名单中会怎样——
    # 昨日成分关系冻结：它在昨日属于专精特新并涨停）
    rows.append(('ZP01', -2.0, False, True, 0, '专精昨日'))
    # 重组：昨日 2 只涨停，今日均晋级
    rows.append(('C01', 10.0, True, True, 2, '重组甲'))
    rows.append(('C02', 10.0, True, True, 2, '重组乙'))
    # 军工：昨日 1 只涨停今日晋级；真视通同时带重组、军工标签
    rows.append(('M01', 10.0, True, True, 2, '军工甲'))
    rows.append(('S01', 10.0, True, True, 1, '真视通'))
    # 成分外填充
    rows.append(('F001', 1.0, False, True, 0, '填充'))
    cs_today = _cs(rows)
    prev_rows = [('ZP01', 10.0, True, True, 1, '专精昨日'),
                 ('C01', 10.0, True, True, 1, '重组甲'),
                 ('C02', 10.0, True, True, 1, '重组乙'),
                 ('M01', 10.0, True, True, 1, '军工甲'),
                 ('S01', 10.0, True, True, 1, '真视通')]
    # 14 只专精特新前日均未涨停（当日全部可确认首板）
    prev_rows += [(f'Z{i:03d}', 1.0, False, True, 0, f'专精{i}')
                  for i in range(14)]
    cs_prev = _cs(prev_rows, date='20261120')
    concept_today = {
        f'sh.Z{i:03d}': ['专精特新'] for i in range(14)}
    concept_today['sh.ZP01'] = ['专精特新']
    concept_today['sh.C01'] = ['重组']
    concept_today['sh.C02'] = ['重组']
    concept_today['sh.M01'] = ['军工']
    concept_today['sh.S01'] = ['重组', '军工']          # 真视通双标签
    concept_prev = {                                   # 昨日成分关系（冻结分母）
        'sh.ZP01': ['专精特新'],
        'sh.C01': ['重组'],
        'sh.C02': ['重组'],
        'sh.M01': ['军工'],
        'sh.S01': ['军工'],                             # 昨日仅军工标签
    }
    clusters = [{'concept': '专精特新', 'count': 14, 'share': 0.09},
                {'concept': '重组', 'count': 3, 'share': 0.2},
                {'concept': '军工', 'count': 2, 'share': 0.1}]
    return cs_today, cs_prev, concept_today, concept_prev, clusters


def test_d2_theme_links_union_dedup_and_promotion():
    """D2：题材承接行 + 共享领涨股代码去重；昨日分母按昨日成分冻结。"""
    cs_today, cs_prev, c_today, c_prev, clusters = _d2_fixture()
    links = theme_industry_links(cs_today, cs_prev, {}, c_today, c_prev,
                                 clusters, ['国防军工'])
    assert links is not None
    themes = {t['concept']: t for t in links['themes']}
    # 专精特新：14 家涨停全部首板（新增首板=14）；昨日 1 家今日晋级 0
    zt = themes['专精特新']
    assert zt['limit_up_n'] == 14 and zt['new_first_board_n'] == 14
    assert zt['limit_rate'] == round(14 / 15, 4)      # 可交易 15（含 ZP01）
    assert zt['prev_up_frozen_n'] == 1 and zt['promoted'] == 0
    # 重组：昨日 2 家今日晋级 2 家；军工昨日 1 晋级 1（真视通昨日不在重组
    # 分母中——昨日标签只有军工，跨日改标签不改昨日分母）
    assert themes['重组']['prev_up_frozen_n'] == 2
    assert themes['重组']['promoted'] == 2
    assert themes['军工']['prev_up_frozen_n'] == 2   # M01 + S01（昨日军工标签）
    assert themes['军工']['promoted'] == 2
    # 跨题材并集去重：真视通在重组+军工各计 1 家次，并集只占一个代码
    # （重组 3 = C01/C02/真视通；军工 2 = M01/真视通 → 并集 = 14 + 3 + 1 = 18）
    assert links['union']['n_sum'] == 14 + 3 + 2
    assert links['union']['n_union'] == 14 + 3 + 1
    shared_names = [s['name'] for s in links['union']['shared']]
    assert shared_names == ['真视通']
    assert links['union']['shared'][0]['concepts'] == ['重组', '军工']


def test_d2_theme_links_unknown_and_mapping_coverage():
    """D2：无行情行成员记未知（不入涨停占比分母）；映射覆盖不足随行标注。"""
    rows = [
        ('A001', 10.0, True, True, 1, '甲'),           # 专精特新成分，涨停
        ('A002', None, False, False, 0, '乙'),          # 成分但停牌标记行 → 未知
        # A003 成分但整日无行 → 未知
        ('X001', 10.0, True, True, 1, '无概念涨停'),   # 涨停但无概念映射
    ]
    cs_today = _cs(rows)
    cs_prev = _cs([], date='20261120')
    c_today = {'sh.A001': ['专精特新'], 'sh.A002': ['专精特新'],
               'sh.A003': ['专精特新']}
    clusters = [{'concept': '专精特新', 'count': 1, 'share': 0.05}]
    links = theme_industry_links(cs_today, cs_prev, {}, c_today, {}, clusters, [])
    t = links['themes'][0]
    assert t['tradable_n'] == 1 and t['unknown_n'] == 2
    assert t['limit_rate'] == 1.0                      # 分母只含可判定成分
    mapping = links['mapping_coverage']
    assert mapping['limit_up_total'] == 2 and mapping['with_concept'] == 1


def test_d2_prev_membership_frozen_against_retagging():
    """D2 核心断言：昨日涨停股今日改标签 → 计入**昨日所属**题材的分母、
    不计入新标签题材的昨日分母（跨日改标签不改昨日分母）。"""
    rows = [('M01', 10.0, True, True, 2, '军工甲'),    # 昨日军工 → 今日仍军工
            ('S01', 10.0, True, True, 2, '真视通')]    # 昨日军工 → 今日改重组
    cs_today = _cs(rows)
    cs_prev = _cs([('M01', 10.0, True, True, 1, '军工甲'),
                   ('S01', 10.0, True, True, 1, '真视通')], date='20261120')
    c_today = {'sh.M01': ['军工'], 'sh.S01': ['重组']}
    c_prev = {'sh.M01': ['军工'], 'sh.S01': ['军工']}
    clusters = [{'concept': '重组', 'count': 1, 'share': 0.1},
                {'concept': '军工', 'count': 1, 'share': 0.1}]
    links = theme_industry_links(cs_today, cs_prev, {}, c_today, c_prev,
                                 clusters, [])
    themes = {t['concept']: t for t in links['themes']}
    # 真视通昨日属军工 → 军工昨日分母 = 2；重组昨日分母 = 0
    assert themes['军工']['prev_up_frozen_n'] == 2
    assert themes['军工']['promoted'] == 2
    assert themes['重组']['prev_up_frozen_n'] == 0
    assert themes['重组']['promoted'] == 0


# ------------------------------------------------------------------ E1 覆盖

def _write_manifests(tmp_path, as_of, scope_codes, gap=None):
    (tmp_path / f'kline_day_scope_{as_of}.json').write_text(json.dumps({
        'codes_total': len(scope_codes),
        'codes_by_market': {'sh': scope_codes},
    }), encoding='utf-8')
    if gap is not None:
        (tmp_path / f'kline_day_gap_{as_of}.json').write_text(
            json.dumps(gap), encoding='utf-8')


def _e1_cs():
    """E=10 只：6 入库（1 无成交、1 涨跌无效）、4 缺行；另有 1 只意外入库。"""
    rows = [
        ('600001', 1.0, False, True, 0, '甲'),        # A ∩ U
        ('600002', 2.0, False, True, 0, '乙'),         # U
        ('600003', 0.5, False, True, 0, '丙'),         # U
        ('600004', -1.0, False, True, 0, '丁'),        # A 但不涨
        ('600005', None, False, True, 0, '戊'),        # V−A（成交但涨跌无效）
        ('600006', 3.0, False, False, 0, '己'),        # O−V（有行无成交）
        ('600011', 4.0, False, True, 0, '意外'),       # 清单外意外入库
    ]
    return _cs(rows)


def test_e1_coverage_facts_and_reasons(tmp_path, monkeypatch):
    """E1：五层漏斗、缺行单主因（一个代码只归一个有证据的主因）、
    双分母占比与意外入库。"""
    from src.review import coverage as cov_mod
    monkeypatch.setattr(cov_mod, '_repo_tmp_dir', lambda: tmp_path)
    e_codes = [f'sh.60000{i}' for i in range(1, 11)]    # sh.600001..sh.600010
    _write_manifests(tmp_path, '20261123', e_codes, gap={
        'failed': ['sh.600007', 'sh.600008', 'sh.600009'],   # 600009 同时在未提交
        'cancelled': [], 'not_submitted': ['sh.600009'],
        'missing_on_target': ['sh.600007', 'sh.600008',
                              'sh.600009', 'sh.600010'],
    })
    cov = coverage_facts('20261123', _e1_cs())
    assert cov['available'] and cov['n_expected'] == 10
    assert cov['n_rows'] == 6 and cov['n_traded'] == 5 \
        and cov['n_valid_change'] == 4 and cov['n_up'] == 3
    assert cov['n_missing'] == 4
    assert cov['n_no_volume'] == 1 and cov['n_invalid_change'] == 1
    assert cov['n_unexpected_rows'] == 1
    assert cov['up_ratio_valid'] == 0.75 and cov['up_ratio_scope'] == 0.3
    # 缺行主因：600007/600008/600009 → 采集失败（优先级高于未提交）；
    # 600010 → 原因待核；一个代码只归一个主因
    assert cov['missing_reasons']['采集失败'] == 3
    assert cov['missing_reasons']['原因待核（停牌等有证据豁免暂无证据源）'] == 1
    assert sum(cov['missing_reasons'].values()) == cov['n_missing']


def test_e1_coverage_no_scope_degrades(tmp_path, monkeypatch):
    """E1：目标清单缺失 → 覆盖降级为面板自身可证计数，E 口径比例不可算
    （面板全集含清单外股票——仅显示自身可证的计数）。"""
    from src.review import coverage as cov_mod
    monkeypatch.setattr(cov_mod, '_repo_tmp_dir', lambda: tmp_path)
    cov = coverage_facts('20261119', _e1_cs())
    assert cov['available'] is False
    assert cov['n_rows'] == 7 and cov['n_traded'] == 6
    assert cov['n_valid_change'] == 5 and cov['n_up'] == 4   # 含意外入库 600011
    assert cov['up_ratio_valid'] == 0.8
    assert cov['up_ratio_scope'] is None and cov['n_missing'] is None


# ------------------------------------------------------------------ E2 回填

def test_e2_limit_up_continuation_fixed_group():
    """E2 验点 1：固定分组不缩小（缺行保留在分母说明中）；翻红严格大于零。"""
    rows = [
        ('A001', 3.0, True, True, 1, '甲'),
        ('A002', 0.0, False, True, 0, '乙'),           # 平盘：不算翻红
        ('A003', -2.0, False, True, 0, '丙'),
        # A004 缺行
    ]
    cs = _cs(rows)
    cohort = freeze_cohort('limit_up', '20261123', 'T 涨停',
                           ['sh.A001', 'sh.A002', 'sh.A003', 'sh.A004'])
    out = verify_limit_up_continuation(cohort, cs)
    assert out['n_total'] == 4 and out['n_valid'] == 3   # 分组不缩小
    assert out['red_n'] == 1                              # 0.0 不算翻红
    assert out['median'] == 0.0
    assert out['invalid']['缺行'] == 1
    assert out['member_hash_match'] is True


def test_e2_two_board_promotion_zero_denominator():
    """E2 验点 2：晋级数/可判定数/不可判定数；分母为零时比率为空。"""
    rows = [('A001', 10.0, True, True, 3, '甲'),
            ('A002', 1.0, False, True, 2, '乙'),
            # A003 缺行 → 不可判定
            ]
    cs = _cs(rows)
    cohort = freeze_cohort('two_board', '20261123', 'T 二板',
                           ['sh.A001', 'sh.A002', 'sh.A003'])
    out = verify_two_board_promotion(cohort, cs)
    assert out['promoted'] == 1 and out['judgeable'] == 2 \
        and out['unjudgeable'] == 1
    assert out['promotion_rate'] == 0.5
    # 全员缺行 → 可判定 0 → 比率为空（不转零）
    out2 = verify_two_board_promotion(cohort, _cs([('X001', 1.0, False, True, 0, 'X')]))
    assert out2['judgeable'] == 0 and out2['promotion_rate'] is None


def _vp_reader(dates, amounts, chgs=None):
    """量价验点替身：dates 升序 6 端点；amounts 同序。"""
    chgs = chgs or [0.1] * 6
    return _BackfillReader(dates, [(d, a, c, '中证全指')
                                   for d, a, c in zip(dates, amounts, chgs)])


def test_e2_volume_price_freeze_and_verify():
    """E2 验点 3：T 冻结（+1.43%、1.10 倍）→ N 回填同公式；六端点齐全。"""
    # T=20261123 窗口六端点 = T 前五个交易日 + T；N=20261124 同理
    dates_t = ['20261116', '20261117', '20261118', '20261119', '20261120',
               '20261123']
    dates_n = ['20261117', '20261118', '20261119', '20261120', '20261123',
               '20261124']
    amounts_t = [100, 100, 100, 100, 100, 110]
    amounts_n = [100, 100, 100, 100, 100, 121]
    chgs_t = [0.1, 0.1, 0.1, 0.1, 0.1, 1.43]
    chgs_n = [0.1, 0.1, 0.1, 0.1, 1.43, 0.12]
    reader_t = _vp_reader(dates_t, amounts_t, chgs_t)
    anchors = freeze_backfill_anchors(reader_t, '20261123')
    anchor = anchors['volume_price']
    assert anchor['status'] == 'ok'
    assert anchor['chg_pct'] == 1.43 and anchor['ratio_5d'] == 1.10
    assert anchor['window_dates'] == dates_t
    # N 日核验：121 / mean(前五日) = 1.21
    reader_n = _vp_reader(dates_n, amounts_n, chgs_n)
    out = verify_volume_price(anchor, reader_n, '20261124')
    assert out['status'] == 'ok'
    assert out['chg_pct'] == 0.12 and out['ratio_5d'] == 1.21
    assert out['anchor']['ratio_5d'] == 1.10


def test_e2_volume_price_missing_window_day_pending():
    """E2：窗口缺日 → 倍数待核（不得用「最近六条非空记录」跳过缺日）；
    N 端点行缺失（盘中占位）→ 待核。"""
    dates = ['20261117', '20261118', '20261119', '20261120', '20261123',
             '20261124']
    # 缺 20261119 的成交额
    rows = [(d, 100, 0.1, '中证全指') for d in dates if d != '20261119']
    reader = _BackfillReader(dates, rows)
    out = freeze_backfill_anchors(reader, '20261124')['volume_price']
    assert out['status'] == '待核' and out['ratio_5d'] is None
    assert '20261119' in (out['reason'] or '')
    # N 日行整体缺失（未收盘/占位行）→ 待核
    reader2 = _BackfillReader(dates, [(d, 100, 0.1, '中证全指')
                                      for d in dates if d != '20261124'])
    out2 = freeze_backfill_anchors(reader2, '20261124')['volume_price']
    assert out2['status'] == '待核' and '20261124' in (out2['reason'] or '')
    # 锚待核传导：N 日即使齐全，结果一并待核
    ok_anchor = {'code': '000985', 'anchor_date': '20261123',
                 'chg_pct': 1.43, 'ratio_5d': 1.10, 'status': '待核',
                 'reason': '窗口缺日', 'window_dates': [], 'window_amounts': []}
    full = _vp_reader(dates, [100] * 6)
    out3 = verify_volume_price(ok_anchor, full, '20261124')
    assert out3['status'] == '待核' and out3['ratio_5d'] is None


# ------------------------------------------------------------------ 首页（§三）

def _home_agg():
    """§三 首页样例形态（11-23 数字）。"""
    return {
        'as_of': '20261123',
        'sentiment': {'data': {
            'cycle': {'position': '回暖/震荡'},
            'max_streak': 5, 'limit_up_count': 153, 'burst_rate': 0.1,
            'premium_mean': 2.0, 'premium_continued': 17,
            'ladder_gap': {'min': 2, 'max': 5, 'gaps': [4]}}},
        'market_env': {'data': {
            'style_spread': {'spread_2000': -3.82},
            'indices': [{'name': '中证2000', 'change_rate': 3.91},
                        {'name': '沪深300', 'change_rate': 0.09}]}},
        'money_effect': {'data': {'feedback': {
            # 「合计」行是分桶行的汇总——首页求昨日涨停总数时须剔除（防双计）
            'rows': [{'bucket': '首板', 'count': 30},
                     {'bucket': '合计', 'count': 30}], 'suspended': 0}}},
        'coverage': {'available': True, 'as_of': '20261123',
                    'n_expected': 6173, 'n_rows': 5561, 'n_traded': 5561,
                    'n_valid_change': 5561, 'n_up': 4651, 'n_missing': 612,
                    'n_no_volume': 0, 'n_invalid_change': 0,
                    'n_unexpected_rows': 0,
                    'up_ratio_valid': 0.836, 'up_ratio_scope': 0.753,
                    'missing_reasons': {'采集失败': 610, '原因待核': 2}},
        'candidate_publication': {
            'open_types': [], 'close_reason': '弱档否决：正式候选全关',
            'pools': {k: {'n_published': 0} for k in ('R1', 'R2', 'R3', 'R4')}},
        'cohorts': {
            'limit_up': {'n_total': 153, 'anchor_date': '20261123'},
            'two_board': {'n_total': 14, 'n_unknown': 0,
                          'anchor_date': '20261123'}},
        'backfill_anchors': {'volume_price': {
            'code': '000985', 'anchor_date': '20261123', 'chg_pct': 1.43,
            'ratio_5d': 1.10, 'status': 'ok'}},
    }


def test_homepage_blocks_sample():
    """§三 首页样例：判断句（小盘领涨的普涨 + 核心读数 + 0 只候选 +
    缺行说明）+ 覆盖注记 + 三项回填表（冻结基准）。"""
    blocks = generator_lede(_home_agg())
    text = '\n'.join(str(b.get('text', '')) for b in blocks)
    assert '小盘领涨的普涨' in text
    assert '83.6% 上涨' in text
    assert '中证 2000 涨 +3.91%，沪深 300 涨 +0.09%' in text
    assert '昨日 30 只涨停股中有 17 只继续涨停' in text
    assert '最高连板为五板，四板空缺' in text
    assert '正式右侧候选为 0 只' in text
    assert '采集缺行 612 只，涨停与题材排名按已采集样本统计' in text
    # 覆盖注记（范围/入库/成交/缺行 + 缺行主因拆分）
    assert '采集范围 6173 只，入库 5561 只，有成交 5561 只，缺行 612 只' in text
    assert '采集失败 610' in text
    # 三项回填表（§三 表格：跟踪问题 | 冻结基准 | 次日回填）
    table = next(b for b in blocks if b.get('type') == 'table'
                 and '跟踪的问题' in (b.get('headers') or []))
    assert len(table['rows']) == 3
    assert '153 只' in table['rows'][0][1]
    assert '14 只可确认二板股' in table['rows'][1][1]
    assert '中证全指当天 +1.43%，成交额为自身近五日均额的 1.10 倍' \
        in table['rows'][2][1]
    assert all('下一交易日收盘后回填' in r[2] for r in table['rows'])


def test_homepage_degrades_without_round2_keys():
    """首页降级：无 coverage/cohorts（旧缓存夹具）→ 退化为情绪定位导语，
    不报错、不出现三项回填表。"""
    agg = {'sentiment': {'data': {'cycle': {'position': '回暖/震荡'},
                                  'max_streak': 5, 'limit_up_count': 153}}}
    blocks = generator_lede(agg)
    text = '\n'.join(str(b.get('text', '')) for b in blocks)
    assert '情绪定位 回暖/震荡' in text
    assert not any('跟踪的问题' in (b.get('headers') or [])
                   for b in blocks if b.get('type') == 'table')


# ------------------------------------------------------------------ 昨日核验渲染

def test_yesterday_review_renders_benchmarks_and_backfills():
    """N 日「昨日核验」：R2 同类基准表 + 三项回填表（§4.1/§4.3 渲染路径）。"""
    from src.review.sections import yesterday_review
    verification = {'available': True, 'data': {
        'prev_date': '20261123',
        'scenario': {'status': '记录', 'triggered': 'P2', 'moves': [],
                     'gap_pct': 1.5, 'completeness': '两锚有效',
                     'scenario_reason': 'x', 'direction_eligible': False},
        'verify_points': [],
        'candidates': {'market_median': 2.41, 'market_limit_rate': 0.03,
                       'pools': {'R2': {'count': 2, 'traded': 2,
                                        'mean': 8.86, 'median': 8.86,
                                        'excess_median': 6.45,
                                        'limit_up_n': 1, 'burst_n': 0,
                                        'down_n': 0, 'rows': []}}},
        'cohorts': {'R2': {
            'anchor_date': '20261123', 'selection_rule': 'R2 发布名单',
            'member_hash': 'abc',
            'candidate': {'n_total': 2, 'n_valid': 2, 'median': 8.86,
                          'invalid': {'缺行': 0, '无成交': 0, '涨跌幅无效': 0}},
            'market': {'n': 5561, 'median': 2.41, 'member_hash': 'def'},
            'peer': {'n_total': 26, 'n_valid': 26, 'median': 9.96,
                     'invalid': {}},
            'peer_unknown_n': 1,
            'excess_vs_market': 6.45, 'excess_vs_peer': -1.10}},
        'backfills': {
            'limit_up': {'anchor_date': '20261123', 'n_total': 153,
                         'n_valid': 150, 'median': 1.2, 'red_n': 80,
                         'invalid': {'缺行': 3, '无成交': 0, '涨跌幅无效': 0},
                         'member_hash_match': True},
            'two_board': {'anchor_date': '20261123', 'n_total': 14,
                          'promoted': 5, 'judgeable': 13, 'unjudgeable': 1,
                          'promotion_rate': 5 / 13, 'n_valid': 13,
                          'median': 2.0, 'red_n': 7,
                          'member_hash_match': True},
            'volume_price': {'code': '000985', 'anchor_date': '20261124',
                             'chg_pct': 0.5, 'ratio_5d': 1.05,
                             'status': 'ok', 'reason': None,
                             'anchor': {'anchor_date': '20261123',
                                        'chg_pct': 1.43, 'ratio_5d': 1.10,
                                        'status': 'ok'}}},
        'rolling': {'days': 5, 'pools': {}, 'r2_benchmarks': {
            'market': {'days': 3, 'avg_excess': 2.0, 'win_rate': 0.67,
                       'avg_valid_members': 2.0},
            'peer': {'days': 2, 'avg_excess': -1.0, 'win_rate': 0.0,
                     'avg_valid_members': 2.5},
            'empty_pool_days': 1}},
    }}
    text = '\n'.join(str(b) for b in yesterday_review.build(verification))
    assert 'R2 同类基准' in text and '+6.45pct' in text and '-1.10pct' in text
    assert '昨日全部可确认首板' in text
    assert '三项次日回填' in text
    assert '二进三 5 / 可判定 13' in text
    assert '翻红 80 / 有效 150' in text
    assert 'R2 对全市场基准：3 个有效交易日' in text
    assert 'R2 空池日 1 天' in text


def test_yesterday_review_empty_pool_benchmark_note():
    """空池日：R2 同类基准段显示「不适用」语义（不转零）。"""
    from src.review.sections import yesterday_review
    verification = {'available': True, 'data': {
        'scenario': {'status': '记录', 'triggered': 'P2', 'moves': []},
        'verify_points': [], 'candidates': {'pools': {}},
        'cohorts': {'R2': {
            'anchor_date': '20261123',
            'candidate': {'n_total': 0, 'n_valid': 0, 'median': None,
                          'invalid': {}},
            'market': {'n': 100, 'median': 1.0, 'member_hash': 'x'},
            'peer': None, 'excess_vs_market': None, 'excess_vs_peer': None}},
        'backfills': {}}}
    text = '\n'.join(str(b) for b in yesterday_review.build(verification))
    assert '同类基准不适用' in text and '不转零' in text
