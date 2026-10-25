"""calibration_stats 免库单测（V4.0 A3/A4）：合成帧 + 确定性数值断言。

覆盖：breadth 序列（inf/NaN 口径）、分布（已知合成分布的分位/占比精确值）、
分桶次日结果（缺日剔除计数）、阈值选项（P90 与固定档并列）、A50 as-of 对齐
（≤D−1 规则/前向填充）、同向率（零值剔除）、触发可用性（越阈频度与 P1/P3
命中计数）、A5 准入门读数（样本不足不给 pass）。
"""
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

import pandas as pd  # noqa: E402

from src.analysis import calibration_stats as cals  # noqa: E402


def _m(rows):
    cols = ['trade_date', 'up_count', 'down_count', 'limit_up_count',
            'max_streak', 'premium_mean', 'burst_rate']
    return pd.DataFrame(rows, columns=cols)


def _sh(rows):
    return pd.DataFrame(rows, columns=['trade_date', 'open', 'preclose', 'close'])


# ---------------------------------------------------------------- A3 breadth

class TestBreadthSeries:
    def test_zero_down_inf_and_missing_nan(self):
        m = _m([('20260101', 100, 50, 10, 3, 2.0, 0.2),
                ('20260102', 100, 0, 10, 3, 2.0, 0.2),
                ('20260103', None, 50, 10, 3, 2.0, 0.2),
                ('20260104', 60, 30, 10, 3, 2.0, 0.2)])
        br = cals.breadth_series(m)
        assert br.iloc[0] == 2.0 and br.iloc[1] == float('inf')
        assert pd.isna(br.iloc[2]) and br.iloc[3] == 2.0
        assert list(br.index) == ['20260101', '20260102', '20260103', '20260104']


class TestBreadthDistribution:
    def test_known_distribution(self):
        vals = ([0.5] * 40 + [1.2] * 30 + [1.8] * 20 + [2.4] * 9
                + [float('inf')] * 1)
        d = cals.breadth_distribution(pd.Series(vals))
        assert d['sample_days'] == 100 and d['inf_days'] == 1
        assert d['quantiles']['P50'] == 1.2 and d['quantiles']['P90'] == 1.8
        assert d['above']['>=2.0'] == {'days': 10, 'pct': 10.0}
        assert d['above']['>=1.5']['days'] == 30


class TestConditionalNextDay:
    def test_buckets_and_dropped_counted(self):
        m = _m([('20260101', 250, 100, 10, 3, 2.0, 0.2),   # 2.5 → [2.5,4)
                ('20260102', 200, 100, 12, 3, 3.0, 0.2),   # 2.0 → [2,2.5)
                ('20260103', 500, 300, 14, 3, 4.0, 0.2),   # 1.667 → [1.5,2)
                ('20260104', 100, 0, 14, 3, 4.0, 0.2)])    # inf
        sh = _sh([('20260102', 10.0, 10.0, 10.2),
                  ('20260103', 10.0, 10.0, 10.0),
                  ('20260106', 10.1, 10.0, 10.3)])   # 0104 的次日 0105 缺 → 计数
        cond = cals.conditional_next_day(m, sh)
        assert cond['rows_count'] == 2
        assert cond['dropped_no_sh'] == 1 and cond['dropped_no_next'] == 1
        labels = {b['bucket']: b for b in cond['buckets']}
        assert set(labels) == {'[2.5,4)', '[2,2.5)'}
        assert labels['[2.5,4)']['sh_close_chg_median'] == 2.0   # 10.2/10−1
        assert labels['[2.5,4)']['next_premium_mean'] == 3.0
        assert labels['[2,2.5)']['next_premium_mean'] == 4.0


class TestThresholdOptions:
    def test_p90_and_fixed_parallel(self):
        targets = ([0.5] * 40 + [1.2] * 30 + [1.8] * 20 + [2.4] * 9
                   + [float('inf')] + [0.5])   # 末位补一天，inf 档才有次日
        updown = {0.5: (50, 100), 1.2: (120, 100), 1.8: (180, 100),
                  2.4: (240, 100), float('inf'): (100, 0)}
        rows = []
        for i, t in enumerate(targets):
            d = (pd.Timestamp('2026-01-01') + pd.Timedelta(days=i)).strftime('%Y%m%d')
            up, down = updown[t]
            rows.append((d, up, down, 10, 3, 5.0, 0.2))
        sh = _sh([( (pd.Timestamp('2026-01-01') + pd.Timedelta(days=i)).strftime('%Y%m%d'),
                    10.1, 10.0, 10.05) for i in range(len(rows))])
        opts = cals.threshold_options(_m(rows), sh)
        by_t = {o['threshold']: o for o in opts}
        assert len(opts) == 4 and 'P90' in by_t
        assert by_t['P90']['thr_value'] == 1.8 and by_t['P90']['days'] == 30
        assert by_t[2.0]['days'] == 10 and by_t[2.0]['pct'] == 10.0
        assert by_t[2.5]['days'] == 1            # 仅 inf 档
        assert by_t[1.5]['days'] == 30
        assert all(o['overlap_core_pct'] == 0.0 for o in opts)  # max_streak=3 非 core


class TestIncrementalPower:
    def test_core_in_out_split(self):
        m = _m([('20260101', 250, 100, 10, 5, 4.5, 0.2),   # core, breadth 2.5
                ('20260102', 150, 100, 10, 5, 4.5, 0.2),   # core, breadth 1.5
                ('20260103', 220, 100, 10, 3, 2.0, 0.2),   # 非 core, breadth 2.2
                ('20260104', 100, 100, 10, 3, 2.0, 0.2),   # 非 core, breadth 1.0
                ('20260105', 120, 100, 10, 3, 2.0, 0.2),   # 非 core, breadth 1.2
                ('20260106', 130, 100, 10, 3, 2.0, 0.2)])  # 末位补一天供 0105 取次日
        sh = _sh([( (pd.Timestamp('2026-01-01') + pd.Timedelta(days=i)).strftime('%Y%m%d'),
                    10.1 if i % 2 == 0 else 9.9, 10.0, 10.0)
                  for i in range(6)])
        inc = cals.incremental_power(m, sh)
        assert inc['core_days'] == 2
        assert inc['in_core']['ge2']['n'] == 1 and inc['in_core']['lt2']['n'] == 1
        assert inc['out_core']['ge1.5']['n'] == 1
        assert inc['out_core']['baseline']['n'] == 3


# ---------------------------------------------------------------- A4 A50

class TestAlignOverseas:
    def test_asof_prev_day_and_forward_fill(self):
        """≤D−1 规则：D 自身 bar 对 D 开盘不可用；ov 休市日前向填充（周一用周五 bar）"""
        sh = _sh([('20260105', 10.0, 10.0, 10.0),
                  ('20260106', 10.2, 10.0, 10.0),
                  ('20260107', 9.8, 10.0, 10.0)])
        ov = pd.DataFrame([('20260103', 100.0), ('20260105', 103.0)],
                          columns=['trade_date', 'close'])
        mg = cals.align_overseas(sh, ov)
        # 0105: c=last≤0104 → 0103 bar=100（0105 bar 周六凌晨才收，不可用）；首日剔除
        # 0106: c=0105 bar=103 → move +3%；gap=+2%
        # 0107: c=last≤0106 → 0105 bar 前向填充=103 → move 0；gap=−2%
        assert list(mg['trade_date']) == ['20260106', '20260107']
        assert mg['ov_move'].iloc[0] == 3.0 and mg['open_gap'].iloc[0] == 2.0
        assert mg['ov_move'].iloc[1] == 0.0 and mg['open_gap'].iloc[1] == -2.0

    def test_first_day_and_bad_preclose_dropped(self):
        sh = _sh([('20260105', 10.0, 10.0, 10.0),
                  ('20260106', 10.2, 0.0, 10.0)])   # preclose=0 → gap 不可算
        ov = pd.DataFrame([('20260103', 100.0), ('20260105', 101.0)],
                          columns=['trade_date', 'close'])
        mg = cals.align_overseas(sh, ov)
        # 0105: c=last≤0104 → 0103 bar=100（首日剔除）；0106: c=0105 bar=101 → +1%
        assert list(mg['trade_date']) == ['20260106']
        assert mg['ov_move'].iloc[0] == 1.0 and pd.isna(mg['open_gap'].iloc[0])


class TestConsistency:
    def test_zero_excluded_and_rate_exact(self):
        mg = pd.DataFrame({'ov_move': [3.0, -2.0, 1.0, -1.0, 0.0],
                           'open_gap': [2.0, -1.0, -0.5, 0.5, 1.0]})
        c = cals.consistency(mg)
        # 同向：+3/+2 ✓、−2/−1 ✓、+1/−0.5 ✗、−1/+0.5 ✗；零值行剔除
        assert c['n'] == 4 and c['agree_rate'] == 50.0 and c['zero_excluded'] == 1

    def test_empty(self):
        mg = pd.DataFrame({'ov_move': [], 'open_gap': []})
        assert cals.consistency(mg)['agree_rate'] is None


class TestTriggerAvailability:
    def test_cross_freq_and_p1_p3_hits(self):
        mondays = ['20260105', '20260112', '20260119', '20260126', '20260202']
        mg = pd.DataFrame({'trade_date': mondays,
                           'ov_move': [-2.0, 1.6, 0.5, -0.9, 1.8],
                           'open_gap': [-1.0, 2.0, 0.1, 0.3, -0.2]})
        out = cals.trigger_availability(mg, thresholds=(1.0, 1.5), weekday=0)
        by_t = {o['threshold']: o for o in out}
        assert by_t[1.0]['anchor_days'] == 5 and by_t[1.0]['cross_days'] == 3
        assert by_t[1.0]['cross_pct'] == 60.0
        assert by_t[1.0]['p1_n'] == 1 and by_t[1.0]['p1_open_down'] == 1
        assert by_t[1.0]['p3_n'] == 2 and by_t[1.0]['p3_open_up'] == 1  # 1.8→−0.2 未兑现
        assert by_t[1.5]['cross_days'] == 3 and by_t[1.5]['cross_pct'] == 60.0

    def test_weekday_filter(self):
        mg = pd.DataFrame({'trade_date': ['20260106', '20260105'],   # 周二/周一
                           'ov_move': [9.0, 9.0], 'open_gap': [1.0, 1.0]})
        out = cals.trigger_availability(mg, thresholds=(1.0,), weekday=0)
        assert out[0]['anchor_days'] == 1


class TestA50GateReads:
    def test_gates_pass_and_pending(self):
        g = cals.a50_gate_reads({'agree_rate': 62.0}, {'cross_pct': 12.0}, bars=2593)
        assert g['sample_sufficient'] is True
        assert g['gate1_same_direction']['pass'] and g['gate2_cross_freq']['pass']
        assert g['gate3_forward_cases']['pass'] is None      # 前向人工登记

    def test_insufficient_sample_no_pass(self):
        """样本不足不给 pass——即使读数达标（防小样本误开门）"""
        g = cals.a50_gate_reads({'agree_rate': 70.0}, {'cross_pct': 15.0}, bars=100)
        assert g['sample_sufficient'] is False
        assert g['gate1_same_direction']['pass'] is False
        assert g['gate2_cross_freq']['pass'] is False

    def test_threshold_fail(self):
        g = cals.a50_gate_reads({'agree_rate': 55.0}, {'cross_pct': 8.0}, bars=2593)
        assert g['gate1_same_direction']['pass'] is False
        assert g['gate2_cross_freq']['pass'] is False
