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
