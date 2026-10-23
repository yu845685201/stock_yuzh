"""量价资金维度单测（V1.1）：volume_trend / money_trend 模块 + P3 采集器解析。

口径红线用例：北向 buy/sell 停披露置 None；南向净额 = Buy−Sell；
资金确认谓词 money_confirm 默认关（默认行为与既有候选集一致）。
"""

import pandas as pd
import pytest

from src.analysis.candidate_rules import is_r2, is_r3
from src.analysis.module_base import ChainContext, ModuleResult, StockInfo
from src.analysis.modules.money_trend import MoneyTrend
from src.analysis.modules.volume_trend import VolumeTrend


def _ctx(as_of="20260930", upstream=None):
    return ChainContext(
        as_of=as_of,
        stock=StockInfo(ts_code="sh.600001", stock_code="600001", stock_name="测试股",
                        board="主板"),
        upstream=dict(upstream or {}),
    )


def _vol_df(vols, as_of="20260930"):
    """以 as_of 为锚定日（末行）按自然日倒推的日期序列（模块只依赖有序性）"""
    from datetime import date, timedelta
    end = date(int(as_of[:4]), int(as_of[4:6]), int(as_of[6:]))
    n = len(vols)
    dates = [(end - timedelta(days=n - 1 - i)).strftime('%Y%m%d') for i in range(n)]
    return pd.DataFrame({'trade_date': dates, 'volume': vols})


class TestVolumeTrend:
    def setup_method(self):
        self.mod = VolumeTrend()

    def test_surge(self):
        base = [100.0] * 20
        df = _vol_df(base + [160.0])
        assert self.mod.analyze(df, _ctx()).value == '放量'

    def test_continuous_surge(self):
        base = [80.0] * 18
        # 近 3 日 r5：1.63 / 1.56 / 1.57 均 ≥1.2，且锚定日 r5=1.57 ≥1.5
        df = _vol_df(base + [130.0, 140.0, 160.0])
        r = self.mod.analyze(df, _ctx())
        assert r.value == '连续放量'

    def test_shrink(self):
        df = _vol_df([100.0] * 20 + [60.0])
        assert self.mod.analyze(df, _ctx()).value == '缩量'

    def test_mild(self):
        df = _vol_df([100.0] * 20 + [100.0])
        assert self.mod.analyze(df, _ctx()).value == '温和'

    def test_insufficient(self):
        df = _vol_df([100.0] * 10)
        assert '数据不足' in self.mod.analyze(df, _ctx()).value
        # 停牌标记行
        df2 = _vol_df([100.0] * 20 + [0.0])
        assert '数据不足' in self.mod.analyze(df2, _ctx()).value


class TestMoneyTrend:
    def setup_method(self):
        self.mod = MoneyTrend()

    def test_inflow_with_streak(self):
        hist = [('2026092' + str(i), 1.0e8) for i in range(1, 6)]
        r = self.mod.analyze(_df(), _ctx(upstream={'mf_history_map':
                                                   {'sh.600001': hist}}))
        assert r.value == '净流入1.00亿/连5天'

    def test_outflow(self):
        hist = [('20260930', -5.0e7)]
        r = self.mod.analyze(_df(), _ctx(upstream={'mf_history_map':
                                                   {'sh.600001': hist}}))
        assert r.value == '净流出0.50亿'

    def test_no_rows(self):
        r = self.mod.analyze(_df(), _ctx(upstream={'mf_history_map': {}}))
        assert r.value == '否'

    def test_missing_env(self):
        r = self.mod.analyze(_df(), _ctx(upstream={}))
        assert r.value == '数据不足'


def _df():
    return pd.DataFrame([{'trade_date': '20260930', 'volume': 100.0}])


class TestMoneyConfirmPredicate:
    """资金确认谓词：默认关不改变既有候选集（用户决策 2026-10-05）"""

    def test_default_off(self):
        assert is_r2(1, False, ['创新药'], ['创新药'], {'money_confirm': False},
                     big_net=-1.0e8)  # 净流出也通过（关）
        assert is_r3(True, False, 2.5, 4.0, {'money_confirm': False})

    def test_on_filters(self):
        assert not is_r2(1, False, ['创新药'], ['创新药'], {'money_confirm': True},
                         big_net=-1.0e8)
        assert not is_r2(1, False, ['创新药'], ['创新药'], {'money_confirm': True},
                         big_net=None)  # 无资金行不硬造
        assert is_r3(True, False, 2.5, 4.0, {'money_confirm': True}, big_net=1.0e6)


class TestHkexParse:
    """港交所日度统计解析（口径红线：北向 buy/sell 置 None）"""

    def test_parse_real_shape(self):
        from src.sync.hkex_connect_manager import parse_daily_stat
        raw = """tabData = [
 {"date": "2026-09-30", "market": "SSE Northbound", "content": [{"table": {
   "schema": [["Total Turnover", "Total Trade Count", "DQB", "ETF Turnover"]],
   "tr": [{"td": [["101,257.88"]]}, {"td": [["5,269,396"]]},
          {"td": [["999,999,999"]]}, {"td": [["1,954.44"]]}]}}]},
 {"date": "2026-09-30", "market": "SSE Southbound", "content": [{"table": {
   "schema": [["Total Turnover", "Buy Turnover", "Sell Turnover",
               "Total Trade Count", "Buy Trade Count", "Sell Trade Count",
               "ETF Turnover"]],
   "tr": [{"td": [["46,015.39"]]}, {"td": [["25,554.86"]]},
          {"td": [["20,460.53"]]}, {"td": [["1,090,204"]]},
          {"td": [["-"]]}, {"td": [["-"]]}, {"td": [["227.63"]]}]}}]}]"""
        rows = parse_daily_stat(raw)
        by_ch = {r['channel']: r for r in rows}
        assert by_ch['north_sse']['total_turnover'] == 101257.88
        assert by_ch['north_sse']['buy_turnover'] is None      # 停披露
        assert by_ch['north_sse']['trade_count'] == 5269396
        assert by_ch['south_sse']['buy_turnover'] == 25554.86
        assert by_ch['south_sse']['sell_turnover'] == 20460.53
        net = (by_ch['south_sse']['buy_turnover']
               - by_ch['south_sse']['sell_turnover'])
        assert round(net, 1) == 5094.3

    def test_placeholder_dqb(self):
        from src.sync.hkex_connect_manager import _num
        assert _num('999,999,999') is None
        assert _num('-') is None
        assert _num('101,257.88') == 101257.88


class TestEtfParse:
    """两所 ETF 行解析"""

    def test_sse_rows(self):
        from src.sync.etf_scale_manager import parse_sse_rows
        rows = parse_sse_rows({'result': [
            {'SEC_CODE': '510050', 'SEC_NAME': '50ETF', 'ETF_TYPE': '单市',
             'TOT_VOL': '693876.68'}]}, '2026-09-30')
        assert rows[0]['fund_code'] == '510050'
        assert rows[0]['shares_wan'] == 693876.68
        assert rows[0]['trade_date'] == '20260930'
        assert rows[0]['exchange'] == 'sse'

    def test_szse_rows(self):
        from src.sync.etf_scale_manager import parse_szse_rows
        rows = parse_szse_rows([
            {'sys_key': "<a href='x'><u>159915</u></a>",
             'kzjcurl': "name=创业板ETF易方达&",
             'dqgm': '2,060,945.49'}], '2026-09-30')
        assert rows[0]['fund_code'] == '159915'
        assert rows[0]['shares_wan'] == 2060945.49
        assert rows[0]['fund_name'] == '创业板ETF易方达'
