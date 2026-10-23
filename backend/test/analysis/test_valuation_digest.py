"""估值摘要模块单测（P0-4 §3.3.3）：env 装配数据的三态输出与边界。

模块零数据库访问——用假 valuation_map 驱动；assemble_valuation 的真实
查询路径由链实跑保护（真实数据对拍见 test_valuation_env_assembly.py）。
"""

import pandas as pd

from src.analysis.env_assembly import ValuationDigest
from src.analysis.module_base import DATA_INSUFFICIENT, ChainContext, StockInfo
from src.analysis.modules.valuation_digest import ValuationDigestModule

AS_OF = "20260930"


def _ctx(ts_code="sh.600000", valuation_map=None):
    return ChainContext(
        as_of=AS_OF,
        stock=StockInfo(ts_code=ts_code, stock_code="600000",
                        stock_name="测试股", board="主板"),
        upstream={'valuation_map': valuation_map} if valuation_map is not None else {})


def _digest(**kw):
    base = dict(pe_ttm=18.2, pb=1.1, total_mv=5e10, pe_percentile=32.0,
                percentile_samples=120, cagr_pct=25.0, digestion_years=2.1,
                digestion_reason='按 CAGR 25.0% 消化至 25x', peg_value=0.73)
    base.update(kw)
    return ValuationDigest(**base)


class TestValuationDigestModule:
    def test_full_combo(self):
        res = ValuationDigestModule().analyze(
            pd.DataFrame(), _ctx(valuation_map={'sh.600000': _digest()}))
        assert res.value == 'PE-TTM 18.2x/历史32%分位/消化2.1年'
        assert res.detail['pe_ttm'] == 18.2
        assert res.detail['peg'] == 0.73

    def test_percentile_insufficient(self):
        res = ValuationDigestModule().analyze(
            pd.DataFrame(), _ctx(valuation_map={'sh.600000': _digest(
                pe_percentile=None, percentile_samples=8)}))
        assert res.value == 'PE-TTM 18.2x/分位样本不足/消化2.1年'
        assert res.detail['percentile_samples'] == 8
        assert res.detail['pe_percentile'] is None

    def test_digestion_absent_when_negative_growth(self):
        res = ValuationDigestModule().analyze(
            pd.DataFrame(), _ctx(valuation_map={'sh.600000': _digest(
                digestion_years=None, digestion_reason='无法消化（负增长）')}))
        assert res.value == 'PE-TTM 18.2x/历史32%分位'     # 消化段省略
        assert res.detail['digestion_reason'] == '无法消化（负增长）'

    def test_env_not_assembled(self):
        res = ValuationDigestModule().analyze(pd.DataFrame(), _ctx())
        assert res.value == DATA_INSUFFICIENT
        assert '未装配' in res.detail['reason']

    def test_no_row_for_stock(self):
        res = ValuationDigestModule().analyze(
            pd.DataFrame(), _ctx(valuation_map={'sh.600001': _digest()}))
        assert res.value == DATA_INSUFFICIENT

    def test_loss_stock_insufficient(self):
        res = ValuationDigestModule().analyze(
            pd.DataFrame(), _ctx(valuation_map={'sh.600000': _digest(pe_ttm=None)}))
        assert res.value == DATA_INSUFFICIENT
        assert '不可算' in res.detail['reason']
