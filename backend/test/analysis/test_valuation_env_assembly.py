"""估值环境装配单测（P0-4 §3.3.3）：assemble_valuation 用 stub reader 验证。

免数据库：stub reader 按 SQL 特征分派三路查询（当日行 / 分位历史 / 财报期），
覆盖分位可算、样本不足、CAGR 可算、空锚定、亏损股五条路径。
"""

import pytest

from src.analysis.env_assembly import assemble_valuation


class _StubReader:
    """按 SQL 内容分派的 stub reader（_query / nth_trading_day_back）。"""

    def __init__(self, anchor_rows, hist_rows, fin_rows, nth='20250930'):
        self.anchor_rows = anchor_rows
        self.hist_rows = hist_rows
        self.fin_rows = fin_rows
        self.nth = nth

    def _query(self, sql, params=None):
        if 'total_mv' in sql:
            return self.anchor_rows
        if 'BETWEEN' in sql:
            return self.hist_rows
        if 'base_financial_report' in sql:
            return self.fin_rows
        raise AssertionError(f'未预期的查询: {sql[:80]}')

    def nth_trading_day_back(self, as_of, n):
        return self.nth


class _StubConfig:
    def load_config(self):
        return {'review': {'valuation_digest': {'pe_anchor': 25.0,
                                                'min_samples': 20}}}


def _reader(anchor, hist, fin):
    return _StubReader(anchor, hist, fin)


ANCHOR = [('sh.600000', 5e10, 18.0, 1.1),      # PE 18
          ('sh.600001', 3e10, None, 0.9)]      # 亏损（pe 空）
HIST = ([('sh.600000', float(v)) for v in range(10, 30)]        # 20 样本（10..29）
        + [('sh.600000', 19.0), ('sh.600000', 25.0)]            # 2 个更高 → 22 样本
        + [('sh.600001', 12.0)])                                # 亏损股历史也被查
FIN = [('sh.600000', d, p) for d, p in [
    ('20231231', 400.0), ('20240630', 240.0), ('20241231', 426.0),
    ('20250630', 248.0), ('20251231', 460.0), ('20260630', 256.0)]]


def test_assemble_percentile_and_cagr():
    out = assemble_valuation(_reader(ANCHOR, HIST, FIN), '20260930', _StubConfig())
    d = out['sh.600000']
    assert d.pe_ttm == 18.0
    assert d.percentile_samples == 22
    # 22 个样本中 ≤18 的 9 个（10..18）→ 40.9%
    assert d.pe_percentile == pytest.approx(40.91, abs=0.01)
    # TTM 序列 400→468（2.5 年）→ CAGR ≈ 5.4%/年
    assert d.cagr_pct is not None and d.cagr_pct > 0
    assert d.digestion_years is not None
    assert d.peg_value is not None


def test_assemble_insufficient_samples():
    hist = [('sh.600000', 15.0)] * 5      # 仅 5 样本 < 20
    out = assemble_valuation(_reader(ANCHOR, hist, FIN), '20260930', _StubConfig())
    assert out['sh.600000'].pe_percentile is None
    assert out['sh.600000'].percentile_samples == 5


def test_assemble_empty_anchor():
    out = assemble_valuation(_reader([], HIST, FIN), '20260930', _StubConfig())
    assert out == {}


def test_assemble_loss_stock():
    out = assemble_valuation(_reader(ANCHOR, HIST, FIN), '20260930', _StubConfig())
    d = out['sh.600001']
    assert d.pe_ttm is None
    assert d.digestion_years is None       # 亏损不输出消化年数
    assert d.digestion_reason              # 有原因说明


def test_assemble_no_financial_rows():
    out = assemble_valuation(_reader(ANCHOR, HIST, []), '20260930', _StubConfig())
    assert out['sh.600000'].cagr_pct is None
    assert out['sh.600000'].digestion_years is None
