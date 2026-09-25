"""黄金样本回归（方案 §3.8）：固定代表股跑链，结果与基线比对，防模块改动引入漂移。

**默认跳过**：需要真实数据库，且前复权序列会随除权重拉而变化（这是预期的「合理漂移」），
不适合放进日常 CI。显式开启：

    ANALYSIS_GOLDEN=1 ../.venv/bin/python -m pytest test/analysis/test_golden_sample.py -q

基线维护：当数据库发生**合法**的前复权基准重建后，重新生成基线并 diff 复核后再更新。
"""

import os
from pathlib import Path

import pytest

from src.config import ConfigManager
from src.analysis.data_reader import DataReader
from src.analysis.lookback import Lookback
from src.analysis.module_base import ChainContext, StockInfo
from src.analysis.modules.consecutive_limit_up import ConsecutiveLimitUp
from src.analysis.modules.limit_up_today import LimitUpToday

ENABLED = os.getenv("ANALYSIS_GOLDEN") == "1"
pytestmark = pytest.mark.skipif(not ENABLED, reason="设 ANALYSIS_GOLDEN=1 开启黄金样本回归")

#: 锚定日（固定，不随库内最新日期漂移）
AS_OF = "20260923"
#: 代表股：主板 / 创业板 / 科创板 / ST / 次新 / 高连板
GOLDEN_STOCKS = [
    "600418", "600825", "601811", "603636", "000560",
    "000850", "300750", "688512", "600053", "002935",
]
#: 基线：{股票代码: (是否涨停, 连续涨停数)}
#: 首次生成于 2026-09-24（uat 库，锚定日 20260923），并与 limit_up_scan 链的 CSV 输出交叉核对一致。
BASELINE = {
    "600418": ("是", 2),
    "600825": ("是", 3),
    "601811": ("是", 4),
    "603636": ("是", 3),
    "000560": ("是", 3),
    "000850": ("是", 1),
    "300750": ("否", 0),
    "688512": ("是", 1),
    "600053": ("否", 0),   # *ST 九鼎：锚定日未涨停
    "002935": ("是", 1),
}


def _load():
    cm = ConfigManager()
    try:
        reader = DataReader(cm)
        reader.latest_trade_date()
    except Exception as exc:  # pragma: no cover - 无数据库环境
        pytest.skip(f"数据库不可用: {exc!r}")
    universe = {s.stock_code: s for s in reader.load_stock_universe()}
    codes = [universe[c] for c in GOLDEN_STOCKS if c in universe]
    start = reader.resolve_window_start(AS_OF, Lookback.parse("250d"))
    kdf = reader.load_kline(
        columns=["change_rate", "close", "high", "is_st"],
        as_of=AS_OF,
        start_date=start,
        ts_codes=[s.ts_code for s in codes],
    )
    reader.close()
    return {s.stock_code: s for s in codes}, kdf


def test_golden_sample_stable():
    universe, kdf = _load()
    mod1, mod2 = LimitUpToday(), ConsecutiveLimitUp()
    actual = {}
    for ts_code, g in kdf.groupby("ts_code", sort=False):
        code = str(g["stock_code"].iloc[0])
        stock = universe.get(code)
        if stock is None:
            continue
        ctx = ChainContext(as_of=AS_OF, stock=stock)
        actual[code] = (
            mod1.analyze(g, ctx).value,
            mod2.analyze(g, ctx).value,
        )

    mismatch = {
        code: (BASELINE.get(code), actual.get(code))
        for code in GOLDEN_STOCKS
        if code in universe and BASELINE.get(code) != actual.get(code)
    }
    assert not mismatch, f"黄金样本漂移: {mismatch}"
