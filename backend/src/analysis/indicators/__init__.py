"""技术指标纯函数库（通达信口径，方案 §3.1 P0-1）。

结构（对齐 MyTT 三层切分，新增通达信指标 = 翻译 5-10 行公式）::

    tdx_core.py        0 级：REF/MA/EMA/SMA/HHV/LLV/STD/SUM/MAX/MIN/ABS/COUNT/CROSS/RD
    tdx_indicators.py  2 级：macd/kdj/rsi/rsi_multi/boll/atr/obv（+ WARMUP 表）

口径红线：
- 每个函数与 MyTT 同名同义同参数（docstring 标注 MyTT.py 行号），**禁无声混用**
  其他口径（TA-Lib/ta/bukosabino）——同名指标数值不可比，见方案附录 B；
- 计算层全精度，RD 只用于展示层；
- NaN 语义与 MyTT 一致（分母 0 / warmup 前行输出 NaN，调用方按「数据不足」处理）。

消费方：analysis/modules/ind_*.py 截面模块（逐股调用）、review/indicator_panel.py
报告层面板复算（同一实现两入口，原则 P6）。
"""

from .tdx_core import (
    ABS, COUNT, CROSS, EMA, HHV, LLV, MAX, MIN, MA, RD, REF, SMA, STD, SUM,
)
from .tdx_indicators import (
    BOLLResult, KDJResult, MACDResult, RSIResult, adx, atr, boll, kdj, macd,
    obv, rsi, rsi_multi,
)

#: 各指标 warmup（交易日）——截面模块 ``max_lookback`` 的声明依据。
#: EMA/SMA 系取 130：MyTT 源码注释「EMA 至少需要 120 周期才精确」（ewm 以首值
#: 播种，短序列初值误差大），+2 对齐引擎 WINDOW_BUFFER 语义；rolling 系 60 足够。
WARMUP: dict = {
    "ma": 60,
    "macd": 130,
    "kdj": 60,
    "rsi": 130,
    "boll": 60,
    "atr": 60,
    "obv": 130,
    "adx": 30,
}

__all__ = [
    # 0 级
    "ABS", "COUNT", "CROSS", "EMA", "HHV", "LLV", "MAX", "MIN", "MA", "RD",
    "REF", "SMA", "STD", "SUM",
    # 2 级
    "BOLLResult", "KDJResult", "MACDResult", "RSIResult",
    "adx", "atr", "boll", "kdj", "macd", "obv", "rsi", "rsi_multi",
    # warmup
    "WARMUP",
]
