"""0 级核心函数（通达信口径，与 ``mpquant/MyTT`` 逐字对应）。

移植来源：本地克隆 ``refrences/mpquant__MyTT/MyTT.py``（V3.3）。口径红线（方案
附录 B）：本库函数与 MyTT 同名同义——公式翻译规则只有三条（``:=``→``=``、
``AND``→``&``、``OR``→``|``），新增通达信指标即「翻译 5-10 行公式」。

与 MyTT 的差异（刻意为之）：
- **出入类型**：MyTT 序列进、``np.ndarray`` 出（无索引）；本库 Series 进、Series 出
  （保留索引，与日 K DataFrame 行对齐）——值逐位一致，仅载体不同；
- **RD 舍入**：MyTT 的 MACD/RSI/BOLL 出口包 ``RD``（3 位小数，展示层对齐行情
  软件）；本库**计算层保留全精度**，RD 只用于展示层（vibe-astock 教训：先舍入
  再平均会对不上）。

NaN 语义（与 MyTT 一致，不额外设防）：分母为 0（如 KDJ 的 RSV 遇一字板区间、
RSI 遇全天平盘）输出 NaN 并由调用方按「数据不足」处理——禁拿 0 或默认值顶替
（原则 P3）。前 ``N-1`` 个 warmup 行同样为 NaN。
"""

from __future__ import annotations

from typing import Union

import numpy as np
import pandas as pd

#: 输入：Series / ndarray / list 均可（MyTT 兼容）
SeriesLike = Union[pd.Series, np.ndarray, list]


def _as_series(s: SeriesLike) -> pd.Series:
    """统一转 Series（保留原索引；ndarray/list 用默认整数索引）。"""
    if isinstance(s, pd.Series):
        return s
    return pd.Series(s)


def _numeric(s: SeriesLike) -> pd.Series:
    """数值化（非数值转 NaN，与 pd.to_numeric(errors='coerce') 同行为）。"""
    return pd.to_numeric(_as_series(s), errors="coerce")


def _numeric_or_scalar(x: Union[SeriesLike, int, float]):
    """数值化；标量原样返回（供 MAX/MIN 的逐元素运算，避免长度-1 Series 广播错位）。"""
    if isinstance(x, (int, float)):
        return float(x)
    return _numeric(x)


# --------------------------------------------------------------- 0 级：序列原子操作
def REF(s: SeriesLike, n: int = 1) -> pd.Series:
    """对序列整体下移 n（前 n 个为 NaN）。MyTT.py:36。"""
    if n < 0:
        raise ValueError(f"REF 的 n 必须 ≥0: {n}")
    return _as_series(s).shift(n)


def MA(s: SeriesLike, n: int) -> pd.Series:
    """N 日简单移动平均。MyTT.py:63。"""
    return _numeric(s).rolling(n).mean()


def EMA(s: SeriesLike, n: int) -> pd.Series:
    """指数移动平均（span=N，adjust=False，首值播种）。MyTT.py:66。

    注：MyTT 源码注释「S>4×N，EMA 至少需要 120 周期」才精确——短序列初值误差大，
    截面取最新值的模块应按 warmup 声明回看（见 indicators.WARMUP）。
    """
    return _numeric(s).ewm(span=n, adjust=False).mean()


def SMA(s: SeriesLike, n: int, m: int = 1) -> pd.Series:
    """中国式 SMA：``Y=(M*X+(N-M)*Y')/N``，即 ``ewm(alpha=m/n, adjust=False)``。

    **不是简单移动平均**（与 ta 库同名不同物）。默认 m=1 时退化为 alpha=1/N
    的 Wilder 型递推。MyTT.py:69。
    """
    if n <= 0:
        raise ValueError(f"SMA 的 n 必须 >0: {n}")
    return _numeric(s).ewm(alpha=m / n, adjust=False).mean()


def HHV(s: SeriesLike, n: int) -> pd.Series:
    """N 周期最高值。MyTT.py:51。"""
    return _numeric(s).rolling(n).max()


def LLV(s: SeriesLike, n: int) -> pd.Series:
    """N 周期最低值。MyTT.py:54。"""
    return _numeric(s).rolling(n).min()


def STD(s: SeriesLike, n: int) -> pd.Series:
    """N 日标准差（**总体标准差 ddof=0**，与 ta 库 BOLL 一致）。MyTT.py:42。"""
    return _numeric(s).rolling(n).std(ddof=0)


def SUM(s: SeriesLike, n: int) -> pd.Series:
    """N 日累计和；``n=0`` 为从头累计（cumsum）。MyTT.py:45。"""
    s = _numeric(s)
    return s.rolling(n).sum() if n > 0 else s.cumsum()


def ABS(s: SeriesLike) -> pd.Series:
    """逐元素绝对值。MyTT.py:24。"""
    return _numeric(s).abs()


def MAX(s1: SeriesLike, s2: SeriesLike) -> pd.Series:
    """逐元素取大（序列 × 序列或序列 × 标量）。MyTT.py:31。"""
    return np.maximum(_numeric_or_scalar(s1), _numeric_or_scalar(s2))


def MIN(s1: SeriesLike, s2: SeriesLike) -> pd.Series:
    """逐元素取小。MyTT.py:32。"""
    return np.minimum(_numeric_or_scalar(s1), _numeric_or_scalar(s2))


def COUNT(cond: SeriesLike, n: int) -> pd.Series:
    """N 周期内条件成立次数（布尔当 1 计）。MyTT.py:94。"""
    return _as_series(cond).fillna(False).astype(int).rolling(n).sum()


def CROSS(s1: SeriesLike, s2: SeriesLike) -> pd.Series:
    """向上金叉：前一根不满足、当前满足（``CROSS(MA5,MA10)`` 即金叉）。MyTT.py:120。

    首行恒为 False（无「前一根」可比）；含 NaN 的比较按不满足处理。
    """
    gt = (_numeric(s1) > _numeric(s2)).fillna(False)
    prev = gt.shift(1, fill_value=False)
    cross = gt & ~prev
    if len(cross):
        cross.iloc[0] = False
    return cross


def RD(s: SeriesLike, d: int = 3) -> pd.Series:
    """展示层舍入（3 位小数，对齐行情软件显示）。MyTT.py:22。

    **只用于展示**：计算层保留全精度，禁在公式中间步骤舍入。
    """
    return _numeric(s).round(d)
