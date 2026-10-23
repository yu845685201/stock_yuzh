"""2 级技术指标（通达信口径，与 ``mpquant/MyTT`` 逐字对应，方案 §3.1.2）。

每个指标的公式、MyTT 源码行号、默认参数与口径禁忌见各自 docstring；
总口径速查见本目录 README.MD 与方案附录 B。

共性约定：
- 输入为按 ``trade_date`` 升序的 Series（或 ndarray/list，入口统一数值化）；
- 输出 Series 与输入等长、同索引；前 warmup-1 行为 NaN（模块侧按「数据不足」处理）；
- 多输出指标返回 frozen dataclass（字段名即通达信术语）。
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from .tdx_core import ABS, EMA, HHV, LLV, MAX, MA, REF, SMA, STD, SUM, _numeric


@dataclass(frozen=True)
class MACDResult:
    """MACD 三输出。hist = (dif-dea)×2 —— 乘 2 是通达信柱口径（多数软件不乘 2）。"""

    dif: pd.Series
    dea: pd.Series
    hist: pd.Series


def macd(close, short: int = 12, long: int = 26, m: int = 9) -> MACDResult:
    """MACD：``DIF=EMA(C,12)-EMA(C,26)``；``DEA=EMA(DIF,9)``；``MACD=(DIF-DEA)*2``。

    MyTT.py:144。warmup 130（EMA 系，见 WARMUP）。
    """
    c = _numeric(close)
    dif = EMA(c, short) - EMA(c, long)
    dea = EMA(dif, m)
    return MACDResult(dif=dif, dea=dea, hist=(dif - dea) * 2)


@dataclass(frozen=True)
class KDJResult:
    """KDJ 三输出。K/D 用 span=5 的 EMA（M1*2-1）——通达信标准构造。"""

    k: pd.Series
    d: pd.Series
    j: pd.Series


def kdj(close, high, low, n: int = 9, m1: int = 3, m2: int = 3) -> KDJResult:
    """KDJ：``RSV=(C-LLV(L,9))/(HHV(H,9)-LLV(H,9))*100``；``K=EMA(RSV,5)``；
    ``D=EMA(K,5)``；``J=3K-2D``。MyTT.py:149。

    口径禁忌：ta 库 StochasticOscillator 用 rolling-mean 平滑，数值不同源。
    分母为 0（一字板区间）→ RSV 为 NaN 并向下传播（调用方按数据不足处理）。
    warmup 60。
    """
    c, h, l = _numeric(close), _numeric(high), _numeric(low)
    rsv = (c - LLV(l, n)) / (HHV(h, n) - LLV(l, n)) * 100
    k = EMA(rsv, m1 * 2 - 1)
    d = EMA(k, m2 * 2 - 1)
    return KDJResult(k=k, d=d, j=k * 3 - d * 2)


def rsi(close, n: int = 24) -> pd.Series:
    """RSI（单线）：``RSI=SMA(MAX(DIF,0),N)/SMA(ABS(DIF),N)*100``，DIF=C-REF(C,1)。

    MyTT.py:154（通达信原式，SMA 分母是中国式递推，非 ta 库 ewm 双轨）。
    分母为 0（全天平盘）→ NaN。warmup 130。
    """
    c = _numeric(close)
    dif = c - REF(c, 1)
    return SMA(MAX(dif, 0), n) / SMA(ABS(dif), n) * 100


@dataclass(frozen=True)
class RSIResult:
    """RSI 三线（A 股行情软件常用 6/12/24 三组）。"""

    rsi6: pd.Series
    rsi12: pd.Series
    rsi24: pd.Series


def rsi_multi(close, ns=(6, 12, 24)) -> RSIResult:
    """RSI 三线（默认 6/12/24；单线口径同 :func:`rsi`）。"""
    r6, r12, r24 = (rsi(close, n) for n in ns)
    return RSIResult(rsi6=r6, rsi12=r12, rsi24=r24)


@dataclass(frozen=True)
class BOLLResult:
    """布林带三输出。STD 为总体标准差（ddof=0）。"""

    upper: pd.Series
    mid: pd.Series
    lower: pd.Series


def boll(close, n: int = 20, p: float = 2) -> BOLLResult:
    """BOLL：``MID=MA(C,20)``；``UPPER=MID+STD(C,20)*P``；``LOWER=MID-STD(C,20)*P``。

    MyTT.py:169（STD 定义见 MyTT.py:43，ddof=0）。warmup 60。
    """
    c = _numeric(close)
    mid = MA(c, n)
    std = STD(c, n)
    return BOLLResult(upper=mid + std * p, mid=mid, lower=mid - std * p)


def atr(close, high, low, n: int = 20) -> pd.Series:
    """ATR：``TR=MAX(MAX(H-L,|REF(C,1)-H|),|REF(C,1)-L|)``；``ATR=MA(TR,N)``。

    MyTT.py:184——**N 日简单平均（通达信口径）**，非 Wilder 递推（TA-Lib/ta
    默认 N=14 且口径不同，数值不可比）。warmup 60。
    """
    c, h, l = _numeric(close), _numeric(high), _numeric(low)
    tr = MAX(MAX(h - l, ABS(REF(c, 1) - h)), ABS(REF(c, 1) - l))
    return MA(tr, n)


def adx(high, low, close, m1: int = 14, m2: int = 6):
    """ADX/DMI（通达信口径，P1-7 §4.6 E 的闸门输入）：返回 ``(PDI, MDI, ADX)``。

    公式（SUM+MA 平滑，标准通达信 DMI）::

        TR  = SUM(MAX(MAX(H-L,|H-REF(C,1)|),|L-REF(C,1)|), M1)
        HD  = H-REF(H,1)；LD = REF(L,1)-L
        DMP = SUM(HD>0 ∧ HD>LD ? HD : 0, M1)
        DMM = SUM(LD>0 ∧ LD>HD ? LD : 0, M1)
        PDI = DMP*100/TR；MDI = DMM*100/TR
        DX  = |PDI-MDI|/(PDI+MDI)*100
        ADX = MA(DX, M2)

    口径声明（诚实）：**本地 MyTT 克隆无 ADX 函数**（grep 实证），故本实现对拍
    基线为通达信标准公式而非克隆——公式逐项对应上述基元（SUM/MA/REF 与本库
    其他指标同源），单测含手算值验证。TR=0（一字板连日）时 PDI/MDI/DX 为 NaN
    （不硬造 0）。warmup ≈ M1+M2。
    """
    h, l, c = _numeric(high), _numeric(low), _numeric(close)
    prev_c = REF(c, 1)
    tr = SUM(MAX(MAX(h - l, ABS(h - prev_c)), ABS(l - prev_c)), m1)
    hd = h - REF(h, 1)
    ld = REF(l, 1) - l
    dmp = SUM(pd.Series(np.where((hd > 0) & (hd > ld), hd, 0.0),
                        index=h.index), m1)
    dmm = SUM(pd.Series(np.where((ld > 0) & (ld > hd), ld, 0.0),
                        index=l.index), m1)
    with np.errstate(divide="ignore", invalid="ignore"):
        pdi = dmp * 100.0 / tr
        mdi = dmm * 100.0 / tr
        dx = (pdi - mdi).abs() / (pdi + mdi) * 100.0
    adx_s = MA(dx, m2)
    return pdi, mdi, adx_s


def obv(close, vol) -> pd.Series:
    """OBV（能量潮）：``SUM(IF(C>REF(C,1),VOL,IF(C<REF(C,1),-VOL,0)),0)/10000``。

    MyTT.py:260。两个口径点：**末尾除 10000 是手→万手单位换算**；**平盘计 0**
    （ta 库平盘计 +vol，口径不同）。NaN 语义与 MyTT 一致：VOL 为 NaN 时该日起
    向后传播（不填 0——填 0 会与 MyTT 结果分叉）。warmup 130。
    """
    c, v = _numeric(close), _numeric(vol)
    prev = REF(c, 1)
    signed = pd.Series(
        np.where(c > prev, v, np.where(c < prev, -v, 0.0)), index=c.index
    )
    return signed.cumsum() / 10000
