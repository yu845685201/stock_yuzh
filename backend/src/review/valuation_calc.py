"""估值 calc 函数库（P0-4，方案 §3.3）：7 个纯函数 + CalcResult + display 格式化。

设计纪律（方案 §3.3.1/§3.3.2）：
- 每函数返回 ``CalcResult(status, value, unit, reason, calculation_id)``，
  ``status ∈ ok/not_meaningful/insufficient``；**任何位置禁 inf/NaN**
  （``math.isfinite`` 防御守卫，宁缺毋错——原则 P3）；
- ``calculation_id = sha256(函数名+版本+规范化参数)[:16]``，进 detail/meta
  （同日重算一致，原则 P7）；
- **扣非 spike 结论（2026-10-20 实测）**：``base_financial_report`` 无扣非列
  （采集走 baostock query_profit_data 无该字段），``pe_deducted_annualized``
  降级为「归母单季×4」口径，reason 显式标注「非扣非口径，含一次性损益放大
  风险」——禁静默当扣非用；
- 前瞻口径（一致预期 EPS）未采集：``forward_cagr`` 改名 ``profit_cagr_hist``
  （历史 TTM CAGR 替代），``forward_vs_ttm_judgement`` 入参 ``forward=None``
  恒返回「前瞻数据未获取」。
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any, Dict, Optional, Sequence

#: 库版本（进 calculation_id 与报告 meta.valuation_calc_version，原则 P7）
VALUATION_CALC_VERSION = '1.0.0'

#: 状态枚举
OK = 'ok'
NOT_MEANINGFUL = 'not_meaningful'
INSUFFICIENT = 'insufficient'

#: 分位最少样本数（方案 §3.3.1：stock_valuation_daily 历史序列，min_samples=20）
PERCENTILE_MIN_SAMPLES = 20


@dataclass(frozen=True)
class CalcResult:
    """单指标计算结果（禁 inf/NaN：value 仅在有义时非 None）。"""

    status: str
    value: Optional[float]
    unit: str
    reason: str
    calculation_id: str


def calculation_id(fn_name: str, params: Dict[str, Any]) -> str:
    """``sha256(函数名+版本+规范化参数)[:16]``（同日重算一致，原则 P7）。"""
    payload = json.dumps({'fn': fn_name, 'v': VALUATION_CALC_VERSION,
                          'params': _normalize(params)},
                         sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()[:16]


def _normalize(params: Dict[str, Any]) -> Dict[str, Any]:
    """参数规范化（数值统一 float，其余原样）——防 1 与 1.0 产出不同 id。"""
    out: Dict[str, Any] = {}
    for k, v in (params or {}).items():
        if isinstance(v, bool):
            out[k] = v
        elif isinstance(v, (int, float)):
            out[k] = float(v)
        else:
            out[k] = v
    return out


def _finite(x: Optional[float]) -> Optional[float]:
    """有限数守卫：None/NaN/inf → None。"""
    if x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


# ------------------------------------------------------------------ 7 个 calc 函数

def pe_deducted_annualized(single_quarter_profit: Optional[float],
                           total_mv: Optional[float]) -> CalcResult:
    """年化 PE（**归母单季×4 口径**——扣非数据未采集的降级，见模块 docstring）。

    单季≤0（亏损）→ ``not_meaningful``（亏损股 PE 无意义，禁输出负 PE）；
    市值缺失 → ``insufficient``。
    """
    cid = calculation_id('pe_deducted_annualized',
                         {'sq': single_quarter_profit, 'mv': total_mv})
    profit = _finite(single_quarter_profit)
    mv = _finite(total_mv)
    if mv is None or mv <= 0:
        return CalcResult(INSUFFICIENT, None, 'x', '总市值缺失或≤0', cid)
    if profit is None:
        return CalcResult(INSUFFICIENT, None, 'x', '单季归母净利缺失', cid)
    if profit <= 0:
        return CalcResult(NOT_MEANINGFUL, None, 'x',
                          '单季亏损（年化 PE 无意义；非扣非口径，含一次性损益放大风险）', cid)
    return CalcResult(OK, mv / (profit * 4), 'x',
                      '归母单季×4 口径（非扣非，含一次性损益放大风险）', cid)


def profit_cagr_hist(ttm_series: Sequence[Optional[float]],
                     years: float) -> CalcResult:
    """历史 TTM CAGR（替代前瞻口径；series 按时间升序，首点>0 且 ≥3 个点）。

    ``cagr = (last/first)^(1/years) - 1``；点数不足/首点≤0/末点缺失 →
    ``insufficient``。
    """
    cid = calculation_id('profit_cagr_hist',
                         {'n': len(ttm_series or []), 'years': years})
    pts = [_finite(x) for x in (ttm_series or [])]
    pts = [p for p in pts if p is not None]
    if len(pts) < 3 or years <= 0:
        return CalcResult(INSUFFICIENT, None, '%/年',
                          f'TTM 序列点不足({len(pts)}<3)或年限无效', cid)
    first, last = pts[0], pts[-1]
    if first <= 0:
        return CalcResult(INSUFFICIENT, None, '%/年', '首期 TTM ≤0（基数无效）', cid)
    ratio = last / first
    if ratio <= 0:
        return CalcResult(INSUFFICIENT, None, '%/年', '末期 TTM ≤0', cid)
    cagr = ratio ** (1.0 / years) - 1.0
    if not math.isfinite(cagr):
        return CalcResult(INSUFFICIENT, None, '%/年', 'CAGR 计算异常', cid)
    return CalcResult(OK, cagr * 100, '%/年', '历史 TTM CAGR（非前瞻一致预期）', cid)


def pe_digestion_years(pe_current: Optional[float], pe_anchor: float,
                       cagr_pct: Optional[float]) -> CalcResult:
    """估值消化年数：``t = ln(PE当前/PE锚)/ln(1+CAGR)``。

    语义：以 CAGR 增速盈利增长，把当前 PE「消化」到锚定 PE 所需年数
    （PE_future = PE_now/(1+g)^t = 锚 ⟹ t = ln(PE_now/锚)/ln(1+g)）。
    - PE 当前 ≤ 锚（已比锚定价便宜）→ 0.0 年；
    - CAGR≤0 → ``not_meaningful``（「无法消化（负增长）」，禁返回 inf）；
    - 输入缺失 → ``insufficient``。
    """
    cid = calculation_id('pe_digestion_years',
                         {'pe': pe_current, 'anchor': pe_anchor, 'cagr': cagr_pct})
    pe, cagr = _finite(pe_current), _finite(cagr_pct)
    anchor = _finite(pe_anchor)
    if pe is None or pe <= 0:
        return CalcResult(INSUFFICIENT, None, '年', '当前 PE 缺失或≤0', cid)
    if anchor is None or anchor <= 0:
        return CalcResult(INSUFFICIENT, None, '年', '锚定 PE 无效', cid)
    if cagr is None:
        return CalcResult(INSUFFICIENT, None, '年', 'CAGR 缺失', cid)
    if cagr <= 0:
        return CalcResult(NOT_MEANINGFUL, None, '年', '无法消化（负增长）', cid)
    if pe <= anchor:
        return CalcResult(OK, 0.0, '年', '当前估值已低于锚定价', cid)
    years = math.log(pe / anchor) / math.log(1 + cagr / 100.0)
    if not math.isfinite(years):
        return CalcResult(NOT_MEANINGFUL, None, '年', '无法消化（计算异常）', cid)
    return CalcResult(OK, years, '年', f'按 CAGR {cagr:.1f}% 消化至 {anchor:g}x', cid)


def forward_vs_ttm_judgement(forward_pe: Optional[float],
                             ttm_pe: Optional[float],
                             ttm_yoy_pct: Optional[float] = None) -> CalcResult:
    """前瞻 vs TTM 判读（±10pp）。**前瞻数据未采集** → 恒 insufficient。

    TTM 同比侧就绪（``net_profit_yoy`` + TTM 自算）时 reason 附带同比读数。
    """
    cid = calculation_id('forward_vs_ttm_judgement',
                         {'fwd': forward_pe, 'ttm': ttm_pe})
    if forward_pe is None:
        note = '前瞻数据未获取（一致预期 EPS 未采集）'
        if ttm_yoy_pct is not None and math.isfinite(ttm_yoy_pct):
            note += f'；TTM 同比 {ttm_yoy_pct:+.1f}%'
        return CalcResult(INSUFFICIENT, None, '', note, cid)
    fwd, ttm = _finite(forward_pe), _finite(ttm_pe)
    if fwd is None or ttm is None or ttm <= 0:
        return CalcResult(INSUFFICIENT, None, '', '前瞻或 TTM PE 缺失', cid)
    diff_pp = (fwd - ttm) / ttm * 100
    if abs(diff_pp) <= 10:
        label = '前瞻与 TTM 基本一致（±10pp 内）'
    elif diff_pp < 0:
        label = '前瞻低于 TTM（预期下修风险）'
    else:
        label = '前瞻高于 TTM（预期上修）'
    return CalcResult(OK, diff_pp, 'pp', label, cid)


def percentile_rank(value: Optional[float], history: Sequence[Optional[float]],
                    min_samples: int = PERCENTILE_MIN_SAMPLES) -> CalcResult:
    """历史分位（0-100）：``count(history ≤ value)/len(history)``。

    样本 < ``min_samples``（默认 20）→ ``insufficient``（禁小样本分位）；
    历史序列剔除 ≤0 与非数值（负 PE 无分位含义）。
    """
    cid = calculation_id('percentile_rank',
                         {'value': value, 'n': len(history or []),
                          'min_samples': min_samples})
    v = _finite(value)
    hist = [h for h in (_finite(x) for x in (history or [])) if h is not None and h > 0]
    if v is None:
        return CalcResult(INSUFFICIENT, None, '%', '当前值缺失', cid)
    if v <= 0:
        return CalcResult(NOT_MEANINGFUL, None, '%', '当前值 ≤0（分位无意义）', cid)
    if len(hist) < min_samples:
        return CalcResult(INSUFFICIENT, None, '%',
                          f'分位样本不足({len(hist)}<{min_samples})', cid)
    below = sum(1 for h in hist if h <= v)
    pct = below / len(hist) * 100
    return CalcResult(OK, pct, '%', f'近 {len(hist)} 个样本分位', cid)


def peg(pe: Optional[float], cagr_pct: Optional[float]) -> CalcResult:
    """PEG = PE / (CAGR% × 100 输入口径下的增长率小数)。

    **PEG 低≠安全**（低 PEG 常伴随盈利下修预期——本库只算数不背书）；
    CAGR≤0 → ``not_meaningful``。
    """
    cid = calculation_id('peg', {'pe': pe, 'cagr': cagr_pct})
    pe_v, cagr = _finite(pe), _finite(cagr_pct)
    if pe_v is None or pe_v <= 0:
        return CalcResult(INSUFFICIENT, None, '', 'PE 缺失或≤0', cid)
    if cagr is None:
        return CalcResult(INSUFFICIENT, None, '', 'CAGR 缺失', cid)
    if cagr <= 0:
        return CalcResult(NOT_MEANINGFUL, None, '',
                          '负增长（PEG 无意义；PEG 低≠安全，常伴盈利下修预期）', cid)
    return CalcResult(OK, pe_v / cagr, '', 'PEG 低≠安全（只算数不背书）', cid)


# ------------------------------------------------------------------ display 格式化

def format_pe(x: Optional[float]) -> str:
    """PE 展示：``18.2x``；None/非有限/≤0 → ``—``。"""
    v = _finite(x)
    return f'{v:.1f}x' if v is not None and v > 0 else '—'


def format_pct(x: Optional[float]) -> str:
    """百分比展示：``32%``；None → ``—``。"""
    v = _finite(x)
    return f'{v:.0f}%' if v is not None else '—'


def format_years(x: Optional[float]) -> str:
    """年数展示：``2.1年``；None → ``—``。"""
    v = _finite(x)
    return f'{v:.1f}年' if v is not None else '—'
