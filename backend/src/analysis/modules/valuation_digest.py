"""M30 ``valuation_digest`` 个股估值摘要（P0-4，方案 §3.3.3）。

数据来自 env 装配（``a_share_valuation`` → ``ctx.upstream['valuation_map']``），
模块自身零数据库访问（纯函数红线）。取值（理由串，§2.5 reason 口径）：

- ``PE-TTM 18.2x/历史32%分位/消化2.1年@25x``：全要素可算；
- ``PE-TTM 18.2x/分位样本不足``：分位样本 <20（禁小样本分位）；
- ``数据不足``：估值环境未装配 / 无当日行 / PE-TTM 不可算（亏损或缺失）。

口径：PE(TTM) 为 sync 侧自算（总市值÷近四季**归母**净利，financial_ttm 同口径）；
分位 = 近 1 年 pe_ttm 历史分位；消化年数 = ln(PE锚/PE当前)/ln(1+CAGR)（3 年
TTM CAGR，负增长 → 不输出）；PEG 低≠安全（只进 detail 不背书）。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ...review.valuation_calc import format_pe, format_years
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class ValuationDigestModule(AnalysisModule):
    """个股估值摘要（PE-TTM/历史分位/消化年数，理由串）。"""

    name: ClassVar[str] = '个股估值摘要'
    name_en: ClassVar[str] = 'valuation_digest'
    description: ClassVar[str] = (
        "个股估值摘要（env a_share_valuation 装配，零库访问）：PE-TTM（自算归母"
        "口径）/ 近1年历史分位（样本<20 显式标注不足）/ 消化至锚定PE年数（3年TTM "
        "CAGR，负增长不输出）；分位样本不足或亏损股显式标注，不硬造"
    )
    required_columns: ClassVar[List[str]] = []   # 数据来自 env，不依赖 K 线列
    max_lookback: ClassVar[int] = 2
    # 命中统计口径（方案 §2.5）：理由串类，非「否/数据不足」即命中
    hit_rule: ClassVar[str] = 'reason'

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        valuation_map = (ctx.upstream or {}).get('valuation_map')
        if not valuation_map:
            return ModuleResult.insufficient('估值环境未装配（链 YAML 未声明 a_share_valuation）')
        digest = valuation_map.get(ctx.stock.ts_code)
        if digest is None:
            return ModuleResult.insufficient('无当日估值行（stock_valuation_daily）')

        pe = digest.pe_ttm
        if pe is None or pe <= 0:
            return ModuleResult.insufficient('PE-TTM 不可算（亏损或数据缺失）')

        parts = [f'PE-TTM {format_pe(pe)}']
        if digest.pe_percentile is not None:
            parts.append(f"历史{digest.pe_percentile:.0f}%分位")
        else:
            parts.append('分位样本不足')
        if digest.digestion_years is not None:
            parts.append(f'消化{format_years(digest.digestion_years)}')
        label = '/'.join(parts)

        detail: Dict[str, object] = {
            'pe_ttm': round(pe, 2),
            'pb': digest.pb,
            'total_mv': digest.total_mv,
            'pe_percentile': (round(digest.pe_percentile, 1)
                              if digest.pe_percentile is not None else None),
            'percentile_samples': digest.percentile_samples,
            'cagr_pct': (round(digest.cagr_pct, 2)
                         if digest.cagr_pct is not None else None),
            'digestion_years': (round(digest.digestion_years, 2)
                                if digest.digestion_years is not None else None),
            'digestion_reason': digest.digestion_reason,
            'peg': (round(digest.peg_value, 2)
                    if digest.peg_value is not None else None),
            'annualized_pe': (round(digest.annualized_pe, 2)
                              if digest.annualized_pe is not None else None),
            'annualized_reason': digest.annualized_reason,
        }
        return ModuleResult(label, detail)
