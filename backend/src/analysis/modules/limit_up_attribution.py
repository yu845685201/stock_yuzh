"""M31 ``limit_up_attribution`` 涨停归因（P1-11，方案 §3.4）。

仅对锚定日**涨停股**输出（读前置模块 ``是否涨停`` 结果，非涨停 → ``否``）；
归因规则与报告层涨停榜「归因」列共用 ``attribution_rules``（原则 P6）。

取值（理由串，§2.5 reason 口径）：
- ``题材催化(中标)`` / ``业绩预增`` 等：窗口内有利好公告（关键词 > 利好类型）；
- ``题材炒作或跟风上涨``：涨停但窗口内无利好公告（利好=0 显式结论，不硬凑）；
- ``否``：非涨停股；
- ``数据不足``：前置模块未运行 / 公告环境未装配。

覆盖断代：锚定日 < 20260928 时输出附「(公告覆盖不全)」——非定期报告公告
自该日起才有量（实测），此前仅定期报告，归因可信度低。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..attribution_rules import coverage_note, pick_attribution
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class LimitUpAttribution(AnalysisModule):
    """涨停归因（公告窗口 × 利好关键词/类型，理由串）。"""

    name: ClassVar[str] = '涨停归因'
    name_en: ClassVar[str] = 'limit_up_attribution'
    description: ClassVar[str] = (
        "涨停股的公告归因（仅涨停股输出）：窗口内利好公告按关键词>类型取首"
        "（题材催化(中标)/业绩预增…），无利好显式记题材炒作或跟风上涨（不硬凑）；"
        "非涨停记否；锚定日早于 20260928 附公告覆盖不全。归因是市场叙事非核验因果"
    )
    required_columns: ClassVar[List[str]] = []   # 读前置模块 + env，不依赖 K 线列
    max_lookback: ClassVar[int] = 1
    # 命中统计口径（方案 §2.5）：理由串类，非「否/数据不足」即命中
    hit_rule: ClassVar[str] = 'reason'

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        upstream = ctx.upstream or {}
        limit_up = upstream.get('是否涨停')
        if limit_up is None:
            return ModuleResult.insufficient('前置模块「是否涨停」未运行')
        if limit_up.value != '是':
            return ModuleResult('否', {'limit_up': False})

        ann_map = upstream.get('cand_announcement_map')
        if ann_map is None:
            return ModuleResult.insufficient('公告环境未装配（链 YAML 未声明 a_share_candidates）')

        announcements = ann_map.get(ctx.stock.ts_code) or []
        label, kind, source = pick_attribution(announcements)
        if ctx.as_of and coverage_note(ctx.as_of):
            label += '(公告覆盖不全)'

        detail: Dict[str, object] = {
            'limit_up': True,
            'kind': kind,
            'ann_count': len(announcements),
            'source': ({'ann_id': source.get('ann_id'),
                        'ann_date': source.get('ann_date'),
                        'ann_type': source.get('ann_type'),
                        'title': (source.get('title') or '')[:40]}
                       if source else None),
            'coverage_full': not (ctx.as_of and coverage_note(ctx.as_of)),
        }
        return ModuleResult(label, detail)
