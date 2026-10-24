"""``money_trend`` 个股资金趋势（量价资金方案 V1.1 §3.1②）。

数据源：环境装配 ``mf_history_map``（money_flow_daily 最近 10 行，引擎装配进
upstream——模块不查库，纯函数红线不变）。**口径注记**（P1-7 动态）：行级口径受
money_flow_daily.calib_note 约束——20260917 起全市场分笔分桶；历史行为异动子集
近似口径，跨期解读以当日 calib_note 为准。

计算：今日大单净额、5 日累计净额、连续净流入天数（big_net_amt>0 计数）。

返回值：``净流入{X}亿/连{n}天`` / ``净流入{X}亿`` / ``净流出{X}亿`` / ``否``
（无资金行）/ ``数据不足``（环境装配缺失）。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ..module_base import AnalysisModule, ChainContext, ModuleResult

#: 历史窗口（5 日累计 + 连续天数计数基准）
_HISTORY_N = 10


class MoneyTrend(AnalysisModule):
    """个股资金趋势（今日/5日累计大单净额与连续流入天数）。"""

    name: ClassVar[str] = '资金趋势'
    name_en: ClassVar[str] = 'money_trend'
    description: ClassVar[str] = (
        "个股资金趋势：今日大单净额、5 日累计净额与连续净流入天数（money_flow_daily "
        "big_net_amt>0 计数），数据来自环境装配 mf_history_map（链须配 "
        "env: a_share_candidates）。返回理由串（如「净流入1.20亿/连3天」「净流出0.50亿」）"
        "或 否（无资金行）/ 数据不足；行级口径受 calib_note 约束（20260917 起全市场，历史为异动子集）"
    )
    required_columns: ClassVar[List[str]] = []
    default_params: ClassVar[Dict[str, object]] = {}
    # 命中统计口径（方案 §2.5）：理由串类，非「否 / 数据不足」即命中
    hit_rule: ClassVar[str] = 'reason'

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        history_map = ctx.upstream.get('mf_history_map')
        if history_map is None:
            return ModuleResult.insufficient('缺环境装配 mf_history_map（链未声明 env）')
        history = history_map.get(ctx.stock.ts_code) or []
        if not history:
            return ModuleResult('否')
        # history: [(trade_date, big_net_amt)] 升序
        recent = [float(net) for _, net in history[-_HISTORY_N:] if net is not None]
        if not recent:
            return ModuleResult('否')
        today = recent[-1]
        cum5 = sum(recent[-5:])
        streak = 0
        for net in reversed(recent):
            if net > 0:
                streak += 1
            else:
                break
        detail: Dict[str, object] = {'today_net': today, 'cum5': cum5,
                                     'streak': streak, 'rows': len(recent)}
        if today > 0:
            label = f'净流入{today / 1e8:.2f}亿'
            label += f'/连{streak}天' if streak >= 2 else ''
        else:
            label = f'净流出{abs(today) / 1e8:.2f}亿'
        return ModuleResult(label, detail)
