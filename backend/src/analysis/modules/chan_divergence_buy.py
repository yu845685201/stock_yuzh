"""缠论背驰买点 ``chan_divergence_buy``（daily_review 链；2026-10-24 引入）。

判定（复用验证项目结构引擎 ``src/chan/``，规约与 2015-2026 全A回测一致）：
锚定日收盘确认的缠论**一买**（下跌两中枢依次下移 + MACD 背驰）或**二买**
（一买后回踩不创新低）。理由串如 ``一买/背驰0.42/触发8.20/中枢[7.10,7.85]``、
``二买/触发8.20/止损7.90/中枢[7.10,7.85]`` / ``否`` / ``数据不足``。

- 仅一买/二买（验证报告 data/chan_validation/report.md：60日费后对随机基线
  超额 +3.87pp / +2.26pp，BH q≈0）；**三买负超额（−2.53pp）不引入**；
- 剔除口径与回测一致：锚定日 ST、K线不足 120 根（次新）——判定主体在
  ``chan.scan.scan_stock_anchor``，与复盘报告层同一实现两入口（对拍保一致）；
- **输入完整性门控（P0-7，2026-10-24）**：引擎按 ``expected_bars``（链引擎按
  ``max(窗口起点, list_date)`` 逐股计算）做窗口覆盖检查——缺日 >2%（约 >10
  天/520 日）→ 数据不足不判定（残缺序列上笔/中枢划分失真，假背驰风险）；
  缺日 ≤2% → 正常判定但理由串尾部带 ``缺日N天`` 标记（报告层表格逐只披露）。
- **向量化红线豁免**：结构引擎为逐K增量计算（分型/笔/中枢的固有形态），
  链内实测全市场 +20~25 秒（520 日窗口）；
- 信号语义：锚定日**收盘确认**，操作口径 = 次日开盘（与验证回测撮合一致）；
  级联下跌中一买约半数失败（历史胜率 53.1%），候选池+人工确认使用。

依赖链窗口：本模块 ``max_lookback=520``（笔中枢结构约需 2 年日K + MACD
warmup 130），引擎会把链窗口自动扩展到 520 日（meta 如实标记）。
"""

from __future__ import annotations

from typing import ClassVar, Dict, List

import pandas as pd

from ...chan.scan import (DEFAULT_MIN_COVERAGE, hit_label, scan_stock_anchor)
from ..module_base import AnalysisModule, ChainContext, ModuleResult


class ChanDivergenceBuy(AnalysisModule):
    """缠论背驰买点：锚定日确认的一买/二买（左侧候选，与 R 系列右侧互斥）。"""

    name: ClassVar[str] = '缠论背驰买点'
    name_en: ClassVar[str] = 'chan_divergence_buy'
    description: ClassVar[str] = (
        "缠论背驰买点（2026-10-24 引入，验证报告 data/chan_validation/report.md）："
        "锚定日收盘确认的一买（下跌两中枢依次下移+MACD背驰）或二买（一买后回踩不创新低）。"
        "理由串如「一买/背驰0.42/触发8.20/中枢[7.10,7.85]」「二买/触发8.20/止损7.90/"
        "中枢[7.10,7.85]」，窗口缺日 ≤2% 时尾部带「/缺日N天」标记（P0-7），"
        "或 否/数据不足/错误。仅一买二买（三买负超额不引入）；锚定日 ST、"
        "K线不足120根不判定；结构引擎逐K计算（向量化红线豁免，全市场+20~25秒）；"
        "max_lookback=520 使链窗口自动扩展（meta 标记）。历史统计：一买60日费后均值+6.8%/"
        "胜率53.1%、二买+5.1%/51.6%——级联中约半数失败，候选池+人工确认使用。"
    )
    required_columns: ClassVar[List[str]] = ['low', 'is_st']
    max_lookback: ClassVar[int] = 520
    hit_rule: ClassVar[str] = 'reason'
    #: P0-7 窗口覆盖率下限（链 YAML params 可覆盖；与 config chan.min_coverage 同口径）
    default_params: ClassVar[Dict[str, Any]] = {'min_coverage': DEFAULT_MIN_COVERAGE}

    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        # P0-7 输入完整性门控：expected_bars 由引擎按 max(窗口起点, list_date)
        # 逐股计算；缺日 >2% → 数据不足，≤2% → 命中但带缺日标记
        status, hit, flag = scan_stock_anchor(
            df, ctx.as_of, expected_bars=ctx.expected_bars,
            min_coverage=float(self.params.get('min_coverage') or DEFAULT_MIN_COVERAGE))
        if status == 'insufficient':
            return ModuleResult.insufficient(flag)
        if status == 'no':
            return ModuleResult('否', {'reason': flag} if flag else None)
        assert hit is not None
        label = hit_label(hit)
        if flag:                      # 缺日标记并入理由串尾部（报告层对拍同规则）
            label = f"{label}/{flag}"
        detail = {
            'kind': hit.kind, 'stop': hit.stop_price, 'trigger_low': hit.trigger_low,
            'zg': hit.zg, 'zd': hit.zd, 'pivot_pid': hit.pivot_pid,
            'refire': hit.refire, 'divergence': hit.divergence,
            'data_flag': flag,
        }
        return ModuleResult(label, detail)
