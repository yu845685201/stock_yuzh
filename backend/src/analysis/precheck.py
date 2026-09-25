"""数据预检规则（R3）（R-09 自 data_reader.py 逐字移动为独立模块）。

规则与分流语义见 ``run_precheck`` docstring；交易日历数据访问仍由 DataReader 提供
（``run_precheck`` 接收 reader，调用其 ``_load_trading_days``）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

import pandas as pd

from .limit_up_rules import annotate_limit_up, board_of
from .module_base import StockInfo
from .date_utils import _dotted, _plain_date, _gap_runs

#: 存疑原因串最大条数（超出截断，避免 CSV 列爆长）
SUSPECT_REASON_LIMIT = 5

#: 缺日处理模式
MISSING_MODE_GAP = "gap"   # 只把「孤立单日缺行」判为存疑；连续多日缺行按疑似停牌处理
MISSING_MODE_ALL = "all"   # 方案原文：所有交易日缺行都判为存疑
MISSING_MODE_OFF = "off"   # 不做缺日检测


@dataclass
class PrecheckResult:
    """数据预检结果。

    Attributes:
        suspects: ``{ts_code: [存疑原因...]}``，写入 CSV 的「是否存疑/存疑原因」。
        suspended: ``{ts_code: [疑似停牌明细...]}``，不计入存疑，仅聚合进运行元信息。
        break_count: 前复权断裂命中行数。
        missing_count: 计入存疑的缺日条数。
    """

    suspects: Dict[str, List[str]] = field(default_factory=dict)
    suspended: Dict[str, List[str]] = field(default_factory=dict)
    break_count: int = 0
    missing_count: int = 0

    @property
    def suspended_stock_count(self) -> int:
        return len(self.suspended)


def run_precheck(
    reader,
    kdf: pd.DataFrame,
    as_of: str,
    start_date: Optional[str] = None,
    break_tolerance_pct: float = 0.6,
    check_missing_day: bool = True,
    universe: Optional[Sequence[StockInfo]] = None,
    reason_limit: int = SUSPECT_REASON_LIMIT,
    missing_day_mode: str = MISSING_MODE_GAP,
) -> PrecheckResult:
    """数据预检（R3）。

    **规则 1「前复权断裂」**：``|change_rate| > 制度上限 + 容差``。
        方案原定规则为「相邻交易日 ``close[t-1]`` 与 ``preclose[t]`` 相对差 > 0.1%」，
        但实测全表 ``preclose == lag(close)`` 占 **99.987%**，该规则恒不触发。
        改用「涨跌幅突破制度上限」这一**可证伪**的一致性规则——涨跌幅在物理上
        不可能超过涨跌停制度上限，突破即为前复权序列基准断裂。
        该规则只能捕捉「超过上限」的断裂，故属于**存疑下界**。

    **规则 2「缺行」**：交易日历内缺失 K 线行。按 ``missing_day_mode`` 分流：
        - ``gap``（默认）：**孤立单日缺行**判为存疑（疑似数据缺口）；
          **连续 ≥2 交易日缺行**归入「疑似停牌」，不计入存疑。
          依据：停牌是市场事实而非数据缺陷，模块按可得行计算，涨停/连板判定不受影响；
          实测连续 ≥2 日缺行呈明显的 5 日 / 10 日簇（典型停牌形态），若一律判存疑，
          会有 8.7% 的股票被标记，淹没真正的数据问题。
        - ``all``：方案原文行为，所有缺行都判存疑。
        - ``off``：不做缺行检测。

    Args:
        reader: 提供 ``_load_trading_days`` 的 DataReader（交易日历数据访问保留在数据层）。
        kdf: ``load_kline`` 结果（含 ts_code/stock_code/trade_date/change_rate/close/high/is_st）。
        as_of: 锚定日 yyyyMMdd。
        start_date: 预检窗口起始日，应与 ``load_kline`` 一致；``None`` = 全历史。
        break_tolerance_pct: 断裂容差（百分点）。
        check_missing_day: 是否执行缺行检测。
        universe: 股票池（提供 list_date，用于把缺行检测限定在上市之后）。
        reason_limit: 单只股票保留的存疑原因条数上限。
        missing_day_mode: 缺行分流模式（gap/all/off）。
    """
    result = PrecheckResult()
    if kdf.empty:
        return result
    if missing_day_mode not in {MISSING_MODE_GAP, MISSING_MODE_ALL, MISSING_MODE_OFF}:
        raise ValueError(f"非法 missing_day_mode: {missing_day_mode}")

    info_by_ts = {s.ts_code: s for s in (universe or [])}
    do_missing = bool(check_missing_day) and missing_day_mode != MISSING_MODE_OFF
    trading_days = reader._load_trading_days(as_of, start_date) if do_missing else []
    expected_all = set(trading_days)

    for ts_code, g in kdf.groupby("ts_code", sort=False):
        stock = info_by_ts.get(ts_code)
        stock_code = str(g["stock_code"].iloc[0])
        board = stock.board if stock else board_of(stock_code)

        # ---- 规则 1：前复权断裂 ----
        ann = annotate_limit_up(g, stock_code, board)
        pct = ann["_limit_pct"]
        cr = pd.to_numeric(ann["change_rate"], errors="coerce")
        broke = pct.notna() & cr.notna() & cr.abs().gt(pct + break_tolerance_pct)
        reasons = [
            f"{d}前复权断裂(涨跌幅{c:.2f}%超出制度上限±{p:.2f}%)"
            for d, c, p in zip(ann.loc[broke, "trade_date"], cr[broke], pct[broke])
        ]
        result.break_count += len(reasons)

        # ---- 规则 2：缺行（按缺口连续段分流）----
        if expected_all:
            actual = set(g["trade_date"].astype(str))
            ordered = sorted(actual)
            start = max(ordered[0], stock.list_date or "") if stock else ordered[0]
            exp = [d for d in trading_days if d >= start]
            for j0, gap_len in _gap_runs(exp, actual):
                if missing_day_mode == MISSING_MODE_ALL:
                    # 方案原文行为：逐日列出每条缺失
                    for k in range(gap_len):
                        j = j0 + k
                        prev = exp[j - 1] if j > 0 else "无"
                        reasons.append(f"{exp[j]}缺日(上一交易日{prev})")
                    result.missing_count += gap_len
                elif gap_len == 1:
                    prev = exp[j0 - 1] if j0 > 0 else "无"
                    reasons.append(f"{exp[j0]}缺日(上一交易日{prev})")
                    result.missing_count += 1
                else:
                    result.suspended.setdefault(ts_code, []).append(
                        f"{exp[j0]}起连续{gap_len}个交易日无K线"
                    )

        if reasons:
            if len(reasons) > reason_limit:
                total = len(reasons)
                reasons = reasons[:reason_limit] + [f"...共{total}处"]
            result.suspects[ts_code] = reasons

    return result
