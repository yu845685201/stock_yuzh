"""日期工具函数（R-09 自 data_reader.py 逐字移动）。"""

from typing import List, Optional, Sequence


def _dotted(value: str) -> str:
    v = str(value).replace("-", "")
    return f"{v[:4]}-{v[4:6]}-{v[6:]}"


def _plain_date(value) -> str:
    if hasattr(value, "strftime"):
        return value.strftime("%Y%m%d")
    return str(value).replace("-", "")


def _gap_runs(expected: Sequence[str], actual: set) -> List[tuple]:
    """把「期望交易日中存在、实际缺失」的日子切成连续段。

    ``expected`` 本身是**连续交易日列表**（不含周末/节假日），因此相邻缺失日
    在 ``expected`` 中必然相邻，无需再做索引连续性判断。

    Returns:
        ``[(段首在 expected 中的下标, 段内交易日数), ...]``，按时间升序。
    """
    runs: List[tuple] = []
    run_start: Optional[int] = None
    run_len = 0
    for j, day in enumerate(expected):
        if day in actual:
            if run_start is not None:
                runs.append((run_start, run_len))
                run_start, run_len = None, 0
            continue
        if run_start is None:
            run_start, run_len = j, 1
        else:
            run_len += 1
    if run_start is not None:
        runs.append((run_start, run_len))
    return runs
