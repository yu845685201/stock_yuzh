"""分析窗口（Lookback）：锚定日往前加载多久的日 K 线。

链 YAML 通过 ``lookback`` 字段配置，例如::

    chain: limit_up_scan
    lookback: 1y          # 默认值：只分析最近 1 年的日K线

支持的写法：

| 写法 | 含义 |
|---|---|
| `1y` / `2y` | 自然年（锚定日往前 1 / 2 个自然年） |
| `6m` / `3m` | 自然月 |
| `250d` / `120d` | **交易日**数 |
| `250`（纯数字） | 交易日数 |
| `0` / `all` | 全历史 |

语义说明：
- 自然年/月按**日历日期**回溯（`20260923` − `1y` = `20250923`），
  窗口内包含该区间全部交易日；月长不一，故 1y ≈ 243 个交易日而非恰好 250。
- 交易日数按 `base_trade_calendar` 精确回溯（不受长假影响）。
- 实际加载窗口 = ``max(链 lookback, 各模块 effective_lookback())``，
  保证模块回看需求不被链窗口截断。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

ALL = "all"
DAYS = "days"
MONTHS = "months"
YEARS = "years"

_VALID_KINDS = {ALL, DAYS, MONTHS, YEARS}

#: 链 YAML 未配置 lookback 时的默认窗口（1 年）
DEFAULT_SPEC = "1y"

_PATTERN = re.compile(r"^(\d+)\s*([dmyDMY]?)$")
_ALIAS_ALL = {"all", "*", "full", "全历史", "全部", "0"}


@dataclass(frozen=True)
class Lookback:
    """分析窗口规格。"""

    kind: str
    value: int = 0
    raw: str = ""

    # ------------------------------------------------------------------ 构造
    @classmethod
    def parse(cls, spec: Any) -> "Lookback":
        """解析配置值。非法写法直接抛错（链配置在加载阶段即校验）。"""
        if isinstance(spec, Lookback):
            return spec
        if spec is None:
            raise ValueError("lookback 不能为空；如需全历史请写 0 或 all")

        text = str(spec).strip()
        if text.lower() in _ALIAS_ALL:
            return cls(ALL, 0, ALL)
        if text.lower() == "0d":
            return cls(ALL, 0, ALL)

        match = _PATTERN.match(text)
        if not match:
            raise ValueError(
                f"非法 lookback: {spec!r}；支持 1y / 2y / 6m / 3m / 250d / 纯数字(交易日) / 0(all)"
            )
        value = int(match.group(1))
        unit = (match.group(2) or "d").lower()
        kind = {"d": DAYS, "m": MONTHS, "y": YEARS}[unit]
        if value <= 0:
            return cls(ALL, 0, ALL)
        return cls(kind, value, f"{value}{unit}")

    @classmethod
    def default(cls) -> "Lookback":
        """链未配置时的默认窗口（1 年日 K 线）。"""
        return cls.parse(DEFAULT_SPEC)

    @classmethod
    def days(cls, n: int) -> "Lookback":
        return cls(DAYS, int(n), f"{int(n)}d")

    # ------------------------------------------------------------------ 计算
    def calendar_start(self, as_of: str) -> Optional[str]:
        """自然年/月的窗口起始日（yyyyMMdd）。

        ``days`` 类型需要交易日历，本方法返回 None，由 ``DataReader`` 处理。
        """
        if self.kind == ALL:
            return None
        if self.kind == DAYS:
            return None
        from dateutil.relativedelta import relativedelta

        base = datetime.strptime(str(as_of), "%Y%m%d").date()
        if self.kind == MONTHS:
            start = base - relativedelta(months=self.value)
        elif self.kind == YEARS:
            start = base - relativedelta(years=self.value)
        else:  # pragma: no cover - 防御性
            return None
        return start.strftime("%Y%m%d")

    @property
    def is_all(self) -> bool:
        return self.kind == ALL

    def __str__(self) -> str:
        return self.raw or (ALL if self.kind == ALL else f"{self.value}{self.kind[0]}")
