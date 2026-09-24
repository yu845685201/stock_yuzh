"""结果写出：CSV + 运行元信息（方案 §2.5 / §3.8）。

CSV schema（列顺序固定）::

    股票代码, 股票名称, 是否存疑, 存疑原因, <模块1列名>, ..., <模块N列名>

- 编码：UTF-8 with BOM（Excel 直接双击可读）；
- 输出路径：``<output_dir>/<链名>_<锚定日>.csv``；
- 同锚定日重复运行结果逐字节一致（稳定性要求，§3.10 验证标准 2）。
"""

from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)

ID_COLUMNS = ["股票代码", "股票名称", "是否存疑", "存疑原因"]


@dataclass
class RunMeta:
    """运行元信息（写入 ``.meta.json``）。"""

    chain: str
    as_of: str
    modules: List[str]
    started_at: str
    finished_at: str
    duration_seconds: float
    stock_count: int
    suspect_count: int
    #: 分析窗口规格（链 YAML 的 lookback），如 ``1y`` / ``250d`` / ``all``
    lookback: str
    #: 窗口起始日 yyyyMMdd，空串 = 全历史
    window_start: str
    #: 窗口内实际交易日数
    loaded_trading_days: int = 0
    #: 窗口是否因模块回看需求被扩展（链 lookback < 模块 max_lookback）
    window_expanded: bool = False
    break_count: int = 0
    missing_count: int = 0
    suspended_count: int = 0
    suspended_detail: Optional[Dict[str, List[str]]] = None
    version: str = "1.0"


class ResultWriter:
    """CSV 与元信息写出。"""

    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def resolve_paths(self, chain: str, as_of: str) -> Dict[str, Path]:
        stem = f"{chain}_{as_of}"
        return {
            "csv": self.output_dir / f"{stem}.csv",
            "progress": self.output_dir / f"{stem}.progress.json",
            "meta": self.output_dir / f"{stem}.meta.json",
        }

    def write_csv(
        self,
        chain: str,
        as_of: str,
        module_names: Sequence[str],
        rows: Sequence[Dict[str, Any]],
    ) -> Path:
        path = self.resolve_paths(chain, as_of)["csv"]
        header = [*ID_COLUMNS, *module_names]
        with path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(header)
            for row in rows:
                writer.writerow([_cell(row.get(col, "")) for col in header])
        logger.info("CSV 已写出: %s（%d 行）", path, len(rows))
        return path

    def write_meta(self, chain: str, as_of: str, meta: RunMeta) -> Path:
        path = self.resolve_paths(chain, as_of)["meta"]
        payload = {
            "chain": meta.chain,
            "as_of": meta.as_of,
            "generator_version": meta.version,
            "modules": list(meta.modules),
            "started_at": meta.started_at,
            "finished_at": meta.finished_at,
            "duration_seconds": round(meta.duration_seconds, 2),
            "stock_count": meta.stock_count,
            "suspect_count": meta.suspect_count,
            "lookback": meta.lookback,
            "window_start": meta.window_start,
            "window_trading_days": meta.loaded_trading_days,
            "window_expanded_by_modules": meta.window_expanded,
            "precheck": {
                "break_count": meta.break_count,
                "missing_day_count": meta.missing_count,
                "suspended_stock_count": meta.suspended_count,
                "suspended_detail_sample": meta.suspended_detail or {},
            },
            "csv": str(self.resolve_paths(chain, as_of)["csv"]),
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return path


def _cell(value: Any) -> Any:
    """CSV 单元格取值：None/NaN → 空串；其余原样（数值不加引号）。"""
    if value is None:
        return ""
    try:
        import math

        if isinstance(value, float) and math.isnan(value):
            return ""
    except (TypeError, ValueError):  # pragma: no cover - 防御性
        pass
    return value


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")
