"""进度跟踪（方案 §3.6，补充 4）。

长任务（分钟级）必须让 agent 能定时汇报进度，机制分三层：
- 引擎侧：定期输出 stdout 进度行 + 写 ``.progress.json``；
- CLI 侧：``analyze progress --chain <链名>`` 读进度文件并格式化输出；
- Skill 侧：后台执行 + 定时调用 ``analyze progress`` 汇报。
"""

from __future__ import annotations

import json
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class ProgressTracker:
    """进度跟踪：每 N 只股票（或每 M%，先到者为准）刷新一次。

    Attributes:
        chain: 链名。
        as_of: 锚定日。
        total: 股票总数。
        path: ``.progress.json`` 落盘路径。
        interval_stocks: 每完成多少只输出一次。
        interval_percent: 每完成多少个百分点输出一次。
    """

    chain: str
    as_of: str
    total: int
    path: Path
    interval_stocks: int = 200
    interval_percent: float = 5.0
    done: int = 0
    warnings: int = 0
    status: str = "running"
    error: Optional[str] = None
    csv_path: Optional[str] = None
    _started: float = field(default_factory=time.monotonic, init=False)
    _next_at: int = field(default=0, init=False)
    _started_at: str = field(default="", init=False)

    def __post_init__(self) -> None:
        self.path = Path(self.path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._started_at = datetime.now().isoformat(timespec="seconds")
        self._next_at = self._first_interval()
        self._write()

    # ------------------------------------------------------------------ 内部
    def _first_interval(self) -> int:
        by_count = max(self.interval_stocks, 1)
        by_percent = max(int(self.total * self.interval_percent / 100.0), 1)
        return min(by_count, by_percent)

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self._started

    def _eta(self) -> Optional[float]:
        if self.done <= 0:
            return None
        remaining = max(self.total - self.done, 0)
        return self.elapsed / self.done * remaining

    def _snapshot(self, status: str) -> Dict[str, Any]:
        percent = round(self.done / self.total * 100, 1) if self.total else 100.0
        eta = self._eta()
        payload: Dict[str, Any] = {
            "chain": self.chain,
            "as_of": self.as_of,
            "status": status,
            "total": self.total,
            "done": self.done,
            "percent": percent,
            "started_at": self._started_at,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "elapsed_seconds": round(self.elapsed, 1),
            "eta_seconds": round(eta, 0) if eta is not None else None,
            "warnings": self.warnings,
            "error": self.error,
        }
        if self.csv_path:
            payload["csv"] = self.csv_path
        return payload

    def _write(self, status: Optional[str] = None) -> Dict[str, Any]:
        payload = self._snapshot(status or self.status)
        tmp = self.path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.path)
        return payload

    # ------------------------------------------------------------------ 接口
    def tick(self, count: int = 1) -> None:
        """每完成一只股票调用一次；到间隔点时刷 stdout 与进度文件。"""
        self.done += count
        if self.done >= self._next_at or self.done >= self.total:
            self._flush_stdout()
            self._next_at = min(self.done + self._first_interval(), max(self.total, 1))
            if self.done < self.total:
                self._write()

    def warn(self, count: int = 1) -> None:
        self.warnings += count

    def finish(self, csv_path: Optional[str] = None, status: str = "done") -> None:
        self.status = status
        self.csv_path = csv_path
        self._flush_stdout(status)
        self._write(status)

    def fail(self, error: str) -> None:
        self.status = "failed"
        self.error = error
        self._write("failed")

    def _flush_stdout(self, status: Optional[str] = None) -> None:
        percent = self.done / self.total * 100 if self.total else 100.0
        eta = self._eta()
        tail = f"，预计剩余 {eta:.0f}s" if eta is not None else ""
        mark = "" if status is None else f" [{status}]"
        sys.stdout.write(
            f"[{self.chain}] 进度 {self.done}/{self.total} ({percent:.1f}%)，"
            f"已耗时 {self.elapsed:.0f}s{tail}{mark}\n"
        )
        sys.stdout.flush()


def read_progress(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def format_progress(payload: Dict[str, Any]) -> str:
    """把进度 JSON 格式化为人类/agent 可读的多行文本。"""
    status_cn = {"running": "运行中", "done": "已完成", "failed": "失败"}.get(
        payload.get("status", ""), payload.get("status", "未知")
    )
    lines = [
        f"链: {payload.get('chain')}  锚定日: {payload.get('as_of')}  状态: {status_cn}",
        f"进度: {payload.get('done')}/{payload.get('total')} ({payload.get('percent')}%)",
        f"已耗时: {payload.get('elapsed_seconds')}s"
        + (f"  预计剩余: {payload['eta_seconds']:.0f}s" if payload.get("eta_seconds") is not None else ""),
        f"更新时间: {payload.get('updated_at')}",
    ]
    if payload.get("warnings"):
        lines.append(f"存疑股票: {payload['warnings']} 只")
    if payload.get("csv"):
        lines.append(f"CSV: {payload['csv']}")
    if payload.get("error"):
        lines.append(f"错误: {payload['error']}")
    return "\n".join(lines)
