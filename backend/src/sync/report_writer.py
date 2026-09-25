"""
统一 Markdown 报告生成器（R-07b 抽取，收敛 4 处同构的报告收尾逻辑）

- 目录推导单点化：resolve_repo_root()（仓库根 stock_yuzh/）
- 落盘收尾单点化：write_markdown_report()（makedirs / 前缀+时间戳命名 / 写文件 / 异常 log+返回 None）
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


def resolve_repo_root() -> Path:
    """仓库根（stock_yuzh/）。

    本文件位于 backend/src/sync/，parents[3] 即仓库根。
    """
    return Path(__file__).resolve().parents[3]


def write_markdown_report(
    lines: List[str],
    file_prefix: str,
    log_label: str = '报告',
    ts: Optional[str] = None,
) -> Optional[str]:
    """将 lines 写入 <repo_root>/doc/reports/<file_prefix>_<YYYYmmdd_HHMMSS>.md。

    Args:
        lines: 报告内容行（逐字保留各调用方原文案）
        file_prefix: 文件名前缀
        log_label: 异常日志中的报告名称（保持各调用方原日志文案）
        ts: 时间戳；不传则取当前时间（调用方需在内容中引用同一时间戳时传入）

    Returns:
        报告文件路径；失败时记录错误日志并返回 None
    """
    try:
        if ts is None:
            ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_dir = resolve_repo_root() / 'doc' / 'reports'
        report_dir.mkdir(parents=True, exist_ok=True)
        report_path = report_dir / f"{file_prefix}_{ts}.md"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        return str(report_path)
    except Exception as e:
        logger.error(f"生成{log_label}失败: {e}")
        return None
