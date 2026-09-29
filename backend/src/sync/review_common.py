"""
每日复盘数据采集共享工具（各 manager 公共件）

- code6_to_ts_code：6 位代码 -> ts_code（sh./sz./bj. 前缀，与 limit_up_rules 板块口径一致）
- load_manifest / save_manifest：全量任务断点（tmp/*.manifest.json，原子写 .tmp+replace，
  禁止手工改动——沿用 fundamentals_rebuild 惯例）
- write_review_report：统一 sync report 骨架（data/reports/，沿用 report_writer）
- ReviewSyncError：manager 通用失败（区别于风控急停 ReviewBlockedError）
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .report_writer import resolve_repo_root, write_markdown_report

logger = logging.getLogger(__name__)


class ReviewSyncError(RuntimeError):
    """review 采集通用失败（网络耗尽/校验不过/结构变更等，不触发冷却）"""


def code6_to_ts_code(code: str) -> str:
    """6 位股票/指数代码 -> ts_code 前缀（60/68 开头 sh.，0/3 开头 sz.，4/8/92 开头 bj.）"""
    c = str(code).strip()
    if c.startswith(('60', '68')):
        return f'sh.{c}'
    if c.startswith(('0', '3')):
        return f'sz.{c}'
    if c.startswith(('4', '8', '92')):
        return f'bj.{c}'
    return f'sz.{c}'


def ts_code_to_code6(ts_code: str) -> str:
    """sz.000001 / 000001.SZ / 000001 -> 6 位代码"""
    c = str(ts_code).strip().lower()
    for prefix in ('sh.', 'sz.', 'bj.'):
        if c.startswith(prefix):
            return c[len(prefix):].upper()
    for suffix in ('.sh', '.sz', '.bj'):
        if c.endswith(suffix):
            return c[:-3].upper()
    return c.upper()


def load_manifest(path: Path) -> Dict[str, Any]:
    """读 manifest（不存在/损坏时重建；每完成一只保存，急停可续）"""
    if path.exists():
        try:
            return json.loads(path.read_text(encoding='utf-8'))
        except Exception as e:
            logger.warning(f'manifest 读取失败，重建: {e}')
    return {'stocks': {}, 'updated_at': None}


def save_manifest(path: Path, manifest: Dict[str, Any]) -> None:
    """原子写 manifest（.tmp + replace）"""
    manifest['updated_at'] = datetime.now().isoformat(timespec='seconds')
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding='utf-8')
    tmp.replace(path)


def write_review_report(title: str, file_prefix: str, status: str, lines: List[str],
                        duration_seconds: Optional[float] = None) -> Optional[str]:
    """统一报告骨架：标题 + 状态 + 自定义行 + 耗时，落 data/reports/"""
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_lines = [f'# {title} {ts}', '', f'- 状态：{status}']
    report_lines.extend(lines)
    if duration_seconds is not None:
        report_lines.append(f'- 耗时：{duration_seconds / 60:.1f} 分钟')
    report_lines.append('')
    return write_markdown_report(report_lines, file_prefix, log_label=title, ts=ts)


def tmp_dir() -> Path:
    """仓库根 tmp/（manifest 与触发清单所在）"""
    return resolve_repo_root() / 'tmp'
