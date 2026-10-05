"""复盘报告层（复盘报告层与 P0 修复技术方案 V1.0 §5，M1/M2）

- ``cross_section``  向量化全市场截面判定（与链模块同口径，§5.7 对拍保证）
- ``aggregator``     六维聚合（独立重算，不依赖历史链 CSV）
- ``sections/``      六维章节纯函数（聚合结果 → 块结构）
- ``renderer``       块 → MD/HTML（内联 CSS，零外部依赖）
- ``generator``      编排：取数 → 聚合 → 渲染 → 落盘
"""

from .generator import ReviewReportGenerator
from .renderer import blocks_to_html, blocks_to_markdown, missing_note

__all__ = [
    'ReviewReportGenerator',
    'blocks_to_markdown',
    'blocks_to_html',
    'missing_note',
]
