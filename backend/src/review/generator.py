"""复盘报告生成器（复盘报告层方案 §5.2/§5.4/§5.5，M1）。

编排：取数（DataReader 60d 窗口）→ 聚合（六维）→ 渲染（MD+HTML）→ 落盘。
- 输出：config review.output_dir（默认 doc/reports/），文件名
  ``daily_review_YYYYMMDD.md/.html`` + ``.meta.json``（耗时等元信息）；
- **同锚定日重复生成逐字节一致**：正文不出现运行时间戳（§5.4）；
- 链 CSV 缺失自动降级：明细章节不受影响（独立重算），CSV 存在时做涨停家数交叉校验。
"""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from ..analysis.data_reader import DataReader
from .aggregator import ReviewAggregator
from .renderer import blocks_to_html, blocks_to_markdown
from .sections import conclusion, fundamentals, market_env, sector_mainline, sentiment, stock_movers

logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = 'doc/reports'


class ReviewReportGenerator:
    """每日复盘报告生成（六维 → MD/HTML）"""

    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        review_cfg = self.config.get('review') or {}
        out_dir = review_cfg.get('output_dir') or DEFAULT_OUTPUT_DIR
        root = self._repo_root()
        self.output_dir = Path(out_dir) if Path(out_dir).is_absolute() else root / out_dir

    def generate(self, as_of: Optional[str] = None,
                 output_dir: Optional[str] = None) -> Dict[str, Any]:
        """生成报告；返回 {md_path, html_path, meta_path, as_of, duration_seconds}"""
        started = time.time()
        out_dir = Path(output_dir) if output_dir else self.output_dir
        out_dir.mkdir(parents=True, exist_ok=True)

        reader = DataReader(self.config_manager)
        try:
            as_of = reader.resolve_as_of(as_of)
            aggregator = ReviewAggregator(reader, self.config)
            agg = aggregator.aggregate(as_of)
        finally:
            reader.close()

        blocks = self._render_blocks(agg, self.config.get('review') or {})
        md_text = blocks_to_markdown(blocks)
        html_text = blocks_to_html(f'每日复盘 {self._fmt_date(as_of)}', blocks)

        stem = f'daily_review_{as_of}'
        md_path = out_dir / f'{stem}.md'
        html_path = out_dir / f'{stem}.html'
        meta_path = out_dir / f'{stem}.meta.json'
        md_path.write_text(md_text, encoding='utf-8')
        html_path.write_text(html_text, encoding='utf-8')
        duration = time.time() - started
        meta = {
            'as_of': as_of,
            'prev_date': agg.get('prev_date'),
            'window_start': agg.get('window_start'),
            'panel_rows': agg.get('panel_rows'),
            'stock_count': agg.get('stock_count'),
            'generator_version': '1.0',
            'generated_at': datetime.now().isoformat(timespec='seconds'),  # 仅 meta，不进正文
            'duration_seconds': round(duration, 1),
            'output_dir': str(out_dir),
        }
        cross = ((agg.get('movers') or {}).get('data') or {}).get('cross_check')
        if cross:
            meta['cross_check'] = cross
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2),
                             encoding='utf-8')
        logger.info(f'复盘报告已生成: {md_path}（{duration:.1f}s）')
        return {'md_path': str(md_path), 'html_path': str(html_path),
                'meta_path': str(meta_path), 'as_of': as_of,
                'duration_seconds': round(duration, 1)}

    # ------------------------------------------------------------------ 章节组装

    @staticmethod
    def _render_blocks(agg: Dict[str, Any], review_cfg: Dict[str, Any]) -> list:
        """报告头 + 六维章节 + 交叉校验尾注"""
        as_of = agg['as_of']
        blocks = [
            {'type': 'heading', 'level': 1,
             'text': f'每日复盘报告（锚定日 {as_of}）'},
            {'type': 'para',
             'text': f"覆盖全市场 {agg.get('stock_count', 0)} 只个股（含北交所），"
                     f"指标窗口 60 交易日（起点 {agg.get('window_start') or '全历史'}，"
                     f"面板 {agg.get('panel_rows', 0)} 行）；"
                     f"上一交易日 {agg.get('prev_date') or '—'}。"},
            {'type': 'note',
             'text': '本报告指标由报告层基于库内日K独立重算（不依赖当日链运行）；'
                     '涨停/连板/炸板等判定与 daily_review 链模块同口径。'
                     '各章节数据口径详见 doc/复盘框架.MD。'},
            *market_env.build(agg.get('market_env'), agg.get('sentiment'), agg.get('margin')),
            *sentiment.build(agg.get('sentiment')),
            *sector_mainline.build(agg.get('sector')),
            *stock_movers.build(agg.get('movers'), agg.get('lhb'), agg.get('money_flow')),
            *fundamentals.build(agg.get('fundamentals'), agg.get('announcement')),
            *conclusion.build(agg, review_cfg),
        ]
        cross = ((agg.get('movers') or {}).get('data') or {}).get('cross_check')
        if cross:
            if cross.get('ok') is True:
                blocks.append({'type': 'note',
                               'text': f"✓ 交叉校验：重算涨停家数 {cross['recomputed_limit_up']} "
                                       f"与链 CSV 一致"})
            elif cross.get('ok') is False:
                blocks.append({'type': 'note',
                               'text': f"⚠ 交叉校验告警：重算涨停 {cross['recomputed_limit_up']} 家 "
                                       f"vs CSV {cross['csv_limit_up']} 家不一致"
                                       '（可能是链运行时点与库内数据不一致，请复跑链后重出报告）'})
            elif cross.get('note'):
                blocks.append({'type': 'note', 'text': f'交叉校验：{cross["note"]}'})
        return blocks

    @staticmethod
    def _fmt_date(ymd: str) -> str:
        return f'{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}' if len(str(ymd)) == 8 else str(ymd)

    def _repo_root(self) -> Path:
        from ..sync.report_writer import resolve_repo_root
        return resolve_repo_root()
