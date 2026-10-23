"""市场要闻采集（复盘闭环与赚钱效应四期优化方案 §4）：新浪 7x24 快讯 → news_telegraph。

源：``https://zhibo.sina.com.cn/api/zhibo/feed?zhibo_id=152&page=N&page_size=50``
（新浪财经 7x24 全球直播公开 JSON 接口，长期稳定、无签名；2026-10-07 curl 实测
200/0.2s，字段 id/create_time/rich_text/tag 齐全）。

选型记录（调研清单逐项实测结论）：
- 财联社电报（cls.cn）：全部数据端点要求签名参数（公开页为客户端渲染无内嵌数据）——
  按**项目合规红线"不破解任何技术措施"**不实现签名，不采；
- 同花顺热榜：hexin-v 反爬，同一红线不采；
- 东财人气榜/互动易：数据源红线（东方财富禁用）不采；
- Anspire/SerpAPI/Tavily 等：需 API key，无 key 不采。

口径：
- news_id 唯一（幂等 upsert，可中断重跑）；增量游标 = 已入库最大 id（sync_cursor）；
- title 取 rich_text 的【】前缀，无则截断前 60 字；content = rich_text 全文；
- importance = 焦点 tag 或 is_focus（1=重点）；trade_date 由 create_time（北京时间）取日；
- 报告消费口径：新闻归因是市场叙事，不是核验过的因果。
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..data_sources.http_collector_base import HttpCollectorBase
from ..database.connection import DatabaseConnection
from ..config.config_manager import ConfigManager
from .review_common import write_review_report

logger = logging.getLogger(__name__)

_FEED_URL = 'https://zhibo.sina.com.cn/api/zhibo/feed'
_FEED_PARAMS = {'zhibo_id': '152', 'tag_id': '0', 'dire': 'f', 'dpc': 1}
_SOURCE_NAME = 'sina_7x24'
_CURSOR_KEY = 'news_telegraph'

_TITLE_RE = re.compile(r'【([^】]{1,60})】')


def parse_feed(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """快讯接口响应 → news_telegraph 行（纯函数，单测覆盖）。

    无 rich_text / 无 id 的行丢弃；解析失败抛 ValueError（页面结构变更）。
    """
    try:
        items = payload['result']['data']['feed']['list']
    except (KeyError, TypeError) as e:
        raise ValueError(f'快讯响应结构变更: {e}') from e
    rows: List[Dict[str, Any]] = []
    for it in items or []:
        news_id = str(it.get('id') or '').strip()
        text = str(it.get('rich_text') or '').strip()
        if not news_id or not text:
            continue
        create_time = str(it.get('create_time') or '').strip()
        trade_date = create_time[:10].replace('-', '') if len(create_time) >= 10 else None
        m = _TITLE_RE.search(text)
        title = m.group(1) if m else text[:60]
        tags = '/'.join(str(t.get('name') or '') for t in (it.get('tag') or [])
                        if t.get('name')) or None
        importance = 1 if (str(it.get('is_focus') or '') in ('1', 'True', 'true')
                           or (tags and '焦点' in tags)) else 0
        rows.append({
            'news_id': news_id, 'publish_time': create_time or None,
            'trade_date': trade_date, 'title': title, 'content': text,
            'subject': tags, 'importance': importance,
            'source': _SOURCE_NAME,
        })
    return rows


class NewsTelegraphManager:
    """市场要闻采集器：新浪 7x24 快讯 → news_telegraph（增量游标 + 幂等 upsert）"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        review = self.config.get('review_sync') or {}
        news_cfg = review.get('news') or {}
        self.page_size = int(news_cfg.get('page_size', 50))
        self.default_pages = int(news_cfg.get('max_pages', 5))
        self.collector = HttpCollectorBase(
            self.config, source_name=_SOURCE_NAME,
            interval_seconds=float(news_cfg.get('interval',
                                                review.get('sina_interval', 1.0))),
            referer='https://finance.sina.com.cn/7x24/')

    def execute(self, pages: Optional[int] = None, dry_run: bool = False,
                write_report: bool = True) -> Dict[str, Any]:
        """增量采集（缺省抓最新页直到游标之下；`pages` 显式页数用于首采/回补；
        dry_run 只输出执行参数不请求不写库）"""
        started = datetime.now()
        pages = max(1, int(pages or self.default_pages))
        last_id = self.db.get_cursor(_SOURCE_NAME, _CURSOR_KEY)
        stats: Dict[str, Any] = {'pages': 0, 'items': 0, 'rows': 0,
                                 'cursor_before': last_id, 'cursor_after': None,
                                 'latest_time': None}
        result: Dict[str, Any] = {'success': True, 'stats': stats, 'errors': [],
                                  'dry_run': dry_run}
        newest_id: Optional[int] = None
        if dry_run:
            result['duration'] = (datetime.now() - started).total_seconds()
            return result
        try:
            for page in range(1, pages + 1):
                stats['pages'] = page
                payload = self.collector.get_json(
                    _FEED_URL, params={**_FEED_PARAMS, 'page': page,
                                       'page_size': self.page_size})
                rows = parse_feed(payload)
                if not rows:
                    break
                stats['items'] += len(rows)
                # 只入比游标新的行（id 数值递增；历史行幂等 upsert 亦无害）
                fresh = [r for r in rows
                         if last_id is None or int(r['news_id']) > int(last_id)]
                if fresh:
                    self.db.upsert_news_telegraph(fresh)
                    stats['rows'] += len(fresh)
                for r in rows:
                    try:
                        nid = int(r['news_id'])
                    except ValueError:
                        continue
                    newest_id = nid if newest_id is None else max(newest_id, nid)
                    stats['latest_time'] = max(stats['latest_time'] or '',
                                               r.get('publish_time') or '')
                # 翻页终止：本页最老一行已不新于游标（增量追平）
                if last_id is not None and all(
                        _safe_int(r['news_id']) <= int(last_id) for r in rows):
                    break
            if newest_id is not None:
                self.db.set_cursor(_SOURCE_NAME, _CURSOR_KEY, str(newest_id))
                stats['cursor_after'] = str(newest_id)
        except Exception as e:
            result['success'] = False
            result['errors'].append(repr(e))
        result['duration'] = (datetime.now() - started).total_seconds()
        if write_report:
            result['report_path'] = self._write_report(result)
        return result

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 页数 {stats['pages']}，条目 {stats['items']}，新入库 {stats['rows']}；"
            f"游标 {stats['cursor_before'] or '—'} → {stats['cursor_after'] or '—'}",
            f"- 最新条目时间：{stats['latest_time'] or '—'}（北京时间）",
            "- 源：新浪 7x24 快讯（公开接口无签名）；财联社签名接口按合规红线不采",
            "- 口径：新闻归因是市场叙事，不是核验过的因果",
        ]
        if result['errors']:
            lines.append(f"- 错误：{result['errors']}")
        status = '成功' if result['success'] else '失败'
        return write_review_report('市场要闻采集报告', 'news_telegraph_report',
                                   status, lines, result.get('duration'))


def _safe_int(v: Any) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return -1
