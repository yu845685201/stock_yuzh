"""
公告清单 + 业绩预告（方案 4.6/4.13，M2；巨潮资讯网，同一批请求双用途）

- 来源：巨潮 hisAnnouncement/query 全市场窗口检索（cninfo_source.query_market_window，
  与财报 PDF 共用请求/风控路径：CninfoBlockedError 急停、CninfoWindowTooLarge 对半拆窗）
- 增量：sync_cursor last_success_date（昨日~今日；失败窗口次日并抓，巨潮可回溯任意历史）
- ann_date 由 announcementTime（epoch 毫秒）转换——遵守项目"披露日以真实 pubDate 为准"约定
- ann_type 标题关键词正则分类：定期报告/业绩预告/业绩快报/分红送转/增减持/重大事项/其他
- 业绩预告按标题筛出 + 解析预计净利润区间（正文不抓，标题解析不出则区间留空）
"""

import logging
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from ..config.config_manager import ConfigManager
from ..data_sources.cninfo_source import CninfoBlockedError, CninfoSource
from ..database.connection import DatabaseConnection
from .review_common import code6_to_ts_code, write_review_report

logger = logging.getLogger(__name__)

# 默认分类词表（config review_sync.announcement_types 可覆盖；顺序敏感：先长后短/先专后泛）
DEFAULT_TYPE_KEYWORDS: List[Tuple[str, List[str]]] = [
    ('定期报告', ('年度报告', '半年度报告', '中期报告', '季度报告', '一季度报告', '三季度报告')),
    ('业绩快报', ('业绩快报',)),
    ('业绩预告', ('业绩预告', '预增', '预减', '扭亏', '首亏', '续盈', '续亏', '略增', '略减')),
    ('分红送转', ('权益分派', '利润分配', '分红', '派息', '送转')),
    ('增减持', ('增持计划', '减持计划', '增持', '减持')),
    ('重大事项', ('重大资产重组', '要约收购', '发行股份', '停牌', '复牌', '收购', '重大合同', '中标')),
]

# 业绩预告类型（标题词 -> forecast_type 归一化）
FORECAST_WORDS = ('预增', '预减', '扭亏', '首亏', '续盈', '续亏', '略增', '略减')

# 预计净利润区间（标题/摘要常见写法：净利润 1.5亿元~2.0亿元 / 8000万–12000万）
_RANGE_PATTERN = re.compile(
    r'([\d.,]+)\s*万?\s*亿?元?\s*[~～\-—–至到]\s*([\d.,]+)\s*万?\s*亿?元?')
_PERIOD_PATTERNS = [
    (re.compile(r'(\d{4})年第[一二三四]季度(业绩预告|报告)'), 'Q'),
    (re.compile(r'(\d{4})年(第[一二三四]季度)?(?:半年度|中期)?(?:年度)?业绩预告'), None),
    (re.compile(r'(\d{4})年年度(业绩预告|报告)'), None),
]

# 排除条目：摘要/英文版等不落库
_EXCLUDE_KEYWORDS = ('摘要', '英文版', '提示性公告仅供参考')


class AnnouncementManager:
    """巨潮公告清单 + 业绩预告（同请求双用途）"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        review = self.config.get('review_sync', {}) or {}
        cninfo_cfg = dict(self.config.get('cninfo', {}) or {})
        cninfo_cfg.setdefault('query_interval', review.get('cninfo_interval', 0.5))
        self.source = CninfoSource(cninfo_cfg)
        self.type_keywords: List[Tuple[str, List[str]]] = [
            (str(k), list(v)) for k, v in
            (review.get('announcement_types') or dict(DEFAULT_TYPE_KEYWORDS)).items()
        ]

    # ---------- 主流程 ----------

    def execute(self, as_of: Optional[str] = None, days_back: int = 1,
                dry_run: bool = False) -> Dict[str, Any]:
        """窗口 [as_of-days_back, as_of] 全市场公告检索入库（as_of: yyyyMMdd，缺省当日）"""
        started = datetime.now()
        end = self._parse_date(as_of) or date.today()
        last_cursor = self.db.get_cursor('announcement', 'last_success_date')
        if last_cursor:
            start = max(self._parse_date(last_cursor) + timedelta(days=1),
                        end - timedelta(days=days_back))
        else:
            start = end - timedelta(days=days_back)
        if start > end:
            start = end

        stats: Dict[str, Any] = {
            'window': f'{start.isoformat()}~{end.isoformat()}',
            'announcements': 0, 'announcements_new': 0,
            'forecast_rows': 0, 'type_counts': {},
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
        try:
            raw = self.source.query_market_window(start.isoformat(), end.isoformat())
            stats['announcements'] = len(raw)
            ann_rows: List[Dict[str, Any]] = []
            forecast_rows: List[Dict[str, Any]] = []
            for a in raw:
                row = self._to_announcement_row(a)
                if row is None:
                    continue
                ann_rows.append(row)
                if row['ann_type'] == '业绩预告':
                    f = self._to_forecast_row(a, row)
                    if f is not None:
                        forecast_rows.append(f)
            stats['announcements_new'] = len(ann_rows)
            stats['forecast_rows'] = len(forecast_rows)
            stats['rows'] = stats['announcements_new'] + stats['forecast_rows']  # 编排器摘要行数约定
            counts: Dict[str, int] = {}
            for r in ann_rows:
                counts[r['ann_type']] = counts.get(r['ann_type'], 0) + 1
            stats['type_counts'] = counts

            if not dry_run:
                self.db.upsert_announcements(ann_rows)
                self.db.upsert_performance_forecasts(forecast_rows)
                self.db.set_cursor('announcement', 'last_success_date', end.isoformat())
            result['success'] = True
        except CninfoBlockedError as e:
            result['blocked'] = True
            result['errors'].append(f'巨潮风控急停（游标未推进，重跑自动补）: {e}')
        except Exception as e:
            result['errors'].append(str(e))
            logger.exception(f'公告采集失败: {e}')
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
        return result

    # ---------- 行构造 ----------

    def _to_announcement_row(self, a: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        title = str(a.get('announcementTitle') or '').strip()
        if not title or any(k in title for k in _EXCLUDE_KEYWORDS):
            return None
        ann_id = str(a.get('announcementId') or '')
        if not ann_id:
            return None
        ts_ms = a.get('announcementTime')
        ann_date = None
        if ts_ms:
            try:
                ann_date = datetime.fromtimestamp(int(ts_ms) / 1000).strftime('%Y%m%d')
            except (ValueError, OSError, OverflowError):
                ann_date = None
        sec_code = str(a.get('secCode') or '').strip()
        adjunct = str(a.get('adjunctUrl') or '')
        return {
            'announcement_id': ann_id,
            'ann_date': ann_date,
            'ts_code': code6_to_ts_code(sec_code) if sec_code else None,
            'stock_name': str(a.get('secName') or '') or None,
            'title': title,
            'ann_type': self.classify_title(title),
            'ann_url': f'http://static.cninfo.com.cn/{adjunct.lstrip("/")}' if adjunct else None,
        }

    def _to_forecast_row(self, a: Dict[str, Any], ann_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        title = ann_row['title']
        forecast_type = self._match_forecast_type(title)
        if forecast_type is None:
            return None
        lo, hi = self._parse_profit_range(title)
        period = self._parse_report_period(title)
        return {
            'announcement_id': ann_row['announcement_id'],
            'ts_code': ann_row['ts_code'],
            'stock_name': ann_row['stock_name'],
            'ann_date': ann_row['ann_date'],
            'report_period': period,
            'forecast_type': forecast_type,
            'net_profit_min': lo,
            'net_profit_max': hi,
            'summary': title[:500],
            'ann_url': ann_row['ann_url'],
        }

    # ---------- 分类与解析（staticmethod 便于单测） ----------

    def classify_title(self, title: str) -> str:
        """标题关键词分类（词表顺序敏感，先配置先匹配）"""
        for ann_type, keywords in self.type_keywords:
            if any(k in title for k in keywords):
                return ann_type
        return '其他'

    @staticmethod
    def _match_forecast_type(title: str) -> Optional[str]:
        for word in FORECAST_WORDS:
            if word in title:
                return word
        return None

    @staticmethod
    def _parse_profit_range(title: str) -> Tuple[Optional[float], Optional[float]]:
        """标题解析预计净利润区间 -> (min, max) 元；解析不出返回 (None, None)"""
        m = _RANGE_PATTERN.search(title)
        if not m:
            return None, None
        lo = AnnouncementManager._amount_to_yuan(m.group(1), m.group(0))
        hi = AnnouncementManager._amount_to_yuan(m.group(2), m.group(0))
        if lo is None or hi is None:
            return None, None
        return (min(lo, hi), max(lo, hi))

    @staticmethod
    def _amount_to_yuan(number_str: str, context: str) -> Optional[float]:
        try:
            v = float(number_str.replace(',', ''))
        except ValueError:
            return None
        if '万亿' in context:
            return v * 1e12
        if '亿' in context:
            return v * 1e8
        if '万' in context:
            return v * 1e4
        return v  # 裸数字按元

    @staticmethod
    def _parse_report_period(title: str) -> Optional[str]:
        """标题解析报告期 -> yyyyMMdd（0331/0630/0930/1231）"""
        m = re.search(r'(\d{4})年第一季度', title)
        if m:
            return f'{m.group(1)}0331'
        m = re.search(r'(\d{4})年(?:第二季度|半年度|中期)', title)
        if m:
            return f'{m.group(1)}0630'
        m = re.search(r'(\d{4})年第三季度', title)
        if m:
            return f'{m.group(1)}0930'
        m = re.search(r'(\d{4})年(?:第四季度|年度)', title)
        if m:
            return f'{m.group(1)}1231'
        return None

    # ---------- 工具 ----------

    @staticmethod
    def _parse_date(value: Optional[str]) -> Optional[date]:
        if not value:
            return None
        s = str(value).replace('-', '')[:8]
        try:
            return datetime.strptime(s, '%Y%m%d').date()
        except ValueError:
            return None

    def _write_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 窗口：{stats['window']}（增量下界 sync_cursor.announcement.last_success_date）",
            f"- 公告：检索 {stats['announcements']} 条，入库 {stats['announcements_new']} 条",
            f"- 业绩预告：{stats['forecast_rows']} 条",
            f"- 分类分布：{stats['type_counts']}",
        ]
        if result['errors']:
            lines.append(f"- 错误：{result['errors']}")
        status = '风控急停' if result['blocked'] else ('成功' if result['success'] else '失败')
        return write_review_report('公告清单与业绩预告同步报告', 'announcement_sync_report',
                                   status, lines, result.get('duration'))
