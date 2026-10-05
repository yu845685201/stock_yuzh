"""
公告清单 + 业绩预告（方案 4.6/4.13，M2；巨潮资讯网，同一批请求双用途）

- 来源：巨潮 hisAnnouncement/query 全市场窗口检索（cninfo_source.query_market_window，
  与财报 PDF 共用请求/风控路径：CninfoBlockedError 急停、CninfoWindowTooLarge 对半拆窗）
- 增量：sync_cursor last_success_date（昨日~今日；失败窗口次日并抓，巨潮可回溯任意历史）
- 历史回补：backfill_range 显式区间模式（开发项 K 披露日回补；不推游标、抑制逐日报告、
  category 过滤、结束显式触发披露日 COALESCE 回填）
- ann_date 由 announcementTime（epoch 毫秒）转换——遵守项目"披露日以真实 pubDate 为准"约定
- ann_type 标题关键词正则分类：定期报告/业绩预告/业绩快报/分红送转/增减持/重大事项/其他
- 业绩预告按标题筛出 + 解析预计净利润区间（正文不抓，标题解析不出则区间留空）
"""

import logging
import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from ..config.config_manager import ConfigManager
from ..data_sources.cninfo_source import (CninfoBlockedError, CninfoSource,
                                          PERIODIC_CATEGORIES)
from ..database.connection import DatabaseConnection
from .review_common import clean_stock_name, code6_to_ts_code, write_review_report

logger = logging.getLogger(__name__)

# 回补 category 别名（backfill_range；periodic = 四类定期报告，与 COALESCE
# 回填的 ann_type IN ('定期报告','业绩快报') 匹配）
CATEGORY_ALIASES: Dict[str, str] = {
    'periodic': ';'.join(PERIODIC_CATEGORIES),
}

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
            ann_rows, forecast_rows = self._rows_from_raw(raw)
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

    # ---------- 历史回补（开发项 K：显式区间模式，与增量 execute 语义分离） ----------

    def backfill_range(self, start_date: str, end_date: str, category: str = '',
                       dry_run: bool = False) -> Dict[str, Any]:
        """显式区间历史回补：按周（7 天）切窗遍历 [start_date, end_date] 全市场公告检索。

        回补语义（与增量 execute 的区别）：
        - 不推进 sync_cursor（回补日期早于游标时推进会造成增量漏采）；
        - 不逐窗落盘报告（X-19：300 窗 × 每窗 1 份将膨胀数千文件），结束写一份汇总；
        - 逐窗 upsert（幂等，中断/急停后重跑自动续，已完成窗口按 announcement_id 去重）；
        - 全部窗口完成后显式触发披露日 COALESCE 回填（开发项 K 验收前提，确保即时生效）。

        category：''=全量（现行为）；'periodic'=四类定期报告（CATEGORY_ALIASES）；
        亦可传分号分隔的原始巨潮分类码。seDate 红线（探路 T18）：窗口日期一律
        isoformat（yyyy-MM-dd）——传 yyyyMMdd 巨潮不报错、静默返回 0 条。
        风控：CninfoBlockedError 急停整个回补（记录续跑点，重跑自动续）；单窗其他异常
        记入错误清单继续下一窗（失败窗口清单可重跑）。
        """
        started = datetime.now()
        start = self._parse_date(start_date)
        end = self._parse_date(end_date)
        if start is None or end is None or start > end:
            raise ValueError(f'回补区间非法: {start_date!r} ~ {end_date!r}')
        resolved_category = CATEGORY_ALIASES.get(category, category)

        stats: Dict[str, Any] = {
            'range': f'{start.isoformat()}~{end.isoformat()}',
            'category': resolved_category or '(全量)',
            'windows_total': 0, 'windows_done': 0,
            'announcements': 0, 'announcements_new': 0, 'forecast_rows': 0,
            'type_counts': {}, 'last_completed_window': None,
            'disclosure_backfill_rows': None,
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [],
                                  'blocked': False, 'dry_run': dry_run}
        windows: List[Tuple[date, date]] = []
        cur = start
        while cur <= end:
            win_end = min(cur + timedelta(days=6), end)
            windows.append((cur, win_end))
            cur = win_end + timedelta(days=1)
        stats['windows_total'] = len(windows)

        blocked_at: Optional[Tuple[date, date]] = None
        for win_start, win_end in windows:
            try:
                raw = self.source.query_market_window(win_start.isoformat(), win_end.isoformat(),
                                                      category=resolved_category)
                ann_rows, forecast_rows = self._rows_from_raw(raw)
                if not dry_run:
                    self.db.upsert_announcements(ann_rows)
                    self.db.upsert_performance_forecasts(forecast_rows)
                stats['announcements'] += len(raw)
                stats['announcements_new'] += len(ann_rows)
                stats['forecast_rows'] += len(forecast_rows)
                for r in ann_rows:
                    stats['type_counts'][r['ann_type']] = \
                        stats['type_counts'].get(r['ann_type'], 0) + 1
                stats['last_completed_window'] = win_end.isoformat()
                stats['windows_done'] += 1
                logger.info(f'回补窗口 {win_start}~{win_end} 完成：检索 {len(raw)} 入库 {len(ann_rows)}')
            except CninfoBlockedError as e:
                blocked_at = (win_start, win_end)
                result['blocked'] = True
                result['errors'].append(f'巨潮风控急停于窗口 {win_start}~{win_end}: {e}')
                logger.warning(f'回补急停于窗口 {win_start}~{win_end}，已完成的 {stats["windows_done"]} '
                               f'个窗口已入库，续跑从 {win_start.isoformat()} 起')
                break
            except Exception as e:
                result['errors'].append(f'窗口 {win_start}~{win_end} 失败（可重跑）: {e}')
                logger.exception(f'回补窗口 {win_start}~{win_end} 失败: {e}')

        if not result['blocked'] and not dry_run:
            # COALESCE 披露日回填（公告 ann_date 优先 / 官网 actual 兜底）——K 项验收即时生效
            stats['disclosure_backfill_rows'] = self.db.backfill_financial_disclosure_dates()
        result['success'] = stats['windows_done'] == stats['windows_total']
        result['duration'] = (datetime.now() - started).total_seconds()
        result['report_path'] = self._write_backfill_report(result, blocked_at)
        return result

    def _rows_from_raw(self, raw: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """原始公告 -> (公告行, 业绩预告行)；增量与回补共用同一映射，保证口径一致"""
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
        return ann_rows, forecast_rows

    # ---------- 历史重分类（开发项 H） ----------

    def reclassify(self, days_back: int = 7, dry_run: bool = False) -> Dict[str, Any]:
        """近 N 日已入库公告按**当前词表**重跑分类，仅 update ann_type（纯本地，不重采）。

        词表在采集时写死、历史行不随词表更新（H 项差距）；本方法幂等可重跑——
        重跑时分类结果与库内一致则不产生更新。统计"其他"占比供验收（≤60%）。
        """
        started = datetime.now()
        end = date.today()
        start = end - timedelta(days=days_back)
        stats: Dict[str, Any] = {'window': f'{start.isoformat()}~{end.isoformat()}',
                                 'rows': 0, 'changed': 0, 'transitions': {},
                                 'other_ratio': None, 'type_counts': {}}
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [],
                                  'blocked': False, 'dry_run': dry_run}
        try:
            rows = self.db.execute_query(
                "SELECT announcement_id, ann_type, title FROM announcement_daily "
                "WHERE ann_date >= %s AND ann_date <= %s",
                (start.strftime('%Y%m%d'), end.strftime('%Y%m%d')))
            stats['rows'] = len(rows)
            new_counts: Dict[str, int] = {}
            updates: List[Dict[str, str]] = []
            for r in rows:
                new_type = self.classify_title(str(r['title'] or ''))
                new_counts[new_type] = new_counts.get(new_type, 0) + 1
                old_type = str(r['ann_type'] or '')
                if new_type != old_type:
                    updates.append({'announcement_id': str(r['announcement_id']),
                                    'ann_type': new_type})
                    key = f'{old_type}→{new_type}'
                    stats['transitions'][key] = stats['transitions'].get(key, 0) + 1
            stats['type_counts'] = new_counts
            total = len(rows)
            stats['other_ratio'] = round(new_counts.get('其他', 0) / total, 4) if total else None
            if not dry_run:
                for u in updates:
                    self.db.execute_update(
                        "UPDATE announcement_daily SET ann_type = %s, update_time = NOW() "
                        "WHERE announcement_id = %s",
                        (u['ann_type'], u['announcement_id']))
            stats['changed'] = len(updates)
            result['success'] = True
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_reclassify_report(result)
        return result

    def _write_reclassify_report(self, result: Dict[str, Any]) -> Optional[str]:
        stats = result['stats']
        lines = [
            f"- 窗口：近 {stats['window']}（days_back 口径，含当日）",
            f"- 行数：{stats['rows']}，重分类变更 {stats['changed']} 行"
            + ('（dry-run 未写库）' if result.get('dry_run') else ''),
            f"- \"其他\"占比："
            + ('—（无行）' if stats['other_ratio'] is None
               else f"{stats['other_ratio']:.1%}（验收线 ≤60%；公告标题天然大量日常事务，不应追求全分类）"),
            f"- 分类分布：{stats['type_counts']}",
        ]
        transitions = stats.get('transitions') or {}
        if transitions:
            top = sorted(transitions.items(), key=lambda kv: -kv[1])[:15]
            lines.append(f"- 主要迁移（至多 15）: {top}")
        if result['errors']:
            lines.append(f"- 错误：{result['errors']}")
        status = '成功' if result['success'] else '失败'
        return write_review_report('公告历史重分类报告', 'announcement_reclassify_report',
                                   status, lines, result.get('duration'))

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
            'stock_name': clean_stock_name(a.get('secName')),
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

    def _write_backfill_report(self, result: Dict[str, Any],
                               blocked_at: Optional[Tuple[date, date]]) -> Optional[str]:
        """回补汇总报告（X-19：整个区间只写一份，抑制逐窗文件膨胀）"""
        stats = result['stats']
        lines = [
            f"- 区间：{stats['range']}，category={stats['category']}",
            f"- 窗口：{stats['windows_done']}/{stats['windows_total']} 完成"
            f"（末窗口 {stats['last_completed_window']}）",
            f"- 公告：检索 {stats['announcements']} 条，入库 {stats['announcements_new']} 条",
            f"- 业绩预告：{stats['forecast_rows']} 条",
            f"- 分类分布：{stats['type_counts']}",
        ]
        if stats.get('disclosure_backfill_rows') is not None:
            lines.append(f"- 披露日 COALESCE 回填：更新 {stats['disclosure_backfill_rows']} 行")
        if result.get('dry_run'):
            lines.append('- dry-run：未写库、未触发 COALESCE')
        if blocked_at is not None:
            lines.append(f"- 续跑点：--start-date {blocked_at[0].isoformat()} "
                         f"（急停窗口，已完成窗口已入库、重跑幂等）")
        errors = result['errors']
        if errors:
            shown = errors[:20]
            lines.append(f"- 错误（{len(errors)} 条，至多展示 20）：{shown}")
        status = '风控急停（部分完成，可续跑）' if result['blocked'] else \
            ('成功' if result['success'] else '失败')
        return write_review_report('公告历史回补报告（披露日回补路径②）', 'announcement_backfill_report',
                                   status, lines, result.get('duration'))
