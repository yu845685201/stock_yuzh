"""
解禁日历同步（每日复盘缺失数据采集方案 V1.0 20260928 §5；改造自待补 stub）

路径变更（2026-09-28 探路实测）：
- 原同花顺解禁页 data.10jqka.com.cn/market/jjsj/ 404、替代路径 401（hexin-v）——弃用
- Tushare share_float 属方案红线"高积分项全部不用"——不采用
- 主路径：巨潮全市场窗口检索 searchkey（实测"限售股上市流通"近30日 180 条，方案 §2.1 P5）
  → 标题过滤（排除核查意见/法律意见/结果公告等非正文条目）
  → 下载 PDF（复用 cninfo_source.download_pdf：%PDF 头校验、.part 原子落盘）
  → pypdf 文本层提取解禁日/数量/占比 → share_unlock_calendar upsert

口径（重要，报告章节需如实标注）：
- 公告制——仅覆盖"已披露的近端解禁"（《限售股上市流通公告》一般提前 3-5 个交易日），
  "未来 30 天前瞻"是本口径固有弱项，不做无据前瞻（数据真实性原则）
- 解禁日/数量在 PDF 正文而非标题：多解禁日/多批次公告取首个匹配（启发式，报告注明）；
  扫描件无文本层 → 如实计 parse_failed，不猜测入库

增量：sync_cursor unlock.last_success_date（与 announcement 同语义；失败窗口重跑自动补）。
"""

import logging
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..config.config_manager import ConfigManager
from ..data_sources.cninfo_source import CninfoBlockedError, CninfoSource
from ..database.connection import DatabaseConnection
from .review_common import code6_to_ts_code, tmp_dir, write_review_report

logger = logging.getLogger(__name__)

# 标题过滤词表（config review_sync.unlock 可覆盖）
DEFAULT_INCLUDE_WORDS = ('上市流通', '解除限售')
DEFAULT_EXCLUDE_WORDS = ('核查意见', '法律意见', '见证意见', '结果公告', '更正', '摘要', '英文')
DEFAULT_SEARCHKEYS = ('限售股上市流通', '解除限售')

# 正文解析模式（顺序敏感：关键词窗内找日期，避免误取公告披露日）
_DATE_RE = re.compile(r'(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日')
_DATE_KEYWORDS = ('上市流通日', '上市流通日期', '解除限售日期', '流通日期', '上市流通时间')
_DATE_KEYWORD_WINDOW = 80
_SHARES_RE = re.compile(
    r'(?:解除限售股份数量|解除限售数量|上市流通数量|限售股份数量)'
    r'[^0-9]{0,16}([\d,，]+(?:\.\d+)?)\s*(万股|股)')
_RATIO_RE = re.compile(r'占总股本比例[^0-9%]{0,10}([\d.]+)\s*%')

# 解禁类型（标题词 -> unlock_type 归一化；顺序敏感先专后泛）
_TYPE_KEYWORDS: List[Tuple[str, str]] = [
    ('发行股份购买资产', '重组限售'),
    ('重大资产重组', '重组限售'),
    ('股权激励', '股权激励限售'),
    ('定向增发', '定向增发限售'),
    ('非公开发行', '定向增发限售'),
    ('首发', '首发限售'),
    ('首次公开发行', '首发限售'),
    ('配股', '配股限售'),
]
DEFAULT_UNLOCK_TYPE = '限售股上市流通'


class UnlockManager:
    """解禁日历：巨潮解禁公告 + PDF 正文解析 -> share_unlock_calendar"""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.db.ensure_review_tables()
        review = self.config.get('review_sync', {}) or {}
        section = review.get('unlock', {}) or {}
        self.enabled = bool(section.get('enabled', False))
        self.searchkeys = list(section.get('searchkeys') or DEFAULT_SEARCHKEYS)
        self.include_words = tuple(section.get('include_words') or DEFAULT_INCLUDE_WORDS)
        self.exclude_words = tuple(section.get('exclude_words') or DEFAULT_EXCLUDE_WORDS)
        self.max_pdf_pages = int(section.get('pdf_extract_max_pages', 3))
        self.default_days_back = int(section.get('days_back', 3))
        cninfo_cfg = dict(self.config.get('cninfo', {}) or {})
        cninfo_cfg.setdefault('query_interval', review.get('cninfo_interval', 0.5))
        self.source = CninfoSource(cninfo_cfg)
        self.pdf_dir = tmp_dir() / 'unlock_pdf'

    # ---------- 主流程 ----------

    def execute(self, as_of: Optional[str] = None, days_back: Optional[int] = None,
                dry_run: bool = False) -> Dict[str, Any]:
        """窗口 [as_of-days_back, as_of] 解禁公告检索 -> PDF 解析入库（as_of: yyyyMMdd）"""
        started = datetime.now()
        end = self._parse_date(as_of) or date.today()
        effective_days_back = int(days_back) if days_back else self.default_days_back
        last_cursor = self.db.get_cursor('unlock', 'last_success_date')
        if last_cursor:
            parsed_cursor = self._parse_date(last_cursor)
            start = max(parsed_cursor + timedelta(days=1), end - timedelta(days=effective_days_back)) \
                if parsed_cursor else end - timedelta(days=effective_days_back)
        else:
            start = end - timedelta(days=effective_days_back)
        if start > end:
            start = end

        stats: Dict[str, Any] = {
            'window': f'{start.isoformat()}~{end.isoformat()}',
            'searched': 0, 'deduped': 0, 'kept': 0, 'downloaded': 0,
            'parse_ok': 0, 'parse_failed': [], 'rows': 0, 'unlock_type_counts': {},
        }
        result: Dict[str, Any] = {'success': False, 'stats': stats, 'errors': [], 'blocked': False}
        if not self.enabled:
            result['errors'].append('配置停用（review_sync.unlock.enabled=false）')
            result['skipped_disabled'] = True
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
            return result

        try:
            raw: List[Dict[str, Any]] = []
            seen = set()
            for key in self.searchkeys:
                for a in self.source.query_market_window(start.isoformat(), end.isoformat(),
                                                         searchkey=key):
                    aid = str(a.get('announcementId') or '')
                    if aid and aid not in seen:
                        seen.add(aid)
                        raw.append(a)
            stats['searched'] = len(raw)
            stats['deduped'] = len(raw)

            kept: List[Dict[str, Any]] = []
            for a in raw:
                cleaned = self.clean_title(str(a.get('announcementTitle') or ''))
                a['announcementTitle'] = cleaned
                if self.keep_title(cleaned):
                    kept.append(a)
            stats['kept'] = len(kept)

            rows: List[Dict[str, Any]] = []
            for a in kept:
                ann_id = str(a.get('announcementId') or '')
                title = str(a.get('announcementTitle') or '').strip()
                sec_code = str(a.get('secCode') or '').strip()
                adjunct = str(a.get('adjunctUrl') or '')
                if not ann_id or not sec_code or not adjunct:
                    stats['parse_failed'].append(f'{title or ann_id}（缺关键字段）')
                    continue
                ts_ms = a.get('announcementTime')
                ann_date = None
                if ts_ms:
                    try:
                        ann_date = datetime.fromtimestamp(int(ts_ms) / 1000).date()
                    except (ValueError, OSError, OverflowError):
                        ann_date = None
                if dry_run:
                    continue
                pdf_path = self.pdf_dir / f'{ann_id}.pdf'
                dl = self.source.download_pdf(adjunct, pdf_path)
                if dl['status'] == 'failed':
                    stats['parse_failed'].append(f'{title}（下载失败: {dl["error"]}）')
                    continue
                stats['downloaded'] += 1
                text = self._extract_pdf_text(pdf_path, self.max_pdf_pages)
                parsed = self.parse_unlock_text(text, not_before=ann_date)
                if parsed.get('unlock_date') is None:
                    stats['parse_failed'].append(f'{title}（正文未解析到不早于发布日的上市流通日）')
                    continue
                unlock_date = parsed['unlock_date'].strftime('%Y%m%d')
                rows.append({
                    'ts_code': code6_to_ts_code(sec_code),
                    'stock_name': str(a.get('secName') or '') or None,
                    'unlock_date': unlock_date,
                    'unlock_shares': parsed.get('unlock_shares'),
                    'unlock_ratio': parsed.get('unlock_ratio'),
                    'unlock_type': self.classify_unlock_type(title),
                })
                type_count = rows[-1]['unlock_type']
                stats['unlock_type_counts'][type_count] = \
                    stats['unlock_type_counts'].get(type_count, 0) + 1
            stats['parse_ok'] = len(rows)
            stats['rows'] = len(rows)

            if not dry_run:
                if rows:
                    self.db.upsert_share_unlocks(rows)
                # 游标随窗口成功推进（窗口内确实无解禁公告也推进，与 announcement 同语义；
                # upsert 幂等，重跑/重叠窗口安全）
                self.db.set_cursor('unlock', 'last_success_date', end.isoformat())
            result['success'] = True
        except CninfoBlockedError as e:
            result['blocked'] = True
            result['errors'].append(f'巨潮风控急停（游标未推进，重跑自动补）: {e}')
        except Exception as e:
            result['errors'].append(str(e))
            logger.exception(f'解禁日历采集失败: {e}')
        finally:
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result)
        return result

    # ---------- 标题过滤与分类（staticmethod 便于单测） ----------

    @staticmethod
    def clean_title(title: str) -> str:
        """去除 isHLtitle 高亮标记（2026-09-28 实测：searchkey 检索返回
        '部分<em>限</em><em>售</em><em>股</em><em>上</em>市<em>流</em>通' 形态，
        标签不打掉会让子串词表匹配全部落空——首轮跑数 446→0 的根因）"""
        return re.sub(r'</?em>', '', str(title))

    def keep_title(self, title: str) -> bool:
        """正文条目判定：命中任一包含词且不含排除词"""
        if not title:
            return False
        if any(k in title for k in self.exclude_words):
            return False
        return any(k in title for k in self.include_words)

    @staticmethod
    def classify_unlock_type(title: str) -> str:
        for word, unlock_type in _TYPE_KEYWORDS:
            if word in title:
                return unlock_type
        return DEFAULT_UNLOCK_TYPE

    # ---------- 正文解析 ----------

    @staticmethod
    def parse_unlock_text(text: str, not_before: Optional[date] = None) -> Dict[str, Any]:
        """PDF 文本 -> {unlock_date: date|None, unlock_shares: float|None, unlock_ratio: float|None}

        解禁日：收集 '上市流通日' 等关键词后窗内的日期候选（找不到再放宽到 '上市流通'
        出现处），取首个不早于 not_before 的候选；not_before 为公告发布日——
        公告正文的背景段落常引用历史批次解禁日（2026-09-28 实跑发现 2018-2025 年脏日期），
        早于发布日的日期一律弃用。仍取不到返回 None，调用方计 parse_failed，
        绝不拿公告披露日凑数。
        """
        if not text:
            return {'unlock_date': None, 'unlock_shares': None, 'unlock_ratio': None}

        candidates: List[date] = []
        for keyword in _DATE_KEYWORDS:
            idx = 0
            while True:
                idx = text.find(keyword, idx)
                if idx < 0:
                    break
                m = _DATE_RE.search(text[idx:idx + len(keyword) + _DATE_KEYWORD_WINDOW])
                if m:
                    parsed = UnlockManager._safe_date(m)
                    if parsed:
                        candidates.append(parsed)
                idx += len(keyword)
        if not candidates:
            idx = 0
            while True:
                idx = text.find('上市流通', idx)
                if idx < 0:
                    break
                m = _DATE_RE.search(text[idx:idx + _DATE_KEYWORD_WINDOW])
                if m:
                    parsed = UnlockManager._safe_date(m)
                    if parsed:
                        candidates.append(parsed)
                idx += len('上市流通')

        unlock_date = None
        for candidate in candidates:
            if not candidate:
                continue
            if not_before is None or candidate >= not_before:
                unlock_date = candidate
                break

        unlock_shares = None
        m = _SHARES_RE.search(text)
        if m:
            try:
                value = float(m.group(1).replace(',', '').replace('，', ''))
                unlock_shares = value * 10000 if m.group(2) == '万股' else value
            except ValueError:
                unlock_shares = None

        unlock_ratio = None
        m = _RATIO_RE.search(text)
        if m:
            try:
                unlock_ratio = float(m.group(1))
            except ValueError:
                unlock_ratio = None

        return {'unlock_date': unlock_date, 'unlock_shares': unlock_shares,
                'unlock_ratio': unlock_ratio}

    @staticmethod
    def _safe_date(m: re.Match) -> Optional[date]:
        try:
            return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            return None

    @staticmethod
    def _extract_pdf_text(path: Path, max_pages: int) -> str:
        """pypdf 文本层提取前 N 页（扫描件返回空串，由调用方如实计失败）"""
        try:
            from pypdf import PdfReader
        except ImportError as e:
            raise RuntimeError('缺少 pypdf 依赖（venv: pip install pypdf）') from e
        reader = PdfReader(str(path))
        texts: List[str] = []
        for page in reader.pages[:max(1, int(max_pages))]:
            try:
                texts.append(page.extract_text() or '')
            except Exception as e:  # 单页解析失败不阻塞其余页
                logger.debug(f'PDF 单页解析失败 {path.name}: {e}')
        return '\n'.join(texts)

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
        if result.get('skipped_disabled'):
            lines = [
                '- 路径：巨潮解禁公告 + PDF 正文解析（方案 §5，2026-09-28 换源）',
                f"- enabled：False（config review_sync.unlock.enabled）",
            ]
            status = '待补（配置停用）'
        else:
            lines = [
                f"- 窗口：{stats['window']}（增量下界 sync_cursor.unlock.last_success_date）",
                f"- 口径：公告制近端解禁（公告提前 3-5 交易日披露），非 30 天前瞻；"
                f"多批次公告取首个上市流通日（启发式）",
                f"- 漏斗：检索去重 {stats['deduped']} → 标题过滤 {stats['kept']} → "
                f"下载 {stats['downloaded']} → 解析成功 {stats['parse_ok']}（入库 {stats['rows']}）",
                f"- 解禁类型分布：{stats['unlock_type_counts']}",
            ]
            if stats['parse_failed']:
                lines.append(f"- 解析失败 {len(stats['parse_failed'])} 条（如实不入库）："
                             f"{stats['parse_failed'][:20]}")
            if result['errors']:
                lines.append(f"- 错误：{result['errors']}")
            status = '风控急停' if result['blocked'] else ('成功' if result['success'] else '失败')
        return write_review_report('解禁日历同步报告', 'unlock_sync_report',
                                   status, lines, result.get('duration'))
