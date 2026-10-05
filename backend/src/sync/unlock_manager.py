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
from .review_common import clean_stock_name, code6_to_ts_code, tmp_dir, write_review_report

logger = logging.getLogger(__name__)

# 标题过滤词表（config review_sync.unlock 可覆盖；'暂不'排除"归属结果但股票暂不
# 上市流通"类非解禁事件——2026-10-04 实测该类 include 词"上市流通"子串误命中）
DEFAULT_INCLUDE_WORDS = ('上市流通', '解除限售')
DEFAULT_EXCLUDE_WORDS = ('核查意见', '法律意见', '见证意见', '结果公告', '更正', '摘要', '英文',
                         '暂不')
DEFAULT_SEARCHKEYS = ('限售股上市流通', '解除限售')

# 正文解析模式 V4.1（2026-10-04；18 份存量+45 份样本外实测）：
# - 日期：关键词正则化（词内容忍换行）+ 新增 可交易时间/可交易日期/上市日期
#   （北交所"股票解除限售公告"版式为"可交易时间为 X 年 X 月 X 日"，旧词表整行丢失）
_DATE_RE = re.compile(r'(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日')
_KW_DATE = re.compile(
    r'(?:上\s*市\s*流\s*通\s*日\s*期?|解\s*除\s*限\s*售\s*日\s*期|流\s*通\s*日\s*期|'
    r'上\s*市\s*流\s*通\s*时\s*间|可\s*交\s*易\s*时\s*间|可\s*交\s*易\s*日\s*期|上\s*市\s*日\s*期)')
# - 股数：关键词正则化（词内容忍换行）+ 变体族 + 44 字符间隔（排除数字/百分号/句读防跨句）；
#   V4.1 较 V4 扩（样本外 45 份实测 56%→98%）：尾部 数量|总数|股数|合计（科创板首发模板
#   "本次股票上市流通总数为 X 股"），词族补 解锁/上市股数/限售股份，名词组容忍换行并加 限售股
_NOUN = r'(?:限\s*制\s*性\s*股\s*票|限\s*售\s*股\s*份?|股\s*票|股\s*份)?'
_KW = (r'(?:解\s*除\s*限\s*售|解\s*锁|上\s*市\s*流\s*通|可\s*解\s*除\s*限\s*售|归\s*属|'
       r'限\s*售\s*股\s*份|上\s*市\s*股\s*数)'
       r'\s*(?:的)?\s*' + _NOUN + r'\s*(?:的)?\s*(?:数\s*量|总\s*数|股\s*数|合\s*计)?')
_SHARES_RE = re.compile(_KW + r'[^0-9%。；]{0,44}([\d,，]+(?:\.\d+)?)\s*(万股|股)')
# - 占比：占…总股本/股本总额…%（间隔容忍换行与"股本数字 股的"夹层）
_RATIO_RE = re.compile(r'占[^。；]{0,32}(?:总股本|股本总额)[^。；]{0,44}?([\d.]+)\s*%')

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
                    # 6 页重试：解禁日偶见排在中后部（多批次公告表格），多翻 3 页再解一次
                    parsed = self.parse_unlock_text(
                        self._extract_pdf_text(pdf_path, self.max_pdf_pages + 3),
                        not_before=ann_date)
                if parsed.get('unlock_date') is None:
                    stats['parse_failed'].append(f'{title}（正文未解析到不早于发布日的上市流通日）')
                    continue
                unlock_date = parsed['unlock_date'].strftime('%Y%m%d')
                rows.append({
                    'ts_code': code6_to_ts_code(sec_code),
                    'stock_name': clean_stock_name(a.get('secName')),
                    'unlock_date': unlock_date,
                    'unlock_shares': parsed.get('unlock_shares'),
                    # ratio 口径统一：股数解析成功时不写 PDF 占比（批次基数差异，实测地铁设计
                    # PDF 1.9887% vs 推导 2.2352%），由 backfill_unlock_ratios 按
                    # shares÷最新总股本统一回填；PDF 占比仅在股数失败时兜底
                    'unlock_ratio': parsed.get('unlock_ratio')
                                    if parsed.get('unlock_shares') is None else None,
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
                # ratio 推导回填（开发项 L，X-11）：仅补空行、幂等——PDF 正文只解析出
                # 解禁股数的行，按最新可得总股本推导占比
                stats['ratio_backfilled'] = self.db.backfill_unlock_ratios()
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
        （V4.1，2026-10-04 实测：存量 18 份 18/18，样本外 45 份股数 44/45）

        - 日期：关键词正则后向 80 字符窗取首个 >= not_before 的候选
          （not_before 为公告发布日——正文背景段落常引用历史批次解禁日，早于发布日一律弃用；
          2026-10-04 实测 *ST发展 正文 2015/2018 年日期被正确滤除）。仍取不到返回 None，
          调用方计 parse_failed，绝不拿公告披露日凑数；
        - 股数：全部命中中优先前置 24 字符含 本次/合计/共计 的（防误取背景段的拟授予数量）；
        - 占比：取与"股数数字字符串"出现位置最近的命中（防误取"限售股占总股本"——
          实测瑞立科密 77.4538% 为限售股占比，解禁占比 23.2526%）；股数未解析时退化为全文首命中。
        """
        if not text:
            return {'unlock_date': None, 'unlock_shares': None, 'unlock_ratio': None}

        unlock_date = None
        for m_kw in _KW_DATE.finditer(text):
            m = _DATE_RE.search(text[m_kw.end():m_kw.end() + 80])
            if not m:
                continue
            parsed = UnlockManager._safe_date(m)
            if not parsed:
                continue
            if not_before is None or parsed >= not_before:
                unlock_date = parsed
                break
        if unlock_date is None:
            # 兜底（历史行为兼容）：无标准关键词时从"上市流通"出现处向后取首个合格日期
            idx = 0
            while True:
                idx = text.find('上市流通', idx)
                if idx < 0:
                    break
                m = _DATE_RE.search(text[idx + 4:idx + 4 + 80])
                parsed = UnlockManager._safe_date(m) if m else None
                if parsed and (not_before is None or parsed >= not_before):
                    unlock_date = parsed
                    break
                idx += 4

        unlock_shares = None
        shares_m = None
        all_m = list(_SHARES_RE.finditer(text))
        if all_m:
            shares_m = next(
                (c for c in all_m
                 if re.search(r'本次|合计|共计', text[max(0, c.start() - 24):c.start()])),
                all_m[0])
            try:
                value = float(shares_m.group(1).replace(',', '').replace('，', ''))
                unlock_shares = value * 10000 if shares_m.group(2) == '万股' else value
            except ValueError:
                unlock_shares = None

        unlock_ratio = None
        ratio_m = None
        if shares_m is not None:
            best_d = None
            for r in _RATIO_RE.finditer(text):
                for occ in re.finditer(re.escape(shares_m.group(1)), text):
                    d = min(abs(r.start() - occ.start()), abs(r.end() - occ.end()))
                    if best_d is None or d < best_d:
                        best_d, ratio_m = d, r
        else:
            ratio_m = _RATIO_RE.search(text)
        if ratio_m:
            try:
                unlock_ratio = float(ratio_m.group(1))
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
