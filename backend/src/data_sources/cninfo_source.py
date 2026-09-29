"""
巨潮资讯网数据源适配器 —— 定期报告（财报）PDF 检索与下载

不继承 DataSourceBase 行情抽象：公告检索/文件下载与 K 线行情接口无共通语义，
独立成类，接口变动的影响面收敛在本文件。

接口事实（2026-09-26 经 backend/test/manual_check_cninfo.py 实测锁定）：
- 映射：GET /new/data/szse_stock.json -> stockList[{code,orgId,zwjc,pinyin,category}]，
  共约 6.3k 条，category 仅 A股/B股/CDR；退市股与北交所股票均在「A股」内
- 检索：POST /new/hisAnnouncement/query（form 表单），column 对按股查询不敏感
  （北交所用 szse 即可）；单页时 totalpages=0，页数需以 hasMore/totalAnnouncement 推算
- 公告字段：announcementId（唯一）、announcementTitle、announcementTime（epoch 毫秒）、
  adjunctUrl（相对路径）、adjunctSize（KB）、secCode、secName
- 下载：GET {static}/adjunctUrl，校验 Content-Length 一致 + %PDF 文件头
- 北交所标题无公司名前缀且用「一季度报告」写法；早年深市用「中期报告」指半年报

安全红线（内置，不可绕过）：串行限速、正常 UA/Referer、不破解任何技术措施、
封禁识别即退避冷却、仅自用不分发。
"""

import logging
import re
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from ..utils.retry import run_with_retry

logger = logging.getLogger(__name__)

QUERY_PATH = '/new/hisAnnouncement/query'
STOCK_MAP_PATH = '/new/data/szse_stock.json'

# 四类定期报告 category（年报/半年报/一季报/三季报，_szsh 后缀覆盖沪深北）
PERIODIC_CATEGORIES = ('category_ndbg_szsh', 'category_bndbg_szsh',
                       'category_yjdbg_szsh', 'category_sjdbg_szsh')

DEFAULT_UA = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
              'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36')

# 非正文条目关键词：摘要/英文版/取消/更正公告等不落盘（试点实测标题中确实混入；
# 注意「更正公告」排除的是更正公告本身，「（更正后）」完整报告由 _REVISION_MARKS 保留）
EXCLUDE_KEYWORDS = ('摘要', '英文', '取消', '更正公告', '更正说明')

# 报告期结束日（NN -> 月-日），进度表 last_stat_date 与报告期比较均用该口径
NN_PERIOD_END = {'01': '-03-31', '02': '-06-30', '03': '-09-30', '04': '-12-31'}

# 标题解析正则（顺序敏感：先长后短；兼容半角/全角括号、无公司名前缀、旧称「中期报告」）
_TITLE_PATTERNS: List[Tuple[re.Pattern, str]] = [
    (re.compile(r'(\d{4})年年度报告'), '04'),
    (re.compile(r'(\d{4})年半年度报告'), '02'),
    (re.compile(r'(\d{4})年中期报告'), '02'),
    (re.compile(r'(\d{4})年第[一1]季度报告'), '01'),
    (re.compile(r'(\d{4})年一季度报告'), '01'),
    (re.compile(r'(\d{4})年第[三3]季度报告'), '03'),
    (re.compile(r'(\d{4})年三季度报告'), '03'),
]
_REVISION_MARKS = ('更新版', '修订版', '更正后', '重述')
_FS_FORBIDDEN = re.compile(r'[\\/:*?"<>|\r\n]')


class CninfoBlockedError(RuntimeError):
    """巨潮风控触发（返回非 JSON/403/429/限流提示）：退避冷却后可重试"""


class CninfoWindowTooLarge(RuntimeError):
    """单检索窗口分页数超上限：调用方应对半拆分窗口后重查"""


class CninfoSource:
    """巨潮资讯网：orgId 映射、定期报告检索、标题解析、PDF 下载（串行限速）"""

    def __init__(self, config: Dict[str, Any]):
        self.query_base_url = (config.get('query_base_url') or 'http://www.cninfo.com.cn').rstrip('/')
        self.static_base_url = (config.get('static_base_url') or 'http://static.cninfo.com.cn').rstrip('/')
        self.timeout = int(config.get('timeout', 30))
        self.query_interval = float(config.get('query_interval', 0.5))
        self.download_interval = float(config.get('download_interval', 0.5))
        self.page_size = int(config.get('page_size', 30))
        self.max_pages_per_window = int(config.get('max_pages_per_window', 40))
        self.user_agent = config.get('user_agent') or DEFAULT_UA
        self._session = requests.Session()
        self._stock_map: Optional[Dict[str, Dict[str, str]]] = None

    # ---------- 基础请求 ----------

    def _headers(self) -> Dict[str, str]:
        return {
            'User-Agent': self.user_agent,
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': f'{self.query_base_url}/new/commonUrl/pageOfSearch?url=disclosure/list/search',
            'Origin': self.query_base_url,
        }

    def _attempt_query(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        if self.query_interval > 0:
            time.sleep(self.query_interval)
        resp = self._session.post(f'{self.query_base_url}{QUERY_PATH}',
                                  data=payload, headers=self._headers(), timeout=self.timeout)
        if resp.status_code in (403, 429):
            raise CninfoBlockedError(f'HTTP {resp.status_code}，疑似触发风控')
        resp.raise_for_status()
        try:
            return resp.json()
        except ValueError as e:
            raise CninfoBlockedError('检索返回非 JSON，疑似触发风控/验证码') from e

    def _post_query(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """检索请求：3 次重试，指数退避；封禁异常不吞，交给上层决策"""
        def _on_error(attempt: int, e: BaseException) -> None:
            logger.warning(f'巨潮检索第 {attempt} 次失败: {e}')
            time.sleep(2 ** attempt)

        return run_with_retry(
            lambda: self._attempt_query(payload),
            attempts=3, backoff=lambda a: 0,  # 退避在 on_error 中做，便于记录日志
            retry_on=(requests.RequestException, CninfoBlockedError),
            on_error=_on_error,
        )

    # ---------- 股票映射 ----------

    def get_stock_map(self) -> Dict[str, Dict[str, str]]:
        """code -> {org_id, name, category, code}；仅保留 category=A股（含退市股/北交所）"""
        if self._stock_map is not None:
            return self._stock_map
        if self.query_interval > 0:
            time.sleep(self.query_interval)
        resp = self._session.get(f'{self.query_base_url}{STOCK_MAP_PATH}',
                                 headers={'User-Agent': self.user_agent}, timeout=60)
        resp.raise_for_status()
        stock_list = resp.json().get('stockList', [])
        stock_map: Dict[str, Dict[str, str]] = {}
        for s in stock_list:
            code = str(s.get('code') or '')
            if not code or s.get('category') != 'A股':
                continue
            stock_map[code] = {
                'code': code,
                'org_id': s.get('orgId') or '',
                'name': s.get('zwjc') or code,
                'category': s.get('category'),
            }
        logger.info(f'巨潮映射加载完成：全量 {len(stock_list)} 条，过滤后 A股 {len(stock_map)} 只')
        self._stock_map = stock_map
        return stock_map

    # ---------- 定期报告检索 ----------

    def query_periodic_reports(self, code: str, org_id: str,
                               start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """检索单只股票披露窗口 [start_date, end_date] 内的四类定期报告公告。

        窗口分页数超上限时自动对半拆分递归（防深分页截断）。
        返回原始公告字典列表（按 announcementId 去重）。
        """
        try:
            results = self._query_window(code, org_id, start_date, end_date)
        except CninfoWindowTooLarge:
            s = date.fromisoformat(start_date)
            e = date.fromisoformat(end_date)
            if s >= e:
                raise
            mid = s + (e - s) / 2
            logger.info(f'{code} 窗口 {start_date}~{end_date} 超分页上限，对半拆分为 '
                        f'{start_date}~{mid} / {mid + timedelta(days=1)}~{end_date}')
            results = (self.query_periodic_reports(code, org_id, start_date, mid.isoformat())
                       + self.query_periodic_reports(code, org_id,
                                                     (mid + timedelta(days=1)).isoformat(), end_date))

        def _key(a: Dict[str, Any]) -> str:
            return str(a.get('announcementId') or a.get('adjunctUrl') or '')

        merged: Dict[str, Dict[str, Any]] = {}
        for a in results:
            merged[_key(a)] = a
        return list(merged.values())

    def _query_window(self, code: str, org_id: str,
                      start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """单窗口分页检索。

        分页字段实测口径（2026-09-26）：totalpages 在多页场景下不可靠
        （36 条/page_size=30 返回 totalpages=1），hasMore 与 totalAnnouncement 可靠，
        故以 hasMore 驱动翻页、totalAnnouncement 推算期望页数做上限保护与安全阀。
        """
        announcements: List[Dict[str, Any]] = []
        page_num = 1
        while True:
            payload = {
                'pageNum': page_num,
                'pageSize': self.page_size,
                'column': 'sse' if code.startswith('6') else 'szse',
                'tabName': 'fulltext',
                'plate': '',
                'stock': f'{code},{org_id}',
                'searchkey': '',
                'secid': '',
                'category': ';'.join(PERIODIC_CATEGORIES),
                'trade': '',
                'seDate': f'{start_date}~{end_date}',
                'sortName': '',
                'sortType': '',
                'isHLtitle': 'true',
            }
            data = self._post_query(payload)
            batch = data.get('announcements') or []
            if not batch:
                break
            announcements.extend(batch)

            has_more = str(data.get('hasMore') or '').lower() == 'true'
            total = int(data.get('totalAnnouncement') or 0)
            expected_pages = -(-total // self.page_size) if total > 0 else None
            if expected_pages is not None and expected_pages > self.max_pages_per_window:
                raise CninfoWindowTooLarge(
                    f'{code} 窗口 {start_date}~{end_date} 共 {total} 条/{expected_pages} 页，超上限')
            if not has_more:
                break
            if expected_pages is not None and page_num >= expected_pages:
                break
            if page_num >= self.max_pages_per_window * 2:
                # 安全阀：分页字段全不可靠时防死循环（去重后不影响正确性，仅可能截断）
                logger.warning(f'{code} 窗口 {start_date}~{end_date} 达安全阀页数上限，停止翻页')
                break
            page_num += 1
        return announcements

    # ---------- 全市场窗口检索（每日复盘方案 4.6/4.13 增量方法，不影响既有按股检索） ----------

    def query_market_window(self, start_date: str, end_date: str,
                            searchkey: str = '') -> List[Dict[str, Any]]:
        """全市场窗口检索：不指定 stock，返回窗口内全部公告（按 announcementId 去重）。

        searchkey：标题关键词过滤（2026-09-28 实测可用，如"限售股上市流通"→近30日 180 条），
        缺省空串保持全量行为不变（公告清单路径不受影响）。
        分页数超上限时对半拆窗递归（巨潮窗口检索可回溯任意历史，失败窗口次日并抓安全）。
        实测（2026-09-27）：stock='' + column='szse' 返回全市场（沪深北），column 对检索不敏感。
        """
        try:
            results = self._query_market_window(start_date, end_date, searchkey=searchkey)
        except CninfoWindowTooLarge:
            s = date.fromisoformat(start_date)
            e = date.fromisoformat(end_date)
            if s >= e:
                raise
            mid = s + (e - s) / 2
            logger.info(f'全市场窗口 {start_date}~{end_date} 超分页上限，对半拆分')
            results = (self.query_market_window(start_date, mid.isoformat())
                       + self.query_market_window((mid + timedelta(days=1)).isoformat(), end_date))

        merged: Dict[str, Dict[str, Any]] = {}
        for a in results:
            key = str(a.get('announcementId') or a.get('adjunctUrl') or '')
            if key:
                merged[key] = a
        return list(merged.values())

    def _query_market_window(self, start_date: str, end_date: str,
                             searchkey: str = '') -> List[Dict[str, Any]]:
        announcements: List[Dict[str, Any]] = []
        page_num = 1
        while True:
            payload = {
                'pageNum': page_num,
                'pageSize': self.page_size,
                'column': 'szse',
                'tabName': 'fulltext',
                'plate': '',
                'stock': '',
                'searchkey': searchkey,
                'secid': '',
                'category': '',
                'trade': '',
                'seDate': f'{start_date}~{end_date}',
                'sortName': '',
                'sortType': '',
                'isHLtitle': 'true',
            }
            data = self._post_query(payload)
            batch = data.get('announcements') or []
            if not batch:
                break
            announcements.extend(batch)
            has_more = str(data.get('hasMore') or '').lower() == 'true'
            total = int(data.get('totalAnnouncement') or 0)
            expected_pages = -(-total // self.page_size) if total > 0 else None
            if expected_pages is not None and expected_pages > self.max_pages_per_window:
                raise CninfoWindowTooLarge(
                    f'全市场窗口 {start_date}~{end_date} 共 {total} 条/{expected_pages} 页，超上限')
            if not has_more:
                break
            if expected_pages is not None and page_num >= expected_pages:
                break
            if page_num >= self.max_pages_per_window * 2:
                logger.warning(f'全市场窗口 {start_date}~{end_date} 达安全阀页数上限，停止翻页')
                break
            page_num += 1
        return announcements

    # ---------- 标题解析 ----------

    @staticmethod
    def parse_report_title(title: str) -> Optional[Tuple[int, str, bool]]:
        """标题 -> (报告期年份, NN, 是否修订版)；非正文条目或无法解析返回 None

        NN：01=一季报 02=半年报 03=三季报 04=年报
        """
        if not title:
            return None
        t = title.replace('（', '(').replace('）', ')')
        if any(k in t for k in EXCLUDE_KEYWORDS):
            return None
        for pattern, nn in _TITLE_PATTERNS:
            m = pattern.search(t)
            if m:
                is_revision = any(k in t for k in _REVISION_MARKS)
                return int(m.group(1)), nn, is_revision
        return None

    @staticmethod
    def period_end_date(year: int, nn: str) -> date:
        """报告期结束日（进度表 last_stat_date 口径）"""
        return date.fromisoformat(f'{year}{NN_PERIOD_END[nn]}')

    @staticmethod
    def sanitize_dir_name(name: str) -> str:
        """目录名清洗：去除文件系统非法字符（*ST -> ST）"""
        return _FS_FORBIDDEN.sub('', (name or '').strip())

    # ---------- PDF 下载 ----------

    @staticmethod
    def validate_pdf_file(path: Path) -> bool:
        """落盘文件有效性：存在、非空、%PDF 文件头"""
        try:
            if not path.exists() or path.stat().st_size <= 0:
                return False
            with open(path, 'rb') as f:
                return f.read(5) == b'%PDF-'
        except OSError:
            return False

    def download_pdf(self, adjunct_url: str, target_path: Path,
                     overwrite: bool = False) -> Dict[str, Any]:
        """下载 PDF 到 target_path（原子落盘：先写 .part 再替换）。

        Args:
            overwrite: True 时跳过「已存在即有效」短路，强制重下（用于更新/修订版覆盖）

        Returns:
            {'status': 'exists_valid'|'downloaded'|'failed', 'size': int, 'error': Optional[str]}
        """
        if not overwrite and self.validate_pdf_file(target_path):
            return {'status': 'exists_valid', 'size': target_path.stat().st_size, 'error': None}

        url = f'{self.static_base_url}/{adjunct_url.lstrip("/")}'
        if self.download_interval > 0:
            time.sleep(self.download_interval)
        part_path = target_path.with_suffix(target_path.suffix + '.part')
        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            resp = self._session.get(
                url, timeout=self.timeout, stream=True,
                headers={'User-Agent': self.user_agent,
                         'Referer': f'{self.query_base_url}/'})
            if resp.status_code in (403, 429):
                raise CninfoBlockedError(f'HTTP {resp.status_code}，疑似触发风控')
            resp.raise_for_status()
            expected = resp.headers.get('Content-Length')
            size = 0
            with open(part_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
                        size += len(chunk)
            with open(part_path, 'rb') as f:
                head = f.read(5)
            if head != b'%PDF-':
                raise ValueError(f'文件头非法: {head!r}')
            if expected and int(expected) != size:
                raise ValueError(f'Content-Length 不一致: 声明 {expected}，实收 {size}')
            part_path.replace(target_path)
            logger.info(f'下载完成: {target_path.name}（{size} 字节）')
            return {'status': 'downloaded', 'size': size, 'error': None}
        except (requests.RequestException, CninfoBlockedError, ValueError, OSError) as e:
            try:
                part_path.unlink(missing_ok=True)
            except OSError:
                pass
            logger.warning(f'下载失败 {url}: {e}')
            return {'status': 'failed', 'size': 0, 'error': str(e)}

    def fetch_pdf_size_kb(self, announcement: Dict[str, Any]) -> int:
        """adjunctSize（KB）——dry-run 容量估算用，缺失返回 0"""
        try:
            return int(announcement.get('adjunctSize') or 0)
        except (TypeError, ValueError):
            return 0
