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
from urllib.parse import urlparse

import requests

from ..utils.retry import run_with_retry
from .domain_gate import build_endpoint_tag, get_gate, parse_retry_after
from .errors import KIND_BLOCKED_PAGE, KIND_FORBIDDEN, KIND_RATE_LIMITED

logger = logging.getLogger(__name__)

QUERY_PATH = '/new/hisAnnouncement/query'
STOCK_MAP_PATH = '/new/data/szse_stock.json'
# 两融全市场明细（盘后采集数据源整合详细技术方案 V1.0 §4.1：POST，查询参数
# tdate=YYYY-MM-DD；单次读 data.records，核 resultCode/count=total/TRADEDATE）
MARGIN_DETAIL_PATH = '/data20/marginTrading/detailList'
# 龙虎榜公开信息（§5.1：POST，sdate=edate=YYYY-MM-DD，page/rows=100；
# 事件行在 marketList，核 total/totalPage/count；detail 买卖各 5 位置，单位元）
LHB_STATIS_PATH = '/data/statis/getMarketStatisticsData'

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
    """巨潮风控触发（返回非 JSON/403/429/限流提示）：退避冷却后可重试

    结构化字段（限流方案 20261120 §1）：kind/host/retry_after 随异常保留，
    供管理器即时登记主机级域冷却与编排器结构化记账。
    """

    def __init__(self, message: str, *, kind: Optional[str] = None,
                 host: Optional[str] = None, retry_after: Optional[int] = None,
                 cooldown_seconds: Optional[int] = None):
        super().__init__(message)
        self.kind = kind
        self.host = host
        self.retry_after = retry_after
        self.cooldown_seconds = int(cooldown_seconds) if cooldown_seconds else None


class CninfoWindowTooLarge(RuntimeError):
    """单检索窗口分页数超上限：调用方应对半拆分窗口后重查"""


class CninfoSource:
    """巨潮资讯网：orgId 映射、定期报告检索、标题解析、PDF 下载（串行限速）"""

    def __init__(self, config: Dict[str, Any], gate: Optional[Any] = None):
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
        # 请求级准入域门（限流方案 §2）：巨潮查询/静态下载两主机分别记账；
        # gate 未显式传入时用进程级单例（配置未开启时为本地 no-op）
        self.gate = gate if gate is not None else get_gate(config)
        self._query_host = urlparse(self.query_base_url).netloc.lower() or None
        self._static_host = urlparse(self.static_base_url).netloc.lower() or None

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
        url = f'{self.query_base_url}{QUERY_PATH}'
        endpoint = build_endpoint_tag(url, data=payload)
        # 请求级准入：冷却/端点暂停中直接拦截（DomainCooldownError，请求不外发）
        self.gate.acquire(url, endpoint=endpoint)
        resp = self._session.post(url, data=payload, headers=self._headers(),
                                  timeout=self.timeout)
        if resp.status_code in (403, 429):
            retry_after = parse_retry_after(resp.headers.get('Retry-After'))
            kind = KIND_RATE_LIMITED if resp.status_code == 429 else KIND_FORBIDDEN
            self.gate.report(url, endpoint=endpoint, kind=kind,
                             retry_after=retry_after, ok=False)
            raise CninfoBlockedError(f'HTTP {resp.status_code}，疑似触发风控',
                                     kind=kind, host=self._query_host,
                                     retry_after=retry_after)
        resp.raise_for_status()
        try:
            return resp.json()
        except ValueError as e:
            self.gate.report(url, endpoint=endpoint, kind=KIND_BLOCKED_PAGE, ok=False)
            raise CninfoBlockedError('检索返回非 JSON，疑似触发风控/验证码',
                                     kind=KIND_BLOCKED_PAGE, host=self._query_host) from e

    def _post_query(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """检索请求：网络类异常 3 次退避重试；明确拒绝（403/429/非 JSON）
        不重试——裸重试会放大封禁（限流方案 §1，2026-10-26 公告 403 先例）"""
        def _on_error(attempt: int, e: BaseException) -> None:
            logger.warning(f'巨潮检索第 {attempt} 次失败: {e}')
            time.sleep(2 ** attempt)

        return run_with_retry(
            lambda: self._attempt_query(payload),
            attempts=3, backoff=lambda a: 0,  # 退避在 on_error 中做，便于记录日志
            retry_on=(requests.RequestException,),
            on_error=_on_error,
        )

    # ---------- 股票映射 ----------

    def get_stock_map(self) -> Dict[str, Dict[str, str]]:
        """code -> {org_id, name, category, code}；仅保留 category=A股（含退市股/北交所）"""
        if self._stock_map is not None:
            return self._stock_map
        if self.query_interval > 0:
            time.sleep(self.query_interval)
        map_url = f'{self.query_base_url}{STOCK_MAP_PATH}'
        self.gate.acquire(map_url, endpoint='szse_stock.json')
        resp = self._session.get(map_url,
                                 headers={'User-Agent': self.user_agent}, timeout=60)
        if resp.status_code in (403, 429):
            kind = KIND_RATE_LIMITED if resp.status_code == 429 else KIND_FORBIDDEN
            self.gate.report(map_url, endpoint='szse_stock.json', kind=kind, ok=False)
            raise CninfoBlockedError(f'映射读取 HTTP {resp.status_code}，疑似触发风控',
                                     kind=kind, host=self._query_host)
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
                            searchkey: str = '', category: str = '') -> List[Dict[str, Any]]:
        """全市场窗口检索：不指定 stock，返回窗口内全部公告（按 announcementId 去重）。

        searchkey：标题关键词过滤（2026-09-28 实测可用，如"限售股上市流通"→近30日 180 条），
        缺省空串保持全量行为不变（公告清单路径不受影响）。
        category：分号分隔的巨潮分类码（如 ';'.join(PERIODIC_CATEGORIES)），缺省空串保持
        全量行为不变（开发项 K 披露日回补用，探路 T18：必须带过滤，否则披露季单窗口超上限）。
        分页数超上限时对半拆窗递归（巨潮窗口检索可回溯任意历史，失败窗口次日并抓安全）。
        实测（2026-09-27）：stock='' + column='szse' 返回全市场（沪深北），column 对检索不敏感。
        """
        try:
            results = self._query_market_window(start_date, end_date,
                                                searchkey=searchkey, category=category)
        except CninfoWindowTooLarge:
            s = date.fromisoformat(start_date)
            e = date.fromisoformat(end_date)
            if s >= e:
                raise
            mid = s + (e - s) / 2
            logger.info(f'全市场窗口 {start_date}~{end_date} 超分页上限，对半拆分')
            results = (self.query_market_window(start_date, mid.isoformat(),
                                                searchkey=searchkey, category=category)
                       + self.query_market_window((mid + timedelta(days=1)).isoformat(), end_date,
                                                  searchkey=searchkey, category=category))

        merged: Dict[str, Dict[str, Any]] = {}
        for a in results:
            key = str(a.get('announcementId') or a.get('adjunctUrl') or '')
            if key:
                merged[key] = a
        return list(merged.values())

    def _query_market_window(self, start_date: str, end_date: str,
                             searchkey: str = '', category: str = '') -> List[Dict[str, Any]]:
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
                'category': category,
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

    # ---------- 两融全市场明细（§4.1 巨潮校验/补缺路径） ----------

    def fetch_margin_detail(self, trade_date: str) -> Dict[str, Any]:
        """巨潮两融全市场明细（trade_date: yyyyMMdd；返回原始 records 与完备证据）。

        - 请求：POST {query_base}/data20/marginTrading/detailList，查询参数
          ``tdate=YYYY-MM-DD``（文档实测口径）；
        - 完备证据：核 ``resultCode``、``count = total = len(records)``、逐行
          ``TRADEDATE`` 一致——任一不符即抛错拒收（不带日期证据的结果不作业务凭据）；
        - 字段语义（优化方案 §三）：``F001N/F002N/F003N/F008N`` 分别为融资余额/
          融资买入额/融资偿还额/融券余额（元），``F010V`` 定市场（sse/sz/bj 等），
          ``F009N`` 两融合计（用于 ``F001N + F008N = F009N`` 可比校验）；
        - 11-20 全市场 4,491 条，按项目范围保留沪深 4,138 条（北交所记录进入范围
          统计不落沪深表）；深市偿还额空值保留。

        Returns:
            {'records': [...], 'total': int, 'count': int, 'trade_date': 'YYYYMMDD'}
        """
        date_param = f'{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}'
        url = f'{self.query_base_url}{MARGIN_DETAIL_PATH}'
        endpoint = build_endpoint_tag(url)
        if self.query_interval > 0:
            time.sleep(self.query_interval)
        self.gate.acquire(url, endpoint=endpoint)
        resp = self._session.post(url, params={'tdate': date_param},
                                  headers=self._headers(), timeout=self.timeout)
        if resp.status_code in (403, 429):
            retry_after = parse_retry_after(resp.headers.get('Retry-After'))
            kind = KIND_RATE_LIMITED if resp.status_code == 429 else KIND_FORBIDDEN
            self.gate.report(url, endpoint=endpoint, kind=kind,
                             retry_after=retry_after, ok=False)
            raise CninfoBlockedError(f'两融明细 HTTP {resp.status_code}，疑似触发风控',
                                     kind=kind, host=self._query_host,
                                     retry_after=retry_after)
        resp.raise_for_status()
        try:
            data = resp.json()
        except ValueError as e:
            self.gate.report(url, endpoint=endpoint, kind=KIND_BLOCKED_PAGE, ok=False)
            raise CninfoBlockedError('两融明细返回非 JSON，疑似触发风控',
                                     kind=KIND_BLOCKED_PAGE,
                                     host=self._query_host) from e
        self.gate.report(url, endpoint=endpoint, ok=True)
        # 2026-11-26 实测响应结构：resultCode/resultMsg/count/total/records 均在
        # ``data`` 内（顶层为 path/code 元信息）；resultCode 成功值 '200'
        payload = data.get('data') or {}
        result_code = str(payload.get('resultCode')
                          or data.get('resultCode') or '')
        if result_code not in ('0', '200'):
            raise ValueError(f'巨潮两融 resultCode={result_code!r}（结构变更或参数错误）: '
                             f'{str(data)[:200]}')
        records = payload.get('records') or []
        total = int(payload.get('total') or 0)
        count = int(payload.get('count') or 0)
        if count != total or count != len(records):
            raise ValueError(f'巨潮两融完备性不符: count={count} total={total} '
                             f'len(records)={len(records)}（拒收防截断入账）')
        for r in records:
            td = str(r.get('TRADEDATE') or '')[:10].replace('-', '')
            if td and td != trade_date:
                raise ValueError(f'巨潮两融行 TRADEDATE={td} ≠ 请求日 {trade_date}（错日拒收）')
        return {'records': records, 'total': total, 'count': count,
                'trade_date': trade_date}

    # ---------- 龙虎榜公开信息（§5.1 巨潮席位路径） ----------

    def fetch_lhb_seats(self, trade_date: str, page_size: int = 5,
                        max_pages: int = 40) -> Dict[str, Any]:
        """巨潮交易公开信息（事件行 + 席位 detail；trade_date: yyyyMMdd）。

        2026-11-26 实测锁定：
        - 响应顶层：``total``（全品种事件数，11-20=73）、``totalPage``、
          ``count``（本页行数）、``marketList``（事件行数组）——rows 即页大小
          （rows=3 → totalPage=25），实测该接口对 rows>5 及连续调用有软风控：
          连续 10+ 次请求后返回 ``total=0`` 空数据（HTTP 200），需大间隔重试；
        - 事件行：``secCode``/``secName``/``tradeTime``/``type``（披露原因规则
          名原文）/``detail``（样本每起 5 个位置，buy/sell 席位成对，金额千分位
          字符串、单位元）；顶层 buyTotal/sellTotal 可能为空；
        - 超 1 页跟随 totalPage 翻页；count 与 len(marketList) 不符拒收。

        Returns:
            {'events': [row, ...], 'total': int, 'trade_date': 'YYYYMMDD'}
        """
        date_param = f'{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}'
        url = f'{self.query_base_url}{LHB_STATIS_PATH}'
        endpoint = build_endpoint_tag(url)
        events: List[Dict[str, Any]] = []
        total = None
        page = 1
        while True:
            if self.query_interval > 0:
                time.sleep(self.query_interval)
            self.gate.acquire(url, endpoint=endpoint)
            resp = self._session.post(
                url, params={'sdate': date_param, 'edate': date_param,
                             'page': page, 'rows': page_size},
                data={'sdate': date_param, 'edate': date_param,
                      'page': str(page), 'rows': str(page_size)},
                headers=self._headers(), timeout=self.timeout)
            if resp.status_code in (403, 429):
                retry_after = parse_retry_after(resp.headers.get('Retry-After'))
                kind = KIND_RATE_LIMITED if resp.status_code == 429 else KIND_FORBIDDEN
                self.gate.report(url, endpoint=endpoint, kind=kind,
                                 retry_after=retry_after, ok=False)
                raise CninfoBlockedError(f'龙虎榜公开信息 HTTP {resp.status_code}',
                                         kind=kind, host=self._query_host,
                                         retry_after=retry_after)
            resp.raise_for_status()
            try:
                data = resp.json()
            except ValueError as e:
                self.gate.report(url, endpoint=endpoint, kind=KIND_BLOCKED_PAGE, ok=False)
                raise CninfoBlockedError('龙虎榜公开信息返回非 JSON',
                                         kind=KIND_BLOCKED_PAGE,
                                         host=self._query_host) from e
            self.gate.report(url, endpoint=endpoint, ok=True)
            batch = data.get('marketList')
            if batch is None:
                raise ValueError(f'巨潮龙虎榜响应缺 marketList: {str(data)[:200]}')
            count = int(data.get('count') or 0)
            if count != len(batch):
                raise ValueError(f'巨潮龙虎榜 count={count} ≠ len(marketList)='
                                 f'{len(batch)}（拒收防截断）')
            if page == 1 and not batch and int(data.get('total') or 0) == 0:
                # 软风控形态：HTTP 200 + total=0 空数据（连续调用后触发）——
                # 与"当日确实无事件"无法区分，一律按风控报错留待补，不入账
                raise CninfoBlockedError('巨潮龙虎榜返回空数据（疑似软风控，稍后重试）',
                                         kind=KIND_BLOCKED_PAGE,
                                         host=self._query_host)
            for ev in batch:
                tt = str(ev.get('tradeTime') or '')[:10].replace('-', '')
                if tt and tt != trade_date:
                    raise ValueError(f'巨潮龙虎榜行 tradeTime={tt} ≠ 请求日 {trade_date}（错日拒收）')
            events.extend(batch)
            total = int(data.get('total') or total or 0)
            total_page = int(data.get('totalPage') or 0)
            if (total and len(events) >= total) or (total_page and page >= total_page):
                break
            if page >= max_pages:
                raise ValueError(f'巨潮龙虎榜达安全阀页数 {page}（少页拒收）')
            page += 1
        if total and len(events) != total:
            raise ValueError(f'巨潮龙虎榜事件数与官方总数不一致: {len(events)} ≠ {total}')
        return {'events': events, 'total': total, 'trade_date': trade_date}

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
        endpoint = build_endpoint_tag(url)
        # 静态下载主机独立记账（限流方案 §2：查询与静态主机分别准入）
        self.gate.acquire(url, endpoint=endpoint)
        part_path = target_path.with_suffix(target_path.suffix + '.part')
        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            resp = self._session.get(
                url, timeout=self.timeout, stream=True,
                headers={'User-Agent': self.user_agent,
                         'Referer': f'{self.query_base_url}/'})
            if resp.status_code in (403, 429):
                retry_after = parse_retry_after(resp.headers.get('Retry-After'))
                kind = KIND_RATE_LIMITED if resp.status_code == 429 else KIND_FORBIDDEN
                self.gate.report(url, endpoint=endpoint, kind=kind,
                                 retry_after=retry_after, ok=False)
                raise CninfoBlockedError(f'HTTP {resp.status_code}，疑似触发风控',
                                         kind=kind, host=self._static_host,
                                         retry_after=retry_after)
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
            self.gate.report(url, endpoint=endpoint, ok=True)
            logger.info(f'下载完成: {target_path.name}（{size} 字节）')
            return {'status': 'downloaded', 'size': size, 'error': None}
        except CninfoBlockedError:
            # 明确拒绝向上抛：停止同主机后续附件与末轮自动重试（限流方案 §1/§3 P0）
            try:
                part_path.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        except (requests.RequestException, ValueError, OSError) as e:
            try:
                part_path.unlink(missing_ok=True)
            except OSError:
                pass
            self.gate.report(url, endpoint=endpoint, ok=True)
            logger.warning(f'下载失败 {url}: {e}')
            return {'status': 'failed', 'size': 0, 'error': str(e)}

    def fetch_pdf_size_kb(self, announcement: Dict[str, Any]) -> int:
        """adjunctSize（KB）——dry-run 容量估算用，缺失返回 0"""
        try:
            return int(announcement.get('adjunctSize') or 0)
        except (TypeError, ValueError):
            return 0
