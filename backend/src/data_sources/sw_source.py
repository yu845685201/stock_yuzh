"""
申万宏源研究官网数据源 —— 申万行业分类 2021 版（缺失数据采集方案 V1.0 §3 增强口径，source=sw_2021）

接口事实（2026-09-28 浏览器探路锁定，评审记录 doc/notes/20260928-缺失数据源变更评审.md）：
- 清单：GET https://www.swsresearch.com/institute-sw/api/download_center/trade_classification/
        ?page=1&page_size=50&indextype=一级行业
  → {"code":"200","data":{"count":31,"results":[{"id":"GUID","swindexname":"农林牧渔"},...]}}
  page_size=50 一次拿全 31 个一级行业
- 文件：GET /institute-sw/api/download_center/download_file/?file_name={swindexname}分类表
  → application/octet-stream，实为 xlsx（PK 头），4 列：行业名称/股票代码/股票名称/计入日期；
  股票代码为整数需补零 6 位（19 -> 000019）
- 全量：GET /institute-sw/api/download_center/all_trade_classification_download/（**尾斜杠必须**）
  → SwClass.xls，实为 GBK 编码 HTML 表格，列：行业名称/股票代码/股票名称/起始日期/结束日期
  （2026-10-02 作者实测：全新会话直接 200，无需会话态/页面预热；第四轮"301 落首页"
    系 URL 未带尾斜杠；比逐行业 xlsx 多结束日期列 → 可实现一级时点成分，G 项改良）
- 页面：https://www.swsresearch.com/institute_sw/allIndex/downloadCenter/industryType（Referer）
- 文件按日更新（下载中心页面表格日期为发布日口径）
- 申万老官网 swsindex.com 已失效（2026-09-28 实测连接失败），本源为唯一官方免费路径

安全红线：串行限速、正常 UA/Referer、不破解任何技术措施、风控识别即退避冷却、仅自用不分发。
"""

import io
import logging
from html.parser import HTMLParser
from typing import Any, Dict, List

import requests

from .http_collector_base import HttpCollectorBase, ReviewBlockedError

logger = logging.getLogger(__name__)

LIST_URL = ('https://www.swsresearch.com/institute-sw/api/'
            'download_center/trade_classification/')
FILE_URL = ('https://www.swsresearch.com/institute-sw/api/'
            'download_center/download_file/?file_name={name}分类表')
# 尾斜杠必须：不带时 301 → 首页（第四轮探路踩坑）
ALL_DOWNLOAD_URL = ('https://www.swsresearch.com/institute-sw/api/'
                    'download_center/all_trade_classification_download/')
SW_REFERER = 'https://www.swsresearch.com/'

_EXPECTED_COLUMNS = ('行业名称', '股票代码')


class SwSectorBlockedError(ReviewBlockedError):
    """申万源风控异常（4xx/非 JSON/结构变更）"""


DEFAULT_CA_BUNDLE = 'config/swsresearch_ca.pem'


class SwSectorSource(HttpCollectorBase):
    """申万行业分类：一级行业清单 + 每行业成分 xlsx（31+1 请求/次）

    证书策略（P0-3，复盘报告层与 P0 修复技术方案 §三）：verify 优先级
    **系统/certifi 默认 → SSLError 降级手工 CA bundle 重试 → 仍失败抛可操作错误**
    （保留诊断分支，不吞异常）。手工 bundle 仅作兜底资产，不删除。

    venv 实测结论（2026-09-30，certifi 2026.06.17 + OpenSSL 3.0.13）：
    certifi 默认路径握手仍失败（unable to get local issuer certificate，站点只发叶子
    证书），手工 bundle（GeoTrust 中间 + DigiCert Global Root G2，config/swsresearch_ca.pem）
    兜底 HTTP 200 通过。上游 2026-08-19「证书链已正常」结论在本环境**不成立**；
    升级 certifi 或站点补链后 certifi 路径若直接通过，bundle 自动不再启用（降级仅在
    SSLError 时触发）。bundle 失效时按报错指引重抓 cacerts.digicert.cn 补链覆盖。
    """

    def __init__(self, config: Dict[str, Any]):
        review = (config or {}).get('review_sync', {}) or {}
        self._bundle_path = _resolve_ca_bundle(review.get('sw_ca_bundle') or DEFAULT_CA_BUNDLE)
        super().__init__(
            config, 'sw_sector',
            interval_seconds=float(review.get('sw_interval', 5.0)),
            referer=SW_REFERER,
            blocked_error_class=SwSectorBlockedError,
            verify=None,  # certifi 默认先行；SSLError 时 _request 降级 bundle
        )

    def _request(self, method: str, url: str, **kwargs):
        """certifi 失败降级手工 bundle（一次会话仅降级一次），双双失败抛可操作错误。"""
        try:
            return super()._request(method, url, **kwargs)
        except requests.exceptions.SSLError as e:
            if self.verify == self._bundle_path:
                raise RuntimeError(
                    '[sw_sector] TLS 握手失败（certifi 与手工 CA bundle 均无法建立连接）。'
                    '先试 pip install -U certifi；若走中间人代理需配置其 CA；'
                    f'手工 bundle（{self._bundle_path}）失效时重抓 cacerts.digicert.cn '
                    '补链后覆盖该文件') from e
            logger.warning(f'[sw_sector] certifi 校验失败，降级手工 CA bundle 重试: {e}')
            self.verify = self._bundle_path
            return super()._request(method, url, **kwargs)

    def fetch_industry_names(self) -> List[str]:
        """一级行业名清单（顺序即官网返回序）"""
        data = self.get_json(LIST_URL, params={
            'page': 1, 'page_size': 50, 'indextype': '一级行业'})
        results = ((data or {}).get('data') or {}).get('results') or []
        names = [str(r.get('swindexname') or '').strip() for r in results]
        return [n for n in names if n]

    def fetch_industry_members(self, name: str) -> List[str]:
        """单行业成分 -> [ts_code]（xlsx 内存解析；缺列/空文件抛 ValueError 由上层记录）"""
        content = self.get_content(FILE_URL.format(name=_quote(name)))
        df = _parse_classification_xlsx(content)
        # 惰性导入：data_sources 层不依赖 sync 包（避免循环导入），运行时复用统一前缀规则
        from ..sync.review_common import code6_to_ts_code
        members: List[str] = []
        for raw in df['股票代码'].tolist():
            ts_code = _code_cell_to_ts_code(raw, code6_to_ts_code)
            if ts_code:
                members.append(ts_code)
        return members

    def fetch_all_classification(self) -> List[Dict[str, str]]:
        """单请求全量一级分类（G 项改良：1 请求替代 31 次逐行业下载，且带时点日期）。

        探路/实测（V1.4，2026-10-02 作者复测）：all_trade_classification_download/
        **带尾斜杠** URL 全新会话直接 200（无需会话态/页面预热）；SwClass.xls 实为
        GBK 编码 HTML 表格，列：行业名称/股票代码/股票名称/起始日期/结束日期。

        Returns:
            [{industry, code6, name, in_date, out_date}]（in/out 归一为 yyyyMMdd，
            结束日期为空 → None=在册；解析行数 <1000 视为结构变更抛 ValueError）
        """
        content = self.get_content(ALL_DOWNLOAD_URL)
        rows = _parse_swclass_html(content)
        if len(rows) < 1000:
            raise ValueError(f'SwClass 全量分类解析行数异常: {len(rows)}（结构变更）')
        from ..sync.review_common import code6_to_ts_code
        out: List[Dict[str, str]] = []
        for r in rows:
            code6 = _code_cell_to_ts_code(r.get('code'), code6_to_ts_code)
            if not code6 or not r.get('industry'):
                continue
            out.append({'industry': r['industry'], 'code6': code6,
                        'name': r.get('name') or '',
                        'in_date': _norm_sw_date(r.get('in_date')),
                        'out_date': _norm_sw_date(r.get('out_date'))})
        return out


def _resolve_ca_bundle(path: str) -> str:
    """CA bundle 相对路径 -> 仓库根绝对路径（惰性导入避免 data_sources→sync 循环）"""
    from pathlib import Path
    p = Path(path)
    if p.is_absolute():
        return str(p)
    from ..sync.report_writer import resolve_repo_root
    return str(resolve_repo_root() / p)


def _quote(name: str) -> str:
    from urllib.parse import quote
    return quote(name)


def _parse_classification_xlsx(content: bytes):
    """xlsx 字节 -> DataFrame；列缺失/内容为空视为结构变更（调用方记失败）"""
    import pandas as pd
    df = pd.read_excel(io.BytesIO(content))
    missing = [c for c in _EXPECTED_COLUMNS if c not in df.columns]
    if missing or df.empty:
        raise ValueError(f'申万分类表结构变更（缺列 {missing} 或空表，'
                         f'实际列 {list(df.columns)}）')
    return df


class _HTMLTableParser(HTMLParser):
    """HTML 表格提取（SwClass.xls 为 GBK HTML 表格；标准库即可，无需 lxml）"""

    def __init__(self):
        super().__init__()
        self.rows: List[List[str]] = []
        self._row: Optional[List[str]] = None
        self._cell: Optional[List[str]] = None

    def handle_starttag(self, tag, attrs):
        if tag == 'tr':
            self._row = []
        elif tag in ('td', 'th') and self._row is not None:
            self._cell = []

    def handle_endtag(self, tag):
        if tag in ('td', 'th') and self._cell is not None and self._row is not None:
            self._row.append(''.join(self._cell).strip())
            self._cell = None
        elif tag == 'tr' and self._row is not None:
            if self._row:
                self.rows.append(self._row)
            self._row = None

    def handle_data(self, data):
        if self._cell is not None:
            self._cell.append(data)


def _parse_swclass_html(content: bytes) -> List[Dict[str, str]]:
    """SwClass（GBK HTML 表格）-> 行字典列表；表头按列名定位，退化为位置映射"""
    text = content.decode('gbk', errors='replace')
    parser = _HTMLTableParser()
    parser.feed(text)
    rows = [r for r in parser.rows if any(c for c in r)]
    if not rows:
        return []
    header = rows[0]
    idx = {name: i for i, name in enumerate(header)}
    if '行业名称' in idx:
        i_ind = idx['行业名称']
        i_code = idx.get('股票代码', i_ind + 1)
        i_name = idx.get('股票名称', i_code + 1)
        i_in = idx.get('起始日期', i_name + 1)
        i_out = idx.get('结束日期', i_in + 1)
    else:
        i_ind, i_code, i_name, i_in, i_out = 0, 1, 2, 3, 4
    out = []
    for r in rows[1:]:
        if len(r) <= i_code:
            continue
        out.append({'industry': r[i_ind], 'code': r[i_code], 'name': r[i_name],
                    'in_date': r[i_in] if i_in < len(r) else '',
                    'out_date': r[i_out] if i_out < len(r) else ''})
    return out


def _norm_sw_date(value: Any) -> str:
    """'2021/7/30 0:00:00'（SwClass 实测格式）/ '2021-12-10' / '20211230' -> yyyyMMdd；
    空串/无法解析返回 ''（结束日期为空 = 在册）"""
    s = str(value or '').strip().split(' ')[0]
    if not s:
        return ''
    for sep in ('-', '/'):
        if sep in s:
            parts = s.split(sep)
            if len(parts) == 3 and all(p.isdigit() for p in parts):
                y, m, d = parts
                return f'{y}{int(m):02d}{int(d):02d}'
    s2 = s.replace('-', '').replace('/', '')
    return s2 if len(s2) == 8 and s2.isdigit() else ''


def _code_cell_to_ts_code(raw: Any, to_ts_code) -> str:
    """股票代码单元格 -> ts_code；仅接受纯数字（整数补零 6 位，19 -> sz.000019），
    含字母/空值一律拒绝（申万分类表该列为纯数字，出现字母即脏数据）"""
    if raw is None or (isinstance(raw, float) and raw != raw):  # NaN
        return None
    s = str(raw).strip()
    try:
        code = str(int(float(s))).zfill(6)
    except (TypeError, ValueError):
        logger.debug(f'申万分类表跳过非法代码: {raw!r}')
        return None
    return to_ts_code(code)
