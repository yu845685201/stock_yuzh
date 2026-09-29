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
- 页面：https://www.swsresearch.com/institute_sw/allIndex/downloadCenter/industryType（Referer）
- 文件按日更新（下载中心页面表格日期为发布日口径）
- 申万老官网 swsindex.com 已失效（2026-09-28 实测连接失败），本源为唯一官方免费路径

安全红线：串行限速、正常 UA/Referer、不破解任何技术措施、风控识别即退避冷却、仅自用不分发。
"""

import io
import logging
from typing import Any, Dict, List

from .http_collector_base import HttpCollectorBase, ReviewBlockedError

logger = logging.getLogger(__name__)

LIST_URL = ('https://www.swsresearch.com/institute-sw/api/'
            'download_center/trade_classification/')
FILE_URL = ('https://www.swsresearch.com/institute-sw/api/'
            'download_center/download_file/?file_name={name}分类表')
SW_REFERER = 'https://www.swsresearch.com/'

_EXPECTED_COLUMNS = ('行业名称', '股票代码')


class SwSectorBlockedError(ReviewBlockedError):
    """申万源风控异常（4xx/非 JSON/结构变更）"""


DEFAULT_CA_BUNDLE = 'config/swsresearch_ca.pem'


class SwSectorSource(HttpCollectorBase):
    """申万行业分类：一级行业清单 + 每行业成分 xlsx（31+1 请求/次）

    证书说明（2026-09-28 实测）：swsresearch.com 只下发叶子证书、80 端口关闭，
    缺 GeoTrust 中间证书导致严格校验失败（浏览器有 AIA 补链能力故可见）。
    CA bundle（中间证书 + DigiCert Global Root G2）落盘 config/swsresearch_ca.pem，
    路径可由 review_sync.sw_ca_bundle 覆盖；重建环境后若失效重跑探路补链即可。
    """

    def __init__(self, config: Dict[str, Any]):
        review = (config or {}).get('review_sync', {}) or {}
        super().__init__(
            config, 'sw_sector',
            interval_seconds=float(review.get('sw_interval', 5.0)),
            referer=SW_REFERER,
            blocked_error_class=SwSectorBlockedError,
            verify=_resolve_ca_bundle(review.get('sw_ca_bundle') or DEFAULT_CA_BUNDLE),
        )

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
