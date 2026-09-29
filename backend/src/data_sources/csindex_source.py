"""
中证指数公司官网数据源 —— 指数估值（方案 4.5，M2）

接口事实（2026-09-27 实测锁定）：
- GET https://oss-ch.csindex.com.cn/static/html/csindex/public/uploads/file/autofile/indicator/{code}indicator.xls
  （注意：csi-web-dev.oss bucket 已收紧 403；oss-ch 域名 200）
- 文件为 OLE2 .xls（需 xlrd），近 20 个交易日滚动窗口，列（双语表头）：
  日期Date / 指数代码 / 指数中文简称 / 市盈率1（总股本）P/E1 / 市盈率2（计算用股本）P/E2 /
  股息率1（总股本）D/P1 / 股息率2（计算用股本）D/P2
- 官方文件无 PE(TTM) 口径 → 按数据真实性原则 pe_ttm 留空；
  pe_static 取 P/E2（计算用股本，官网页面展示主口径）、dividend_yield 取 D/P2；
  首周按 §5.4 抽样比对，如确认 P/E1 更贴近官网展示可在配置切换
- 官网保留历史文件，失败次日可回溯补齐

安全红线：串行限速、正常 UA/Referer、不破解任何技术措施、风控识别即退避冷却、仅自用不分发。
"""

import io
import logging
from typing import Any, Dict, List, Optional

from .http_collector_base import HttpCollectorBase, ReviewBlockedError

logger = logging.getLogger(__name__)

INDICATOR_URL = ('https://oss-ch.csindex.com.cn/static/html/csindex/public/'
                 'uploads/file/autofile/indicator/{code}indicator.xls')

# 列名按前缀匹配（表头含中英双语，避免整串匹配脆弱）
_COL_DATE_PREFIX = '日期'
_COL_CODE_PREFIX = '指数代码'
_COL_NAME_PREFIX = '指数中文简称'
_COL_PE = {'pe1': '市盈率1', 'pe2': '市盈率2'}
_COL_DP = {'dp1': '股息率1', 'dp2': '股息率2'}


class CsindexFileNotPublished(Exception):
    """当日估值文件未发布（404/空窗口）：次日回溯补齐，不算风控"""


class CsindexSource(HttpCollectorBase):
    """中证指数官网：每日指数估值文件下载与解析（每日 1-2 请求）"""

    def __init__(self, config: Dict[str, Any]):
        review = (config or {}).get('review_sync', {}) or {}
        super().__init__(
            config, 'csindex',
            interval_seconds=float(review.get('csindex_interval', 2.0)),
            referer='https://www.csindex.com.cn/',
        )
        self.pe_static_source = review.get('csindex_pe_static_source', 'pe2')

    def fetch_indicator_file(self, index_code: str) -> List[Dict[str, Any]]:
        """下载并解析单个指数的估值文件，返回按日期升序的记录列表。

        Raises:
            CsindexFileNotPublished: 文件 404（当日未发布/该指数不属中证管理，如深交所指数）
            ReviewBlockedError: 403 等风控信号（由基类抛出）
        """
        content = self.get_content(INDICATOR_URL.format(code=index_code),
                                   allow_status=(404,))
        # allow_status 命中时返回原始 Response（如 404 页；中证 404 页为 JPEG 图片体）
        if isinstance(content, bytes):
            # OLE2 .xls 幻数校验（防御 404/拦截页伪装 200 的软失败）
            if len(content) < 512 or content[:4] != b'\xd0\xcf\x11\xe0':
                raise CsindexFileNotPublished(
                    f'{index_code} indicator 文件缺失或非法（{len(content)}B）')
        else:
            status = getattr(content, 'status_code', None)
            if status == 404:
                raise CsindexFileNotPublished(f'{index_code} indicator 文件未发布（HTTP 404）')
            content = content.content

        import pandas as pd
        df = pd.read_excel(io.BytesIO(content), engine='xlrd')
        rows = self._parse_dataframe(df, index_code)
        if not rows:
            raise ValueError(
                f'{index_code} indicator 文件解析为空，疑似列结构变更'
                f'（实际列: {list(df.columns)[:12]}）')
        return rows

    def fetch_latest(self, index_code: str, index_name: str) -> Optional[Dict[str, Any]]:
        """取文件内最新一条估值记录"""
        rows = self.fetch_indicator_file(index_code)
        return self.to_row(rows[-1], index_code, index_name) if rows else None

    # ---------- 解析 ----------

    def _parse_dataframe(self, df, index_code: str) -> List[Dict[str, Any]]:
        """列名前缀匹配提取（结构变更时对不上即返回空，由调用方报结构异常）"""
        cols = {str(c): c for c in df.columns}
        def _find(prefix: str) -> Optional[str]:
            for name, orig in cols.items():
                if name.startswith(prefix):
                    return orig
            return None

        col_date = _find(_COL_DATE_PREFIX)
        col_code = _find(_COL_CODE_PREFIX)
        col_name = _find(_COL_NAME_PREFIX)
        col_pe = {'pe1': _find(_COL_PE['pe1']), 'pe2': _find(_COL_PE['pe2'])}
        col_dp = {'dp1': _find(_COL_DP['dp1']), 'dp2': _find(_COL_DP['dp2'])}
        if col_date is None:
            return []

        out: List[Dict[str, Any]] = []
        for _, r in df.iterrows():
            raw_date = r.get(col_date)
            trade_date = self._normalize_date(raw_date)
            if trade_date is None:
                continue
            out.append({
                'trade_date': trade_date,
                'index_code': str(r.get(col_code) or index_code),
                'index_name': str(r.get(col_name) or index_code),
                'pe1': _to_float(r.get(col_pe['pe1'])),
                'pe2': _to_float(r.get(col_pe['pe2'])),
                'dp1': _to_float(r.get(col_dp['dp1'])),
                'dp2': _to_float(r.get(col_dp['dp2'])),
            })
        out.sort(key=lambda x: x['trade_date'])
        return out

    def to_row(self, parsed: Dict[str, Any], index_code: str,
               index_name: str) -> Dict[str, Any]:
        """解析记录 -> index_valuation_daily 行

        index_code 用请求码（文件内为短码如 300，落库统一 6 位）；
        pe 口径由配置决定，默认 P/E2（计算用股本）。
        """
        pe_static = parsed.get(self.pe_static_source)
        return {
            'trade_date': parsed['trade_date'],
            'index_code': index_code,
            'index_name': index_name or parsed['index_name'],
            'pe_ttm': None,            # 官方 indicator 文件无 TTM 口径，如实留空
            'pe_static': pe_static,
            'dividend_yield': parsed.get('dp2') if self.pe_static_source == 'pe2' else parsed.get('dp1'),
        }

    @staticmethod
    def _normalize_date(value: Any) -> Optional[str]:
        import datetime as _dt
        if value is None:
            return None
        if isinstance(value, (_dt.datetime, _dt.date)):
            return value.strftime('%Y%m%d')
        s = str(value).strip()[:10]
        if len(s) >= 8 and s[:8].isdigit():
            return s[:8]
        if '-' in s:
            parts = s.split('-')
            if len(parts) == 3 and all(p.isdigit() for p in parts):
                return ''.join(p.zfill(2) for p in parts)
        return None


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None or str(value).strip() in ('', '--'):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
