"""国证行业数据源（盘后采集数据源整合详细技术方案 V1.0 §6 批次 B）。

两步取数（2026-11-26 实测锁定）：
1. 目录 ``GET https://www.cnindex.com.cn/syl/YYYY-MM-DD/cninfo.json``——root 为
   分组数组，取 ``id=4``（plate=深沪京市场）分组；``sylItemVoList`` 中 ``tl=1``
   的一级行业：``tc`` 行业码 / ``tn`` 名称 / ``tt`` 查询分类 / ``ca`` 原始数量 /
   ``date`` 目录日期。各市场子组与全市场组有重叠，**不相加**（只取 id=4 组）。
2. 明细 ``POST https://www.cnindex.com.cn/queryStockSylDetail``——查询参数
   ``plateCode=4, category=<tt>, industry=<tc>, dateStr=YYYY-MM-DD, pageNo,
   pageSize=1000``；按 **``data.total`` 与 ``data.rows``** 分页，核每行
   ``tradeDate`` 与 ``industryCode1``。剔除只有 ``stockCodeB`` 的纯 B 股行；
   双代码证券只用其 ``stockCode`` 对应的 A 股。

安全红线（同全部外网源）：串行限速、正常 UA、风控识别即退避、仅自用不分发。
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import requests

from .http_collector_base import ReviewBlockedError, response_snippet

logger = logging.getLogger(__name__)

CNINDEX_BASE = 'https://www.cnindex.com.cn'
CATALOG_PATH = '/syl/{date}/cninfo.json'
DETAIL_PATH = '/queryStockSylDetail'

#: 目录解析版本（§6：分类体系快照随版本记录）
CATALOG_VERSION = 'v1'

_NUM_CLEAN = re.compile(r'[,，\s]')
DEFAULT_UA = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
              'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36')


class CnindexIndustrySource:
    """国证行业：目录 + 逐一级行业明细分页（串行限速）"""

    def __init__(self, config: Dict[str, Any], gate: Optional[Any] = None):
        review = (config or {}).get('review_sync', {}) or {}
        cni_cfg = (review.get('industry_cni') or {}) or {}
        self.page_size = int(cni_cfg.get('page_size', 1000))
        self.request_interval = float(cni_cfg.get('interval_seconds', 1.0))
        self.timeout = (5.0, 30.0)
        self.user_agent = review.get('user_agent') or DEFAULT_UA
        self.bypass_proxy = bool(review.get('bypass_proxy', False))
        self.gate = gate  # 预留：cnindex 域登记（批次内先用源内节拍）

    def _headers(self) -> Dict[str, str]:
        return {'User-Agent': self.user_agent,
                'Accept': 'application/json,text/html;q=0.9,*/*;q=0.8',
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': f'{CNINDEX_BASE}/module/analysis-pe.html'}

    def _sleep(self) -> None:
        if self.request_interval > 0:
            import time
            time.sleep(self.request_interval)

    @staticmethod
    def _raise_blocked(text: str, status: int) -> None:
        raise ReviewBlockedError(
            f'[cnindex] HTTP {status} 非 JSON（疑似风控/改版）: {response_snippet_from_text(text)}',
            600, kind='blocked_page', host='www.cnindex.com.cn')

    # ---------- 目录 ----------

    def fetch_catalog(self, trade_date: str) -> Dict[str, Any]:
        """当日目录 -> {'industries': [{code,name,category,expected,date}], 'catalog_date'}

        trade_date: yyyyMMdd（目录路径用 YYYY-MM-DD）。目录未发布（404/无 id=4 组）
        时抛 ValueError——由 manager 标待补（未发布 ≠ 已确认空）。
        """
        date_param = f'{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}'
        url = f'{CNINDEX_BASE}{CATALOG_PATH.format(date=date_param)}'
        self._sleep()
        resp = requests.get(url, headers=self._headers(), timeout=self.timeout,
                            proxies=({'http': None, 'https': None}
                                     if self.bypass_proxy else None))
        if resp.status_code != 200:
            raise ValueError(f'国证目录 HTTP {resp.status_code}: '
                             f'{response_snippet(resp)[:120]}（未发布或改版）')
        try:
            payload = resp.json()
        except ValueError:
            self._raise_blocked(resp.text, resp.status_code)
        groups = payload if isinstance(payload, list) else (payload.get('data') or [])
        target = None
        for g in groups or []:
            if isinstance(g, dict) and int(g.get('id') or -1) == 4:
                target = g
                break
        if target is None:
            raise ValueError('国证目录无 id=4 深沪京市场分组（结构变更）')
        industries: List[Dict[str, Any]] = []
        for it in target.get('sylItemVoList') or []:
            if str(it.get('tl') or '') != '1':
                continue
            industries.append({
                'code': str(it.get('tc') or ''),
                'name': str(it.get('tn') or ''),
                'category': str(it.get('tt') or ''),
                'expected': int(str(it.get('ca') or '0') or 0),
                'catalog_date': str(it.get('date') or ''),
            })
        if not industries:
            raise ValueError(f'国证目录 {trade_date} 无一级行业（未发布或结构变更）')
        return {'industries': industries,
                'catalog_date': industries[0]['catalog_date'].replace('-', '')}

    # ---------- 明细 ----------

    def fetch_industry_detail(self, trade_date: str, industry: Dict[str, Any],
                              ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """单个一级行业全部成员（分页闭合 + 行级校验）。

        Returns:
            (rows, evidence)：rows 为剔除纯 B 股后的明细行（stockCode/tradeDate/
            industryCode1/industryName1 原样保留）；evidence 含 total/页数/
            b_only_剔除数。
        """
        date_param = f'{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}'
        rows: List[Dict[str, Any]] = []
        page = 1
        total = None
        b_only = 0
        while True:
            self._sleep()
            resp = requests.post(
                f'{CNINDEX_BASE}{DETAIL_PATH}',
                params={'plateCode': 4, 'category': industry['category'],
                        'industry': industry['code'], 'dateStr': date_param,
                        'pageNo': page, 'pageSize': self.page_size},
                headers=self._headers(), timeout=self.timeout,
                proxies=({'http': None, 'https': None}
                         if self.bypass_proxy else None))
            if resp.status_code != 200:
                raise ValueError(f'国证明细 {industry["code"]} HTTP {resp.status_code}: '
                                 f'{response_snippet(resp)[:120]}')
            try:
                payload = resp.json()
            except ValueError:
                self._raise_blocked(resp.text, resp.status_code)
            data = payload.get('data') or {}
            batch = data.get('rows') or []
            page_total = int(data.get('total') or 0)
            total = page_total if total is None else total
            for r in batch:
                td = str(r.get('tradeDate') or '')[:10].replace('-', '')
                if td and td != trade_date:
                    raise ValueError(f'国证明细行 tradeDate={td} ≠ 请求日 {trade_date}（错日拒收）')
                code_a = str(r.get('stockCode') or '').strip()
                code_b = str(r.get('stockCodeB') or '').strip()
                if not code_a and code_b:
                    b_only += 1
                    continue            # 纯 B 股行剔除（§6：先闭合分页再剔除）
                if not code_a:
                    continue            # 无 A 股代码且无 B 股代码的脏行
                rows.append(r)
            if page_total and len(rows) + b_only >= page_total:
                break
            if not batch:
                raise ValueError(f'国证明细 {industry["code"]} 第 {page} 页空'
                                 f'（已取 {len(rows)}/{total}，少页拒收）')
            if page > 20:
                raise ValueError(f'国证明细 {industry["code"]} 达安全阀页数')
            page += 1
        if total and len(rows) + b_only != total:
            raise ValueError(f'国证明细 {industry["code"]} 行数不闭合: '
                             f'{len(rows)}+{b_only} ≠ {total}')
        expected = int(industry.get('expected') or 0)
        if expected and len(rows) > expected:
            raise ValueError(f'国证明细 {industry["code"]} 行数 {len(rows)} > 目录数 {expected}')
        return rows, {'total': total, 'pages': page - 1, 'b_only_dropped': b_only,
                      'expected': expected, 'kept': len(rows)}

    # ---------- 全市场快照 ----------

    def fetch_market_snapshot(self, trade_date: str) -> Dict[str, Any]:
        """当日全市场一级快照（11 个行业逐个取数；任一行业失败整体失败——
        §6：先完成所有页级对账，再一事务发布整套当日快照）。"""
        catalog = self.fetch_catalog(trade_date)
        members: Dict[str, Dict[str, Any]] = {}   # ts_code -> row
        evidence: List[Dict[str, Any]] = []
        for ind in catalog['industries']:
            rows, ev = self.fetch_industry_detail(trade_date, ind)
            ev['industry'] = f"{ind['code']} {ind['name']}"
            evidence.append(ev)
            for r in rows:
                code = str(r.get('stockCode') or '')
                ts_code = _cnindex_to_ts_code(code)
                prev = members.get(ts_code)
                if prev is not None and prev.get('industryCode1') != ind['code']:
                    raise ValueError(f'{ts_code} 同时出现在 '
                                     f'{prev.get("industryCode1")} 与 {ind["code"]}（分类体系异常）')
                members[ts_code] = r
        return {'trade_date': trade_date, 'catalog': catalog,
                'members': members, 'evidence': evidence,
                'unique_codes': len(members)}


def _cnindex_to_ts_code(code6: str) -> str:
    """国证 6 位代码 -> ts_code（与 review_common.code6_to_ts_code 同一口径）"""
    from ..sync.review_common import code6_to_ts_code
    return code6_to_ts_code(code6)


def response_snippet_from_text(text: str, limit: int = 120) -> str:
    return ' '.join(text.split())[:limit]
