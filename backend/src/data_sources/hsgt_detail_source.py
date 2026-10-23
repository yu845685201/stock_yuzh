"""北向个股明细数据源（第三阶段-3；东财 datacenter 直连采集）。

不依赖 akshare 库——接口事实经 2026-10-23 requests 实测锁定（本文件即接口
文档；如上游变更，先实测再改，禁照抄记忆）。

接口（东财 datacenter，与 macro_source 同族）：
- ``RPT_MUTUAL_HOLDSTOCKNDATE_STA``：个股北向持股历史（filter
  ``(SECURITY_CODE="600519")(INTERVAL_TYPE="1")``，TRADE_DATE 倒序全量；
  实测茅台 1683 行，**数据止于 2024-08-16**——沪深交易所自 2024-08-19 起
  停止披露北向个股每日明细（真实口径变化，非采集缺陷；此后仅季度/总额），
  本采集器如实记录该边界，日增量届时自然 no_data）；
- 字段：TRADE_DATE/HOLD_SHARES(持股数量)/HOLD_MARKET_CAP(持股市值)/
  HOLD_SHARES_RATIO(占A股%)/ADD_SHARES_REPA(增持股数)/ADD_MARKET_CAP(增持资金)。

总额口径（hkex_connect_daily，港交所日统计）不在本模块——那是每日仍在发布
的真实总额（北向仅成交总额），由 hkex_connect_manager 采集；本模块只补
**个股明细**，两层口径在报告层分工标注（近端用总额、历史明细用本表）。

口径注记：
- MUTUAL_TYPE 001=沪股通/003=深股通；同一股票可两侧都有持仓 → 按
  (trade_date, ts_code) 聚合（sum 持股/市值，max 占比），不双计；
- 采集按**个股全量历史**拉取（无按日截面接口：东财日截面报表
  RPT_MUTUAL_STOCK_NORTHSTA 已下线，实测「服务器繁忙」），故日增量 =
  对上次有持仓的股票重拉（发现新日期即入库）；
- upsert 按 (trade_date, ts_code) 幂等。

安全红线（同全部外网源）：串行限速、正常 UA/Referer、不破解任何技术措施、
风控识别即退避冷却、仅自用不分发。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .http_collector_base import HttpCollectorBase

logger = logging.getLogger(__name__)

EM_DATACENTER_URL = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
REPORT_HOLD_STOCK = 'RPT_MUTUAL_HOLDSTOCKNDATE_STA'

#: MUTUAL_TYPE → 通道名（明细分侧记录）
MUTUAL_TYPE_MAP = {'001': '沪股通', '003': '深股通'}


class HsgtDetailSource(HttpCollectorBase):
    """北向个股明细采集（东财）。"""

    def __init__(self, config: Dict[str, Any]):
        review = (config or {}).get('review_sync', {}) or {}
        nf_cfg = review.get('north_flow_detail') or {}
        super().__init__(
            config, source_name='hsgt_detail',
            interval_seconds=float(nf_cfg.get('interval_seconds', 1.0)),
            referer='https://data.eastmoney.com/')

    def fetch_stock_history(self, code6: str) -> List[Dict[str, Any]]:
        """单只股票北向持股全量历史（6 位代码；分页拉全，result.data 为空 → []）。"""
        base_params = {
            'sortColumns': 'TRADE_DATE', 'sortTypes': '-1',
            'pageSize': '500',
            'reportName': REPORT_HOLD_STOCK, 'columns': 'ALL',
            'source': 'WEB', 'client': 'WEB',
            'filter': f'(SECURITY_CODE="{code6}")(INTERVAL_TYPE="1")',
        }
        out: List[Dict[str, Any]] = []
        page = 1
        while page <= 20:   # 单股历史上限保护（实测最长 ~1700 行 = 4 页）
            params = dict(base_params, pageNumber=str(page))
            try:
                j = self.get_json(EM_DATACENTER_URL, params=params)
            except Exception as e:
                logger.warning('北向明细 %s 第 %d 页失败: %r', code6, page, e)
                break
            result = (j or {}).get('result') if isinstance(j, dict) else None
            data = (result or {}).get('data') or []
            if not data:
                break
            out.extend(data)
            if len(data) < int(base_params['pageSize']):
                break     # 末页
            page += 1
        return out


def parse_hold_row(row: Dict[str, Any], code6: str) -> Optional[Dict[str, Any]]:
    """个股持股原始行 → north_flow_detail_daily 行（MUTUAL_TYPE 通道带在 detail）。

    数值千分位清洗；占比为百分点数值（6.55 = 6.55%）。
    """
    d = str(row.get('TRADE_DATE') or '')[:10].replace('-', '')
    if not d:
        return None
    channel = MUTUAL_TYPE_MAP.get(str(row.get('MUTUAL_TYPE') or ''), '')
    return {
        'trade_date': d,
        'ts_code': code6_to_ts_code(code6),
        'hold_shares': _num(row.get('HOLD_SHARES')),
        'hold_market_value': _num(row.get('HOLD_MARKET_CAP')),
        'hold_ratio': _num(row.get('HOLD_SHARES_RATIO')),
        'change_shares': _num(row.get('ADD_SHARES_REPA')),
        'change_amount': _num(row.get('ADD_MARKET_CAP')),
        'channel': channel,
        'stock_name': str(row.get('SECURITY_NAME') or '') or None,
    }


def code6_to_ts_code(code6: str) -> str:
    """6 位代码 → ts_code（复用 review_common 同款规则，原则 P6）"""
    from ..sync.review_common import code6_to_ts_code as _impl
    return _impl(code6)


def _num(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).replace(',', '').strip()
    if s in ('', '--', '-', 'None', 'nan'):
        return None
    try:
        return float(s)
    except ValueError:
        return None
