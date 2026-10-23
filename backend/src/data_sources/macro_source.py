"""宏观指标数据源（第三阶段-1；东财 datacenter + 金十数据中心，直连 HTTP 采集）。

不依赖 akshare 库——接口事实经 2026-10-23 curl/requests 实测锁定（本文件即
接口文档；如上游变更，先实测再改，禁照抄记忆）。

已接入指标族（config review_sync.macro.indicators 控制）：
- ``LPR_*``：贷款市场报价利率（东财 RPTA_WEB_RATE；LPR1Y/LPR5Y）
- ``SHIBOR_*``：银行间同业拆放（金十 CDN il_1.json；8 档期限定价）
- ``PMI_*``：制造业/非制造业 PMI（东财 RPT_ECONOMY_PMI；MAKE_INDEX/NMAKE_INDEX）
- ``M1_YOY``：货币同比（东财 RPT_ECONOMY_CURRENCY_SUPPLY 的 CURRENCY_SAME）
- ``M2_YOY``：M2 同比（金十 reports/list_v2 attr_id=59——东财该报表无 M2 列；
  金十免费 tier 滞后约一年，报告层按发布日如实展示）
- ``SOCIAL_FINANCING``：社融新增人民币贷款（东财 RPT_ECONOMY_RMB_LOAN）
- ``CPI_YOY``：居民消费价格同比（东财 RPT_ECONOMY_CPI；NATIONAL_SAME=全国当月
  同比，实测更新至 2026-09）
- ``PPI_YOY``：工业品价格同比（金十 reports/list_v2 attr_id=60——东财无发布同比
  报表，其 RPT_ECONOMY_PPI 为定基指数不可直接用；金十免费 tier 数据滞后约一年，
  报告层按发布日如实展示，禁当最新值解读）

**已知缺口（如实记录）**：中债国债收益率曲线（chinabond yield 站有 JS 反爬，
GET/POST 均返回挑战页；新浪债券接口已下线）——本版不采，指标表留待后续
可访问时按 config 增补（indicator 注册表模式，加一行即可）。

口径注记：
- 东财 datacenter 返回 ``result.data``（分页）；金十 CDN 返回 ``values``
  按日期聚合；金十 reports/list_v2 返回 ``data.values``（[日期,今值,预测,前值]）；
- 宏观指标是**低频发布**（日/月频），采集器逐指标拉全量后按发布日过滤，
  不做逐日轮询（禁把月频指标当日频拉——浪费且无意义）；
- 发布日为指标自身日期列（非采集日），upsert 按 (trade_date, indicator_code)
  幂等；失败按两态契约分流（确实没有数据 = no_data；接口异常 = error）。

安全红线（同全部外网源）：串行限速、正常 UA/Referer、不破解任何技术措施、
风控识别即退避冷却、仅自用不分发。
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from .http_collector_base import HttpCollectorBase

logger = logging.getLogger(__name__)

EM_DATACENTER_URL = 'https://datacenter-web.eastmoney.com/api/data/v1/get'
JIN10_CDN_SHIBOR = 'https://cdn.jin10.com/data_center/reports/il_1.json'
JIN10_REPORTS_URL = 'https://datacenter-api.jin10.com/reports/list_v2'
#: 东财 LPR 报表 token（页面公开 token，实测 2026-10-23 可用）
_EM_TOKEN = '894050c76af8597a853f5b408b759f5d'
#: 金十 reports/list_v2 必需头（实测）
_JIN10_HEADERS = {
    'x-app-id': 'rU6QIu7JHe2gOUeR',
    'x-csrf-token': 'x-csrf-token',
    'x-version': '1.0.0',
}

#: 金十 attr_id 注册表（PPI/M2 年率；CPI 已切东财更新源）
#: 注：金十免费 tier 数据滞后约一年（实测 2026-10 仅到 2025-08/09），
#: 东财无发布同比报表（RPT_ECONOMY_PPI 为定基指数；货币供应报表无 M2 列）
JIN10_ATTR: Dict[str, int] = {'PPI_YOY': 60, 'M2_YOY': 59}

#: 指标元数据注册表（code → (中文名, 单位, 发布频率)）
INDICATOR_META: Dict[str, tuple] = {
    'LPR_1Y': ('LPR 1年期', '%', '月'),
    'LPR_5Y': ('LPR 5年期', '%', '月'),
    'SHIBOR_ON': ('SHIBOR 隔夜', '%', '日'),
    'SHIBOR_1W': ('SHIBOR 1周', '%', '日'),
    'SHIBOR_2W': ('SHIBOR 2周', '%', '日'),
    'SHIBOR_1M': ('SHIBOR 1月', '%', '日'),
    'SHIBOR_3M': ('SHIBOR 3月', '%', '日'),
    'SHIBOR_6M': ('SHIBOR 6月', '%', '日'),
    'SHIBOR_9M': ('SHIBOR 9月', '%', '日'),
    'SHIBOR_1Y': ('SHIBOR 1年', '%', '日'),
    'PMI_MFG': ('制造业PMI', '', '月'),
    'PMI_NONMFG': ('非制造业PMI', '', '月'),
    'M1_YOY': ('M1同比', '%', '月'),
    'M2_YOY': ('M2同比', '%', '月'),
    'SOCIAL_FINANCING': ('社融新增人民币贷款', '亿元', '月'),
    'CPI_YOY': ('CPI同比', '%', '月'),  # 东财源（更新快）
    'PPI_YOY': ('PPI同比', '%', '月'),
}

#: 默认指标集（config review_sync.macro.indicators 可覆盖）
DEFAULT_INDICATORS = list(INDICATOR_META)


class MacroSource(HttpCollectorBase):
    """宏观指标采集（东财 + 金十）。"""

    def __init__(self, config: Dict[str, Any]):
        review = (config or {}).get('review_sync', {}) or {}
        macro_cfg = review.get('macro') or {}
        super().__init__(
            config, source_name='macro',
            interval_seconds=float(macro_cfg.get('interval_seconds', 1.5)),
            referer='https://data.eastmoney.com/')

    # ---------- 东财 datacenter 通用 ----------

    def _em_fetch(self, report_name: str, *, sort_columns: str = 'REPORT_DATE',
                  sort_types: str = '-1', page_size: int = 500,
                  columns: str = 'ALL', extra_params: Optional[Dict[str, Any]] = None
                  ) -> List[Dict[str, Any]]:
        """东财 datacenter 拉取（单报表）；result.data 为空 → []"""
        params: Dict[str, Any] = {
            'reportName': report_name, 'columns': columns,
            'sortColumns': sort_columns, 'sortTypes': sort_types,
            'token': _EM_TOKEN, 'pageNumber': '1', 'pageSize': str(page_size),
            'source': 'WEB', 'client': 'WEB',
        }
        if extra_params:
            params.update(extra_params)
        try:
            j = self.get_json(EM_DATACENTER_URL, params=params)
        except Exception as e:
            logger.warning('东财 %s 请求失败: %r', report_name, e)
            return []
        result = (j or {}).get('result') if isinstance(j, dict) else None
        if not result or not result.get('data'):
            return []
        return list(result['data'])

    # ---------- 指标族拉取（返回 [{indicator_code, trade_date, value}]） ----------

    def fetch_lpr(self) -> List[Dict[str, Any]]:
        rows = self._em_fetch('RPTA_WEB_RATE', sort_columns='TRADE_DATE')
        out = []
        for r in rows:
            d = str(r.get('TRADE_DATE') or '')[:10].replace('-', '')
            if not d:
                continue
            for code, col in (('LPR_1Y', 'LPR1Y'), ('LPR_5Y', 'LPR5Y')):
                v = _num(r.get(col))
                if v is not None:
                    out.append({'indicator_code': code, 'trade_date': d, 'value': v})
        return out

    def fetch_pmi(self) -> List[Dict[str, Any]]:
        rows = self._em_fetch('RPT_ECONOMY_PMI')
        out = []
        for r in rows:
            d = str(r.get('REPORT_DATE') or '')[:10].replace('-', '')
            if not d:
                continue
            for code, col in (('PMI_MFG', 'MAKE_INDEX'), ('PMI_NONMFG', 'NMAKE_INDEX')):
                v = _num(r.get(col))
                if v is not None:
                    out.append({'indicator_code': code, 'trade_date': d, 'value': v})
        return out

    def fetch_money_supply(self) -> List[Dict[str, Any]]:
        """东财货币供应（RPT_ECONOMY_CURRENCY_SUPPLY；CURRENCY_SAME=货币 M1 同比）。

        **该报表无 M2 列**（实测字段仅 基础货币/货币/流通中现金及各自同比）——
        M2 同比走金十 attr_id=59（口径正确但免费 tier 滞后约一年）。
        """
        rows = self._em_fetch('RPT_ECONOMY_CURRENCY_SUPPLY')
        out = []
        for r in rows:
            d = str(r.get('REPORT_DATE') or '')[:10].replace('-', '')
            v = _num(r.get('CURRENCY_SAME'))
            if d and v is not None:
                out.append({'indicator_code': 'M1_YOY', 'trade_date': d,
                            'value': v})
        return out

    def fetch_social_financing(self) -> List[Dict[str, Any]]:
        rows = self._em_fetch('RPT_ECONOMY_RMB_LOAN')
        out = []
        for r in rows:
            d = str(r.get('REPORT_DATE') or '')[:10].replace('-', '')
            v = _num(r.get('RMB_LOAN'))
            if d and v is not None:
                out.append({'indicator_code': 'SOCIAL_FINANCING',
                            'trade_date': d, 'value': v})
        return out

    def fetch_shibor(self, tenor: Optional[str] = None) -> List[Dict[str, Any]]:
        """金十 CDN SHIBOR（values: {date: {tenor: [定价, 涨跌幅]}}）。

        ``tenor`` 指定档位（'ON'/'1W'/.../None=全档）；单指标拉取传对应档位，
        避免一次拉全档重复入库（统计虚胖）。"""
        try:
            j = self.get_json(JIN10_CDN_SHIBOR,
                              headers={'Referer': 'https://www.jin10.com/'})
        except Exception as e:
            logger.warning('SHIBOR 请求失败: %r', e)
            return []
        values = (j or {}).get('values') if isinstance(j, dict) else None
        if not values:
            return []
        want = f'SHIBOR_{tenor}' if tenor else None
        out = []
        for date_str, tenors in values.items():
            d = str(date_str).replace('-', '')
            for t, pair in (tenors or {}).items():
                code = f'SHIBOR_{t.replace("/", "")}'
                if code not in INDICATOR_META:
                    continue
                if want and code != want:
                    continue
                v = _num((pair or [None])[0] if isinstance(pair, list) else pair)
                if v is not None:
                    out.append({'indicator_code': code, 'trade_date': d, 'value': v})
        return out

    def fetch_cpi(self) -> List[Dict[str, Any]]:
        """东财 CPI（RPT_ECONOMY_CPI；NATIONAL_SAME=全国当月同比，全量历史）"""
        rows = self._em_fetch('RPT_ECONOMY_CPI')
        out = []
        for r in rows:
            d = str(r.get('REPORT_DATE') or '')[:10].replace('-', '')
            v = _num(r.get('NATIONAL_SAME'))
            if d and v is not None:
                out.append({'indicator_code': 'CPI_YOY', 'trade_date': d,
                            'value': v})
        return out

    def fetch_jin10_indicator(self, code: str) -> List[Dict[str, Any]]:
        """金十 reports/list_v2（CPI/PPI 年率；values: [[日期,今值,预测,前值]]）"""
        attr_id = JIN10_ATTR.get(code)
        if attr_id is None:
            return []
        try:
            j = self.get_json(JIN10_REPORTS_URL,
                              params={'max_date': '', 'category': 'ec',
                                      'attr_id': str(attr_id), '_': str(_now_ms())},
                              headers=dict(_JIN10_HEADERS,
                                           Referer='https://datacenter.jin10.com/'))
        except Exception as e:
            logger.warning('金十 %s 请求失败: %r', code, e)
            return []
        out: List[Dict[str, Any]] = []
        max_date = ''
        # 金十 cursor 分页：max_date = 上一页最早日期 - 1 天，直到空页
        for _ in range(50):
            try:
                j = self.get_json(JIN10_REPORTS_URL,
                                  params={'max_date': max_date, 'category': 'ec',
                                          'attr_id': str(attr_id),
                                          '_': str(_now_ms())},
                                  headers=dict(_JIN10_HEADERS,
                                               Referer='https://datacenter.jin10.com/'))
            except Exception as e:
                logger.warning('金十 %s 分页请求失败: %r', code, e)
                break
            data = (j or {}).get('data') if isinstance(j, dict) else None
            rows = (data or {}).get('values') if data else None
            if not rows:
                break
            for row in rows:
                if not row or len(row) < 2:
                    continue
                d = str(row[0])[:10].replace('-', '')
                v = _num(row[1])
                if d and v is not None:
                    out.append({'indicator_code': code, 'trade_date': d,
                                'value': v})
            # 下一页 cursor：本页最早日期退一日
            dates = sorted(str(r[0])[:10] for r in rows if r and r[0])
            if not dates:
                break
            from datetime import datetime as _dt, timedelta as _td
            try:
                nxt = (_dt.strptime(dates[0], '%Y-%m-%d') - _td(days=1)
                       ).strftime('%Y-%m-%d')
            except ValueError:
                break
            if nxt == max_date:
                break
            max_date = nxt
        return out

    # ---------- 汇总入口 ----------

    def fetch_indicator(self, code: str) -> List[Dict[str, Any]]:
        """单指标全量历史（管理器的拉取单元；失败/无数据 → []，两态由上层判定）"""
        if code.startswith('LPR_'):
            return self.fetch_lpr()
        if code.startswith('SHIBOR_'):
            return self.fetch_shibor(tenor=code[len('SHIBOR_'):])
        if code in ('PMI_MFG', 'PMI_NONMFG'):
            return self.fetch_pmi()
        if code == 'M1_YOY':
            return self.fetch_money_supply()
        if code == 'M2_YOY':
            return self.fetch_jin10_indicator(code)
        if code == 'SOCIAL_FINANCING':
            return self.fetch_social_financing()
        if code == 'CPI_YOY':
            return self.fetch_cpi()
        if code in JIN10_ATTR:
            return self.fetch_jin10_indicator(code)
        logger.warning('未知指标 code: %s', code)
        return []


def _num(value: Any) -> Optional[float]:
    """数值清洗：'1.2612' → 1.2612；None/'--'/'' → None；禁字符串硬转。"""
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


def _now_ms() -> int:
    return int(time.time() * 1000)
