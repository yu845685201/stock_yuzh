"""
沪深交易所官网数据源 —— 披露日历 / 龙虎榜 / 两融（方案 4.4/4.9/4.11）

接口事实（2026-09-27 经官网页面实抓锁定；官网无契约保证，结构变更时报错必须带
原始响应片段，便于快速定位改版）：

深交所（ShowReport/data，GET，Referer 必须为 www.szse.cn 相关页面）：
- 两融明细: CATALOGID=1837_xxpl&TABKEY=tab2&txtDate=YYYY-MM-DD&PAGENO=N
  （pagesize=20 固定；列: zqdm/zqjc/jrrzmr 融资买入额亿元/jrrzye 融资余额亿元/
    jrrjmc 融券卖出量万股/jrrjyl 融券余量万股/jrrjye 融券余额万元/jrrzrjye）
- 龙虎榜列表: CATALOGID=1842_xxpl_after&txtStart=YYYY-MM-DD&txtEnd=YYYY-MM-DD&PAGENO=N
  （列: dqrq 公告日期/zqdm/zqjc/cjje 成交金额亿元/plyy 披露原因/bz 含明细入口）
- 龙虎榜席位明细: CATALOGID=1842_detal&TABKEY=tab2&DQRQ=YYYY-MM-DD&ZQDM=&ZBDM=
  （列: mmlb 买1..卖5/zsmc 营业部/mrje 买入金额元/mcje 卖出金额元）
- 预约披露: CATALOGID 官网改版后未定位到 → 配置化，探路结论"待核实"（本项失败标待补，
  实际披露日由巨潮公告 pubDate 回填，方案 4.3 COALESCE 规则不受影响）

上交所（query.sse.com.cn，Referer 必须 www.sse.com.cn 相关页面，日期参数 yyyyMMdd 无横杠）：
- 两融明细: commonSoaQuery.do?sqlId=RZRQ_MX_INFO&preStockCode=&beginDate=YYYYMMDD&endDate=YYYYMMDD
  （全市场单日 ~2000 条；列: stockCode/securityAbbr/rzye 融资余额元/rzmre 融资买入额/
    rzche 融资偿还额/rqyl 融券余量/无融券余额金额列 → 留空）
- 龙虎榜(含席位): marketdata/tradedata/queryTradeOpenInfo.do?tradeDate=YYYYMMDD&secCode=&refType=
  （行按 个股×原因×席位 展开：bsType B/S、branchRank、branchName、branchTxAmt；
    refType 为披露原因代码）
- 预约披露: commonSoaQuery.do?sqlId=SSE_SZSGG_DQBGYYQK_CAST_NEW&bulletintype=L011~L014&publishYear=YYYY
  （L011 年报/L012 半年报/L013 一季报/L014 三季报；列: companyCode/companyAbbr/
    publishDate0 预约日/actualDate 实际披露日）

安全红线：串行限速、正常 UA/Referer、不破解任何技术措施、风控识别即退避冷却、仅自用不分发。
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from .http_collector_base import HttpCollectorBase, ReviewBlockedError

logger = logging.getLogger(__name__)

# 上交所 commonSoaQuery/queryTradeOpenInfo 返回 JSONP（jsonpCallback({...})），需剥壳
_JSONP_PATTERN = re.compile(r'^[\w$.]+\((.*)\)\s*;?\s*$', re.S)

SZSE_SHOWREPORT = 'http://www.szse.cn/api/report/ShowReport/data'
SZSE_REFERER = 'http://www.szse.cn/'
SSE_QUERY = 'http://query.sse.com.cn/commonSoaQuery.do'
SSE_LHB_URL = 'http://query.sse.com.cn/marketdata/tradedata/queryTradeOpenInfo.do'
SSE_REFERER = 'http://www.sse.com.cn/'

# 上交所定期报告预约 bulletintype -> 报告期 (yyyy, MMDD)
SSE_BULLETIN_PERIOD = {
    'L011': ('1231', '年报'),
    'L012': ('0630', '半年报'),
    'L013': ('0331', '一季报'),
    'L014': ('0930', '三季报'),
}

_NUM_CLEAN = re.compile(r'[,，\s]')


def _num(value: Any, scale: float = 1.0) -> Optional[float]:
    """'1,504.76' / '15.25' -> float×scale；无效返回 None"""
    if value is None:
        return None
    s = _NUM_CLEAN.sub('', str(value).strip().replace('万元', '').replace('亿元', ''))
    if s in ('', '--', '-'):
        return None
    try:
        return float(s) * scale
    except ValueError:
        return None


class SzseSseSource(HttpCollectorBase):
    """沪深交易所官网：披露日历（预约/实际）、龙虎榜、两融（串行限速）"""

    def __init__(self, config: Dict[str, Any]):
        review = (config or {}).get('review_sync', {}) or {}
        exchange = review.get('exchange', {}) or {}
        super().__init__(
            config, 'sse_szse',
            interval_seconds=float(review.get('sse_szse_interval', 1.5)),
            referer=SZSE_REFERER,
        )
        # 结构化默认值与官网改版应急开关（配置可覆盖，无需改代码）
        self.szse_margin_catalog = exchange.get('szse_margin_catalog', '1837_xxpl')
        self.szse_lhb_catalog = exchange.get('szse_lhb_catalog', '1842_xxpl_after')
        self.szse_lhb_detail_catalog = exchange.get('szse_lhb_detail_catalog', '1842_detal')
        self.szse_plan_catalog = exchange.get('szse_plan_catalog')  # 预约披露目录，未定位到
        self.sse_plan_sqlid = exchange.get('sse_plan_sqlid', 'SSE_SZSGG_DQBGYYQK_CAST_NEW')
        self.sse_margin_sqlid = exchange.get('sse_margin_sqlid', 'RZRQ_MX_INFO')

    # ---------- 深交所通用 ----------

    def _szse_showreport(self, catalog_id: str, params: Dict[str, Any],
                         tabkey: Optional[str] = None) -> List[Dict[str, Any]]:
        """ShowReport/data GET：返回 [{metadata, data}]，取第一个非空报表"""
        query = {'SHOWTYPE': 'JSON', 'CATALOGID': catalog_id, 'random': '0.123456'}
        if tabkey:
            query['TABKEY'] = tabkey
        query.update(params)
        headers = {'Referer': SZSE_REFERER}
        resp = self.get_json(SZSE_SHOWREPORT, params=query, headers=headers)
        if not isinstance(resp, list) or not resp:
            raise ValueError(f'深交所报表 {catalog_id} 响应结构变更: {str(resp)[:200]}')
        first = resp[0]
        if isinstance(first, dict) and first.get('error'):
            # e.g. "noalert@显示报表数据失败"（目录不存在/参数错误）
            raise ValueError(f'深交所报表 {catalog_id} 返回错误: {first["error"]}')
        return first.get('data') or []

    # ---------- 深交所 两融明细 ----------

    def fetch_szse_margin(self, trade_date: str) -> List[Dict[str, Any]]:
        """深交所两融明细（trade_date: yyyyMMdd -> txtDate yyyy-MM-dd，翻页至尽）"""
        date_param = f'{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}'
        rows: List[Dict[str, Any]] = []
        page = 1
        while True:
            data = self._szse_showreport(self.szse_margin_catalog, {
                'txtDate': date_param, 'PAGENO': page,
            }, tabkey='tab2')
            if not data:
                if page == 1:
                    logger.info(f'[sse_szse] 深交所两融 {trade_date} 无数据（非交易日或未发布）')
                break
            rows.extend(data)
            if len(data) < 20:  # pagesize 固定 20
                break
            page += 1
            if page > 400:  # 安全阀：2100+ 标的 / 20 = ~106 页，400 页足够冗余
                logger.warning(f'[sse_szse] 深交所两融 {trade_date} 达安全阀页数 {page}，停止翻页')
                break
        return rows

    # ---------- 深交所 龙虎榜 ----------

    def fetch_szse_lhb(self, trade_date: str) -> List[Dict[str, Any]]:
        """深交所龙虎榜列表（行含公告日期/代码/名称/披露原因；成交金额亿元）"""
        date_param = f'{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}'
        rows: List[Dict[str, Any]] = []
        page = 1
        while True:
            data = self._szse_showreport(self.szse_lhb_catalog, {
                'txtStart': date_param, 'txtEnd': date_param, 'PAGENO': page,
            })
            if not data:
                break
            rows.extend(data)
            if len(data) < 10:
                break
            page += 1
            if page > 100:
                logger.warning(f'[sse_szse] 深交所龙虎榜 {trade_date} 达安全阀页数，停止翻页')
                break
        return rows

    def fetch_szse_lhb_seats(self, ann_date: str, code: str, zb_dm: str) -> List[Dict[str, Any]]:
        """深交所龙虎榜席位明细（DQRQ/ZQDM/ZBDM 三元组定位，买1~卖5）"""
        date_param = f'{ann_date[:4]}-{ann_date[4:6]}-{ann_date[6:]}'
        return self._szse_showreport(self.szse_lhb_detail_catalog, {
            'TABKEY': 'tab2', 'DQRQ': date_param, 'ZQDM': code, 'ZBDM': zb_dm,
        })

    # ---------- 上交所通用 ----------

    @staticmethod
    def _parse_jsonp(text: str) -> Dict[str, Any]:
        """JSONP 剥壳（复用 spider-ths 工程模式：无 cookie 纯 JSONP 回调）；纯 JSON 直通"""
        text = text.strip()
        if text.startswith('{') or text.startswith('['):
            return json.loads(text)
        m = _JSONP_PATTERN.match(text)
        if not m:
            raise ReviewBlockedError(f'上交所响应非 JSON/JSONP: {text[:200]}')
        return json.loads(m.group(1))

    def _sse_query(self, params: Dict[str, Any]) -> Dict[str, Any]:
        headers = {'Referer': SSE_REFERER}
        data = self._parse_jsonp(self.get_text(SSE_QUERY, params=params, headers=headers))
        if not isinstance(data, dict):
            raise ValueError(f'上交所查询响应结构变更: {str(data)[:200]}')
        return data

    # ---------- 上交所 两融明细 ----------

    def fetch_sse_margin(self, trade_date: str) -> List[Dict[str, Any]]:
        """上交所两融明细（全市场单日，yyyyMMdd 日期参数；分页 pageHelp）"""
        rows: List[Dict[str, Any]] = []
        page = 1
        page_size = 200
        while True:
            data = self._sse_query({
                'isPagination': 'true',
                'pageHelp.pageSize': page_size,
                'pageHelp.pageNo': page,
                'pageHelp.beginPage': 1,
                'pageHelp.cacheSize': 1,
                'pageHelp.endPage': 5,
                'preStockCode': '',
                'beginDate': trade_date,
                'endDate': trade_date,
                'sqlId': self.sse_margin_sqlid,
            })
            page_help = data.get('pageHelp') or {}
            batch = page_help.get('data') or []
            if not batch:
                break
            rows.extend(batch)
            total = int(page_help.get('total') or 0)
            if page * page_size >= total or page > 50:
                break
            page += 1
        return rows

    # ---------- 上交所 龙虎榜（含席位） ----------

    def fetch_sse_lhb(self, trade_date: str) -> List[Dict[str, Any]]:
        """上交所每日交易公开信息（行按 个股×原因×席位 展开；tradeDate yyyyMMdd）

        返回原始行（bsType B/S、branchRank、branchName、branchTxAmt、refType）。
        """
        headers = {'Referer': 'http://www.sse.com.cn/disclosure/diclosure/public/dailydata/'}
        data = self._parse_jsonp(self.get_text(SSE_LHB_URL, params={
            'jsonCallBack': 'jsonpCallback', 'orderB': 'desc', 'orderS': 'desc',
            'Token': 'QUERY', 'tradeDate': trade_date, 'secCode': '', 'refType': '',
        }, headers=headers))
        if not isinstance(data, dict):
            raise ValueError(f'上交所龙虎榜响应结构变更: {str(data)[:200]}')
        page_help = data.get('pageHelp') or {}
        rows = page_help.get('data')
        if rows is None:
            rows = data.get('result') or []
        return rows

    # ---------- 上交所 预约披露 ----------

    def fetch_sse_plan(self, year: int, bulletin_type: str) -> List[Dict[str, Any]]:
        """上交所定期报告预约（bulletintype: L011年报/L012半年报/L013一季报/L014三季报）"""
        if bulletin_type not in SSE_BULLETIN_PERIOD:
            raise ValueError(f'未知 bulletintype: {bulletin_type}')
        rows: List[Dict[str, Any]] = []
        page = 1
        page_size = 200
        while True:
            data = self._sse_query({
                'jsonCallBack': 'jsonpCallback',
                'sqlId': self.sse_plan_sqlid,
                'isPagination': 'true',
                'pageHelp.pageSize': page_size,
                'pageHelp.pageNo': page,
                'pageHelp.beginPage': 1,
                'pageHelp.cacheSize': 1,
                'pageHelp.endPage': 5,
                'bulletintype': bulletin_type,
                'publishYear': year,
                'companyCode': '',
                'startTime': '',
                'order': 'companyCode|asc',
            })
            page_help = data.get('pageHelp') or {}
            batch = page_help.get('data') or []
            if not batch:
                break
            rows.extend(batch)
            total = int(page_help.get('total') or 0)
            if page * page_size >= total or page > 30:
                break
            page += 1
        return rows

    # ---------- 深交所 预约披露（探路结论：目录未定位，占位待核实） ----------

    def fetch_szse_plan(self, year: int, report_kind: str) -> List[Dict[str, Any]]:
        """深交所定期报告预约披露。

        探路记录（2026-09-27）：官网改版后预约披露页未再走 ShowReport 旧目录，
        现有 CATALOGID 无法定位；本方法保留配置化入口（review_sync.exchange.szse_plan_catalog），
        未配置或失败即抛 NotImplementedError 由 manager 标"待补"。
        实际披露日不受影响：由巨潮公告 pubDate 回填（方案 4.3 COALESCE 规则）。
        """
        if not self.szse_plan_catalog:
            raise NotImplementedError('深交所预约披露目录未定位（探路结论：待核实），本项标待补')
        raise NotImplementedError('深交所预约披露解析逻辑待目录核实后补充')
