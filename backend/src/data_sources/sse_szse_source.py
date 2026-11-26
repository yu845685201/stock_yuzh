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
- 龙虎榜汇总: queryAllTradeOpenDate.do?Token=QUERY&tradeDate=YYYYMMDD&flag=1
  （2026-10-02 实测切换：1 请求返回当日全部上榜行（个股×原因），旧 queryTradeOpenInfo.do
    + refType 空传只返回 refType=1 一个分类（X-1 漏采 96%，已废弃删除）；
    行字段: secCode/secAbbr/refType/secTxAmount 当日总成交额/
    branchNameB/branchTxAmtB/branchNameS/branchTxAmtS（买卖各 5 席位逗号打包，
    T12 实测一一对应；bsType 汇总行恒空））
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

# 上交所 commonSoaQuery/queryAllTradeOpenDate 返回 JSONP（jsonpCallback({...})），需剥壳
_JSONP_PATTERN = re.compile(r'^[\w$.]+\((.*)\)\s*;?\s*$', re.S)

SZSE_SHOWREPORT = 'http://www.szse.cn/api/report/ShowReport/data'
SZSE_SHOWREPORT_EXPORT = 'http://www.szse.cn/api/report/ShowReport'
SZSE_REFERER = 'http://www.szse.cn/'
SSE_QUERY = 'http://query.sse.com.cn/commonSoaQuery.do'
# 两融明细新接口（盘后采集数据源整合详细技术方案 V1.0 §4.1：tabType=mxtype +
# detailsDate，每页 pageNo=beginPage=endPage=n 全部带 pageHelp. 前缀——只改 pageNo
# 会取回第一页，2026-11-20 实测 2,000+19 两页）
SSE_MARGIN_URL = 'http://query.sse.com.cn/marketdata/tradedata/queryMargin.do'
SSE_MARGIN_REFERER = 'http://www.sse.com.cn/market/othersdata/margin/detail/'
SSE_LHB_SUMMARY_URL = 'http://query.sse.com.cn/marketdata/tradedata/queryAllTradeOpenDate.do'
SSE_LHB_REFERER = 'http://www.sse.com.cn/disclosure/diclosure/public/dailydata/'
# 科创板龙虎榜独立接口（§5.1：使用日期选择器的 tradeDate=D + 空 flag；区间成交额
# 与席位额均为万元；refType 按科创板字典解释）
SSE_KCB_LHB_URL = 'http://query.sse.com.cn/marketdata/tradedata/queryKCBTradeInfo.do'
SSE_KCB_LHB_REFERER = 'http://www.sse.com.cn/disclosure/diclosure/public/dailydatatib/'
SSE_REFERER = 'http://www.sse.com.cn/'

# 上交所龙虎榜 refType -> 披露原因文本（第七轮 T21：全量提取自官网
# search_diclosurePublic_2022.js 的 disclosureTypeByGgOption，共 19 项。
# 注意官方文案 11/12 为"当日偏离"、1/2 为"累计达 20%"；未知代码回落
# '披露原因代码{N}' 保持兼容、不丢数据）
SSE_REF_TYPE_NAMES = {
    '11': '涨幅偏离', '12': '跌幅偏离', '13': '振幅达15%', '14': '换手率达20%',
    '15': '无涨跌幅限制', '1': '涨幅偏离累计达20%', '2': '跌幅偏离累计达20%',
    '7': 'S涨幅累计偏离', '8': 'S跌幅累计偏离', '3': 'ST涨幅累计偏离',
    '4': 'ST跌幅累计偏离', '5': '日均换手率及累计换手率指标',
    '31': '异常波动停牌（A股）', '32': '异常波动停牌（B股）',
    '41': '融资买入量达50%', '42': '融券卖出量达50%',
    '37': '风险警示股票换手率达30%', '51': '退市整理的证券', '52': '实施特别停牌的证券',
}

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


# 深交所两融 XLSX 列名 → 行键映射（括号全/半角兼容；§4.1：余额/买入额/融券余额
# 列已是元，两融合计仅作校验，融资偿还额该文件无 → rzche 留空）
_SZSE_MARGIN_XLSX_COLUMNS = {
    'rzye': ('融资余额(元)',),
    'rzmre': ('融资买入额(元)',),
    'rqye': ('融券余额(元)',),
    'rzrqhj': ('两融合计(元)',),
}


# 深交所龙虎榜 XLSX 列名 → 行键映射（2026-11-20 实测表头）
_SZSE_LHB_XLSX_COLUMNS = {
    'ann_date': ('公告日期',),
    'zqdm': ('证券代码',),
    'zqjc': ('证券简称',),
    'cjje': ('成交金额(元)',),
    'sl': ('成交量(股/份)',),
    'plyy': ('披露原因',),
}


def parse_szse_lhb_xlsx(content: bytes) -> List[Dict[str, Any]]:
    """深交所龙虎榜 XLSX 字节流 → 原始行列表（模块级纯函数，离线样本可测）。

    A1:A1 维度陷阱同两融 XLSX：read_only 先 reset_dimensions 再遍历实际末行。
    证券代码保留六位前导零；成交金额/成交量千分位清洗为 Decimal（元、股/份）；
    披露原因保留原文（原始实际涨跌值，§5.1 原文随事件保存）。
    """
    from decimal import Decimal
    from io import BytesIO

    from openpyxl import load_workbook

    wb = load_workbook(BytesIO(content), read_only=True, data_only=True)
    try:
        ws = wb.worksheets[0]
        ws.reset_dimensions()
        col_of: Dict[str, int] = {}
        out: List[Dict[str, Any]] = []
        for row in ws.iter_rows(values_only=True):
            if row is None:
                continue
            cells = ['' if c is None else str(c).strip() for c in row]
            if not col_of:
                header = [_xlsx_header_key(c) for c in cells]
                if not any(h in {_xlsx_header_key(c) for c in candidates}
                           for key, candidates in _SZSE_LHB_XLSX_COLUMNS.items()
                           for h in header):
                    continue
                for key, candidates in _SZSE_LHB_XLSX_COLUMNS.items():
                    col_of[key] = next(
                        (i for i, h in enumerate(header)
                         if h in {_xlsx_header_key(c) for c in candidates}), -1)
                if col_of['zqdm'] < 0:
                    raise ValueError(f'深交所龙虎榜 XLSX 表头缺证券代码列: {cells}')
                continue
            code = cells[col_of['zqdm']] if col_of['zqdm'] < len(cells) else ''
            code = code.replace('.', '').strip()
            if not code or not code.isdigit() or len(code) != 6:
                continue
            ann = cells[col_of['ann_date']] if 0 <= col_of['ann_date'] < len(cells) else ''
            item: Dict[str, Any] = {
                'ann_date': ann.replace('-', ''),
                'zqdm': code,
                'zqjc': cells[col_of['zqjc']] if 0 <= col_of['zqjc'] < len(cells) else None,
                'plyy': cells[col_of['plyy']] if 0 <= col_of['plyy'] < len(cells) else '',
            }
            for key in ('cjje', 'sl'):
                idx = col_of.get(key, -1)
                raw = cells[idx] if 0 <= idx < len(cells) else ''
                s = _NUM_CLEAN.sub('', raw)
                item[key] = Decimal(s) if s not in ('', '--', '-') else None
            out.append(item)
        if not out:
            raise ValueError('深交所龙虎榜 XLSX 未解析到有效数据行（缺表头/截断）')
        return out
    finally:
        wb.close()


def _xlsx_header_key(name: str) -> str:
    """表头名归一：全角括号/空格 → 半角紧凑，供列名匹配"""
    return (str(name or '').strip()
            .replace('（', '(').replace('）', ')')
            .replace(' ', '').replace('\u00a0', ''))


def parse_szse_margin_xlsx(content: bytes) -> List[Dict[str, Any]]:
    """深交所两融 XLSX 字节流 → 行列表（模块级纯函数，离线样本可测）。

    解析陷阱（§4.1）：11-20 两融/龙虎榜 XLSX 工作表尺寸均声明为 ``A1:A1``——
    ``openpyxl`` read_only 模式下必须先 ``ws.reset_dimensions()`` 再遍历到实际
    末行（样本 2,120 行含表头）。证券代码保留六位（前导零），金额按表头识别、
    千分位清洗，值以 Decimal 返回（§4.1：规范化金额全程 Decimal，落 NUMERIC(24,4)）。
    """
    from decimal import Decimal
    from io import BytesIO

    from openpyxl import load_workbook

    wb = load_workbook(BytesIO(content), read_only=True, data_only=True)
    try:
        ws = wb.worksheets[0]
        ws.reset_dimensions()          # A1:A1 声明陷阱：重置后才能读到实际末行
        col_of: Dict[str, int] = {}
        out: List[Dict[str, Any]] = []
        for row in ws.iter_rows(values_only=True):
            if row is None:
                continue
            cells = ['' if c is None else str(c).strip() for c in row]
            if not col_of:
                if any('证券代码' in _xlsx_header_key(c) or c == '证券代码' for c in cells):
                    header = [_xlsx_header_key(c) for c in cells]
                    code_col = next((i for i, h in enumerate(header)
                                     if h == '证券代码'), None)
                    if code_col is None:
                        raise ValueError(f'深交所两融 XLSX 表头缺证券代码列: {cells}')
                    col_of['code'] = code_col
                    for key, candidates in _SZSE_MARGIN_XLSX_COLUMNS.items():
                        col_of[key] = next(
                            (i for i, h in enumerate(header)
                             if h in {_xlsx_header_key(c) for c in candidates}), -1)
                continue
            code = cells[col_of['code']] if col_of['code'] < len(cells) else ''
            code = code.replace('.', '').strip()
            if not code or not code.isdigit() or len(code) != 6:
                continue               # 表头重复行/说明行/脏行剔除（六位代码保留）
            item: Dict[str, Any] = {'zqdm': code}
            for key in ('rzye', 'rzmre', 'rqye', 'rzrqhj'):
                idx = col_of.get(key, -1)
                raw = cells[idx] if 0 <= idx < len(cells) else ''
                s = _NUM_CLEAN.sub('', raw)
                item[key] = Decimal(s) if s not in ('', '--', '-') else None
            if item['rzye'] is None and item['rzmre'] is None:
                continue               # 脏行剔除（与旧 JSON 口径一致）
            out.append(item)
        if not out:
            raise ValueError('深交所两融 XLSX 未解析到有效数据行（缺表头/维度未重置/截断）')
        return out
    finally:
        wb.close()


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
        """上交所两融明细（§4.1：queryMargin.do，tabType=mxtype + detailsDate）。

        分页陷阱（2026-11-20 实测锁定）：第 n 页须把 ``pageHelp.pageNo/beginPage/endPage``
        **同设为 n**（均带 pageHelp. 前缀，cacheSize=1）；只改 pageNo 会重复第一页——
        本方法对"第 n>1 页返回第一页内容"显式报错拒收。逐行核 opDate（错日拒收），
        核 pageHelp.total/pageCount 闭合。
        """
        rows: List[Dict[str, Any]] = []
        page = 1
        page_size = 2000
        total = None
        page_count = None
        first_page_sig: Optional[Tuple[Any, ...]] = None
        while True:
            data = self._parse_jsonp(self.get_text(SSE_MARGIN_URL, params={
                'isPagination': 'true',
                'tabType': 'mxtype',
                'detailsDate': trade_date,
                'pageHelp.pageSize': page_size,
                'pageHelp.cacheSize': 1,
                'pageHelp.pageNo': page,
                'pageHelp.beginPage': page,
                'pageHelp.endPage': page,
                # 2026-11-26 实测：缺 jsonCallBack 参数时该端点 HTTP 400
                'jsonCallBack': 'jsonpCallback',
            }, headers={'Referer': SSE_MARGIN_REFERER}))
            page_help = data.get('pageHelp') or {}
            batch = page_help.get('data') or data.get('result') or []
            if page == 1 and not batch:
                return []          # 未发布/非交易日：与旧行为一致返回空
            if batch:
                sig = (batch[0].get('stockCode'), batch[-1].get('stockCode'),
                       len(batch))
                if page == 1:
                    first_page_sig = sig
                elif sig == first_page_sig:
                    raise ValueError(
                        f'上交所两融第 {page} 页返回第一页内容（pageNo/beginPage/endPage '
                        f'未生效或被服务端忽略），拒收整批防重复写入')
                rows.extend(batch)
            total = int(page_help.get('total') or total or 0)
            page_count = int(page_help.get('pageCount') or page_count or 0)
            if total and len(rows) >= total:
                break
            if page_count and page >= page_count:
                break
            if not batch:
                raise ValueError(
                    f'上交所两融分页中断：已取 {len(rows)}/{total or "?"} 行，'
                    f'第 {page} 页无数据（少页拒收）')
            if page > 50:
                raise ValueError(f'上交所两融达安全阀页数 {page}，疑似分页异常')
            page += 1
        if total and len(rows) != total:
            raise ValueError(f'上交所两融行数与官方总数不一致: {len(rows)} ≠ {total}')
        for r in rows:
            op = str(r.get('opDate') or '')[:10].replace('-', '')
            if op and op != trade_date:
                raise ValueError(f'上交所两融行 opDate={op} ≠ 请求日 {trade_date}（错日拒收）')
        return rows

    # ---------- 深交所 两融明细（XLSX 整表导出） ----------

    def fetch_szse_margin_xlsx(self, trade_date: str) -> List[Dict[str, Any]]:
        """深交所两融 XLSX 整表导出（§4.1：SHOWTYPE=xlsx，一份 ~145KB / 2,119 行）。

        替代旧 JSON 翻页（106 页 × pagesize=20）。文件名与生成时刻均不含业务日——
        调用方须用带日期的巨潮同日代码集验证归属后入库。无数据（非交易日/未发布）
        返回空列表；HTTP 404/结构异常向上抛。
        """
        date_param = f'{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}'
        content = self.get_content(SZSE_SHOWREPORT_EXPORT, params={
            'SHOWTYPE': 'xlsx',
            'CATALOGID': self.szse_margin_catalog,
            'TABKEY': 'tab2',
            'txtDate': date_param,
            'random': '0.123456',
        }, headers={'Referer': SZSE_REFERER})
        if not content:
            return []
        if content[:2] != b'PK':    # xlsx=ZIP 容器；HTML/JSON 拦截页在此拦下
            raise ValueError(f'深交所两融 XLSX 响应非 ZIP 容器: {content[:80]!r}')
        return parse_szse_margin_xlsx(content)

    # ---------- 上交所 龙虎榜（汇总接口：1 请求全部上榜行 + 打包席位） ----------

    def fetch_sse_lhb_summary(self, trade_date: str) -> List[Dict[str, Any]]:
        """上交所每日交易公开信息汇总（X-1 修复，2026-10-02 实测切换；tradeDate yyyyMMdd）。

        1 请求返回当日全部上榜行（个股×原因，实测 20260930 为 27 行 25 只，
        refTypes 覆盖 [1,11,12,13,14,2]）。行字段：secCode/secAbbr/refType/
        secTxAmount（个股当日总成交额）/branchNameB/branchTxAmtB/branchNameS/
        branchTxAmtS（买卖各 5 席位逗号打包，T12 实测一一对应）；bsType 汇总行恒空。
        旧接口 queryTradeOpenInfo.do + refType 空传只返回 refType=1 单分类
        （沪市每日仅 1 只、漏采约 96%），已废弃删除。
        """
        headers = {'Referer': SSE_LHB_REFERER}
        data = self._parse_jsonp(self.get_text(SSE_LHB_SUMMARY_URL, params={
            'jsonCallBack': 'jsonpCallback', 'Token': 'QUERY',
            'tradeDate': trade_date, 'flag': '1',
        }, headers=headers))
        if not isinstance(data, dict):
            raise ValueError(f'上交所龙虎榜汇总响应结构变更: {str(data)[:200]}')
        rows = data.get('result')
        if rows is None:
            rows = (data.get('pageHelp') or {}).get('data') or []
        return rows

    def fetch_sse_kcb_lhb(self, trade_date: str) -> List[Dict[str, Any]]:
        """上交所科创板龙虎榜（§5.1：queryKCBTradeInfo.do，tradeDate=D + 空 flag）。

        2026-11-20 实测 3 起（688078/688199/688679）。行字段：secCode/secAbbr/
        secType（'A'）/fileDate/tradeDate/unnormalStartDate/unnormalEndDate/
        regionTxAmt（万元，披露区间竞价成交额）/regionTxVol/refType/
        deptNameB/deptTxAmtB/deptNameS/deptTxAmtS（买卖各 5 席位逗号打包，万元）。
        返回行数需结合实际数组核对，分页壳可能给零。
        """
        data = self._parse_jsonp(self.get_text(SSE_KCB_LHB_URL, params={
            'jsonCallBack': 'jsonpCallback', 'isPagination': 'false',
            'tradeDate': trade_date, 'flag': '',
        }, headers={'Referer': SSE_KCB_LHB_REFERER}))
        if not isinstance(data, dict):
            raise ValueError(f'上交所科创板龙虎榜响应结构变更: {str(data)[:200]}')
        rows = data.get('result')
        if rows is None:
            rows = (data.get('pageHelp') or {}).get('data') or []
        for r in rows:
            td = str(r.get('tradeDate') or '')[:10].replace('-', '')
            if td and td != trade_date:
                raise ValueError(f'科创板龙虎榜行 tradeDate={td} ≠ 请求日 {trade_date}（错日拒收）')
        return rows

    def fetch_szse_lhb_xlsx(self, trade_date: str) -> List[Dict[str, Any]]:
        """深交所龙虎榜列表 XLSX 整表导出（§5.1：SHOWTYPE=xlsx，替代 JSON 翻页）。

        表头（2026-11-20 实测）：公告日期、证券代码、证券简称、成交金额(元)、
        成交量(股/份)、披露原因。同股多原因为多行；金额千分位元；公告日期带横线。
        """
        date_param = f'{trade_date[:4]}-{trade_date[4:6]}-{trade_date[6:]}'
        content = self.get_content(SZSE_SHOWREPORT_EXPORT, params={
            'SHOWTYPE': 'xlsx',
            'CATALOGID': self.szse_lhb_catalog,
            'txtStart': date_param,
            'txtEnd': date_param,
            'random': '0.123456',
        }, headers={'Referer': SZSE_REFERER})
        if not content:
            return []
        if content[:2] != b'PK':
            raise ValueError(f'深交所龙虎榜 XLSX 响应非 ZIP 容器: {content[:80]!r}')
        return parse_szse_lhb_xlsx(content)

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
