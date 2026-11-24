"""上交所 ETF 期权数据源（第三阶段-2；query.sse.com.cn 官方接口直连采集）。

不依赖 akshare 库——接口事实经 2026-10-23 requests 实测锁定（本文件即接口
文档；如上游变更，先实测再改，禁照抄记忆）。

接口（全部 Referer: http://www.sse.com.cn/）：
- ``SSE_ZQPZ_YSP_GGQQZSXT_YSHQ_QQFXZB_DATE_L``：期权风险指标（逐合约希腊字母
  Delta/Theta/Gamma/Vega/Rho + 隐含波动率 IMPLC_VOLATLTY；trade_date 参数，
  2015-02-09 起有数；实测 20240626 → 396 行）
- ``COMMON_SSE_ZQPZ_YSP_QQ_SJTJ_MRTJ_CX``：每日统计（逐标的：合约数量/总成交额/
  总成交量/认购认沽量/PC率/未平仓合约总数及分项）
- ``SSE_ZQPZ_YSP_GGQQZSXT_XXPL_DRHY_SEARCH_L``：合约主数据（全量挂牌合约：
  SECURITY_ID/CONTRACT_SYMBOL/END_DATE/CALL_OR_PPUT/行权价等；实测 692 行）

注意（实测 2026-10-23，易错点固化）：
- 响应同时含 ``pageHelp``（分页壳，data 恒 null）与 ``result``（数据体）——
  **取 result 不取 pageHelp.data**；
- 数值字段为千分位字符串（'574,056'），解析走 _num 清洗；
- SSE 仅覆盖上交所 ETF 期权（50/300/500/科创50 等标的）；深交所 ETF 期权
  未接入（数据边界如实声明，后续按需扩 szse 源）。

安全红线（同全部外网源）：串行限速、正常 UA/Referer、不破解任何技术措施、
风控识别即退避冷却、仅自用不分发。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .http_collector_base import HttpCollectorBase, ReviewBlockedError

logger = logging.getLogger(__name__)

SSE_QUERY_URL = 'http://query.sse.com.cn/commonQuery.do'
SSE_REFERER = 'http://www.sse.com.cn/'

#: sqlId 注册表
SQL_RISK_INDICATOR = 'SSE_ZQPZ_YSP_GGQQZSXT_YSHQ_QQFXZB_DATE_L'
SQL_DAILY_STATS = 'COMMON_SSE_ZQPZ_YSP_QQ_SJTJ_MRTJ_CX'
SQL_CONTRACT_MASTER = 'SSE_ZQPZ_YSP_GGQQZSXT_XXPL_DRHY_SEARCH_L'


class SseOptionSource(HttpCollectorBase):
    """上交所 ETF 期权采集（风险指标/每日统计/合约主数据）。"""

    def __init__(self, config: Dict[str, Any]):
        review = (config or {}).get('review_sync', {}) or {}
        opt_cfg = review.get('option') or {}
        super().__init__(
            config, source_name='sse_option',
            interval_seconds=float(opt_cfg.get('interval_seconds', 1.0)),
            referer=SSE_REFERER,
            ajax=True,
            headers_override={'Host': 'query.sse.com.cn'})

    # ---------- 通用 ----------

    def _query(self, sql_id: str, *, trade_date: Optional[str] = None,
               extra: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        params: Dict[str, Any] = {'sqlId': sql_id, 'isPagination': 'false'}
        if trade_date:
            params['trade_date'] = trade_date
        if extra:
            params.update(extra)
        try:
            j = self.get_json(SSE_QUERY_URL, params=params)
        except ReviewBlockedError:
            # 明确被拒向上抛（限流方案 §1）：此前被吞成空列表，
            # manager 无法区分"风控"与"未发布"，编排器也看不到急停
            raise
        except Exception as e:
            logger.warning('SSE %s 请求失败: %r', sql_id, e)
            return []
        # 注意：数据在 result（非 pageHelp.data——后者恒 null，实测）
        result = (j or {}).get('result') if isinstance(j, dict) else None
        if not result:
            return []
        return list(result)

    # ---------- 三类数据 ----------

    def fetch_risk_indicators(self, trade_date: str) -> List[Dict[str, Any]]:
        """逐合约希腊字母 + IV（返回原始 dict 列表，字段见文件头）"""
        return self._query(SQL_RISK_INDICATOR, trade_date=trade_date)

    def fetch_daily_stats(self, trade_date: str) -> List[Dict[str, Any]]:
        """逐标的每日统计（原始 dict 列表）"""
        return self._query(SQL_DAILY_STATS, trade_date=trade_date)

    def fetch_contract_master(self) -> List[Dict[str, Any]]:
        """全量挂牌合约主数据（原始 dict 列表）"""
        return self._query(SQL_CONTRACT_MASTER)


def parse_risk_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """风险指标原始行 → option_daily 行（SECURITY_ID=合约编码）。

    主数据字段（underlying/strike/expiry）由合约主数据表合并，此处只取行情字段。
    """
    code = str(row.get('SECURITY_ID') or '').strip()
    if not code:
        return None
    d = str(row.get('TRADE_DATE') or '')[:10].replace('-', '')
    if not d:
        return None
    underlying, call_put = _parse_contract_id(row.get('CONTRACT_ID'))
    return {
        'trade_date': d,
        'contract_code': code,
        'contract_symbol': str(row.get('CONTRACT_SYMBOL') or '') or None,
        'underlying_code': underlying,
        'call_put': call_put,
        'iv': _num(row.get('IMPLC_VOLATLTY')),
        'delta': _num(row.get('DELTA_VALUE')),
        'gamma': _num(row.get('GAMMA_VALUE')),
        'theta': _num(row.get('THETA_VALUE')),
        'vega': _num(row.get('VEGA_VALUE')),
        'rho': _num(row.get('RHO_VALUE')),
    }


def parse_stats_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """每日统计原始行 → option_underlying_daily 行（数值千分位清洗）。"""
    code = str(row.get('SECURITY_CODE') or '').strip()
    if not code:
        return None
    d = str(row.get('TRADE_DATE') or '')[:10].replace('-', '')
    if not d:
        return None
    return {
        'trade_date': d,
        'underlying_code': code,
        'underlying_name': str(row.get('SECURITY_ABBR') or '') or None,
        'contract_count': _int(row.get('CONTRACT_VOLUME')),
        'total_amount': _num(row.get('TOTAL_MONEY')),
        'total_volume': _num(row.get('TOTAL_VOLUME')),
        'call_volume': _num(row.get('CALL_VOLUME')),
        'put_volume': _num(row.get('PUT_VOLUME')),
        'put_call_ratio': _num(row.get('CP_RATE')),
        'oi_total': _num(row.get('LEAVES_QTY')),
        'oi_call': _num(row.get('LEAVES_CALL_QTY')),
        'oi_put': _num(row.get('LEAVES_PUT_QTY')),
    }


def parse_contract_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """合约主数据原始行 → option_contract_master 行。

    字段：SECURITY_ID（合约编码）/ CONTRACT_SYMBOL（简称）/ END_DATE（到期日）
    / CALL_OR_PPUT（购/沽）/ EXERCISE_PRICE（行权价）/ UNDERLYING_ASSET（标的）。
    """
    code = str(row.get('SECURITY_ID') or '').strip()
    if not code:
        return None
    # CONTRACT_ID = 510050C2610M02700（标的6位+C/P+到期YYMM+M+行权价5位），
    # 主数据接口无独立 underlying 列（实测无 UNDERLYING_ASSET），以 ID 解析为准，
    # EXERCISE_PRICE/END_DATE 为权威行权价/到期日（覆盖 ID 推断）
    underlying, call_put, expiry, strike_id = _parse_contract_id(
        row.get('CONTRACT_ID'), full=True)
    name_by_id = str(row.get('SECURITYNAMEBYID') or '')
    underlying_name = name_by_id.split('(')[0] if name_by_id else None
    return {
        'contract_code': code,
        'contract_symbol': str(row.get('CONTRACT_SYMBOL') or '') or None,
        'underlying_code': underlying,
        'underlying_name': underlying_name,
        'call_put': call_put,
        'strike': _num(row.get('EXERCISE_PRICE')) or strike_id,
        'expiry_date': (str(row.get('END_DATE') or '')[:10].replace('-', '')
                        or expiry),
    }



def _parse_contract_id(contract_id: Any, full: bool = False):
    """解析 CONTRACT_ID：``510050C2610M02700`` → (标的, 购/沽[, 到期, 行权价])。

    编码规则（SSE 期权）：6 位标的代码 + C(认购)/P(认沽) + YYMM 到期月 + M +
    5 位行权价（末位为小数位，如 02700 → 2.700）。无法解析返回
    (None, None[, None, None])——禁硬造。
    """
    s = str(contract_id or '').strip()
    if len(s) < 13 or not s[:6].isdigit():
        return (None, None, None, None) if full else (None, None)
    underlying = s[:6]
    call_put = '购' if s[6:7] == 'C' else '沽' if s[6:7] == 'P' else None
    if not full:
        return underlying, call_put
    expiry = None
    strike = None
    if len(s) >= 11 and s[7:11].isdigit():
        expiry = '20' + s[7:11]
    if len(s) >= 13 and s[11:12] == 'M' and s[12:].isdigit():
        raw = s[12:]
        # 5 位行权价编码：02700 → 2.700（÷1000；末位为第三位小数）
        strike = float(raw) / 1000.0 if len(raw) == 5 else _num(raw)
    return underlying, call_put, expiry, strike


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


def _int(value: Any) -> Optional[int]:
    v = _num(value)
    return int(v) if v is not None else None
