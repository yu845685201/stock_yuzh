"""
腾讯 ifzq 日线数据源 —— 港美股指数与港股个股日K（技术方案 V1.0 20261004 §3）

接口事实（2026-10-04 实测锁定）：
- 港股/指数（hkHSI/hkHSCEI/hkHSTECH/hk00700 等）：
  GET https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?param=<code>,day[,<start>,<end>],<count>,qfq
  start/end 双边界生效，count 在窗口内从 end 截断（区间 2019-01-01~2019-06-30 + count=800
  返回全窗口 119 根；count=60 返回窗口内末 60 根——实测）
- 美股（usDJI/usIXIC/usINX）必须走 usfqkline/get 同形端点（fqkline/get 对美股代码损坏：
  尾部请求恒回 1 根、日期区间回 0 根——实测）；usfqkline 语义：end 生效、**start 被忽略**、
  count=自 end 向后取的根数（向后分页）
- 响应：{code:0, data:{<code>:{day:[[...],...], qt:{<code>:[...]}}}}
  day 行布局 **[date, open, close, high, low, volume]**（close 在第 2 位，勿与 open 颠倒——
  sina rt_hkHSI 位次坑的前车之鉴，有真实样本回归测试固化）；
  港股个股行可带第 7 元素（公司行动字典，可能为空/缺省/键不定），解析防御式跳过；
  指数行无第 7 元素；无数据的窗口返回 day: []（如 hkHSTECH 2020-07-27 发布前）
- qt 实时块：[3]现值，[30]时间（yyyy-mm-dd 或 yyyy/mm/dd 开头）；day 末行 close 与
  qt[3] 采集时点自洽（自洽校验输入），日期不一致（跨日取数）时校验跳过
- 日期区间形式 param=hk00700,day,2016-01-01,2016-12-31,640,qfq 实测通过（qfq 历史可回溯）

安全红线（同 sina_source）：串行限速、正常 UA/Referer、不破解任何技术措施、
风控识别即退避冷却、仅自用不分发。ifzq 无官方文档，字段布局属实测记录，
**每次解析变更前先 curl 实测核对**。
"""

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from .http_collector_base import HttpCollectorBase

logger = logging.getLogger(__name__)

IFZQ_KLINE_URL = 'https://web.ifzq.gtimg.cn/appstock/app/fqkline/get'
IFZQ_US_KLINE_URL = 'https://web.ifzq.gtimg.cn/appstock/app/usfqkline/get'
IFZQ_REFERER = 'https://gu.qq.com/'

# day 行日期列（yyyy-mm-dd）；qt 块时间列前缀（yyyy-mm-dd / yyyy/mm/dd 两种实测形态）
_DAY_DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')
_QT_DATE_PATTERN = re.compile(r'^(\d{4})[/\-](\d{2})[/\-](\d{2})')

# day 末行 close 与 qt[3] 现值同源自洽，容差从紧（0.2%，覆盖盘中两快照间微小漂移）
_SELF_CHECK_TOL = 0.002


class IfzqSourceError(RuntimeError):
    """ifzq 源结构变更/响应异常（区别于风控急停 ReviewBlockedError，不触发冷却）"""


class IfzqSource(HttpCollectorBase):
    """腾讯 ifzq 日线：港美股指数/港股个股 OHLCV（单接口族，1 请求/代码/窗口）"""

    def __init__(self, config: Dict[str, Any]):
        review = (config or {}).get('review_sync', {}) or {}
        super().__init__(
            config, 'ifzq',
            interval_seconds=float(review.get('overseas_kline_interval', 1.5)),
            referer=IFZQ_REFERER,
        )

    def fetch_daily(self, code: str, start: Optional[str] = None,
                    end: Optional[str] = None, count: int = 20) -> Dict[str, Any]:
        """拉取某代码日线窗口。

        Args:
            code: ifzq 代码（hkHSI / hk00700 / usDJI …）
            start/end: 日期区间 yyyy-mm-dd（缺省尾部窗口）
            count: 尾部根数上限（日期区间模式下为窗口内根数上限，须 ≥ 窗口交易日数；
                   美股代码下为自 end 向后取的根数，start 不生效——调用方需自行过滤）

        Returns:
            {'code', 'rows', 'skipped_rows', 'qt_close', 'qt_date'}；
            rows 为升序 [{'market_code','trade_date'(yyyyMMdd),'open','high','low',
            'close','volume'}]，解析失败行跳过计入 skipped_rows（数据真实性：不硬造）。
        Raises:
            IfzqSourceError: code!=0 / data 节点缺失（结构变更）
        """
        url = IFZQ_US_KLINE_URL if self.is_us_code(code) else IFZQ_KLINE_URL
        param = f'{code},day,{start or ""},{end or ""},{int(count)},qfq'
        payload = self.get_json(url, params={'param': param})
        return self._parse_payload(code, payload)

    @staticmethod
    def is_us_code(code: str) -> bool:
        """美股代码（us 前缀）须走 usfqkline/get 端点（fqkline 对美股损坏，实测）"""
        return str(code).lower().startswith('us')

    @classmethod
    def _parse_payload(cls, code: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(payload, dict) or payload.get('code') != 0:
            raise IfzqSourceError(
                f'[ifzq] {code} 响应 code 非 0（结构变更/风控）: {str(payload)[:200]}')
        data = payload.get('data') or {}
        node = data.get(code)
        if not isinstance(node, dict):
            raise IfzqSourceError(
                f'[ifzq] {code} data 节点缺失（结构变更）: {str(payload)[:200]}')
        raw_day = node.get('day')
        if raw_day is None:
            # 无 day 键且无 qt：节点整体异常；有 qt 无 day 视为空窗口（HSTECH 发布前实测）
            if not node.get('qt'):
                raise IfzqSourceError(
                    f'[ifzq] {code} 缺 day 与 qt 节点（结构变更）: {str(node)[:200]}')
            raw_day = []
        rows: List[Dict[str, Any]] = []
        skipped = 0
        for item in raw_day:
            row = cls._parse_day_row(code, item)
            if row is None:
                skipped += 1
                continue
            rows.append(row)
        qt_close, qt_date = cls._parse_qt(node.get('qt', {}).get(code))
        return {'code': code, 'rows': rows, 'skipped_rows': skipped,
                'qt_close': qt_close, 'qt_date': qt_date}

    @staticmethod
    def _parse_day_row(code: str, item: Any) -> Optional[Dict[str, Any]]:
        """day 行解析：[date, open, close, high, low, volume, (第7元素?)]

        close 取下标 2（open 在下标 1，勿颠倒）；第 7 元素（公司行动字典）防御式忽略；
        日期非法/价格不可解析的行返回 None 由调用方跳过并计数。
        """
        if not isinstance(item, (list, tuple)) or len(item) < 6:
            logger.warning(f'[ifzq] {code} day 行元素不足 6，跳过: {str(item)[:80]}')
            return None
        raw_date = str(item[0]).strip()
        if not _DAY_DATE_PATTERN.match(raw_date):
            logger.warning(f'[ifzq] {code} day 行日期非法，跳过: {raw_date!r}')
            return None
        values = {}
        for key, idx in (('open', 1), ('close', 2), ('high', 3), ('low', 4), ('volume', 5)):
            try:
                v = float(str(item[idx]).strip())
            except (TypeError, ValueError):
                v = None
            values[key] = v
        if values['open'] is None or values['close'] is None \
                or values['high'] is None or values['low'] is None:
            logger.warning(f'[ifzq] {code} {raw_date} 价格列不可解析，跳过: {str(item)[:80]}')
            return None
        return {
            'market_code': code,
            'trade_date': raw_date.replace('-', ''),
            'open': values['open'], 'high': values['high'],
            'low': values['low'], 'close': values['close'],
            'volume': values['volume'],
            # 指数无复权概念标 raw；个股 qfq 滚动口径（调用方按代码类型覆写）
            'adj_type': 'raw', 'source': 'ifzq',
        }

    @staticmethod
    def _parse_qt(qt: Any) -> Tuple[Optional[float], Optional[str]]:
        """qt 实时块：[3]现值，[30]时间（找不到时间字段时 qt_date=None → 自洽校验跳过）"""
        if not isinstance(qt, (list, tuple)) or len(qt) <= 3:
            return None, None
        qt_close = None
        try:
            qt_close = float(str(qt[3]).strip())
        except (TypeError, ValueError, IndexError):
            pass
        qt_date = None
        for field in qt:
            m = _QT_DATE_PATTERN.match(str(field).strip())
            if m:
                qt_date = f'{m.group(1)}{m.group(2)}{m.group(3)}'
                break
        return qt_close, qt_date

    @classmethod
    def self_check(cls, parsed: Dict[str, Any]) -> Optional[str]:
        """采集时点自洽：day 末行 close vs qt 实时 close（腾讯内部自证）。

        qt 日期缺省或与 day 末行不一致（跨日取数，qt 已是新交易日/盘前态）→ 跳过返回 None；
        日期一致但偏差超容差 → 返回告警文案（调用方告警不入库，方案 §6.1）。
        """
        rows = parsed.get('rows') or []
        qt_close, qt_date = parsed.get('qt_close'), parsed.get('qt_date')
        if not rows or qt_close is None:
            return None
        last = rows[-1]
        if qt_date is not None and qt_date != last['trade_date']:
            return None
        last_close = last['close']
        if last_close and abs(qt_close - last_close) / last_close > _SELF_CHECK_TOL:
            return (f"{last['market_code']} 自洽校验失败：day 末行 {last['trade_date']} "
                    f"close={last_close} vs qt 现值={qt_close}（超容差 "
                    f"{_SELF_CHECK_TOL:.1%}），疑似源数据异常")
        return None
