"""
新浪财经数据源 —— 外围市场（方案 4.14，M1）

接口事实（2026-09-27 实测锁定；gb_ 分支 2026-10-04 补充）：
- GET https://hq.sinajs.cn/list={codes逗号拼接}，必须带 Referer https://finance.sina.com.cn/（否则 403）
- 响应 GBK 编码，形如 var hq_str_int_dji="道琼斯,46247.29,299.97,0.65";
- int_* 指数（道指/纳指/标普等）字段：[0]名称, [1]当前点位, [2]涨跌额, [3]涨跌幅(%)
  ⚠ int_* 三大美指接口数值已冻结（20260927~0930 四日同值，10-04 实测仍冻结），
  已弃用换 gb_ 前缀（见 config review_sync.sina_markets 注释）
- gb_ 美股个股/指数（O4 2026-10-04 实测）：[0]名称, [1]最新价, [2]涨跌幅%,
  [3]报价时间, [26]昨收——与腾讯 qt/usfqkline 双源对拍一致
- rt_hk* 港股指数（恒生等；字段布局 2026-10-04 实测修正，P0）：[0]代码, [1]中文名,
  [2]今开, [3]昨收, [4]最高, [5]最低, [6]最新/收盘, [7]涨跌额, [8]涨跌幅(%), …[17]日期
  ⚠ close 取 [6]；原取 [2]（今开）自 09-27 上线起恒生"收盘"一直为开盘价，已修复回补
- fx_* 汇率字段为另一种布局（2026-09 实测）：[1]最新价，日期由尾部 yyyy-MM-dd 扫描定位；
  涨跌幅字段暂无可靠定位 → 按数据真实性原则留空，补充代码启用前需人工核对
- 口径注意：北京时间盘后取到的美股为昨夜收盘（T-1），汇率/美元指数为最新值

安全红线：串行限速、正常 UA/Referer、不破解任何技术措施、风控识别即退避冷却、仅自用不分发。
"""

import logging
import re
from typing import Any, Dict, List, Optional

from .http_collector_base import HttpCollectorBase, ReviewBlockedError

logger = logging.getLogger(__name__)

QUOTE_URL = 'https://hq.sinajs.cn/list={codes}'
SINA_REFERER = 'https://finance.sina.com.cn/'

# fx_* 汇率日期字段：从尾部扫描 yyyy-MM-dd（fx 字段数随品种有差异，不写死下标）
_DATE_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}$')
FX_PRICE_INDEX = 1


class SinaSource(HttpCollectorBase):
    """新浪行情：外围指数与汇率批量报价（1-2 请求/日，天然轻量）"""

    def __init__(self, config: Dict[str, Any]):
        review = (config or {}).get('review_sync', {}) or {}
        super().__init__(
            config, 'sina',
            interval_seconds=float(review.get('sina_interval', 1.0)),
            referer=SINA_REFERER,
        )

    def fetch_global_futures_daily(self, symbol: str,
                                   start: Optional[str] = None,
                                   end: Optional[str] = None) -> List[Dict[str, Any]]:
        """全球期货日K（如 CHA50CFD 富时A50）——A50 日频源（附录 C.5 接入）。

        返回 [{trade_date: YYYYMMDD, open, high, low, close, volume}]（date 升序，
        start/end 为 YYYYMMDD 闭区间过滤）。接口一次返回全量历史（2016-10 起），
        无分页。 Raises: IfzqSourceError 同族的采集基类异常（风控退避复用）。
        """
        text = self.get_text(GLOBAL_FUTURES_KLINE_URL, params={'symbol': symbol})
        return self.parse_global_futures_daily(text, start, end)

    @staticmethod
    def parse_global_futures_daily(text: str,
                                   start: Optional[str] = None,
                                   end: Optional[str] = None) -> List[Dict[str, Any]]:
        """jsonp ``var t=([{...}])`` → 行清单（静态可测；坏响应抛 ValueError）。"""
        import json as _json
        i, j = text.find('['), text.rfind(']')
        if i < 0 or j <= i:
            raise ValueError(f'全球期货日K响应无可解析数组: {text[:80]!r}')
        items = _json.loads(text[i:j + 1])
        rows: List[Dict[str, Any]] = []
        for it in items:
            d = str(it.get('date') or '').replace('-', '')
            close = _to_float(it.get('close'))
            if len(d) != 8 or close is None:
                continue
            if start and d < start:
                continue
            if end and d > end:
                continue
            rows.append({'trade_date': d,
                         'open': _to_float(it.get('open')),
                         'high': _to_float(it.get('high')),
                         'low': _to_float(it.get('low')),
                         'close': close,
                         'volume': _to_float(it.get('volume'))})
        return rows

    def fetch_markets(self, markets: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """批量抓取市场报价。

        Args:
            markets: [{'code': 'int_dji', 'name': '道琼斯'}, ...]

        Returns:
            [{'market_code', 'market_name', 'close', 'change_pct', 'quote_date'}]，
            单代码解析失败返回 None 字段并记日志（不阻塞其余代码）。
        """
        if not markets:
            return []
        codes = ','.join(m['code'] for m in markets)
        text = self.get_text(QUOTE_URL.format(codes=codes))
        try:
            text_bytes = text.encode('latin-1', errors='replace')
            text = text_bytes.decode('gbk', errors='replace')
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass  # requests 已按响应头解码成功时保持原样

        by_code = self._parse_response(text)
        rows: List[Dict[str, Any]] = []
        for m in markets:
            parsed = by_code.get(m['code'])
            row = {
                'market_code': m['code'],
                'market_name': m.get('name') or (parsed or {}).get('name'),
                'close': None,
                'change_pct': None,
                'quote_date': None,
            }
            if parsed is None:
                logger.warning(f'[sina] 代码 {m["code"]} 无报价返回')
            elif parsed.get('family') == 'fx':
                row['close'] = parsed.get('close')
                row['quote_date'] = parsed.get('date')
            elif parsed.get('family') == 'hf':
                # 外盘期货：现价入库；change_pct 留空由 manager 本地昨收自算（T20）
                row['close'] = parsed.get('close')
                row['quote_date'] = parsed.get('date')
            else:
                row['close'] = parsed.get('close')
                row['change_pct'] = parsed.get('change_pct')
            rows.append(row)
        return rows

    @staticmethod
    def _parse_response(text: str) -> Dict[str, Dict[str, Any]]:
        """解析 var hq_str_{code}="f0,f1,..."; 逐代码提取"""
        result: Dict[str, Dict[str, Any]] = {}
        for chunk in text.split(';'):
            chunk = chunk.strip()
            if not chunk or '=' not in chunk:
                continue
            var, _, body = chunk.partition('=')
            code = var.replace('var hq_str_', '').strip()
            fields = [f.strip() for f in body.strip().strip('"').split(',')]
            if not code or not fields or not any(fields):
                continue
            if code.startswith('fx_'):
                date_str = None
                for field in reversed(fields):
                    if _DATE_PATTERN.match(field):
                        date_str = field
                        break
                result[code] = {
                    'family': 'fx',
                    'close': _to_float(fields[FX_PRICE_INDEX]) if len(fields) > FX_PRICE_INDEX else None,
                    'date': date_str,
                    'name': fields[0] if fields else None,
                }
            elif code.startswith('hf_'):
                # 外盘期货（hf_，I-2；2026-10-03 实测 CHA50CFD 布局）：
                # [4]最高 [5]最低 [6]时间 [7]最新价 [9]持仓量 [12]日期 [13]中文名 [14]成交量
                # 快照无昨结/昨收可靠字段（T20 双时段采样）→ change_pct 一律留空，
                # 由 OverseasManager 用 overseas_market_daily 本地昨收自算（首条留空）
                result[code] = {
                    'family': 'hf',
                    'close': _to_float(fields[7]) if len(fields) > 7 else None,
                    'high': _to_float(fields[4]) if len(fields) > 4 else None,
                    'low': _to_float(fields[5]) if len(fields) > 5 else None,
                    'time': fields[6] if len(fields) > 6 else None,
                    'date': fields[12] if len(fields) > 12 else None,
                    'name': fields[13] if len(fields) > 13 else None,
                    'change_pct': None,
                }
            elif code.startswith('rt_hk'):
                # 港股指数（rt_hkHSI 等）：[1]中文名 [2]今开 [3]昨收 [4]最高 [5]最低
                # [6]最新/收盘 [8]涨跌幅% [17]日期——close 取 [6]，勿取 [2]（今开，
                # 前车之鉴：09-27~10-04 库内恒生"收盘"一直是开盘价）
                result[code] = {
                    'family': 'index',
                    'close': _to_float(fields[6]) if len(fields) > 6 else None,
                    'change_pct': _to_float(fields[8]) if len(fields) > 8 else None,
                    'name': fields[1] if len(fields) > 1 else (fields[0] if fields else None),
                }
            elif code.startswith('gb_'):
                # 美股个股/指数（gb_，O4 2026-10-04 实测）：[0]名称 [1]最新价 [2]涨跌幅%
                # [3]报价时间（北京时间）[26]昨收；替代冻结的 int_* 三大美指
                result[code] = {
                    'family': 'index',
                    'close': _to_float(fields[1]) if len(fields) > 1 else None,
                    'change_pct': _to_float(fields[2]) if len(fields) > 2 else None,
                    'name': fields[0] if fields else None,
                }
            else:
                result[code] = {
                    'family': 'index',
                    'close': _to_float(fields[1]) if len(fields) > 1 else None,
                    'change_pct': _to_float(fields[3]) if len(fields) > 3 else None,
                    'name': fields[0] if fields else None,
                }
        return result


#: 全球期货日K（A50 接入，P1-5 采集侧收口/附录 C.5：jsonp 全量历史，date 升序）
GLOBAL_FUTURES_KLINE_URL = (
    'https://stock.finance.sina.com.cn/futures/api/jsonp.php/var%20t=/'
    'GlobalFuturesService.getGlobalFuturesDailyKLine')


def _to_float(value: Optional[str]) -> Optional[float]:
    try:
        f = float(str(value).strip())
        return f
    except (TypeError, ValueError):
        return None
