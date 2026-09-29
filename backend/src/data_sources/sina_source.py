"""
新浪财经数据源 —— 外围市场（方案 4.14，M1）

接口事实（2026-09-27 实测锁定）：
- GET https://hq.sinajs.cn/list={codes逗号拼接}，必须带 Referer https://finance.sina.com.cn/（否则 403）
- 响应 GBK 编码，形如 var hq_str_int_dji="道琼斯,46247.29,299.97,0.65";
- int_* 指数（道指/纳指/标普等）字段：[0]名称, [1]当前点位, [2]涨跌额, [3]涨跌幅(%)
- rt_hk* 港股指数（恒生等，2026-09-27 实测）：[1]中文名, [2]当前点位, [8]涨跌幅(%)
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
            elif code.startswith('rt_hk'):
                # 港股指数（rt_hkHSI 等，2026-09-27 实测）：[1]中文名 [2]点位 [8]涨跌幅%
                result[code] = {
                    'family': 'index',
                    'close': _to_float(fields[2]) if len(fields) > 2 else None,
                    'change_pct': _to_float(fields[8]) if len(fields) > 8 else None,
                    'name': fields[1] if len(fields) > 1 else (fields[0] if fields else None),
                }
            else:
                result[code] = {
                    'family': 'index',
                    'close': _to_float(fields[1]) if len(fields) > 1 else None,
                    'change_pct': _to_float(fields[3]) if len(fields) > 3 else None,
                    'name': fields[0] if fields else None,
                }
        return result


def _to_float(value: Optional[str]) -> Optional[float]:
    try:
        f = float(str(value).strip())
        return f
    except (TypeError, ValueError):
        return None
