"""
K线 normalize 工具族单一来源（R-07d 下沉：sync_manager 与 kline_1min/domain 双份实现收敛于此）

函数体逐字取自 sync_manager.py 现行版本（日K主链路口径）；与 kline_1min/domain.py 的漂移点
以本模块（sync_manager 版）为准，漂移明细见 R-07d 提交说明。
"""

from bisect import bisect_right
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

from .data_transformer import DataTransformer


def normalize_date_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime('%Y%m%d')
    if isinstance(value, date):
        return value.strftime('%Y%m%d')

    s = str(value).strip()
    if len(s) >= 8 and s[0:8].isdigit():
        if '-' in s and len(s) >= 10:
            return s[0:10].replace('-', '')
        return s[0:8]
    if '-' in s and len(s) >= 10:
        return s[0:10].replace('-', '')
    return None


def normalize_time_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if ':' in s:
        parts = s.split(':')
        if len(parts) >= 2:
            hh = parts[0].zfill(2)
            mm = parts[1].zfill(2)
            return f"{hh}{mm}"
    if s.isdigit():
        if len(s) == 4:
            return s
        if len(s) == 3:
            return s.zfill(4)
        if len(s) >= 5:
            return s[0:4]
    return None


def split_datetime(value: Any) -> Tuple[Optional[str], Optional[str]]:
    s = str(value).strip()
    try:
        dt = datetime.fromisoformat(s)
        return dt.strftime('%Y%m%d'), dt.strftime('%H%M')
    except Exception:
        pass
    if len(s) >= 12 and s[0:12].isdigit():
        return s[0:8], s[8:12]
    if ' ' in s:
        date_part, time_part = s.split(' ', 1)
        date_str = normalize_date_str(date_part)
        time_str = normalize_time_str(time_part)
        return date_str, time_str
    return None, None


def to_float(value: Any) -> Optional[float]:
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def scale_price(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return value / 1000.0


def scale_amount(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return value / 1000.0


def scale_volume(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return value * 100.0


def get_kline_limit_rate(stock_code: Optional[str], stock_name: Optional[str]) -> float:
    if stock_name and DataTransformer.check_is_st(stock_name):
        return 5.1
    if stock_code:
        # 创业板（300/301开头）和科创板（68开头）涨跌幅20.1%
        if stock_code.startswith(('300', '301')):
            return 20.1
        if stock_code.startswith('68'):
            return 20.1
    return 10.1


def build_fundamentals_records_map(data: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """由已取数的基本面记录构建时点匹配索引（原 SyncManager._build_fundamentals_map 构建段逐字移动）"""
    fundamentals_map: Dict[str, Dict[str, Any]] = {}
    for item in data:
        ts_code = item.get('ts_code')
        if not ts_code:
            continue
        disclosure_date = normalize_date_str(item.get('disclosure_date'))
        if not disclosure_date:
            # 无真实披露日的记录不参与时点匹配（时点不明宁缺毋滥）
            continue
        record = {
            'disclosure_date': disclosure_date,
            'stat_date': normalize_date_str(item.get('stat_date')),
            'total_share': item.get('total_share'),
            'float_share': item.get('float_share')
        }
        entry = fundamentals_map.setdefault(ts_code, {'dates': [], 'records': []})
        entry['dates'].append(disclosure_date)
        entry['records'].append(record)

    for ts_code, entry in fundamentals_map.items():
        # 排序键 (disclosure_date, stat_date)：同日披露年报+一季报时，
        # bisect 取报告期更新的一份（股本更新）
        combined = sorted(zip(entry['dates'], entry['records']),
                          key=lambda x: (x[0], x[1].get('stat_date') or ''))
        entry['dates'] = [x[0] for x in combined]
        entry['records'] = [x[1] for x in combined]

    return fundamentals_map


def match_fundamentals(
    ts_code: str,
    trade_date: str,
    fundamentals_map: Dict[str, Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    if not trade_date:
        return None
    entry = fundamentals_map.get(ts_code)
    if not entry:
        return None
    dates = entry.get('dates', [])
    if not dates:
        return None
    idx = bisect_right(dates, trade_date)
    if idx <= 0:
        return None
    return entry['records'][idx - 1]
