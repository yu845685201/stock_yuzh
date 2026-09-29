"""
同花顺数据源 —— 个股财务指标（方案 4.3，M2 首选来源；概念 4.8 / 解禁 4.12 探路结论见下）

接口事实（2026-09-27 实测锁定）：
- GET http://basic.10jqka.com.cn/api/stock/finance/{code}_main.json
  正常 UA + Referer http://basic.10jqka.com.cn/ 即可，无 hexin-v 强制（裸 requests 可用）
- 响应顶层 {"flashData": "<JSON字符串>", "fieldflashData": ...}；
  flashData 内 keys: title / report / report_yoy / simple / year / ...
  - title[i] = [指标名, 单位, ...]，与 report[i]、report_yoy[i] 行一一对应
  - report[0] = 报告期列表（yyyy-MM-dd，最新在前，实测 122 期全历史）
  - report[i] = 格式化值（'256.96亿' / '3.32%' / '1.2400'），report_yoy[i] 为数值同比
- 指标映射：营业总收入->revenue、净利润->net_profit、基本每股收益->eps、
  净资产收益率->roe、毛利率->gross_margin、每股净资产->net_assets(×总股本)、
  每股经营现金流->operate_cashflow(×总股本)；同比优先取 report_yoy 数值行

探路结论（方案 4.8/4.12，共用结论，止损即"待补"）：
- 概念成分页 q.10jqka.com.cn/gn/detail/code/{code}/ 仅首页 20 只可取，ajax/非ajax
  翻页均不可用（401 hexin-v / 返回列表页）——残缺数据不可入库 → 概念板块标"待补"
- 解禁页 data.10jqka.com.cn/market/jjsj/ 已 404，替代路径 401 → 解禁日历标"待补"
- 以上两项 enabled 配置默认 false；报告侧对应章节标注"待补"

安全红线：串行限速、正常 UA/Referer、不破解任何技术措施（hexin-v 一律不做）、
风控识别即退避冷却、仅自用不分发。
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple

from .http_collector_base import HttpCollectorBase, ReviewBlockedError

logger = logging.getLogger(__name__)

FINANCE_URL = 'http://basic.10jqka.com.cn/api/stock/finance/{code}_main.json'
THS_REFERER = 'http://basic.10jqka.com.cn/'

# 指标名 -> (base_financial_report 列, 是否每股类需乘总股本)
METRIC_MAP = {
    '营业总收入': 'revenue',
    '净利润': 'net_profit',
    '基本每股收益': 'eps',
    '净资产收益率': 'roe',
    '毛利率': 'gross_margin',
    '每股净资产': ('net_assets', True),
    '每股经营现金流': ('operate_cashflow', True),
}
# 同比数值行映射（report_yoy 与 title 同序）
YOY_METRIC_MAP = {
    '营业总收入同比增长率': 'revenue_yoy',
    '净利润同比增长率': 'net_profit_yoy',
}

_CAPTCHA_MARKS = ('验证码', '请输入', 'captcha', 'hexin-v', '访问过于频繁')


class ThsBlockedError(ReviewBlockedError):
    """同花顺风控触发（403/验证码页/非 JSON）：急停+冷却，绝不重试轰炸"""

    cooldown_seconds = 900


class ThsSource(HttpCollectorBase):
    """同花顺：个股财务指标全历史（单股 1 请求拿全历史期数）"""

    blocked_error_class = ThsBlockedError

    def __init__(self, config: Dict[str, Any]):
        review = (config or {}).get('review_sync', {}) or {}
        super().__init__(
            config, 'ths',
            interval_seconds=float(review.get('ths_interval', 2.0)),
            referer=THS_REFERER,
            blocked_error_class=ThsBlockedError,
        )

    def _check_risk_control(self, resp) -> None:
        """验证码页特征串识别：HTTP 200 但内容为验证码/拦截页"""
        try:
            head = resp.content[:4096].decode(resp.encoding or 'utf-8', errors='replace')
        except Exception:  # pragma: no cover
            return
        for mark in _CAPTCHA_MARKS:
            if mark in head:
                raise self._make_blocked(f'命中风控特征串「{mark}」')

    def fetch_finance_main(self, code: str) -> Dict[str, Any]:
        """抓取单股主要指标全历史原始 flash 字典（结构变更抛 ValueError）"""
        data = self.get_json(FINANCE_URL.format(code=code))
        if not isinstance(data, dict) or 'flashData' not in data:
            raise ValueError(f'{code} 财务响应结构变更: {str(data)[:200]}')
        try:
            flash = json.loads(data['flashData'])
        except (TypeError, ValueError) as e:
            raise ValueError(f'{code} flashData 解析失败: {e}') from e
        if not isinstance(flash, dict) or 'title' not in flash or 'report' not in flash:
            raise ValueError(f'{code} flashData 结构变更: {list(flash.keys())[:10]}')
        return flash

    def parse_finance_flash(self, flash: Dict[str, Any], total_share: Optional[float] = None,
                            backfill_years: Optional[int] = None) -> List[Dict[str, Any]]:
        """flash 字典 -> base_financial_report 行列表（每期一行）。

        Args:
            total_share: 最新总股本（股）——每股类指标折算绝对值用，缺失则对应列留空
            backfill_years: 只保留最近 N 年报告期；None 全保留
        """
        titles = flash.get('title') or []
        report = flash.get('report') or []
        report_yoy = flash.get('report_yoy') or []
        if not report or not report[0]:
            return []

        periods = [self._norm_period(p) for p in report[0]]
        n = len(periods)

        # 行定位：指标名（title[i][0]）-> 列值/同比值
        value_rows: Dict[str, List[Any]] = {}
        yoy_rows: Dict[str, List[Any]] = {}
        for i in range(1, min(len(titles), len(report))):
            name = str(titles[i][0]).strip() if titles[i] else ''
            if name in METRIC_MAP and i < len(report):
                value_rows[name] = report[i]
            if name in YOY_METRIC_MAP and i < len(report_yoy):
                yoy_rows[name] = report_yoy[i]

        rows: List[Dict[str, Any]] = []
        min_year = None
        if backfill_years:
            import datetime as _dt
            min_year = _dt.date.today().year - int(backfill_years)
        # 全列初始化（落库模板要求键齐全，缺失列保持 None 不硬造）
        row_template = {
            'revenue': None, 'revenue_yoy': None, 'net_profit': None, 'net_profit_yoy': None,
            'roe': None, 'gross_margin': None, 'operate_cashflow': None, 'eps': None,
            'net_assets': None, 'disclosure_date': None,
        }
        for j, period in enumerate(periods):
            if period is None:
                continue
            if min_year and int(period[:4]) < min_year:
                continue
            row: Dict[str, Any] = {'ts_code': '', 'stat_date': period, **row_template}
            for metric_name, col in METRIC_MAP.items():
                col_name, per_share = (col if isinstance(col, tuple) else (col, False))
                raw = value_rows.get(metric_name, [None] * (j + 1))[j] if j < len(value_rows.get(metric_name, [])) else None
                val = parse_ths_number(raw)
                if val is not None and per_share and total_share:
                    val = val * total_share
                elif val is not None and per_share:
                    val = None  # 每股类无总股本不硬造绝对值
                row[col_name] = val
            for metric_name, col in YOY_METRIC_MAP.items():
                yoy_list = yoy_rows.get(metric_name) or []
                val = yoy_list[j] if j < len(yoy_list) else None
                row[col] = float(val) if isinstance(val, (int, float)) else None
            rows.append(row)
        return rows

    @staticmethod
    def _norm_period(value: Any) -> Optional[str]:
        s = str(value or '').strip()
        m = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', s)
        if m:
            return f'{m.group(1)}{m.group(2)}{m.group(3)}'
        return None


_CN_NUM_UNITS = {'万亿': 1e12, '亿': 1e8, '万': 1e4}


def parse_ths_number(raw: Any) -> Optional[float]:
    """'256.96亿' / '3.32%' / '1.2400' / '-' -> float；无法解析返回 None"""
    if raw is None:
        return None
    s = str(raw).strip().replace(',', '')
    if s in ('', '--', '-', '—'):
        return None
    s = s.rstrip('%')
    for unit, scale in _CN_NUM_UNITS.items():
        if s.endswith(unit):
            try:
                return float(s[: -len(unit)]) * scale
            except ValueError:
                return None
    try:
        return float(s)
    except ValueError:
        return None
