"""
数据新鲜度巡检（F1，数据采集链路风险修复方案 V1.0）

对全部日频/周频数据表做 max(日期列) 巡检：与"最近已收盘交易日"基准日（P-11 口径，
review_common.latest_closed_trade_date）比对，超限亮红灯。目的：让不在盘后编排内的
源（macro/option/north_flow 等）断更时也能被发现——2026-11-18 实测 macro 静默停更
26 天、option 停 2 个交易日、north_flow 源死 2 年均无机制感知。

规则来源：review_sync.freshness_rules（config 可覆盖）；缺省用 DEFAULT_RULES。
专项规则：base_trade_calendar 的 max(calendar_date) 须 ≥ 今天+30 自然日（服务 2027
跨年保障 F7），不足即红灯。

豁免：exempt=true 的表只灰显不计红灯（north_flow_detail_daily 源侧 2024-08 起停披露，
hkex_connect_manager.py 已注明）。

用法：
    manager = FreshnessCheckManager(cm); result = manager.execute()
    # result: {'success': 无红灯, 'red': [...], 'yellow': [...], 'rows': [...],
    #          'report_path': ..., 'base_date': 'YYYYMMDD'}
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from ..config.config_manager import ConfigManager
from .review_common import latest_closed_trade_date, write_review_report

logger = logging.getLogger(__name__)

# 缺省巡检规则：table / date_column / freq / max_lag_days / label / exempt
# lag 计算：基准日（最近已收盘交易日）- 表内 MAX(date_column)，单位自然日
DEFAULT_RULES: List[Dict[str, Any]] = [
    {'table': 'his_kline_day', 'date_column': 'trade_date', 'freq': 'daily', 'max_lag_days': 3, 'label': '全市场日K'},
    {'table': 'trade_index_info', 'date_column': 'trade_date', 'freq': 'daily', 'max_lag_days': 3, 'label': '指数日K'},
    {'table': 'money_flow_daily', 'date_column': 'trade_date', 'freq': 'daily', 'max_lag_days': 3, 'label': '分笔资金流'},
    {'table': 'lhb_stock_daily', 'date_column': 'trade_date', 'freq': 'daily', 'max_lag_days': 3, 'label': '龙虎榜'},
    {'table': 'margin_trade_daily', 'date_column': 'trade_date', 'freq': 'daily', 'max_lag_days': 3, 'label': '两融'},
    {'table': 'hkex_connect_daily', 'date_column': 'trade_date', 'freq': 'daily', 'max_lag_days': 3, 'label': '互联互通'},
    {'table': 'news_telegraph', 'date_column': 'trade_date', 'freq': 'daily', 'max_lag_days': 3, 'label': '市场要闻'},
    {'table': 'sentiment_metrics_daily', 'date_column': 'trade_date', 'freq': 'daily', 'max_lag_days': 3, 'label': '情绪指标'},
    {'table': 'stock_valuation_daily', 'date_column': 'trade_date', 'freq': 'daily', 'max_lag_days': 3, 'label': 'PE/PB自算'},
    {'table': 'limit_seal_stats_daily', 'date_column': 'trade_date', 'freq': 'daily', 'max_lag_days': 3, 'label': '封板统计'},
    {'table': 'etf_scale_daily', 'date_column': 'trade_date', 'freq': 'daily', 'max_lag_days': 3, 'label': 'ETF份额'},
    {'table': 'announcement_daily', 'date_column': 'ann_date', 'freq': 'daily', 'max_lag_days': 3, 'label': '公告清单'},
    {'table': 'index_valuation_daily', 'date_column': 'trade_date', 'freq': 'daily', 'max_lag_days': 3, 'label': '指数估值'},
    {'table': 'option_daily', 'date_column': 'trade_date', 'freq': 'daily', 'max_lag_days': 3, 'label': 'ETF期权'},
    {'table': 'macro_indicator_daily', 'date_column': 'trade_date', 'freq': 'daily', 'max_lag_days': 3, 'label': '宏观指标'},
    {'table': 'overseas_market_daily', 'date_column': 'trade_date', 'freq': 'daily', 'max_lag_days': 5, 'label': '外围市场'},
    {'table': 'overseas_kline_day', 'date_column': 'trade_date', 'freq': 'daily', 'max_lag_days': 5, 'label': '外围日K'},
    {'table': 'north_flow_detail_daily', 'date_column': 'trade_date', 'freq': 'daily', 'max_lag_days': 3,
     'label': '北向个股明细', 'exempt': True},  # 源侧 2024-08-19 起停披露（hkex_connect_manager 已注明），灰显不计红
]
# 专项规则：交易日历覆盖度（max(calendar_date) ≥ 今天+30 自然日，服务 F7 跨年保障）
CALENDAR_MIN_AHEAD_DAYS = 30


def _to_date(value: Any) -> Optional[date]:
    """表内日期值归一为 date：兼容 'YYYYMMDD' / 'YYYY-MM-DD' / datetime / date / Decimal-like"""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    s = str(value).strip()
    if len(s) >= 8 and s[:8].isdigit():
        if len(s) >= 10 and '-' in s:
            s = s[:10].replace('-', '')
        try:
            return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))
        except ValueError:
            return None
    if len(s) >= 10 and '-' in s:
        try:
            return date(int(s[0:4]), int(s[5:7]), int(s[8:10]))
        except ValueError:
            return None
    return None


class FreshnessCheckManager:
    """数据新鲜度巡检（只读，不写任何业务表）"""

    def __init__(self, config_manager: Optional[ConfigManager] = None):
        self.config_manager = config_manager or ConfigManager()
        self.logger = logging.getLogger(__name__)

    def _load_rules(self) -> List[Dict[str, Any]]:
        rules = self.config_manager.get('review_sync.freshness_rules', None)
        if isinstance(rules, list) and rules:
            return rules
        return DEFAULT_RULES

    def execute(self, dry_run: bool = False) -> Dict[str, Any]:
        started = datetime.now()
        db = DatabaseFacade(self.config_manager)
        base_str = latest_closed_trade_date(db)
        base_date = _to_date(base_str) or date.today()
        rules = self._load_rules()

        rows: List[Dict[str, Any]] = []
        red: List[Dict[str, Any]] = []
        yellow: List[Dict[str, Any]] = []

        for rule in rules:
            table = rule.get('table')
            col = rule.get('date_column')
            label = rule.get('label') or table
            exempt = bool(rule.get('exempt', False))
            max_lag = int(rule.get('max_lag_days', 3))
            entry = {'table': table, 'label': label, 'date_column': col,
                     'max_date': None, 'lag_days': None, 'status': '未知',
                     'exempt': exempt, 'max_lag_days': max_lag}
            if not table or not col:
                entry['status'] = '错误'
                entry['detail'] = '规则缺 table/date_column'
                red.append(entry)
                rows.append(entry)
                continue
            try:
                r = db.execute_query(f'SELECT MAX({col}) AS mx FROM {table}')
                mx_raw = r[0]['mx'] if r else None
            except Exception as e:
                entry['status'] = '错误'
                entry['detail'] = str(e)[:120]
                red.append(entry)
                rows.append(entry)
                continue
            mx_date = _to_date(mx_raw)
            entry['max_date'] = mx_date.strftime('%Y%m%d') if mx_date else None
            if mx_date is None:
                entry['status'] = '红灯'
                entry['lag_days'] = None
                entry['detail'] = '表内无数据'
                red.append(entry)
            else:
                lag = (base_date - mx_date).days
                entry['lag_days'] = lag
                if exempt:
                    entry['status'] = '豁免'
                elif lag > max_lag:
                    entry['status'] = '红灯'
                    red.append(entry)
                elif lag > 0:
                    entry['status'] = '黄灯'
                    yellow.append(entry)
                else:
                    entry['status'] = '绿灯'
            rows.append(entry)

        # 专项：交易日历覆盖度（F7 跨年保障）
        cal_entry = {'table': 'base_trade_calendar', 'label': '交易日历覆盖',
                     'date_column': 'calendar_date', 'max_date': None,
                     'lag_days': None, 'status': '未知', 'exempt': False,
                     'max_lag_days': -CALENDAR_MIN_AHEAD_DAYS}
        try:
            r = db.execute_query('SELECT MAX(calendar_date) AS mx FROM base_trade_calendar')
            mx_date = _to_date(r[0]['mx']) if r else None
            if mx_date:
                cal_entry['max_date'] = mx_date.strftime('%Y%m%d')
                ahead = (mx_date - date.today()).days
                cal_entry['lag_days'] = -ahead  # 负数=超前天数
                if ahead >= CALENDAR_MIN_AHEAD_DAYS:
                    cal_entry['status'] = '绿灯'
                else:
                    cal_entry['status'] = '红灯'
                    red.append(cal_entry)
            else:
                cal_entry['status'] = '红灯'
                cal_entry['detail'] = '日历表无数据'
                red.append(cal_entry)
        except Exception as e:
            cal_entry['status'] = '错误'
            cal_entry['detail'] = str(e)[:120]
            red.append(cal_entry)
        rows.append(cal_entry)

        # 专项：复权因子 divergent 比例监控（§8 批次 C——候选=权益重建独立口径，
        # divergent 占比跳变=官方因子被修订/出错的早期信号；基线 2026-11-26 ≈ 44%）
        DIV_BASELINE = 44.0
        DIV_ALERT_PP = 10.0   # 相对基线跳变 10 个百分点即告警
        div_entry = {'table': 'adjust_factor_candidate', 'label': '复权因子口径校验',
                     'date_column': 'update_time', 'max_date': None,
                     'lag_days': None, 'status': '未知', 'exempt': False,
                     'max_lag_days': 0}
        try:
            r = db.execute_query(
                "SELECT check_result, COUNT(*) AS n FROM adjust_factor_candidate "
                "WHERE check_result IS NOT NULL GROUP BY check_result")
            cnt = {x['check_result']: x['n'] for x in r}
            total = sum(cnt.values())
            if total:
                div_pct = round(cnt.get('divergent', 0) * 100.0 / total, 2)
                div_entry['max_date'] = datetime.now().strftime('%Y%m%d')
                div_entry['lag_days'] = 0
                div_entry['detail'] = (f'官方对比 {total} 行：divergent {div_pct}%'
                                       f'（基线 {DIV_BASELINE}%）/ match '
                                       f'{round(cnt.get("match", 0) * 100.0 / total, 1)}%')
                if abs(div_pct - DIV_BASELINE) <= DIV_ALERT_PP:
                    div_entry['status'] = '绿灯'
                else:
                    div_entry['status'] = '红灯'
                    div_entry['detail'] += f'——跳变超 {DIV_ALERT_PP}pp，官方因子可能被修订，排查'
                    red.append(div_entry)
            else:
                div_entry['status'] = '黄灯'
                div_entry['detail'] = '无已评校验行（候选未校验）'
        except Exception as e:
            div_entry['status'] = '错误'
            div_entry['detail'] = str(e)[:120]
        rows.append(div_entry)

        red_n = len(red)
        status = '正常' if red_n == 0 else f'红灯 {red_n} 项'
        lines = self._render(base_str, rows, red, yellow)
        report_path = write_review_report('数据新鲜度巡检报告', 'freshness_check_report',
                                          status, lines,
                                          duration_seconds=(datetime.now() - started).total_seconds())
        return {
            'success': red_n == 0,
            'base_date': base_str.replace('-', ''),
            'red': red,
            'yellow': yellow,
            'rows': rows,
            'report_path': report_path,
            'duration': round((datetime.now() - started).total_seconds(), 1),
        }

    def _render(self, base_str: str, rows: List[Dict[str, Any]],
                red: List[Dict], yellow: List[Dict]) -> List[str]:
        lines = [
            f'- 基准日（最近已收盘交易日）: {base_str}',
            f'- 红灯 {len(red)} 项 / 黄灯 {len(yellow)} 项',
            '',
            '| 表 | 名称 | 最新数据日 | 滞后自然日 | 阈值 | 状态 |',
            '| --- | --- | --- | --- | --- | --- |',
        ]
        for r in rows:
            lines.append(
                f"| {r['table']} | {r['label']} | {r.get('max_date') or '-'} | "
                f"{r.get('lag_days') if r.get('lag_days') is not None else '-'} | "
                f"{r['max_lag_days']} | {r['status']}{'（' + r.get('detail', '') + '）' if r.get('detail') else ''} |"
            )
        if red:
            lines.append('')
            lines.append('**红灯处置**：对应源补采（编排内源重跑 `sync-review-all --sources=<源>`；'
                         '编排外源跑对应 sync-* 命令）；north_flow_detail_daily 为源侧停披露豁免项，不计红。')
        return lines


class DatabaseFacade:
    """最小 DB 门面：只暴露 execute_query，便于巡检在无完整 DatabaseConnection 场景复用"""

    def __init__(self, config_manager: ConfigManager):
        from ..database import DatabaseConnection
        self._db = DatabaseConnection(config_manager)

    def execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        return self._db.execute_query(query, params)

    # F2 数据日校验复用同一门面
    def fetch_one(self, query: str, params: tuple = None) -> Optional[Dict[str, Any]]:
        return self._db.fetch_one(query, params)
