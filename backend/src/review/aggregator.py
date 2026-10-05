"""六维聚合器（复盘报告层方案 §5.2/§5.3，M1）。

职责：输入 DataReader/DAO 取数，输出 ``dict[维度] -> AggregationResult``。
**指标独立计算，不依赖历史链 CSV**（§5.2 关键设计）：晋级率、昨日涨停溢价等
需要 T-1 截面的指标基于 ``DataReader.load_kline``（60 交易日窗口）+
``cross_section`` 向量化重算任意截面；当日链 CSV 仅用于明细罗列（含是否存疑）
与交叉校验（重算涨停家数 vs CSV 涨停家数）。

数据真实性原则：某维度输入表为空 → 该维结果带 ``available=False`` 与缺失原因，
由 sections 渲染为「⏳ 待补」，禁止编造。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from bisect import bisect_right
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ..analysis.candidate_rules import (
    BUY_RULES,
    is_r1,
    is_r2,
    is_r3,
    is_r4,
)
from ..analysis.data_reader import DataReader
from .cross_section import (
    annotate_panel,
    cross_section_at,
    sentiment_metrics,
    suppress_first_day,
)

logger = logging.getLogger(__name__)

#: 行业/概念源常量（§5.3 维度3）
SOURCE_SW = 'sw_2021'
SOURCE_SINA_HY = 'sina_hy'
SOURCE_SINA_GN = 'sina_gn'

#: 快照补充项白名单（阶段 2 V2.0 §3.2：指数已由 overseas_kline_day 渲染，
#: 快照表仅保留 A50 与汇率三码；fx_shkdcny 为 A-H 溢价分母）
_OVERSEAS_SNAPSHOT_WHITELIST = {'富时A50期货', '离岸人民币', '美元兑人民币', '港元兑人民币'}


@dataclass
class AggregationResult:
    """单维聚合结果（available=False 时 reason 说明缺口）

    提供 dict 式访问器（``get``/``[]``），sections 以 ``result.get('data')``
    等语法消费，兼容 dict 约定。
    """

    available: bool = True
    reason: str = ''
    data: Dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        return getattr(self, key, default)

    def __getitem__(self, key: str) -> Any:
        try:
            return getattr(self, key)
        except AttributeError as e:
            raise KeyError(key) from e

    def __contains__(self, key: str) -> bool:
        return hasattr(self, key)


class ReviewAggregator:
    """六维聚合（维度1 大盘环境 / 2 市场情绪 / 3 板块主线 / 4 个股异动 / 5 业绩估值）"""

    def __init__(self, data_reader: DataReader, config: Dict[str, Any]):
        self.reader = data_reader
        self.config = config
        review_cfg = (config.get('review') or {})
        self.industry_sources = list(review_cfg.get('industry_sources')
                                     or [SOURCE_SW, SOURCE_SINA_HY])
        self.volume_surge_min = float(review_cfg.get('volume_surge_min', 2.0))
        self.top_n = int(review_cfg.get('top_n', 20))
        self.sentiment_rules = dict(review_cfg.get('sentiment_rules') or {})
        # 情绪判据定稿开关（config review.sentiment_calibrated）：定稿后摘除"试用版"水印
        self.sentiment_calibrated = bool(review_cfg.get('sentiment_calibrated', False))
        self.db = data_reader  # 辅助表走 DataReader._query（同一只读连接）

    # ------------------------------------------------------------------ 入口

    def aggregate(self, as_of: str) -> Dict[str, Any]:
        """全量聚合：加载 60d 面板 → 截面（今/昨）→ 六维结果。

        Returns:
            {as_of, prev_date, window_start, trading_days, panel_rows,
             market_env, sentiment, sector, movers, fundamentals,
             cross_check} 各键为 AggregationResult 或原始 dict。
        """
        start = self.reader.nth_trading_day_back(as_of, 62)
        cols = ['stock_name', 'open', 'high', 'low', 'close', 'preclose',
                'change_rate', 'volume', 'amount', 'is_st']
        df = self.reader.load_kline(cols, as_of, start_date=start)
        if df.empty:
            raise ValueError(f'锚定日 {as_of} 窗口内无日K数据')
        prev_date = self.reader.nth_trading_day_back(as_of, 1)

        panel = annotate_panel(df)
        # 新股上市首日不判定（与链模块口径对齐；北交所 N 前缀兜底）
        list_dates = {s.ts_code: s.list_date for s in self.reader.load_stock_universe()}
        panel = suppress_first_day(panel, list_dates)
        cs_today = cross_section_at(panel, as_of)
        cs_prev = cross_section_at(panel, prev_date) if prev_date else cs_prev_empty()

        out: Dict[str, Any] = {
            'as_of': as_of,
            'prev_date': prev_date,
            'window_start': start,
            'panel_rows': int(len(df)),
            'stock_count': int(cs_today['ts_code'].nunique()) if not cs_today.empty else 0,
        }
        out['sentiment'] = self._aggregate_sentiment(cs_today, cs_prev, as_of=as_of)
        out['market_env'] = self._aggregate_market_env(panel, cs_today, as_of, prev_date)
        out['sector'] = self._aggregate_sector(panel, cs_today, as_of)
        out['movers'] = self._aggregate_movers(cs_today, as_of)
        out['fundamentals'] = self._aggregate_fundamentals(as_of, prev_date)
        # 开发项 D：五类已采集数据进报告（表空 → available=False → 渲染"⏳ 待补"）
        out['margin'] = self._aggregate_margin(as_of)
        out['lhb'] = self._aggregate_lhb(as_of)
        out['money_flow'] = self._aggregate_money_flow(as_of)
        out['unlock'] = self._aggregate_unlock(as_of)
        out['announcement'] = self._aggregate_announcement(as_of)
        # 转天买入候选池（第六章预案 × tomorrow_candidates 链同口径；candidate_rules
        # 谓词共用，全量不截断，展示层按 top_n 截断——与链 CSV top N 同集同序）
        clusters = ((out['sector'].data or {}).get('limit_up_clusters')
                    if isinstance(out['sector'], AggregationResult) else None) or []
        lhb_data = (out['lhb'].data if isinstance(out['lhb'], AggregationResult) else {}) or {}
        out['candidates'] = self._build_candidates(
            cs_today, as_of, clusters,
            self._seal_map_of_day(as_of),
            lhb_data.get('institution') or [])
        return out

    # ------------------------------------------------------------------ 维度1

    def _aggregate_market_env(self, panel: pd.DataFrame, cs_today: pd.DataFrame,
                              as_of: str, prev_date: Optional[str]) -> AggregationResult:
        indices = self._index_positions(as_of)
        turnover = self._turnover_delta(panel, as_of, prev_date)
        adv_dec = self._advance_decline(cs_today)
        overseas_all = self._overseas_all()
        overseas = [r for r in overseas_all if r['name'] in _OVERSEAS_SNAPSHOT_WHITELIST]
        data = {'indices': indices, 'turnover': turnover,
                'advance_decline': adv_dec, 'overseas': overseas,
                'overseas_all': overseas_all,
                'style_spread': self._style_spread(indices),
                # 阶段 2（V2.0 §3.5）：外围日K指数 / 假期外围观察 / A-H 映射（全部只读）
                'as_of': as_of,
                'overseas_indices': self._overseas_indices(as_of),
                'holiday_overseas': self._holiday_overseas(as_of, prev_date),
                'ah_mapping': self._ah_mapping(as_of)}
        available = bool(indices or turnover or adv_dec)
        return AggregationResult(available=available,
                                 reason='' if available else 'trade_index_info/overseas_market_daily 为空',
                                 data=data)

    @staticmethod
    def _style_spread(indices: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """大小盘风格：沪深300 vs 中证1000 vs 中证2000 当日涨跌幅（I-1 + N2 2026-10-04；
        正差值=大盘占优）。中证2000 缺数据时仅保留 1000 口径（spread_2000=None）"""
        by_name = {i.get('name'): i for i in indices}
        big, small = by_name.get('沪深300'), by_name.get('中证1000')
        if not big or not small:
            return None
        b, s = big.get('change_rate'), small.get('change_rate')
        if b is None or s is None:
            return None
        small_2000 = by_name.get('中证2000')
        s2 = small_2000.get('change_rate') if small_2000 else None
        return {'big': b, 'small': s, 'spread': round(b - s, 2),
                'small_2000': s2,
                'spread_2000': round(b - s2, 2) if s2 is not None else None}

    def _index_positions(self, as_of: str) -> List[Dict[str, Any]]:
        """指数涨跌与 60d 位置百分位（trade_index_info；清单来自 review_sync.indices
        配置 + csindex_perf_indices（中证全指作全市场基准、中证2000 作微盘对照，N2））"""
        review_cfg = self.config.get('review_sync') or {}
        entries = list(review_cfg.get('indices') or [])
        entries += list(review_cfg.get('csindex_perf_indices') or [])
        ts_codes = [i.get('ts_code') for i in entries if i.get('ts_code')]
        ts_codes = list(dict.fromkeys(ts_codes))
        if not ts_codes:
            return []
        placeholders = ', '.join(['%s'] * len(ts_codes))
        try:
            rows = self.db._query(
                f"""
                SELECT index_name, trade_date, close, preclose, change_rate
                FROM trade_index_info
                WHERE trade_date <= %s AND ts_code IN ({placeholders})
                ORDER BY index_name, trade_date
                """, (as_of, *ts_codes))
        except Exception as e:
            logger.warning(f'指数行情读取失败: {e}')
            return []
        if not rows:
            return []
        df = pd.DataFrame(rows, columns=['name', 'date', 'close', 'preclose', 'change_rate'])
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df['change_rate'] = pd.to_numeric(df['change_rate'], errors='coerce')
        out = []
        for name, g in df.groupby('name'):
            last = g.iloc[-1]
            if str(last['date']) != as_of:
                continue  # 仅呈现锚定日有数据的指数
            w = g.tail(61)
            lo, hi = float(w['close'].min()), float(w['close'].max())
            pos = (float(last['close']) - lo) / (hi - lo) * 100 if hi > lo else None
            out.append({'name': name, 'change_rate': _f(last['change_rate']),
                        'close': _f(last['close']), 'position_pct': _f(pos)})
        return out

    @staticmethod
    def _turnover_delta(panel: pd.DataFrame, as_of: str,
                        prev_date: Optional[str]) -> Dict[str, Any]:
        """两市成交额今昨对比（沪深+北交所分组）"""
        df = panel[panel['trade_date'].isin([d for d in (as_of, prev_date) if d])]
        if df.empty:
            return {}
        grp = df.assign(mkt=df['ts_code'].str.split('.').str[0]).groupby(
            ['mkt', 'trade_date'])['amount'].sum().unstack(fill_value=0)
        today_col, prev_col = as_of, prev_date
        result = {'today_total': 0.0, 'prev_total': 0.0, 'delta': None, 'delta_pct': None,
                  'by_market': {}}
        for mkt in grp.index:
            t = float(grp.loc[mkt, today_col]) if today_col in grp.columns else 0.0
            p = float(grp.loc[mkt, prev_col]) if (prev_col and prev_col in grp.columns) else 0.0
            result['by_market'][mkt] = {'today': t, 'prev': p}
            result['today_total'] += t
            result['prev_total'] += p
        if result['prev_total'] > 0:
            result['delta'] = result['today_total'] - result['prev_total']
            result['delta_pct'] = round(result['delta'] / result['prev_total'] * 100, 2)
        return result

    @staticmethod
    def _advance_decline(cs_today: pd.DataFrame) -> Dict[str, Any]:
        """上涨/下跌/平盘家数（剔除停牌无成交行）"""
        if cs_today.empty:
            return {}
        traded = cs_today[cs_today['_traded']]
        change = pd.to_numeric(traded['change_rate'], errors='coerce').dropna()
        return {'up': int((change > 0).sum()), 'down': int((change < 0).sum()),
                'flat': int((change == 0).sum()), 'total': int(len(change))}

    def _overseas_all(self) -> List[Dict[str, Any]]:
        """外围快照全行（最新可得日期）——日K缺失时的降级回退与快照过滤的输入"""
        try:
            rows = self.db._query(
                "SELECT trade_date, market_name, close, change_pct FROM overseas_market_daily "
                "WHERE trade_date = (SELECT MAX(trade_date) FROM overseas_market_daily) "
                "ORDER BY market_name")
        except Exception as e:
            logger.warning(f'外围市场读取失败: {e}')
            return []
        return [{'date': r[0], 'name': r[1], 'close': _f(r[2]), 'change_pct': _f(r[3])}
                for r in rows]

    def _overseas_indices(self, as_of: str) -> List[Dict[str, Any]]:
        """外围指数 4 指标（overseas_kline_day；窗口=各市场自身日历，与 _index_positions
        同构——阶段 2 V2.0 §3.2，Q1 定稿：60 日位置同构而非 250 日分位）。

        每代码升序收盘尾部窗口：当日/5日/20日累计 + 60 日位置百分位；
        行数不足时对应指标置 None（不硬造），全部代码无行返回 []（渲染层降级快照口径）。
        """
        entries = self._overseas_index_entries()
        if not entries:
            return []
        codes = [c for c, _ in entries]
        placeholders = ', '.join(['%s'] * len(codes))
        try:
            rows = self.db._query(
                f"""
                SELECT market_code, trade_date, close FROM overseas_kline_day
                WHERE market_code IN ({placeholders}) AND trade_date <= %s
                  AND close IS NOT NULL
                ORDER BY market_code, trade_date
                """, (*codes, as_of))
        except Exception as e:
            logger.warning(f'外围日K读取失败: {e}')
            return []
        if not rows:
            return []
        series: Dict[str, List] = {}
        for code, d, close in rows:
            series.setdefault(code, []).append((str(d), float(close)))
        out = []
        for code, name in entries:
            data = series.get(code)
            if not data:
                continue  # 该代码 ≤as_of 无行（如新纳入）不出行
            dates = [d for d, _ in data]
            closes = [v for _, v in data]
            out.append({
                'name': name, 'date': dates[-1], 'close': _f(closes[-1]),
                'change_pct': _pct2(closes[-1] / closes[-2] - 1) if len(closes) >= 2 else None,
                'chg_5d': _pct2(closes[-1] / closes[-6] - 1) if len(closes) >= 6 else None,
                'chg_20d': _pct2(closes[-1] / closes[-21] - 1) if len(closes) >= 21 else None,
                'position_pct': self._window_position(closes),
            })
        return out

    @staticmethod
    def _window_position(closes: List[float]) -> Optional[float]:
        """60 日窗口位置百分位（tail(61) 同构）；样本 <30 不出分位（防误导，B2）"""
        if len(closes) < 30:
            return None
        w = closes[-61:]
        lo, hi = min(w), max(w)
        if hi <= lo:
            return None
        return round((closes[-1] - lo) / (hi - lo) * 100, 1)

    def _holiday_overseas(self, as_of: str,
                          prev_date: Optional[str]) -> Optional[Dict[str, Any]]:
        """假期外围观察（阶段 2 V2.0 §3.3；Q3 定稿：区间首尾收盘比）。

        触发：锚定日与前一 A 股交易日自然日差 >3 天（B4/B5）；区间 = (prev_date, as_of)
        双开——锚定日当日外围变化属"当日%"列，不重复计入。区间无行的市场 accum=None（B6）。
        """
        if not prev_date or as_of <= prev_date:
            return None
        gap = (datetime.strptime(as_of, '%Y%m%d').date()
               - datetime.strptime(prev_date, '%Y%m%d').date()).days
        if gap <= 3:
            return None
        entries = self._overseas_index_entries()
        if not entries:
            return None
        codes = [c for c, _ in entries]
        placeholders = ', '.join(['%s'] * len(codes))
        try:
            base_rows = self.db._query(
                f"""
                SELECT DISTINCT ON (market_code) market_code, trade_date, close
                FROM overseas_kline_day
                WHERE market_code IN ({placeholders}) AND trade_date <= %s
                  AND close IS NOT NULL
                ORDER BY market_code, trade_date DESC
                """, (*codes, prev_date))
            span_rows = self.db._query(
                f"""
                SELECT market_code, trade_date, close FROM overseas_kline_day
                WHERE market_code IN ({placeholders}) AND trade_date > %s
                  AND trade_date < %s AND close IS NOT NULL
                ORDER BY market_code, trade_date
                """, (*codes, prev_date, as_of))
        except Exception as e:
            logger.warning(f'假期外围区间读取失败: {e}')
            return None
        base = {r[0]: float(r[2]) for r in base_rows}
        span: Dict[str, List] = {}
        for code, d, close in span_rows:
            span.setdefault(code, []).append((str(d), float(close)))
        out_rows, last_dates = [], []
        for code, name in entries:
            data = span.get(code) or []
            b = base.get(code)
            accum = (data[-1][1] / b - 1) * 100 if data and b else None
            last = data[-1][0] if data else None
            if last:
                last_dates.append(last)
            out_rows.append({'name': name, 'accum_pct': None if accum is None
                             else round(accum, 2),
                             'days': len(data), 'last_date': last})
        return {'prev_date': prev_date, 'rows': out_rows,
                'last_date': max(last_dates) if last_dates else None}

    def _ah_mapping(self, as_of: str) -> Dict[str, Any]:
        """A-H 15 只 5 日累计对比 + 溢价率（阶段 2 V2.0 §3.4；Q5/Q8/Q9 定稿）。

        H 侧 overseas_kline_day（qfq）+ A 侧 his_kline_day（前复权），各自市场日历
        5 日窗口；对齐判定按市场交易日滞后（任一侧 >3 标 unaligned，防长期停牌假对比）；
        溢价率 = A 收盘 /（H 收盘 × fx_shkdcny 最新快照）− 1（参考口径），fx 缺失整列 None。
        """
        out: Dict[str, Any] = {'rows': [], 'fx': None, 'fx_date': None}
        pairs = [(e.get('code'), e.get('name'), e.get('a_code'))
                 for e in ((self.config.get('review_sync') or {})
                           .get('overseas_kline_stocks') or [])
                 if e.get('code') and e.get('a_code')]
        if not pairs:
            return out
        try:
            fx_rows = self.db._query(
                "SELECT trade_date, close FROM overseas_market_daily "
                "WHERE market_code = 'fx_shkdcny' ORDER BY trade_date DESC LIMIT 1")
            if fx_rows and fx_rows[0][1] is not None:
                out['fx_date'] = str(fx_rows[0][0])
                out['fx'] = float(fx_rows[0][1])
            cal_rows = self.db._query(
                "SELECT calendar_date FROM base_trade_calendar "
                "WHERE is_trading_day = 1 AND calendar_date <= %s", (_dot(as_of),))
        except Exception as e:
            logger.warning(f'A-H 映射（汇率/日历）读取失败: {e}')
            fx_rows, cal_rows = [], []
        fx = out['fx']
        a_calendar = sorted(_plain(r[0]) for r in (cal_rows or []))

        a_data: Dict[str, List] = {}
        h_data: Dict[str, List] = {}
        min_dh = None
        for h_code, _, a_code in pairs:
            a_data[h_code] = self._tail_closes(
                "SELECT trade_date, close FROM his_kline_day WHERE ts_code = %s "
                "AND trade_date <= %s AND close IS NOT NULL ORDER BY trade_date DESC LIMIT 6",
                (a_code, as_of))
            h_data[h_code] = self._tail_closes(
                "SELECT trade_date, close FROM overseas_kline_day WHERE market_code = %s "
                "AND trade_date <= %s AND close IS NOT NULL ORDER BY trade_date DESC LIMIT 6",
                (h_code, as_of))
            dh = (h_data[h_code] or [(None, None)])[-1][0]
            if dh and (min_dh is None or dh < min_dh):
                min_dh = dh
        # H 侧滞后判定输入：hkHSI 在 [最早 d_h, as_of] 的交易日（一次查询，§9.2）
        hsi_dates: List[str] = []
        if min_dh:
            try:
                hsi_rows = self.db._query(
                    "SELECT trade_date FROM overseas_kline_day WHERE market_code = 'hkHSI' "
                    "AND trade_date > %s AND trade_date <= %s ORDER BY trade_date",
                    (min_dh, as_of))
                hsi_dates = [str(r[0]) for r in hsi_rows]
            except Exception as e:
                logger.warning(f'hkHSI 交易日读取失败: {e}')

        for h_code, h_name, a_code in pairs:
            a_series, h_series = a_data[h_code], h_data[h_code]
            a_close = a_series[-1][1] if a_series else None
            h_close = h_series[-1][1] if h_series else None
            chg_a = (a_series[-1][1] / a_series[0][1] - 1) * 100 \
                if a_series and len(a_series) >= 6 else None
            chg_h = (h_series[-1][1] / h_series[0][1] - 1) * 100 \
                if h_series and len(h_series) >= 6 else None
            d_a = a_series[-1][0] if a_series else None
            d_h = h_series[-1][0] if h_series else None
            lag_a = _count_gt(a_calendar, d_a) if d_a else None
            lag_h = _count_gt(hsi_dates, d_h) if d_h else None
            unaligned = bool((lag_a is not None and lag_a > 3)
                             or (lag_h is not None and lag_h > 3))
            diff = (chg_a - chg_h) if (chg_a is not None and chg_h is not None) else None
            premium = None
            if a_close and h_close and fx:
                premium = (a_close / (h_close * fx) - 1) * 100
            out['rows'].append({'h_name': h_name,
                                'chg_a_5d': None if chg_a is None else round(chg_a, 2),
                                'chg_h_5d': None if chg_h is None else round(chg_h, 2),
                                'diff': None if diff is None else round(diff, 2),
                                'premium': None if premium is None else round(premium, 2),
                                'unaligned': unaligned})
        return out

    def _tail_closes(self, sql: str, params: tuple) -> List[Tuple[str, float]]:
        """尾部收盘序列（SQL 须 DESC LIMIT 取尾窗；此处反转为升序 [(date, close), …]）。
        查询失败返回空——单股缺数不阻塞整块（B9）。"""
        try:
            rows = self.db._query(sql, params)
        except Exception as e:
            logger.warning(f'A-H 尾窗查询失败（{params[0]}）: {e}')
            return []
        return [(str(r[0]), float(r[1])) for r in reversed(rows)]

    def _overseas_index_entries(self) -> List:
        review_cfg = self.config.get('review_sync') or {}
        return [(e.get('code'), e.get('name'))
                for e in (review_cfg.get('overseas_kline_indices') or [])
                if e.get('code')]

    # ------------------------------------------------------------------ 维度2

    #: sentiment_metrics_daily 读取列（顺序即 _query 元组顺序）
    _SENTIMENT_TABLE_COLS = ('trade_date', 'stock_count', 'up_count', 'down_count',
                             'flat_count', 'limit_up_count', 'limit_down_count',
                             'burst_count', 'burst_rate', 'max_streak', 'ladder',
                             'premium_mean', 'premium_median', 'premium_count',
                             'premium_continued', 'promotion')

    def _aggregate_sentiment(self, cs_today: pd.DataFrame, cs_prev: pd.DataFrame,
                             as_of: Optional[str] = None) -> AggregationResult:
        """先查 sentiment_metrics_daily（开发项 B 回填表），miss 回退面板重算——
        报告永不因新表未回填而失败。表行与重算逐字节同口径（同一实现产出）。"""
        row = self._fetch_sentiment_table_row(as_of) if as_of else None
        if row is not None:
            metrics = self._sentiment_row_to_metrics(row)
            metrics['from_table'] = True
        else:
            metrics = sentiment_metrics(cs_today, cs_prev)
            metrics['from_table'] = False
        metrics['cycle'] = self._sentiment_cycle(metrics)
        metrics['percentiles'] = self._sentiment_percentiles(as_of, metrics) if as_of else None
        return AggregationResult(data=metrics)

    def _fetch_sentiment_table_row(self, as_of: Optional[str]) -> Optional[Dict[str, Any]]:
        cols = ', '.join(self._SENTIMENT_TABLE_COLS)
        try:
            rows = self.db._query(
                f"SELECT {cols} FROM sentiment_metrics_daily WHERE trade_date = %s", (as_of,))
        except Exception as e:
            logger.warning(f'情绪日频表读取失败（回退重算）: {e}')
            return None
        return dict(zip(self._SENTIMENT_TABLE_COLS, rows[0])) if rows else None

    @staticmethod
    def _sentiment_row_to_metrics(row: Dict[str, Any]) -> Dict[str, Any]:
        """表行 -> sentiment_metrics 同构 dict（JSONB/Decimal 还原；键与截面重算一致）"""
        def _num(v, cast=float):
            if v is None:
                return None
            try:
                return cast(v)
            except (TypeError, ValueError):
                return None

        return {
            'limit_up_count': _num(row.get('limit_up_count'), int) or 0,
            'limit_down_count': _num(row.get('limit_down_count'), int) or 0,
            'burst_count': _num(row.get('burst_count'), int) or 0,
            'burst_rate': _num(row.get('burst_rate')),
            # JSONB 往返后键为字符串——还原为 int（报告层排序与 n-1 晋级档位依赖 int 键）
            'ladder': {int(k): v for k, v in (row.get('ladder') or {}).items()},
            'max_streak': _num(row.get('max_streak'), int) or 0,
            'premium_mean': _num(row.get('premium_mean')),
            'premium_median': _num(row.get('premium_median')),
            'premium_count': _num(row.get('premium_count'), int) or 0,
            'premium_continued': _num(row.get('premium_continued'), int) or 0,
            'promotion': {int(k): v for k, v in (row.get('promotion') or {}).items()},
            'stock_count': _num(row.get('stock_count'), int),
            'up_count': _num(row.get('up_count'), int),
            'down_count': _num(row.get('down_count'), int),
            'flat_count': _num(row.get('flat_count'), int),
        }

    def _sentiment_percentiles(self, as_of: Optional[str],
                               metrics: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """四个情绪指标的过去 250 交易日分位（sentiment 章节"一年分位"注记）。

        历史样本 < 30 天不出分位（防误导）；表缺失/读取失败一律返回 None（报告不失败）。
        """
        if not as_of:
            return None
        try:
            rows = self.db._query(
                """
                SELECT trade_date, limit_up_count, burst_rate, max_streak, premium_mean
                FROM sentiment_metrics_daily
                WHERE trade_date <= %s
                ORDER BY trade_date DESC LIMIT 251
                """, (as_of,))
        except Exception as e:
            logger.warning(f'情绪分位历史读取失败: {e}')
            return None
        hist = [r for r in rows if str(r[0]) != str(as_of)][:250]
        if len(hist) < 30:
            return None

        def _pct(idx: int, value):
            if value is None:
                return None
            vals = [r[idx] for r in hist if r[idx] is not None]
            if len(vals) < 30:
                return None
            return round(100.0 * sum(1 for v in vals if v <= float(value)) / len(vals), 1)

        out: Dict[str, Any] = {}
        for idx, key in ((1, 'limit_up_count'), (2, 'burst_rate'),
                         (3, 'max_streak'), (4, 'premium_mean')):
            value = metrics.get(key)
            if isinstance(value, (int, float)) or value is None:
                out[key] = {'value': value, 'pct': _pct(idx, value), 'n': len(hist)}
        return out

    def _sentiment_cycle(self, m: Dict[str, Any]) -> Dict[str, Any]:
        """情绪周期定位（判据阈值取 config review.sentiment_rules，试用期水印）"""
        rules = self.sentiment_rules
        max_streak = m.get('max_streak') or 0
        burst_rate = m.get('burst_rate')
        premium_mean = m.get('premium_mean')
        cycle, basis = '试用版判据待校准', []
        try:
            climax = rules.get('climax') or {}
            ice = rules.get('ice') or {}
            ebb = rules.get('ebb') or {}
            if (max_streak >= climax.get('max_streak_gte', 99)
                    and (premium_mean is not None and premium_mean >= climax.get('premium_mean_gte', 99))):
                cycle, basis = '高潮', [f"高度{max_streak}≥{climax.get('max_streak_gte')}",
                                        f"溢价{premium_mean}≥{climax.get('premium_mean_gte')}"]
            elif (max_streak <= ice.get('max_streak_lte', -1)
                    or (burst_rate is not None and burst_rate >= ice.get('burst_rate_gte', 99))):
                cycle, basis = '冰点', [f"高度{max_streak}≤{ice.get('max_streak_lte')}"
                                        f" 或 炸板率{burst_rate}≥{ice.get('burst_rate_gte')}"]
            elif premium_mean is not None and premium_mean <= ebb.get('premium_mean_lte', 999):
                cycle, basis = '退潮', [f"溢价均值{premium_mean}≤{ebb.get('premium_mean_lte')}"]
            else:
                cycle, basis = '回暖/震荡', ['未命中冰点/高潮/退潮判据']
        except Exception as e:
            logger.warning(f'情绪周期判据计算失败: {e}')
            cycle, basis = '试用版判据待校准', [f'判据计算异常: {e}']
        return {'position': cycle, 'basis': basis,
                'trial': not self.sentiment_calibrated}

    # ------------------------------------------------------------------ 维度3

    def _aggregate_sector(self, panel: pd.DataFrame, cs_today: pd.DataFrame,
                          as_of: str) -> AggregationResult:
        industry_map = self._industry_map(as_of)
        concept_map = self._concept_map(as_of)
        if not industry_map and not concept_map:
            return AggregationResult(available=False,
                                     reason='sector_concept_map 为空（未跑 sync-industry/sync-concept）')
        data: Dict[str, Any] = {}
        if industry_map:
            data['industry_today'] = self._industry_ranking(panel, industry_map, [as_of])
            last5 = self._last_n_dates(as_of, 5)
            data['industry_5d'] = self._industry_ranking(panel, industry_map, last5)
        # N3（2026-10-04）：申万二级行业 Top10 子表（THS 快照口径）
        l2_map = self._industry_l2_map(as_of)
        if l2_map:
            data['industry_l2_today'] = self._industry_ranking(panel, l2_map, [as_of])
        if concept_map:
            data['limit_up_clusters'] = self._limit_up_clusters(cs_today, concept_map)
        return AggregationResult(data=data)

    def _industry_map(self, as_of: str) -> Dict[str, str]:
        """ts_code → 行业名（按 industry_sources 优先级，时点匹配）"""
        result: Dict[str, str] = {}
        for source in self.industry_sources:
            try:
                rows = self.db._query(
                    """
                    SELECT m.ts_code, c.concept_name
                    FROM sector_concept_map m
                    JOIN sector_concept c ON c.concept_code = m.concept_code
                    WHERE c.source = %s
                      AND (m.in_date IS NULL OR m.in_date <= %s)
                      AND (m.out_date IS NULL OR m.out_date > %s)
                    """, (source, as_of, as_of))
            except Exception as e:
                logger.warning(f'行业映射读取失败（{source}）: {e}')
                continue
            for ts_code, name in rows:
                result.setdefault(ts_code, name)
        return result

    def _industry_l2_map(self, as_of: str) -> Dict[str, str]:
        """ts_code → 申万二级行业名（source=sw_2021_l2_ths，时点匹配；N3 2026-10-04）。

        THS 为当前快照口径（无历史时点），仅当前成分准确。
        """
        try:
            rows = self.db._query(
                """
                SELECT m.ts_code, c.concept_name
                FROM sector_concept_map m
                JOIN sector_concept c ON c.concept_code = m.concept_code
                WHERE c.source = 'sw_2021_l2_ths'
                  AND (m.in_date IS NULL OR m.in_date <= %s)
                  AND (m.out_date IS NULL OR m.out_date > %s)
                """, (as_of, as_of))
        except Exception as e:
            logger.warning(f'申万二级映射读取失败: {e}')
            return {}
        return {ts_code: name for ts_code, name in rows}

    def _concept_map(self, as_of: str) -> Dict[str, str]:
        """ts_code → 概念名清单（sina_gn，时点匹配）"""
        result: Dict[str, List[str]] = {}
        try:
            rows = self.db._query(
                """
                SELECT m.ts_code, c.concept_name
                FROM sector_concept_map m
                JOIN sector_concept c ON c.concept_code = m.concept_code
                WHERE c.source = %s
                  AND (m.in_date IS NULL OR m.in_date <= %s)
                  AND (m.out_date IS NULL OR m.out_date > %s)
                """, (SOURCE_SINA_GN, as_of, as_of))
        except Exception as e:
            logger.warning(f'概念映射读取失败: {e}')
            return {}
        for ts_code, name in rows:
            result.setdefault(ts_code, []).append(name)
        return result

    def _last_n_dates(self, as_of: str, n: int) -> List[str]:
        rows = self.db._query(
            "SELECT calendar_date FROM base_trade_calendar "
            "WHERE is_trading_day = 1 AND calendar_date <= %s "
            "ORDER BY calendar_date DESC LIMIT %s",
            (_dot(as_of), n))
        return [_plain(r[0]) for r in reversed(rows)]

    @staticmethod
    def _industry_ranking(panel: pd.DataFrame, industry_map: Dict[str, str],
                          dates: List[str]) -> List[Dict[str, Any]]:
        """行业等权涨跌幅榜（§5.3 口径：行业内个股等权均值）"""
        df = panel[panel['trade_date'].isin(dates) & panel['ts_code'].isin(industry_map)]
        if df.empty:
            return []
        df = df.assign(industry=df['ts_code'].map(industry_map))
        change = pd.to_numeric(df['change_rate'], errors='coerce')
        df = df.assign(change=change)
        g = df.groupby('industry')['change'].agg(['mean', 'count']).dropna()
        g = g.sort_values('mean', ascending=False)
        return [{'industry': idx, 'mean_change': round(float(r['mean']), 2),
                 'stocks': int(r['count'])} for idx, r in g.iterrows()]

    @staticmethod
    def _limit_up_clusters(cs_today: pd.DataFrame,
                           concept_map: Dict[str, List[str]]) -> List[Dict[str, Any]]:
        """概念涨停聚集度（§5.3：概念内涨停家数、占成分比，按家数排序）"""
        if cs_today.empty:
            return []
        ups = cs_today[cs_today['_limit_up']]
        counts: Dict[str, int] = {}
        members: Dict[str, int] = {}
        for ts_code in concept_map:
            for c in concept_map[ts_code]:
                members[c] = members.get(c, 0) + 1
        for ts_code in ups['ts_code']:
            for c in concept_map.get(ts_code, []):
                counts[c] = counts.get(c, 0) + 1
        clusters = [{'concept': c, 'count': n,
                     'share': round(n / members[c], 4) if members.get(c) else None}
                    for c, n in counts.items()]
        return sorted(clusters, key=lambda x: (-x['count'], x['concept']))[:10]

    # ------------------------------------------------------------------ 维度4

    def _aggregate_movers(self, cs_today: pd.DataFrame, as_of: str) -> AggregationResult:
        if cs_today.empty:
            return AggregationResult(available=False, reason='锚定日无截面数据')
        ups = cs_today[cs_today['_limit_up']].sort_values('_streak', ascending=False)
        downs = cs_today[cs_today['_limit_down']]
        bursts = cs_today[cs_today['_burst']].sort_values('amount', ascending=False)
        surged = cs_today[(cs_today['_big_move'] == '大涨')
                          & (cs_today['_volume_surge'] >= self.volume_surge_min)]
        surged = surged.sort_values('amount', ascending=False)
        data = {
            'limit_up': _rows(ups, self.top_n),
            'limit_up_count': int(len(ups)),
            'limit_down': _rows(downs, self.top_n),
            'limit_down_count': int(len(downs)),
            'burst': _rows(bursts, self.top_n),
            'burst_count': int(len(bursts)),
            'surge_up': _rows(surged, self.top_n),
            'new_high': _rows(cs_today[cs_today['_new_high']].sort_values('amount', ascending=False), self.top_n),
            'new_low': _rows(cs_today[cs_today['_new_low']].sort_values('amount', ascending=False), self.top_n),
        }
        # D2/D3 交叉标记：当日龙虎榜上榜（"榜"）与大单净流入（有 money_flow 行才显示）
        lhb_codes = self._lhb_codes_of_day(as_of)
        mf_map = self._money_flow_map(as_of)
        for key in ('limit_up', 'limit_down', 'burst', 'surge_up'):
            for r in data[key]:
                r['lhb'] = '是' if r['ts_code'] in lhb_codes else None
                amt = mf_map.get(r['ts_code'])
                r['big_net_wan'] = round(amt / 1e4) if amt is not None else None
        # E 题材自算归因：仅涨停明细加"题材候选"列（推断口径）
        concept_map = self._concept_map(as_of)
        if concept_map:
            exclude = ((self.config.get('review') or {}).get('theme_exclude_concepts')) or []
            theme_map = self._theme_candidates(cs_today, concept_map, exclude=exclude)
            for r in data['limit_up']:
                r['theme'] = theme_map.get(r['ts_code'])
        # F 封板指标交叉列：涨停/炸板明细加"首封/开板"（limit_seal_stats_daily 有行才显示）
        seal_map = self._seal_map_of_day(as_of)
        for key in ('limit_up', 'burst'):
            for r in data[key]:
                s = seal_map.get(r['ts_code'])
                r['seal_time'] = s.get('first_seal_time') if s else None
                r['open_count'] = s.get('open_count') if s else None
        csv_path = self._csv_path(as_of)
        if csv_path and csv_path.exists():
            data['csv_path'] = str(csv_path)
            data['cross_check'] = self._cross_check(cs_today, csv_path)
        return AggregationResult(data=data)

    def _lhb_codes_of_day(self, as_of: str) -> set:
        """当日龙虎榜上榜代码集（D2 交叉标记；表空返回空集）"""
        try:
            rows = self.db._query(
                "SELECT ts_code FROM lhb_stock_daily WHERE trade_date = %s", (as_of,))
        except Exception as e:
            logger.warning(f'龙虎榜当日代码读取失败: {e}')
            return set()
        return {r[0] for r in rows}

    def _money_flow_map(self, as_of: str) -> Dict[str, float]:
        """当日大单净额映射（D3 交叉标记；表空返回空 dict）"""
        try:
            rows = self.db._query(
                "SELECT ts_code, big_net_amt FROM money_flow_daily "
                "WHERE trade_date = %s AND big_net_amt IS NOT NULL", (as_of,))
        except Exception as e:
            logger.warning(f'资金流当日净额读取失败: {e}')
            return {}
        return {r[0]: float(r[1]) for r in rows if r[1] is not None}

    def _seal_map_of_day(self, as_of: str) -> Dict[str, Dict[str, Any]]:
        """当日封板指标映射（F"首封/开板"列；表空/读失败返回空 dict）"""
        try:
            rows = self.db._query(
                "SELECT ts_code, first_seal_time, open_count FROM limit_seal_stats_daily "
                "WHERE trade_date = %s", (as_of,))
        except Exception as e:
            logger.warning(f'封板指标读取失败: {e}')
            return {}
        return {r[0]: {'first_seal_time': r[1], 'open_count': r[2]} for r in rows}

    # ------------------------------------------------------------------ 开发项 D：五类数据聚合

    def _aggregate_margin(self, as_of: str) -> AggregationResult:
        """D1 两融 → 维度1：最近 6 个**两所齐备**数据日的融资余额序列（沪深合计，元）。

        口径：数据日期 T-1（交易所次日盘前发布）；融券余额仅深市有值（rqye 覆盖
        约 55%，沪市官网接口无该列），v1 仅展示融资余额。
        假期口径（第八轮实测）：一所单边缺数据的日子（如深市假期停更）不计入
        趋势对比——单边合计会造成"余额骤降"假象，如实标注该日未计入。
        """
        try:
            rows = self.db._query(
                """
                SELECT trade_date,
                       SUM(CASE WHEN ts_code LIKE 'sh.%%' THEN rzye ELSE 0 END) AS sh_sum,
                       SUM(CASE WHEN ts_code LIKE 'sz.%%' THEN rzye ELSE 0 END) AS sz_sum,
                       SUM(rzye)
                FROM margin_trade_daily
                WHERE trade_date <= %s
                GROUP BY trade_date
                ORDER BY trade_date DESC
                LIMIT 10
                """, (as_of,))
        except Exception as e:
            logger.warning(f'两融读取失败: {e}')
            rows = []
        if not rows:
            return AggregationResult(available=False,
                                     reason='margin_trade_daily 无数据（回补未跑或当日未发布）')
        complete = []       # 两所齐备日（升序）
        partial_dates = []  # 单边日（如实标注，不参与趋势）
        for r in reversed(rows):
            if r[1] and r[2]:
                complete.append({'date': r[0], 'rzye': _f(r[3])})
            elif r[3]:
                partial_dates.append(r[0])
        if not complete:
            return AggregationResult(available=False,
                                     reason='margin_trade_daily 无两所齐备的数据日（单边停更中）')
        latest = complete[-1]
        prev = complete[-2] if len(complete) >= 2 else None
        delta = round(latest['rzye'] - prev['rzye'], 2) if prev else None
        trend = ''.join(
            '↑' if b['rzye'] > a['rzye'] else ('↓' if b['rzye'] < a['rzye'] else '平')
            for a, b in zip(complete, complete[1:])) or None
        return AggregationResult(data={
            'series': complete[-6:], 'latest_date': latest['date'],
            'latest_rzye': latest['rzye'], 'delta': delta, 'trend': trend,
            'partial_dates': partial_dates,
        })

    def _aggregate_lhb(self, as_of: str) -> AggregationResult:
        """D2 龙虎榜 → 维度4：净额 Top10（行粒度，禁跨原因求和）+ 机构净买 Top5。

        金额口径：该上榜原因的前五席位合计（非全市场买卖总额）；成交额列
        沪市 secTxAmount / 深市列表成交金额。
        """
        try:
            rows = self.db._query(
                """
                SELECT ts_code, stock_name, reason, buy_amt, sell_amt, net_amt, sec_tx_amount
                FROM lhb_stock_daily
                WHERE trade_date = %s
                ORDER BY net_amt DESC NULLS LAST
                LIMIT 10
                """, (as_of,))
            inst_rows = self.db._query(
                """
                SELECT x.ts_code, x.buy, x.sell FROM (
                    SELECT d.ts_code,
                           SUM(CASE WHEN d.side LIKE '买%%' THEN d.amount ELSE 0 END) AS buy,
                           SUM(CASE WHEN d.side LIKE '卖%%' THEN d.amount ELSE 0 END) AS sell
                    FROM lhb_detail d
                    WHERE d.trade_date = %s AND d.seat_name LIKE '%%机构专用%%'
                    GROUP BY d.ts_code
                ) x
                ORDER BY (x.buy - x.sell) DESC NULLS LAST
                LIMIT 5
                """, (as_of,))
            names = self.db._query(
                "SELECT ts_code, MAX(stock_name) FROM lhb_stock_daily "
                "WHERE trade_date = %s GROUP BY ts_code", (as_of,))
        except Exception as e:
            logger.warning(f'龙虎榜读取失败: {e}')
            rows, inst_rows, names = [], [], []
        if not rows and not inst_rows:
            return AggregationResult(available=False,
                                     reason='lhb_stock_daily 当日无数据（未发布或未采集）')
        name_map = {r[0]: r[1] for r in names}
        top = [{'ts_code': r[0], 'name': r[1], 'reason': r[2], 'buy': _f(r[3]),
                'sell': _f(r[4]), 'net': _f(r[5]), 'sec_tx_amount': _f(r[6])}
               for r in rows]
        inst = [{'ts_code': r[0], 'name': name_map.get(r[0]), 'net': _f(_safe_sub(r[1], r[2])),
                 'buy': _f(r[1]), 'sell': _f(r[2])} for r in inst_rows]
        return AggregationResult(data={'top': top, 'institution': inst})

    def _aggregate_money_flow(self, as_of: str) -> AggregationResult:
        """D3 分笔资金流 → 维度4：big_net_amt 降序/升序 Top10。

        口径：异动子集近似口径（涨停∪跌停∪|涨跌幅|≥7%∪成交额Top50，单笔≥50 万
        分桶），非全市场。
        """
        try:
            top = self.db._query(
                """
                SELECT m.ts_code, k.stock_name, m.big_net_amt, m.total_amt
                FROM money_flow_daily m
                LEFT JOIN his_kline_day k ON k.ts_code = m.ts_code AND k.trade_date = m.trade_date
                WHERE m.trade_date = %s AND m.big_net_amt IS NOT NULL
                ORDER BY m.big_net_amt DESC
                LIMIT 10
                """, (as_of,))
            bottom = self.db._query(
                """
                SELECT m.ts_code, k.stock_name, m.big_net_amt, m.total_amt
                FROM money_flow_daily m
                LEFT JOIN his_kline_day k ON k.ts_code = m.ts_code AND k.trade_date = m.trade_date
                WHERE m.trade_date = %s AND m.big_net_amt IS NOT NULL
                ORDER BY m.big_net_amt ASC
                LIMIT 10
                """, (as_of,))
        except Exception as e:
            logger.warning(f'资金流读取失败: {e}')
            top, bottom = [], []
        if not top and not bottom:
            return AggregationResult(available=False,
                                     reason='money_flow_daily 当日无数据（未采集）')

        def _pack(rs):
            return [{'ts_code': r[0], 'name': r[1], 'big_net': _f(r[2]),
                     'total_amt': _f(r[3])} for r in rs]
        return AggregationResult(data={'inflow_top': _pack(top), 'outflow_top': _pack(bottom)})

    def _aggregate_unlock(self, as_of: str) -> AggregationResult:
        """D4 解禁 → 维度6：[as_of, as_of+10 交易日] 解禁日历（按 unlock_date 升序，X-11）。

        口径：公告制，仅含已披露的近端解禁（一般提前 3-5 个交易日），非完整前瞻；
        占比为推导值（unlock_shares ÷ 最新可得总股本，L 项回填）。
        """
        try:
            upcoming = self._next_n_dates(as_of, 10)
        except Exception as e:
            logger.warning(f'解禁窗口日历读取失败: {e}')
            upcoming = []
        end = upcoming[-1] if upcoming else as_of
        try:
            rows = self.db._query(
                """
                SELECT ts_code, stock_name, unlock_date, unlock_shares, unlock_ratio, unlock_type
                FROM share_unlock_calendar
                WHERE unlock_date >= %s AND unlock_date <= %s
                ORDER BY unlock_date ASC
                LIMIT 30
                """, (as_of, end))
        except Exception as e:
            logger.warning(f'解禁读取失败: {e}')
            rows = []
        if not rows:
            return AggregationResult(available=False,
                                     reason='share_unlock_calendar 窗口内无已披露记录')
        items = [{'ts_code': r[0], 'name': r[1], 'unlock_date': r[2],
                  'shares': _f(r[3]), 'ratio': _f(r[4]), 'type': r[5]} for r in rows]
        return AggregationResult(data={'window_end': end, 'items': items})

    def _aggregate_announcement(self, as_of: str) -> AggregationResult:
        """D5 公告摘要 → 维度5：当日按 ann_type 计数（剔除"其他"）+ 每类代表条目。"""
        try:
            rows = self.db._query(
                """
                SELECT ann_type, ts_code, stock_name, title
                FROM announcement_daily
                WHERE ann_date = %s AND ann_type <> '其他'
                ORDER BY ann_type, ts_code
                """, (as_of,))
        except Exception as e:
            logger.warning(f'公告读取失败: {e}')
            rows = []
        if not rows:
            return AggregationResult(available=False,
                                     reason='announcement_daily 当日无非"其他"分类公告')
        by_type: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            by_type.setdefault(r[0], []).append(
                {'ts_code': r[1], 'name': r[2], 'title': (r[3] or '')[:40]})
        counts = {k: len(v) for k, v in by_type.items()}
        samples = {k: v[:2] for k, v in by_type.items()}
        return AggregationResult(data={'counts': counts, 'samples': samples,
                                       'total': len(rows)})

    def _next_n_dates(self, as_of: str, n: int) -> List[str]:
        """as_of 之后 n 个交易日（升序，D4 解禁窗口）"""
        rows = self.db._query(
            "SELECT calendar_date FROM base_trade_calendar "
            "WHERE is_trading_day = 1 AND calendar_date > %s "
            "ORDER BY calendar_date ASC LIMIT %s", (as_of, n))
        return [str(r[0]).replace('-', '') for r in rows]

    # ------------------------------------------------------------------ 开发项 E：题材自算归因

    @staticmethod
    def _theme_candidates(cs_today: pd.DataFrame,
                          concept_map: Dict[str, List[str]],
                          exclude: Optional[List[str]] = None) -> Dict[str, str]:
        """涨停股题材候选（方案 4.5，推断口径）。

        对每只涨停股 s 的概念集（sina_gn 时点匹配由调用方保证）：
        - n_up(c) = 概念内当日涨停家数（聚集度）
        - pct(c) = 概念成员当日等权涨跌幅（有涨跌幅数据的成员）
        候选 = { c : n_up(c) ≥ 2 或 pct(c) ≥ 2% }，按 (n_up, pct) 降序取前 3 用"/"连接；
        无候选 → 不出串（渲染为"—"）。黑名单剔除宽基属性概念（config
        review.theme_exclude_concepts，子串匹配）。
        """
        if cs_today.empty or not concept_map:
            return {}
        blacklist = [str(x) for x in (exclude or [])]

        def _blocked(name: str) -> bool:
            return any(b in name for b in blacklist)

        change = pd.to_numeric(cs_today['change_rate'], errors='coerce')
        change_by_code = dict(zip(cs_today['ts_code'], change))
        n_up: Dict[str, int] = {}
        chg_sum: Dict[str, float] = {}
        chg_cnt: Dict[str, int] = {}
        for ts_code, concepts in concept_map.items():
            c = change_by_code.get(ts_code)
            c = float(c) if c is not None and pd.notna(c) else None
            for name in concepts:
                if _blocked(name):
                    continue
                if c is not None:
                    chg_sum[name] = chg_sum.get(name, 0.0) + c
                    chg_cnt[name] = chg_cnt.get(name, 0) + 1
        ups = cs_today[cs_today['_limit_up']]
        for ts_code in ups['ts_code']:
            for name in concept_map.get(ts_code, []):
                if not _blocked(name):
                    n_up[name] = n_up.get(name, 0) + 1
        pct = {name: chg_sum[name] / chg_cnt[name] for name in chg_sum if chg_cnt[name]}

        out: Dict[str, str] = {}
        for ts_code in ups['ts_code']:
            cands = []
            for name in concept_map.get(ts_code, []):
                if _blocked(name):
                    continue
                n = n_up.get(name, 0)
                p = pct.get(name)
                if n >= 2 or (p is not None and p >= 2.0):
                    cands.append((n, p if p is not None else -999.0, name))
            if cands:
                cands.sort(key=lambda x: (-x[0], -x[1]))
                out[str(ts_code)] = '/'.join(name for _, _, name in cands[:3])
        return out

    def _csv_path(self, as_of: str) -> Optional[Path]:
        out_dir = self.config.get('analysis', {}).get('output_dir', 'data/analysis')
        root = _repo_root()
        path = (Path(out_dir) if Path(out_dir).is_absolute() else root / out_dir) \
            / f'daily_review_{as_of}.csv'
        return path if path.exists() else None

    @staticmethod
    def _cross_check(cs_today: pd.DataFrame, csv_path: Path) -> Dict[str, Any]:
        """重算涨停家数 vs 链 CSV 涨停家数（§5.2 交叉校验）"""
        try:
            csv_df = pd.read_csv(csv_path, dtype=str, encoding='utf-8-sig')
            col = '是否涨停'
            if col not in csv_df.columns:
                return {'ok': None, 'note': f'CSV 无 {col} 列'}
            csv_up = int((csv_df[col] == '是').sum())
            recomputed = int(cs_today['_limit_up'].sum())
            return {'ok': csv_up == recomputed, 'csv_limit_up': csv_up,
                    'recomputed_limit_up': recomputed}
        except Exception as e:
            return {'ok': None, 'note': f'CSV 读取失败: {e}'}

    # ------------------------------------------------------------------ 转天候选

    def _build_candidates(self, cs_today: pd.DataFrame, as_of: str,
                          clusters: List[Dict[str, Any]],
                          seal_map: Dict[str, Dict[str, Any]],
                          institution: List[Dict[str, Any]]) -> Dict[str, Any]:
        """转天买入候选池（全量，谓词 candidate_rules 与链模块共用）。

        每类行带链同款理由串 label（如「7板/开板0次」「首板/创新药」），
        报告展示 top_n 截断、链 CSV 全量——同规则同数据则同集同序。
        去重优先级 R1 > R2 > R3 > R4；库表缺失时对应池为空（不硬造）。
        """
        rules = dict(BUY_RULES)
        rules.update(((self.config.get('review') or {}).get('tomorrow_plan')
                      or {}).get('buy_rules') or {})
        out: Dict[str, Any] = {'available': not cs_today.empty, 'rules': rules,
                               'R1': [], 'R2': [], 'R3': [], 'R4': []}
        if cs_today.empty:
            return out

        top_concepts = [str(c['concept']) for c in (clusters or [])[:3]]
        concept_map = self._concept_map(as_of)

        def _seal_fields(ts_code: str):
            s = seal_map.get(ts_code)
            return (s.get('open_count') if s else None,
                    s.get('first_seal_time') if s else None)

        used: set = set()

        # R1 梯队接力（全量涨停截面）
        lu = cs_today[cs_today['_limit_up']]
        r1_rows = []
        for _, r in lu.iterrows():
            oc, seal_time = _seal_fields(str(r['ts_code']))
            streak = int(r['_streak']) if pd.notna(r.get('_streak')) else 0
            if not is_r1(streak, bool(r.get('is_st')), oc, rules):
                continue
            r1_rows.append({
                'ts_code': str(r['ts_code']), 'code': str(r['stock_code']),
                'name': str(r['stock_name']), 'streak': streak,
                'open_count': oc, 'seal_time': seal_time,
                'amount': _f(r.get('amount')),
                'label': (f'{streak}板/封板数据缺失' if oc is None
                          else f'{streak}板/开板{int(oc)}次')})
        r1_rows.sort(key=lambda x: (-x['streak'], -(x['amount'] or 0)))
        for row in r1_rows:
            used.add(row['ts_code'])
        out['R1'] = r1_rows

        # R2 主线跟随（首板 × 概念全量成分命中聚集前列）
        r2_rows = []
        for _, r in lu.iterrows():
            ts_code = str(r['ts_code'])
            if ts_code in used:
                continue
            streak = int(r['_streak']) if pd.notna(r.get('_streak')) else 0
            concepts = concept_map.get(ts_code) or []
            if not is_r2(streak, bool(r.get('is_st')), concepts, top_concepts, rules):
                continue
            hit = next(c for c in top_concepts if c in set(concepts))
            r2_rows.append({
                'ts_code': ts_code, 'code': str(r['stock_code']),
                'name': str(r['stock_name']), 'streak': streak,
                'concepts': concepts, 'hit_concept': hit,
                'amount': _f(r.get('amount')),
                'label': f'首板/{hit}'})
        r2_rows.sort(key=lambda x: -(x['amount'] or 0))
        used.update(r['ts_code'] for r in r2_rows)
        out['R2'] = r2_rows

        # R3 趋势新高（全量新高截面）
        nh = cs_today[cs_today['_new_high']]
        r3_rows = []
        for _, r in nh.iterrows():
            ts_code = str(r['ts_code'])
            if ts_code in used:
                continue
            volume_surge = (_f(r.get('_volume_surge'))
                            if pd.notna(r.get('_volume_surge')) else None)
            change_rate = _f(r.get('change_rate'))
            if not is_r3(True, bool(r.get('is_st')), volume_surge,
                         change_rate, rules):
                continue
            r3_rows.append({
                'ts_code': ts_code, 'code': str(r['stock_code']),
                'name': str(r['stock_name']), 'amount': _f(r.get('amount')),
                'change_rate': change_rate, 'volume_surge': volume_surge,
                'label': f'新高/{change_rate:+.1f}%/{volume_surge:.1f}x'})
        r3_rows.sort(key=lambda x: -(x['amount'] or 0))
        used.update(r['ts_code'] for r in r3_rows)
        out['R3'] = r3_rows

        # R4 机构席位（lhb 机构净买前列；名称 ST 近似排除 + 不与前三类重复）
        r4_rows = []
        for r in institution or []:
            ts_code = str(r.get('ts_code') or '')
            name = str(r.get('name') or '')
            net = r.get('net')
            if not is_r4(net, 'ST' in name) or ts_code in used:
                continue
            r4_rows.append({
                'ts_code': ts_code, 'code': ts_code.split('.')[-1],
                'name': name or '—', 'net': _f(net),
                'label': f'机构净买{net / 1e8:+.2f}亿'})
        out['R4'] = r4_rows
        return out

    # ------------------------------------------------------------------ 维度5

    def _aggregate_fundamentals(self, as_of: str, prev_date: Optional[str]) -> AggregationResult:
        data: Dict[str, Any] = {}
        available = False
        disclosure = self._disclosure(as_of)
        if disclosure:
            data['disclosure'] = disclosure
            available = True
        forecasts = self._forecasts(as_of)
        if forecasts:
            data['forecasts'] = forecasts
            available = True
        valuation = self._stock_valuation(as_of)
        if valuation:
            data['stock_valuation'] = valuation
            available = True
        index_val = self._index_valuation(as_of)
        if index_val:
            data['index_valuation'] = index_val
            available = True
        return AggregationResult(available=available,
                                 reason='' if available else '业绩估值各表为空（披露/预告/估值未采集）',
                                 data=data)

    def _disclosure(self, as_of: str) -> Dict[str, Any]:
        """披露日历：未来 5/10 个交易日预约披露家数 + 当日实际披露家数"""
        try:
            future = self.db._query(
                "SELECT calendar_date FROM base_trade_calendar "
                "WHERE is_trading_day = 1 AND calendar_date > %s "
                "ORDER BY calendar_date LIMIT 10", (_dot(as_of),))
            rows = self.db._query(
                "SELECT plan_disclosure_date, actual_disclosure_date "
                "FROM base_disclosure_calendar "
                "WHERE plan_disclosure_date IS NOT NULL "
                "   OR actual_disclosure_date IS NOT NULL")
        except Exception as e:
            logger.warning(f'披露日历读取失败: {e}')
            return {}
        if not rows:
            return {}
        future_dates = [_plain(r[0]) for r in future]
        plan = {}
        actual_today = 0
        for plan_date, actual_date in rows:
            p = _plain(plan_date) if plan_date else None
            if p:
                plan[p] = plan.get(p, 0) + 1
            if actual_date and _plain(actual_date) == as_of:
                actual_today += 1
        return {'next_5': sum(plan.get(d, 0) for d in future_dates[:5]),
                'next_10': sum(plan.get(d, 0) for d in future_dates[:10]),
                'actual_today': actual_today}

    def _forecasts(self, as_of: str) -> Dict[str, Any]:
        """当日新增业绩预告（分类计数 + 代表性条目）"""
        try:
            rows = self.db._query(
                "SELECT stock_name, forecast_type, summary FROM performance_forecast "
                "WHERE ann_date = %s LIMIT 50", (as_of,))
        except Exception as e:
            logger.warning(f'业绩预告读取失败: {e}')
            return {}
        if not rows:
            return {}
        by_type: Dict[str, int] = {}
        for _, ftype, _ in rows:
            key = ftype or '未分类'
            by_type[key] = by_type.get(key, 0) + 1
        items = [{'name': r[0], 'type': r[1], 'summary': (r[2] or '')[:80]} for r in rows[:5]]
        return {'count': len(rows), 'by_type': by_type, 'items': items}

    def _stock_valuation(self, as_of: str) -> Dict[str, Any]:
        """全市场 PE/PB 分布与极端值（stock_valuation_daily）"""
        try:
            rows = self.db._query(
                "SELECT ts_code, pe_ttm, pb FROM stock_valuation_daily "
                "WHERE trade_date = %s", (as_of,))
        except Exception as e:
            logger.warning(f'个股估值读取失败: {e}')
            return {}
        if not rows:
            return {}
        df = pd.DataFrame(rows, columns=['ts_code', 'pe_ttm', 'pb'])
        pe = pd.to_numeric(df['pe_ttm'], errors='coerce').dropna()
        pe = pe[(pe > 0) & (pe < 500)]
        pbv = pd.to_numeric(df['pb'], errors='coerce').dropna()
        pbv = pbv[(pbv > 0) & (pbv < 100)]
        extremes = df.assign(pe=pd.to_numeric(df['pe_ttm'], errors='coerce')) \
            .dropna(subset=['pe']).query('0 < pe < 500') \
            .sort_values('pe')['ts_code'].head(self.top_n).tolist()
        return {'pe_count': int(len(pe)), 'pb_count': int(len(pbv)),
                'pe_quantiles': {q: _f(pe.quantile(q)) for q in (0.1, 0.25, 0.5, 0.75, 0.9)},
                'pb_quantiles': {q: _f(pbv.quantile(q)) for q in (0.1, 0.25, 0.5, 0.75, 0.9)},
                'lowest_pe_stocks': extremes}

    def _index_valuation(self, as_of: str) -> List[Dict[str, Any]]:
        """指数估值（最新可得日期，pe_static 口径注明无 TTM）"""
        try:
            rows = self.db._query(
                "SELECT trade_date, index_name, pe_static, pe_ttm FROM index_valuation_daily "
                "WHERE trade_date = (SELECT MAX(trade_date) FROM index_valuation_daily) "
                "ORDER BY index_name")
        except Exception as e:
            logger.warning(f'指数估值读取失败: {e}')
            return []
        return [{'date': r[0], 'name': r[1], 'pe_static': _f(r[2]), 'pe_ttm': _f(r[3])}
                for r in rows]


# ------------------------------------------------------------------ 工具

def cs_prev_empty() -> pd.DataFrame:
    return pd.DataFrame()


def _rows(df: pd.DataFrame, n: int) -> List[Dict[str, Any]]:
    """截面切片 → 明细行（代码/名称/涨跌幅/连板/放量倍数/成交额/ST + ts_code 供交叉标记）"""
    if df.empty:
        return []
    out = []
    for _, r in df.head(n).iterrows():
        out.append({
            'code': str(r['stock_code']),
            'ts_code': str(r['ts_code']),
            'name': str(r['stock_name']),
            'change_rate': _f(r.get('change_rate')),
            'streak': int(r['_streak']) if '_streak' in r and pd.notna(r['_streak']) else 0,
            'volume_surge': _f(r.get('_volume_surge')),
            'amount': _f(r.get('amount')),
            'is_st': bool(r.get('is_st')),
        })
    return out


def _safe_sub(a: Any, b: Any) -> Optional[float]:
    """a - b（任一为 None 返回 None，不硬造）"""
    if a is None or b is None:
        return None
    return float(a) - float(b)


def _f(v: Any) -> Optional[float]:
    try:
        if v is None or (isinstance(v, float) and v != v):
            return None
        return round(float(v), 4)
    except (TypeError, ValueError):
        return None


def _pct2(ratio: Any) -> Optional[float]:
    """收益率小数 → 百分数两位小数（0.0037 → 0.37；None/非法 → None）"""
    try:
        if ratio is None:
            return None
        return round(float(ratio) * 100, 2)
    except (TypeError, ValueError):
        return None


def _count_gt(sorted_dates: List[str], d: str) -> int:
    """升序日期列表中 > d 的元素个数（市场交易日滞后计数，Q8 对齐判定）"""
    return len(sorted_dates) - bisect_right(sorted_dates, d)


def _dot(ymd: str) -> str:
    return f'{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}'


def _plain(v: Any) -> str:
    return str(v).replace('-', '')


def _repo_root() -> Path:
    from ..sync.report_writer import resolve_repo_root
    return resolve_repo_root()
