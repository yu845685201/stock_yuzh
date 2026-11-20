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

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, time as dtime
from bisect import bisect_right
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from ..analysis.candidate_rules import (
    BUY_RULES,
    high_board_longyin_recent,
    is_r1,
    is_r2,
    is_r3,
    is_r4,
    r1_reject_reason,
    r1_tier,
    score_candidate,
)
from ..analysis.attribution_rules import FALLBACK_LABEL, pick_attribution
from ..analysis.data_reader import DataReader
from .cross_section import (
    annotate_panel,
    cross_section_at,
    sentiment_metrics,
    suppress_no_limit_window,
)
from . import money_effect as _money_effect
from .sentiment_calibration import quantile_calibration, quantile_state

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


# P4-6/P4-1：lhb 发布时点门控移至 analysis.candidate_veto 单一出处（报告层与
# 链侧 env 装配共用）；此处 re-export 保持 P4-6 既有导入路径兼容
from ..analysis.candidate_veto import (LHB_PUBLISH_TIME,  # noqa: E402,F401
                                       lhb_published,
                                       load_candidate_veto_map)


def _values_match(mine: Any, theirs: Any) -> bool:
    """指标对拍的逐值比较：数值列容忍展示舍入（≤0.05），其余字符串精确匹配。"""
    try:
        return abs(float(mine) - float(theirs)) <= 0.05
    except (TypeError, ValueError):
        return str(mine) == str(theirs)


def _disp_rate(v: Any, nd: int = 1) -> str:
    """小数口径比率（如炸板率 0.2222）→ 百分数字符串（W1）；None→'—'（不带 %）。"""
    return f'{float(v):.{nd}%}' if v is not None else '—'


def _disp_pct(v: Any, nd: int = 2) -> str:
    """已为百分数值的指标（如溢价均值 3.38）→ 带 % 尾字符串；None→'—'。"""
    return f'{float(v):.{nd}f}%' if v is not None else '—'


class _ConfigShim:
    """dict 配置 → ConfigManager 形态（assemble_valuation 只需 load_config）。"""

    def __init__(self, cfg: Dict[str, Any]):
        self._cfg = cfg

    def load_config(self) -> Dict[str, Any]:
        return self._cfg


def _cand_score(big_net: Optional[float], volume_surge: Optional[float],
                cluster_count: Optional[int],
                fundamental: Optional[float] = None,
                event: Optional[float] = None) -> Optional[float]:
    """候选五维评分的可得子集（P1-7 §4.6 D；P4-2 补全基本面/事件两维）。

    五维：资金（大单净流入）/ 技术（放量倍数）/ 板块（聚集家数）/
    基本面（PEG 锚，``fundamental_dim``）/ 事件（归因极性锚，``event_dim``）；
    缺维按剩余权重归一化（不硬造），全缺 → None。补全后资金 0.35 权重
    恢复设计含义（原最多三维时被归一化放大至 ~0.44）。
    """
    dims: Dict[str, Optional[float]] = {
        'money': (min(100.0, max(0.0, (big_net or 0.0) / 1e8 * 50.0))
                  if big_net is not None else None),
        'tech': (min(100.0, float(volume_surge) * 25.0)
                 if volume_surge is not None else None),
        'sector': (min(100.0, float(cluster_count) * 20.0)
                   if cluster_count is not None else None),
        'fundamental': fundamental,
        'event': event,
    }
    if all(v is None for v in dims.values()):
        return None
    res = score_candidate(dims)
    return res['score']


def fundamental_dim(digest: Any) -> Optional[float]:
    """P4-2 基本面维（0~100，初版线性映射**未校准**）：PEG 锚。

    0 < PEG ≤ 1 → 90 / ≤ 2 → 70 / ≤ 3 → 50 / > 3 → 30；
    缺失或 ≤ 0（亏损/负增长时 PEG 无意义）→ None（不硬造）。
    """
    peg = getattr(digest, 'peg_value', None)
    if peg is None or float(peg) <= 0:
        return None
    peg = float(peg)
    return 90.0 if peg <= 1 else 70.0 if peg <= 2 else 50.0 if peg <= 3 else 30.0


def event_dim(announcements: Optional[List[dict]]) -> Optional[float]:
    """P4-2 事件维（0~100，初版线性映射**未校准**）：归因极性锚。

    利好 80 / 利空 20 / 其他极性 40 / 无利好显式结论（窗口内未识别催化）
    10；窗口内无公告 → None（缺维，与"有公告但归因为无利好"区分）。
    """
    if not announcements:
        return None
    label, polarity, _ = pick_attribution(announcements)
    if label == FALLBACK_LABEL:
        return 10.0
    if polarity == '利好':
        return 80.0
    if polarity == '利空':
        return 20.0
    return 40.0


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

    #: P3-2（C5）聚合缓存版本号：聚合层任何影响产出的代码变更时手动 +1（防陈旧缓存）
    #: v3：P4-5 判据表第 4 行（涨跌家数比）+ P4-1 VETO map 卖一占比接线（两处聚合产出变更）
    #: v4：P4-2 五维评分补全（候选行 score 计入基本面/事件两维）
    #: v5：P4-1 反抽板 veto（近 N 日高位板放量长阴，R1/R2 行 veto 列可新增理由）
    #: v6：指标面板窗口 133→522 与链对齐（指标状态/分布/对拍输出变更）
    #: v7：A50 日频优先（overseas 行 kline 覆盖 + a50_source 键）
    #: v8：A50 日频 as_of 截断（防前视——v7 历史缓存含未来值须失效）
    #: v9：out 增 next_trade_date（W7 周末缝提示）——旧缓存缺键须失效
    #: v10：_next_trade_date 修 calendar_date TEXT 比较格式（_dot 归一），None→真值
    #: v11：_aggregate_unlock 同源 _dot 修复（X1）——v10 缓存 unlock=available=False 须失效
    #: v12：X2 raw-R1 命中播种 used（弱档日 R2/R3/R4 去重对齐链侧）——聚合产物变更
    #: v13：V4.2 批三——turnover.series 新增（T3）+ 判据 expr 去 P4-5（T4）+
    #:      news reason 三态分支（T6a），聚合产物变更一次升版
    #: v14：V4.3——candidates 增 R1_filtered（A2）、cycle 增 nearest_miss（B2）、
    #:      chan items 增 close（C1）、consec_premium 增 best（B3），聚合产物变更升版
    #: v15：指标面板窗口起点去 -1（off-by-one，20261027 对拍 2 处失配共因）——
    #:      七模块状态/交叉榜/parity 全部重算，聚合产物变更升版
    #: v16：卖一占比 VETO 修复（LIKE 裸 % 致 psycopg2 插值失败，维度自 P4-1 起
    #:      静默失效，20261028 修复）——candidates 否决输入变更，聚合产物升版
    #: v17：市场宽度判据升格定位（X4 用户决策 20261028：阈值 2.0→2.5、role
    #:      reference→position、回暖/震荡偏暖标注）——sentiment 判据表/basis
    #:      变更，聚合产物升版
    AGG_CACHE_VERSION = 'v17-20261028'

    def _agg_cache_path(self, as_of: str):
        """聚合结果缓存路径（<repo>/tmp/agg_cache/{as_of}_{ver}.pkl）；关闭返回 None。"""
        if not bool((self.config.get('review') or {}).get('agg_cache', True)):
            return None
        root = Path(__file__).resolve().parents[3]
        d = root / 'tmp' / 'agg_cache'
        d.mkdir(parents=True, exist_ok=True)
        return d / f'{as_of}_{self.AGG_CACHE_VERSION}.pkl'

    def aggregate(self, as_of: str) -> Dict[str, Any]:
        """全量聚合：加载 60d 面板 → 截面（今/昨）→ 六维结果。

        P3-2（C5）：结果 pickle 落盘（config review.agg_cache，默认开），generate 与
        digest 复用同一份聚合（缠论 520d 面板双跑成本消除）；键含代码版本号，
        读取异常回退直算。

        Returns:
            {as_of, prev_date, window_start, trading_days, panel_rows,
             market_env, sentiment, sector, movers, fundamentals,
             cross_check} 各键为 AggregationResult 或原始 dict。
        """
        cache_file = self._agg_cache_path(as_of)
        if cache_file is not None and cache_file.exists():
            try:
                import pickle
                cached = pickle.loads(cache_file.read_bytes())
                logger.info('聚合缓存命中: %s', cache_file.name)
                return cached
            except Exception as e:  # noqa: BLE001 —— 缓存坏即回退直算
                logger.warning('聚合缓存读取失败（回退直算）: %r', e)
        out = self._aggregate_uncached(as_of)
        if cache_file is not None:
            try:
                import pickle
                cache_file.write_bytes(pickle.dumps(out))
            except Exception as e:  # noqa: BLE001 —— 落盘失败不影响结果
                logger.warning('聚合缓存写入失败: %r', e)
        return out

    def _aggregate_uncached(self, as_of: str) -> Dict[str, Any]:
        """aggregate 的直算本体（缓存未命中时执行）。"""
        start = self.reader.nth_trading_day_back(as_of, 62)
        cols = ['stock_name', 'open', 'high', 'low', 'close', 'preclose',
                'change_rate', 'volume', 'amount', 'is_st', 'raw_close']
        df = self.reader.load_kline(cols, as_of, start_date=start)
        if df.empty:
            raise ValueError(f'锚定日 {as_of} 窗口内无日K数据')
        prev_date = self.reader.nth_trading_day_back(as_of, 1)

        panel = annotate_panel(df)
        # 新股上市首日不判定（与链模块口径对齐；北交所 N 前缀兜底）
        list_dates = {s.ts_code: s.list_date for s in self.reader.load_stock_universe()}
        # P0-2：注册制次新前 5 交易日（北交所首日）不判定、不计分母
        panel = suppress_no_limit_window(panel, list_dates)
        cs_today = cross_section_at(panel, as_of)
        cs_prev = cross_section_at(panel, prev_date) if prev_date else cs_prev_empty()

        out: Dict[str, Any] = {
            'as_of': as_of,
            'prev_date': prev_date,
            'next_trade_date': self._next_trade_date(as_of),
            'window_start': start,
            'panel_rows': int(len(df)),
            'stock_count': int(cs_today['ts_code'].nunique()) if not cs_today.empty else 0,
        }
        out['sentiment'] = self._aggregate_sentiment(cs_today, cs_prev, as_of=as_of)
        out['money_effect'] = self._aggregate_money_effect(cs_today, cs_prev)
        out['verification'] = self._aggregate_verification(
            as_of, prev_date, cs_today, (out['sentiment'].data or {}))
        out['market_env'] = self._aggregate_market_env(panel, cs_today, as_of, prev_date)
        out['sector'] = self._aggregate_sector(panel, cs_today, cs_prev, as_of)
        out['movers'] = self._aggregate_movers(cs_today, as_of)
        # 量价资金维度（量价资金方案 V1.1 §3.3）：量价四象限双锚 / 板块资金聚合 / 资金锚
        out['volume_price'] = self._volume_price(as_of)
        out['sector_money'] = self._sector_money(as_of)
        out['capital_anchors'] = self._capital_anchors(as_of)
        out['fundamentals'] = self._aggregate_fundamentals(as_of, prev_date)
        # 开发项 D：五类已采集数据进报告（表空 → available=False → 渲染"⏳ 待补"）
        out['margin'] = self._aggregate_margin(as_of)
        out['lhb'] = self._aggregate_lhb(as_of)
        out['money_flow'] = self._aggregate_money_flow(as_of)
        out['unlock'] = self._aggregate_unlock(as_of)
        out['announcement'] = self._aggregate_announcement(as_of)
        # 消息催化（四期方案 §4）：当日要闻 + 聚集前列概念关键词命中（市场叙事口径）
        out['news'] = self._aggregate_news(as_of)
        # 技术指标状态（P0-1 §3.1.7）：133d 面板复算七模块，与 daily_review 链
        # 指标列同一实现两入口（indicator_panel 调同一批模块，对拍保一致）
        out['indicators'] = self._aggregate_indicators(as_of)
        # 个股估值摘要（P0-4 §3.3.3）：与链 valuation_digest 模块同一
        # assemble_valuation 入口（原则 P6 两入口同一实现）
        out['valuation_digest'] = self._aggregate_valuation_digest(as_of)
        # 转天买入候选池（第六章预案 × daily_review 链 R 列同口径；candidate_rules
        # 谓词共用，全量不截断，展示层按 top_n 截断——与链 CSV R 列同集同序）
        clusters = ((out['sector'].data or {}).get('limit_up_clusters')
                    if isinstance(out['sector'], AggregationResult) else None) or []
        lhb_data = (out['lhb'].data if isinstance(out['lhb'], AggregationResult) else {}) or {}
        sm_data = (out['sector_money'].data if isinstance(out['sector_money'], AggregationResult) else {}) or {}
        out['candidates'] = self._build_candidates(
            cs_today, as_of, clusters,
            self._seal_map_of_day(as_of),
            lhb_data.get('institution') or [],
            top3_members=sm_data.get('top3_members') or set(),
            recent_windows=self._recent_windows(panel))
        # 缠论背驰候选（2026-10-24 引入）：520d 面板独立复算，与链
        # chan_divergence_buy 模块同一实现两入口（chan.scan.scan_stock_anchor）
        out['chan'] = self._aggregate_chan(as_of)
        return out

    # ------------------------------------------------------------------ 维度1

    def _aggregate_market_env(self, panel: pd.DataFrame, cs_today: pd.DataFrame,
                              as_of: str, prev_date: Optional[str]) -> AggregationResult:
        indices = self._index_positions(as_of)
        turnover = self._turnover_delta(panel, as_of, prev_date)
        adv_dec = self._advance_decline(cs_today)
        overseas_all = self._overseas_all()
        overseas = [r for r in overseas_all if r['name'] in _OVERSEAS_SNAPSHOT_WHITELIST]
        # A50 日频优先（P1-5 采集侧收口，附录 C.5）：overseas_kline_day 有行时
        # 以日K两根收盘自算隔夜涨跌幅覆盖快照行；缺行/读失败回退快照（P1-5 降级）
        a50 = self._a50_from_kline(as_of)
        if a50:
            overseas = [a50 if o['name'] == '富时A50期货' else o
                        for o in overseas] or [a50]
            if not any(o['name'] == '富时A50期货' for o in overseas):
                overseas.insert(0, a50)
        data = {'indices': indices, 'turnover': turnover,
                'advance_decline': adv_dec, 'overseas': overseas,
                'a50_source': 'kline' if a50 else 'snapshot',
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

    def _volume_price(self, as_of: str) -> AggregationResult:
        """量价四象限（双锚：上证+中证全指，config review.volume_price.index_codes）。

        每锚：当日成交额 vs 前 5 日均额比值 r5 分档（放量≥1.1/缩量≤0.9/温和）×
        当日涨跌（±0.3% 算平）→ 四象限判定；附 5 日均量趋势（递增/递减）。
        阈值判据初版待校准（量价资金方案 V1.1 §3.3①）。
        """
        codes = list((self.config.get('review') or {}).get('volume_price', {})
                     .get('index_codes') or ['000001', '000985'])
        try:
            rows = self.db._query(
                """
                SELECT index_code, index_name, trade_date, amount, change_rate
                FROM trade_index_info
                WHERE index_code = ANY(%s) AND trade_date <= %s
                  AND amount IS NOT NULL
                ORDER BY index_code, trade_date
                """, (codes, as_of))
        except Exception as e:
            logger.warning(f'量价序列读取失败: {e}')
            return AggregationResult(available=False, reason='trade_index_info 为空')
        if not rows:
            return AggregationResult(available=False, reason='trade_index_info 为空')
        series: Dict[str, Dict[str, Any]] = {}
        for code, name, d, amt, chg in rows:
            series.setdefault(code, {'name': name,
                                     'amount': [], 'chg': [], 'date': []})
            series[code]['amount'].append(float(amt))
            series[code]['chg'].append(float(chg) if chg is not None else 0.0)
            series[code]['date'].append(str(d))
        out = []
        for code in codes:
            sdata = series.get(code)
            if not sdata or len(sdata['amount']) < 6:
                continue
            amounts, chgs = sdata['amount'], sdata['chg']
            today_amt = amounts[-1]
            prev5 = amounts[-6:-1]
            r5 = today_amt / (sum(prev5) / len(prev5)) if sum(prev5) > 0 else None
            # 5 日均量趋势（前 5 日均 vs 前 5 日前的 5 日均）
            if len(amounts) >= 11:
                m1 = sum(amounts[-6:-1]) / 5
                m0 = sum(amounts[-11:-6]) / 5
                trend = '递增' if m1 > m0 * 1.05 else ('递减' if m1 < m0 * 0.95 else '平稳')
            else:
                trend = None
            chg_today = chgs[-1]
            vol_state = ('放量' if r5 is not None and r5 >= 1.1
                         else '缩量' if r5 is not None and r5 <= 0.9 else '温和') if r5 else None
            direction = ('涨' if chg_today > 0.3 else
                         '跌' if chg_today < -0.3 else '平')
            quad = (f'{vol_state}{direction}' if vol_state and direction != '平'
                    else (f'缩量{direction}' if vol_state == '缩量' else
                          f'放量{direction}' if vol_state == '放量' else '温和盘整')
                    if vol_state else None)
            verdict = {
                '放量涨': '健康，趋势可信', '缩量涨': '背离，反弹缺承接',
                '放量跌': '恐慌抛售', '缩量跌': '阴跌磨底',
            }.get(quad, '观望' if direction == '平' else None)
            out.append({
                'code': code, 'name': sdata['name'],
                'date': sdata['date'][-1],
                'amount_yi': round(today_amt / 1e8, 0),
                'r5': round(r5, 3) if r5 is not None else None,
                'chg': round(chg_today, 2),
                'volume_state': vol_state, 'direction': direction,
                'quad': quad, 'verdict': verdict, 'trend_5d': trend,
            })
        return AggregationResult(available=bool(out), data={'anchors': out})

    def industry_map(self, as_of: str) -> Dict[str, str]:
        """ts_code → 行业名归属映射（C2/P0-6：同数据同结果）。

        sw_2021 优先、sina_hy 兜底（config review.industry_sources 声明的
        优先级在此落地）：SQL 稳定排序后逐股取首行，不再依赖 DB 返回顺序。
        结果按 as_of 缓存，行业榜（_sector_money）与缠论候选行业列
        （_aggregate_chan）共用同一归属结果。
        """
        cached = getattr(self, '_industry_map_cache', None)
        if cached is not None and cached[0] == as_of:
            return cached[1]
        try:
            ind_rows = self.db._query(
                """
                SELECT m.ts_code, c.concept_name, c.source FROM sector_concept_map m
                JOIN sector_concept c ON c.concept_code = m.concept_code
                WHERE c.source IN ('sw_2021', 'sina_hy')
                  AND (m.in_date IS NULL OR m.in_date <= %s)
                  AND (m.out_date IS NULL OR m.out_date > %s)
                ORDER BY m.ts_code,
                         CASE c.source WHEN 'sw_2021' THEN 0 ELSE 1 END,
                         c.concept_code
                """, (as_of, as_of))
        except Exception as e:
            logger.warning(f'行业映射读取失败: {e}')
            ind_rows = []
        mapping: Dict[str, str] = {}
        sources: Dict[str, str] = {}
        for ts_code, ind, src in ind_rows:
            if ind and str(ts_code) not in mapping:
                mapping[str(ts_code)] = str(ind)
                sources[str(ts_code)] = str(src)
        self._industry_map_cache = (as_of, mapping)
        self._industry_source_cache = (as_of, sources)
        return mapping

    def industry_source_map(self, as_of: str) -> Dict[str, str]:
        """ts_code → 行业归属来源（'sw_2021'/'sina_hy'；评价-3 概念板打标用）。

        与 industry_map 同一次查询缓存；未加载时返回 {}。
        """
        self.industry_map(as_of)
        cached = getattr(self, '_industry_source_cache', None)
        return dict(cached[1]) if cached is not None and cached[0] == as_of else {}

    def _sector_money(self, as_of: str) -> AggregationResult:
        """板块资金流向（行业 sw_2021 聚合大单净额；概念 sina_gn Top5）。

        **口径（P1-7 动态）**：行级口径受 money_flow_daily.calib_note 约束——
        20260917 起为全市场分笔分桶（'分笔分桶:全市场'）；更早 as_of 为异动
        子集近似口径（'近似口径:分笔分桶'，系统性偏向异动板块）。当日
        calib_note 随 data['calib_note'] 带回，渲染层据此动态生成注记；
        子集覆盖率随行展示。
        """
        try:
            rows = self.db._query(
                """
                SELECT m.ts_code, m.big_net_amt
                FROM money_flow_daily m
                WHERE m.trade_date = %s AND m.big_net_amt IS NOT NULL
                """, (as_of,))
        except Exception as e:
            logger.warning(f'板块资金读取失败: {e}')
            return AggregationResult(available=False, reason='money_flow_daily 为空')
        if not rows:
            return AggregationResult(available=False,
                                     reason='money_flow_daily 当日无数据')
        try:
            total_rows = self.db._query(
                "SELECT COUNT(*) FROM his_kline_day WHERE trade_date = %s "
                "AND volume > 0", (as_of,))
            traded = int(total_rows[0][0]) if total_rows else 0
        except Exception:
            traded = 0
        mf_map = {str(t): float(net) for t, net in rows}
        # 行业归属（sw_2021 主 + sina_hy 回退，稳定排序+缓存共用，C2/P0-6）
        ind_map = self.industry_map(as_of)
        src_map = self.industry_source_map(as_of)
        industry_net: Dict[str, float] = {}
        industry_in: Dict[str, int] = {}
        for ts_code, ind in ind_map.items():
            if ts_code not in mf_map:
                continue
            net = mf_map[ts_code]
            industry_net[ind] = industry_net.get(ind, 0.0) + net
            if net > 0:
                industry_in[ind] = industry_in.get(ind, 0) + 1
        ranked = sorted(industry_net.items(), key=lambda kv: -kv[1])
        # 评价-3：纯 sina_hy 成分板 = 概念板，渲染层打（概念）标签
        def _is_concept(board: str) -> bool:
            members = [ts for ts, ind in ind_map.items() if ind == board]
            return bool(members) and all(src_map.get(ts) != 'sw_2021'
                                         for ts in members)
        concept_boards = {k: _is_concept(k) for k, _ in ranked}
        top = [{'industry': k, 'net_yi': round(v / 1e8, 2),
                'in_count': industry_in.get(k, 0),
                'is_concept': concept_boards.get(k, False)} for k, v in ranked[:5]]
        bottom = [{'industry': k, 'net_yi': round(v / 1e8, 2),
                   'in_count': industry_in.get(k, 0),
                   'is_concept': concept_boards.get(k, False)}
                  for k, v in ranked[-5:][::-1]]
        # 净流入 Top3 行业的成分股集（候选证据标记"板块"用；量价资金 V1.1）
        top3_industries = {k for k, _ in ranked[:3]}
        top3_members = {ts for ts, ind in ind_map.items()
                        if ind in top3_industries}
        # P1-7（评价-4）：当日行级口径随数据带回，渲染层按此动态生成注记
        # （20260917 起全市场分笔分桶；更早 as_of 为异动子集近似口径）
        try:
            calib_rows = self.db._query(
                "SELECT DISTINCT calib_note FROM money_flow_daily "
                "WHERE trade_date = %s LIMIT 2", (as_of,))
            calib_note = str(calib_rows[0][0]) if calib_rows else ''
        except Exception:
            calib_note = ''
        return AggregationResult(data={
            'industry_top': top, 'industry_bottom': bottom,
            'top3_members': top3_members,
            'subset_count': len(mf_map), 'traded_count': traded,
            'coverage': round(len(mf_map) / traded * 100, 1) if traded else None,
            'calib_note': calib_note,
        })

    def _capital_anchors(self, as_of: str) -> AggregationResult:
        """市场级资金锚（P3）：北向总额（活跃度，无方向）+ 南向净额 + ETF 宽基份额变动。

        口径（量价资金方案 V1.1 §5）：北向仅成交总额禁止解读净额；南向净买入=Buy−Sell；
        ETF 份额变动为 T 日快照差分（两所不足 2 日积累时标待积累）。
        """
        data: Dict[str, Any] = {}
        try:
            rows = self.db._query(
                """
                SELECT channel, total_turnover, buy_turnover, sell_turnover
                FROM hkex_connect_daily WHERE trade_date = %s
                """, (as_of,))
        except Exception as e:
            logger.warning(f'互联互通读取失败: {e}')
            rows = []
        ch = {str(r[0]): r for r in rows}
        if ch:
            north_total = sum(float(ch[c][1]) for c in ('north_sse', 'north_szse') if c in ch)
            south_net = sum(
                float(ch[c][2] or 0) - float(ch[c][3] or 0)
                for c in ('south_sse', 'south_szse') if c in ch)
            data['hkex_date'] = as_of
            data['north_total_yi'] = round(north_total / 100, 1)   # 百万 → 亿
            data['south_net_yi'] = round(south_net / 100, 1)
        else:
            data['hkex_date'] = None
        # ETF 宽基份额变动（头部宽基当日 vs 前一可得日，两所合计，亿份）
        try:
            etf_rows = self.db._query(
                """
                SELECT trade_date, SUM(shares_wan) FROM etf_scale_daily
                WHERE fund_code = ANY(%s) AND trade_date <= %s
                GROUP BY trade_date ORDER BY trade_date DESC LIMIT 2
                """, (['510300', '510050', '588000', '159915', '159919',
                       '510500', '159922'], as_of))
            if len(etf_rows) >= 2:
                d1, v1 = str(etf_rows[0][0]), float(etf_rows[0][1])
                d0, v0 = str(etf_rows[1][0]), float(etf_rows[1][1])
                data['etf'] = {'date': d1, 'prev_date': d0,
                               'chg_yi_fen': round((v1 - v0) / 10000, 2)}
            else:
                data['etf'] = {'date': None, 'chg_yi_fen': None,
                               'note': '待积累（需 ≥2 个交易日）'}
        except Exception as e:
            logger.warning(f'ETF 宽基读取失败: {e}')
            data['etf'] = None

        # ---------- 第三阶段新锚（2026-10-23） ----------
        # 宏观指标锚（低频背景；缺失指标跳过不硬造）
        try:
            data['macro'] = self._macro_anchor()
        except Exception as e:
            logger.warning(f'宏观指标锚读取失败: {e}')
            data['macro'] = None
        # ETF 期权情绪锚（逐标的 PC 率/OI；SSE 口径，衔接 ETF 规模章）
        try:
            data['option'] = self._option_anchor(as_of)
        except Exception as e:
            logger.warning(f'期权锚读取失败: {e}')
            data['option'] = None
        # 北向个股明细锚（Top N 持仓；披露止于 20240816——数据边界如实标注）
        try:
            data['north_detail'] = self._north_detail_anchor(as_of)
        except Exception as e:
            logger.warning(f'北向个股明细锚读取失败: {e}')
            data['north_detail'] = None

        # P1-6 资金锚质量标签（评价-9/B5）：实测/代理/停旧值 + 综合置信度。
        # 规则表 config review.money_anchor_tags；快照日期 < as_of 一律覆盖为停旧值
        tag_rules = dict(((self.config.get('review') or {})
                          .get('money_anchor_tags')) or {})
        tags = {
            'south_net': tag_rules.get('south_net', '实测'),
            'north_total': tag_rules.get('north_total', '代理'),
            'etf': tag_rules.get('etf', '代理'),
            'macro': tag_rules.get('macro', '实测'),
            'option': tag_rules.get('option', '实测'),
            'north_detail': tag_rules.get('north_detail', '停旧值'),
        }
        hkex_date = str(data.get('hkex_date') or '')
        if hkex_date and hkex_date < str(as_of):
            tags['south_net'] = tags['north_total'] = '停旧值'
        etf_date = str((data.get('etf') or {}).get('date') or '')
        if etf_date and etf_date < str(as_of):
            tags['etf'] = '停旧值'
        data['anchor_tags'] = tags
        # 综合置信度按决策锚（南向/北向总额/ETF/宏观/期权）推导；
        # north_detail 恒为停旧值（披露止于 20240816），不参与综合推导只在行内标注
        decision_vals = [tags[k] for k in
                         ('south_net', 'north_total', 'etf', 'macro', 'option')]
        data['confidence'] = ('低' if '停旧值' in decision_vals
                              else ('高' if all(v == '实测' for v in decision_vals)
                                    else '中'))
        return AggregationResult(available=bool(data), data=data)

    def _macro_anchor(self) -> Optional[Dict[str, Any]]:
        """宏观指标最新值快照（第三阶段-1；报告层只读渲染）。

        缺表/无数据 → None（章节显示「宏观数据未采集」提示，不硬造）。
        """
        try:
            rows = self.db._query(
                """
                SELECT DISTINCT ON (indicator_code)
                       indicator_code, value, unit, trade_date
                FROM macro_indicator_daily
                ORDER BY indicator_code, trade_date DESC
                """)
        except Exception as e:
            logger.warning(f'宏观指标查询失败: {e}')
            return None
        if not rows:
            return None
        from ..data_sources.macro_source import INDICATOR_META
        items = []
        for code, value, unit, d in rows:   # DataReader._query：元组行
            code = str(code)
            name = INDICATOR_META.get(code, (code,))[0]
            items.append({'code': code, 'name': name,
                          'value': float(value) if value is not None else None,
                          'unit': unit, 'trade_date': str(d)})
        # 展示顺序：利率 → 量 → 价 → 景气
        order = ['LPR_1Y', 'LPR_5Y', 'SHIBOR_ON', 'SHIBOR_1W', 'SHIBOR_1M',
                 'M1_YOY', 'M2_YOY', 'SOCIAL_FINANCING', 'PMI_MFG',
                 'PMI_NONMFG', 'CPI_YOY', 'PPI_YOY']
        items.sort(key=lambda x: (order.index(x['code'])
                                  if x['code'] in order else 99, x['code']))
        return {'items': items, 'max_date': max(i['trade_date'] for i in items)}

    def _option_anchor(self, as_of: str) -> Optional[Dict[str, Any]]:
        """ETF 期权情绪锚（第三阶段-2：逐标的 PC 率/未平仓；无数据 → None）"""
        def _query(date: str):
            return self.db._query(
                """
                SELECT underlying_code, underlying_name, contract_count,
                       put_call_ratio, oi_total, total_amount
                FROM option_underlying_daily WHERE trade_date = %s
                ORDER BY total_amount DESC NULLS LAST
                """, (date,))
        try:
            rows = _query(as_of)
            use_date, fallback = as_of, False
            if not rows:
                # 当日无统计（SSE 统计仅最新日；或当日未采）→ 回落最新数据日
                max_row = self.db._query(
                    'SELECT MAX(trade_date) FROM option_underlying_daily')
                max_date = str(max_row[0][0]) if max_row and max_row[0][0] else None
                if not max_date:
                    return None
                rows, use_date, fallback = _query(max_date), max_date, True
        except Exception as e:
            logger.warning(f'期权锚查询失败: {e}')
            return None
        if not rows:
            return None
        items = [{'underlying_code': str(r[0]),
                  'underlying_name': r[1] or str(r[0]),
                  'contract_count': int(r[2]) if r[2] is not None else None,
                  'put_call_ratio': float(r[3]) if r[3] is not None else None,
                  'oi_total': float(r[4]) if r[4] is not None else None,
                  'total_amount': float(r[5]) if r[5] is not None else None}
                 for r in rows]
        return {'items': items[:6], 'trade_date': use_date,
                'count': len(items), 'as_of': as_of,
                'is_latest_fallback': use_date != as_of}

    def _north_detail_anchor(self, as_of: str, limit: int = 10
                             ) -> Optional[Dict[str, Any]]:
        """北向个股持仓 Top N（第三阶段-3；披露止于 20240816）。

        取 as_of 当日明细；as_of 晚于披露截止日时自动回落到**库内最新
        数据日**并标注 data_date（禁用陈旧数据冒充当日——边界必须显式）。
        """
        def _top(date: str):
            return self.db._query(
                """
                SELECT n.ts_code, n.hold_market_value, n.hold_ratio
                FROM north_flow_detail_daily n
                WHERE n.trade_date = %s
                ORDER BY n.hold_market_value DESC NULLS LAST LIMIT %s
                """, (date, limit))
        try:
            rows = _top(as_of)
            use_date, fallback = as_of, False
            if not rows:
                # 当日无明细（披露止于 2024-08-16）→ 回落库内最新数据日
                max_row = self.db._query(
                    'SELECT MAX(trade_date) FROM north_flow_detail_daily')
                max_date = str(max_row[0][0]) if max_row and max_row[0][0] else None
                if not max_date:
                    return None
                rows, use_date, fallback = _top(max_date), max_date, True
        except Exception as e:
            logger.warning(f'北向明细查询失败: {e}')
            return None
        if not rows:
            return None
        # 名称回查（base_stock_info；缺失留 code）
        codes = [str(r[0]) for r in rows]
        name_map = self._stock_name_map(codes)
        items = []
        for r in rows:
            mv, ratio = r[1], r[2]
            items.append({
                'ts_code': str(r[0]),
                'name': name_map.get(str(r[0]), str(r[0])),
                'hold_market_value_yi': (round(float(mv) / 1e8, 2)
                                         if mv is not None else None),
                'hold_ratio': float(ratio) if ratio is not None else None,
            })
        max_row2 = self.db._query(
            'SELECT MAX(trade_date) FROM north_flow_detail_daily')
        max_date = (str(max_row2[0][0])
                    if max_row2 and max_row2[0][0] else use_date)
        return {'items': items, 'data_date': use_date, 'max_date': max_date,
                'as_of': as_of, 'is_latest_fallback': fallback}

    def _stock_name_map(self, ts_codes: List[str]) -> Dict[str, str]:
        if not ts_codes:
            return {}
        try:
            rows = self.db._query(
                'SELECT ts_code, stock_name FROM base_stock_info '
                'WHERE ts_code = ANY(%s)', (ts_codes,))
            return {str(r[0]): str(r[1]) for r in rows}
        except Exception:
            return {}

    @staticmethod
    def _turnover_delta(panel: pd.DataFrame, as_of: str,
                        prev_date: Optional[str]) -> Dict[str, Any]:
        """两市成交额今昨对比（沪深+北交所分组）+ 60 日逐日序列（T3）。

        series 与 today_total 同源同列（panel.amount 按日求和）——digest §一
        成交额图末值与 V1 验点基准天然对拍一致。
        """
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
        daily = panel.groupby('trade_date')['amount'].sum().sort_index()
        result['series'] = [{'date': str(d), 'total_yi': round(float(v) / 1e8, 2)}
                            for d, v in daily.items()]
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

    def _a50_from_kline(self, as_of: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """A50 日频行（overseas_kline_day CHA50CFD；附录 C.5）。

        涨跌幅 = 锚定日口径下最新两根收盘环比（新交所自身交易日历，隔夜口径
        优于快照本地昨收自算）。**as_of 截断防前视**——历史重跑须取 ≤ as_of 的
        两根（首版取"最新两根"，0929/0930 重跑曾显示 10-23 值，勘误见附录 D.2）。
        无行/仅一根/读失败 → None（回退快照，不硬造）。
        """
        try:
            sql = ('SELECT trade_date, close FROM overseas_kline_day '
                   "WHERE market_code = 'CHA50CFD'")
            params: tuple = ()
            if as_of:
                sql += ' AND trade_date <= %s'
                params = (str(as_of),)
            sql += ' ORDER BY trade_date DESC LIMIT 2'
            rows = self.db._query(sql, params)
        except Exception as e:
            logger.warning(f'A50 日K读取失败（回退快照）: {e}')
            return None
        if len(rows) < 2:
            return None
        (d1, c1), (d0, c0) = rows[0], rows[1]
        try:
            c1, c0 = float(c1), float(c0)
        except (TypeError, ValueError):
            return None
        if c0 <= 0:
            return None
        return {'date': str(d1), 'name': '富时A50期货', 'close': _f(c1),
                'change_pct': _f(round((c1 - c0) / c0 * 100, 2))}

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
        metrics['cycle'] = self._sentiment_cycle(metrics, as_of=as_of)
        metrics['cycle_trend'] = self._sentiment_cycle_trend(as_of, metrics) if as_of else None
        metrics['ladder_gap'] = _ladder_gap(metrics.get('ladder') or {})
        metrics['seal_dist'] = self._seal_time_dist(as_of, cs_today) if as_of else None
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

    def _sentiment_quantile_calibration(self, as_of: Optional[str],
                                        window_days: int = 250
                                        ) -> Dict[str, Dict[str, Optional[float]]]:
        """滚动窗口的情绪读数分位（P1-10 §4.4；报告层分位四态的基准）。

        口径：sentiment_metrics_daily 最近 ``window_days`` 个交易日（不含
        锚定日之后），对 max_streak/premium_mean/burst_rate 取 p10/p60/p90；
        表空/查询失败 → {}（quantile_state 归兜底并标注，不硬造）。
        """
        if not as_of:
            return {}
        try:
            rows = self.db._query(
                """
                SELECT max_streak, premium_mean, burst_rate
                FROM sentiment_metrics_daily
                WHERE trade_date < %s
                ORDER BY trade_date DESC LIMIT %s
                """, (as_of, window_days))
        except Exception as e:
            logger.warning(f'情绪分位滚动窗口读取失败: {e!r}')
            return {}
        if not rows:
            return {}
        df = pd.DataFrame(rows, columns=['max_streak', 'premium_mean',
                                         'burst_rate'])
        return quantile_calibration(df)

    def _sentiment_cycle(self, m: Dict[str, Any],
                         as_of: Optional[str] = None) -> Dict[str, Any]:
        """情绪周期定位（判据阈值取 config review.sentiment_rules，试用期水印）。

        返回含结构化 ``criteria``（P1-1：判据对照表数据——判据|阈值|当日值|命中，
        digest 与报告层直接渲染，回答"为什么不是 X"）与 ``trial``（P1-4：
        config review.sentiment_calibrated=False 时展示"判据试用版"水印）。
        """
        rules = self.sentiment_rules
        max_streak = m.get('max_streak') or 0
        burst_rate = m.get('burst_rate')
        premium_mean = m.get('premium_mean')
        cycle, basis = '试用版判据待校准', []
        criteria: List[Dict[str, Any]] = []
        breadth_warm: Optional[bool] = None
        try:
            climax = rules.get('climax') or {}
            ice = rules.get('ice') or {}
            ebb = rules.get('ebb') or {}
            # 市场宽度（涨跌家数比，全市场宽度独立于涨停 cohort；P4-5 引入时为
            # 参考判据，2026-10-28 按校准包升格定位判据（阈值 2.0→2.5）：高潮日
            # 作第二证据确认/分歧披露，非高潮日（回暖/震荡档）宽度达标记「偏暖」。
            up_count, down_count = m.get('up_count'), m.get('down_count')
            breadth_ratio = (round(up_count / down_count, 2)
                             if up_count and down_count else None)
            br_gte = climax.get('breadth_ratio_gte', 2.5)
            criteria = [
                {'name': '高潮',
                 'expr': f"高度≥{climax.get('max_streak_gte')} 且 "
                         f"溢价≥{climax.get('premium_mean_gte')}%",
                 'values': f"高度{max_streak} / 溢价{_disp_pct(premium_mean)}",
                 'hit': False, 'role': 'position'},
                {'name': '冰点',
                 'expr': f"高度≤{ice.get('max_streak_lte')} 或 "
                         f"炸板率≥{ice.get('burst_rate_gte')}",
                 'values': f"高度{max_streak} / 炸板率{_disp_rate(burst_rate)}",
                 'hit': False, 'role': 'position'},
                {'name': '退潮',
                 'expr': f"溢价均值≤{ebb.get('premium_mean_lte')}%",
                 'values': f"溢价均值{_disp_pct(premium_mean)}",
                 'hit': False, 'role': 'position'},
                {'name': '市场宽度（涨跌家数比）',
                 'expr': f"涨跌家数比≥{br_gte}（市场宽度，独立于涨停 cohort；"
                         "2026-10-28 校准升格定位判据——高潮日作第二证据"
                         "确认/分歧，非高潮日宽度达标记「偏暖」）",
                 'values': (f"涨{up_count if up_count is not None else '—'} / "
                            f"跌{down_count if down_count is not None else '—'} / "
                            f"比{breadth_ratio if breadth_ratio is not None else '—'}"),
                 'hit': bool(breadth_ratio is not None and breadth_ratio >= br_gte),
                 'role': 'position'},
            ]
            if (max_streak >= climax.get('max_streak_gte', 99)
                    and (premium_mean is not None and premium_mean >= climax.get('premium_mean_gte', 99))):
                cycle, basis = '高潮', [f"高度{max_streak}≥{climax.get('max_streak_gte')}",
                                        f"溢价{_disp_pct(premium_mean)}≥{climax.get('premium_mean_gte')}%",
                                        f"第二证据涨跌家数比"
                                        f"{breadth_ratio if breadth_ratio is not None else '—'}"
                                        f"{'确认' if criteria[3]['hit'] else '未确认（分歧，人工注意）'}"]
                criteria[0]['hit'] = True
            elif (max_streak <= ice.get('max_streak_lte', -1)
                    or (burst_rate is not None and burst_rate >= ice.get('burst_rate_gte', 99))):
                cycle, basis = '冰点', [f"高度{max_streak}≤{ice.get('max_streak_lte')}"
                                        f" 或 炸板率{_disp_rate(burst_rate)}≥{ice.get('burst_rate_gte')}%"]
                criteria[1]['hit'] = True
            elif premium_mean is not None and premium_mean <= ebb.get('premium_mean_lte', 999):
                cycle, basis = '退潮', [f"溢价均值{_disp_pct(premium_mean)}≤{ebb.get('premium_mean_lte')}%"]
                criteria[2]['hit'] = True
            else:
                cycle, basis = '回暖/震荡', ['未命中冰点/高潮/退潮判据']
                # 2026-10-28 升格：非高潮日宽度达标（涨跌家数比≥阈值）→ 定位标
                # 注「偏暖」（校准依据：ge2.5 非高潮日次日溢价 2.44% vs 基线
                # 1.95%）。档名不变（候选联动/核验口径零影响），仅定位读数加向。
                breadth_warm = (bool(breadth_ratio >= br_gte)
                                if breadth_ratio is not None else None)
                if breadth_warm:
                    basis.append(f"宽度确认偏暖（涨跌家数比{breadth_ratio}"
                                 f"≥{br_gte}）")
        except Exception as e:
            logger.warning(f'情绪周期判据计算失败: {e}')
            cycle, basis = '试用版判据待校准', [f'判据计算异常: {e}']

        # 分位标定四态（P1-10 §4.4 并列列；规则四态保持基线不替换）
        try:
            q_calib = self._sentiment_quantile_calibration(as_of)
            q_cycle, q_basis = quantile_state(m, q_calib)
        except Exception as e:  # pragma: no cover - 分位异常不拖垮主判据
            logger.warning(f'情绪分位标定失败: {e!r}')
            q_calib, q_cycle, q_basis = {}, '数据不足', [f'分位计算异常: {e!r}']
        divergence = (q_cycle in ('高潮', '冰点', '退潮')
                      and cycle in ('高潮', '冰点', '退潮')
                      and q_cycle != cycle)
        # V4.3 B2：距最近定位判据（仅回暖/震荡——即高潮/冰点/退潮均未命中；
        # 同量纲比较只取溢价均值 pct 差，跨量纲高度/炸板率不比）。给"中性偏多"
        # 倾向一个可检验的钩子（次日 V3 直接验证）；数值缺失为 None 不硬造。
        nearest_miss = None
        if cycle == '回暖/震荡' and premium_mean is not None:
            c_gte = climax.get('premium_mean_gte')
            c_lte = ebb.get('premium_mean_lte')
            misses = []
            if c_gte is not None and premium_mean < float(c_gte):
                misses.append({'name': '高潮', 'metric': '溢价均值',
                               'value': round(float(premium_mean), 2),
                               'threshold': float(c_gte),
                               'gap': round(float(c_gte) - float(premium_mean), 2)})
            if c_lte is not None and premium_mean > float(c_lte):
                misses.append({'name': '退潮', 'metric': '溢价均值',
                               'value': round(float(premium_mean), 2),
                               'threshold': float(c_lte),
                               'gap': round(float(premium_mean) - float(c_lte), 2)})
            nearest_miss = min(misses, key=lambda x: x['gap']) if misses else None
        # P1-4（C1）：原第二个不可达 return（trial 水印永不显示）删除，
        # trial 并入本 return——sentiment.py/conclusion.py 的水印消费恢复生效
        return {'position': cycle, 'basis': basis, 'criteria': criteria,
                'breadth_warm': breadth_warm,
                'trial': not self.sentiment_calibrated,
                'quantile_position': q_cycle, 'quantile_basis': q_basis,
                'quantile_calibration': q_calib,
                'quantile_divergence': divergence,
                'nearest_miss': nearest_miss}

    def _sentiment_cycle_trend(self, as_of: Optional[str],
                               metrics: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """情绪周期走向（四期方案 §3；vibe-astock 10 日窗口口径）：

        近 10 个交易日（sentiment_metrics_daily，锚定日无表行时以当日重算值补位），
        涨停家数/最高连板 0-1 归一 + 炸板率取反（越高越冷），三项均值为当日 score。
        score 是窗口内相对读数，没有绝对含义；样本 <10 天标注不完整。
        """
        if not as_of:
            return None
        try:
            rows = self.db._query(
                """
                SELECT trade_date, limit_up_count, burst_rate, max_streak
                FROM sentiment_metrics_daily
                WHERE trade_date <= %s
                ORDER BY trade_date DESC LIMIT 10
                """, (as_of,))
        except Exception as e:
            logger.warning(f'情绪周期走向历史读取失败: {e}')
            return None
        series = [{'date': str(r[0]), 'limit_up': r[1],
                   'burst_rate': None if r[2] is None else float(r[2]),
                   'max_streak': r[3]} for r in reversed(rows)]
        if not series or series[-1]['date'] != str(as_of):
            series.append({'date': str(as_of), 'limit_up': metrics.get('limit_up_count'),
                           'burst_rate': metrics.get('burst_rate'),
                           'max_streak': metrics.get('max_streak')})
        if len(series) < 2:
            return None

        def _norm(value, values, invert):
            vals = [v for v in values if v is not None]
            if value is None or not vals:
                return None
            lo, hi = min(vals), max(vals)
            u = 0.5 if hi <= lo else (value - lo) / (hi - lo)
            return 1.0 - u if invert else u

        for s in series:
            comps = []
            vals = {'limit_up': [x['limit_up'] for x in series],
                    'max_streak': [x['max_streak'] for x in series],
                    'burst_rate': [x['burst_rate'] for x in series]}
            for key, invert in (('limit_up', False), ('max_streak', False),
                                ('burst_rate', True)):
                u = _norm(s[key], vals[key], invert)
                if u is not None:
                    comps.append(u)
            s['score'] = round(sum(comps) / len(comps), 3) if comps else None
        scores = [s['score'] for s in series if s['score'] is not None]
        if not scores:
            return None
        today_score = scores[-1]
        delta = round(today_score - scores[0], 3)
        direction = '上行' if delta >= 0.05 else ('下行' if delta <= -0.05 else '走平')
        lo_idx = min(range(len(scores)), key=lambda i: scores[i])
        below = sum(1 for v in scores if v <= today_score)
        return {'window': len(scores),
                'complete': len(scores) >= 10,
                'score': today_score, 'delta': delta, 'direction': direction,
                'days_since_low': len(scores) - 1 - lo_idx,
                'window_pct': round(100.0 * below / len(scores)),
                'scores': [{'date': s['date'], 'score': s['score']} for s in series]}

    def _seal_time_dist(self, as_of: str,
                        cs_today: pd.DataFrame) -> Optional[Dict[str, Any]]:
        """封板时段分布（四期方案 §3）：当日涨停股首封时间五时段 + 封板质量。

        数据源 limit_seal_stats_daily（tdx 分笔口径，含炸板股）；涨停股无行时
        覆盖率如实展示，整表无行返回 None（渲染"待补"）。
        """
        if cs_today.empty:
            return None
        up_mask = cs_today['_limit_up'].fillna(False).astype(bool)
        up_codes = [str(c) for c in cs_today.loc[up_mask, 'ts_code']]
        if not up_codes:
            return None
        try:
            rows = self.db._query(
                "SELECT ts_code, first_seal_time, open_count FROM limit_seal_stats_daily "
                "WHERE trade_date = %s AND ts_code = ANY(%s)", (as_of, up_codes))
        except Exception as e:
            logger.warning(f'封板时段读取失败: {e}')
            return None
        if not rows:
            return None
        bounds = (('09:25-09:35', 93500), ('09:35-10:00', 100000),
                  ('10:00-11:30', 113000), ('11:30-14:00', 140000),
                  ('14:00-15:00', 150000))
        buckets = {name: 0 for name, _ in bounds}
        never_broken = late_seal = 0
        parsed = 0
        for row in rows:
            if row is None or len(row) < 3:
                continue  # 行结构异常（存根/旧数据）跳过，不硬造
            _, first, open_count = row[0], row[1], row[2]
            try:
                t = int(str(first)[:6]) if first is not None else None
            except (TypeError, ValueError):
                t = None
            if t is None:
                continue
            parsed += 1
            for name, bound in bounds:
                if t <= bound:
                    buckets[name] += 1
                    break
            if open_count is not None and int(open_count) == 0:
                never_broken += 1
            if t > 140000:
                late_seal += 1
        if not parsed:
            return None
        return {'limit_up_count': len(up_codes), 'covered': len(rows), 'parsed': parsed,
                'buckets': [{'bucket': name, 'count': buckets[name]}
                            for name, _ in bounds],
                'never_broken_rate': round(never_broken / parsed, 4),
                'late_seal_rate': round(late_seal / parsed, 4)}

    # ------------------------------------------------------------------ 维度3

    def _aggregate_verification(self, as_of: str, prev_date: Optional[str],
                                cs_today: pd.DataFrame,
                                metrics: Dict[str, Any]) -> AggregationResult:
        """昨日核验（四期方案 §1）：读 review_prediction_daily 昨日载荷 → 今日截面
        三态核验 + 候选池战绩 + 滚动战绩。昨日无预测记录（体系建立前）available=False，
        渲染"⏳ 无昨日预期记录"。落库由 generator 完成（DataReader 只读）。"""
        from . import verifier as _verifier
        if not prev_date:
            return AggregationResult(available=False, reason='无前一交易日')
        try:
            rows = self.db._query(
                "SELECT payload FROM review_prediction_daily WHERE trade_date = %s",
                (prev_date,))
        except Exception as e:
            return AggregationResult(available=False, reason=f'预测表读取失败: {e}',
                                     data={'prev_date': prev_date})
        if not rows or not rows[0][0]:
            return AggregationResult(
                available=False,
                reason=f'无 {prev_date} 预测记录（核验体系 2026-10-07 起逐日积累）',
                data={'prev_date': prev_date})
        try:
            payload = rows[0][0] if isinstance(rows[0][0], dict) else json.loads(rows[0][0])
            name2code = {e.get('name'): e.get('code') for e in
                         ((self.config.get('review_sync') or {})
                          .get('overseas_kline_indices') or [])
                         if e.get('name') and e.get('code')}
            verified = _verifier.verify_predictions(payload, prev_date, cs_today,
                                                    metrics, self.db, name2code)
            verified['rolling'] = _verifier.rolling_stats(as_of, verified, self.db)
            verified['sentiment_prev'] = payload.get('sentiment') or {}
            return AggregationResult(data=verified)
        except Exception as e:
            logger.warning(f'昨日核验计算失败: {e}')
            return AggregationResult(available=False, reason=f'核验计算失败: {e}',
                                     data={'prev_date': prev_date})

    def _aggregate_money_effect(self, cs_today: pd.DataFrame,
                                cs_prev: pd.DataFrame) -> AggregationResult:
        """赚钱效应/亏钱效应五组指标（四期方案 §2；money_effect.py 同一实现）。"""
        try:
            data = _money_effect.money_effect(cs_today, cs_prev)
        except Exception as e:
            logger.warning(f'赚钱效应聚合失败: {e}')
            return AggregationResult(available=False, reason=f'计算失败: {e}')
        return AggregationResult(data=data)

    def _aggregate_sector(self, panel: pd.DataFrame, cs_today: pd.DataFrame,
                          cs_prev: pd.DataFrame, as_of: str) -> AggregationResult:
        industry_map = self._industry_map(as_of)
        concept_map = self._concept_map(as_of)
        if not industry_map and not concept_map:
            return AggregationResult(available=False,
                                     reason='sector_concept_map 为空（未跑 sync-industry/sync-concept）')
        data: Dict[str, Any] = {}
        if industry_map:
            source_map = self.industry_source_map(as_of)
            data['industry_today'] = self._industry_ranking(
                panel, industry_map, [as_of], source_map)
            last5 = self._last_n_dates(as_of, 5)
            data['industry_5d'] = self._industry_ranking(
                panel, industry_map, last5, source_map)
        # N3（2026-10-04）：申万二级行业 Top10 子表（THS 快照口径）
        l2_map = self._industry_l2_map(as_of)
        if l2_map:
            data['industry_l2_today'] = self._industry_ranking(panel, l2_map, [as_of])
        if concept_map:
            data['limit_up_clusters'] = self._limit_up_clusters(cs_today, concept_map)
        # 四期方案 §3：头部题材集中度 + 题材跨日事件树
        exclude = ((self.config.get('review') or {}).get('theme_exclude_concepts')) or []
        data['theme_concentration'] = _theme_concentration(
            data.get('limit_up_clusters') or [], cs_today, exclude)
        data['theme_tree'] = self._theme_tree(cs_today, cs_prev, as_of,
                                              data.get('limit_up_clusters') or [],
                                              concept_map, exclude)
        return AggregationResult(data=data)

    def _industry_map(self, as_of: str) -> Dict[str, str]:
        """ts_code → 行业名（评价-3 收口：委托 industry_map 统一实现——
        原分源循环查询无稳定排序，归属依赖 DB 返回顺序，与 C2 同源）"""
        return self.industry_map(as_of)

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
                          dates: List[str],
                          source_map: Optional[Dict[str, str]] = None
                          ) -> List[Dict[str, Any]]:
        """行业等权涨跌幅榜（§5.3 口径：行业内个股等权均值）。

        评价-3：成分全部来自 sina_hy 兜底（无任何 sw_2021 成分）的板为概念板
        （如"次新股""ST 板"），条目带 ``is_concept=True``——渲染层打（概念）标签，
        不再以真实行业身份入榜。
        """
        df = panel[panel['trade_date'].isin(dates) & panel['ts_code'].isin(industry_map)]
        if df.empty:
            return []
        df = df.assign(industry=df['ts_code'].map(industry_map))
        change = pd.to_numeric(df['change_rate'], errors='coerce')
        df = df.assign(change=change)
        g = df.groupby('industry')['change'].agg(['mean', 'count']).dropna()
        g = g.sort_values('mean', ascending=False)
        concept_boards: Dict[str, bool] = {}
        if source_map:
            src = df.assign(_src=df['ts_code'].map(source_map))
            comp = src.groupby('industry')['_src'].apply(
                lambda s: (s == 'sw_2021').any())
            concept_boards = {k: not v for k, v in comp.items()}
        return [{'industry': idx, 'mean_change': round(float(r['mean']), 2),
                 'stocks': int(r['count']),
                 'is_concept': concept_boards.get(idx, False)}
                for idx, r in g.iterrows()]

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
        # G 涨停归因（P1-11）：涨停明细加"归因"列——与链 limit_up_attribution
        # 模块共用 attribution_rules + 同一公告窗口装配（原则 P6）
        self._enrich_attribution(data['limit_up'], as_of)
        csv_path = self._csv_path(as_of)
        if csv_path and csv_path.exists():
            data['csv_path'] = str(csv_path)
            data['cross_check'] = self._cross_check(cs_today, csv_path)
        return AggregationResult(data=data)

    def _enrich_attribution(self, rows: List[Dict[str, Any]], as_of: str) -> None:
        """涨停明细归因列（P1-11）：链 limit_up_attribution 模块的同一实现两入口。

        公告窗口复用 env_assembly._announcement_map（同一装配）；谓词共用
        attribution_rules.pick_attribution；覆盖断代附注一致。
        """
        from ..analysis.attribution_rules import coverage_note, pick_attribution
        from ..analysis.env_assembly import _announcement_map
        window = int(((self.config.get('review') or {}).get('attribution') or {})
                     .get('ann_window_days') or 3)
        try:
            ann_map = _announcement_map(self.reader, as_of, window)
        except Exception as e:
            logger.warning(f'涨停归因公告窗口读取失败: {e!r}')
            for r in rows:
                r['attribution'] = None
            return
        for r in rows:
            label, _, _ = pick_attribution(ann_map.get(r['ts_code']) or [])
            if coverage_note(as_of):
                label += '(公告覆盖不全)'
            r['attribution'] = label

    def _lhb_codes_of_day(self, as_of: str,
                          now: Optional[datetime] = None) -> set:
        """当日龙虎榜上榜代码集（D2 交叉标记；表空返回空集；now 可注入测试）"""
        if not lhb_published(as_of, now=now):
            return set()   # P4-6：早跑时当日榜未发布，不产生交叉标记
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

    def _aggregate_lhb(self, as_of: str,
                       now: Optional[datetime] = None) -> AggregationResult:
        """D2 龙虎榜 → 维度4：净额 Top10（行粒度，禁跨原因求和）+ 机构净买 Top5。

        金额口径：该上榜原因的前五席位合计（非全市场买卖总额）；成交额列
        沪市 secTxAmount / 深市列表成交金额。
        """
        if not lhb_published(as_of, now=now):
            return AggregationResult(
                available=False,
                reason='龙虎榜 17:30 后发布，当日报告早跑未到发布时点（P4-6 门控停显）')
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

        口径（P1-7 动态）：行级口径受 money_flow_daily.calib_note 约束——
        20260917 起全市场分笔分桶（单笔≥50 万分桶）；更早 as_of 为异动子集
        近似口径（涨停∪跌停∪|涨跌幅|≥7%∪成交额Top50），非全市场。
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
        窗口日历查询须 _dot 归一（X1）：裸 YYYYMMDD 与 TEXT 日期比较恒小于，静默返回空。
        """
        try:
            # calendar_date 为 TEXT(YYYY-MM-DD)，比较前须 _dot 归一（X1；同
            # _next_trade_date/_disclosure 先例——裸 YYYYMMDD 恒小于带连字符日期）
            upcoming = self._next_n_dates(_dot(as_of), 10)
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

    def _aggregate_indicators(self, as_of: str) -> AggregationResult:
        """技术指标状态（P0-1 §3.1.7）：522d 面板复算七模块 + 交叉榜。

        与链同一实现两入口（indicator_panel.compute_indicator_states 调同一批
        modules），窗口与链 daily_review 逐字节对齐（520+2，2026-10-25 修复
        133d 错位——此前对拍 731 处失配的根因，附录 C.6）；窗口起点统一走
        panel_window_start（20261027 修复调用处 -1 的 off-by-one）；常量闸见
        test_review_indicator_parity.py。
        失败/空面板 → available=False（报告渲染「⏳ 待补」，不阻塞其他维度）。
        """
        from .indicator_panel import (
            compute_indicator_states, load_indicator_panel,
            panel_window_start, summarize_indicator_states,
        )
        try:
            start = panel_window_start(self.reader, as_of)
            if start is None:
                return AggregationResult(
                    available=False,
                    reason='交易日历不足 522 日，无法满足指标 warmup')
            panel = load_indicator_panel(self.reader, as_of, start)
            if panel.empty:
                return AggregationResult(available=False, reason='指标面板窗口内无日K数据')
            states = compute_indicator_states(panel, as_of)
        except Exception as e:
            logger.warning(f'指标面板复算失败: {e!r}')
            return AggregationResult(available=False, reason=f'指标面板复算失败: {e!r}')
        if states.empty:
            return AggregationResult(available=False, reason='指标面板为空')
        data = summarize_indicator_states(states, self.volume_surge_min, self.top_n)
        data['window_start'] = start
        parity = self._indicator_parity(states, as_of)
        if parity is not None:
            data['parity'] = parity
        return AggregationResult(data=data)

    #: 指标对拍列（报告层面板 vs 链 CSV，同一实现两入口的运行时校验）
    _INDICATOR_PARITY_COLUMNS = ['MACD状态', 'KDJ状态', 'RSI状态', 'BOLL位置%',
                                 'ATR波动%', 'OBV趋势', '均线排列']

    # ------------------------------------------------------------------ 缠论背驰候选
    def _aggregate_chan(self, as_of: str) -> AggregationResult:
        """缠论背驰候选（2026-10-24）：520d 面板复算一买/二买锚定日确认信号。

        与链 chan_divergence_buy 模块同一实现（chan.scan.scan_stock_anchor）；
        排序：一买在前、背驰强度升序（越小越强）；失败/空面板 → available=False。
        独立性红线（报告不依赖链运行）+ 逐股引擎逐K计算（向量化红线豁免同链），
        实测全市场 +20~30 秒（520 日窗口，指标面板先例 +33 秒）。

        P0-7（2026-10-24）输入完整性门控：逐股 expected_bars 按
        ``[max(窗口起点, list_date), as_of]`` 交易日数计算（防次新误判缺日），
        缺日 >2% 不判定、≤2% 保留判定带 ``data_flag``；并与链 meta 存疑清单
        （链 CSV「是否存疑/存疑原因」列）交叉 → ``suspect_overlap``。
        """
        from ..chan.scan import CHAN_WINDOW_DAYS, DEFAULT_MIN_COVERAGE
        from ..chan.scan import hit_label, scan_stock_anchor
        from bisect import bisect_left
        try:
            start = self.reader.nth_trading_day_back(as_of, CHAN_WINDOW_DAYS - 1)
            if start is None:
                return AggregationResult(available=False,
                                         reason=f'交易日历不足 {CHAN_WINDOW_DAYS} 日')
            panel = self.reader.load_kline(['stock_name', 'low', 'is_st'],
                                           as_of, start_date=start)
            if panel.empty:
                return AggregationResult(available=False, reason='缠论面板窗口内无日K数据')
            min_coverage = float((self.config.get('chan') or {})
                                 .get('min_coverage', DEFAULT_MIN_COVERAGE))
            window_td = self.reader.trading_days(as_of, start)
            list_dates = {s.ts_code: (s.list_date or '') for s in
                          self.reader.load_stock_universe()}
            items: List[Dict[str, Any]] = []
            for code, g in panel.groupby('ts_code', sort=False):
                # 次新股按上市日起算应含K数（上市晚 ≠ 缺日）
                eff_start = max(start, list_dates.get(code) or start)
                idx = bisect_left(window_td, eff_start)
                expected = len(window_td) - idx
                status, hit, flag = scan_stock_anchor(
                    g, as_of, expected_bars=expected or None,
                    min_coverage=min_coverage)
                if status != 'hit' or hit is None:
                    continue
                label = hit_label(hit)
                if flag:                      # 缺日标记并入理由串（与链侧同规则，保对拍）
                    label = f'{label}/{flag}'
                _close_last = pd.to_numeric(g['close'], errors='coerce').iloc[-1] \
                    if 'close' in g else None
                items.append({
                    'ts_code': code,
                    'code': str(g['stock_code'].iloc[-1]),
                    'name': str(g['stock_name'].iloc[-1]) if 'stock_name' in g else '',
                    'label': label,
                    'kind': hit.kind,
                    'close': (round(float(_close_last), 2)
                              if _close_last is not None and pd.notna(_close_last)
                              else None),
                    'stop': round(hit.stop_price, 2),
                    'trigger_low': round(hit.trigger_low, 2),
                    'zg': round(hit.zg, 2),
                    'zd': round(hit.zd, 2),
                    'divergence': round(hit.divergence, 2) if hit.divergence is not None else None,
                    'refire': hit.refire,
                    'data_flag': flag,
                })
            items.sort(key=lambda x: (0 if x['kind'] == '一买' else 1,
                                      x['divergence'] if x['divergence'] is not None else 9.99))
            data: Dict[str, Any] = {'items': items, 'count': len(items),
                                    'window_start': start}
            parity = self._chan_parity(items, as_of)
            if parity is not None:
                data['parity'] = parity
            data['suspect_overlap'] = self._chan_suspect_overlap(items, as_of)
            return AggregationResult(data=data)
        except Exception as e:
            logger.warning(f'缠论背驰候选复算失败: {e!r}')
            return AggregationResult(available=False, reason=f'缠论面板复算失败: {e!r}')

    def _chan_suspect_overlap(self, items: List[Dict[str, Any]],
                              as_of: str) -> Dict[str, Any]:
        """缠论候选 × 链存疑清单交叉（P0-7 报告层披露，覆盖 data_flag 之外的
        前复权断裂等存疑原因）。链未跑/无存疑列 → available=False 不硬造。"""
        csv_path = self._csv_path(as_of)
        if csv_path is None:
            return {'available': False, 'note': '存疑清单不可得（链未跑）'}
        try:
            csv_df = pd.read_csv(csv_path, dtype=str, encoding='utf-8-sig')
            col = '是否存疑'
            if col not in csv_df.columns:
                return {'available': False, 'note': '链 CSV 无存疑列'}
            sus = csv_df[csv_df[col] == '是']
            reason_map = {str(r['股票代码']): str(r.get('存疑原因') or '')
                          for _, r in sus.iterrows()}
            hits = []
            for it in items:
                reason = reason_map.get(it['code'])
                if reason:
                    it['suspect_reason'] = reason     # 逐只随行（表格数据标记列引用）
                    hits.append({'code': it['code'], 'name': it['name'],
                                 'reason': reason[:60]})
            return {'available': True, 'suspect_total': int(len(sus)),
                    'hit_count': len(hits), 'hits': hits}
        except Exception as e:
            logger.warning(f'缠论存疑交叉读取失败（跳过）: {e!r}')
            return {'available': False, 'note': f'存疑清单读取失败: {e!r}'}

    def _chan_parity(self, items: List[Dict[str, Any]],
                     as_of: str) -> Optional[Dict[str, Any]]:
        """链 CSV 缠论列 vs 报告层复算（命中集逐股对拍）；CSV 未跑/无列 → None。"""
        csv_path = self._csv_path(as_of)
        if csv_path is None:
            return None
        try:
            csv_df = pd.read_csv(csv_path, dtype=str, encoding='utf-8-sig')
            col = '缠论背驰买点'
            if col not in csv_df.columns:
                return None
            hits_csv = csv_df[csv_df[col].str.startswith(('一买', '二买'), na=False)]
            mine = {r['code']: r['label'] for r in items}
            theirs = {str(r): str(lbl) for r, lbl in
                      zip(hits_csv['股票代码'], hits_csv[col])}
            diff = sorted(set(mine.items()) ^ set(theirs.items()))
            return {'match': not diff, 'report_n': len(mine), 'chain_n': len(theirs),
                    'diff': diff[:5]}
        except Exception as e:
            logger.warning(f'缠论对拍读取失败（跳过）: {e!r}')
            return None

    def _indicator_parity(self, states: pd.DataFrame,
                          as_of: str) -> Optional[Dict[str, Any]]:
        """链 CSV 指标列 vs 报告层面板复算（逐股逐值）；CSV 未跑/无列 → None。"""
        csv_path = self._csv_path(as_of)
        if csv_path is None:
            return None
        try:
            csv_df = pd.read_csv(csv_path, dtype=str, encoding='utf-8-sig')
        except Exception as e:
            logger.warning(f'指标对拍 CSV 读取失败: {e!r}')
            return None
        cols = [c for c in self._INDICATOR_PARITY_COLUMNS if c in csv_df.columns]
        if not cols or '股票代码' not in csv_df.columns:
            return None
        csv_idx = csv_df.set_index(csv_df['股票代码'].astype(str).str.zfill(6))
        mismatches: List[Dict[str, Any]] = []
        compared = 0
        for _, row in states.iterrows():
            code = str(row['ts_code']).split('.')[-1].zfill(6)
            if code not in csv_idx.index:
                continue
            csv_row = csv_idx.loc[code]
            if isinstance(csv_row, pd.DataFrame):  # 代码重复时取首行
                csv_row = csv_row.iloc[0]
            for col in cols:
                mine, theirs = row[col], csv_row[col]
                if _values_match(mine, theirs):
                    compared += 1
                    continue
                mismatches.append({'code': code, 'column': col,
                                   'panel': str(mine), 'csv': str(theirs)})
                compared += 1
        return {'ok': not mismatches, 'compared': compared,
                'columns': len(cols), 'mismatches': mismatches[:20],
                'mismatch_count': len(mismatches)}

    def _valuation_map_of_day(self, as_of: str) -> Optional[Dict[str, Any]]:
        """当日估值 digest map（P4-2）：维度5 与候选基本面维共享一次装配。

        memo 按 as_of 缓存（同一次 aggregate 内维度5 先行、维度4 复用）；
        装配异常返回 None（区别于空 map"当日无行"），不硬造。
        """
        cached = getattr(self, '_p42_valuation_cache', None)
        if cached and cached[0] == str(as_of):
            return cached[1]
        from ..analysis.env_assembly import assemble_valuation
        try:
            m = assemble_valuation(self.reader, as_of, _ConfigShim(self.config))
        except Exception as e:
            logger.warning(f'估值摘要装配失败: {e!r}')
            m = None
        self._p42_valuation_cache = (str(as_of), m)
        return m

    def _ann_map_of_day(self, as_of: str) -> Dict[str, List[dict]]:
        """当日公告窗口 map（P4-2 事件维）：与 _enrich_attribution 同源
        env_assembly._announcement_map（原则 P6），memo 按 as_of 缓存。"""
        cached = getattr(self, '_p42_ann_cache', None)
        if cached and cached[0] == str(as_of):
            return cached[1]
        from ..analysis.env_assembly import _announcement_map
        window = int(((self.config.get('review') or {}).get('attribution') or {})
                     .get('ann_window_days') or 3)
        try:
            m = _announcement_map(self.reader, as_of, window)
        except Exception as e:
            logger.warning(f'候选事件维公告窗口读取失败: {e!r}')
            m = {}
        self._p42_ann_cache = (str(as_of), m)
        return m

    def _aggregate_valuation_digest(self, as_of: str) -> AggregationResult:
        """个股估值摘要聚合（P0-4 §3.3.3）：分位/消化年数/年化 PE 分布 + 判读。

        与链 valuation_digest 模块**同一 assemble_valuation 入口**（原则 P6）；
        分布为全市场截面，消化年数取 P25/P50/P75 + 极值点名。
        （P4-2 起经 _valuation_map_of_day 装配，与候选基本面维共享同一次查询。）
        """
        valuation_map = self._valuation_map_of_day(as_of)
        if valuation_map is None:
            return AggregationResult(available=False,
                                     reason='估值摘要装配失败（见日志）')
        if not valuation_map:
            return AggregationResult(available=False,
                                     reason='stock_valuation_daily 当日无行')

        def _quant(vals, qs=(0.25, 0.5, 0.75)):
            s = pd.Series([v for v in vals if v is not None], dtype=float)
            if s.empty:
                return {'sample': 0}
            return {'sample': int(len(s)),
                    **{f'p{int(q * 100)}': _f(s.quantile(q)) for q in qs}}

        pe_pct = _quant([d.pe_percentile for d in valuation_map.values()])
        digest_years = _quant([d.digestion_years for d in valuation_map.values()])
        annualized = _quant([d.annualized_pe for d in valuation_map.values()
                             if d.annualized_pe is not None and d.annualized_pe > 0])
        # 极值点名（消化年数最小 5 只——消化最快；最大 5 只——消化最慢）
        ranked = sorted(
            [d for d in valuation_map.values() if d.digestion_years is not None],
            key=lambda d: d.digestion_years)
        fastest = [(d.pe_ttm and round(d.pe_ttm, 1), round(d.digestion_years, 2))
                   for d in ranked[:5]]
        slowest = [(d.pe_ttm and round(d.pe_ttm, 1), round(d.digestion_years, 2))
                   for d in ranked[-5:]]
        return AggregationResult(data={
            'as_of': as_of,
            'stocks': len(valuation_map),
            'pe_percentile': pe_pct,
            'digestion_years': digest_years,
            'annualized_pe': annualized,
            'fastest': fastest, 'slowest': slowest,
            'cagr_available': sum(1 for d in valuation_map.values()
                                  if d.cagr_pct is not None),
        })

    def _aggregate_news(self, as_of: str) -> AggregationResult:
        """消息催化（四期方案 §4）：news_telegraph 当日要闻 + 概念关键词命中。

        口径：要闻 importance 优先、时间倒序 top_n；概念关联 = 聚集前列概念名
        在当日要闻标题/内容中出现即计数（子串匹配，噪声如实注明）。
        **新闻归因是市场叙事，不是核验过的因果。**
        """
        try:
            rows = self.db._query(
                """
                SELECT publish_time, title, content, subject, importance
                FROM news_telegraph
                WHERE trade_date = %s
                ORDER BY importance DESC, publish_time DESC
                LIMIT %s
                """, (as_of, self.top_n))
        except Exception as e:
            logger.warning(f'市场要闻读取失败: {e}')
            return AggregationResult(available=False, reason='news_telegraph 无数据（未采集）')
        if not rows:
            # T6（V4.2）reason 三态：库内最新 < 锚定日 = 采集断档（不可回补）；
            # 覆盖锚定日窗口而无当日行 = 当日确无快讯。_dot 归一防 TEXT 日期格式漂移。
            latest = None
            try:
                latest = self.db._query(
                    'SELECT MAX(trade_date) FROM news_telegraph')[0][0]
            except Exception as e:  # noqa: BLE001 —— 查询失败保留当日无行口径
                logger.warning(f'news_telegraph 最新日期查询失败: {e}')
            latest_s = str(latest).replace('.', '').replace('-', '') \
                if latest is not None else None
            if latest_s is not None and latest_s < as_of:
                reason = (f'采集断档（库内最新 {latest_s} 早于锚定日 {as_of}；'
                          '增量游标口径，历史缺口不可回补）')
            else:
                reason = '当日无快讯（已采集，当日无行）'
            return AggregationResult(available=False, reason=reason)
        items = [{'time': str(r[0] or ''), 'title': str(r[1] or ''),
                  'content': str(r[2] or ''), 'subject': r[3],
                  'importance': int(r[4] or 0)} for r in rows]
        return AggregationResult(data={'items': items})

    def _next_n_dates(self, as_of: str, n: int) -> List[str]:
        """as_of 之后 n 个交易日（升序，D4 解禁窗口）"""
        rows = self.db._query(
            "SELECT calendar_date FROM base_trade_calendar "
            "WHERE is_trading_day = 1 AND calendar_date > %s "
            "ORDER BY calendar_date ASC LIMIT %s", (as_of, n))
        return [str(r[0]).replace('-', '') for r in rows]

    def _next_trade_date(self, as_of: str) -> Optional[str]:
        """as_of 后第一个交易日（W7 周末缝提示；空结果/查询异常→None 不拖垮聚合）。

        calendar_date 为 TEXT（YYYY-MM-DD），比较前须 _dot 归一——裸 YYYYMMDD
        字符串比较恒小于带连字符日期，会静默返回空（_disclosure 同源先例）。
        """
        try:
            dates = self._next_n_dates(_dot(as_of), 1)
            return dates[0] if dates else None
        except Exception as e:  # noqa: BLE001 —— 日历缺行/库异常降级
            logger.warning(f'下一交易日查询失败: {e}')
            return None

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

    def _candidate_veto_map(self, as_of: str,
                            rules: Dict[str, Any]) -> Dict[str, List[str]]:
        """候选 VETO 预查（P4-1 起委托 analysis.candidate_veto 单一出处）。

        两入口同源：报告层与链侧 env 装配共用 ``load_candidate_veto_map``
        （业绩暴雷含披露约束 C3 / 大股东减持 / 龙虎榜卖一占比 + P4-6 发布
        时点门控），保证否决清单一致。高位板长阴为行级谓词，在 R1/R2 行
        构建处接入 ``veto_reasons``（结构可达性注记见 veto_reasons docstring）。
        """
        return load_candidate_veto_map(self.db, as_of, rules)

    def _index_adx_gate(self, as_of: str) -> Dict[str, Any]:
        """指数 ADX 闸门（P1-7 §4.6 E）：档位控制候选开放度。

        口径：默认上证指数（config review.tomorrow_plan.adx_index 可覆盖），
        ADX 走 indicators.adx（通达信 SUM+MA 口径），MA20 简单平均；
        full/half/closed 判定在 candidate_rules.adx_gate。库内无指数高低价或
        数据不足 → gate='closed' 且 reason 标注（禁默认开放）。
        """
        from ..analysis.indicators import adx as _adx_fn
        from ..analysis.indicators import MA as _ma_fn
        from ..analysis.candidate_rules import adx_gate as _gate_fn
        tp_cfg = ((self.config.get('review') or {}).get('tomorrow_plan') or {})
        ts_code = str(tp_cfg.get('adx_index') or 'sh.000001')
        try:
            rows = self.db._query(
                """
                SELECT trade_date, high, low, close FROM trade_index_info
                WHERE ts_code = %s AND trade_date <= %s
                ORDER BY trade_date DESC LIMIT 80
                """, (ts_code, as_of))
        except Exception as e:
            logger.warning(f'指数 ADX 读取失败: {e!r}')
            return {'gate': 'closed', 'reason': f'指数行情读取失败: {e!r}'}
        if len(rows) < 30:
            return {'gate': 'closed',
                    'reason': f'指数高低价数据不足（{len(rows)} 行 < 30）'}
        df = pd.DataFrame(reversed(rows),
                          columns=['trade_date', 'high', 'low', 'close'])
        df = df.sort_values('trade_date').reset_index(drop=True)
        try:
            _, _, adx_s = _adx_fn(pd.to_numeric(df['high']),
                                  pd.to_numeric(df['low']),
                                  pd.to_numeric(df['close']))
            ma20 = _ma_fn(pd.to_numeric(df['close']), 20)
        except Exception as e:  # pragma: no cover - 指标异常兜底
            logger.warning(f'指数 ADX 计算失败: {e!r}')
            return {'gate': 'closed', 'reason': f'ADX 计算失败: {e!r}'}
        adx_now = adx_s.iloc[-1]
        adx_prev = adx_s.iloc[-2] if len(adx_s) >= 2 else None
        close_now = float(pd.to_numeric(df['close']).iloc[-1])
        ma20_now = ma20.iloc[-1]
        if pd.isna(adx_now) or pd.isna(ma20_now):
            return {'gate': 'closed', 'reason': 'ADX/MA20 warmup 不足'}
        rising = bool(adx_prev is not None and not pd.isna(adx_prev)
                      and float(adx_now) > float(adx_prev))
        gate = _gate_fn(float(adx_now), rising, close_now, float(ma20_now))
        # 弱档判定（P1-10 §4.4）：ADX 闸门 closed，或简化规则「收盘<MA60 且
        # MA60 下行」——两口径取或（任一弱势即不打板）
        close_s = pd.to_numeric(df['close'], errors='coerce')
        ma60 = close_s.rolling(60).mean()
        ma60_now = ma60.iloc[-1]
        ma60_prev = ma60.iloc[-2] if len(ma60) >= 2 else None
        ma60_weak = bool(pd.notna(ma60_now) and ma60_prev is not None
                         and not pd.isna(ma60_prev)
                         and close_now < float(ma60_now)
                         and float(ma60_now) < float(ma60_prev))
        market_weak = (gate == 'closed') or ma60_weak
        return {'gate': gate, 'index': ts_code,
                'adx': round(float(adx_now), 2), 'adx_rising': rising,
                'close': close_now, 'ma20': round(float(ma20_now), 2),
                'ma60': round(float(ma60_now), 2) if pd.notna(ma60_now) else None,
                'market_weak': market_weak,
                'as_of': str(df['trade_date'].iloc[-1])}

    @staticmethod
    def _recent_windows(panel: pd.DataFrame) -> Dict[str, tuple]:
        """P4-1 反抽板 veto 窗口：每股最近 10 日的 streak/涨跌幅/放量序列。

        取自 annotate_panel 产物列（_streak/change_rate/_volume_surge 单一出处）；
        长度取宽（10 日），实际回看天数由 high_board_longyin_recent 按 config
        veto_high_board_lookback 自行截取——两者解耦，config 调整无需改窗口。
        """
        recent = panel.sort_values(['ts_code', 'trade_date']).groupby('ts_code').tail(10)
        out: Dict[str, tuple] = {}
        for ts, g in recent.groupby('ts_code'):
            out[str(ts)] = (list(g['_streak']),
                            list(pd.to_numeric(g['change_rate'], errors='coerce')),
                            list(g['_volume_surge']))
        return out

    def _build_candidates(self, cs_today: pd.DataFrame, as_of: str,
                          clusters: List[Dict[str, Any]],
                          seal_map: Dict[str, Dict[str, Any]],
                          institution: List[Dict[str, Any]],
                          top3_members: Optional[set] = None,
                          recent_windows: Optional[Dict[str, tuple]] = None) -> Dict[str, Any]:
        """转天买入候选池（全量，谓词 candidate_rules 与链模块共用）。

        每类行带链同款理由串 label（如「7板/开板0次」「首板/创新药」）与
        **多维证据标记 evidence**（资金=大单净流入>0 / 机构=龙虎榜机构净买 /
        聚集=概念命中聚集前列 / 板块=净流入Top3行业成分 / 放量=量比≥2，
        量价资金 V1.1 2026-10-07——解决"候选多而无优先级"问题），
        报告展示按证据强度降序截断 top_n、链 CSV 全量（链无证据概念，顺序不约束）。
        去重优先级 R1 > R2 > R3 > R4；库表缺失时对应池为空（不硬造）。
        """
        rules = dict(BUY_RULES)
        rules.update(((self.config.get('review') or {}).get('tomorrow_plan')
                      or {}).get('buy_rules') or {})
        out: Dict[str, Any] = {'available': not cs_today.empty, 'rules': rules,
                               'R1': [], 'R2': [], 'R3': [], 'R4': [],
                               'R1_filtered': []}
        if cs_today.empty:
            return out

        top_concepts = [str(c['concept']) for c in (clusters or [])[:3]]
        concept_map = self._concept_map(as_of)
        # 当日大单净额（候选资金确认列；异动子集口径，无行 None）
        try:
            mf_rows = self.db._query(
                "SELECT ts_code, big_net_amt FROM money_flow_daily "
                "WHERE trade_date = %s AND big_net_amt IS NOT NULL", (as_of,))
            big_net_map = {str(t): float(n) for t, n in mf_rows}
        except Exception as e:
            logger.warning(f'候选资金确认读取失败: {e}')
            big_net_map = {}

        # 聚集家数（P1-7 R2 聚集强度输入；clusters 已是当日聚集前列）
        cluster_counts = {str(c['concept']): int(c['count'])
                          for c in (clusters or [])}

        # 指数 ADX 闸门 + 弱档判定（P1-7 E / P1-10）：前置计算，R1 谓词与
        # 报告层开放度共用（一次计算两消费点）
        adx_info = self._index_adx_gate(as_of)
        market_weak = bool(adx_info.get('market_weak'))

        # VETO 输入预查（P1-7 §4.6 B）：最新报告期净利同比 + 窗口内减持公告
        veto_map = self._candidate_veto_map(as_of, rules)

        # P4-2（B9）：基本面/事件两维数据——估值 digest 复用维度5 同一次装配，
        # 公告窗口与涨停归因同源（_announcement_map）
        valuation_map = self._valuation_map_of_day(as_of) or {}
        ann_map = self._ann_map_of_day(as_of) or {}

        def _score_dims(ts_code: str) -> tuple:
            return (fundamental_dim(valuation_map.get(ts_code)),
                    event_dim(ann_map.get(ts_code)))

        def _cluster_hit(ts_code: str, concepts: Optional[List[str]] = None) -> Optional[int]:
            cs = concepts if concepts is not None else (concept_map.get(ts_code) or [])
            hits = [cluster_counts[c] for c in cs if c in cluster_counts]
            return max(hits) if hits else None

        def _seal_fields(ts_code: str):
            s = seal_map.get(ts_code)
            return (s.get('open_count') if s else None,
                    s.get('first_seal_time') if s else None)

        used: set = set()
        inst_codes = {str(r.get('ts_code') or '') for r in (institution or [])
                      if (r.get('net') or 0) > 0}
        top3_members = set(top3_members or set())

        def _evidence(ts_code: str, big_net: Optional[float],
                      concepts: List[str], volume_surge: Optional[float]) -> tuple:
            """五维证据标记（程序化确定性；报告展示与精选依据）"""
            ev = []
            if big_net is not None and big_net > 0:
                ev.append('资金')
            if ts_code in inst_codes:
                ev.append('机构')
            if concepts and top_concepts and (set(concepts) & set(top_concepts)):
                ev.append('聚集')
            if ts_code in top3_members:
                ev.append('板块')
            if volume_surge is not None and float(volume_surge) >= 2.0:
                ev.append('放量')
            return ev

        # R1 梯队接力（全量涨停截面）
        lu = cs_today[cs_today['_limit_up']]
        # X2：R1 原命中集（raw 谓词、不带弱档闸门）先播种 used——与链侧
        # r2/r3/r4 模块读 upstream R1 全量命中去重同构（r1_ladder_relay.py:73
        # 不传 market_weak）；out['R1'] 输出仍走闸门，弱档日 0 开放不变。
        # 仅作去重输入，不进任何渲染输出。
        def _r1_raw_hit(r) -> bool:
            streak = int(r['_streak']) if pd.notna(r.get('_streak')) else 0
            oc = _seal_fields(str(r['ts_code']))[0]
            return is_r1(streak, bool(r.get('is_st')), oc, rules)
        used |= {str(r['ts_code']) for _, r in lu.iterrows() if _r1_raw_hit(r)}
        r1_rows = []
        # V4.3 A2：R1 拒绝明细（仅连板达标且非 ST 行，连板高度不足者不入避免
        # 噪声）——弱档否决/开板超限的逐股落点，报告层三态归因与链对账解释底表
        r1_filtered: List[Dict[str, Any]] = []
        for _, r in lu.iterrows():
            ts_code = str(r['ts_code'])
            oc, seal_time = _seal_fields(ts_code)
            streak = int(r['_streak']) if pd.notna(r.get('_streak')) else 0
            if not is_r1(streak, bool(r.get('is_st')), oc, rules,
                         market_weak=market_weak):
                if (streak >= int(rules['r1_min_streak'])
                        and not bool(r.get('is_st'))):
                    r1_filtered.append({
                        'ts_code': ts_code, 'code': str(r['stock_code']),
                        'name': str(r['stock_name']), 'streak': int(streak),
                        'open_count': oc,
                        'reason': r1_reject_reason(streak, bool(r.get('is_st')),
                                                   oc, rules,
                                                   market_weak=market_weak)})
                continue
            vs = (_f(r.get('_volume_surge'))
                  if pd.notna(r.get('_volume_surge')) else None)
            big_net = big_net_map.get(ts_code)
            ev = _evidence(ts_code, big_net,
                           concept_map.get(ts_code) or [], vs)
            tier = r1_tier(streak, rules) or ''
            fund, evt = _score_dims(ts_code)
            # P4-1 反抽板 veto（语义修订实施）：近 N 日高位板放量长阴后的涨停 = 反抽板
            ly = (high_board_longyin_recent(
                *(recent_windows or {}).get(ts_code, ([], [], [])), rules=rules)
                if recent_windows else None)
            r1_rows.append({
                'ts_code': ts_code, 'code': str(r['stock_code']),
                'name': str(r['stock_name']), 'streak': streak,
                'open_count': oc, 'seal_time': seal_time,
                'amount': _f(r.get('amount')),
                'big_net': big_net,
                'evidence': ev, 'evidence_count': len(ev),
                'score': _cand_score(big_net, vs, _cluster_hit(ts_code), fund, evt),
                'veto': (list(veto_map.get(ts_code) or [])
                         + ([ly] if ly else [])),
                'label': (f'{tier}/{streak}板/封板数据缺失' if oc is None
                          else f'{tier}/{streak}板/开板{int(oc)}次')})
        r1_rows.sort(key=lambda x: (-x['streak'], -(x['amount'] or 0)))
        for row in r1_rows:
            used.add(row['ts_code'])
        out['R1'] = r1_rows
        out['R1_filtered'] = r1_filtered

        # R2 主线跟随（首板 × 概念全量成分命中聚集前列）
        r2_rows = []
        for _, r in lu.iterrows():
            ts_code = str(r['ts_code'])
            if ts_code in used:
                continue
            streak = int(r['_streak']) if pd.notna(r.get('_streak')) else 0
            concepts = concept_map.get(ts_code) or []
            cc = max([cluster_counts.get(c, 0) for c in concepts
                      if c in set(top_concepts)] or [None]) if concepts else None
            if not is_r2(streak, bool(r.get('is_st')), concepts, top_concepts, rules,
                         big_net=big_net_map.get(ts_code), cluster_count=cc):
                continue
            # P3-4（C9）：补 default——is_r2 通过但交集为空的退化态不抛 StopIteration
            hit = next((c for c in top_concepts if c in set(concepts)),
                       (concepts[0] if concepts else '未知'))
            vs = (_f(r.get('_volume_surge'))
                  if pd.notna(r.get('_volume_surge')) else None)
            big_net = big_net_map.get(ts_code)
            ev = _evidence(ts_code, big_net, concepts, vs)
            fund, evt = _score_dims(ts_code)
            ly = (high_board_longyin_recent(
                *(recent_windows or {}).get(ts_code, ([], [], [])), rules=rules)
                if recent_windows else None)
            r2_rows.append({
                'ts_code': ts_code, 'code': str(r['stock_code']),
                'name': str(r['stock_name']), 'streak': streak,
                'concepts': concepts, 'hit_concept': hit,
                'cluster_count': cc,
                'amount': _f(r.get('amount')),
                'big_net': big_net,
                'evidence': ev, 'evidence_count': len(ev),
                'score': _cand_score(big_net, vs, cc, fund, evt),
                'veto': (list(veto_map.get(ts_code) or [])
                         + ([ly] if ly else [])),
                'label': f'首板/{hit}' + (f'(聚集{cc}家)' if cc is not None else '')})
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
            big_net = big_net_map.get(ts_code)
            # 反高开低走输入（P1-7 A）：开盘涨幅 + 收盘<开盘；缺失不过滤
            open_gain = None
            close_lt_open = False
            try:
                o_v, pc_v, c_v = (float(r.get('open')), float(r.get('preclose')),
                                  float(r.get('close')))
                if pc_v > 0:
                    open_gain = (o_v - pc_v) / pc_v * 100.0
                    close_lt_open = c_v < o_v
            except (TypeError, ValueError):
                open_gain, close_lt_open = None, False
            if not is_r3(True, bool(r.get('is_st')), volume_surge,
                         change_rate, rules, big_net=big_net,
                         open_gain=open_gain, close_lt_open=close_lt_open):
                continue
            ev = _evidence(ts_code, big_net,
                           concept_map.get(ts_code) or [], volume_surge)
            fund, evt = _score_dims(ts_code)
            r3_rows.append({
                'ts_code': ts_code, 'code': str(r['stock_code']),
                'name': str(r['stock_name']), 'amount': _f(r.get('amount')),
                'change_rate': change_rate, 'volume_surge': volume_surge,
                'big_net': big_net,
                'evidence': ev, 'evidence_count': len(ev),
                'score': _cand_score(big_net, volume_surge,
                                     _cluster_hit(ts_code), fund, evt),
                'veto': veto_map.get(ts_code) or [],
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
            ev = ['机构']
            if ts_code in top3_members:
                ev.append('板块')
            fund, evt = _score_dims(ts_code)
            r4_rows.append({
                'ts_code': ts_code, 'code': ts_code.split('.')[-1],
                'name': name or '—', 'net': _f(net),
                'evidence': ev, 'evidence_count': len(ev),
                'score': _cand_score(net, None, None, fund, evt),
                'veto': veto_map.get(ts_code) or [],
                'label': f'机构净买{net / 1e8:+.2f}亿'})
        out['R4'] = r4_rows

        # VETO 分流（P1-7 §4.6 B）：命中一票否决的候选移出主池、记入 vetoed
        # （报告层点名「出池股及原因」——禁静默缩池）
        vetoed: List[Dict[str, Any]] = []
        for ctype in ('R1', 'R2', 'R3', 'R4'):
            kept, dropped = [], []
            for row in out[ctype]:
                (dropped if row.get('veto') else kept).append(row)
            out[ctype] = kept
            for row in dropped:
                vetoed.append({'type': ctype, 'code': row.get('code'),
                               'name': row.get('name'),
                               'reasons': row.get('veto')})
        out['vetoed'] = vetoed
        out['vetoed_count'] = len(vetoed)
        out['adx_gate'] = adx_info
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


    def _theme_tree(self, cs_today: pd.DataFrame, cs_prev: pd.DataFrame, as_of: str,
                    clusters: List[Dict[str, Any]], concept_map: Dict[str, List[str]],
                    exclude: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """题材跨日事件树（四期方案 §3；vibe-astock theme_tree 口径）：

        对涨停聚集前 3 概念（黑名单剔除后）：昨日题材涨停家数→今日家数（扩散/收敛）、
        昨日题材涨停股今日均值/翻红率/晋级数、今日题材内炸板数与最高板、题材内首封
        时间范围（首末拉开=发酵判读）。概念成分为 sina_gn 时点匹配口径。
        """
        blacklist = [str(x) for x in (exclude or [])]
        top = [c['concept'] for c in (clusters or [])[:3]
               if not any(b in c['concept'] for b in blacklist)]
        if not top or cs_today.empty:
            return []
        members: Dict[str, set] = {}
        for ts_code, concepts in concept_map.items():
            for c in concepts:
                if c in top:
                    members.setdefault(c, set()).add(ts_code)

        change = pd.to_numeric(cs_today['change_rate'], errors='coerce')
        today: Dict[str, Dict[str, Any]] = {}
        lu = cs_today['_limit_up'].fillna(False).astype(bool)
        bu = cs_today['_burst'].fillna(False).astype(bool)
        for ts_code, chg, up, burst, streak in zip(cs_today['ts_code'], change, lu, bu,
                                                   cs_today['_streak']):
            today[str(ts_code)] = {
                'chg': None if pd.isna(chg) else float(chg), 'up': bool(up),
                'burst': bool(burst),
                'streak': int(streak) if pd.notna(streak) else 0}
        prev_up: set = set()
        if not cs_prev.empty:
            pu = cs_prev[cs_prev['_limit_up'].fillna(False).astype(bool)]
            prev_up = {str(c) for c in pu['ts_code']}
        seal_map = self._seal_map_of_day(as_of)

        rows: List[Dict[str, Any]] = []
        for concept in top:
            mem = members.get(concept) or set()
            today_up = [ts for ts in mem if today.get(ts, {}).get('up')]
            prev_up_mem = [ts for ts in mem if ts in prev_up]
            chgs = [today[ts]['chg'] for ts in prev_up_mem
                    if today.get(ts) and today[ts]['chg'] is not None]
            relimit = sum(1 for ts in prev_up_mem if today.get(ts, {}).get('up'))
            burst_n = sum(1 for ts in mem if today.get(ts, {}).get('burst'))
            max_streak = max((today[ts]['streak'] for ts in today_up), default=0)
            seals = sorted(int(str(seal_map[ts]['first_seal_time'])[:6])
                           for ts in today_up
                           if seal_map.get(ts) and seal_map[ts].get('first_seal_time')
                           and str(seal_map[ts]['first_seal_time'])[:6].isdigit())
            if len(today_up) > len(prev_up_mem):
                flow = '扩散'
            elif len(today_up) < len(prev_up_mem):
                flow = '收敛'
            else:
                flow = '持平' if prev_up_mem else '—'
            rows.append({
                'concept': concept, 'prev_up': len(prev_up_mem),
                'today_up': len(today_up), 'flow': flow,
                'prev_mean': round(sum(chgs) / len(chgs), 2) if chgs else None,
                'red_rate': round(sum(1 for c in chgs if c > 0) / len(chgs), 4)
                if chgs else None,
                'relimit': relimit, 'burst': burst_n, 'max_streak': max_streak,
                'seal_first': _fmt_hhmm(seals[0]) if seals else None,
                'seal_last': _fmt_hhmm(seals[-1]) if seals else None,
            })
        return rows


def _ladder_gap(ladder: Dict[int, int]) -> Optional[Dict[str, Any]]:
    """梯队断层（四期方案 §3；vibe-astock ladder_gap）：最高板与最低板间缺失档位。"""
    heights = sorted(int(h) for h in (ladder or {}) if int(h) >= 2)
    if not heights:
        return None
    lo, hi = heights[0], heights[-1]
    gaps = [h for h in range(lo, hi + 1) if h not in heights]
    return {'min': lo, 'max': hi, 'gaps': gaps}


def _theme_concentration(clusters: List[Dict[str, Any]], cs_today: pd.DataFrame,
                         exclude: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    """头部题材集中度（四期方案 §3）：第一题材涨停数 ÷ 全市场涨停数（越高越一枝独秀）。"""
    if not clusters or cs_today.empty:
        return None
    blacklist = [str(x) for x in (exclude or [])]
    first = next((c for c in clusters
                  if not any(b in c['concept'] for b in blacklist)), None)
    if not first:
        return None
    mask = cs_today['_limit_up'].fillna(False).astype(bool)
    total = int(mask.sum())
    if not total:
        return None
    return {'concept': first['concept'], 'count': first['count'],
            'rate': round(first['count'] / total, 4)}


def _fmt_hhmm(t: int) -> str:
    """整型 HHMMSS 时间 92500 → '09:25'。"""
    return f'{t // 10000:02d}:{t // 100 % 100:02d}'


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
