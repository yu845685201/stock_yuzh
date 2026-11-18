"""候选 VETO 一票否决数据装载（P4-1/B3：报告层与链侧单一出处）。

报告层（``review.aggregator._candidate_veto_map``）与链侧（env_assembly
``a_share_candidates`` → upstream ``cand_veto_map`` → R 模块否决入口）共用
:func:`load_candidate_veto_map`，保证两入口否决清单一致（P4-1 验收）。

查询失败 → 对应项不过滤（不硬造否决）。高位板长阴为行级谓词
（``candidate_rules.veto_reasons`` 的 streak/volume_surge/change_rate 入参），
不在本 map 内——其三条件在涨停池内结构性互斥（见 veto_reasons docstring
P4-1 注记），语义修订属结论性规则变更，待用户决策。
"""

from __future__ import annotations

import logging
from datetime import datetime, time as dtime, timedelta
from typing import Any, Dict, Optional

import pandas as pd

from .candidate_rules import veto_reasons

logger = logging.getLogger(__name__)

#: P4-6（B10）：龙虎榜发布时点——交易所 17:30 后发布，当日盘后早跑不得把当日榜
#: 当已发布呈现（防部分交易所先发布的部分数据被当全量引用）
LHB_PUBLISH_TIME = dtime(17, 30)


def lhb_published(as_of: str, now: Optional[datetime] = None) -> bool:
    """龙虎榜是否已到可引用时点（P4-6 门控；as_of=当天且未到 17:30 → False）。

    历史 as_of（早于当天）恒为已发布；now 可注入便于测试。
    （原实现于 review.aggregator，P4-1 移至本模块单一出处，聚合器 re-export。）
    """
    now = now or datetime.now()
    return str(as_of) != now.strftime('%Y%m%d') or now.time() >= LHB_PUBLISH_TIME


def load_candidate_veto_map(db, as_of: str, rules: Dict[str, Any],
                            now: Optional[datetime] = None) -> Dict[str, list]:
    """VETO 预查：①业绩暴雷（披露约束 C3）②大股东减持 ③龙虎榜卖一占比。

    ③口径（P4-1 接线）：per-stock ``SUM(side='卖1')/SUM(side LIKE '卖%')×100``，
    超过 ``rules['veto_lhb_sell1_pct']`` 即否决。lhb_detail 无榜单 reason 列，
    同股多榜单时为加权混合——稀释方向偏保守（不放大否决），该近似在理由串
    如实呈现百分比、不做行粒度拆分。未到发布时点（:func:`lhb_published`，
    P4-6 门控）或查询失败 → 该项跳过（不过滤，不硬造）。

    ST/*ST 与高位板长阴不在本 map：前者由各池谓词（is_r1/is_r2/is_r3/is_r4）
    直接排除，后者为行级谓词（见模块 docstring）。
    """
    profit_map: Dict[str, float] = {}
    try:
        rows = db._query(
            """
            SELECT DISTINCT ON (ts_code) ts_code, net_profit_yoy
            FROM base_financial_report
            WHERE net_profit_yoy IS NOT NULL
              AND COALESCE(disclosure_date,
                           to_char(to_date(stat_date, 'YYYYMMDD') + 304,
                                   'YYYYMMDD')) <= %s
            ORDER BY ts_code,
                     COALESCE(disclosure_date,
                              to_char(to_date(stat_date, 'YYYYMMDD') + 304,
                                      'YYYYMMDD')) DESC,
                     stat_date DESC
            """, (str(as_of),))
        profit_map = {str(t): float(v) for t, v in rows if v is not None}
    except Exception as e:
        logger.warning(f'VETO 业绩同比读取失败: {e}')

    reduction_codes: set = set()
    try:
        end = datetime.strptime(str(as_of), '%Y%m%d')
        start = (end - timedelta(days=2)).strftime('%Y%m%d')
        rows = db._query(
            "SELECT DISTINCT ts_code FROM announcement_daily "
            "WHERE ann_type = '增减持' AND ann_date BETWEEN %s AND %s",
            (start, str(as_of)))
        reduction_codes = {str(r[0]) for r in rows}
    except Exception as e:
        logger.warning(f'VETO 减持公告读取失败: {e}')

    sell1_map: Dict[str, float] = {}
    if lhb_published(as_of, now=now):
        try:
            rows = db._query(
                """
                SELECT ts_code,
                       100.0 * SUM(CASE WHEN side = '卖1' THEN amount END)
                       / NULLIF(SUM(CASE WHEN side LIKE '卖%%' THEN amount END), 0)
                FROM lhb_detail
                WHERE trade_date = %s
                GROUP BY ts_code
                """, (str(as_of),))
            sell1_map = {str(t): float(v) for t, v in rows if v is not None}
        except Exception as e:
            logger.warning(f'VETO 龙虎榜卖一占比读取失败: {e}')
    else:
        logger.info('龙虎榜未到发布时点（P4-6 门控），卖一占比 VETO 跳过')

    out: Dict[str, list] = {}
    for ts_code in sorted(set(profit_map) | reduction_codes | set(sell1_map)):
        reasons = veto_reasons(
            profit_yoy=profit_map.get(ts_code),
            has_reduction_ann=ts_code in reduction_codes,
            lhb_sell1_pct=sell1_map.get(ts_code),
            rules=rules)
        if reasons:
            out[ts_code] = reasons
    return out


def recent_longyin_window(ordered: pd.DataFrame, board: str,
                          list_date: Optional[str] = None,
                          rules: Optional[Dict[str, Any]] = None) -> Optional[str]:
    """链侧反抽板检测（P4-1 语义修订）：单股 df → 逐日序列 → 判据纯函数。

    与报告层 panel 同口径的单股等价计算：annotate_limit_up（链
    consecutive_limit_up 同一函数）+ 逐日 streak（_add_streak 语义：连续
    up∧known 含当日）+ volume_surge（_add_rolling_metrics 同式，groupby
    单股退化）。序列长度不足 / 缺 volume 列 → None（不硬造）。
    """
    if ordered is None or ordered.empty or 'volume' not in ordered.columns:
        return None
    from .candidate_rules import high_board_longyin_recent
    from .limit_up_rules import annotate_limit_up
    from ..review.cross_section import VOLUME_SURGE_WINDOW  # 同值常量（无反向依赖）
    o = ordered.sort_values('trade_date').reset_index(drop=True)
    ann = annotate_limit_up(o, str(o['ts_code'].iloc[0]) if 'ts_code' in o.columns else '',
                            board, list_date=list_date)
    up = ann['_limit_up'].to_numpy(dtype=bool)
    known = ann['_limit_known'].to_numpy(dtype=bool)
    streaks, s = [], 0
    for u, k in zip(up, known):
        s = s + 1 if (u and k) else 0
        streaks.append(s)
    volume = pd.to_numeric(o['volume'], errors='coerce')
    valid_vol = volume.where(volume.gt(0))
    prev_mean = valid_vol.shift(1).rolling(VOLUME_SURGE_WINDOW,
                                           min_periods=VOLUME_SURGE_WINDOW).mean()
    vs = (volume / prev_mean).round(4)
    cr = pd.to_numeric(o['change_rate'], errors='coerce')
    r = dict(rules or {})
    n = int(r.get('veto_high_board_lookback', 3)) + 1
    return high_board_longyin_recent(streaks[-n:], cr.tolist()[-n:], vs.tolist()[-n:],
                                     rules)
