"""链级截面环境装配（tomorrow_candidates 链专用，fx_hkdcny 装配机制的 A 股扩展）。

链 YAML 声明 ``env: a_share_candidates`` 时，引擎在数据加载后调用本模块装配
截面查表数据（一次查询 → dict，逐股模块经 ``ctx.upstream`` 只读），模块不做
任何网络/数据库访问（模块纯函数红线不变）。

装配键（模块只读，逐股按 ``ctx.stock.ts_code`` 查）：

- ``cand_seal_open_map``：{ts_code: 开板次数}（limit_seal_stats_daily，分笔
  自算口径；无行 = 该股无封板数据）；
- ``cand_theme_map``：{ts_code: [概念名]}（sina_gn 时点匹配，全部成分——
  非报告层题材候选截断串，避免前 3 截断噪声）；
- ``cand_top_concepts``：涨停聚集前列概念名列表（概念内当日涨停家数降序
  前 3，黑名单剔除——与报告层 _limit_up_clusters 同口径）；
- ``cand_institution_net_map``：{ts_code: 机构净买(元)}（lhb_detail 席位名含
  「机构专用」归并，净买降序前 5——与报告层机构榜同 SQL 同 LIMIT）。

口径一致性：涨停集合判定复用 limit_up_rules（change_rate 阈值 + close≥high
护栏，与 limit_up_today 模块/报告层重算同口径）。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from .limit_up_rules import board_of, limit_pct_series

logger = logging.getLogger(__name__)

#: 环境装配标识（链 YAML env 字段合法值；新增装配场景时在此登记）
ENV_A_SHARE_CANDIDATES = 'a_share_candidates'

#: 概念来源（与报告层 aggregator.SOURCE_SINA_GN 一致）
_SOURCE_SINA_GN = 'sina_gn'

#: 聚集前列概念数量（与报告层 tomorrow_plan._buy_candidates 的 clusters[:3] 一致）
_TOP_CONCEPT_N = 3


def assemble_env(env_name: str, reader, as_of: str, config_manager) -> Dict[str, Any]:
    """按链声明的 env 标识装配截面环境值；未知标识直接报错（防拼错静默空装配）。"""
    if env_name != ENV_A_SHARE_CANDIDATES:
        raise ValueError(
            f'链配置 env 非法: {env_name}（可用: {ENV_A_SHARE_CANDIDATES}）')
    review_cfg = (config_manager.load_config().get('review') or {})
    blacklist = [str(x) for x in (review_cfg.get('theme_exclude_concepts') or [])]

    limit_up_codes = _limit_up_codes(reader, as_of)
    theme_map = _theme_map(reader, as_of)
    top_concepts = _top_concepts(theme_map, limit_up_codes, blacklist)
    seal_map = _seal_open_map(reader, as_of)
    inst_map = _institution_net_map(reader, as_of)

    logger.info('候选环境装配：涨停 %d 只 / 概念映射 %d 股 / 聚集前列 %s / '
                '封板数据 %d 股 / 机构净买前 5 %d 只',
                len(limit_up_codes), len(theme_map), top_concepts,
                len(seal_map), len(inst_map))
    return {
        'cand_seal_open_map': seal_map,
        'cand_theme_map': theme_map,
        'cand_top_concepts': top_concepts,
        'cand_institution_net_map': inst_map,
    }


def _limit_up_codes(reader, as_of: str) -> set:
    """锚定日全市场涨停代码集（change_rate 阈值 + close≥high，同链模块口径）。"""
    try:
        rows = reader._query(
            "SELECT ts_code, stock_code, trade_date, change_rate, close, high, is_st "
            "FROM his_kline_day WHERE trade_date = %s", (as_of,))
    except Exception as e:  # pragma: no cover - 库异常兜底
        logger.warning('涨停截面读取失败: %r', e)
        return set()
    if not rows:
        return set()
    df = pd.DataFrame(rows, columns=['ts_code', 'stock_code', 'trade_date',
                                     'change_rate', 'close', 'high', 'is_st'])
    codes: set = set()
    for board, g in df.groupby(df['stock_code'].astype(str).map(board_of)):
        pct = limit_pct_series(g, board)
        change = pd.to_numeric(g['change_rate'], errors='coerce')
        hit = change.ge(pct - 0.10)
        close = pd.to_numeric(g['close'], errors='coerce')
        high = pd.to_numeric(g['high'], errors='coerce')
        guard = close.ge(high).where(high.notna(), True)
        hit = (hit & guard & pct.notna() & change.notna()).fillna(False)
        codes.update(g.loc[hit, 'ts_code'].astype(str))
    return codes


def _theme_map(reader, as_of: str) -> Dict[str, List[str]]:
    """ts_code → 概念名清单（sina_gn 时点匹配；同报告层 _concept_map SQL）。"""
    try:
        rows = reader._query(
            """
            SELECT m.ts_code, c.concept_name
            FROM sector_concept_map m
            JOIN sector_concept c ON c.concept_code = m.concept_code
            WHERE c.source = %s
              AND (m.in_date IS NULL OR m.in_date <= %s)
              AND (m.out_date IS NULL OR m.out_date > %s)
            """, (_SOURCE_SINA_GN, as_of, as_of))
    except Exception as e:  # pragma: no cover - 库异常兜底
        logger.warning('概念映射读取失败: %r', e)
        return {}
    out: Dict[str, List[str]] = {}
    for ts_code, name in rows:
        out.setdefault(str(ts_code), []).append(str(name))
    return out


def _top_concepts(theme_map: Dict[str, List[str]], limit_up_codes: set,
                  blacklist: List[str]) -> List[str]:
    """涨停聚集前列概念（概念内涨停家数降序，黑名单子串剔除；前 3）。"""
    def _blocked(name: str) -> bool:
        return any(b in name for b in blacklist)

    counts: Dict[str, int] = {}
    for ts_code in limit_up_codes:
        for name in theme_map.get(ts_code, []):
            if not _blocked(name):
                counts[name] = counts.get(name, 0) + 1
    ranked = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return [name for name, _ in ranked[:_TOP_CONCEPT_N]]


def _seal_open_map(reader, as_of: str) -> Dict[str, Optional[int]]:
    """ts_code → 开板次数（limit_seal_stats_daily；同报告层 _seal_map SQL）。"""
    try:
        rows = reader._query(
            "SELECT ts_code, open_count FROM limit_seal_stats_daily "
            "WHERE trade_date = %s", (as_of,))
    except Exception as e:  # pragma: no cover - 库异常兜底
        logger.warning('封板数据读取失败: %r', e)
        return {}
    return {str(r[0]): (int(r[1]) if r[1] is not None else None) for r in rows}


def _institution_net_map(reader, as_of: str) -> Dict[str, float]:
    """ts_code → 机构净买(元)（lhb_detail 机构专用归并，净买降序前 5——
    与报告层机构榜同 SQL 同 LIMIT，链命中即报告前 5）。"""
    try:
        rows = reader._query(
            """
            SELECT x.ts_code, (x.buy - x.sell) AS net FROM (
                SELECT d.ts_code,
                       SUM(CASE WHEN d.side LIKE '买%%' THEN d.amount ELSE 0 END) AS buy,
                       SUM(CASE WHEN d.side LIKE '卖%%' THEN d.amount ELSE 0 END) AS sell
                FROM lhb_detail d
                WHERE d.trade_date = %s AND d.seat_name LIKE '%%机构专用%%'
                GROUP BY d.ts_code
            ) x
            ORDER BY net DESC NULLS LAST
            LIMIT 5
            """, (as_of,))
    except Exception as e:  # pragma: no cover - 库异常兜底
        logger.warning('机构席位读取失败: %r', e)
        return {}
    return {str(r[0]): float(r[1]) for r in rows if r[1] is not None}
