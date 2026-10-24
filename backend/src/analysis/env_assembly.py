"""链级截面环境装配（daily_review 链 R 系列与报告层共用，fx_hkdcny 装配机制的 A 股扩展）。

链 YAML 声明 ``env`` 时，引擎在数据加载后调用本模块装配截面查表数据
（一次查询 → dict，逐股模块经 ``ctx.upstream`` 只读），模块不做任何网络/
数据库访问（模块纯函数红线不变）。

装配键（模块只读，逐股按 ``ctx.stock.ts_code`` 查）：

- ``cand_seal_open_map``：{ts_code: 开板次数}（limit_seal_stats_daily，分笔
  自算口径；无行 = 该股无封板数据）；
- ``cand_theme_map``：{ts_code: [概念名]}（sina_gn 时点匹配，全部成分——
  非报告层题材候选截断串，避免前 3 截断噪声）；
- ``cand_top_concepts``：涨停聚集前列概念名列表（概念内当日涨停家数降序
  前 3，黑名单剔除——与报告层 _limit_up_clusters 同口径）；
- ``cand_cluster_counts``：{概念名: 当日涨停家数}（聚集强度，全量不限前 N——
  P1-7 R2 聚集强度阈值的输入，R2 模块只读）；
- ``cand_institution_net_map``：{ts_code: 机构净买(元)}（lhb_detail 席位名含
  「机构专用」归并，净买降序前 5——与报告层机构榜同 SQL 同 LIMIT）；
- ``a_share_rps``（P1-12）：{ts_code: {20: RPS, 60: RPS}}（截面相对强弱
  百分位——各股窗口前复权收盘涨幅的全市场同日 rank(pct)×100，窗口 [20,60]）；
- ``valuation_map``（``a_share_valuation``）：{ts_code: ValuationDigest}
  （stock_valuation_daily 当日行 + 近 1 年 pe_ttm 分位 + 3 年 TTM CAGR +
  消化年数；报告层 aggregator 复用同一 assemble_valuation，原则 P6）。

口径一致性：涨停集合判定复用 limit_up_rules（change_rate 阈值 + close≥high
护栏，与 limit_up_today 模块/报告层重算同口径）。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from ..review.valuation_calc import (
    OK, pe_deducted_annualized, pe_digestion_years, peg, profit_cagr_hist,
)
from ..utils.financial_ttm import single_quarter, ttm_series
from .limit_up_rules import board_of, limit_pct_series

logger = logging.getLogger(__name__)

#: 环境装配标识（链 YAML env 字段合法值；新增装配场景时在此登记）
ENV_A_SHARE_CANDIDATES = 'a_share_candidates'
ENV_A_SHARE_VALUATION = 'a_share_valuation'
ENV_A_SHARE_RPS = 'a_share_rps'

#: 估值装配默认参数（config review.valuation_digest 可覆盖）
_VALUATION_DEFAULTS = {
    'pe_anchor': 25.0,          # 消化年数锚定 PE（A 股常用 25x 锚）
    'percentile_history_days': 250,   # 分位历史窗口（自然日≈1 年）
    'cagr_years': 3,            # CAGR 回溯年数
    'min_samples': 20,          # 分位最少样本（方案 §3.3.1）
    'report_periods': 14,       # TTM 序列加载的报告期数（3 年 + 缓冲）
}


@dataclass(frozen=True)
class ValuationDigest:
    """个股估值摘要（模块输出与报告层共用的最小面）。"""

    pe_ttm: Optional[float]
    pb: Optional[float]
    total_mv: Optional[float]
    pe_percentile: Optional[float]      # 近 1 年分位（%）；样本不足 None
    percentile_samples: int
    cagr_pct: Optional[float]           # 3 年 TTM CAGR（%/年）；不足 None
    digestion_years: Optional[float]    # 消化至锚定 PE 年数；负增长 None
    digestion_reason: str
    peg_value: Optional[float]
    # 年化 PE（归母单季×4 降级口径——库内无扣非列，spike 结论 2026-10-20）
    annualized_pe: Optional[float] = None
    annualized_reason: str = ''


def _rps_map(reader, as_of: str, windows: Sequence[int] = (20, 60)) -> Dict[str, Dict[int, float]]:
    """ts_code → {窗口: RPS 百分位}（P1-12 §4.5：截面相对强弱排名）。

    口径：各股窗口内前复权收盘涨幅 = close[T] / close[T-w] - 1（T = as_of 前
    最后一个有价日），全市场同日截面 ``rank(pct)`` 转 0~100 百分位
    （qstock 的 RPS 口径：值越大越强）。窗口 [20, 60] 两档起步
    （五档按需扩）；历史不足窗口的股票该档记 None（不硬造）。
    """
    max_w = max(windows)
    try:
        rows = reader._query(
            """
            SELECT ts_code, trade_date, close FROM (
                SELECT ts_code, trade_date, close,
                       ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                FROM his_kline_day
                WHERE trade_date <= %s AND close IS NOT NULL
            ) t WHERE rn <= %s ORDER BY ts_code, trade_date
            """, (as_of, max_w + 1))
    except Exception as e:  # pragma: no cover - 库异常兜底
        logger.warning('RPS 收盘价读取失败: %r', e)
        return {}
    if not rows:
        return {}
    df = pd.DataFrame(rows, columns=['ts_code', 'trade_date', 'close'])
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    out: Dict[str, Dict[int, float]] = {}
    for w in windows:
        pct: Dict[str, float] = {}
        for ts_code, g in df.groupby('ts_code', sort=False):
            g = g.sort_values('trade_date')
            if len(g) < w + 1:
                continue
            base = g['close'].iloc[-(w + 1)]
            last = g['close'].iloc[-1]
            if base and base > 0:
                pct[str(ts_code)] = float(last) / float(base) - 1.0
        if not pct:
            continue
        s = pd.Series(pct)
        rps = (s.rank(pct=True) * 100.0).round(2)
        for ts_code, v in rps.items():
            out.setdefault(ts_code, {})[int(w)] = float(v)
    return out


def assemble_rps(reader, as_of: str) -> Dict[str, Any]:
    """env ``a_share_rps``：{ts_code: {20: RPS, 60: RPS}}（P1-12）。"""
    return _rps_map(reader, as_of)


def assemble_valuation(reader, as_of: str, config_manager) -> Dict[str, ValuationDigest]:
    """装配个股估值摘要（P0-4 §3.3.3；报告层 aggregator 复用同一函数）。

    三次查询：① stock_valuation_daily 当日行；② 近 1 年 pe_ttm 历史（分位）；
    ③ base_financial_report 近 N 期（TTM 序列 → CAGR）。calc 全部走
    review.valuation_calc（禁 inf/NaN；样本不足显式 None）。
    """
    cfg = ((config_manager.load_config().get('review') or {})
           .get('valuation_digest') or {})
    p = {**_VALUATION_DEFAULTS, **{k: v for k, v in cfg.items() if v is not None}}
    hist_days = int(p['percentile_history_days'])

    # ① 当日估值行（自算口径：PE(TTM)=总市值÷近四季归母净利，PB(LF)=市值÷净资产）
    anchor_rows = reader._query(
        "SELECT ts_code, total_mv, pe_ttm, pb FROM stock_valuation_daily "
        "WHERE trade_date = %s", (as_of,))
    anchor = pd.DataFrame(anchor_rows,
                          columns=['ts_code', 'total_mv', 'pe_ttm', 'pb'])
    if anchor.empty:
        logger.info('估值装配：锚定日 %s 无 stock_valuation_daily 行', as_of)
        return {}

    # ② 近 1 年 pe_ttm 历史（分位窗口；含锚定日自身）。
    # 分位 = count(history ≤ anchor)/n（向量化计数，不逐股回传序列）；
    # n < min_samples → None（方案 §3.3.1 禁小样本分位）。
    start = reader.nth_trading_day_back(as_of, hist_days) or as_of
    hist_rows = reader._query(
        "SELECT ts_code, pe_ttm FROM stock_valuation_daily "
        "WHERE trade_date BETWEEN %s AND %s AND pe_ttm > 0", (start, as_of))
    hist = pd.DataFrame(hist_rows, columns=['ts_code', 'pe_ttm'])
    pct_map: Dict[str, Any] = {}
    if not hist.empty:
        h = hist.merge(anchor[['ts_code', 'pe_ttm']], on='ts_code',
                       suffixes=('', '_anchor'))
        h['below'] = h['pe_ttm'] <= h['pe_ttm_anchor']
        agg = h.groupby('ts_code').agg(n=('pe_ttm', 'size'),
                                       below=('below', 'sum'))
        for ts_code, r in agg.iterrows():
            n = int(r['n'])
            pct_map[ts_code] = {
                'pct': (float(r['below']) / n * 100)
                if n >= int(p['min_samples']) else None,
                'n': n,
            }

    # ③ 财报 TTM 序列 → CAGR；同期算单季 → 年化 PE（归母×4 降级口径）
    fin_rows = reader._query(
        "SELECT ts_code, stat_date, net_profit FROM base_financial_report "
        "WHERE stat_date >= %s ORDER BY ts_code, stat_date",
        (str(int(as_of[:4]) - int(p['cagr_years']) - 1) + '0101',))
    cagr_map: Dict[str, float] = {}
    annualized_map: Dict[str, Any] = {}
    if fin_rows:
        reports = [{'ts_code': r[0], 'stat_date': r[1], 'net_profit': r[2]}
                   for r in fin_rows]
        series_df = ttm_series(reports)
        years = float(p['cagr_years'])
        for ts_code, g in series_df.groupby('ts_code'):
            # 过滤 None/NaN（输入不足期；profit_cagr_hist 的 _finite 双保险）
            vals = [v for v in g['ttm_profit'].tolist()
                    if v is not None and pd.notna(v)]
            res = profit_cagr_hist(vals, years)
            if res.status == OK and res.value is not None:
                cagr_map[ts_code] = res.value
        # 单季换算：最新期累计 − 上年同期累计（同一 reports，按 (ts_code, md) 配对）
        fin_df = pd.DataFrame(reports)
        fin_df['year'] = fin_df['stat_date'].astype(str).str[:4]
        fin_df['md'] = fin_df['stat_date'].astype(str).str[4:8]
        prev_same = fin_df[['ts_code', 'year', 'md', 'net_profit']].copy()
        prev_same['year'] = (prev_same['year'].astype(int) + 1).astype(str)
        prev_same = prev_same.rename(columns={'net_profit': 'prev_same_cum'})
        merged = fin_df.merge(prev_same, on=['ts_code', 'year', 'md'], how='inner')
        for _, r in merged.iterrows():
            sq = single_quarter(r['net_profit'], r['prev_same_cum'])
            hit = anchor[anchor['ts_code'] == r['ts_code']]
            mv = (float(hit['total_mv'].iloc[0])
                  if not hit.empty and pd.notna(hit['total_mv'].iloc[0]) else None)
            annualized_map[r['ts_code']] = pe_deducted_annualized(sq, mv)

    out: Dict[str, ValuationDigest] = {}
    for _, row in anchor.iterrows():
        ts_code = str(row['ts_code'])
        pe = float(row['pe_ttm']) if pd.notna(row['pe_ttm']) else None
        pct_info = pct_map.get(ts_code) or {'pct': None, 'n': 0}
        cagr = cagr_map.get(ts_code)
        digest = pe_digestion_years(pe, float(p['pe_anchor']), cagr)
        peg_res = peg(pe, cagr)
        ann = annualized_map.get(ts_code)
        out[ts_code] = ValuationDigest(
            pe_ttm=pe,
            pb=float(row['pb']) if pd.notna(row['pb']) else None,
            total_mv=float(row['total_mv']) if pd.notna(row['total_mv']) else None,
            pe_percentile=pct_info['pct'],
            percentile_samples=pct_info['n'],
            cagr_pct=cagr,
            digestion_years=digest.value if digest.status == OK else None,
            digestion_reason=digest.reason,
            peg_value=peg_res.value if peg_res.status == OK else None,
            annualized_pe=ann.value if (ann is not None and ann.status == OK) else None,
            annualized_reason=ann.reason if ann is not None else '财报期不足（单季不可算）',
        )
    logger.info('估值装配：锚定日 %s，%d 只（分位可算 %d / CAGR 可算 %d）',
                as_of, len(out),
                sum(1 for d in out.values() if d.pe_percentile is not None),
                sum(1 for d in out.values() if d.cagr_pct is not None))
    return out

#: 概念来源（与报告层 aggregator.SOURCE_SINA_GN 一致）
_SOURCE_SINA_GN = 'sina_gn'

#: 聚集前列概念数量（与报告层 tomorrow_plan._buy_candidates 的 clusters[:3] 一致）
_TOP_CONCEPT_N = 3


#: 可用环境装配标识（链 YAML env 字段合法值；新增装配场景时在此登记，
#: 并在 assemble_env 中加分派——未知标识直接报错，防拼错静默空装配）
ENV_ASSEMBLERS = (ENV_A_SHARE_CANDIDATES, ENV_A_SHARE_VALUATION, ENV_A_SHARE_RPS)


def assemble_env(env_name: str, reader, as_of: str, config_manager) -> Dict[str, Any]:
    """按链声明的 env 标识装配截面环境值；未知标识直接报错（防拼错静默空装配）。"""
    if env_name == ENV_A_SHARE_CANDIDATES:
        return _assemble_candidates(reader, as_of, config_manager)
    if env_name == ENV_A_SHARE_VALUATION:
        return {'valuation_map': assemble_valuation(reader, as_of, config_manager)}
    if env_name == ENV_A_SHARE_RPS:
        return {'rps_map': assemble_rps(reader, as_of)}
    raise ValueError(
        f'链配置 env 非法: {env_name}（可用: {ENV_A_SHARE_CANDIDATES}, '
        f'{ENV_A_SHARE_VALUATION}, {ENV_A_SHARE_RPS}）')


def _assemble_candidates(reader, as_of: str, config_manager) -> Dict[str, Any]:
    """候选环境装配（原 assemble_env 主体；R 系列 + money_trend 用）。"""
    review_cfg = (config_manager.load_config().get('review') or {})
    blacklist = [str(x) for x in (review_cfg.get('theme_exclude_concepts') or [])]
    ann_window = int(((review_cfg.get('attribution') or {})
                      .get('ann_window_days')) or 3)

    limit_up_codes = _limit_up_codes(reader, as_of)
    theme_map = _theme_map(reader, as_of)
    top_concepts = _top_concepts(theme_map, limit_up_codes, blacklist)
    cluster_counts = _concept_cluster_counts(theme_map, limit_up_codes, blacklist)
    seal_map = _seal_open_map(reader, as_of)
    inst_map = _institution_net_map(reader, as_of)
    mf_history_map = _mf_history_map(reader, as_of)
    announcement_map = _announcement_map(reader, as_of, ann_window)

    logger.info('候选环境装配：涨停 %d 只 / 概念映射 %d 股 / 聚集前列 %s / '
                '封板数据 %d 股 / 机构净买前 5 %d 只 / 资金历史 %d 股 / '
                '公告窗口 %d 日 %d 股',
                len(limit_up_codes), len(theme_map), top_concepts,
                len(seal_map), len(inst_map), len(mf_history_map),
                ann_window, len(announcement_map))
    return {
        'cand_seal_open_map': seal_map,
        'cand_theme_map': theme_map,
        'cand_top_concepts': top_concepts,
        'cand_cluster_counts': cluster_counts,
        'cand_institution_net_map': inst_map,
        'mf_history_map': mf_history_map,
        'cand_announcement_map': announcement_map,
    }


def _announcement_map(reader, as_of: str, window_days: int = 3) -> Dict[str, List[dict]]:
    """锚定日前 N 个自然日的公告窗口（P1-11）：{ts_code: [公告]}，ann_date 降序。

    公告按自然日发布（含周末），故窗口为自然日；报告层聚合复用同一函数
    （原则 P6）。窗口内无行的股票不出现在 map（模块按「无公告」处理）。
    """
    from datetime import datetime, timedelta

    end = datetime.strptime(str(as_of), '%Y%m%d')
    start = (end - timedelta(days=max(window_days, 1) - 1)).strftime('%Y%m%d')
    rows = reader._query(
        "SELECT ts_code, announcement_id, ann_date, ann_type, title "
        "FROM announcement_daily WHERE ann_date BETWEEN %s AND %s "
        "ORDER BY ann_date DESC", (start, str(as_of)))
    out: Dict[str, List[dict]] = {}
    for r in rows:
        out.setdefault(r[0], []).append({
            'ann_id': r[1], 'ann_date': str(r[2]), 'ann_type': r[3],
            'title': r[4] or '',
        })
    return out


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
    ranked = _concept_cluster_counts(theme_map, limit_up_codes, blacklist)
    return list(ranked.keys())[:_TOP_CONCEPT_N]


def _concept_cluster_counts(theme_map: Dict[str, List[str]], limit_up_codes: set,
                            blacklist: List[str]) -> Dict[str, int]:
    """聚集概念 → 当日涨停家数（P1-7 R2 聚集强度阈值的输入）。

    与 ``_top_concepts`` 同一计数口径（排序 + 前 N）；调用方取 count 作
    ``is_r2(cluster_count=...)``。全量计数不限前 N（阈值判定需要真实家数，
    前列截断只影响展示）。
    """
    def _blocked(name: str) -> bool:
        return any(b in name for b in blacklist)

    counts: Dict[str, int] = {}
    for ts_code in limit_up_codes:
        for name in theme_map.get(ts_code, []):
            if not _blocked(name):
                counts[name] = counts.get(name, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])))


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


def _mf_history_map(reader, as_of: str) -> Dict[str, List]:
    """ts_code → [(trade_date, big_net_amt)] 最近 10 行升序（money_trend 输入；
    行级口径受 calib_note 约束（P1-7）：20260917 起全市场分笔分桶，
    历史行为异动子集近似口径——跨期解读须以当日 calib_note 为准）"""
    try:
        rows = reader._query(
            """
            SELECT ts_code, trade_date, big_net_amt FROM (
                SELECT ts_code, trade_date, big_net_amt,
                       ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                FROM money_flow_daily
                WHERE trade_date <= %s AND big_net_amt IS NOT NULL
            ) t WHERE rn <= 10 ORDER BY ts_code, trade_date
            """, (as_of,))
    except Exception as e:  # pragma: no cover - 库异常兜底
        logger.warning('资金历史读取失败: %r', e)
        return {}
    out: Dict[str, List] = {}
    for ts_code, d, net in rows:
        out.setdefault(str(ts_code), []).append((str(d), float(net)))
    return out
