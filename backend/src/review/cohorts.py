# -*- coding: utf-8 -*-
"""样本身份冻结（每日复盘报告优化技术方案 §4.1，2026-11-24）。

问题（D1 背景）：候选池战绩此前只对照全市场中位——「筛选是否跑赢同类股票」
无从回答。11-23 样例：R2 两只中位 +8.86% 对全市场 +2.41% 为 +6.45pct
（看似很强），但对前一日 26 只可确认首板的中位 +9.96% 为 −1.10pct
（同类中并不占优）——两条基准回答两个不同的问题。

设计：**样本身份与次日指标分开**——

- ``member_hash``：冻结名单的稳定哈希（排序去重后 sha256 前 16 位）；
- ``freeze_cohort``：``{cohort_id, anchor_date, selection_rule, ts_codes,
  member_hash, n_total}``——T 日预测 payload 随行，N 日核验按代码对截面
  显式左连接（return_metrics.evaluate_cohort）；
- ``limit_up_cohort``：T 全部涨停名单（E2 验点 1 回填分母——涨停状态本身
  可判定，无需日历核验）；
- ``first_board_cohort`` / ``two_board_cohort``：按**交易日历连续性**核验
  首板/二板身份。``cross_section._add_prev_limit_up`` / ``_add_streak`` 用
  「上一条可得记录」——缺日时会把前几日涨停或停牌误作昨日状态；此处要求
  锚定日行的前一根**日历相邻**交易日行存在（``cs_prev`` 有该股行），
  否则该成员记「身份未知」，不入可确认名单（E2 二板分母同口径）。
"""

from __future__ import annotations

import hashlib
from typing import Any, Dict, Iterable, List, Optional


def member_hash(ts_codes: Iterable[str]) -> str:
    """冻结名单稳定哈希（排序去重 sha256 前 16 位；空名单 → ''）。"""
    codes = sorted({str(c) for c in ts_codes if c})
    if not codes:
        return ''
    return hashlib.sha256('\n'.join(codes).encode('utf-8')).hexdigest()[:16]


def freeze_cohort(cohort_id: str, anchor_date: str, selection_rule: str,
                  ts_codes: Iterable[str], **extra: Any) -> Dict[str, Any]:
    """冻结一个样本身份（预测 payload 随行；核验侧按 ts_codes 左连接）。

    extra 附加字段原样并入（如 first_board 的 unknown 计数）。
    """
    codes = sorted({str(c) for c in ts_codes if c})
    out: Dict[str, Any] = {
        'cohort_id': cohort_id,
        'anchor_date': anchor_date,
        'selection_rule': selection_rule,
        'ts_codes': codes,
        'member_hash': member_hash(codes),
        'n_total': len(codes),
    }
    out.update(extra)
    return out


def _limit_up_index(cs) -> Dict[str, int]:
    """截面涨停行索引 {ts_code: streak}；空截面返回空。"""
    if cs is None or cs.empty:
        return {}
    mask = cs['_limit_up'].fillna(False).astype(bool)
    out: Dict[str, int] = {}
    for _, r in cs[mask].iterrows():
        streak = r.get('_streak')
        out[str(r['ts_code'])] = int(streak) if streak == streak else 0  # NaN 防御
    return out


def limit_up_cohort(cs_today, anchor_date: str) -> Dict[str, Any]:
    """T 全部涨停名单（E2 验点 1：涨停延续性回填，冻结成员哈希）。"""
    ups = _limit_up_index(cs_today)
    return freeze_cohort('limit_up', anchor_date,
                        'T 全部涨停（收盘判定，无需日历核验）', ups.keys())


def first_board_cohort(cs_today, cs_prev,
                      anchor_date: str) -> Dict[str, Any]:
    """T 全部**可确认**首板（§4.1 同类基准分母；D1/E2 共用）。

    可确认 = 锚定日涨停 ∧ 日历相邻前一交易日有行（cs_prev 含该股）∧ 该行
    未涨停。锚定日涨停但前一交易日无行者记 unknown（停牌/缺采集期间身份
    不可确认——可能为复牌连板，不得冒充首板，也不入「可确认非首板」）。
    前一交易日有行且涨停者属 ≥2 板，天然排除。
    """
    today_up = _limit_up_index(cs_today)
    prev_up = _limit_up_index(cs_prev)
    prev_has: set = set()
    if cs_prev is not None and not cs_prev.empty:
        prev_has = {str(c) for c in cs_prev['ts_code']}
    confirmable: List[str] = []
    unknown: List[str] = []
    for ts in today_up:
        if ts in prev_has:
            if ts not in prev_up:
                confirmable.append(ts)
        else:
            unknown.append(ts)
    return freeze_cohort('first_board', anchor_date,
                         'T 全部可确认首板（日历相邻行核验；停牌缺行记未知）',
                         confirmable,
                         unknown=sorted(unknown), n_unknown=len(unknown))


def two_board_cohort(cs_today, cs_prev, anchor_date: str) -> Dict[str, Any]:
    """T 全部**可确认**二板（E2 验点 2：二板能否升三板的回填分母）。

    可确认 = 锚定日涨停 ∧ ``_streak == 2`` ∧ 日历相邻前一交易日有行且涨停
    （两根涨停行按日历相邻——中间隔停牌/缺行的「复牌连板」不算连续封板）。
    streak==2 但前一交易日无行者记 unknown（分母「可判定数」不含、单列）。
    """
    today_up = _limit_up_index(cs_today)
    prev_up = _limit_up_index(cs_prev)
    prev_has: set = set()
    if cs_prev is not None and not cs_prev.empty:
        prev_has = {str(c) for c in cs_prev['ts_code']}
    confirmable: List[str] = []
    unknown: List[str] = []
    for ts, streak in today_up.items():
        if streak != 2:
            continue
        if ts in prev_has and ts in prev_up:
            confirmable.append(ts)
        else:
            unknown.append(ts)
    return freeze_cohort('two_board', anchor_date,
                         'T 全部可确认二板（两根涨停行日历相邻核验）',
                         confirmable,
                         unknown=sorted(unknown), n_unknown=len(unknown))


def r2_published_cohort(pub: Optional[Dict[str, Any]],
                         anchor_date: str) -> Optional[Dict[str, Any]]:
    """R2 正式发布 cohort（§4.1：同类基准的候选组 = T 首发发布名单）。

    池未开放/发布名单为空时仍冻结（n_total=0，空池语义由核验侧按
    「收益/超额留空」处理）；发布批次缺失（旧缓存）返回 None（跳过该组，
    不硬造研究样本充当正式名单）。
    """
    if pub is None:
        return None
    published = ((pub.get('pools') or {}).get('R2') or {}).get('published') or []
    version = str(pub.get('version') or '')
    return freeze_cohort('R2_published', anchor_date,
                         f'R2 正式发布名单（candidate_publication {version}）',
                         (r.get('ts_code') for r in published))
