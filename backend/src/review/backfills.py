# -*- coding: utf-8 -*-
"""三项次日回填验点（每日复盘报告优化技术方案 §4.3/§三，2026-11-24）。

T 日报告首页冻结三个「跟踪的问题」，N 日收盘后在「昨日核验」回填：

1. **涨停延续**（``limit_up``）：T 涨停名单（成员哈希冻结）→ N 可比收盘
   涨跌幅中位数、翻红数（严格大于零）/ 有效数、缺行及其他无效数；
2. **二板晋级**（``two_board``）：T 可确认二板名单（两根涨停行日历相邻）→
   按日历相邻的 T、N 与同一代码确认连续封板，晋级数 / 可判定数，
   另列不可判定数（缺行）；分母为零时比率为空；
3. **量价延续**（``volume_price``）：中证全指 ``000985`` 的 T 日涨跌与
   「成交额 / T 之前五个连续市场交易日均额」→ N 日涨跌及同公式倍数。

量价公式复用 ``_volume_price`` 的指数与前五日口径，但增加**应到日期检查**：
窗口六个端点（N 与其前五个交易日）须与 ``base_trade_calendar`` 完全一致且
成交额齐全——「最近六条非空记录」会在缺日时静默跳过（旧实现缺陷）；
窗口缺日或均额为零则倍数待核，缺涨跌幅单独标注。盘中占位行不能充当
N 日收盘：N 端点行缺失或成交额为空 → 待核。

冻结侧产物（含窗口端点与金额）进 T 日预测 payload；核验侧按 N 日截面/
行情表重算——首发文件保留原貌，回填结果走 N 日「昨日核验」。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .cohorts import member_hash
from .return_metrics import evaluate_cohort, median_precise, red_count

logger = logging.getLogger(__name__)

#: 量价验点指数（技术方案 §4.3：中证全指）
VP_INDEX_CODE = '000985'
VP_WINDOW = 5  # 「N 之前五个连续市场交易日」


def _dot(ymd: str) -> str:
    return f'{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}'


def _plain(v: Any) -> str:
    return str(v).replace('-', '')


def _last_n_trade_dates(reader, as_of: str, n: int) -> List[str]:
    """截至 as_of（含）最近 n 个市场交易日（升序，日历口径）。"""
    try:
        rows = reader._query(
            "SELECT calendar_date FROM base_trade_calendar "
            "WHERE is_trading_day = 1 AND calendar_date <= %s "
            "ORDER BY calendar_date DESC LIMIT %s", (_dot(as_of), n))
    except Exception as e:
        logger.warning(f'回填窗口日历读取失败: {e}')
        return []
    return [_plain(r[0]) for r in reversed(rows)]


def _index_rows(reader, code: str, dates: List[str]) -> Dict[str, Dict[str, Any]]:
    """指数指定日期行 {date: {'amount', 'chg', 'name'}}（缺日不在 dict）。

    ``trade_index_info.trade_date`` 为 TEXT 'YYYYMMDD'（与 _volume_price 等
    既有查询同格式——不用带点的 DATE 格式）。
    """
    if not dates:
        return {}
    try:
        rows = reader._query(
            "SELECT trade_date, amount, change_rate, index_name "
            "FROM trade_index_info WHERE index_code = %s AND trade_date = ANY(%s)",
            (code, list(dates)))
    except Exception as e:
        logger.warning(f'回填指数行情读取失败（{code}）: {e}')
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for d, amt, chg, name in rows:
        out[_plain(d)] = {'amount': (float(amt) if amt is not None else None),
                          'chg': (float(chg) if chg is not None else None),
                          'name': name}
    return out


def _ratio(amounts: List[Optional[float]]) -> Optional[float]:
    """末位成交额 / 前五个交易日均额（任一缺或均额为零 → None）。"""
    if len(amounts) != VP_WINDOW + 1 or any(a is None for a in amounts):
        return None
    prev5 = [float(a) for a in amounts[:-1]]
    mean5 = sum(prev5) / len(prev5)
    if mean5 <= 0:
        return None
    return float(amounts[-1]) / mean5


def _volume_price_at(reader, code: str, anchor: str) -> Optional[Dict[str, Any]]:
    """量价锚快照（anchor 日涨跌 + 成交额 / 前五个连续交易日均额；应到日期检查）。

    窗口 = [anchor−5, …, anchor]（日历口径）；任一端点缺行或成交额为空 →
    status='待核' 并列缺失日期；均额为零同待核。涨跌幅缺失单独标注
    （不阻断倍数）。
    """
    dates = _last_n_trade_dates(reader, anchor, VP_WINDOW + 1)
    if len(dates) != VP_WINDOW + 1:
        return {'code': code, 'anchor_date': anchor, 'status': '待核',
                'reason': f'交易日历不足 {VP_WINDOW + 1} 个端点',
                'window_dates': dates, 'window_amounts': [],
                'chg_pct': None, 'ratio_5d': None}
    rows = _index_rows(reader, code, dates)
    amounts: List[Optional[float]] = [
        (rows.get(d) or {}).get('amount') for d in dates]
    missing = [d for d, a in zip(dates, amounts) if a is None]
    name = next((r.get('name') for r in rows.values() if r.get('name')), None)
    ratio = None if missing else _ratio(amounts)
    chg = (rows.get(anchor) or {}).get('chg')
    out: Dict[str, Any] = {
        'code': code, 'name': name, 'anchor_date': anchor,
        'window_dates': dates,
        'window_amounts': [round(a / 1e8, 2) if a is not None else None
                           for a in amounts],
        'chg_pct': round(chg, 2) if chg is not None else None,
        'chg_missing': chg is None,
        'ratio_5d': round(ratio, 3) if ratio is not None else None,
        'status': 'ok', 'reason': None,
    }
    if missing:
        out['status'] = '待核'
        out['reason'] = f"窗口缺日（{ '、'.join(missing) }）——「最近六条非空记录」会静默跳过缺日，本验点按应到日期核对"
        out['ratio_5d'] = None
    elif ratio is None:
        out['status'] = '待核'
        out['reason'] = '前五日均额为零或不可算'
        out['ratio_5d'] = None
    return out


def freeze_backfill_anchors(reader, as_of: str) -> Dict[str, Any]:
    """T 日冻结三项回填锚（预测 payload 的 backfills 段；§4.3 表 3 列 2）。

    涨停/二板名单在 cohorts 冻结（aggregator 装配），此处只含量价锚。
    """
    return {'volume_price': _volume_price_at(reader, VP_INDEX_CODE, as_of)}


# ------------------------------------------------------------------ N 日核验

def _cohort_check(cohort: Dict[str, Any]) -> bool:
    """冻结名单一致性（成员哈希复算——回填按代码对账的前提）。"""
    return member_hash(cohort.get('ts_codes') or []) == cohort.get('member_hash')


def verify_limit_up_continuation(cohort: Dict[str, Any], cs_n) -> Dict[str, Any]:
    """验点 1：T 涨停名单 → N 涨跌幅中位数 / 翻红数 / 有效数 / 缺行数。

    翻红采用严格大于零；固定分组不缩小（缺行成员保留在分母说明中，
    E2：「对 153 只涨停注入次日缺行——固定分组不缩小」）。
    """
    ev = evaluate_cohort(cohort.get('ts_codes') or [], cs_n)
    median = median_precise(ev['changes'])
    return {
        'anchor_date': cohort.get('anchor_date'),
        'n_total': ev['n_total'], 'n_valid': ev['n_valid'],
        'median': round(median, 2) if median is not None else None,
        'red_n': red_count(ev['changes']),
        'invalid': ev['invalid'],
        'member_hash': cohort.get('member_hash'),
        'member_hash_match': _cohort_check(cohort),
    }


def verify_two_board_promotion(cohort: Dict[str, Any], cs_n) -> Dict[str, Any]:
    """验点 2：T 可确认二板 → 按日历相邻 T、N 同一代码确认连续封板。

    晋级 = N 日该股涨停（T、N 为日历相邻交易日，连续封板即三板）；
    可判定 = N 日有行且涨停状态可判定；不可判定 = 缺行单列。
    分母（可判定数）为零时晋级率为空（不转零）。
    """
    codes = cohort.get('ts_codes') or []
    today: Dict[str, Dict[str, Any]] = {}
    if cs_n is not None and not cs_n.empty:
        lu = cs_n['_limit_up'].fillna(False).astype(bool)
        for ts, chg, up in zip(cs_n['ts_code'], cs_n['change_rate'], lu):
            today[str(ts)] = {'up': bool(up),
                              'chg': (None if chg != chg or chg is None
                                      else float(chg))}
    promoted = judgeable = unjudgeable = 0
    chgs: List[float] = []
    for ts in codes:
        t = today.get(ts)
        if t is None:
            unjudgeable += 1
            continue
        judgeable += 1
        if t['up']:
            promoted += 1
        if t['chg'] is not None:
            chgs.append(t['chg'])
    median = median_precise(chgs)
    return {
        'anchor_date': cohort.get('anchor_date'),
        'n_total': len(codes), 'promoted': promoted, 'judgeable': judgeable,
        'unjudgeable': unjudgeable,
        'promotion_rate': round(promoted / judgeable, 4) if judgeable else None,
        'n_valid': len(chgs),
        'median': round(median, 2) if median is not None else None,
        'red_n': red_count(chgs),
        'member_hash': cohort.get('member_hash'),
        'member_hash_match': _cohort_check(cohort),
    }


def verify_volume_price(anchor: Dict[str, Any], reader,
                        as_of_n: str) -> Dict[str, Any]:
    """验点 3：中证全指 N 日涨跌 + N 成交额 / N 之前五个连续交易日均额。

    锚（T 日冻结）自身待核时结果一并待核（基准不可比则倍数无意义）；
    应到日期检查同冻结侧——六个端点与完整成交额逐日核对。
    """
    result = _volume_price_at(reader, anchor.get('code') or VP_INDEX_CODE,
                              as_of_n)
    result['anchor'] = {
        'anchor_date': anchor.get('anchor_date'),
        'chg_pct': anchor.get('chg_pct'), 'ratio_5d': anchor.get('ratio_5d'),
        'status': anchor.get('status'), 'reason': anchor.get('reason'),
    }
    if anchor.get('status') != 'ok':
        result['status'] = '待核'
        result['reason'] = (f"T 日锚待核（{anchor.get('reason') or '原因未记录'}）"
                            + (f"；N 日：{result['reason']}"
                               if result.get('reason') else ''))
        result['ratio_5d'] = None
    return result


def verify_backfills(backfills: Dict[str, Any], cs_n, reader,
                     as_of_n: str) -> Dict[str, Any]:
    """三项回填核验入口（verifier 调用；缺项跳过不硬造）。"""
    out: Dict[str, Any] = {}
    if not backfills:
        return out
    if backfills.get('limit_up'):
        out['limit_up'] = verify_limit_up_continuation(
            backfills['limit_up'], cs_n)
    if backfills.get('two_board'):
        out['two_board'] = verify_two_board_promotion(
            backfills['two_board'], cs_n)
    if backfills.get('volume_price'):
        out['volume_price'] = verify_volume_price(
            backfills['volume_price'], reader, as_of_n)
    return out
