"""龙虎榜读端辅助（§5.2 批次 B 后续：逐股票当日机构口径 + 深市席位配对）。

两个纯函数模块（无 IO，离线可测）：
- ``pick_single_day_event``：同股同日多事件选一个"代表单日"事件——固定、
  版本化的原因优先次序（REASON_PRIORITY_V1），平手按稳定 event_id 字典序；
  跨日榜（period_start ≠ trade_date）不参与单日口径，单列显示披露区间。
- ``normalize_szse_reason`` / ``pair_szse_seats``：深市 XLSX 披露原因原文与
  巨潮规则名的归一化配对——剔除实际数值后按关键词对齐；同股当日巨潮仅
  1 个事件时直接配对，多事件按归一化关键词匹配，未配对留待核（不猜）。
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

# 版本化原因优先次序（§5.2：固定、版本化——改动生成新版本号；匹配词命中
# reason_code 或 reason_text 越靠前越优先代表"单日"）
REASON_PRIORITY_V1: List[str] = [
    '跌幅偏离', '涨幅偏离', '换手率', '振幅', '成交价同比例', '成交量同比例',
    '成交金额', '连续三个', '异常期间', 'ST', '异常波动',
]
_REASON_PRIORITY_VERSION = 'v1'

_NUM_TAIL = re.compile(r'(达到?|超?过?)?[\d.,]+%?$')
_PCT = re.compile(r'[\d.,]+%')


def normalize_szse_reason(text: str) -> str:
    """披露原因原文 -> 归一化规则关键词（剔除实际数值/百分比/连接词）

    '日价格涨幅偏离值达到13.64%' -> '价格涨幅偏离值'
    '涨幅偏离值达7%的证券'       -> '涨幅偏离值'（巨潮规则名口径）
    """
    s = str(text or '').strip()
    s = _PCT.sub('', s)
    s = _NUM_TAIL.sub('', s)
    for kw in ('的证券', '达到', '达', '连续', '异常期间'):
        s = s.replace(kw, '')
    if s.startswith('日'):
        s = s[1:]
    return s.strip()


def pick_single_day_event(events: List[Dict[str, Any]],
                          trade_date: str) -> Optional[Dict[str, Any]]:
    """同股同日多事件选代表（机构口径不重复相加；None=全部为跨日榜）。

    优先次序：REASON_PRIORITY_V1 命中序号小者优先；平手按 event_id 字典序
    （事件 ID 由归一身份哈希生成，稳定）。跨日榜（period_start≠trade_date）
    不参与，仅当**全部**候选都是跨日榜时返回 None（读端单列显示披露区间）。
    """
    same_day = [e for e in events
                if str(e.get('period_start') or trade_date) == trade_date]
    if not same_day:
        return None

    def _priority(e: Dict[str, Any]) -> int:
        text = f"{e.get('reason_code') or ''}|{e.get('reason_text') or ''}"
        for i, kw in enumerate(REASON_PRIORITY_V1):
            if kw in text:
                return i
        return len(REASON_PRIORITY_V1)

    return min(same_day,
               key=lambda e: (_priority(e), str(e.get('event_id') or '')))


def pair_szse_seats(xlsx_rows: List[Dict[str, Any]],
                    cn_rows: List[Dict[str, Any]],
                    ts_code: str) -> Dict[str, Any]:
    """深市单股当日事件与巨潮事件的席位配对（§5.1）。

    策略：巨潮单事件且 XLSX 单行 -> 直接配对；多事件按归一化关键词对齐；
    配不上的行返回 pending（留待核，不猜配对、不用披露日伪造区间）。

    Returns:
        {'matched': [{xlsx_row, cn_event}], 'pending': [xlsx_row]}
    """
    matched: List[Dict[str, Any]] = []
    pending: List[Dict[str, Any]] = []
    if len(cn_rows) == 1 and len(xlsx_rows) == 1:
        ka = normalize_szse_reason(str(xlsx_rows[0].get('plyy') or ''))
        kb = normalize_szse_reason(str(cn_rows[0].get('type') or ''))
        # 单事件捷径也须规则相容（双向包含），不相容留待核
        if ka and kb and (ka in kb or kb in ka):
            return {'matched': [{'xlsx_row': xlsx_rows[0], 'cn_event': cn_rows[0]}],
                    'pending': []}
        return {'matched': [], 'pending': list(xlsx_rows)}
    used: set = set()
    for row in xlsx_rows:
        key = normalize_szse_reason(str(row.get('plyy') or ''))
        hit = None
        for ev in cn_rows:
            if id(ev) in used:
                continue
            kb = normalize_szse_reason(str(ev.get('type') or ''))
            # 双向包含：官网带"价格"前缀而巨潮省略（价格涨幅偏离值 ⊃ 涨幅偏离值）
            if key and kb and (key in kb or kb in key):
                hit = ev
                break
        if hit is not None:
            used.add(id(hit))
            matched.append({'xlsx_row': row, 'cn_event': hit})
        else:
            pending.append(row)
    return {'matched': matched, 'pending': pending}
