# -*- coding: utf-8 -*-
"""采集覆盖事实（每日复盘报告优化技术方案 §4.3 E1，2026-11-24）。

覆盖分母来自**生成该批行情时的采集目标代码清单**
（``tmp/kline_day_scope_<日>.json``，sync 侧 ``_save_kline_day_scope`` 产物）。

五层漏斗（按交易日与完整代码去重）：

- ``E`` 采集目标（scope 清单）；``O`` = E ∩ 锚定日有日K行；``V`` = O ∩ 有成交；
  ``A`` = V ∩ 涨跌幅有效；``U`` = A ∩ 上涨。
- 另记：缺行 ``E−O``（按可识别原因拆分——采集失败/熔断取消/未提交/待核，
  证据来自 ``tmp/kline_day_gap_<日>.json`` 缺口清单，每个缺行代码只归一个
  有证据的主因）；有行无成交 ``O−V``；成交但涨跌无效 ``V−A``；
  意外入库（O 以外的锚定日行）。

展示口径：``上涨数 / 有效涨跌样本数``（U/A）为主比例，下界
``上涨数 / 采集范围``（U/E）并列；目标清单缺失时仅显示自身可证的计数，
两个比例按各自分母决定可算性（不硬造）。首页、正文与 meta 读取同一个
coverage 对象（§4.3：单一事实源）。
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

#: 缺行主因优先级（一个代码只归一个有证据的主因）
_REASON_PRIORITY = (
    ('failed', '采集失败'),
    ('cancelled', '熔断取消'),
    ('not_submitted', '未提交'),
)

#: 待核兜底文案（停牌等有证据豁免暂无证据源——sync 缺口清单同口径）
_PENDING_REASON = '原因待核（停牌等有证据豁免暂无证据源）'


def _repo_tmp_dir() -> Path:
    from ..sync.report_writer import resolve_repo_root
    return resolve_repo_root() / 'tmp'


def _load_manifest(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception as e:  # noqa: BLE001 —— 清单缺失/损坏 → 覆盖降级
        logger.warning(f'覆盖清单读取失败（{path.name}）: {e}')
        return None


def _scope_codes(as_of: str) -> Optional[Dict[str, Any]]:
    """采集目标代码清单（scope manifest；缺失返回 None）。"""
    manifest = _load_manifest(_repo_tmp_dir() / f'kline_day_scope_{as_of}.json')
    if not manifest:
        return None
    codes: set = set()
    for lst in (manifest.get('codes_by_market') or {}).values():
        codes.update(str(c) for c in lst or [])
    if not codes:
        return None
    return {'codes': codes, 'n_expected': int(manifest.get('codes_total') or len(codes)),
            'source': f'tmp/kline_day_scope_{as_of}.json'}


def _missing_reasons(as_of: str, missing: set) -> Dict[str, Any]:
    """缺行代码 → 单主因归类（证据来自 gap 清单；无清单全部待核）。"""
    out: Dict[str, Any] = {'reasons': {}, 'source': None}
    if not missing:
        out['reasons'] = {}
        return out
    gap = _load_manifest(_repo_tmp_dir() / f'kline_day_gap_{as_of}.json')
    if not gap:
        out['reasons'] = {_PENDING_REASON: len(missing)}
        out['source'] = f'tmp/kline_day_gap_{as_of}.json（缺失——成员级缺因待核）'
        return out
    out['source'] = f'tmp/kline_day_gap_{as_of}.json'
    evidence: Dict[str, set] = {key: {str(c) for c in (gap.get(key) or [])}
                                for key, _ in _REASON_PRIORITY}
    reasons: Dict[str, int] = {}
    pending = 0
    for code in missing:
        for key, label in _REASON_PRIORITY:
            if code in evidence[key]:
                reasons[label] = reasons.get(label, 0) + 1
                break
        else:
            pending += 1
    if pending:
        reasons[_PENDING_REASON] = pending
    out['reasons'] = reasons
    return out


def coverage_facts(as_of: str, cs_today) -> Dict[str, Any]:
    """锚定日采集覆盖事实（聚合层单次计算，首页/正文/meta 共用）。

    Args:
        as_of: 锚定日（YYYYMMDD）。
        cs_today: 锚定日截面（cross_section_at 产物；含 ``_traded`` 列）。

    Returns:
        覆盖对象（见模块 docstring）；scope 清单缺失时 ``available=False``
        并给出面板自身可证计数（O/V/A/U 按面板全集计算，不含 E 的比例置
        None）。
    """
    rows: set = set()
    traded: set = set()
    valid: set = set()
    up: set = set()
    if cs_today is not None and not cs_today.empty:
        tr = cs_today['_traded'].fillna(False).astype(bool) \
            if '_traded' in cs_today.columns \
            else cs_today['change_rate'].notna()
        for ts, chg, t in zip(cs_today['ts_code'], cs_today['change_rate'], tr):
            ts = str(ts)
            rows.add(ts)
            if not t:
                continue
            traded.add(ts)
            if chg is None or chg != chg:
                continue
            valid.add(ts)
            if float(chg) > 0:
                up.add(ts)
    scope = _scope_codes(as_of)
    out: Dict[str, Any] = {
        'as_of': as_of,
        'n_rows_total': len(rows),
        'n_traded_total': len(traded),
        'n_valid_change_total': len(valid),
        'n_up_total': len(up),
        'available': False,
        'scope_source': None,
        'n_expected': None, 'n_rows': None, 'n_traded': None,
        'n_valid_change': None, 'n_up': None,
        'n_missing': None, 'n_no_volume': None, 'n_invalid_change': None,
        'n_unexpected_rows': None,
        'up_ratio_valid': None, 'up_ratio_scope': None,
        'missing_reasons': None, 'missing_reason_source': None,
    }
    if scope is None:
        out['reason'] = ('无采集目标清单（kline_day_scope 缺失）——覆盖仅按面板'
                         '自身可证计数，E 口径比例不可算')
        # 面板全集口径的自身可证比例（分母=有效涨跌样本）
        out['n_rows'] = len(rows)
        out['n_traded'] = len(traded)
        out['n_valid_change'] = len(valid)
        out['n_up'] = len(up)
        out['up_ratio_valid'] = round(len(up) / len(valid), 4) if valid else None
        return out
    expected = scope['codes']
    o = rows & expected
    v = traded & expected
    a = valid & expected
    u = up & expected
    missing = expected - o
    reasons = _missing_reasons(as_of, missing)
    out.update({
        'available': True,
        'scope_source': scope['source'],
        'n_expected': len(expected),
        'n_rows': len(o),
        'n_traded': len(v),
        'n_valid_change': len(a),
        'n_up': len(u),
        'n_missing': len(missing),
        'n_no_volume': len(o) - len(v),
        'n_invalid_change': len(v) - len(a),
        'n_unexpected_rows': len(rows) - len(o),
        'up_ratio_valid': round(len(u) / len(a), 4) if a else None,
        'up_ratio_scope': round(len(u) / len(expected), 4) if expected else None,
        'missing_reasons': reasons['reasons'],
        'missing_reason_source': reasons.get('source'),
    })
    return out


def coverage_note(cov: Dict[str, Any]) -> str:
    """覆盖一行注记（首页/正文共用渲染；§三 样例口径）。

    非 coverage 对象（空 dict/旧缓存缺键）返回 '' 不渲染——避免 None 脏读数。
    """
    if not cov or 'as_of' not in cov:
        return ''
    if not cov.get('available'):
        return (f"采集覆盖：目标清单缺失——面板自身可证 计 {cov.get('n_rows')} 行、"
                f"有成交 {cov.get('n_traded')}、涨跌幅有效 {cov.get('n_valid_change')}、"
                f"上涨 {cov.get('n_up')}（E 口径缺行与占比不可算，如实标注）")
    bits = [f"采集范围 {cov['n_expected']} 只，入库 {cov['n_rows']} 只，"
            f"有成交 {cov['n_traded']} 只，缺行 {cov['n_missing']} 只"]
    reasons = cov.get('missing_reasons') or {}
    if reasons:
        detail = '、'.join(f'{k} {v}' for k, v in reasons.items())
        bits.append(f'（缺行主因：{detail}）')
    if cov.get('n_no_volume'):
        bits.append(f"；有行无成交 {cov['n_no_volume']} 只")
    if cov.get('n_invalid_change'):
        bits.append(f"；成交但涨跌幅无效 {cov['n_invalid_change']} 只")
    if cov.get('n_unexpected_rows'):
        bits.append(f"；清单外意外入库 {cov['n_unexpected_rows']} 只")
    return ''.join(bits)


def coverage_sensitive_note(cov: Dict[str, Any]) -> str:
    """结论对缺口的敏感性说明（§4.3：说明哪些结论对缺口敏感）。"""
    if not cov.get('available') or not cov.get('n_missing'):
        return ''
    ratios = []
    if cov.get('up_ratio_valid') is not None:
        ratios.append(f"已交易样本中 {cov['up_ratio_valid']:.1%} 上涨")
    if cov.get('up_ratio_scope') is not None:
        ratios.append(f"即便以整个采集范围作分母也有 {cov['up_ratio_scope']:.1%}")
    text = '缺口敏感性：' + '，'.join(ratios) + '——普涨判断较稳' \
        if ratios else '缺口敏感性：'
    text += (f"；涨停与题材排名按已采集 {cov['n_rows']} 只样本统计"
             f"（缺 {cov['n_missing']} 只，榜单读数受采样影响）")
    return text
