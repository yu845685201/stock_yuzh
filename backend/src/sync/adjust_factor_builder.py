"""兼容因子候选构建（§8 批次 C 后续：adjust_factor_candidate）。

权益重建路径（方案 §8"走权益重建时先用官方参考参数生成可回放的 Q/H 序列，
再按同一 R 转成现有定义"）：

- 事件输入：``stock_action_event_version`` category=1（除权除息，f1=分红/f2=
  配股价/f3=送转/f4=配股，per10 每 10 股单位）；
- 参考价公式（交易所标准）：``ex = (preclose - 每股现金 + 配股价×配股率)
  / (1 + 送股率 + 配股率)``——送转股不计价；per10 统一 /10 转每股；
- R（参考未复权收盘）= 事件日前最后交易日 raw_close（his_kline_day，干净列）；
  **R 缺失或为零 → 停止该证券的提升**（§8 红线）；
- 后复权链：``hfq_e = hfq_prev × preclose / ex``（从锚定日 hfq=1 起步）；
  前复权兼容值 ``q = hfq / hfq_last``（现有定义 q=同花顺前复权/通达信未复权
  的重建投影，因子阶梯语义与 base_adjust_factor 一致）；
- 单一乘数无法表示的现金平移不得忽略——除权参考价公式天然含现金项 ✓；
- 写入 ``adjust_factor_candidate``（series_version=锚定日+算法版本），
  **不触碰正式键 base_adjust_factor**（§8：只从已验收的完整股票序列投影）。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

ALGO_VERSION = 'equity_rebuild_v1'


def build_candidate_series(db: Any, ts_code: str,
                           events: List[Dict[str, Any]],
                           series_version: str,
                           anchor_date: Optional[str] = None,
                           ) -> Dict[str, Any]:
    """单只股票的因子候选序列。

    events 为该股 category=1 事件（按 datetime 升序，含 f1..f4）；
    返回 {'rows': [...], 'skipped': 原因 or None}——skipped 非空时该证券
    不更新候选（§8：R 缺失或为零、参考参数不明时停止该证券，其他证券继续）。
    """
    import bisect as _bisect
    if not events:
        return {'rows': [], 'skipped': 'no_events'}
    klines = db.execute_query(
        "SELECT trade_date, raw_close FROM his_kline_day WHERE ts_code = %s "
        "AND raw_close IS NOT NULL AND raw_close > 0 ORDER BY trade_date",
        (ts_code,))
    if not klines:
        return {'rows': [], 'skipped': 'no_kline'}
    kd = [str(k['trade_date']) for k in klines]
    kc = [float(k['raw_close']) for k in klines]
    events = sorted(events, key=lambda e: str(e['effective_date']))
    # C 修正：cat=2 送配股上市的股本比 = 实际送转执行结果（整数股零头舍入）
    cat2_ratio = {}
    for e in events:
        if int(e.get('category') or 0) == 2:
            prev_total = float(e.get('cash_reference') or 0.0)     # f2=前总
            post_total = float(e.get('rights_issue_ratio') or 0.0)  # f4=后总
            if prev_total > 0 and post_total > 0:
                cat2_ratio[str(e['effective_date'])] = post_total / prev_total
    # §8 批次 C 续：2026-11-26 实测 600000 事件结构——cat=2 送配股上市与 cat=1
    # 同日成对（股本比=1+送转率，上市日不二次除权）；cat=5 前总=后总（限售解禁
    # 不除权）；cat=6/8/9 增发不除权。故价格调整类仅 cat=1（除权除息）与
    # cat=11/12（扩缩股，比例字段 f3）；其余类别转审计不产因子行。
    first_event = str(events[0]['effective_date'])
    # 锚定日 = 首个事件日前的最后交易日（R 必须存在，§8）
    idx = _bisect.bisect_left(kd, first_event)
    if idx == 0:
        return {'rows': [], 'skipped': 'no_pre_anchor_close'}
    rows: List[Dict[str, Any]] = []
    hfq = 1.0
    anchor = anchor_date or kd[idx - 1]
    qfq_last = None
    for ev in events:
        eff = str(ev['effective_date'])
        eidx = _bisect.bisect_left(kd, eff)
        if eidx == 0 or kd[eidx - 1] > eff:
            # 事件日之前没有任何交易日（新股未上市即除权？异常）→ 停
            return {'rows': rows, 'skipped': f'{eff}:no_prior_close'}
        preclose = kc[eidx - 1]                       # R = 事件日前收盘
        if not preclose or preclose <= 0:
            return {'rows': rows, 'skipped': f'{eff}:R_missing_or_zero'}
        if int(ev.get('category') or 0) == 2:
            continue                  # cat=2 仅作股本比修正源，不产因子行
        if int(ev.get('category') or 1) in (11, 12):
            # 扩缩股：f3=比例（价格 ×比例方向按股本），无现金/配股
            ratio = float(ev.get('bonus_ratio') or 0.0)
            if ratio <= 0:
                return {'rows': rows, 'skipped': f'{eff}:bad_scale_ratio'}
            hfq = hfq * ratio
            if qfq_last is None:
                qfq_last = hfq
            q = hfq / qfq_last if qfq_last else None
            rows.append({'series_version': series_version, 'ts_code': ts_code,
                         'trade_date': eff,
                         'qfq_factor': round(q, 10) if q is not None else None,
                         'hfq_factor': round(hfq, 10),
                         'reference_raw_close': preclose, 'anchor_date': anchor,
                         'algo_version': ALGO_VERSION,
                         'based_event_version': str(ev.get('source_version') or ''),
                         'check_result': None, 'check_detail': None})
            continue
        d = float(ev.get('cash_dividend') or 0.0) / 10.0     # 每股现金
        s = float(ev.get('bonus_ratio') or 0.0) / 10.0       # 送转率（公告）
        p = float(ev.get('rights_issue_ratio') or 0.0) / 10.0  # 配股率
        actual = cat2_ratio.get(eff)
        if actual is not None:
            s_actual = actual - 1.0 - p          # 股本比 = 1 + 送转 + 配股
            if abs(s_actual - s) > 1e-9:
                s = max(s_actual, 0.0)           # 整数股零头舍入后的实际执行
        pr = float(ev.get('cash_reference') or 0.0)          # 配股价（每股）
        denom = 1.0 + s + p
        if denom <= 0:
            return {'rows': rows, 'skipped': f'{eff}:bad_denominator'}
        ex = (preclose - d + p * pr) / denom
        if not ex or ex <= 0:
            # 参考参数不明/组合非法 → 停该证券（§8：单一乘数无法表示不忽略）
            return {'rows': rows, 'skipped': f'{eff}:ex_nonpositive'}
        hfq = hfq * preclose / ex
        if qfq_last is None:
            qfq_last = hfq
        q = hfq / qfq_last if qfq_last else None
        rows.append({
            'series_version': series_version,
            'ts_code': ts_code,
            'trade_date': eff,
            'qfq_factor': round(q, 10) if q is not None else None,
            'hfq_factor': round(hfq, 10),
            'reference_raw_close': preclose,
            'anchor_date': anchor,
            'algo_version': ALGO_VERSION,
            'based_event_version': str(ev.get('source_version') or ''),
            'check_result': None,
            'check_detail': None,
        })
    return {'rows': rows, 'skipped': None}


def build_candidates_bulk(db: Any, source_version_prefix: str = 'gbbq:',
                          codes: Optional[List[str]] = None,
                          dry_run: bool = False) -> Dict[str, Any]:
    """批量构建候选（每股独立 series_version=锚定日+算法版本；跳过原因单列）"""
    import hashlib
    cond = "AND ts_code = ANY(%s)" if codes else ''
    ev_rows = db.execute_query(
        "SELECT ts_code, effective_date, category, cash_dividend, cash_reference,"
        " bonus_ratio, rights_issue_ratio, source_version FROM stock_action_event_version "
        f"WHERE source = 'tdx_gbbq' AND category IN (1, 2, 11, 12) {cond} ORDER BY ts_code, effective_date",
        (codes,) if codes else ())
    by_code: Dict[str, List[Dict[str, Any]]] = {}
    for r in ev_rows:
        if not str(r.get('source_version') or '').startswith(source_version_prefix):
            continue
        by_code.setdefault(r['ts_code'], []).append(r)
    stats: Dict[str, Any] = {'codes': len(by_code), 'rows': 0, 'ok': 0,
                             'skipped': {}}
    all_rows: List[Dict[str, Any]] = []
    for ts_code, evs in sorted(by_code.items()):
        r = build_candidate_series(db, ts_code, evs, series_version='pending')
        if r['skipped'] and not r['rows']:
            reason = r['skipped'].split(':')[0] if ':' in r['skipped'] else r['skipped']
            stats['skipped'][reason] = stats['skipped'].get(reason, 0) + 1
            continue
        anchor = r['rows'][0]['anchor_date'] if r['rows'] else None
        sv = (f'{ALGO_VERSION}@{anchor}@'
              + hashlib.sha1(ts_code.encode()).hexdigest()[:8])
        for row in r['rows']:
            row['series_version'] = sv
        all_rows.extend(r['rows'])
        if not r['skipped']:
            stats['ok'] += 1
        stats['rows'] = len(all_rows)
    if all_rows and not dry_run:
        db.upsert_adjust_factor_candidates(all_rows)
    return {'success': bool(all_rows), 'stats': stats}


def verify_candidates(db: Any, tolerance_match: float = 0.005,
                      tolerance_near: float = 0.02) -> Dict[str, Any]:
    """候选因子校验（§8 验收标准：逐事件日核候选与官方因子差值并归档）。

    对每条候选（有同日官方行的）：
    - h 差 = |hfq_cand − hfq_official| / hfq_official；q 差同理（双方非空时）；
    - check_result：match(≤0.5%) / near(≤2%) / divergent(>2%)；无同日官方行
      不评（check_result 保持 NULL——候选可独立存在的重建依据）；
    - 差异只登记不否定：候选方法论=纯权益重建，官方=同花顺价格口径（含舍入
      规则），系统性偏移属口径差异（§8"来源已有额外四舍五入时记录其精度"）。
    """
    rows = db.execute_query("""
        SELECT c.series_version, c.ts_code, c.trade_date,
               c.qfq_factor, c.hfq_factor, c.reference_raw_close,
               a.qfq_factor AS q_off, a.hfq_factor AS h_off
        FROM adjust_factor_candidate c
        JOIN base_adjust_factor a ON a.ts_code = c.ts_code AND a.trade_date = c.trade_date
        WHERE c.check_result IS NULL
    """)
    stats: Dict[str, Any] = {'checked': 0, 'match': 0, 'near': 0, 'divergent': 0}
    updates: List[Dict[str, Any]] = []
    import json as _json
    for r in rows:
        h_c = round(float(r['hfq_factor'] or 0), 6)   # B: 官方 6 位截断，对齐后比
        h_o = round(float(r['h_off'] or 0), 6)
        if not h_o:
            continue
        raw_close = float(r.get('reference_raw_close') or 0.0)
        # A: 价位自适应阈值——2 位小数网格半格 0.005/R + 基础 0.2%
        # 只放宽不收紧：网格半格仅在超低价股（R<1 元）超过基础容差
        tol_match = max(tolerance_match, 0.005 / raw_close) if raw_close else tolerance_match
        tol_near = max(tolerance_near, 0.02) if raw_close else tolerance_near
        h_diff = abs(h_c - h_o) / h_o
        detail: Dict[str, Any] = {'h_diff_pct': round(h_diff * 100, 4)}
        q_c = r.get('qfq_factor')
        q_o = r.get('q_off')
        if q_c is not None and q_o is not None and float(q_o):
            q_diff = abs(float(q_c) - float(q_o)) / float(q_o)
            detail['q_diff_pct'] = round(q_diff * 100, 4)
        if h_diff <= tol_match:
            result = 'match'
        elif h_diff <= tol_near:
            result = 'near'
        else:
            result = 'divergent'
        stats[result] += 1
        stats['checked'] += 1
        updates.append({'series_version': r['series_version'],
                        'ts_code': r['ts_code'], 'trade_date': r['trade_date'],
                        'check_result': result,
                        'check_detail': detail,
                        '_json_detail': _json.dumps(detail, ensure_ascii=False)})
    if updates:
        sql = ("UPDATE adjust_factor_candidate SET check_result = %s, "
               "check_detail = %s, update_time = NOW() "
               "WHERE series_version = %s AND ts_code = %s AND trade_date = %s")
        db.execute_batch(sql, [{
            'p1': u['check_result'], 'p2': u['_json_detail'],
            'p3': u['series_version'], 'p4': u['ts_code'], 'p5': u['trade_date'],
        } for u in updates] and [(u['check_result'], u['_json_detail'],
                                  u['series_version'], u['ts_code'], u['trade_date'])
                                 for u in updates] or None)
    return {'success': True, 'stats': stats}


def verify_688150_daily(db: Any) -> List[Dict[str, Any]]:
    """688150 差异化分红逐日核对（§8 验收样本：现金参考 0.199 vs 实际 0.20）"""
    return db.execute_query("""
        SELECT c.trade_date, c.reference_raw_close AS R, c.qfq_factor, c.hfq_factor,
               round(c.hfq_factor * c.reference_raw_close, 4) AS R_x_h,
               a.hfq_factor AS h_official,
               round(c.hfq_factor * c.reference_raw_close
                     - a.hfq_factor * c.reference_raw_close, 4) AS diff
        FROM adjust_factor_candidate c
        LEFT JOIN base_adjust_factor a ON a.ts_code = c.ts_code AND a.trade_date = c.trade_date
        WHERE c.ts_code = 'sh.688150' ORDER BY c.trade_date
    """)


def sync_gbbq_incremental(db: Any, dry_run: bool = False) -> Dict[str, Any]:
    """gbbq 版本检查与增量提取（§8 挂接：日常作业——按文件 SHA-1 判断版本变化，
    变了才重新解析；category IN (1,2) upsert 幂等，未变化零成本返回）。

    事件表 source_version=文件内容哈希——版本不变时新包内容必然相同（通达信
    全量重打包），历史事件天然去重；只补新包新增/修订的事件。
    """
    import hashlib
    import datetime as _dt
    import requests as _rq
    resp = _rq.get('http://www.tdx.com.cn/products/data/data/dbf/gbbq.zip',
                   timeout=180, headers={'User-Agent': 'Mozilla/5.0'})
    resp.raise_for_status()
    raw_zip = resp.content
    # 版本指纹 = 解压后 gbbq 主文件内容 SHA-1（而非 zip 整体）：服务端盘中重打包
    # 只改 zip entry 时间戳/压缩参数，gbbq 数据不变——内容哈希天然滤掉打包噪声；
    # gbbq 加密为确定性 Feistel（无 IV），同数据=同密文=同哈希，指纹稳定。
    # （2026-11-26 实测：同日 14:18 与 17:09 两次下载 zip 整体 SHA 不同而内容未变）
    from ..data_sources.tdx_reference_files import load_gbbq_pack as _load_gbbq
    pack = _load_gbbq(raw_zip)
    sha = hashlib.sha1(pack['raw']).hexdigest()
    src_version = f'gbbq:{sha[:16]}'
    latest = db.execute_query(
        "SELECT DISTINCT source_version FROM stock_action_event_version "
        "WHERE source = 'tdx_gbbq' ORDER BY source_version DESC LIMIT 1")
    if latest and latest[0]['source_version'] == src_version:
        return {'success': True, 'changed': False,
                'stats': {'source_version': src_version}}
    # 版本变化：解析入库（pack 已在上方解压校验）
    from ..data_sources.tdx_reference_files import parse_gbbq_records
    from .review_common import code6_to_ts_code
    recs = [r for r in parse_gbbq_records(pack['raw'])
            if r['category'] in (1, 2)]
    rows = []
    for r in recs:
        try:
            ts = code6_to_ts_code(r['code'])
        except Exception:
            continue
        if r['category'] == 1:
            ev_key = f"{r['datetime']}:{round(r['f1'],4)}:{round(r['f2'],4)}:{round(r['f3'],4)}:{round(r['f4'],4)}"
        else:
            ev_key = f"{r['datetime']}:cat2:{r['f2']}:{r['f4']}"
        rows.append({'source': 'tdx_gbbq', 'source_version': src_version,
                     'ts_code': ts, 'effective_date': str(r['datetime']),
                     'event_key': ev_key, 'category': r['category'],
                     'raw_event_type': '除权除息' if r['category'] == 1
                     else '送配股上市:f1前流通/f2前总/f3后流通/f4后总',
                     'cash_dividend': r['f1'], 'cash_reference': r['f2'],
                     'bonus_ratio': r['f3'], 'rights_issue_ratio': r['f4'],
                     'raw_unit': 'per10' if r['category'] == 1 else 'wan_gu',
                     'observed_at': _dt.datetime.now()})
    stats = {'source_version': src_version, 'events': len(rows),
             'prev': (latest or [{}])[0].get('source_version')}
    if rows and not dry_run:
        db.upsert_stock_action_events(rows)
    return {'success': True, 'changed': True, 'stats': stats}
