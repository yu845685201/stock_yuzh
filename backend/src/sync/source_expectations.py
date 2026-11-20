"""源期望注册表（每日复盘采集与分析闭环技术方案 V1.0 §3.1，D01）

统一提供：市场日历、收盘时段、发布延迟与数据集注册表，输出
``expected_partition(context)``——逐数据集的"应到业务日 + 应到时刻 + 状态提示"。

设计红线（技术方案 §3.1）：

- 尚未发布的源返回 ``NOT_DUE``，不算采集失败，指标显示"尚未到时"；
- 日历未知（如美股/港股尚无当地交易日历表）返回待核（``CALENDAR_UNKNOWN``），
  **不能用自然日差硬算过期**；
- 两融按其业务日（T−1 交易日）与发布延迟判断应到；
- 美国时区按当地时区与夏令时转换（zoneinfo 内置）。

本模块只做"期望"计算，不发起网络请求；日历查询通过 db.execute_query
（DatabaseConnection）可选注入，缺省时仅做静态判定。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

from .collection_contracts import (
    REASON_CALENDAR_UNKNOWN,
    REASON_NOT_PUBLISHED_YET,
    RunContext,
    normalize_ymd,
)

logger = logging.getLogger(__name__)

_TZ_SH = ZoneInfo('Asia/Shanghai')
_TZ_HK = ZoneInfo('Asia/Hong_Kong')
_TZ_NY = ZoneInfo('America/New_York')

# 市场时区与当地收盘时刻（HH:MM，当地时区；仅用于应到时刻计算）
MARKET_SESSIONS: Dict[str, Dict[str, Any]] = {
    'CN': {'tz': _TZ_SH, 'close': time(15, 0), 'label': 'A股'},
    'HK': {'tz': _TZ_HK, 'close': time(16, 10), 'label': '港股'},
    'US': {'tz': _TZ_NY, 'close': time(16, 0), 'label': '美股'},
}


@dataclass
class DatasetSpec:
    """数据集注册表条目：源、市场、交易所、发布延迟与数据依赖"""

    dataset: str                       # 数据集名（= 编排源名，唯一）
    source: str                        # 上游源标识
    market: str                        # CN / HK / US / MULTI
    exchanges: List[str] = field(default_factory=list)   # 需分别验收的交易所
    publish_delay_minutes: int = 15    # 当地收盘后到数据可得的典型延迟
    business_date_offset: int = 0      # 业务日相对 A 股锚定日的偏移（交易日，两融=-1）
    depends_on: tuple = ()             # 数据依赖（数据集名）
    notes: str = ''


# 数据集注册表（对齐编排器源名；发布延迟为默认值，可由 config.review_sync
# .source_expectations.<dataset>.publish_delay_minutes 覆盖）
DATASET_REGISTRY: Dict[str, DatasetSpec] = {
    # ---- A 股行情族 ----
    'kline': DatasetSpec('kline', 'tdx-api', 'CN', ['sse', 'szse', 'bse'], 15,
                         notes='全市场日K（一键模式内置伪源）'),
    'index': DatasetSpec('index', 'tdx-api', 'CN', ['sse', 'szse'], 15,
                         notes='境内指数日K（TDX 7 个配置指数逐代码验收）'),
    'valuation_idx': DatasetSpec('valuation_idx', 'csindex', 'CN', [], 30,
                                 notes='中证接口指数估值与行情分开记账（D04 落地）'),
    'factor': DatasetSpec('factor', 'tdx-api', 'CN', ['sse', 'szse', 'bse'], 15,
                          depends_on=('kline',)),
    'bj_kline': DatasetSpec('bj_kline', 'eastmoney', 'CN', ['bse'], 15),
    # ---- 资金与情绪族 ----
    'money_flow': DatasetSpec('money_flow', 'tdx-api', 'CN', ['sse', 'szse', 'bse'], 15,
                              depends_on=('kline',), notes='分笔资金流+封板统计'),
    'sentiment_metrics': DatasetSpec('sentiment_metrics', 'ths', 'CN', [], 30,
                                     depends_on=('kline', 'ths_finance')),
    'valuation_calc': DatasetSpec('valuation_calc', 'self-calc', 'CN', [], 20,
                                  depends_on=('kline', 'ths_finance')),
    'lhb': DatasetSpec('lhb', 'sse+szse', 'CN', ['sse', 'szse'], 150,
                       notes='龙虎榜沪深分别发布（沪市约 18:00 后，深市约 17:10）'),
    'margin': DatasetSpec('margin', 'sse+szse', 'CN', ['sse', 'szse'], 60,
                          business_date_offset=-1,
                          notes='两融 T−1 业务日、T 日盘后发布'),
    'hkex_connect': DatasetSpec('hkex_connect', 'hkex', 'HK', ['north', 'south'], 60),
    'etf_scale': DatasetSpec('etf_scale', 'sse+szse', 'CN', ['sse', 'szse'], 40,
                             notes='沪市按 STAT_DATE 查历史；深市仅最新快照'),
    # ---- 外围族（当地日历未接入前一律待核） ----
    'overseas': DatasetSpec('overseas', 'sina', 'MULTI', [], 10,
                            notes='实时快照；源报价时间由 D04 补齐 TIME_UNVERIFIED 判定'),
    'overseas_kline': DatasetSpec('overseas_kline', 'ifzq', 'MULTI', [], 30,
                                  notes='外围日K逐市场验收（D04 落地）'),
    'option': DatasetSpec('option', 'sse', 'CN', ['sse'], 30),
    # ---- 公告/披露/新闻/宏观（日历无关或盘后陆续发布） ----
    'announcement': DatasetSpec('announcement', 'eastmoney', 'CN', [], 0),
    'disclosure': DatasetSpec('disclosure', 'sse+szse', 'CN', ['sse', 'szse'], 0),
    'news': DatasetSpec('news', 'cls', 'CN', [], 0),
    'macro': DatasetSpec('macro', 'eastmoney', 'CN', [], 0,
                         notes='按指标发布频率判断应到（D04 扩展）'),
    'ths_finance': DatasetSpec('ths_finance', 'ths', 'CN', [], 0,
                               depends_on=('announcement', 'disclosure')),
    'industry': DatasetSpec('industry', 'ths', 'CN', [], 0),
    'concept': DatasetSpec('concept', 'ths', 'CN', [], 0),
    'unlock': DatasetSpec('unlock', 'eastmoney', 'CN', [], 0),
    'ths_l2': DatasetSpec('ths_l2', 'ths', 'CN', [], 0),
    'shares_xdxr': DatasetSpec('shares_xdxr', 'baostock', 'CN', [], 0),
}


def _latest_closed_before(db: Any, target_date_iso: str, market: str) -> Optional[str]:
    """交易日历中 ≤ 给定日的最近交易日（A股日历 base_trade_calendar；横线格式）。

    仅 CN 市场有本地日历；其他市场返回 None（日历未知 → 待核）。
    """
    if market not in ('CN',) or db is None:
        return None
    try:
        rows = db.execute_query(
            "SELECT calendar_date FROM base_trade_calendar "
            "WHERE calendar_date <= %s AND is_trading_day = 1 "
            "ORDER BY calendar_date DESC LIMIT 1", (target_date_iso,))
        if not rows:
            return None
        d = rows[0]['calendar_date']
        return (d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d))
    except Exception as e:
        logger.warning(f'交易日历查询失败（按待核处理）: {e}')
        return None


def _prev_trade_date(db: Any, target_date_iso: str) -> Optional[str]:
    """A股日历中严格早于给定日的最近交易日"""
    if db is None:
        return None
    try:
        rows = db.execute_query(
            "SELECT calendar_date FROM base_trade_calendar "
            "WHERE calendar_date < %s AND is_trading_day = 1 "
            "ORDER BY calendar_date DESC LIMIT 1", (target_date_iso,))
        if not rows:
            return None
        d = rows[0]['calendar_date']
        return (d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d))
    except Exception as e:
        logger.warning(f'交易日历查询失败（按待核处理）: {e}')
        return None


def expected_partition(ctx: RunContext, dataset: str, db: Any = None,
                       config: Optional[Dict[str, Any]] = None,
                       cutoff_local: Optional[datetime] = None) -> Dict[str, Any]:
    """逐数据集输出应到分区（技术方案 §3.1 expected_partition(context)）。

    Returns:
        {
          'dataset', 'market', 'exchanges',
          'expected_date': YYYYMMDD 或 None（日历未知→待核）,
          'due_at': UTC datetime 或 None,
          'status_hint': 'OK' | 'NOT_DUE' | 'CALENDAR_UNKNOWN',
          'reason_codes': [...],
        }
    status_hint 说明：NOT_DUE = 截止时刻尚未应发布（不算采集失败）；
    CALENDAR_UNKNOWN = 日历未知待核；OK = 可按 expected_date 验收。
    """
    spec = DATASET_REGISTRY.get(dataset)
    if spec is None:
        return {'dataset': dataset, 'market': 'UNKNOWN', 'exchanges': [],
                'expected_date': None, 'due_at': None,
                'status_hint': 'CALENDAR_UNKNOWN',
                'reason_codes': [REASON_CALENDAR_UNKNOWN]}

    target = normalize_ymd(ctx.target_date)
    market = spec.market
    cfg = ((config or {}).get('source_expectations', {}) or {}).get(dataset, {}) or {}
    delay_min = int(cfg.get('publish_delay_minutes', spec.publish_delay_minutes))

    # ---- 业务日（相对 A 股锚定日的交易日偏移，如两融 -1）----
    expected_iso: Optional[str] = None
    reason_codes: List[str] = []
    if market == 'CN':
        # A 股锚定日本身必须是交易日：非交易日（周末/节假日）目标 → 该数据集
        # 不适用（静态判断不够严谨，日历可查时以日历为准）
        if db is not None:
            latest = _latest_closed_before(db, f'{target[:4]}-{target[4:6]}-{target[6:]}', market)
            if latest is None:
                reason_codes.append(REASON_CALENDAR_UNKNOWN)
            else:
                latest_compact = latest.replace('-', '')
                expected_iso = latest_compact
                # business_date_offset：两融等 T-1 业务日数据集
                off = int(cfg.get('business_date_offset', spec.business_date_offset))
                for _ in range(max(0, -off)):
                    prev = _prev_trade_date(db, f'{latest_compact[:4]}-{latest_compact[4:6]}-{latest_compact[6:]}')
                    if prev is None:
                        reason_codes.append(REASON_CALENDAR_UNKNOWN)
                        break
                    latest_compact = prev.replace('-', '')
                    expected_iso = latest_compact
        else:
            # 无日历连接：DAILY 目标日本身可信任（编排器已按日历解析），
            # 但业务日偏移（两融 T-1 等）无法展开 → 不返回应到日，按待核处理，
            # 不能用自然日差硬算 T-1
            off = int(cfg.get('business_date_offset', spec.business_date_offset))
            if off < 0:
                expected_iso = None
                reason_codes.append(REASON_CALENDAR_UNKNOWN)
            else:
                expected_iso = target
    else:
        # HK/US/MULTI：当地日历未接入 → 待核（不能用自然日差硬算过期）
        reason_codes.append(REASON_CALENDAR_UNKNOWN)

    # ---- 应到时刻（当地收盘 + 发布延迟 → UTC）----
    due_at: Optional[datetime] = None
    status_hint = 'OK'
    if market in MARKET_SESSIONS and expected_iso:
        sess = MARKET_SESSIONS[market]
        tz = sess['tz']
        try:
            local_due = datetime.combine(
                datetime.strptime(expected_iso, '%Y%m%d').date(), sess['close'], tzinfo=tz)
            local_due += timedelta(minutes=delay_min)
            due_at = local_due.astimezone(timezone.utc)
        except ValueError:
            reason_codes.append(REASON_CALENDAR_UNKNOWN)
        cutoff = ctx.cutoff_at or (cutoff_local.astimezone(timezone.utc)
                                   if cutoff_local else None)
        if due_at is not None and cutoff is not None and cutoff < due_at:
            status_hint = 'NOT_DUE'
            reason_codes.append(REASON_NOT_PUBLISHED_YET)
    elif expected_iso is None:
        status_hint = 'CALENDAR_UNKNOWN'

    return {
        'dataset': dataset,
        'market': market,
        'exchanges': list(spec.exchanges),
        'expected_date': expected_iso,
        'due_at': due_at,
        'status_hint': status_hint,
        'reason_codes': reason_codes,
    }


def expected_partitions_for(ctx: RunContext, datasets: List[str], db: Any = None,
                            config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """批量应到分区（编排器接入用，保持注册表声明序）"""
    return [expected_partition(ctx, d, db=db, config=config) for d in datasets]
