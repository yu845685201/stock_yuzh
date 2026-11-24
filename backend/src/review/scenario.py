"""盘前情景判档（每日复盘报告优化技术方案 §3.1，2026-11-24）。

取数与计算分离：

- ``read_anchor_quote``：按时点读取合格报价（业务日约束防前视）；
- ``build_anchor_obs``：单锚观测（基准 + 新报价 → 变动与缺失原因）；
- ``classify_scenario``：纯函数判档（先完整度后方向），状态取值
  P1 / P2 / P3 / 分歧 / 部分证据 / 待核 / 无新观测。

防前视核心规则（A1，2026-11-23 案例）：核验 A 股 ``as_of`` 日开盘情景的海外
新观测，其交易时段必须在 ``as_of`` 开盘（09:15 盘前截止，09:30 开盘）之前已
完整结束——即海外业务日**严格早于** ``as_of``：

- 恒指 ``as_of`` 当日 16:00 收盘晚于 A 股当日 09:30 开盘 → 不可用；
- 纳指 ``as_of`` 当日收盘为北京 ``as_of`` 次日凌晨 → 不可用；
- 假期累计同理：取该海外市场自身日历上业务日 < as_of 的最新收盘。

「无新观测」与「零变动」区分：同一业务日旧报价重复出现记无新观测；真实新
收盘与上次同价属有效新观测（记零变动，P2 证据）。判断依据是业务日而非价格。
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

#: 判档状态（classify_scenario.triggered 取值域）
P1 = 'P1'
P2 = 'P2'
P3 = 'P3'
DIVERGENT = '分歧'
PARTIAL = '部分证据'
PENDING = '待核'
NO_NEW = '无新观测'

#: 方向核验资格：仅 P1/P3 产生方向预期（低开/高开）并进入滚动命中率分母；
#: 分歧/部分证据单列，待核/无新观测排除于分母（数据不足 ≠ 判错）。
DIRECTION_STATUSES = (P1, P3)

#: 方向预期文案（P1 低开 / P3 高开；消费方 verifier）
DIRECTION_EXPECTATIONS = {P1: '低开', P3: '高开'}

#: 阈值比较容差：仅消除「数学上恰为 ±gap」的二进制浮点表示误差
#: （如 24613×1.015 → 1.4999999…%），不改变未舍入判定口径。
_EPS = 1e-9


def read_anchor_quote(reader, market_code: Optional[str], as_of: str,
                      base_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """读取 A 股 as_of 开盘前可知的海外最新合格收盘（取数侧）。

    SQL 约束 ``trade_date < as_of``：同业务日港美收盘（恒指 as_of 当日行、
    纳指 as_of 当日行）一律剔除——它们在 A 股 as_of 开盘后才产生。行内
    ``create_time``（系统首次入库时间）一并带出作观察证据：滞后于 as_of 开盘
    说明该行是事后补录（判档主规则只依赖业务日，不受其影响；回放研究时人工
    核对，区分「当时已入库」与「补录」）。

    Args:
        reader: DataReader（_query 只读）。
        market_code: 外围指数代码（None → 无锚可用）。
        as_of: A 股锚定日（YYYYMMDD）。
        base_date: 预测冻结的基准业务日（仅用于调用方判断 is_new，本函数不依赖）。

    Returns:
        {'close': float, 'date': str, 'first_seen': str|None}；无合格行/读失败
        返回 None。
    """
    if not market_code:
        return None
    try:
        rows = reader._query(
            "SELECT close, trade_date, create_time FROM overseas_kline_day "
            "WHERE market_code = %s AND close IS NOT NULL AND trade_date < %s "
            "ORDER BY trade_date DESC LIMIT 1", (market_code, as_of))
    except Exception as e:
        logger.warning(f'情景锚报价读取失败（{market_code}）: {e}')
        return None
    if not rows or rows[0][0] is None:
        return None
    r = rows[0]
    first_seen = (str(r[2]) if len(r) > 2 and r[2] is not None else None)
    return {'close': float(r[0]), 'date': str(r[1]), 'first_seen': first_seen}


def build_anchor_obs(name: str, base: Dict[str, Any],
                     quote: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """单锚观测：冻结基准 + 开盘前可知新报价 → 变动百分比与缺失原因。

    has_new 依据业务日（quote.date != base.date）而非价格：同一业务日重复
    报价记「无新观测」；真实新收盘与上次同价属有效新观测（move_pct=0）。
    基准缺失或非正 → has_new=False（无可比变动）。
    """
    base_close = base.get('close') if base else None
    base_date = base.get('date') if base else None
    base_valid = base_close is not None and float(base_close) > 0
    quote_date = (quote or {}).get('date')
    has_new = bool(quote and base_valid and quote_date != base_date)
    move = None
    if has_new and quote.get('close') is not None and float(quote['close']) > 0:
        move = 100.0 * (float(quote['close']) / float(base_close) - 1.0)
    obs: Dict[str, Any] = {
        'name': name,
        'base_close': float(base_close) if base_valid else None,
        'base_date': base_date,
        'base_valid': base_valid,
        'new_close': float(quote['close']) if has_new else None,
        'new_date': quote_date if has_new else None,
        'first_seen': (quote or {}).get('first_seen'),
        'has_new': has_new,
        'move_pct': move,
        'reason': None,
    }
    if not base_valid:
        obs['reason'] = '基准缺失或非正'
    elif quote is None:
        obs['reason'] = '无开盘前可知报价'
    elif not has_new:
        obs['reason'] = '无新观测（最新合格报价与基准同业务日）'
    return obs


def _other_reason(obs_list: List[Dict[str, Any]],
                  except_obs: Dict[str, Any]) -> str:
    for o in obs_list:
        if o is not except_obs:
            return str(o.get('reason') or '缺失')
    return '缺失'


def classify_scenario(hsi: Dict[str, Any], nasdaq: Dict[str, Any],
                      gap_pct: float) -> Dict[str, Any]:
    """纯函数判档（方案 §3.1 规则表；按未取整变动判定，展示层自行舍入）。

    输入为 build_anchor_obs 产物（两锚）。规则（阈值为百分数，如 1.5）：

    +-------------------------------------------+--------------------+
    | 两锚输入                                   | 输出                |
    +===========================================+====================+
    | 两锚有效更新，一侧或两侧 ≥ +gap，无锚 ≤ −gap  | P3                 |
    +-------------------------------------------+--------------------+
    | 两锚有效更新，一侧或两侧 ≤ −gap，无锚 ≥ +gap  | P1                 |
    +-------------------------------------------+--------------------+
    | 两锚均有新收盘，变动绝对值均 < gap            | P2（同价可计 0%）   |
    +-------------------------------------------+--------------------+
    | 两锚分别向相反方向越线                       | 分歧（单列）        |
    +-------------------------------------------+--------------------+
    | 唯一合格新锚越线                            | 部分证据（单列）    |
    +-------------------------------------------+--------------------+
    | 可用新锚少于两个且无有效越线，或基准不可验证   | 待核（列明原因）    |
    +-------------------------------------------+--------------------+
    | 两锚基准有效但均无新观测                     | 无新观测            |
    +-------------------------------------------+--------------------+

    Returns:
        {'triggered', 'completeness', 'moves', 'direction_eligible', 'reason'}；
        moves 每锚含 accum（两位小数展示值）、原始 base/new、has_new 与缺失原因。
    """
    obs_list = [hsi, nasdaq]
    gap = float(gap_pct)
    moves: List[Dict[str, Any]] = []
    for o in obs_list:
        moves.append({
            'name': o.get('name'),
            'accum': (round(float(o['move_pct']), 2)
                      if o.get('move_pct') is not None else None),
            'base_close': o.get('base_close'),
            'base_date': o.get('base_date'),
            'last_close': o.get('new_close'),
            'new_date': o.get('new_date'),
            'first_seen': o.get('first_seen'),
            'has_new': bool(o.get('has_new')),
            'reason': o.get('reason'),
        })

    new_obs = [o for o in obs_list
               if o.get('has_new') and o.get('move_pct') is not None]
    raw_moves = [float(o['move_pct']) for o in new_obs]
    has_down = any(m <= -gap + _EPS for m in raw_moves)
    has_up = any(m >= gap - _EPS for m in raw_moves)
    base_invalid = [o for o in obs_list if not o.get('base_valid')]

    if len(new_obs) == 2:
        if has_down and has_up:
            triggered, completeness = DIVERGENT, '两锚有效·反向'
            reason = '两锚分别向相反方向越线——单列，不计方向战绩'
        elif has_down:
            triggered, completeness = P1, '两锚有效'
            reason = '一锚或两锚跌幅越线'
        elif has_up:
            triggered, completeness = P3, '两锚有效'
            reason = '一锚或两锚涨幅越线'
        else:
            triggered, completeness = P2, '两锚有效'
            reason = f'两锚变动绝对值均小于 {gap:g}%（真实新收盘同价可计 0%）'
    elif len(new_obs) == 1:
        only = new_obs[0]
        other = _other_reason(obs_list, only)
        if raw_moves[0] <= -gap + _EPS or raw_moves[0] >= gap - _EPS:
            triggered = PARTIAL
            completeness = '单锚证据'
            reason = f"唯一合格新锚（{only.get('name')}）越线；另一锚：{other}"
        else:
            triggered = PENDING
            completeness = '单锚证据'
            reason = (f'唯一合格新锚（{only.get("name")}）未越线，'
                      f'另一锚：{other}——可用新锚不足两个，排除于方向命中率分母')
    else:
        if base_invalid:
            triggered = PENDING
            completeness = '基准不可验证'
            reason = '；'.join(str(o.get('reason')) for o in base_invalid)
        else:
            triggered = NO_NEW
            completeness = '无新观测'
            reason = '；'.join(str(o.get('reason'))
                              for o in obs_list if o.get('reason')) \
                or '两锚均无新观测（排除于方向命中率分母）'

    return {
        'triggered': triggered,
        'completeness': completeness,
        'moves': moves,
        'direction_eligible': triggered in DIRECTION_STATUSES,
        'reason': reason,
    }
