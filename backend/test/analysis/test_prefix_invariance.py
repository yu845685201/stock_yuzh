"""前缀不变性（P1-6 方案 §4.3 ②）：截面模块输出只依赖 ≤锚定日数据。

对 daily_review 链全部模块（存量 + 新增，随链配置自动扩展）做两项检验：

- **A 前缀缩短**：全量窗口 vs 删掉最旧一行的窗口（锚定行不变）→ 输出必须
  逐值一致（证明模块不依赖超出所需回看的历史，与声明 max_lookback 相洽）；
- **B 未来行防御**：窗口尾部追加两个未来交易日 → 模块不得把未来行当锚定行
  静默使用（读 df 的模块必须报「锚定日无 K 线」类数据不足；不读 df 的模块
  输出不变）。

实现说明（对方案原文的显式偏离）：方案原文「截断到锚定日前一日的 df」若按
字面实施会删掉锚定行本身、使模块无法产出锚定日输出；本测试按「截断前缀
（删最旧行）/ 追加未来行」两种可判定构造落地，直接覆盖「只依赖 ≤锚定日
数据」的目标语义。新模块验收门槛：不过本测试不得合并（方案红线）。
"""

from datetime import date, timedelta

import pandas as pd
import pytest

from src.analysis.chain_config import load_chain
from src.analysis.module_base import ChainContext, ModuleResult, StockInfo
from src.analysis.module_registry import build as build_module, load_registry
from src.analysis.modules.money_trend import _HISTORY_N

#: 合成窗口长度（> 最大 warmup 133，保证指标类模块满窗）
_N_DAYS = 140
_ANCHOR = '20260930'
_TS = 'sh.600001'


def _synthetic_dates(n: int, end: date = date(2026, 9, 30)) -> list:
    """合成交易日序列（自然日近似，模块只认字符串顺序与连续性）。"""
    days, d = [], end
    while len(days) < n:
        if d.weekday() < 5:
            days.append(d.strftime('%Y%m%d'))
        d -= timedelta(days=1)
    return list(reversed(days))


def _synthetic_df(n: int = _N_DAYS, future: int = 0) -> pd.DataFrame:
    """合成日 K：涨跌停交替（含一根 ST 行）、放量模式、下跌行。

    ``future > 0`` 时窗口末端延到锚定日之后（模拟未来行），锚定行仍在
    ``20260930``（未来行出现在其后）。
    """
    end = date(2026, 9, 30) + timedelta(days=future * 3)
    dates = _synthetic_dates(n + future, end=end)
    rows = []
    for i, d in enumerate(dates):
        is_st = 1 if i == 5 else 0
        limit_up = (i % 7 == 3)
        preclose = 10.0
        close = 11.0 if limit_up else (9.0 if i % 11 == 5 else 10.0 + 0.01 * (i % 5))
        high = max(close, 10.05 if limit_up else close + 0.02)
        low = min(close, 9.95 if i % 13 == 2 else close - 0.02)
        rows.append({
            'ts_code': _TS, 'stock_code': '600001', 'stock_name': '测试股',
            'trade_date': d, 'open': 10.0, 'high': high, 'low': low,
            'close': close, 'preclose': preclose,
            'change_rate': round((close / preclose - 1) * 100, 2),
            'volume': 1000.0 * (1 + (i % 3)),
            'amount': 10000.0 * (1 + (i % 3)),
            'is_st': is_st,
        })
    return pd.DataFrame(rows)


def _ctx(as_of=_ANCHOR):
    """最小 env（各查表空/常量 dict）——模块不查库，只读 upstream。"""
    return ChainContext(
        as_of=as_of,
        stock=StockInfo(ts_code=_TS, stock_code='600001',
                        stock_name='测试股', board='主板'),
        upstream={
            'cand_seal_open_map': {_TS: 0},
            'cand_theme_map': {_TS: ['测试概念']},
            'cand_top_concepts': ['测试概念'],
            'cand_institution_net_map': {_TS: 1000.0},
            'mf_history_map': {_TS: [(f'202609{20 + j}', 100.0 * (j + 1))
                                     for j in range(_HISTORY_N)]},
            'cand_announcement_map': {_TS: []},
            'valuation_map': {},
            '是否涨停': ModuleResult('是'),
        },
    )


def _chain_modules():
    """daily_review 链全部模块实例（随链配置自动扩展，新模块自动入检）。"""
    registry = load_registry()
    chain = load_chain('daily_review')
    return [(spec.name_en, build_module(spec.name_en))
            for spec in chain.modules if spec.name_en in registry]


class TestPrefixInvariance:
    @pytest.mark.parametrize('name_en,module', _chain_modules(),
                             ids=[n for n, _ in _chain_modules()])
    def test_a_prefix_shortening(self, name_en, module):
        """删掉最旧一行（锚定行不变）→ 输出一致。"""
        full = _synthetic_df()
        short = full.iloc[1:].reset_index(drop=True)
        r_full = module.analyze(full, _ctx())
        r_short = module.analyze(short, _ctx())
        assert r_full.value == r_short.value, (
            f'{name_en} 前缀缩短后输出漂移: {r_full.value!r} → {r_short.value!r}')

    @pytest.mark.parametrize('name_en,module', _chain_modules(),
                             ids=[n for n, _ in _chain_modules()])
    def test_b_future_rows_defense(self, name_en, module):
        """追加两个未来交易日 → 不得静默改用未来行作锚定。"""
        full = _synthetic_df()
        with_future = _synthetic_df(future=2)
        assert with_future['trade_date'].iloc[-1] > full['trade_date'].iloc[-1]
        r_full = module.analyze(full, _ctx())
        r_future = module.analyze(with_future, _ctx())
        if r_future.value == r_full.value:
            return                       # 不读 df 的模块：输出不变 ✓
        # 读 df 的模块：必须显式报锚定日不匹配（禁静默用未来行）
        assert r_future.value == '数据不足', (
            f'{name_en} 面对未来行输出了非数据不足值: {r_future.value!r}')
        reason = str(r_future.detail.get('reason', ''))
        assert '锚定日' in reason or '无K线' in reason, (
            f'{name_en} 未来行拒绝原因不透明: {reason!r}')

    def test_all_chain_modules_covered(self):
        """链模块全部实例化成功（防 registry 与链配置脱节）。"""
        mods = _chain_modules()
        assert len(mods) >= 21, f'覆盖模块过少: {len(mods)}'
