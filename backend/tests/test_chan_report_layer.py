"""缠论报告层测试：第六章小节三态 + verifier 缠论池。"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from src.review.sections import chan_candidates  # noqa: E402
from src.review import verifier  # noqa: E402


@dataclass
class FakeAggResult:
    available: bool = True
    reason: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)


def _hit(code="000733", name="振华科技", kind="一买", div=0.08, refire=False):
    return {'ts_code': f'sz.{code}', 'code': code, 'name': name, 'kind': kind,
            'label': f'一买/背驰{div:.2f}/触发36.26/中枢[38.58,41.73]',
            'stop': 36.26, 'trigger_low': 36.26, 'zg': 41.73, 'zd': 38.58,
            'divergence': div, 'refire': refire}


# ---------------------------------------------------------------- 小节三态
def test_section_with_hits():
    agg = {'chan': FakeAggResult(data={'items': [_hit(), _hit('002057', '中钢天源', div=0.12),
                                                _hit('300071', '福石控股', kind='二买')],
                                       'count': 3})}
    blocks = chan_candidates.build(agg, {})
    kinds = [b['type'] for b in blocks]
    assert kinds[0] == 'heading' and 'table' in kinds and 'note' in kinds
    table = next(b for b in blocks if b['type'] == 'table')
    assert len(table['rows']) == 3
    assert table['rows'][0][2] == '一买'      # 一买在前
    # V4.3 C1：插入「收盘距止损%」列（索引 4）——背驰强度后移至索引 6
    assert table['headers'][4] == '收盘距止损%'
    assert table['rows'][0][6] == '0.08'      # 背驰强度升序
    assert '历史统计非预测' in ''.join(b.get('text', '') for b in blocks)


def test_section_empty_items_normal_state():
    agg = {'chan': FakeAggResult(data={'items': [], 'count': 0})}
    blocks = chan_candidates.build(agg, {})
    text = ''.join(b.get('text', '') for b in blocks)
    assert '今日无一买/二买信号确认' in text
    assert '历史统计非预测' in text


def test_section_unavailable_degrades():
    agg = {'chan': FakeAggResult(available=False, reason='缠论面板复算失败: boom')}
    blocks = chan_candidates.build(agg, {})
    assert len(blocks) == 2
    assert '⏳ 待补' in blocks[1]['text'] and 'boom' in blocks[1]['text']


def test_section_max_rows_truncation_and_refire_note():
    items = [_hit(f'{600000 + i}', f'股{i}') for i in range(15)]
    items[0]['refire'] = True
    agg = {'chan': FakeAggResult(data={'items': items, 'count': 15})}
    blocks = chan_candidates.build(agg, {'chan_candidates': {'max_rows': 10}})
    table = next(b for b in blocks if b['type'] == 'table')
    assert len(table['rows']) == 10
    text = ''.join(b.get('text', '') for b in blocks)
    assert '全量 15 个' in text and '同中枢重发' in text


def test_section_disabled():
    blocks = chan_candidates.build({'chan': FakeAggResult(data={'items': [_hit()]})},
                                   {'chan_candidates': {'enabled': False}})
    assert blocks == []


def test_section_parity_mismatch_flagged():
    agg = {'chan': FakeAggResult(data={'items': [_hit()],
                                       'parity': {'match': False, 'report_n': 1,
                                                  'chain_n': 2, 'diff': [('000733', 'x')]}})}
    blocks = chan_candidates.build(agg, {})
    assert any('对拍不一致' in b.get('text', '') for b in blocks)


# ---------------------------------------------------------------- verifier
def _fake_agg_with_chan():
    return {'chan': FakeAggResult(data={'items': [_hit(), _hit('002057', '中钢天源')]}),
            'sentiment': {}, 'market_env': {}, 'volume_price': {}, 'movers': {},
            'candidates': {}}


def test_build_predictions_includes_chan_pool():
    pred = verifier.build_predictions(_fake_agg_with_chan(), {})
    assert 'CHAN' in pred['candidates']
    assert len(pred['candidates']['CHAN']) == 2
    assert pred['candidates']['CHAN'][0]['code'] == '000733'
    assert pred['candidates']['CHAN'][0]['label'].startswith('一买')


def test_verify_candidates_chan_pool_stats():
    pools = {'R1': [], 'R2': [], 'R3': [], 'R4': [],
             'CHAN': [{'code': '000733', 'name': '振华科技', 'label': '一买/…'},
                      {'code': '002057', 'name': '中钢天源', 'label': '一买/…'}]}
    import pandas as pd
    cs = pd.DataFrame({
        'stock_code': ['000733', '002057', '600000'],
        'change_rate': [3.5, -1.0, 1.0],
        '_traded': [True, True, True],
        '_limit_up': [False, False, False],
        '_burst': [False, False, False],
        '_limit_down': [False, False, False],
    })
    out = verifier._verify_candidates(pools, cs)
    chan = out['pools']['CHAN']
    assert chan['count'] == 2 and chan['traded'] == 2
    assert chan['win_rate'] == 0.5 and chan['mean'] == pytest.approx(1.25)


def test_verify_candidates_chan_pool_absent_graceful():
    """旧 payload（无 CHAN 池）→ 空池统计，不报错。"""
    pools = {'R1': [], 'R2': [], 'R3': [], 'R4': []}
    out = verifier._verify_candidates(pools, None)
    assert out['pools']['CHAN']['count'] == 0
