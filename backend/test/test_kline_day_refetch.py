"""B-1 回归测试：除权检测命中 → 整股重拉自愈路径可正常执行

修复前：process_one_stock 命中分支引用裸名 logger → NameError → 被 safe 包装吞掉，
除权自愈重拉路径从未成功执行过（重构方案 §4 B-1）。
"""

import logging
import sys
import os
from typing import Any, Dict, List

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.sync.kline_day_pipeline import (
    KlineDayDeps,
    KlineDaySharedState,
    process_one_stock,
)


class FakeTdxSource:
    def __init__(self):
        self.qfq_full_refresh = None
        self.qfq_full_calls = 0

    def get_kline_qfq_tail(self, stock_code: str, tail_limit: int) -> List[Dict[str, Any]]:
        return [{'trade_date': '20260923', 'close': '10.0', 'open': '10.0'}]

    def get_kline_raw_tail(self, stock_code: str, tail_limit: int) -> List[Dict[str, Any]]:
        return [{'trade_date': '20260923', 'close': '10.0', 'open': '10.0'}]

    def get_kline_qfq_full(self, stock_code: str, refresh: bool = False) -> List[Dict[str, Any]]:
        self.qfq_full_calls += 1
        self.qfq_full_refresh = refresh
        return [{'trade_date': '20260923', 'close': '10.0', 'open': '10.0'}]

    def get_kline_raw_full(self, stock_code: str) -> List[Dict[str, Any]]:
        return [{'trade_date': '20260923', 'close': '10.0', 'open': '10.0'}]


class FakeCsvWriter:
    def write_his_kline_day_raw(self, ts_code: str, rows: List[Dict[str, Any]]) -> None:
        pass


class FakeDbConn:
    def __init__(self):
        self.replaced: List[Any] = []

    def replace_his_kline_day(self, ts_code: str, kline_data: List[Dict[str, Any]]) -> int:
        self.replaced.append((ts_code, kline_data))
        return len(kline_data)


def _make_deps(tdx: FakeTdxSource, db: FakeDbConn) -> KlineDayDeps:
    return KlineDayDeps(
        tdx_api_source=tdx,
        csv_writer=FakeCsvWriter(),
        db_conn=db,
        logger=logging.getLogger('test_b1'),
        fundamentals_map={},
        trade_dates=['20260923'],
        init_mode=False,
        is_explicit_range=False,
        save_to_csv=True,
        save_to_db=True,
        fetch_full=False,
        tail_limit=5,
        total_stocks=1,
        log_progress=lambda *args, **kwargs: None,
        merge_sources=lambda qfq_list, raw_list: [{'trade_date': '20260923', 'close': '10.0', 'open': '10.0'}],
        detect_refetch=lambda ts_code, merged: (True, {'reason': 'unit-test-hit'}),
        filter_raw=lambda raw, trade_dates: [],
        normalize_records=lambda merged, stock, fmap, allowed_dates=None: (
            [{'ts_code': 'sz.000001', 'trade_date': '20260923'}], []
        ),
    )


def test_refetch_hit_path_executes_without_nameerror():
    """命中检测后应完成整股重拉（refresh=True）并整股替换写库，全程无 NameError"""
    tdx = FakeTdxSource()
    db = FakeDbConn()
    deps = _make_deps(tdx, db)
    ctx = KlineDaySharedState(result={'errors': [], 'failed_stocks': 0, 'records': 0, 'db_rows': 0})

    res = process_one_stock(
        {'ts_code': 'sz.000001', 'stock_code': '000001', 'stock_name': '平安银行'},
        1,
        ctx=ctx,
        deps=deps,
    )

    assert res['error'] is None
    assert res['detection'] == {'checked': 1, 'hits': 1, 'refetched': 1}
    assert res['detection_details'] == [{'ts_code': 'sz.000001', 'reason': 'unit-test-hit'}]
    # 命中后必须 refresh=True 绕过缓存回源（增强路径语义）
    assert tdx.qfq_full_calls == 1
    assert tdx.qfq_full_refresh is True
    # refetched=True → 走整股原子替换而非 upsert
    assert len(db.replaced) == 1
    assert db.replaced[0][0] == 'sz.000001'
    assert res['records'] == 1
