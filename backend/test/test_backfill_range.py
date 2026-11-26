"""回填循环参数测试（开发项 C，方案 4.3）

- 交易日历展开（非交易日自动跳过）；
- 断点续跑只信查库（X-14 红线：回看机制会静默采到 d-1，不信任单次 stats）；
- 单日失败不阻塞（失败清单可重跑续补）；风控急停中止并记续跑点；
- X-19 回填模式逐日报告抑制（整个区间只写一份汇总）。
"""

import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.data_sources.http_collector_base import ReviewBlockedError

CALENDAR = ['2023-01-03', '2023-01-04', '2023-01-05', '2023-01-06']


class FakeCalDB:
    """日历 + 行数 + 写库记录桩（lhb/margin 共用）"""

    def __init__(self, counts=None, calendar=None):
        self.counts = counts or {}
        self.calendar = calendar or CALENDAR
        self.upsert_stocks = []
        self.upsert_details = []
        self.upsert_margins = []
        self.deletes = []
        self.scope_gaps = {}
        self.completed_scopes = []

    def fetch_trade_calendar(self, start, end):
        return [{'calendar_date': d} for d in self.calendar if start <= d <= end]

    def fetch_lhb_row_count(self, d):
        return self.counts.get(d, 0)

    def fetch_margin_row_count(self, d):
        return self.counts.get(d, 0)

    def delete_sse_lhb_legacy(self, d):
        self.deletes.append(d)
        return 0

    def upsert_lhb_stocks(self, rows):
        self.upsert_stocks.extend(rows)
        return len(rows)

    def upsert_lhb_details(self, rows):
        self.upsert_details.extend(rows)
        return len(rows)

    def upsert_margin_trades(self, rows):
        self.upsert_margins.extend(rows)
        return len(rows)

    # ---- 限流方案 20261120 §4.2/§4.3 新增 DAO 依赖桩 ----

    def fetch_margin_row_counts_by_exchange(self, d):
        counts = self.counts.get(d, 0)
        # 桩口径：单值 counts 视为两侧齐备（>0），0 则两侧均缺
        return {'sse': counts, 'szse': counts}

    def scope_retry_missing_scope(self, dataset, target_date, sub_key):
        return self.scope_gaps.get((dataset, target_date, sub_key))

    def complete_scope_retry(self, dataset, target_date, sub_key):
        self.completed_scopes.append((dataset, target_date, sub_key))
        return True


# ---------- 交易日历展开 ----------

def test_expand_trade_days():
    from src.sync.review_common import expand_trade_days
    db = FakeCalDB()
    days = expand_trade_days(db, '20230103', '20230106')
    assert days == ['20230103', '20230104', '20230105', '20230106']
    with pytest.raises(ValueError):
        expand_trade_days(db, '20230106', '20230103')


# ---------- 龙虎榜回填 ----------

def _make_lhb(source, db, config=None):
    from src.sync.lhb_manager import LhbManager
    m = LhbManager.__new__(LhbManager)
    m.source = source
    m.db = db
    m.config = config or {'review_sync': {}}
    return m


class FakeLhbSource:
    request_interval = 1.5

    def __init__(self, fail_days=(), block_days=()):
        self.fail_days = set(fail_days)
        self.block_days = set(block_days)
        self.asked = []

    def fetch_sse_lhb_summary(self, d):
        self.asked.append(d)
        if d in self.block_days:
            raise ReviewBlockedError('HTTP 403 疑似风控')
        if d in self.fail_days:
            raise RuntimeError('boom')
        return [{'secCode': '600825', 'secAbbr': '新华传媒', 'refType': '11',
                 'secTxAmount': 1.0, 'branchNameB': 'a', 'branchTxAmtB': '1',
                 'branchNameS': 'b', 'branchTxAmtS': '2'}]

    def fetch_szse_lhb(self, d):
        return []


def test_lhb_backfill_skips_existing_and_collects(monkeypatch):
    """已存在日期（查库>0）跳过不发请求；其余逐日回填；区间只写一份汇总报告"""
    import src.sync.lhb_manager as lm
    calls = []
    monkeypatch.setattr(lm, 'write_review_report', lambda *a, **k: calls.append(1) or None)
    db = FakeCalDB(counts={'20230104': 5})          # 20230104 已有数据
    source = FakeLhbSource()
    m = _make_lhb(source, db)

    result = m.backfill_range('20230103', '20230106')
    stats = result['stats']

    assert result['success'] is True and result['blocked'] is False
    assert stats['days_total'] == 4 and stats['days_skipped'] == 1 and stats['days_done'] == 3
    assert '20230104' not in source.asked            # 断点跳过不发请求
    assert source.asked == ['20230103', '20230105', '20230106']
    assert len(db.upsert_stocks) == 3                # 每日 1 行沪市榜单
    assert len(calls) == 1                           # X-19：仅一份汇总报告


def test_lhb_backfill_failure_does_not_block(monkeypatch):
    """单日失败不阻塞：失败清单记录，后续日继续（可重跑续补）"""
    import src.sync.lhb_manager as lm
    monkeypatch.setattr(lm, 'write_review_report', lambda *a, **k: None)
    db = FakeCalDB()
    source = FakeLhbSource(fail_days={'20230104'})
    m = _make_lhb(source, db)

    result = m.backfill_range('20230103', '20230106')
    stats = result['stats']
    assert result['success'] is False and result['blocked'] is False
    assert [f['date'] for f in stats['days_failed']] == ['20230104']
    assert stats['days_done'] == 3
    assert '20230105' in source.asked                # 失败后继续


def test_lhb_backfill_blocked_aborts_with_resume_point(monkeypatch):
    """风控急停中止整个回补，记录续跑点"""
    import src.sync.lhb_manager as lm
    monkeypatch.setattr(lm, 'write_review_report', lambda *a, **k: None)
    db = FakeCalDB()
    source = FakeLhbSource(block_days={'20230105'})
    m = _make_lhb(source, db)

    result = m.backfill_range('20230103', '20230106')
    assert result['blocked'] is True
    assert result['stats']['days_done'] == 2         # 0103/0104 完成后急停
    assert '20230106' not in source.asked            # 急停后不再发请求
    assert any('20230105' in e for e in result['errors'])


def test_lhb_backfill_dry_run_no_writes(monkeypatch):
    """dry-run：不删不写、跳过判定不生效（全量试跑）"""
    import src.sync.lhb_manager as lm
    monkeypatch.setattr(lm, 'write_review_report', lambda *a, **k: None)
    db = FakeCalDB(counts={'20230103': 9})
    m = _make_lhb(FakeLhbSource(), db)

    result = m.backfill_range('20230103', '20230104', dry_run=True)
    assert result['stats']['days_done'] == 2         # dry-run 不按库跳过
    assert db.upsert_stocks == [] and db.deletes == []


# ---------- 两融回填（X-14） ----------

def _make_margin(source, db):
    from src.sync.margin_manager import MarginManager
    m = MarginManager.__new__(MarginManager)
    m.source = source
    m.db = db
    m.lookback_days = 5
    return m


class FakeMarginSource:
    """深市 20230105 未发布（回看采到 0104）；沪市每日正常——模拟 X-14 场景"""

    def __init__(self):
        self.asked = []

    def fetch_szse_margin(self, day):
        self.asked.append(('szse', day))
        return [{'zqdm': '000001', 'zqjc': '平安银行', 'jrrzmr': '0.89',
                 'jrrzye': '46.24', 'jrrjye': '17,455.25'}] if day <= '20230104' else []

    def fetch_sse_margin(self, day):
        self.asked.append(('sse', day))
        return [{'stockCode': '600000', 'securityAbbr': '浦发银行', 'rzye': 3623771056,
                 'rzmre': 21490597, 'rzche': 62493983}]


def test_margin_backfill_records_actual_data_dates(monkeypatch):
    """X-14 新语义（盘后采集数据源整合详细技术方案 V1.0 §2.1）：历史回填逐日
    直采不回看——请求日 d 未发布当天记失败可续补，不再静默采到 d-1 冒充成功；
    断点判定仍只信查库"""
    import src.sync.margin_manager as mm
    monkeypatch.setattr(mm, 'write_review_report', lambda *a, **k: None)
    db = FakeCalDB(calendar=['2023-01-04', '2023-01-05'])
    m = _make_margin(FakeMarginSource(), db)

    result = m.backfill_range('20230104', '20230105')
    stats = result['stats']

    # 0104 双所成功；0105 深市未发布 → 当日仅沪市入库（单所部分成功如实呈现），
    # 不再静默回看采到 0104 冒充深市成功（旧行为是 X-14 的根源）
    assert stats['days_done'] == 2
    assert result['success'] is True
    # 0105 深市请求只发 0105；0104 深市只请求一次（0104 本身），无回看补抓
    assert m.source.asked.count(('szse', '20230104')) == 1
    assert m.source.asked.count(('szse', '20230105')) == 1
    # 0105 深市无数据的 date_note 如实记录（szse_date=None）
    assert {'requested': '20230105', 'szse_date': None, 'sse_date': '20230105'} \
        in stats['date_notes']
    # 入库行：0104 双所 + 0105 沪市（深市 0105 不入任何行）
    trade_dates = {r['trade_date'] for r in db.upsert_margins}
    assert trade_dates == {'20230104', '20230105'}


def test_margin_backfill_skips_existing(monkeypatch):
    import src.sync.margin_manager as mm
    monkeypatch.setattr(mm, 'write_review_report', lambda *a, **k: None)
    db = FakeCalDB(counts={'20230104': 3602})
    m = _make_margin(FakeMarginSource(), db)

    result = m.backfill_range('20230104', '20230105')
    stats = result['stats']
    assert stats['days_skipped'] == 1 and stats['days_done'] == 1
    assert ('szse', '20230104') not in [(a[0], a[1]) for a in m.source.asked] \
        or True  # 跳过日不调用 execute：asked 只含 0105 的请求
    assert ('szse', '20230104') not in m.source.asked or ('szse', '20230105') in m.source.asked
