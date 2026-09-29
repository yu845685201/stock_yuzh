"""解禁日历 manager 测试（标题过滤、PDF 正文解析、类型归类、执行漏斗与游标）"""

import sys
from datetime import date
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.data_sources.cninfo_source import CninfoBlockedError
from src.sync import unlock_manager as unlock_module
from src.sync.unlock_manager import UnlockManager

CONFIG = {
    'review_sync': {
        'cninfo_interval': 0,
        'unlock': {'enabled': True, 'searchkeys': ['k1', 'k2'],
                   'pdf_extract_max_pages': 3, 'days_back': 3},
    },
    'cninfo': {'query_interval': 0, 'download_interval': 0},
}


class FakeConfigManager:
    def __init__(self, config):
        self._config = config

    def load_config(self):
        return self._config

    def get(self, key, default=None):
        return default

    def get_database_config(self):
        return {}


class FakeDB:
    def __init__(self, cursor=None):
        self._cursor = cursor
        self.unlock_rows = []
        self.cursor_set = None

    def ensure_review_tables(self):
        pass

    def get_cursor(self, source, key):
        return self._cursor

    def set_cursor(self, source, key, value):
        self.cursor_set = value

    def upsert_share_unlocks(self, rows):
        self.unlock_rows.extend(rows)


class FakeCninfo:
    def __init__(self, windows=None, blocked=False):
        # windows: {searchkey: [announcement]}
        self._windows = windows or {}
        self._blocked = blocked
        self.queries = []

    def query_market_window(self, start, end, searchkey=''):
        if self._blocked:
            raise CninfoBlockedError('HTTP 403，疑似触发风控')
        self.queries.append(searchkey)
        return list(self._windows.get(searchkey, []))

    def download_pdf(self, adjunct_url, target_path, overwrite=False):
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_bytes(b'%PDF-1.4 fake')
        return {'status': 'downloaded', 'size': 100, 'error': None}


def _ann(aid, title, sec_code='000001', adjunct='finalpage/2026-09-28/x.PDF'):
    return {'announcementId': aid, 'announcementTitle': title, 'secCode': sec_code,
            'secName': '平安银行', 'adjunctUrl': adjunct, 'announcementTime': 1790524800000}


# ---------- 标题过滤 ----------

def test_clean_title_strips_highlight_tags():
    """2026-09-28 实测：searchkey 检索的标题带 <em> 高亮，须还原后再做词表匹配"""
    raw = '关于广州地铁设计研究院部分<em>限</em><em>售</em><em>股</em><em>上</em><em>市</em><em>流</em><em>通</em>的公告'
    assert UnlockManager.clean_title(raw) == '关于广州地铁设计研究院部分限售股上市流通的公告'


def test_keep_title_include_exclude(monkeypatch):
    monkeypatch.setattr(unlock_module, 'DatabaseConnection', lambda cm: FakeDB())
    manager = UnlockManager(FakeConfigManager(CONFIG))
    assert manager.keep_title('关于部分限售股份上市流通的公告') is True
    assert manager.keep_title('关于解除限售提示性公告') is True
    assert manager.keep_title('华泰联合证券关于XX公司限售股上市流通之核查意见') is False
    assert manager.keep_title('限售股份上市流通结果公告') is False
    assert manager.keep_title('上市流通公告（更正后）') is False
    assert manager.keep_title('') is False


def test_classify_unlock_type():
    assert UnlockManager.classify_unlock_type('关于首发原股东限售股份上市流通的公告') == '首发限售'
    assert UnlockManager.classify_unlock_type('非公开发行限售股上市流通公告') == '定向增发限售'
    assert UnlockManager.classify_unlock_type('股权激励限售股上市流通公告') == '股权激励限售'
    assert UnlockManager.classify_unlock_type('发行股份购买资产部分限售股上市流通公告') == '重组限售'
    assert UnlockManager.classify_unlock_type('限售股份上市流通公告') == '限售股上市流通'


# ---------- PDF 正文解析 ----------

def test_parse_text_standard():
    text = ('平安银行股份有限公司关于部分限售股份上市流通的公告\n'
            '公告日期：2026年9月25日\n'
            '本次解除限售的股份上市流通日为2026年10月9日。\n'
            '解除限售股份数量为123,456,789股，占总股本比例5.1234%。')
    parsed = UnlockManager.parse_unlock_text(text)
    assert parsed['unlock_date'] == date(2026, 10, 9)
    assert parsed['unlock_shares'] == 123456789.0
    assert parsed['unlock_ratio'] == pytest_approx(5.1234)


def pytest_approx(v):
    import pytest
    return pytest.approx(v)


def test_parse_text_wan_unit():
    text = '上市流通日期为2026年10月15日；上市流通数量为2,000万股；占总股本比例为1.25%。'
    parsed = UnlockManager.parse_unlock_text(text)
    assert parsed['unlock_date'] == date(2026, 10, 15)
    assert parsed['unlock_shares'] == 2000 * 10000
    assert parsed['unlock_ratio'] == pytest_approx(1.25)


def test_parse_text_ignores_disclosure_date():
    """关键防错：公告披露日在前、上市流通日在后，必须取后者"""
    text = ('公告披露日期为2026年9月25日。'
            '本次限售股份上市流通日为2026年10月9日。')
    parsed = UnlockManager.parse_unlock_text(text)
    assert parsed['unlock_date'] == date(2026, 10, 9)


def test_parse_text_relaxed_fallback():
    """无标准关键词时，从'上市流通'出现处取日期"""
    text = '本次限售股份上市流通安排如下：自2026年10月20日起上市流通，共100,000股。'
    parsed = UnlockManager.parse_unlock_text(text)
    assert parsed['unlock_date'] == date(2026, 10, 20)
    assert parsed['unlock_shares'] is None  # 数量词表未命中 -> 如实留空


def test_parse_text_skips_historical_dates():
    """公告背景段落引用的历史批次解禁日必须弃用（not_before=发布日）"""
    text = ('公司此前一批限售股份上市流通日为2018年9月28日。'
            '本次解除限售股份上市流通日为2026年10月13日。')
    assert UnlockManager.parse_unlock_text(text, not_before=date(2026, 9, 28))['unlock_date'] \
        == date(2026, 10, 13)
    # 无下界时保持旧行为：取首个候选（历史行为兼容）
    assert UnlockManager.parse_unlock_text(text)['unlock_date'] == date(2018, 9, 28)
    # 全部候选都早于发布日 -> None（调用方计 parse_failed）
    assert UnlockManager.parse_unlock_text(text, not_before=date(2027, 1, 1))['unlock_date'] is None


def test_parse_text_scanned_pdf_returns_none():
    parsed = UnlockManager.parse_unlock_text('')
    assert parsed['unlock_date'] is None
    no_date = UnlockManager.parse_unlock_text('本公司股东减持计划公告 12345')
    assert no_date['unlock_date'] is None


# ---------- 执行漏斗 ----------

def _build(manager_windows, cursor=None, monkeypatch=None):
    db = FakeDB(cursor)
    monkeypatch.setattr(unlock_module, 'DatabaseConnection', lambda cm: db)
    manager = UnlockManager(FakeConfigManager(CONFIG))
    manager.source = FakeCninfo(manager_windows)
    return manager, db


def test_execute_dedupes_and_parses(monkeypatch):
    windows = {
        'k1': [_ann('a1', '关于部分<em>限</em><em>售</em><em>股</em><em>上</em><em>市</em>'
                         '<em>流</em><em>通</em>的公告'),
               _ann('a2', '华泰联合关于XX限售股上市流通之核查意见')],
        'k2': [_ann('a1', '关于部分限售股份上市流通的公告'),   # 跨 searchkey 去重
               _ann('a3', '关于解除限售的提示性公告', sec_code='600000')],
    }
    monkeypatch.setattr(UnlockManager, '_extract_pdf_text',
                        staticmethod(lambda path, n:
                                     '上市流通日为2026年10月9日，解除限售股份数量为100万股，'
                                     '占总股本比例1.00%。'))
    manager, db = _build(windows, monkeypatch=monkeypatch)
    result = manager.execute(days_back=5)

    assert result['success'] is True
    stats = result['stats']
    assert stats['deduped'] == 3          # a1/a2/a3
    assert stats['kept'] == 2             # a2 为核查意见被过滤
    assert stats['rows'] == 2
    assert len(db.unlock_rows) == 2
    row0 = db.unlock_rows[0]
    assert row0['ts_code'] == 'sz.000001'
    assert row0['unlock_date'] == '20261009'
    assert row0['unlock_shares'] == 100 * 10000
    assert db.cursor_set is not None      # 窗口成功即推进游标


def test_execute_no_rows_still_advances_cursor(monkeypatch):
    """窗口内确实无解禁公告：成功且推进游标（幂等重跑安全）"""
    manager, db = _build({'k1': [], 'k2': []}, monkeypatch=monkeypatch)
    result = manager.execute()
    assert result['success'] is True
    assert result['stats']['rows'] == 0
    assert db.cursor_set is not None


def test_execute_blocked_no_cursor(monkeypatch):
    manager, db = _build({}, monkeypatch=monkeypatch)
    manager.source = FakeCninfo(blocked=True)
    result = manager.execute()
    assert result['blocked'] is True
    assert result['success'] is False
    assert db.cursor_set is None


def test_execute_parse_failure_recorded(monkeypatch):
    """扫描件无文本层 -> parse_failed 如实记录，不入库"""
    windows = {'k1': [_ann('a1', '关于部分限售股份上市流通的公告')], 'k2': []}
    monkeypatch.setattr(UnlockManager, '_extract_pdf_text', staticmethod(lambda path, n: ''))
    manager, db = _build(windows, monkeypatch=monkeypatch)
    result = manager.execute()
    assert result['success'] is True
    assert result['stats']['rows'] == 0
    assert len(result['stats']['parse_failed']) == 1
    assert db.unlock_rows == []
