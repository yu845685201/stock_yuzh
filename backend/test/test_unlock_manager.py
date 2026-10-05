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

    def backfill_unlock_ratios(self):
        return 0


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
    """无标准关键词时，从'上市流通'出现处取日期；V4.1 起 44 字符间隔窗口
    顺带覆盖'上市流通，共 X 股'句式（旧词表漏、V4.1 正确捕获该真实股数）"""
    text = '本次限售股份上市流通安排如下：自2026年10月20日起上市流通，共100,000股。'
    parsed = UnlockManager.parse_unlock_text(text)
    assert parsed['unlock_date'] == date(2026, 10, 20)
    assert parsed['unlock_shares'] == pytest_approx(100000)


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


# ---------- V4/V4.1 新增用例（2026-10-04 探路实测：存量 18 份 + 样本外 45 份） ----------

def test_parse_text_keyword_variants():
    """关键词变体：'解除限售的股份数量' / '归属的股票数量'（冒号夹层）"""
    text = '本次解除限售的股份数量为 41,977,976 股，占公司总股本的 23.2526%。'
    assert UnlockManager.parse_unlock_text(text)['unlock_shares'] == pytest_approx(41977976)
    text2 = '本次归属的股票数量：1,061,000 股。'
    assert UnlockManager.parse_unlock_text(text2)['unlock_shares'] == pytest_approx(1061000)


def test_parse_text_linebreak_truncation():
    """关键词/单位被 PDF 换行从中间截断：'解除限⏎售' 与 '557,071⏎股'"""
    text = '解除限\n售数量为 18,078,510 股。'
    assert UnlockManager.parse_unlock_text(text)['unlock_shares'] == pytest_approx(18078510)
    text2 = '解除限售数量为557,071\n股。'
    assert UnlockManager.parse_unlock_text(text2)['unlock_shares'] == pytest_approx(557071)


def test_parse_text_long_gap():
    """关键词与数字间填充过长（旧 16 字符窗漏，44 字符窗命中）"""
    text = ('本次解除限售数量为控股股东重庆财信房地产开发集团\n'
            '有限公司所持有的46,696,315\n股。')
    assert UnlockManager.parse_unlock_text(text)['unlock_shares'] == pytest_approx(46696315)


def test_parse_text_ratio_nearest_to_shares():
    """占比取与股数位置最近的命中：同文含限售股占比 77.4538% 与解禁占比 23.2526%"""
    text = ('公司限售股占发行后总股本的比例为 77.4538%；'
            '本次解除限售的股份数量为 41,977,976 股，'
            '占公司总股本的 23.2526%。')
    parsed = UnlockManager.parse_unlock_text(text)
    assert parsed['unlock_shares'] == pytest_approx(41977976)
    assert parsed['unlock_ratio'] == pytest_approx(23.2526)


def test_parse_text_bse_tradeable_date():
    """北交所版式：'可交易时间为 X 年 X 月 X 日'（旧词表整行丢失的根因）"""
    text = '本次解除限售股份可交易时间为 2026 年 10 月 9 日。'
    assert UnlockManager.parse_unlock_text(text)['unlock_date'] == date(2026, 10, 9)


def test_parse_text_listing_date_and_filter_history():
    """'上市日期'关键词 + 历史批次日期滤除"""
    text = ('公司此前一批限售股上市流通日为2018年9月28日。'
            '本次流通股上市日期为2026 年9月30 日。')
    parsed = UnlockManager.parse_unlock_text(text, not_before=date(2026, 9, 28))
    assert parsed['unlock_date'] == date(2026, 9, 30)


def test_keep_title_excludes_not_yet_listed(monkeypatch):
    """'归属结果但股票暂不上市流通'属非解禁事件，排除词'暂不'拦截"""
    monkeypatch.setattr(unlock_module, 'DatabaseConnection', lambda cm: FakeDB())
    manager = UnlockManager(FakeConfigManager(CONFIG))
    assert manager.keep_title(
        '关于2024年限制性股票激励计划归属结果但股票暂不上市流通的公告') is False
    assert manager.keep_title('关于解除限售股票上市流通的提示性公告') is True


def test_parse_text_wan_unit_decimal():
    """'共计192.4646万股' 万股换算（含小数）"""
    text = '本次上市流通的限售股共计192.4646万股，占公司总股本的0.48%。'
    parsed = UnlockManager.parse_unlock_text(text)
    assert parsed['unlock_shares'] == pytest_approx(1924646)
    assert parsed['unlock_ratio'] == pytest_approx(0.48)


def test_parse_text_v41_out_of_sample_variants():
    """V4.1 样本外实测补的变体族：总数/解锁/数⏎量/合计为/上市股数"""
    # 科创板首发公告标准模板："本次股票上市流通总数为 X 股"
    assert UnlockManager.parse_unlock_text(
        '本次股票上市流通总数为1,923,024股，占公司股本总数的1.20%。')['unlock_shares'] \
        == pytest_approx(1923024)
    # "数⏎量"两字被换行拆开
    assert UnlockManager.parse_unlock_text(
        '可解除限售的限制性股票数\n量为 100,900 股，占公司总股本的0.0069%。')['unlock_shares'] \
        == pytest_approx(100900)
    # "解锁"动词
    assert UnlockManager.parse_unlock_text(
        '本次解锁的第一类限制性股票数量：31万股，占目前公司总股本的0.0031%。')['unlock_shares'] \
        == pytest_approx(310000)
    # "可上市流通股份的总数"
    assert UnlockManager.parse_unlock_text(
        '2、本次可上市流通股份的总数为 5,886,199 股，占公司总股本1.0978%。')['unlock_shares'] \
        == pytest_approx(5886199)
    # "上市股数为" + 万股小数（日联科技）
    assert UnlockManager.parse_unlock_text(
        '上市股数为\n5,081.5112 万股。')['unlock_shares'] == pytest_approx(50815112)
    # "限制⏎性股票"名词换行 + "合计为"替代数量
    assert UnlockManager.parse_unlock_text(
        '解除限售的限制\n性股票合计为 8,731,507 股，占目前公司总股本的0.67%。')['unlock_shares'] \
        == pytest_approx(8731507)


def test_execute_ratio_strategy_prefers_derived(monkeypatch):
    """ratio 口径统一：股数解析成功时不写 PDF 占比（由推导管道回填）；股数失败时 PDF 占比兜底"""
    windows = {'k1': [_ann('a1', '关于部分限售股份上市流通的公告'),
                      _ann('a2', '关于部分限售股份上市流通的公告', sec_code='600000')],
               'k2': []}
    texts = {'a1': '上市流通日为2026年10月9日，解除限售股份数量为100万股，占总股本比例1.00%。',
             'a2': '上市流通日为2026年10月9日，占总股本比例2.00%。'}

    def fake_extract(path, n):
        return texts[path.stem]

    monkeypatch.setattr(UnlockManager, '_extract_pdf_text', staticmethod(fake_extract))
    manager, db = _build(windows, monkeypatch=monkeypatch)
    result = manager.execute(days_back=5)

    assert result['success'] is True
    assert len(db.unlock_rows) == 2
    by_code = {r['ts_code']: r for r in db.unlock_rows}
    # a1 有股数 -> PDF 占比不写（推导管道统一回填）
    assert by_code['sz.000001']['unlock_ratio'] is None
    assert by_code['sz.000001']['unlock_shares'] == 100 * 10000
    # a2 股数失败 -> PDF 占比兜底保留
    assert by_code['sh.600000']['unlock_ratio'] == pytest_approx(2.00)
    assert by_code['sh.600000']['unlock_shares'] is None


def test_execute_six_page_retry(monkeypatch):
    """日期解析失败时以 max_pages+3 页重解一次（多批次公告解禁日排中后部）"""
    calls = []
    windows = {'k1': [_ann('a1', '关于部分限售股份上市流通的公告')], 'k2': []}

    def fake_extract(path, n):
        calls.append(n)
        if n <= 3:
            return '解禁安排详见后文。'
        return '本次解除限售股份上市流通日为2026年10月13日。'

    monkeypatch.setattr(UnlockManager, '_extract_pdf_text', staticmethod(fake_extract))
    manager, db = _build(windows, monkeypatch=monkeypatch)
    result = manager.execute(days_back=5)

    assert calls == [3, 6]
    assert result['success'] is True
    assert result['stats']['rows'] == 1
    assert db.unlock_rows[0]['unlock_date'] == '20261013'


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
