"""THS 源测试（方案 4.3：flash 结构解析、中文数字解析、风控特征识别）"""

import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.data_sources.ths_source import ThsSource, parse_ths_number


def make_flash():
    """构造与实测接口同构的 flash（title/report/report_yoy 行列对齐）

    注意：report_yoy[i] 与 title[i] 同序（每行是该指标的同比数值行），
    实测接口中每行都对齐到各自指标下标。
    """
    return {
        'title': [
            ['科目\\时间'],
            ['净利润', '元'], ['净利润同比增长率', ''],
            ['营业总收入', '元'], ['营业总收入同比增长率', ''],
            ['基本每股收益', '元'], ['每股净资产', '元'],
        ],
        'report': [
            ['2026-06-30', '2026-03-31', '2025-12-31'],
            ['256.96亿', '145.23亿', '426.33亿'],       # 净利润（累计）
            ['3.32%', '3.03%', '-4.21%'],               # 净利同比（report 里也有格式化行）
            ['706.17亿', '352.77亿', '1314.42亿'],      # 营收
            ['1.78%', '4.65%', '-10.40%'],
            ['1.2400', '0.6700', '2.0700'],             # EPS
            ['22.51', '22.30', '21.98'],                # 每股净资产
        ],
        'report_yoy': [
            ['2026-06-30', '2026-03-31', '2025-12-31'],
            [None, None, None],
            [3.32127061, 3.02922815, -4.2127258],       # 净利同比数值行（title 下标 2）
            [None, None, None],
            [1.78, 4.65, -10.40],                       # 营收同比数值行（title 下标 4）
            [None, None, None],
            [None, None, None],
        ],
    }


def test_parse_ths_number():
    assert parse_ths_number('256.96亿') == pytest.approx(2.5696e10)
    assert parse_ths_number('3.32%') == pytest.approx(3.32)
    assert parse_ths_number('1.2400') == pytest.approx(1.24)
    assert parse_ths_number('1200万') == pytest.approx(1.2e7)
    assert parse_ths_number('--') is None
    assert parse_ths_number(None) is None
    assert parse_ths_number('') is None


def test_parse_finance_flash_rows():
    source = ThsSource.__new__(ThsSource)  # 不发起请求
    rows = source.parse_finance_flash(make_flash(), total_share=None)
    assert len(rows) == 3
    latest = rows[0]
    assert latest['stat_date'] == '20260630'
    assert latest['net_profit'] == pytest.approx(2.5696e10)
    assert latest['revenue'] == pytest.approx(7.0617e10)
    assert latest['eps'] == pytest.approx(1.24)
    # 同比优先取 report_yoy 数值行
    assert latest['net_profit_yoy'] == pytest.approx(3.32127061)
    assert latest['revenue_yoy'] == pytest.approx(1.78)
    # 每股类指标无总股本不硬造绝对值
    assert latest['net_assets'] is None


def test_parse_finance_flash_with_share():
    source = ThsSource.__new__(ThsSource)
    rows = source.parse_finance_flash(make_flash(), total_share=2e9)
    # 每股净资产 22.51 × 20亿股 = 450.2 亿
    assert rows[0]['net_assets'] == pytest.approx(22.51 * 2e9)


def test_parse_finance_flash_backfill_years():
    source = ThsSource.__new__(ThsSource)
    rows = source.parse_finance_flash(make_flash(), backfill_years=1)
    # 只保留近 1 年（2026-1=2025，含 2025-12-31 年报）
    assert [r['stat_date'] for r in rows] == ['20260630', '20260331', '20251231']


def test_risk_control_captcha_marks():
    source = ThsSource({'review_sync': {'ths_interval': 0}})

    class FakeResp:
        content = '请输入验证码'.encode('utf-8')
        encoding = 'utf-8'

    with pytest.raises(Exception) as exc_info:
        source._check_risk_control(FakeResp())
    assert '验证码' in str(exc_info.value)


def test_blocked_error_cooldown():
    from src.data_sources.ths_source import ThsBlockedError
    err = ThsBlockedError('测试')
    assert err.cooldown_seconds == 900


# ---------- N3：申万二级（公司概况页，延后项补齐方案 2026-10-04） ----------

_SW_SNIPPET = ('<td><strong class="hltip fl">所属申万行业：</strong>'
               '<span>银行 — 股份制银行Ⅱ</span></td>')


def test_fetch_sw_industry_parse(monkeypatch):
    """实测 4 样本统一格式：'所属申万行业：</strong><span>{一级} — {二级}</span>'（GBK 页面）"""
    source = ThsSource({'review_sync': {'ths_interval': 0}})
    monkeypatch.setattr(source, 'get_content', lambda url: _SW_SNIPPET.encode('gbk'))
    assert source.fetch_sw_industry('600000') == ('银行', '股份制银行Ⅱ')


def test_fetch_sw_industry_dash_space_variant(monkeypatch):
    """'—' 两侧空格变体兼容（split 后 strip）"""
    source = ThsSource({'review_sync': {'ths_interval': 0}})
    page = '<strong>所属申万行业：</strong><span>电子—半导体</span>'
    monkeypatch.setattr(source, 'get_content', lambda url: page.encode('gbk'))
    assert source.fetch_sw_industry('688981') == ('电子', '半导体')


def test_fetch_sw_industry_no_field_returns_none(monkeypatch):
    """无申万分类字段页 -> None（调用方如实计缺）"""
    source = ThsSource({'review_sync': {'ths_interval': 0}})
    monkeypatch.setattr(source, 'get_content', lambda url: '<html>概况页无该字段</html>'.encode('gbk'))
    assert source.fetch_sw_industry('920999') is None


class _FakeL2Source:
    """code6 -> (一级, 二级)；920476 无分类返回 None；000002 抛异常；000003 异常版式"""

    def fetch_sw_industry(self, code6):
        if code6 == '600000':
            return '银行', '股份制银行Ⅱ'
        if code6 == '300750':
            return '电力设备', '电池'
        if code6 == '920476':
            return None
        if code6 == '000003':
            return '钢铁', '钢铁'   # 一级名占两段的异常版式
        raise RuntimeError('网络超时')


class _FakeL2DB:
    def __init__(self, mapped):
        self._mapped = mapped
        self.concept_rows = []
        self.map_rows = []

    def ensure_review_tables(self):
        pass

    def fetch_stock_basic(self):
        return [{'ts_code': t} for t in
                ('sz.600000', 'sz.000002', 'sz.300750', 'bj.920476', 'sz.000003')]

    def fetch_concept_active_codes(self, source=None):
        assert source == 'sw_2021_l2_ths'
        return dict(self._mapped)

    def upsert_concepts(self, rows):
        self.concept_rows.extend(rows)
        return len(rows)

    def upsert_concept_maps_explicit(self, rows):
        self.map_rows.extend(rows)
        return len(rows)


def _build_l2_manager(mapped=None):
    from src.sync.ths_industry_l2_manager import ThsIndustryL2Manager
    mgr = ThsIndustryL2Manager.__new__(ThsIndustryL2Manager)
    mgr.enabled = True
    mgr.db = _FakeL2DB(mapped or {})
    mgr.source = _FakeL2Source()
    return mgr


def test_l2_target_codes_incremental_skips_mapped():
    """目标集差集：在册（source=sw_2021_l2_ths）跳过，增量只抓新上市 + 缺失"""
    mgr = _build_l2_manager({'股份制银行Ⅱ': ['sz.600000']})
    targets = mgr._target_codes(init_mode=False, codes=None)
    assert targets == ['sz.000002', 'sz.300750', 'bj.920476', 'sz.000003']
    # --init 全量不剔除
    assert mgr._target_codes(init_mode=True, codes=None) == \
        ['sz.600000', 'sz.000002', 'sz.300750', 'bj.920476', 'sz.000003']
    # 显式 codes 优先
    assert mgr._target_codes(init_mode=False, codes=['sz.600000']) == ['sz.600000']


def test_l2_execute_rows_and_counts():
    """入库行构造：concept_code=「一级—二级」组合码（防与一级 code=名称 冲突，2026-10-04
    事故修复）、concept_name=二级名原文、source=sw_2021_l2_ths、in_date=快照日 yyyyMMdd、
    out_date=NULL；无分类/异常版式计 missing、单股异常计 failed 且不阻塞"""
    mgr = _build_l2_manager()
    result = mgr.execute(init_mode=True)
    stats = result['stats']
    assert result['success'] is True
    assert stats['targets'] == 5 and stats['ok'] == 2
    assert stats['missing'] == ['bj.920476', 'sz.000003']  # 无分类 + 异常版式（钢铁—钢铁）
    assert len(stats['failed']) == 1               # 单股失败不阻塞
    assert stats['rows'] == 2
    assert {r['concept_code'] for r in mgr.db.concept_rows} ==         {'银行—股份制银行Ⅱ', '电力设备—电池'}
    assert {r['concept_name'] for r in mgr.db.concept_rows} == {'股份制银行Ⅱ', '电池'}
    assert all(r['source'] == 'sw_2021_l2_ths' for r in mgr.db.concept_rows)
    bank_map = next(r for r in mgr.db.map_rows if r['ts_code'] == 'sz.600000')
    assert bank_map['concept_code'] == '银行—股份制银行Ⅱ'
    assert bank_map['in_date'] and len(bank_map['in_date']) == 8   # varchar(8) yyyyMMdd
    assert bank_map['out_date'] is None


def test_l2_execute_dry_run_no_writes():
    mgr = _build_l2_manager()
    result = mgr.execute(init_mode=True, dry_run=True)
    assert result['success'] is True
    assert mgr.db.concept_rows == [] and mgr.db.map_rows == []


def test_l2_execute_disabled():
    from src.sync.ths_industry_l2_manager import ThsIndustryL2Manager
    mgr = ThsIndustryL2Manager.__new__(ThsIndustryL2Manager)
    mgr.enabled = False
    result = mgr.execute()
    assert result.get('skipped_disabled') is True
