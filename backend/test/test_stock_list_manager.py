"""股票列表维护单测（baostock 下线换源 20261025）：名称归一化/临时标记/截断判定/行构建"""

import pytest

from src.sync.stock_list_manager import (
    MARKET_META,
    STOCK_CODE_PREFIXES,
    build_stock_row,
    has_temp_marker,
    is_truncated_name,
    normalize_name,
)


def test_normalize_name_fullwidth_and_spaces():
    """全角字母/空格归一化（万科Ａ vs 万科A、万 科Ａ 类行情名差异）"""
    assert normalize_name('万 科Ａ') == '万科A'
    assert normalize_name('ＸＤ浙能电') == 'XD浙能电'


def test_temp_markers():
    assert has_temp_marker('XD浙能电') is True
    assert has_temp_marker('XR强一股') is True
    assert has_temp_marker('DR某股') is True
    assert has_temp_marker('C通则康威') is True     # 上市初期 C 前缀
    assert has_temp_marker('浙能电力') is False
    assert has_temp_marker('*ST三六五') is False    # ST 是真实状态，非临时标记


def test_truncated_name_detection():
    """TDX 名称字段按 4 汉字截断：旧名以新名为前缀 → 截断，保持库内全名"""
    assert is_truncated_name('华润新能源', '华润新能') is True
    assert is_truncated_name('粤芯半导体', '粤芯半导') is True
    assert is_truncated_name('万科A', '万 科Ａ') is False        # 归一化后等长非前缀关系
    assert is_truncated_name('金证股份', '方正科技') is False    # 真改名不是前缀
    assert is_truncated_name('金证股份', '金证股份') is False    # 等长不算截断


def test_build_stock_row_all_markets():
    row = build_stock_row('600519', '贵州茅台', 'sh', '20010827')
    assert row['ts_code'] == 'sh.600519'
    assert row['market_code'] == 'sh'
    assert row['exchange_code'] == 'SSE'
    assert row['list_date'] == '20010827'
    assert row['cnspell']  # 拼音非空

    bj = build_stock_row('833171', '同辉信息', 'bj', None)
    assert bj['ts_code'] == 'bj.833171'
    assert bj['market_name'] == '北京证券交易所'
    assert bj['exchange_code'] == 'BJSE'
    assert bj['list_date'] is None


def test_prefix_whitelist_excludes_preferred_shares():
    """038 深市优先股（如康佳KJP1）不得进入股票池"""
    assert '038043'.startswith(STOCK_CODE_PREFIXES) is False
    for ok in ('000001', '301381', '600519', '688827', '302001', '689009', '833171', '430047'):
        assert ok.startswith(STOCK_CODE_PREFIXES), ok


def test_market_meta_covers_bj():
    assert 'bj' in MARKET_META
