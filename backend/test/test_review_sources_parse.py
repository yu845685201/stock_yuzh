"""新源解析测试（新浪外围市场 / 中证估值文件 / 通用工具与断点）"""

import json
import sys
from pathlib import Path

import pytest

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.data_sources.sina_source import SinaSource
from src.data_sources.csindex_source import CsindexSource
from src.sync.review_common import code6_to_ts_code, load_manifest, save_manifest, ts_code_to_code6


# ---------- 新浪 ----------

def test_sina_parse_index_format():
    """2026-09-27 实测格式：名称,当前,涨跌额,涨跌幅（GBK 已在上层解码）"""
    text = ('var hq_str_int_dji="道琼斯,46247.29,299.97,0.65";\n'
            'var hq_str_int_nasdaq="纳斯达克,22484.07,99.37,0.44";\n')
    parsed = SinaSource._parse_response(text)
    assert parsed['int_dji']['close'] == 46247.29
    assert parsed['int_dji']['change_pct'] == 0.65
    assert parsed['int_dji']['name'] == '道琼斯'
    assert parsed['int_nasdaq']['close'] == 22484.07


def test_sina_parse_fx_format():
    """fx_* 新格式（2026-09 实测 16 字段）：[1]最新价 [15]日期"""
    text = ('var hq_str_fx_susdcny="00:56:01,6.7121000000,6.7141000000,6.7125000000,'
            '92.0000000000,6.7150000000,6.7195000000,6.7103000000,6.7125000000,离岸人民币,'
            '0.0000,0.0000,0.0092,伦敦银行间外汇市场,0.0000,0.0000,,2026-09-25";\n')
    parsed = SinaSource._parse_response(text)
    fx = parsed['fx_susdcny']
    assert fx['family'] == 'fx'
    assert fx['close'] == pytest.approx(6.7121)
    assert fx['date'] == '2026-09-25'
    assert fx.get('change_pct') is None  # 未核实字段不取


def test_sina_parse_empty_and_garbage():
    parsed = SinaSource._parse_response('var hq_str_int_dji="";')
    assert 'int_dji' not in parsed


# ---------- 中证 ----------

def test_csindex_normalize_date():
    assert CsindexSource._normalize_date('2026-09-25 00:00:00') == '20260925'
    assert CsindexSource._normalize_date('20260925') == '20260925'
    assert CsindexSource._normalize_date(None) is None
    assert CsindexSource._normalize_date('bad') is None


def test_csindex_parse_dataframe():
    import pandas as pd
    source = CsindexSource({'review_sync': {'csindex_interval': 0}})
    df = pd.DataFrame([
        {'日期Date': '2026-09-25', '指数代码Index Code': '300', '指数中文简称Index Chinese Name': '沪深300',
         '市盈率1（总股本）P/E1': 14.49, '市盈率2（计算用股本）P/E2': 16.66,
         '股息率1（总股本）D/P1': 2.65, '股息率2（计算用股本）D/P2': 2.36},
        {'日期Date': '2026-09-24', '指数代码Index Code': '300', '指数中文简称Index Chinese Name': '沪深300',
         '市盈率1（总股本）P/E1': 14.66, '市盈率2（计算用股本）P/E2': 16.95,
         '股息率1（总股本）D/P1': 2.62, '股息率2（计算用股本）D/P2': 2.32},
    ])
    rows = source._parse_dataframe(df, '000300')
    assert len(rows) == 2
    assert rows[-1]['trade_date'] == '20260925'  # 升序
    row = source.to_row(rows[-1], '000300', '沪深300')
    assert row['index_code'] == '000300'         # 落库用请求码而非文件短码
    assert row['pe_static'] == 16.66             # 默认 P/E2 口径
    assert row['dividend_yield'] == 2.36
    assert row['pe_ttm'] is None                 # 官方文件无 TTM，如实留空


def test_csindex_parse_structure_change_returns_empty():
    """列结构变更 -> 解析为空 -> 调用方报结构异常（不带臆造数据）"""
    import pandas as pd
    source = CsindexSource({'review_sync': {'csindex_interval': 0}})
    df = pd.DataFrame([{'foo': 1}])
    assert source._parse_dataframe(df, '000300') == []


# ---------- 通用工具 ----------

def test_code_conversions():
    assert code6_to_ts_code('600000') == 'sh.600000'
    assert code6_to_ts_code('688981') == 'sh.688981'
    assert code6_to_ts_code('000001') == 'sz.000001'
    assert code6_to_ts_code('300750') == 'sz.300750'
    assert code6_to_ts_code('899050') == 'bj.899050'
    assert ts_code_to_code6('sz.000001') == '000001'
    assert ts_code_to_code6('000001.SZ') == '000001'
    assert ts_code_to_code6('600000') == '600000'


def test_manifest_roundtrip(tmp_path):
    path = tmp_path / 'm.json'
    manifest = load_manifest(path)
    assert manifest == {'stocks': {}, 'updated_at': None}
    manifest['stocks']['000001'] = {'done': True}
    save_manifest(path, manifest)
    assert load_manifest(path)['stocks']['000001']['done'] is True
    # 原子写不残留 .tmp
    assert not (tmp_path / 'm.tmp').exists()
