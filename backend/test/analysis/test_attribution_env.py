"""公告窗口 env 装配单测（P1-11 §3.4）：自然日窗口边界、降序、空窗口、SQL 分发。

免数据库：stub reader 按 SQL 内容分派 announcement_daily 查询。
"""

from src.analysis.env_assembly import _announcement_map


class _StubReader:
    def __init__(self, rows):
        self.rows = rows
        self.captured = None

    def _query(self, sql, params=None):
        if 'announcement_daily' not in sql:
            raise AssertionError(f'未预期的查询: {sql[:60]}')
        self.captured = params
        return self.rows


def test_window_natural_days():
    """3 自然日窗口：0928~0930（公告按自然日发布，含周末也按自然日算）。"""
    reader = _StubReader([
        ('sh.600000', 'A3', '20260930', '其他', '关于中标的公告'),
        ('sh.600000', 'A2', '20260929', '回购', '回购公告'),
        ('sh.600000', 'A1', '20260928', '解禁', '限售股上市流通'),
        ('sh.600001', 'B1', '20260930', '重大事项', '重大事项公告'),
    ])
    out = _announcement_map(reader, '20260930', 3)
    assert reader.captured == ('20260928', '20260930')
    assert len(out) == 2
    assert [a['ann_id'] for a in out['sh.600000']] == ['A3', 'A2', 'A1']  # 降序

    # 自定义 5 日 → 0926 起
    reader2 = _StubReader([])
    _announcement_map(reader2, '20260930', 5)
    assert reader2.captured == ('20260926', '20260930')


def test_empty_window():
    out = _announcement_map(_StubReader([]), '20260930', 3)
    assert out == {}


def test_window_includes_anchor():
    """窗口含锚定日当日（BETWEEN start AND as_of，闭区间）。"""
    reader = _StubReader([('sh.600000', 'A1', '20260930', '其他', '公告')])
    out = _announcement_map(reader, '20260930', 1)
    assert reader.captured == ('20260930', '20260930')
    assert out['sh.600000'][0]['ann_id'] == 'A1'
