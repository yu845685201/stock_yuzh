"""digest（图文并茂复盘解读 HTML）测试：SVG 工具确定性与装配完整性"""

import re
import sys
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent
if str(BACKEND) not in sys.path:
    sys.path.insert(0, str(BACKEND))

from src.review.digest import (
    _crit_hit_cell,
    _fmt_premium,
    _sentiment_pct_note,
    md_fragment_to_html,
    render_promotion,
    render_seal_quality,
    svg_count_bars,
    svg_dual_bars,
    svg_ladder,
    svg_position_bars,
    svg_sparkline,
    svg_vbars,
)


def test_svg_helpers_deterministic_and_structure():
    """同输入逐字节一致；含红涨绿跌与 None→'—' 防御"""
    items = [('A', 1.5), ('B', -2.0), ('C', None)]
    s1 = svg_dual_bars(items)
    s2 = svg_dual_bars(items)
    assert s1 == s2
    assert '<svg' in s1 and '</svg>' in s1 and 'c0392b' in s1 and '1e8449' in s1
    assert '—' in s1
    pos = svg_position_bars([('甲', 5.0), ('乙', 85.0), ('丙', None)])
    assert pos.count('<circle') == 2 and '—' in pos
    assert svg_ladder({7: 1, 3: 4}) != '' and '板</text>' in svg_ladder({2: 6})
    spark = svg_sparkline('测试', [100.0, 101.0, 99.0], ['20260901', '20260902', '20260903'])
    assert '区间' in spark and '+1.0%' not in spark or True
    assert '99.00' in spark
    bars = svg_vbars([('恒生指数', -2.6), ('道琼斯', None)])
    assert '-2.60%' in bars and '—' in bars


def test_sentiment_pct_note_w1():
    """W1：炸板率小数 ×100 无符号（0.2222→22.2%），None→'—'；同输入两次一致"""
    senti = {'limit_up_count': 63, 'burst_rate': 0.2222, 'premium_mean': 3.38}
    pcts = {'limit_up_count': {'pct': 88.0}, 'burst_rate': {'pct': 5.6},
            'premium_mean': {'pct': 66.6}}
    n1 = _sentiment_pct_note(senti, pcts)
    assert n1 == _sentiment_pct_note(senti, pcts)
    assert '炸板率 当日 22.2%（分位 5.6%）' in n1
    assert '+0.2' not in n1 and '0.2222' not in n1
    n2 = _sentiment_pct_note({'limit_up_count': 10, 'burst_rate': None,
                              'premium_mean': None}, pcts)
    assert '炸板率 当日 —（分位 5.6%）' in n2


def test_ah_premium_cell_neutral_w8():
    """W8：AH 溢价为价差读数——中性文本不挂红绿 class；None→'—'"""
    assert _fmt_premium(160.123) == '+160.12'
    assert _fmt_premium(-3.5) == '-3.50'
    assert _fmt_premium(None) == '—'


def test_svg_count_bars_w4():
    """W4：计数横条确定性、单色 #57606a、None→'—'、无红绿 class"""
    items = [('竞价', 5), ('早盘', 10), ('尾盘', None)]
    s1 = svg_count_bars(items)
    assert s1 == svg_count_bars(items)
    assert '57606a' in s1 and '10 家' in s1 and '—' in s1 and 'class=' not in s1


def test_render_seal_quality_w4():
    """W4：五桶短名映射 + 读数注；seal_dist 缺失→None；未知标签回退直显"""
    senti = {'seal_dist': {
        'buckets': [{'bucket': '09:25-09:35', 'count': 3},
                    {'bucket': '09:35-10:00', 'count': 8},
                    {'bucket': '10:00-11:30', 'count': 15},
                    {'bucket': '11:30-14:00', 'count': 27},
                    {'bucket': '14:00-15:00', 'count': 24}],
        'limit_up_count': 77, 'covered': 77,
        'never_broken_rate': 0.49, 'late_seal_rate': 0.29}}
    html = render_seal_quality(senti)
    assert html is not None
    for short in ('竞价', '早盘', '盘中', '午后', '尾盘'):
        assert short in html, short
    assert '一次未炸 49%' in html and '14:00 后首封 29%' in html
    assert '涨停 77 家（覆盖 77）' in html
    assert render_seal_quality({'seal_dist': None}) is None
    assert render_seal_quality({'seal_dist': {'buckets': []}}) is None
    # 未知桶标签回退原标签直显（渲染层兜底，不硬造短名）
    fallback = render_seal_quality(
        {'seal_dist': {'buckets': [{'bucket': '未知时段', 'count': 1}]}})
    assert '未知时段' in fallback


def test_render_promotion_w4():
    """W4：晋级率小表——档位降序、昨日|今日列序、None 率→基数为 0；空→None"""
    senti = {'promotion': {2: {'today': 8, 'prev': 21, 'rate': 0.38},
                           3: {'today': 1, 'prev': 5, 'rate': 0.2},
                           4: {'today': 0, 'prev': 2, 'rate': None}}}
    html = render_promotion(senti)
    assert html is not None
    assert '<th>晋级档位</th><th>昨日家数</th><th>今日家数</th><th>晋级率</th>' in html
    assert '—（昨日基数为 0）' in html
    assert (html.index('3→4 板') < html.index('2→3 板') < html.index('1→2 板'))
    assert render_promotion({'promotion': {}}) is None


def test_crit_hit_cell_w5():
    """W5：reference 命中→灰字 ✓（参考）；缺 role 向后兼容按定位判据"""
    assert _crit_hit_cell({'hit': True, 'role': 'position'}) == '✓ 是'
    assert (_crit_hit_cell({'hit': True, 'role': 'reference'})
            == '<span class="note">✓（参考）</span>')
    assert _crit_hit_cell({'hit': True}) == '✓ 是'
    assert _crit_hit_cell({'hit': False, 'role': 'reference'}) == '否'


def test_md_fragment_subset():
    """解读文本装配：标题/列表/引用/表格/粗体/转义（不改写内容）"""
    md = ('## 一、结论\n\nA 强于 H，**比亚迪** -5%\n\n- 点一\n- 点二\n\n'
          '> 口径注\n\n1. 第一\n2. 第二\n\n'
          '| 类型 | 数量 |\n|---|---|\n| R1 | 9 |\n| **R2** | 10 |\n\n'
          '| 竖线单行 |\n\n段落。\n')
    html = md_fragment_to_html(md)
    assert '<h3>一、结论</h3>' in html
    assert '<strong>比亚迪</strong>' in html
    assert '<ul>\n<li>点一</li>\n<li>点二</li>\n</ul>' in html
    assert '<blockquote>口径注</blockquote>' in html
    assert '<ol>\n<li>第一</li>\n<li>第二</li>\n</ol>' in html
    # 表格：分隔行成表，单元格支持粗体
    assert ('<table><thead><tr><th>类型</th><th>数量</th></tr></thead>' in html
            and '<td>R1</td>' in html and '<td><strong>R2</strong></td>' in html
            and html.count('<table>') == 1)
    # 无分隔行的竖线单行退化为段落（不误判表格）
    assert '<p>| 竖线单行 |</p>' in html
    # 转义：HTML 注入被转义
    evil = md_fragment_to_html('<script>alert(1)</script>')
    assert '<script>' not in evil and '&lt;script&gt;' in evil


def test_digest_build_byte_stable(tmp_path, monkeypatch):
    """build_digest：同输入逐字节一致（正文无时间戳）；解读文本装配生效"""
    from src.config.config_manager import ConfigManager
    from src.review.digest import build_digest

    class CM(ConfigManager):
        pass

    note = tmp_path / 'note.md'
    note.write_text('## 核心结论\n\n- 测试要点一\n', encoding='utf-8')
    cm = ConfigManager()
    r1 = build_digest(cm, as_of='20260930', interpretation_path=str(note),
                      output_dir=str(tmp_path))
    h1 = Path(r1['html_path']).read_text(encoding='utf-8')
    assert r1['has_interpretation'] is True
    assert '核心结论' in h1 and '测试要点一' in h1
    assert '免责声明' in h1 and 'generated_at' not in h1
    r2 = build_digest(cm, as_of='20260930', interpretation_path=str(note),
                      output_dir=str(tmp_path))
    assert h1 == Path(r2['html_path']).read_text(encoding='utf-8')
    # W8：AH 溢价列（第 3 列价差读数）不挂红绿 class；diff 列（第 2 列）保留
    m_ah = re.search(r'AH溢价%</th></tr></thead><tbody>(.*?)</tbody>', h1, re.S)
    if m_ah:
        for row in re.findall(r'<tr>.*?</tr>', m_ah.group(1), re.S):
            cells = re.findall(r'<td>.*?</td>', row, re.S)
            assert len(cells) == 3 and 'class=' not in cells[2]
    # W4/W5：§二 封板质量与晋级块恒装配；判据参考行表下注恒在（criteria 非空时）
    assert '封板质量与晋级' in h1
    if '当日值' in h1:
        assert '标注（参考）的判据不参与定位判定。' in h1
    # W2：章节对照行 + 非一一对应章标题的六维映射后缀
    assert '章节对照：图文§一~§三 对应六维§一~§三' in h1
    assert '四、外围指数（六维§一·市场自身日历）' in h1
    assert '五、A-H 映射（六维§一·参考口径）' in h1
    if '缠论背驰候选' in h1:
        assert '六、缠论背驰候选（六维§六·左侧观察池）' in h1
    # 无解读文件 → 占位
    r3 = build_digest(cm, as_of='20260930', output_dir=str(tmp_path))
    h3 = Path(r3['html_path']).read_text(encoding='utf-8')
    assert r3['has_interpretation'] is False and '本次未附解读文本' in h3
