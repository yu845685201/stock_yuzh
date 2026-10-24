"""缠论结构引擎单元测试（Phase 1）。

合成K线构造约定：``zigzag`` 生成整数收盘价折线（相邻差 1），high/low = 收盘
±0.4 —— 相邻K永无包含，合并K = 原始K，便于精确推理；包含场景单独手工构造。
"""

from __future__ import annotations

import numpy as np
import pytest

from src.chan.structure import BOTTOM, TOP, ChanEngine, analyze


# ---------------------------------------------------------------- helpers
def zigzag(waypoints):
    """整数路径 → (high, low, closes)。相邻收盘差恰 1，无包含K。"""
    closes = [waypoints[0]]
    for a, b in zip(waypoints, waypoints[1:]):
        step = 1 if b > a else -1
        for c in range(a + step, b + step, step):
            closes.append(c)
    return (
        np.array([c + 0.4 for c in closes]),
        np.array([c - 0.4 for c in closes]),
        closes,
    )


def bars(closes):
    """任意收盘序列 → (high, low)。等宽 ±0.4 窗口、相邻收盘不等 → 无包含K。"""
    assert all(abs(b - a) >= 0.5 for a, b in zip(closes, closes[1:])), "相邻收盘差过小会引入包含"
    return np.array([c + 0.4 for c in closes]), np.array([c - 0.4 for c in closes])


def kinds(events):
    return [e.kind for e in events]


def events_of(events, kind):
    return [e for e in events if e.kind == kind]


# ---------------------------------------------------------------- 包含处理
def test_inclusion_merge_up_direction():
    # 上升途中出现内含K（高低都被前K包含）→ 合并取高高
    high = np.array([10.0, 9.5, 11.0])
    low = np.array([8.0, 8.5, 9.0])
    eng = ChanEngine()
    eng.run(high, low)
    # bar1 被 bar0 包含，方向上 → merged = (10, 8.5)；bar2 新K
    assert [(b.high, b.low) for b in eng.bars] == [(10.0, 8.5), (11.0, 9.0)]
    assert eng.bars[0].last_i == 1  # 合并K确认时刻 = 最后成员原始K索引


def test_inclusion_merge_down_direction():
    # 先跌出方向，再出现包含K → 合并取低低
    high = np.array([10.0, 8.0, 7.5])
    low = np.array([9.0, 7.0, 7.5])
    eng = ChanEngine()
    eng.run(high, low)
    # bar1 跌破 bar0（方向下）；bar2 被 bar1 包含（7.5<=8, 7.5>=7）→ merged=(7.5, 7)
    assert [(b.high, b.low) for b in eng.bars] == [(10.0, 9.0), (7.5, 7.0)]


# ---------------------------------------------------------------- 分型
def test_fractal_kind_and_price():
    high, low, _ = zigzag([10, 16, 10])
    r = analyze(high, low)
    # 仅中间路径的分型可确认：首尾无左/右邻
    assert [(f.kind, f.price) for f in r.fractals] == [(TOP, 16.4)]


def test_fractal_confirm_timing():
    # 无包含时：分型在第 4 根合并K诞生时确认 → confirm_i = 中间K索引 + 2
    high, low, closes = zigzag([10, 16, 10, 16, 10])
    r = analyze(high, low)
    mids = [
        i for i in range(1, len(closes) - 1)
        if (closes[i] > closes[i - 1] and closes[i] > closes[i + 1])
        or (closes[i] < closes[i - 1] and closes[i] < closes[i + 1])
    ]
    assert len(r.fractals) == len(mids) == 3
    for f, m in zip(r.fractals, mids):
        assert f.confirm_i == m + 2


# ---------------------------------------------------------------- 笔
def test_stroke_alternation_and_endpoints():
    high, low, closes = zigzag([10, 16, 10, 16, 10, 16])
    r = analyze(high, low)
    alive = [s for s in r.strokes if s.alive]
    assert len(alive) == 3
    assert [s.direction for s in alive] == [-1, 1, -1]  # 首分型是顶 → 首笔向下
    assert alive[0].end.price == pytest.approx(9.6)      # 底分型价 = 中间K low
    assert alive[1].end.price == pytest.approx(16.4)


def test_stroke_distance_too_short_rejected():
    # 相邻分型中间K间距 3（< 4）且无更远反向分型 → 不成笔
    high, low, _ = zigzag([10, 13, 10, 13, 10])
    r = analyze(high, low)
    assert not any(s.alive for s in r.strokes)


def test_stroke_skips_unreachable_fractals():
    # 距离不足的分型被丢弃后不阻塞后续配对：top@3 与 bottom@12 间距 9 → 成笔
    # （与 czsc 一致：中间同级/更浅的分型不参与结构）
    high, low, _ = zigzag([10, 13, 10, 13, 10, 13, 10])
    r = analyze(high, low)
    alive = [s for s in r.strokes if s.alive]
    assert len(alive) == 1
    assert alive[0].direction == -1
    assert alive[0].start.mid_seq == 3
    assert alive[0].end.mid_seq == 12


def test_stroke_distance_boundary_valid():
    # 间距恰 4 → 成笔（老笔：中间至少 5 根合并K）
    high, low, _ = zigzag([10, 14, 10, 14, 10, 14])
    r = analyze(high, low)
    alive = [s for s in r.strokes if s.alive]
    assert len(alive) == 3


def test_stroke_extension():
    # s0 下笔 16→10；s1 上笔 10→18；回撤 15 距离不足不成笔；新高 22 → s1 延伸到 22
    high, low, _ = zigzag([8, 16, 10, 18, 15, 22, 15, 17])
    r = analyze(high, low)
    alive = [s for s in r.strokes if s.alive]
    assert len(alive) == 3
    s1 = alive[1]
    assert s1.direction == 1
    assert s1.end.price == pytest.approx(22.4)
    assert len(s1.end_history) == 1
    assert "stroke_extended" in kinds(r.events)


def test_stroke_destroy_first():
    # 首笔 = 下笔 16→9；随后 3 格内 V 型冲 17（越过该笔起点 16.4）→ 首笔破坏，
    # 候选起点换为顶@17；其后 17→11 成新下笔
    closes = [10, 11, 12, 13, 14, 15, 16, 15, 14, 13, 12, 11, 10, 9,
              10, 13, 17, 16, 15, 14, 13, 12, 11, 12, 13]
    high, low = bars(closes)
    r = analyze(high, low)
    dead = [s for s in r.strokes if not s.alive]
    alive = [s for s in r.strokes if s.alive]
    assert len(dead) == 1
    assert dead[0].direction == -1 and dead[0].destroyed_i is not None
    assert len(alive) == 1
    assert alive[0].direction == -1
    assert alive[0].start.price == pytest.approx(17.4)
    assert alive[0].end.price == pytest.approx(10.6)
    assert "stroke_destroyed" in kinds(r.events)


def test_stroke_destroy_middle():
    # s0 下 16→10，s1 上 10→14；随后 3 格内跌破 10（越过 s1 起点 9.6）→ s1 破坏，s0 延伸到新低
    closes = [10, 11, 12, 13, 14, 15, 16, 15, 14, 13, 12, 11, 10,
              11, 12, 13, 14, 13, 10, 8, 9, 10]
    high, low = bars(closes)
    r = analyze(high, low)
    dead = [s for s in r.strokes if not s.alive]
    alive = [s for s in r.strokes if s.alive]
    assert len(dead) == 1 and dead[0].direction == 1
    assert len(alive) == 1
    assert alive[0].direction == -1
    assert alive[0].end.price == pytest.approx(7.6)   # s0 延伸到 8-0.4
    assert len(alive[0].end_history) == 1


# ---------------------------------------------------------------- 中枢
def test_pivot_form_extend_complete():
    # 锯齿 [10,16]×3 → 中枢(zg=16.4, zd=9.6)；随后 10→22→18：
    # 上笔(起点在中枢内)延伸，回踩笔整体高于 ZG → 中枢完成、向上离开
    high, low, _ = zigzag([10, 16, 10, 16, 10, 16, 10, 22, 18, 24])
    r = analyze(high, low)
    assert len(r.pivots) >= 1
    p = r.pivots[0]
    assert p.zg == pytest.approx(16.4)
    assert p.zd == pytest.approx(9.6)
    assert p.completed_i is not None
    assert p.exit_dir == 1
    assert "pivot_formed" in kinds(r.events)
    assert "pivot_extended" in kinds(r.events)
    assert "pivot_completed" in kinds(r.events)
    # 离开笔（回踩笔）整体在 ZG 上方
    leaving = [s for s in r.strokes if s.seq == p.leaving_stroke_seq][0]
    assert leaving.low > p.zg


def test_pivot_overflow_cap():
    # 长锯齿：中枢延伸至上限 9 笔后溢出强制完成
    high, low, _ = zigzag([10, 16] + [10, 16] * 6)
    r = analyze(high, low)
    p = r.pivots[0]
    assert p.n_strokes == 9
    assert p.exit_dir == 0
    assert "pivot_overflow" in kinds(r.events)


def test_pivot_reopen_on_leaving_stroke_destroy():
    # 中枢向上完成后，回踩笔被 V 型新高破坏 → 中枢重开
    high, low, _ = zigzag([10, 16, 10, 16, 10, 16, 10, 22, 18])
    # 在 18 之后接 19,21,23,22,21（3 格到 23 顶，第 4、5 根下跌确认分型并破坏回踩笔）
    extra = [19, 21, 23, 22, 21]
    high = np.append(high, [c + 0.4 for c in extra])
    low = np.append(low, [c - 0.4 for c in extra])
    r = analyze(high, low)
    p = r.pivots[0]
    assert "pivot_reopen" in kinds(r.events) or "pivot_reopened" in kinds(r.events), kinds(r.events)
    assert p.completed_i is None  # 回到进行态
    assert p.exit_dir is None
    # 破坏后前笔(上笔)延伸到 23.4
    alive = [s for s in r.strokes if s.alive]
    assert alive[-1].direction == 1
    assert alive[-1].end.price == pytest.approx(23.4)
    assert len(alive[-1].end_history) == 1


# ---------------------------------------------------------------- 因果性
def _causality_check(high, low, cuts):
    full = analyze(high, low)
    for k in cuts:
        part = analyze(high[:k], low[:k])
        expect = [e for e in full.events if e.i <= k - 1]
        got = part.events
        assert len(got) == len(expect), f"k={k}: {len(got)} vs {len(expect)}"
        for a, b in zip(expect, got):
            assert (a.i, a.kind, a.snap) == (b.i, b.kind, b.snap), f"k={k} @ {a.snap}"


def test_causality_synthetic():
    rng = np.random.default_rng(42)
    n = 800
    steps = rng.choice([-2, -1, 1, 2], size=n, p=[0.2, 0.3, 0.3, 0.2])
    close = 100 + np.cumsum(steps)
    high = close + rng.uniform(0.3, 0.9, n)
    low = close - rng.uniform(0.3, 0.9, n)
    # 注入包含K：随机拉高拉低收敛
    for _ in range(60):
        i = rng.integers(1, n)
        high[i] = (high[i - 1] + high[i]) / 2 + 0.05
        low[i] = (low[i - 1] + low[i]) / 2 - 0.05
    _causality_check(high, low, cuts=[37, 120, 251, 400, 599, 799])


def test_causality_structured_patterns():
    # 各类形态拼接后仍满足因果前缀
    h1, l1, _ = zigzag([10, 16, 10, 16, 10, 16, 10, 22, 18, 14, 20])
    h2, l2, _ = zigzag([20, 14, 20, 13, 25, 12, 26, 15, 24, 13, 28])
    high = np.concatenate([h1, h2])
    low = np.concatenate([l1, l2])
    _causality_check(high, low, cuts=[11, 33, 55, 77, len(high) - 1])


# ---------------------------------------------------------------- 真实数据
DB = dict(host="127.0.0.1", port=5432, user="postgres", password="yuzh1234", dbname="stock_analysis_uat")


def _load_real(ts_code, limit=1800):
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(**DB)
    except Exception:
        pytest.skip("PostgreSQL 不可用")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT high, low FROM his_kline_day WHERE ts_code=%s AND trade_date>='2015-01-01' "
                "ORDER BY trade_date LIMIT %s",
                (ts_code, limit),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    return np.array([r[0] for r in rows], dtype=float), np.array([r[1] for r in rows], dtype=float)


@pytest.mark.parametrize("ts_code", ["sz.000001", "sh.600000", "sz.300750"])
def test_realdata_invariants_and_causality(ts_code):
    high, low = _load_real(ts_code)
    assert len(high) > 500
    r = analyze(high, low)
    # 不变量
    alive = [s for s in r.strokes if s.alive]
    for a, b in zip(alive, alive[1:]):
        assert a.direction == -b.direction
        assert b.end.mid_seq > a.end.mid_seq
    for p in r.pivots:
        assert p.zg > p.zd
    # 因果性（随机 4 个截断点）
    rng = np.random.default_rng(7)
    cuts = sorted(rng.integers(60, len(high), size=4).tolist() + [len(high)])
    _causality_check(high, low, cuts)
