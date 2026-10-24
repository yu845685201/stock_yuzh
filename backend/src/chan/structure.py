"""缠论结构引擎（实战能力验证项目 Phase 1）。

单只股票日K → 合并K线 → 分型 → 笔 → 中枢，全部事件携带 ``confirmed_i``
（事件可被知道的最早原始K线索引），保证下游回测可做因果性截断测试。

规约（v1，一次定死，变体走参数敏感性）
--------------------------------------
1. **K线包含处理**：相邻合并K存在包含（高低全包或被包）时合并——向上趋势取
   ``max(high), max(low)``，向下取 ``min(high), min(low)``；方向取进入该合并K
   时的趋势方向。合并K的确认时刻 = 其最后一个成员原始K的索引。
2. **分型**：合并后连续三根K，中间K为极值。分型在第 4 根合并K诞生时确认
   （此时前三根全部冻结）；确认时刻 = 第 4 根合并K的首个原始K索引。
3. **笔（老笔）**：顶底分型交替；两分型中间K的合并K序号差 >= 4（中间至少 5 根
   合并K）；向上笔顶点须高于底分型中间K的高点，向下笔镜像。
   末笔允许延伸（同向更极端分型替换端点）；反向分型距离不足但比该笔起点
   更极端时构成笔破坏——撤销末笔、前笔延伸至新分型。
4. **中枢（笔中枢）**：连续三笔重叠区间 ``ZG=min(笔高点)`` / ``ZD=max(笔低点)``
   且 ``ZG > ZD``，区间冻结于首三笔；之后与 ``[ZD, ZG]`` 重叠的笔计入延伸，
   上限 9 笔（第 10 笔起溢出，中枢强制完成，溢出笔归下一候选）；
   无重叠笔 = 离开笔，中枢完成；离开方向按区间关系判定：该笔整体在 ZG 上方为
   向上离开（若该笔为下笔，即"回踩不回中枢"的三买形态）、整体在 ZD 下方为向下
   离开；离开笔同时作为下一中枢的候选首笔。三笔不成重叠则滑动窗口继续尝试。
   笔破坏回滚：离开笔被破坏 → 中枢重开（completed 事件可被 reopened 撤销）；
   进行中枢末成员笔被破坏 → 中枢缩笔，不足 3 笔则中枢撤销。
5. 走势类型（趋势 = 同向两个中枢依次波动）在 signals 层基于中枢序列判定，
   本模块只产出结构事实。

口径说明：日线图上的"笔中枢"对应缠论原文 30 分钟级别走势中枢，是实操通行口径。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

import numpy as np

TOP = "top"
BOTTOM = "bottom"


# ---------------------------------------------------------------- 数据模型
@dataclass
class MergedBar:
    """合并K线（包含处理后）。``seq`` 为合并K序号。"""

    seq: int
    high: float
    low: float
    first_i: int  # 首个成员原始K索引
    last_i: int   # 最后成员原始K索引（该合并K数值的确认时刻）
    dir: int      # 进入方向：+1 上 / -1 下 / 0 初始


@dataclass
class Fractal:
    """分型。``price`` 为中间K极值；确认于第 4 根合并K诞生时。"""

    kind: str  # TOP / BOTTOM
    price: float
    mid_seq: int        # 中间合并K序号
    mid_high: float     # 中间合并K high/low（笔有效性判据用）
    mid_low: float
    mid_first_i: int    # 中间合并K的原始K区间（MACD 窗口/可视化落点用）
    mid_last_i: int
    confirm_i: int      # 确认时的原始K索引
    seq_lo: int         # 分型三根合并K序号 [seq_lo, seq_hi]
    seq_hi: int


@dataclass
class Stroke:
    """笔。生命周期：added → (extended)* → (destroyed)?。"""

    seq: int
    direction: int  # +1 上 / -1 下
    start: Fractal
    end: Fractal
    confirm_i: int  # 建立时刻（= 终点分型确认时刻）
    end_history: List[Tuple[Fractal, int]] = field(default_factory=list)  # 延伸 (新终点, 时刻)
    alive: bool = True
    destroyed_i: Optional[int] = None

    @property
    def high(self) -> float:
        return max(self.start.price, self.end.price)

    @property
    def low(self) -> float:
        return min(self.start.price, self.end.price)


@dataclass
class Pivot:
    """笔中枢。``exit_dir``：+1 向上离开 / -1 向下离开 / 0 溢出完成；None 未完成。"""

    pid: int
    first_stroke_seq: int
    last_stroke_seq: int
    zg: float
    zd: float
    n_strokes: int
    formed_i: int                      # 成立时刻
    completed_i: Optional[int] = None
    exit_dir: Optional[int] = None
    leaving_stroke_seq: Optional[int] = None


@dataclass
class ChanEvent:
    """结构事件流（因果性测试与信号派生的统一入口）。

    ``snap`` 为发射时刻的稳定快照——``ref`` 指向的对象此后可能被延伸/破坏修改，
    快照保证事件流可做逐事件前缀比对（因果性测试）。
    """

    i: int     # 原始K索引（= 事件确认时刻 confirmed_i）
    kind: str  # fractal / stroke_added / stroke_extended / stroke_destroyed /
               # pivot_formed / pivot_extended / pivot_completed / pivot_overflow /
               # pivot_reopened / pivot_reshaped / pivot_cancelled
    ref: object
    snap: str = ""


@dataclass
class ChanResult:
    fractals: List[Fractal]
    strokes: List[Stroke]   # 含被破坏笔（alive=False）
    pivots: List[Pivot]     # 含进行中（completed_i=None）与最终被撤销的除外
    events: List[ChanEvent]
    n_bars: int


# ---------------------------------------------------------------- 引擎
class ChanEngine:
    """单股票增量结构引擎：逐根原始K推进，事件即时携带确认时刻。

    ``strokes`` 为全历史（含死笔）；状态机只用 ``_alive``（存活笔，恒为
    ``strokes`` 的某个后缀）。
    """

    def __init__(self, stroke_min_bars: int = 5, pivot_max_strokes: int = 9):
        self.stroke_gap = stroke_min_bars - 1  # 两分型中间K的最小合并K序号差
        self.pivot_max = pivot_max_strokes
        self.reset()

    def reset(self) -> None:
        self.bars: List[MergedBar] = []
        self.fractals: List[Fractal] = []
        self.strokes: List[Stroke] = []
        self.pivots: List[Pivot] = []
        self.events: List[ChanEvent] = []
        self._alive: List[Stroke] = []
        self._seed: Optional[Fractal] = None  # 首笔建立前的候选起点分型
        self._open: Optional[Pivot] = None
        self._buffer: List[Stroke] = []  # 未成中枢的候选笔（最近若干笔）
        self._n_pivot = 0
        self._n_stroke = 0

    # ------------------------------------------------------------ 主流程
    def run(self, high: np.ndarray, low: np.ndarray) -> ChanResult:
        for i in range(len(high)):
            self._on_bar(i, float(high[i]), float(low[i]))
        return ChanResult(
            fractals=self.fractals,
            strokes=self.strokes,
            pivots=self.pivots,
            events=self.events,
            n_bars=len(high),
        )

    def _emit(self, i: int, kind: str, ref) -> None:
        self.events.append(ChanEvent(i=i, kind=kind, ref=ref, snap=_snapshot(kind, ref)))

    # ------------------------------------------------------------ 1. 包含合并
    def _on_bar(self, i: int, h: float, l: float) -> None:
        if not self.bars:
            self.bars.append(MergedBar(0, h, l, i, i, 0))
            return
        last = self.bars[-1]
        inclusion = (h >= last.high and l <= last.low) or (h <= last.high and l >= last.low)
        if inclusion:
            d = last.dir
            if d == 0:  # 初始方向未定：按重心方向定
                d = 1 if (h + l) >= (last.high + last.low) else -1
                last.dir = d
            if d > 0:
                last.high = max(last.high, h)
                last.low = max(last.low, l)
            else:
                last.high = min(last.high, h)
                last.low = min(last.low, l)
            last.last_i = i
            return
        # 新合并K诞生 → 倒数第 3 根（三根结构的中间K）分型确认
        newdir = 1 if h > last.high else -1
        self.bars.append(MergedBar(len(self.bars), h, l, i, i, newdir))
        if len(self.bars) >= 4:
            self._confirm_fractal(self.bars[-3])

    def _confirm_fractal(self, mid: MergedBar) -> None:
        a = self.bars[mid.seq - 1]
        c = self.bars[mid.seq + 1]
        kind = None
        if mid.dir == 1 and c.dir == -1 and mid.high > a.high and mid.high > c.high:
            kind, price = TOP, mid.high
        elif mid.dir == -1 and c.dir == 1 and mid.low < a.low and mid.low < c.low:
            kind, price = BOTTOM, mid.low
        if kind is None:
            return
        f = Fractal(
            kind=kind,
            price=price,
            mid_seq=mid.seq,
            mid_high=mid.high,
            mid_low=mid.low,
            mid_first_i=mid.first_i,
            mid_last_i=mid.last_i,
            confirm_i=self.bars[-1].first_i,
            seq_lo=mid.seq - 1,
            seq_hi=mid.seq + 1,
        )
        self.fractals.append(f)
        self._emit(f.confirm_i, "fractal", f)
        self._on_fractal(f)

    # ------------------------------------------------------------ 2. 笔状态机
    def _stroke_ok(self, start: Fractal, end: Fractal) -> bool:
        """老笔有效性：中间K序号差足够 + 方向性价格条件。"""
        if end.mid_seq - start.mid_seq < self.stroke_gap:
            return False
        if end.kind == TOP:
            return end.price > start.mid_high  # 上笔：顶须越过底分型中间K高点
        return end.price < start.mid_low       # 下笔：底须跌破顶分型中间K低点

    @staticmethod
    def _more_extreme(f: Fractal, ref: Fractal) -> bool:
        """同向比较：f 是否比 ref 更极端（仅对同 kind 分型有意义）。"""
        assert f.kind == ref.kind, f"异类分型不可比: {f.kind} vs {ref.kind}"
        return f.price > ref.price if f.kind == TOP else f.price < ref.price

    def _on_fractal(self, f: Fractal) -> None:
        if not self._alive:
            self._bootstrap_stroke(f)
            return
        last = self._alive[-1]
        if f.kind == last.end.kind:
            if self._more_extreme(f, last.end):
                # 笔延伸
                last.end_history.append((f, f.confirm_i))
                last.end = f
                self._emit(f.confirm_i, "stroke_extended", last)
                self._pivot_on_last_stroke_changed()
            return
        # 反向分型：候选新笔
        if self._stroke_ok(last.end, f):
            self._add_stroke(last.end, f)
            return
        # 距离不足：仅当 f 比末笔起点更极端才构成笔破坏
        if self._more_extreme(f, last.start):
            last.alive = False
            last.destroyed_i = f.confirm_i
            self._alive.pop()
            self._emit(f.confirm_i, "stroke_destroyed", last)
            if not self._alive:
                # 首笔被破坏：回到候选起点状态（此前必无任何中枢）
                self._seed = f
                return
            prev = self._alive[-1]
            prev.end_history.append((f, f.confirm_i))
            prev.end = f
            self._emit(f.confirm_i, "stroke_extended", prev)
            self._pivot_on_destroy(last, prev)
        # 否则丢弃该分型

    def _bootstrap_stroke(self, f: Fractal) -> None:
        if self._seed is None:
            self._seed = f
            return
        if f.kind == self._seed.kind:
            if self._more_extreme(f, self._seed):
                self._seed = f  # 更极端的起点候选
            return
        if self._stroke_ok(self._seed, f):
            self._add_stroke(self._seed, f)
        # 反向但距离不足：丢弃，等待更远的反向分型

    def _add_stroke(self, start: Fractal, end: Fractal) -> None:
        self._seed = None
        s = Stroke(
            seq=self._n_stroke,
            direction=1 if end.kind == TOP else -1,
            start=start,
            end=end,
            confirm_i=end.confirm_i,
        )
        self._n_stroke += 1
        self.strokes.append(s)
        self._alive.append(s)
        self._emit(s.confirm_i, "stroke_added", s)
        self._pivot_on_stroke_added(s)

    # ------------------------------------------------------------ 3. 中枢状态机
    def _overlap(self, s: Stroke, p: Pivot) -> bool:
        return s.low <= p.zg and s.high >= p.zd

    def _pivot_on_stroke_added(self, s: Stroke) -> None:
        if self._open is not None:
            if self._overlap(s, self._open):
                if self._open.n_strokes >= self.pivot_max:
                    # 溢出：强制完成（无离开方向），溢出笔归下一候选
                    self._finish_pivot(s.confirm_i, exit_dir=0, leaving=None)
                    self._open = None
                    self._buffer = [s]
                    return
                self._open.n_strokes += 1
                self._open.last_stroke_seq = s.seq
                self._buffer = []
                self._emit(s.confirm_i, "pivot_extended", self._open)
                return
            # 无重叠笔：中枢完成。离开方向按区间关系——整笔在 ZG 上方 = 向上离开
            # （此时该笔若为下笔即"回踩不回中枢"，正是三买形态）；整笔在 ZD 下方 = 向下离开。
            exit_dir = 1 if s.low > self._open.zg else -1
            self._finish_pivot(s.confirm_i, exit_dir=exit_dir, leaving=s.seq)
            self._open = None
            self._buffer = [s]
            return
        # 无进行中枢：滑动窗口找三笔重叠
        self._buffer.append(s)
        self._try_form_from_buffer(s.confirm_i)

    def _try_form_from_buffer(self, confirm_i: int) -> None:
        if self._open is not None or len(self._buffer) < 3:
            return
        tri = self._buffer[-3:]
        zg = min(x.high for x in tri)
        zd = max(x.low for x in tri)
        if zg <= zd:
            return
        p = Pivot(
            pid=self._n_pivot,
            first_stroke_seq=tri[0].seq,
            last_stroke_seq=tri[2].seq,
            zg=zg,
            zd=zd,
            n_strokes=3,
            formed_i=confirm_i,
        )
        self._n_pivot += 1
        self._open = p
        self.pivots.append(p)
        self._buffer = []
        self._emit(confirm_i, "pivot_formed", p)

    def _pivot_on_last_stroke_changed(self) -> None:
        """末笔延伸后的中枢维护（区间只增不减，重叠保持）。

        - 进行中枢的末成员延伸：无需处理；
        - 候选缓冲中的末笔延伸：重测三笔重叠（延伸可能使原本不重叠的三笔重叠）。
        """
        if self._open is not None:
            return  # 末笔必为进行中枢末成员（见状态机），重叠保持
        if not self._buffer or not self._alive:
            return
        last = self._alive[-1]
        if self._buffer[-1] is not last:
            return
        formed_i = last.end_history[-1][1] if last.end_history else last.confirm_i
        self._try_form_from_buffer(formed_i)

    def _pivot_on_destroy(self, dead: Stroke, prev: Stroke) -> None:
        """笔破坏回滚：dead 是刚被撤销的末笔，prev 是被延伸后的新末笔。"""
        if self._open is not None:
            # dead 必为进行中枢的末成员（有进行中枢时最新笔即末成员）
            if self._open.n_strokes - 1 < 3:
                # 中枢撤销：存活成员退回候选缓冲
                self.pivots.remove(self._open)
                self._emit(dead.destroyed_i, "pivot_cancelled", self._open)
                members = [
                    s for s in self._alive
                    if self._open.first_stroke_seq <= s.seq < dead.seq
                ]
                self._open = None
                self._buffer = members
            else:
                self._open.n_strokes -= 1
                self._open.last_stroke_seq = prev.seq
                self._buffer = []
                self._emit(dead.destroyed_i, "pivot_reshaped", self._open)
            # prev 被延伸：若回到候选缓冲，重测三笔
            self._pivot_on_last_stroke_changed()
            return
        # 无进行中枢：dead 在候选缓冲中（必为 buffer[-1]）
        if self._buffer and self._buffer[-1] is dead:
            self._buffer.pop()
        # dead 是否为最后一个已完成中枢的离开/溢出笔 → 重开该中枢
        if self.pivots:
            p = self.pivots[-1]
            if p.completed_i is not None and (
                p.leaving_stroke_seq == dead.seq
                or (p.exit_dir == 0 and p.last_stroke_seq + 1 == dead.seq)
            ):
                p.completed_i = None
                p.exit_dir = None
                p.leaving_stroke_seq = None
                self._open = p
                self._buffer = []
                self._emit(dead.destroyed_i, "pivot_reopened", p)
                # prev（原末成员，已延伸）重叠保持，无需处理
                return
        # 普通候选缓冲收缩：prev 被延伸，重测三笔
        self._pivot_on_last_stroke_changed()

    def _finish_pivot(self, i: int, exit_dir: int, leaving: Optional[int]) -> None:
        self._open.completed_i = i
        self._open.exit_dir = exit_dir
        self._open.leaving_stroke_seq = leaving
        self._emit(i, "pivot_completed" if exit_dir != 0 else "pivot_overflow", self._open)


def _snapshot(kind: str, ref) -> str:
    """事件发射时刻的稳定摘要（值语义，供因果性前缀比对）。"""
    if kind == "fractal":
        return f"{ref.kind}@{ref.mid_seq}:{ref.price:.4g}/{ref.confirm_i}"
    if kind in ("stroke_added", "stroke_extended", "stroke_destroyed"):
        state = "" if kind == "stroke_added" else ("" if ref.alive else "X")
        return f"s{ref.seq}{state}:{ref.start.kind}{ref.start.mid_seq}->{ref.end.kind}{ref.end.mid_seq}:{ref.end.price:.4g}/{ref.confirm_i if kind == 'stroke_added' else ref.destroyed_i if kind == 'stroke_destroyed' else ref.end_history[-1][1]}"
    # pivot 系
    if kind in ("pivot_formed", "pivot_extended", "pivot_completed", "pivot_overflow",
                "pivot_reopened", "pivot_reshaped", "pivot_cancelled"):
        return f"p{ref.pid}:{ref.first_stroke_seq}-{ref.last_stroke_seq}:{ref.zg:.4g}/{ref.zd:.4g}:n{ref.n_strokes}:{ref.exit_dir}:{ref.completed_i}"
    return kind


def analyze(high: np.ndarray, low: np.ndarray, **kw) -> ChanResult:
    """便捷入口：两列价格数组 → 结构结果。"""
    return ChanEngine(**kw).run(high, low)
