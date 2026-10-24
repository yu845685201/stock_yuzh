"""信号派生层（Phase 2）：结构事件流 → 买卖点信号。

信号规约
--------
- **三买 b3**：中枢完成事件且向上离开（离开/回踩笔整体在 ZG 上方）。fire = 完成
  事件确认时刻；stop = ZG；trigger_low = 离开笔低点。
- **三卖 s3**：镜像（整体在 ZD 下方的向上反抽笔完成中枢）。仅统计方向性证据，
  A 股不构造空头交易。
- **一买 b1**：相邻两个已完成中枢依次下移（P2.zg < P1.zd）构成下跌趋势，
  P2 向下离开笔 D2 与 P1 的离开笔 L1（即 P2 进入笔）比较 MACD 背驰：
  ``|Σhist(D2)| < |Σhist(L1)|`` 且 ``DIF(D2端) > DIF(L1端)``。
  fire = P2 完成事件时刻；stop = D2 低点。
- **二买 b2**：一买后首笔回踩（下笔）不创新低（end > 一买低点）。fire = 该下笔
  确认时刻；stop = 一买低点。

全部信号只依赖 ``confirmed_i`` 及之前的信息（因果性由结构层保证，本层不得
引用事件的最终状态字段——延伸/破坏发生在此后）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

import numpy as np

from .structure import ChanResult, Pivot, Stroke

KIND_B1 = "b1"
KIND_B2 = "b2"
KIND_B3 = "b3"
KIND_S3 = "s3"


@dataclass
class Signal:
    kind: str
    fire_i: int
    pivot_pid: Optional[int]
    zg: float
    zd: float
    stop_price: float
    trigger_low: float
    extra: dict = field(default_factory=dict)


def _hist_area(hist: np.ndarray, s: Stroke) -> float:
    return float(np.sum(hist[s.start.mid_first_i: s.end.mid_last_i + 1]))


def derive_signals(res: ChanResult, dif: Optional[np.ndarray] = None,
                   hist: Optional[np.ndarray] = None) -> List[Signal]:
    """从结构结果派生信号。``dif/hist`` 为与原始K等长的 MACD 序列（一买需要）。"""
    strokes_by_seq = {s.seq: s for s in res.strokes}
    completed: List[Pivot] = []
    pending_b1: List[dict] = []  # 待二买确认的一买：{sig, leaving_seq, bottom_low}
    out: List[Signal] = []

    for ev in res.events:
        if ev.kind == "pivot_completed":
            p: Pivot = ev.ref
            leaving = strokes_by_seq.get(p.leaving_stroke_seq)
            if p.exit_dir == 1:
                out.append(Signal(
                    kind=KIND_B3, fire_i=ev.i, pivot_pid=p.pid,
                    zg=p.zg, zd=p.zd, stop_price=p.zg,
                    trigger_low=leaving.low,
                    extra={"leaving_seq": p.leaving_stroke_seq},
                ))
            elif p.exit_dir == -1:
                out.append(Signal(
                    kind=KIND_S3, fire_i=ev.i, pivot_pid=p.pid,
                    zg=p.zg, zd=p.zd, stop_price=p.zd,
                    trigger_low=leaving.high,
                    extra={"leaving_seq": p.leaving_stroke_seq},
                ))
                # 一买：与前一完成中枢构成下跌趋势（P2 整体低于 P1）+ 背驰。
                # 背驰段 D2 = 制造新低的下笔：向下离开笔本身，或反抽上笔的前一根；
                # 参照段 D1 = 进入本中枢的下笔（前一跌段）。
                if dif is not None and hist is not None and completed:
                    prev = completed[-1]
                    if p.zg < prev.zd:
                        d2 = (leaving if leaving.direction == -1
                              else strokes_by_seq.get(p.leaving_stroke_seq - 1))
                        d1 = strokes_by_seq.get(p.first_stroke_seq - 1)
                        if d2 is not None and d1 is not None and d2.direction == -1 and d1.direction == -1:
                            a1, a2 = _hist_area(hist, d1), _hist_area(hist, d2)
                            d1_end = float(dif[d1.end.mid_last_i])
                            d2_end = float(dif[d2.end.mid_last_i])
                            if abs(a2) < abs(a1) and a1 < 0 and d2_end > d1_end:
                                sig = Signal(
                                    kind=KIND_B1, fire_i=ev.i, pivot_pid=p.pid,
                                    zg=p.zg, zd=p.zd, stop_price=d2.low,
                                    trigger_low=d2.low,
                                    extra={
                                        "d1_seq": d1.seq, "d2_seq": d2.seq,
                                        "area1": a1, "area2": a2,
                                        "dif1": d1_end, "dif2": d2_end,
                                    },
                                )
                                out.append(sig)
                                pending_b1.append({
                                    "sig": sig,
                                    "leaving_seq": d2.seq,
                                    "bottom_i": d2.end.mid_last_i,
                                    "bottom_low": d2.low,
                                })
            completed.append(p)
        elif ev.kind in ("pivot_overflow",):
            p: Pivot = ev.ref
            completed.append(p)  # 溢出完成同样视为已完成中枢（可作趋势判定的前枢）
        elif ev.kind == "pivot_reopened":
            rp: Pivot = ev.ref
            if completed and completed[-1] is rp:
                completed.pop()
        elif ev.kind == "stroke_extended":
            s: Stroke = ev.ref
            if s.direction == -1:
                for pb in pending_b1:
                    if pb["leaving_seq"] == s.seq:
                        # 背驰段延伸创新低：二买参照改用最深低点（一买信号止损不变）
                        pb["bottom_low"] = s.low
        elif ev.kind == "stroke_added":
            s: Stroke = ev.ref
            if s.direction == -1:
                for pb in list(pending_b1):
                    if s.start.mid_first_i > pb["bottom_i"]:
                        pending_b1.remove(pb)
                        if s.end.price > pb["bottom_low"]:
                            out.append(Signal(
                                kind=KIND_B2, fire_i=ev.i, pivot_pid=pb["sig"].pivot_pid,
                                zg=pb["sig"].zg, zd=pb["sig"].zd,
                                stop_price=pb["bottom_low"],
                                trigger_low=s.end.price,
                                extra={"b1_fire_i": pb["sig"].fire_i, "pullback_seq": s.seq},
                            ))
    out.sort(key=lambda x: (x.fire_i, x.kind))
    return out
