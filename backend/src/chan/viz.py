"""结构可视化（抽样人工核对 / 报告插图）。

图元：K线（红涨绿跌）、分型标记（▲顶 ▼底）、笔折线、中枢矩形（ZD~ZG）、
信号星标（发射时刻）。
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

from .structure import BOTTOM, TOP, ChanResult

UP = "#d43a3a"    # A股红涨
DOWN = "#1a9e6b"  # 绿跌


def _candles(ax, o, h, l, c, x):
    for i in x:
        xi = int(i)
        color = UP if c[xi] >= o[xi] else DOWN
        ax.plot([xi, xi], [l[xi], h[xi]], color=color, lw=0.7, zorder=1)
        body_lo, body_hi = min(o[xi], c[xi]), max(o[xi], c[xi])
        ax.add_patch(Rectangle((xi - 0.35, body_lo), 0.7, max(body_hi - body_lo, 1e-9),
                               facecolor=color, edgecolor=color, zorder=2))


def plot_case(out_png: Path, ts_code: str, dates, o, h, l, c, res: ChanResult,
              lo: int, hi: int, title: str,
              fire_points: Optional[List[tuple]] = None) -> None:
    n = len(c)
    lo, hi = max(0, lo), min(hi, n - 1)
    fig, ax = plt.subplots(figsize=(14, 6), dpi=110)
    xs = range(lo, hi + 1)
    _candles(ax, o, h, l, c, xs)

    # 分型与笔（只画起止点都落在窗口内的存活笔）
    alive = [s for s in res.strokes if s.alive]
    for s in alive:
        x0, y0 = s.start.mid_first_i, s.start.price
        x1, y1 = s.end.mid_first_i, s.end.price
        if lo <= x0 <= hi and lo <= x1 <= hi:
            ax.plot([x0, x1], [y0, y1], color="#2b6cb0", lw=1.6, alpha=0.85, zorder=3)
    for f in res.fractals:
        if lo <= f.mid_first_i <= hi:
            if f.kind == TOP:
                ax.scatter([f.mid_first_i], [f.price], marker="v", color="#b7791f", s=26, zorder=4)
            else:
                ax.scatter([f.mid_first_i], [f.price], marker="^", color="#2c7a7b", s=26, zorder=4)

    # 中枢矩形
    strokes_alive = {s.seq: s for s in alive}
    for p in res.pivots:
        s0 = strokes_alive.get(p.first_stroke_seq)
        s1 = strokes_alive.get(p.last_stroke_seq)
        if s0 is None or s1 is None:
            continue
        x0, x1 = s0.start.mid_first_i, s1.end.mid_first_i
        if x1 < lo or x0 > hi:
            continue
        ax.add_patch(Rectangle((max(x0, lo), p.zd), min(x1, hi) - max(x0, lo) + 1,
                               max(p.zg - p.zd, 1e-9), facecolor="#805ad5",
                               alpha=0.15, edgecolor="#805ad5", lw=1.0, zorder=0))
        ax.text(max(x0, lo), p.zg, f" ZG={p.zg:.2f}", fontsize=7, color="#553c9a", va="bottom")

    if fire_points:
        for i, label in fire_points:
            if lo <= i <= hi:
                ax.axvline(i, color="#e53e3e", lw=0.8, ls="--", alpha=0.6)
                ax.text(i, max(h[lo:hi + 1]), f" {label}", fontsize=9, color="#e53e3e",
                        ha="left", va="top")

    ticks = list(range(lo, hi + 1, max(1, (hi - lo) // 10)))
    ax.set_xticks(ticks)
    ax.set_xticklabels([str(dates[k]) for k in ticks], fontsize=7, rotation=45)
    ax.set_xlim(lo - 1, hi + 1)
    ax.set_title(title, fontsize=11)
    ax.grid(alpha=0.2)
    fig.tight_layout()
    fig.savefig(out_png)
    plt.close(fig)
