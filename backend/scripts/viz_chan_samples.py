"""抽样可视化核对驱动：按信号种类分层抽样，渲染 K线+结构+信号图。

用法：python scripts/viz_chan_samples.py [--n 20] [--out ../data/chan_validation/viz]
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
import psycopg2

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from src.chan.data import DB, load_bars_batch, load_calendar  # noqa: E402
from src.chan.signals import derive_signals  # noqa: E402
from src.chan.structure import ChanEngine  # noqa: E402
from src.chan.viz import plot_case  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
END = "20260930"
KINDS = ["b3", "b1", "b2", "s3"]


def main(n_per_kind: int, out_dir: Path) -> None:
    sig = pd.read_csv(BACKEND.parents[0] / "data" / "chan_validation" / "base" / "signals.csv",
                      dtype={"ts_code": str, "fire_date": str})
    picks = []
    for k in KINDS:
        g = sig[sig["kind"] == k]
        if len(g):
            picks.append(g.sample(n=min(n_per_kind, len(g)), random_state=11))
    picks = pd.concat(picks)
    out_dir.mkdir(parents=True, exist_ok=True)

    conn = psycopg2.connect(**DB)
    cal = load_calendar(conn, "20140601", END)
    done = set()
    for _, row in picks.iterrows():
        code = row["ts_code"]
        for bars in load_bars_batch(conn, [code], END, cal):
            res = ChanEngine().run(bars.high, bars.low)
            from src.analysis.indicators.tdx_indicators import macd as tdx_macd
            m = tdx_macd(pd.Series(bars.close))
            sigs = derive_signals(res, dif=m.dif.to_numpy(), hist=m.hist.to_numpy())
            date_pos = {d: i for i, d in enumerate(bars.date)}
            i = date_pos.get(row["fire_date"])
            if i is None:
                continue
            fps = [(s.fire_i, s.kind.upper())
                   for s in sigs if i - 70 <= s.fire_i <= i + 70]
            tag = f"{code}_{row['kind']}_{row['fire_date']}"
            plot_case(
                out_dir / f"{tag}.png", code, bars.date,
                bars.open, bars.high, bars.low, bars.close, res,
                lo=i - 70, hi=i + 50,
                title=f"{code} {row['kind'].upper()} fire={row['fire_date']} "
                      f"ZG={row['zg']:.2f} ZD={row['zd']:.2f}",
                fire_points=fps,
            )
            done.add(code)
    logging.info("渲染完成 %d 张 → %s", len(picks), out_dir)
    conn.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=5)
    ap.add_argument("--out", default=str(BACKEND.parents[0] / "data" / "chan_validation" / "viz"))
    a = ap.parse_args()
    main(a.n, Path(a.out))
