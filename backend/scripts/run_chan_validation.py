"""缠论验证批量运行器（Phase 2/3）。

全A 逐批加载日K（qfq 域，负价股转后复权）→ 结构引擎 → 信号 → 逐信号撮合回测，
输出信号与成交明细 CSV 到 ``data/chan_validation/``（敏感性变体用 --tag 区分目录）。

用法：
    python scripts/run_chan_validation.py [--tag base] [--stroke-min 5] [--pivot-max 9]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from src.analysis.indicators.tdx_indicators import macd as tdx_macd  # noqa: E402
from src.chan.backtest import simulate  # noqa: E402
from src.chan.data import DB, load_bars_batch, load_calendar, load_universe  # noqa: E402
from src.chan.signals import derive_signals  # noqa: E402
from src.chan.structure import ChanEngine  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("chan_validation")

EXIT_RULES = [
    ("stop", 20),
    ("fixed", 5),
    ("fixed", 10),
    ("fixed", 20),
    ("fixed", 60),
]


def run(start: str, end: str, batch_size: int, out_dir: Path,
        stroke_min: int = 5, pivot_max: int = 9) -> None:
    conn = psycopg2.connect(**DB)
    t0 = time.time()
    cal = load_calendar(conn, "20140601", end)
    codes = load_universe(conn)["ts_code"].tolist()
    log.info("日历 %d 天，股票池 %d 只（stroke_min=%d, pivot_max=%d）",
             len(cal), len(codes), stroke_min, pivot_max)

    all_trades = []
    all_signals = []
    n_batches = (len(codes) + batch_size - 1) // batch_size
    for bi in range(n_batches):
        chunk = codes[bi * batch_size:(bi + 1) * batch_size]
        for bars in load_bars_batch(conn, chunk, end, cal):
            res = ChanEngine(stroke_min_bars=stroke_min,
                             pivot_max_strokes=pivot_max).run(bars.high, bars.low)
            m = tdx_macd(pd.Series(bars.close))
            sigs = derive_signals(res, dif=m.dif.to_numpy(), hist=m.hist.to_numpy())
            for s in sigs:
                fd = str(bars.date[s.fire_i])
                if not (start <= fd <= end):
                    continue
                all_signals.append({
                    "kind": s.kind, "ts_code": bars.ts_code, "fire_date": fd,
                    "zg": round(s.zg, 4), "zd": round(s.zd, 4),
                    "stop": round(s.stop_price, 4), "fire_i": s.fire_i,
                })
                if s.kind == "s3":
                    continue  # 仅方向性统计
                for mode, param in EXIT_RULES:
                    t = simulate(bars, s, mode, param)
                    if t is not None:
                        all_trades.append(t)
                if s.kind == "b3":
                    t = simulate(bars, s, "stop_zd", 20)  # 敏感性：深止损（中枢下沿）
                    if t is not None:
                        all_trades.append(t)
        if (bi + 1) % 5 == 0 or bi + 1 == n_batches:
            log.info("批次 %d/%d 完成，累计信号 %d，成交 %d，耗时 %.0fs",
                     bi + 1, n_batches, len(all_signals), len(all_trades), time.time() - t0)
            _dump(out_dir, all_signals, all_trades)

    _dump(out_dir, all_signals, all_trades, final=True)
    log.info("全部完成：信号 %d，成交记录 %d，总耗时 %.0fs",
             len(all_signals), len(all_trades), time.time() - t0)
    conn.close()


def _dump(out_dir: Path, signals: list, trades: list, final: bool = False) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    suffix = "" if final else ".partial"
    pd.DataFrame(signals).to_csv(out_dir / f"signals{suffix}.csv", index=False)
    pd.DataFrame([t.__dict__ for t in trades]).to_csv(out_dir / f"trades{suffix}.csv", index=False)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="20150105")
    ap.add_argument("--end", default="20260930")
    ap.add_argument("--batch", type=int, default=300)
    ap.add_argument("--tag", default="base")
    ap.add_argument("--stroke-min", type=int, default=5)
    ap.add_argument("--pivot-max", type=int, default=9)
    args = ap.parse_args()
    out = BACKEND.parents[0] / "data" / "chan_validation" / args.tag
    run(args.start, args.end, args.batch, out, args.stroke_min, args.pivot_max)
