"""缠论验证结果分析（Phase 3）：随机基线 + 分层 + Bootstrap 置信区间 + BH 校正。

设计要点
--------
- **随机基线**：对每个信号在其所属股票上随机重抽入场日（同 pool、同可执行性
  约束、同出场规则），重复 ``REPS`` 轮 → 随机均值分布。幸存者偏差在
  「信号 − 随机」差值中抵消（同一股票池）。
- **显著性**：signal 均值 − 随机均值的差值 Bootstrap 95% CI；族内
  Benjamini-Hochberg 校正。
- **分层**：按年度；按市场状态（全池等权累计净值 vs 其 MA200）。
- 三卖 s3 不做交易，统计信号后 20 日前瞻收益作方向性证据。

用法：
    python scripts/analyze_chan_results.py --dir data/chan_validation/base
"""

from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from src.chan.backtest import _open_limit_up, simulate  # noqa: E402
from src.chan.data import DB, load_bars_batch, load_calendar, load_pool_daily_returns  # noqa: E402
from src.chan.signals import Signal  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("chan_analysis")

START, END = "20150105", "20260930"
FIXED_RULES = [(5, "fixed"), (10, "fixed"), (20, "fixed"), (60, "fixed")]
REPS = 12
BOOT = 2000
RNG = np.random.default_rng(20261024)


# ---------------------------------------------------------------- 市场状态
def market_states() -> pd.DataFrame:
    conn = psycopg2.connect(**DB)
    try:
        pool = load_pool_daily_returns(conn, START, END)
    finally:
        conn.close()
    pool["trade_date"] = pool["trade_date"].astype(str).str.replace("-", "")
    pool["cum"] = (1 + pool["ret"]).cumprod()
    pool["ma200"] = pool["cum"].rolling(200, min_periods=100).mean()
    pool["state"] = np.where(pool["cum"] >= pool["ma200"], "bull", "bear")
    return pool[["trade_date", "state"]]


# ---------------------------------------------------------------- 随机基线
def valid_fire_mask(bars) -> np.ndarray:
    n = len(bars.date)
    m = np.zeros(n, dtype=bool)
    lo, hi = 120, n - 2
    if hi <= lo:
        return m
    idx = np.arange(lo, hi + 1)
    e = idx + 1
    ok = (bars.is_st[idx] == 0) & (bars.is_st[e] == 0)
    ok &= (bars.cal_idx[e] - bars.cal_idx[idx]) <= 20
    limit_ok = np.array([not _open_limit_up(bars, int(j)) for j in e])
    m[idx] = ok & limit_ok
    return m


def run_baseline(trades: pd.DataFrame, signals: pd.DataFrame, out_dir: Path) -> pd.DataFrame:
    rows = (trades[trades["mode"] == "fixed"]
            [["ts_code", "signal_kind", "fire_date"]].drop_duplicates())
    s3 = signals[signals["kind"] == "s3"].sample(n=min(len(signals[signals["kind"] == "s3"]), 8000),
                                                random_state=7)[["ts_code", "fire_date"]]
    codes = sorted(set(rows["ts_code"]) | set(s3["ts_code"]))
    log.info("基线覆盖 %d 只股票，信号行 %d，s3 抽样 %d", len(codes), len(rows), len(s3))

    conn = psycopg2.connect(**DB)
    cal = load_calendar(conn, "20140601", END)
    rand_recs: list[dict] = []
    s3_fwd: list[dict] = []
    B = 300
    for bi in range(0, len(codes), B):
        chunk = codes[bi:bi + B]
        row_by_code = {c: g for c, g in rows.groupby("ts_code")}
        s3_by_code = {c: g for c, g in s3.groupby("ts_code")}
        for bars in load_bars_batch(conn, chunk, END, cal):
            vmask = valid_fire_mask(bars)
            valid = np.flatnonzero(vmask)
            n = len(bars.date)
            g = row_by_code.get(bars.ts_code)
            if g is not None and len(valid) > 0:
                kinds = g["signal_kind"].to_numpy()
                for rep in range(REPS):
                    picks = RNG.choice(valid, size=len(g))
                    for k, i in zip(kinds, picks):
                        sig = Signal(kind=str(k), fire_i=int(i), pivot_pid=-1,
                                     zg=0.0, zd=0.0, stop_price=0.0, trigger_low=0.0)
                        for h, mode in FIXED_RULES:
                            t = simulate(bars, sig, mode, h)
                            if t is not None:
                                rand_recs.append({"kind": k, "rep": rep, "param": h,
                                                  "ret": t.ret_net})
            g3 = s3_by_code.get(bars.ts_code)
            if g3 is not None and n > 260:
                date_pos = {d: k for k, d in enumerate(bars.date)}
                for _, r in g3.iterrows():
                    i = date_pos.get(r["fire_date"])
                    if i is None or i + 20 >= n:
                        continue
                    s3_fwd.append({"ts_code": bars.ts_code,
                                   "fwd20": bars.close[i + 20] / bars.close[i] - 1})
        log.info("基线进度 %d/%d", min(bi + B, len(codes)), len(codes))
    conn.close()

    rand = pd.DataFrame(rand_recs)
    rand.to_csv(out_dir / "random_baseline.csv", index=False)
    pd.DataFrame(s3_fwd).to_csv(out_dir / "s3_forward20.csv", index=False)
    return rand


# ---------------------------------------------------------------- 统计
def bootstrap_diff(sig_rets: np.ndarray, rand_means: np.ndarray) -> dict:
    m_s = float(sig_rets.mean())
    m_r = float(rand_means.mean())
    diffs = np.empty(BOOT)
    for b in range(BOOT):
        s = RNG.choice(sig_rets, size=len(sig_rets), replace=True).mean()
        r = RNG.choice(rand_means, size=len(rand_means), replace=True).mean()
        diffs[b] = s - r
    return {
        "sig_mean": m_s, "rand_mean": m_r, "diff": m_s - m_r,
        "ci_lo": float(np.percentile(diffs, 2.5)),
        "ci_hi": float(np.percentile(diffs, 97.5)),
        "p_less_equal_0": float((diffs <= 0).mean()),
    }


def bh(pvals: np.ndarray) -> np.ndarray:
    p = np.asarray(pvals, dtype=float)
    q = np.empty_like(p)
    order = np.argsort(p)
    m = len(p)
    prev = 1.0
    for rank, k in enumerate(order[::-1]):
        i = m - 1 - rank
        prev = min(prev, p[i] * m / (i + 1))
        q[k] = prev
    return q


def main(dir_: Path) -> None:
    trades = pd.read_csv(dir_ / "trades.csv", dtype={"ts_code": str, "fire_date": str})
    signals = pd.read_csv(dir_ / "signals.csv", dtype={"ts_code": str, "fire_date": str})
    rand_path = dir_ / "random_baseline.csv"
    rand = (pd.read_csv(rand_path) if rand_path.exists() else run_baseline(trades, signals, dir_))

    state = market_states()
    smap = dict(zip(state["trade_date"], state["state"]))
    trades["year"] = trades["fire_date"].str[:4]
    trades["state"] = trades["fire_date"].map(smap)

    # 主表：各口径费后表现
    g = trades.groupby(["signal_kind", "mode", "param"])["ret_net"].agg(
        n="count", mean="mean", median="median",
        win=lambda x: x.gt(0).mean(), std="std").round(4)
    g.to_csv(dir_ / "summary_main.csv")

    # 信号 vs 随机（固定持有口径）
    recs = []
    rand_means = rand.groupby(["kind", "rep", "param"])["ret"].mean().reset_index()
    for (kind, param), grp in trades[trades["mode"] == "fixed"].groupby(
            ["signal_kind", "param"]):
        rm = rand_means[(rand_means["kind"] == kind) &
                        (rand_means["param"] == param)]["ret"].to_numpy()
        if len(rm) == 0:
            continue
        d = bootstrap_diff(grp["ret_net"].to_numpy(), rm)
        recs.append({"kind": kind, "param": param, "n": len(grp), **d})
    vs = pd.DataFrame(recs)
    vs["bh_q"] = bh(vs["p_less_equal_0"].to_numpy())
    vs.to_csv(dir_ / "summary_vs_random.csv", index=False)

    # 分层
    trades.groupby(["signal_kind", "mode", "param", "year"])["ret_net"].agg(
        ["count", "mean", "median", lambda x: x.gt(0).mean()]).rename(
        columns={"<lambda_0>": "win"}).round(4).to_csv(dir_ / "summary_by_year.csv")
    trades.groupby(["signal_kind", "mode", "param", "state"])["ret_net"].agg(
        ["count", "mean", "median", lambda x: x.gt(0).mean()]).rename(
        columns={"<lambda_0>": "win"}).round(4).to_csv(dir_ / "summary_by_state.csv")

    print("=== 主表（费后） ===")
    print(g.to_string())
    print()
    print("=== 信号 vs 随机基线（固定持有，差值 Bootstrap 95%% CI） ===")
    cols = ["kind", "param", "n", "sig_mean", "rand_mean", "diff", "ci_lo", "ci_hi",
            "p_less_equal_0", "bh_q"]
    print(vs[cols].round(4).to_string(index=False))
    print()
    s3f = dir_ / "s3_forward20.csv"
    if s3f.exists():
        f = pd.read_csv(s3f)
        print(f"=== 三卖 s3 信号后 20 日前瞻收益（方向性证据，n={len(f)}） ===")
        print(f"mean={f['fwd20'].mean():.4f}  median={f['fwd20'].median():.4f}  "
              f"P(<0)={ (f['fwd20'] < 0).mean():.3f}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    main(Path(ap.parse_args().dir))
