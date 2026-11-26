"""缠论验证共享数据装载（runner 与分析脚本共用）。"""

from __future__ import annotations

import io
from typing import Dict, Iterator, List

import numpy as np
import pandas as pd

from ..analysis.limit_up_rules import board_of, limit_pct_series
from .backtest import Bars

DB = dict(host="127.0.0.1", port=5432, user="postgres", password="yuzh1234",
          dbname="stock_analysis_uat")

LOAD_FROM = "20140601"   # 含 MACD warmup
STAIR_STEP = 0.005       # 复权因子阶梯化阈值


def fetch_df(conn, sql: str, params) -> pd.DataFrame:
    with conn.cursor() as cur:
        rendered = cur.mogrify(sql, tuple(params)).decode("utf-8")
        buf = io.StringIO()
        cur.copy_expert(f"COPY ({rendered}) TO STDOUT WITH CSV HEADER", buf)
    buf.seek(0)
    return pd.read_csv(buf, dtype={"ts_code": str, "stock_code": str, "trade_date": str})


def load_calendar(conn, start: str, end: str) -> Dict[str, int]:
    df = fetch_df(
        conn,
        "SELECT calendar_date FROM base_trade_calendar "
        "WHERE is_trading_day=1 AND calendar_date>=%s AND calendar_date<=%s ORDER BY calendar_date",
        (_dotted(start), _dotted(end)),
    )
    return {d.replace("-", ""): k for k, d in enumerate(df["calendar_date"])}


def _dotted(d: str) -> str:
    return f"{d[:4]}-{d[4:6]}-{d[6:]}"


def load_universe(conn, from_date: str = LOAD_FROM) -> pd.DataFrame:
    return fetch_df(
        conn,
        "SELECT DISTINCT ts_code, stock_code FROM base_stock_info b "
        "WHERE b.type='1' AND b.list_status='L' "
        "AND EXISTS (SELECT 1 FROM his_kline_day k WHERE k.ts_code=b.ts_code AND k.trade_date>=%s) "
        "ORDER BY ts_code",
        (from_date,),
    )


def load_pool_daily_returns(conn, start: str, end: str) -> pd.DataFrame:
    """全池等权日收益（指数替代，用于市场状态分层）。"""
    return fetch_df(
        conn,
        "SELECT trade_date, avg(close/NULLIF(preclose,0)-1.0) AS ret, count(*) AS n "
        "FROM his_kline_day WHERE trade_date>=%s AND trade_date<=%s AND volume>0 "
        "AND preclose>0 AND close>0 "
        "GROUP BY trade_date ORDER BY trade_date",
        (start, end),
    )


def load_bars_batch(conn, codes: List[str], end: str, cal: Dict[str, int],
                    min_bars: int = 260) -> Iterator[Bars]:
    """一批股票 → 逐股 Bars（qfq 负价股自动转后复权域）。"""
    df = fetch_df(
        conn,
        "SELECT ts_code, stock_code, trade_date, open, high, low, close, preclose, "
        "(CASE WHEN is_st THEN 1 ELSE 0 END) AS is_st "
        "FROM his_kline_day WHERE ts_code = ANY(%s) AND trade_date>=%s AND trade_date<=%s "
        "ORDER BY ts_code, trade_date",
        (codes, LOAD_FROM, end),
    )
    if df.empty:
        return
    df = _rebuild_hfq(conn, df)
    for code, g in df.groupby("ts_code", sort=True):
        g = g.reset_index(drop=True)
        if len(g) < min_bars:
            continue
        try:
            board = board_of(str(g["stock_code"].iloc[0]))
            g["_lp"] = limit_pct_series(g, board)
            dates = g["trade_date"].to_numpy()
            cal_idx = np.array([cal[d] for d in dates])
        except KeyError:
            continue
        high = g["high"].to_numpy(float)
        low = g["low"].to_numpy(float)
        if not (np.isfinite(high).all() and np.isfinite(low).all()):
            continue
        yield Bars(
            ts_code=code,
            date=dates,
            open=g["open"].to_numpy(float),
            high=high,
            low=low,
            close=g["close"].to_numpy(float),
            preclose=g["preclose"].to_numpy(float),
            is_st=g["is_st"].fillna(0).to_numpy(np.int8),
            limit_pct=g["_lp"].to_numpy(float),
            cal_idx=cal_idx,
        )


def _rebuild_hfq(conn, df: pd.DataFrame) -> pd.DataFrame:
    """qfq 负价股转后复权域：qfq × hfq_factor/qfq_factor（因子阶梯化）。"""
    codes = df.loc[df["close"] <= 0, "ts_code"].unique().tolist()
    if not codes:
        return df
    fac = fetch_df(
        conn,
        "SELECT ts_code, trade_date, qfq_factor, hfq_factor FROM base_adjust_factor "
        "WHERE ts_code = ANY(%s)",
        (codes,),
    )
    fac["trade_date"] = fac["trade_date"].astype(str).str.replace("-", "")
    code_set = set(codes)
    out = []
    from bisect import bisect_right
    for code, g in df.groupby("ts_code"):
        if code not in code_set:
            out.append(g)
            continue
        f = fac[fac["ts_code"] == code].sort_values("trade_date")
        if f.empty:
            out.append(g)
            continue
        ratio = pd.Series((f["hfq_factor"] / f["qfq_factor"]).to_numpy())
        stair = ratio.where(ratio.diff().abs() > STAIR_STEP).ffill().to_numpy()
        dates_sorted = f["trade_date"].tolist()
        g = g.copy()

        def _lookup(d: str):
            """§8 修复：目标日只允许 ≤ 该日的最近因子阶梯（不取序列末值——
            那是未来值前视）；早于因子首日为无前值，输出不可用"""
            idx = bisect_right(dates_sorted, d)
            if idx == 0:
                return None
            return stair[idx - 1]

        ratios: list = []
        notes: list = []
        for d in g["trade_date"]:
            v = _lookup(str(d))
            if v is None or not np.isfinite(v):
                ratios.append(1.0)
                notes.append("hfq_stair_unavailable:无前值/无匹配因子阶梯")
            else:
                ratios.append(v)
                notes.append(None)
        g["_hfq_note"] = notes
        for col in ("open", "high", "low", "close", "preclose"):
            g[col] = g[col] * np.array(ratios)
        out.append(g)
    return pd.concat(out, ignore_index=True)
