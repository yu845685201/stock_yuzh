"""缠论背驰买点模块测试：三态 + 与验证引擎 signals.csv 真实对拍。"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

BACKEND = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND))

from src.analysis.module_base import ChainContext, StockInfo  # noqa: E402
from src.analysis.modules.chan_divergence_buy import ChanDivergenceBuy  # noqa: E402

DB = dict(host="127.0.0.1", port=5432, user="postgres", password="yuzh1234",
          dbname="stock_analysis_uat")
SIGNALS_CSV = BACKEND.parents[0] / "data" / "chan_validation" / "base" / "signals.csv"


def make_ctx(as_of: str, ts_code: str = "sz.000001") -> ChainContext:
    return ChainContext(
        as_of=as_of,
        stock=StockInfo(ts_code=ts_code, stock_code=ts_code.split(".")[1], stock_name="测试"),
    )


def make_df(closes, start="20250101"):
    """等差K线构造 df（相邻差≥1 无包含；末行=锚定日）。"""
    dates = pd.bdate_range(start, periods=len(closes)).strftime("%Y%m%d")
    return pd.DataFrame({
        "trade_date": dates,
        "high": [c + 0.4 for c in closes],
        "low": [c - 0.4 for c in closes],
        "close": closes,
        "is_st": 0,
    })


# ---------------------------------------------------------------- 三态
def test_insufficient_empty():
    r = ChanDivergenceBuy().analyze(pd.DataFrame(), make_ctx("20260101"))
    assert r.value == "数据不足"


def test_insufficient_anchor_mismatch():
    df = make_df(list(range(100, 100 + 200)))
    r = ChanDivergenceBuy().analyze(df, make_ctx("20200101"))
    assert r.value == "数据不足"


def test_insufficient_short_history():
    df = make_df(list(range(100, 100 + 100)))  # 100 根 < 120
    ctx = make_ctx(str(df["trade_date"].iloc[-1]))
    r = ChanDivergenceBuy().analyze(df, ctx)
    assert r.value == "数据不足"
    assert "次新" in (r.detail or {}).get("reason", "")


def test_st_anchor_rejected():
    df = make_df(list(range(100, 100 + 200)))
    df.loc[df.index[-1], "is_st"] = 1
    ctx = make_ctx(str(df["trade_date"].iloc[-1]))
    r = ChanDivergenceBuy().analyze(df, ctx)
    assert r.value == "否"
    assert (r.detail or {}).get("reason") == "锚定日ST剔除"


def test_flat_series_no_hit():
    # 温和上行无背驰结构 → 否（不抛异常、不数据不足）
    closes = [100 + 0.9 * i + (2 if i % 7 == 0 else 0) for i in range(300)]
    df = make_df(closes)
    ctx = make_ctx(str(df["trade_date"].iloc[-1]))
    r = ChanDivergenceBuy().analyze(df, ctx)
    assert r.value == "否"


def test_negative_price_insufficient():
    closes = [100 + i for i in range(200)]
    closes[5] = -1
    df = make_df(closes)
    ctx = make_ctx(str(df["trade_date"].iloc[-1]))
    r = ChanDivergenceBuy().analyze(df, ctx)
    assert r.value == "数据不足"
    assert "非正价" in (r.detail or {}).get("reason", "")


# ---------------------------------------------------------------- 真实对拍
def _load_window(ts_code: str, as_of: str, bars: int = 520):
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        conn = psycopg2.connect(**DB)
    except Exception:
        pytest.skip("PostgreSQL 不可用")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT trade_date, high, low, close, "
                "(CASE WHEN is_st THEN 1 ELSE 0 END) AS is_st "
                "FROM his_kline_day WHERE ts_code=%s AND trade_date<=%s "
                "ORDER BY trade_date DESC LIMIT %s",
                (ts_code, as_of, bars),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    rows = list(reversed(rows))
    return pd.DataFrame(rows, columns=["trade_date", "high", "low", "close", "is_st"])


@pytest.mark.skipif(not SIGNALS_CSV.exists(), reason="验证信号文件不存在")
def test_reproduce_validation_signals():
    """抽样对拍：模块在信号 fire 日应复现验证引擎的 b1/b2（≥70% 匹配率）。

    520 日窗口截断会漏掉跨窗口的早前中枢（少数不复现属预期）；
    匹配的信号种类必须一致（不允许多报三买/错报种类）。
    """
    sig = pd.read_csv(SIGNALS_CSV, dtype={"ts_code": str, "fire_date": str})
    pool = sig[(sig["kind"].isin(["b1", "b2"]))
               & (sig["fire_date"] >= "20160101") & (sig["fire_date"] <= "20260601")]
    # 确定性抽样：每股取一个信号，散布取样 40 只
    one_per_stock = pool.drop_duplicates("ts_code")
    step = max(1, len(one_per_stock) // 40)
    sample = one_per_stock.iloc[::step].head(40)

    matched = total = 0
    mis = []
    for _, row in sample.iterrows():
        df = _load_window(row["ts_code"], row["fire_date"])
        if len(df) < 120:
            continue
        total += 1
        r = ChanDivergenceBuy().analyze(df, make_ctx(row["fire_date"], row["ts_code"]))
        if r.value.startswith(row["kind"] == "b1" and "一买" or "二买"):
            matched += 1
        else:
            mis.append((row["ts_code"], row["fire_date"], row["kind"], r.value))
    assert total >= 30, f"可对拍样本不足: {total}"
    rate = matched / total
    assert rate >= 0.7, f"复现率过低: {rate:.2f}（失配样例前5: {mis[:5]}）"


@pytest.mark.skipif(not SIGNALS_CSV.exists(), reason="验证信号文件不存在")
def test_no_false_positive_on_quiet_days():
    """非信号日的股票原则上不得命中（允许 ≤2 例窗口截断带来的合法差异）。"""
    sig = pd.read_csv(SIGNALS_CSV, dtype={"ts_code": str, "fire_date": str})
    fired = set(zip(sig["ts_code"], sig["fire_date"]))
    pool = sig[sig["kind"].isin(["b1", "b2"])].drop_duplicates("ts_code")
    checked = unexpected = 0
    for _, row in pool.head(15).iterrows():
        # 取该股信号日前 30 个交易日的一天（结构存在但非信号日）
        df = _load_window(row["ts_code"], row["fire_date"])
        if len(df) < 160:
            continue
        quiet = df.iloc[-31]
        qd = str(quiet["trade_date"])
        if (row["ts_code"], qd) in fired:
            continue
        sub = df[df["trade_date"] <= qd].reset_index(drop=True)
        checked += 1
        r = ChanDivergenceBuy().analyze(sub, make_ctx(qd, row["ts_code"]))
        if r.value.startswith(("一买", "二买")):
            unexpected += 1
    assert checked >= 10
    assert unexpected <= 2, f"非信号日意外命中 {unexpected} 例（疑似误报）"
