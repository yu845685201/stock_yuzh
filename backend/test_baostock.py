"""
baostock 接口连通性测试

按 baostock 官方接口文档约定，逐步验证：
  1) bs.login()                          认证（无需注册，anonymous/123456 即可）
  2) bs.query_stock_basic(code=None)    全部证券基本资料
     字段: code, code_name, ipoDate, outDate, type(1股/2指/3其它/4可转债/5ETF), status(1上市/0退市)
  3) bs.query_stock_basic(code="sh.600000")  单只股票
  4) bs.query_trade_dates(start_date, end_date)  交易日历
     字段: calendar_date, is_trading_day
  5) bs.query_profit_data(code, year, quarter)  基本面（季频盈利能力）
     字段: code, pubDate, statDate, totalShare, liqaShare ...

每步打印 error_code/error_msg/返回条数/首条样本，便于定位失败环节。
"""

import sys
import time
import baostock as bs


def step(name, fn, depend_ok):
    """统一执行 + 打印包装。depend_ok 为前序依赖是否成功。"""
    print(f"\n>>> {name}")
    if not depend_ok:
        print(f"    跳过（前序依赖失败）")
        return False, None
    try:
        ok, payload = fn()
        return ok, payload
    except Exception as e:
        print(f"    异常: {type(e).__name__}: {e}")
        return False, None


def test_login():
    lg = bs.login()
    print(f"    login.error_code={lg.error_code}, error_msg={lg.error_msg}")
    return lg.error_code == "0", lg


def test_query_all_stock_basic(login_ok):
    if not login_ok:
        return False, None
    rs = bs.query_stock_basic()  # 不传参 = 全部证券
    print(f"    query_stock_basic() error_code={rs.error_code}, error_msg={rs.error_msg}")
    print(f"    fields={rs.fields}")
    if rs.error_code != "0":
        return False, None
    rows = []
    while rs.next():
        rows.append(rs.get_row_data())
    print(f"    返回条数: {len(rows)}")
    if rows:
        print(f"    首条: {rows[0]}")
        print(f"    末条: {rows[-1]}")
    return len(rows) > 0, rows


def test_query_one_stock_basic(login_ok):
    if not login_ok:
        return False, None
    rs = bs.query_stock_basic(code="sh.600000")
    print(f"    query_stock_basic(code='sh.600000') error_code={rs.error_code}, error_msg={rs.error_msg}")
    if rs.error_code != "0":
        return False, None
    rows = []
    while rs.next():
        rows.append(rs.get_row_data())
    print(f"    返回条数: {len(rows)}, 数据: {rows}")
    return len(rows) > 0, rows


def test_query_trade_dates(login_ok):
    if not login_ok:
        return False, None
    rs = bs.query_trade_dates(start_date="2026-01-01", end_date="2026-01-31")
    print(f"    query_trade_dates(2026-01) error_code={rs.error_code}, error_msg={rs.error_msg}")
    print(f"    fields={rs.fields}")
    if rs.error_code != "0":
        return False, None
    rows = []
    while rs.next():
        rows.append(rs.get_row_data())
    print(f"    返回条数: {len(rows)}")
    if rows:
        print(f"    首条: {rows[0]}")
    return len(rows) > 0, rows


def test_query_profit_data(login_ok):
    if not login_ok:
        return False, None
    rs = bs.query_profit_data(code="sh.600000", year=2025, quarter=1)
    print(f"    query_profit_data(sh.600000, 2025Q1) error_code={rs.error_code}, error_msg={rs.error_msg}")
    print(f"    fields={rs.fields}")
    if rs.error_code != "0":
        return False, None
    rows = []
    while rs.next():
        rows.append(rs.get_row_data())
    print(f"    返回条数: {len(rows)}")
    if rows:
        print(f"    首条: {rows[0]}")
    return True, rows


def main():
    print(f"baostock 版本: {getattr(bs, '__version__', '未知')}")
    print(f"Python: {sys.version.split()[0]}")

    login_ok, _ = step("1) login", test_login, depend_ok=True)

    step("2) query_stock_basic() 全部证券",
         lambda: test_query_all_stock_basic(login_ok), depend_ok=login_ok)

    step("3) query_stock_basic(code='sh.600000') 单只",
         lambda: test_query_one_stock_basic(login_ok), depend_ok=login_ok)

    step("4) query_trade_dates(2026-01) 交易日历",
         lambda: test_query_trade_dates(login_ok), depend_ok=login_ok)

    step("5) query_profit_data(sh.600000, 2025Q1) 基本面",
         lambda: test_query_profit_data(login_ok), depend_ok=login_ok)

    print("\n>>> logout")
    try:
        bs.logout()
        print("    ok")
    except Exception as e:
        print(f"    异常: {e}")


if __name__ == "__main__":
    main()
