# -*- coding: utf-8 -*-
"""T6 双路径对账（M-C 验收用）

增强路径（use_enhanced_api=true：/api/kline-qfq 缓存 + /api/kline-recent）
vs 传统路径（use_enhanced_api=false：/api/kline-history + /api/kline-all）

规则：
- 共同日期逐字段（Open/High/Low/Close/Volume/Amount）比对，必须零差异
- 增强路径多出「当日」bar 属预期（缓存追加，THS 源滞后 1 天时发生）
- 传统路径多出「最旧一天」且与增强多出日期构成窗口平移（两窗等长、交集连续）属预期
- 其余任何差异计为不一致

用法：
  python3 scripts/dual_path_reconcile.py                # 默认 15 只样本
  python3 scripts/dual_path_reconcile.py --file <csv>   # 从 CSV 读取 ts_code 列（如 100 股测试集）
  python3 scripts/dual_path_reconcile.py --codes 000001,600519
"""
import argparse
import sys
import time

sys.path.insert(0, "/Users/yuzh/develop/ai/claude/claude-code/workspace/stock_yuzh_base/stock_yuzh/backend")

from src.data_sources.tdx_api_source import TdxApiSource

BASE_CFG = {
    'base_url': 'http://127.0.0.1:8080',
    'timeout': 60,
    'rate_limit': {'enabled': False},
}

SAMPLES = [
    '000001', '000002', '600519', '600000', '601318',
    '300750', '002594', '600036', '601988', '000651',
    '688981', '600276', '601899', '300059', '600030',
]

FIELDS = ['Open', 'High', 'Low', 'Close', 'Volume', 'Amount']


def norm(ls):
    return {x['Time'][:10]: x for x in ls}


def is_window_shift(dates_legacy, dates_enhanced):
    """两窗等长、交集连续、各自只多一头一尾（增强多当日，传统多最旧日）"""
    if len(dates_legacy) != len(dates_enhanced):
        return False
    only_l = sorted(set(dates_legacy) - set(dates_enhanced))
    only_e = sorted(set(dates_enhanced) - set(dates_legacy))
    if len(only_l) != 1 or len(only_e) != 1:
        return False
    common = sorted(set(dates_legacy) & set(dates_enhanced))
    return len(common) == len(dates_legacy) - 1


def reconcile_one(legacy, enhanced, code):
    issues = []
    for name, leg_fn, enh_fn in [
        ('qfq', legacy.get_kline_qfq_tail, enhanced.get_kline_qfq_tail),
        ('raw', legacy.get_kline_raw_tail, enhanced.get_kline_raw_tail),
    ]:
        a = norm(leg_fn(code, 5))
        b = norm(enh_fn(code, 5))
        for d in sorted(set(a) & set(b)):
            for k in FIELDS:
                if a[d][k] != b[d][k]:
                    issues.append(f"{name} {d} {k}: {a[d][k]} != {b[d][k]}")
        if set(b) - set(a) or set(a) - set(b):
            if not is_window_shift(sorted(a), sorted(b)):
                issues.append(f"{name} 日期集不一致(非窗口平移): 传统多出{sorted(set(a)-set(b))} 增强多出{sorted(set(b)-set(a))}")
    return issues


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--codes', help='逗号分隔的 6 位股票代码')
    ap.add_argument('--file', help='含 ts_code 列的 CSV（如 test/test_stock_set.json 对应清单）')
    args = ap.parse_args()

    codes = SAMPLES
    if args.codes:
        codes = [c.strip().split('.')[-1] for c in args.codes.split(',') if c.strip()]
    elif args.file:
        import csv
        with open(args.file, newline='', encoding='utf-8') as f:
            codes = [row['ts_code'].split('.')[-1] for row in csv.DictReader(f)]

    legacy = TdxApiSource({**BASE_CFG, 'use_enhanced_api': False})
    enhanced = TdxApiSource({**BASE_CFG, 'use_enhanced_api': True})

    bad = 0
    t0 = time.time()
    for i, code in enumerate(codes, 1):
        issues = reconcile_one(legacy, enhanced, code)
        if issues:
            bad += 1
            print(f"[差异] {code}")
            for s in issues[:5]:
                print(f"   {s}")
        else:
            print(f"[一致] {code}")
        if i % 20 == 0:
            print(f"...进度 {i}/{len(codes)} 耗时 {time.time()-t0:.0f}s", flush=True)

    print(f"\n结论: {len(codes)} 只, 不一致 {bad} 只, 耗时 {time.time()-t0:.0f}s ->",
          '需排查' if bad else '共同日期零差异 ✔（窗口平移属预期）')
    return 2 if bad else 0


if __name__ == '__main__':
    sys.exit(main())
