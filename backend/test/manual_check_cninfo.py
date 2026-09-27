# 手工验证脚本：巨潮资讯网 财报PDF采集 链路检查（非 pytest 用例，顶层直接执行）
# 说明：
#   1. 拉取 code->orgId 映射（szse_stock.json），确认 A股/退市股/北交所覆盖
#   2. hisAnnouncement/query 按股票+四类定期报告检索，锁定参数与分页行为
#   3. 标题解析（报告类型/报告期）演示
#   4. 下载 1 份 PDF 到 tmp 并校验（%PDF 文件头 + 大小）
# 运行：cd backend && ../.venv/bin/python test/manual_check_cninfo.py

import json
import sys
import time
from pathlib import Path

import requests

QUERY_URL = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'
STOCK_MAP_URL = 'http://www.cninfo.com.cn/new/data/szse_stock.json'
STATIC_URL = 'http://static.cninfo.com.cn/'

CATEGORIES = 'category_ndbg_szsh;category_bndbg_szsh;category_yjdbg_szsh;category_sjdbg_szsh'
HEADERS = {
    'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'),
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
    'Referer': 'http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search',
    'Origin': 'http://www.cninfo.com.cn',
}


def post_query(payload):
    resp = requests.post(QUERY_URL, data=payload, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def query_announcements(code, org_id, column, se_date, plate='', page_num=1, page_size=30):
    payload = {
        'pageNum': page_num,
        'pageSize': page_size,
        'column': column,
        'tabName': 'fulltext',
        'plate': plate,
        'stock': f'{code},{org_id}',
        'searchkey': '',
        'secid': '',
        'category': CATEGORIES,
        'trade': '',
        'seDate': se_date,
        'sortName': '',
        'sortType': '',
        'isHLtitle': 'true',
    }
    return post_query(payload)


def parse_title(title):
    """标题 -> (报告期年份, NN)；NN: 01一季报 02半年报 03三季报 04年报；解析失败返回 None"""
    import re
    t = title.replace('（', '(').replace('）', ')')
    m = re.search(r'(\d{4})年年度报告', t)
    if m:
        return int(m.group(1)), '04'
    m = re.search(r'(\d{4})年半年度报告', t)
    if m:
        return int(m.group(1)), '02'
    m = re.search(r'(\d{4})年中期报告', t)
    if m:
        return int(m.group(1)), '02'
    m = re.search(r'(\d{4})年第[一1]季度报告', t)
    if m:
        return int(m.group(1)), '01'
    m = re.search(r'(\d{4})年一季度报告', t)
    if m:
        return int(m.group(1)), '01'
    m = re.search(r'(\d{4})年第[三3]季度报告', t)
    if m:
        return int(m.group(1)), '03'
    m = re.search(r'(\d{4})年三季度报告', t)
    if m:
        return int(m.group(1)), '03'
    return None


def main():
    print('=' * 70)
    print('步骤1：拉取 code->orgId 映射')
    resp = requests.get(STOCK_MAP_URL, headers={'User-Agent': HEADERS['User-Agent']}, timeout=60)
    resp.raise_for_status()
    stock_list = resp.json().get('stockList', [])
    print(f'  映射总条数: {len(stock_list)}')
    by_category = {}
    for s in stock_list:
        by_category[s.get('category', '?')] = by_category.get(s.get('category', '?'), 0) + 1
    print(f'  category 分布: {by_category}')
    code_map = {s['code']: s for s in stock_list}
    for probe in ['600519', '000003', '600001', '600003', '600625', '835185', '832566']:
        info = code_map.get(probe)
        print(f'  {probe}: {info}')

    print('=' * 70)
    print('步骤2：hisAnnouncement/query 沪市主板 600519（近端窗口 2025-01-01~今天）')
    mt = code_map['600519']
    data = query_announcements('600519', mt['orgId'], 'sse', '2025-01-01~2026-12-31')
    print(f'  响应键: {sorted(data.keys())}')
    print(f'  totalAnnouncement={data.get("totalAnnouncement")} totalpages={data.get("totalpages")} '
          f'hasMore={data.get("hasMore")}')
    anns = data.get('announcements') or []
    anns_main = anns  # 步骤4会覆盖 anns，下载步骤用保留副本
    if anns:
        print(f'  单条公告完整字段: {json.dumps(anns[0], ensure_ascii=False)}')
    for a in anns:
        title = a.get('announcementTitle', '')
        print(f"  [{a.get('secCode')}] {title} | {a.get('adjunctUrl')} | "
              f"解析={parse_title(title)}")

    print('=' * 70)
    print('步骤3：北交所 835185（贝特瑞）column 组合探测')
    br = code_map.get('835185')
    if br:
        for column, plate in [('szse', ''), ('szse', 'bj'), ('sse', '')]:
            try:
                d = query_announcements('835185', br['orgId'], column, '2025-01-01~2026-12-31', plate=plate)
                n = d.get('totalAnnouncement')
                titles = [a.get('announcementTitle', '') for a in (d.get('announcements') or [])][:3]
                print(f'  column={column!r} plate={plate!r}: totalAnnouncement={n} 示例={titles}')
            except Exception as e:
                print(f'  column={column!r} plate={plate!r}: 异常 {e}')
            time.sleep(0.5)

    print('=' * 70)
    print('步骤4：退市股探测（000003/600001/600003/600625 任一存在映射即试查 2001 年窗口）')
    for code in ['000003', '600001', '600003', '600625']:
        info = code_map.get(code)
        if not info:
            print(f'  {code}: 映射中无此代码')
            continue
        column = 'sse' if code.startswith('6') else 'szse'
        try:
            d = query_announcements(code, info['orgId'], column, '2001-01-01~2002-12-31')
            anns = d.get('announcements') or []
            print(f"  {code}({info.get('zwjc')}): totalAnnouncement={d.get('totalAnnouncement')} "
                  f"示例={[a.get('announcementTitle') for a in anns[:3]]}")
        except Exception as e:
            print(f'  {code}: 异常 {e}')
        time.sleep(0.5)

    print('=' * 70)
    print('步骤5：分页行为验证（600519 长窗口 2001~今天，翻到第2页）')
    d1 = query_announcements('600519', mt['orgId'], 'sse', '2001-01-01~2026-12-31', page_num=1)
    total_pages = d1.get('totalpages')
    print(f'  第1页: totalAnnouncement={d1.get("totalAnnouncement")} totalpages={total_pages} '
          f'本页条数={len(d1.get("announcements") or [])}')
    if total_pages and int(total_pages) >= 2:
        d2 = query_announcements('600519', mt['orgId'], 'sse', '2001-01-01~2026-12-31', page_num=2)
        anns2 = d2.get('announcements') or []
        print(f'  第2页: 本页条数={len(anns2)} 首条标题={anns2[0].get("announcementTitle") if anns2 else "无"}')

    print('=' * 70)
    print('步骤6：下载 1 份 PDF 校验（取步骤2中的年报正文）')
    target = None
    for a in anns_main:
        title = a.get('announcementTitle', '')
        if parse_title(title) and '摘要' not in title and '英文' not in title and parse_title(title)[1] == '04':
            target = a
            break
    if not target and anns:
        target = anns[0]
    if target:
        url = STATIC_URL + target['adjunctUrl']
        print(f'  下载: {url}')
        dl = requests.get(url, headers={'User-Agent': HEADERS['User-Agent'],
                                        'Referer': 'http://www.cninfo.com.cn/'},
                          stream=True, timeout=60)
        dl.raise_for_status()
        out = Path(__file__).resolve().parents[2] / 'tmp' / 'manual_cninfo_check.pdf'
        out.parent.mkdir(parents=True, exist_ok=True)
        size = 0
        with open(out, 'wb') as f:
            for chunk in dl.iter_content(chunk_size=65536):
                f.write(chunk)
                size += len(chunk)
        head = open(out, 'rb').read(5)
        print(f'  Content-Length 响应头: {dl.headers.get("Content-Length")} 实落盘: {size} 字节')
        print(f'  文件头: {head!r} -> {"✓ 合法PDF" if head == b"%PDF-" else "✗ 非法"}')
        print(f'  落盘: {out}')
    else:
        print('  无可下载目标')


if __name__ == '__main__':
    main()
