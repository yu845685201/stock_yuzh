"""
通达信官网盘后包数据源（P0-2 方案 B，复盘报告层与 P0 修复技术方案 §2.3/§2.4）

用途：北交所日K唯一入口（T1 探路 2026-09-30 实测：tdx-api 中间件 /api/kline-all 与
/api/xdxr 支持 920xxx，但 pipeline 主链路依赖的 /api/kline-all/ths 前复权与
/api/ths-factor 对 920982/920185 返回 THS 上游 HTML 错误页（2/3 样本失败）→
方案 A「北交所走既有 per-stock 前复权链路」不成立）；同时产出全市场收盘对拍源
（盘后包覆盖沪深北，用于交叉校验库内沪深日K，见 bj_kline_manager）。

接口事实（doc/reference/a-stock-data/SKILL.md §1.3 引入；2026-09-30 本机复测一致）：
- URL：https://www.tdx.com.cn/products/data/data/g4day/{ymd}.zip（ymd=yyyyMMdd，约 2.7MB）
- 结构：每市场一对文件 {sh|sz|bj}{yyMMdd}.cod（代码表，150 字节/条）+ .md1（行情块，512 字节/块）
- 二进制布局（小端）：
    cod 条目 code=record[0:6]（ASCII 6 位数字）、seq=<H record[32:34]（对应 md1 块号）、
            name=record[40:72]（GBK，NUL 结尾）
    md1 块   prev_close=<d [4:12]、open/high/low/close=<4d [12:44]、
            volume=<Q [56:64]、amount=<d [72:80]
- 单位：volume 股、amount 元（2026-09-29 包实测：平安银行 close=11.35 与库内 raw_close 全等，
  5 只样本对拍 diff=0，volume/amount 仅舍入级差异）
- 坑记录：
  * close<=0 的行是通达信自编板块指数（880/881 等）非证券，跳过；
  * 北交所文件自 2022-05-06 起（此前包无 bj 文件，不报错；此后缺 bj 文件按格式改变报错）；
  * 非交易日/当日未发布（通常收盘后数小时）官网返回 404 → 抛 ValueError（不返回空表）；
    2026-09-30 实测 20260925（中秋节）404、0924/0928/0929 均 200，官网保留范围较长；
  * 校验一律「宁抛错不返回空表」，格式残缺抛 RuntimeError（防北交所静默缺失/静默错数）。
- 名称含 *ST 形态（实测 *ST康乐 920575）→ is_st 由 DAO 派生自动覆盖（P0-1 单一出口）。

安全红线（沿用 http_collector_base 文件头）：串行限速（review_sync.tdx_official_interval，
每交易日仅 1 请求）、正常 UA、不破解任何技术措施、风控识别即退避冷却、仅自用不分发。
"""

import io
import logging
import struct
import zipfile
from typing import Any, Dict, List, Optional, Tuple

from .http_collector_base import HttpCollectorBase, ReviewBlockedError

logger = logging.getLogger(__name__)

PACKAGE_URL = 'https://www.tdx.com.cn/products/data/data/g4day/{ymd}.zip'

#: 北交所文件起始日（此前包无 bj 文件）
BSE_PACKAGE_START = '20220506'

#: cod 条目长度 / md1 块长度（字节）
COD_RECORD_SIZE = 150
MD1_BLOCK_SIZE = 512

#: 逐市场有价（close>0）行数下限（11 个真实包实测最小值，防北交所静默缺失）
MARKET_MIN_PRICED_ROWS = {'sh': 10000, 'sz': 3000, 'bj': 50}

_MARKETS = ('sh', 'sz', 'bj')


class TdxOfficialSource(HttpCollectorBase):
    """通达信官网盘后包：单方法 fetch_daily_package（每交易日 1 请求）"""

    def __init__(self, config: Dict[str, Any]):
        review = (config or {}).get('review_sync', {}) or {}
        super().__init__(
            config, 'tdx_official',
            interval_seconds=float(review.get('tdx_official_interval', 2.0)),
            referer='https://www.tdx.com.cn/',
            blocked_error_class=TdxOfficialBlockedError,
        )

    def fetch_daily_package(self, ymd: str) -> List[Dict[str, Any]]:
        """下载并解析指定交易日盘后包 → 沪深北全量行（含指数行已剔除）。

        Returns:
            行 dict 列表：{market, code, name, prev_close, open, high, low,
            close, volume, amount}（volume 股 / amount 元，价格不复权）。

        Raises:
            ValueError: 非交易日/当日未发布（HTTP 404）。
            RuntimeError: 包结构残缺或校验不过（宁抛错不返回空表）。
        """
        ymd = str(ymd).replace('-', '')
        if len(ymd) != 8 or not ymd.isdigit():
            raise ValueError(f'非法交易日: {ymd}，应为 yyyyMMdd')
        resp = self._request('GET', PACKAGE_URL.format(ymd=ymd),
                             expect_json=False, allow_status=(404,))
        if resp.status_code == 404:
            raise ValueError(
                f'盘后包 {ymd} 未发布（HTTP 404；非交易日或当日尚未出包，'
                '通常收盘后数小时发布）')
        return parse_package(resp.content, ymd)


class TdxOfficialBlockedError(ReviewBlockedError):
    """盘后包源风控/不可用异常（4xx 非 404 等；404 属"未发布"走 ValueError）"""


# ---------------------------------------------------------------- 解析（纯函数）

def parse_package(zip_bytes: bytes, ymd: str) -> List[Dict[str, Any]]:
    """盘后包 zip 字节 → 沪深北全量行。格式残缺抛 RuntimeError。"""
    try:
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
    except zipfile.BadZipFile as e:
        raise RuntimeError(f'盘后包 {ymd} 不是合法 zip: {e}') from e

    with zf:
        names = zf.namelist()
        yyMMdd = ymd[2:]
        files: Dict[str, Tuple[bytes, bytes]] = {}
        for market in _MARKETS:
            cod_name = f'{market}{yyMMdd}.cod'
            md1_name = f'{market}{yyMMdd}.md1'
            cod = zf.read(cod_name) if cod_name in names else None
            md1 = zf.read(md1_name) if md1_name in names else None
            if cod is not None and md1 is not None:
                files[market] = (cod, md1)

    if 'sh' not in files or 'sz' not in files:
        raise RuntimeError(f'盘后包 {ymd} 缺沪深主文件（现有: {sorted(names)}）')
    if 'bj' not in files:
        # 北交所文件自 2022-05-06 起；此前缺 bj 是正常形态，此后缺 bj 按格式改变报错
        if ymd >= BSE_PACKAGE_START:
            raise RuntimeError(f'盘后包 {ymd} 缺北交所文件（2022-05-06 起应存在，'
                               f'现有: {sorted(names)}）')
        logger.info(f'盘后包 {ymd} 无北交所文件（2022-05-06 起才有，属正常）')

    rows: List[Dict[str, Any]] = []
    for market, (cod, md1) in files.items():
        rows.extend(parse_market(cod, md1, market, ymd))
    return rows


def parse_market(cod: bytes, md1: bytes, market: str, ymd: str,
                 min_priced_rows: Optional[int] = None) -> List[Dict[str, Any]]:
    """单市场 cod+md1 字节 → 行 dict 列表（指数行剔除，校验宁抛错不返回空表）。

    ``min_priced_rows`` 缺省取 :data:`MARKET_MIN_PRICED_ROWS` 表（真实包口径）；
    单测可显式传入以绕开真实包下限。
    """
    if min_priced_rows is None:
        min_priced_rows = MARKET_MIN_PRICED_ROWS.get(market)
    if len(cod) % COD_RECORD_SIZE != 0:
        raise RuntimeError(f'{market} cod 长度 {len(cod)} 不能整除 {COD_RECORD_SIZE}')
    if len(md1) % MD1_BLOCK_SIZE != 0:
        raise RuntimeError(f'{market} md1 长度 {len(md1)} 不能整除 {MD1_BLOCK_SIZE}')
    n_cod = len(cod) // COD_RECORD_SIZE
    n_md1 = len(md1) // MD1_BLOCK_SIZE
    if n_cod != n_md1:
        raise RuntimeError(f'{market} cod 条数 {n_cod} 与 md1 块数 {n_md1} 不一致')

    codes = _decode_cod(cod, market)
    # md1 块号解析与校验（重复/越界即格式残缺）
    seen_seq: set = set()
    for code, seq, _name in codes:
        if seq in seen_seq:
            raise RuntimeError(f'{market} cod 序号重复: {code} seq={seq}')
        if seq >= n_md1:
            raise RuntimeError(f'{market} cod 序号越界: {code} seq={seq} >= 块数 {n_md1}')
        seen_seq.add(seq)

    rows: List[Dict[str, Any]] = []
    priced = 0
    for code, seq, name in codes:
        blk = md1[seq * MD1_BLOCK_SIZE:(seq + 1) * MD1_BLOCK_SIZE]
        prev_close = struct.unpack_from('<d', blk, 4)[0]
        open_, high, low, close = struct.unpack_from('<4d', blk, 12)
        volume = struct.unpack_from('<Q', blk, 56)[0]
        amount = struct.unpack_from('<d', blk, 72)[0]
        prices = (prev_close, open_, high, low, close, amount)
        if not all(_is_finite(v) for v in prices):
            raise RuntimeError(f'{market} {code} {name} 含非有限数（NaN/Inf，'
                               f'prev_close={prev_close} close={close}）')
        if close <= 0:
            continue  # 通达信自编板块指数（880/881 等）非证券，跳过
        priced += 1
        rows.append({
            'market': market, 'code': code, 'name': name,
            'prev_close': prev_close,
            'open': open_, 'high': high, 'low': low, 'close': close,
            'volume': int(volume), 'amount': amount,
        })

    min_rows = min_priced_rows
    if min_rows and priced < min_rows:
        raise RuntimeError(f'{market} 有价行数 {priced} 低于下限 {min_rows}'
                           '（防静默缺失/解析残缺）')
    return rows


def _decode_cod(cod: bytes, market: str) -> List[Tuple[str, int, str]]:
    """cod 字节 → [(code, seq, name)]；代码非 6 位数字或重复即格式残缺。"""
    out: List[Tuple[str, int, str]] = []
    seen: set = set()
    n = len(cod) // COD_RECORD_SIZE
    for i in range(n):
        rec = cod[i * COD_RECORD_SIZE:(i + 1) * COD_RECORD_SIZE]
        try:
            code = rec[0:6].decode('ascii')
        except UnicodeDecodeError as e:
            raise RuntimeError(f'{market} cod 第 {i} 条代码非 ASCII: {e}') from e
        if not (len(code) == 6 and code.isdigit()):
            raise RuntimeError(f'{market} cod 第 {i} 条代码非 6 位数字: {code!r}')
        if code in seen:
            raise RuntimeError(f'{market} cod 代码重复: {code}')
        seen.add(code)
        seq = struct.unpack_from('<H', rec, 32)[0]
        name = rec[40:72].split(b'\x00')[0].decode('gbk', errors='replace').strip()
        out.append((code, seq, name))
    return out


def _is_finite(v: float) -> bool:
    return v == v and v not in (float('inf'), float('-inf'))
