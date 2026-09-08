# XDXR 协议参考（pytdx 逆向，供 tdx-api Go spike 使用）

来源：`/opt/anaconda3/lib/python3.12/site-packages/pytdx/parser/get_xdxr_info.py`

## 请求
```
pkg = bytes.fromhex('0c1f187600010b000b000f000100')
pkg += struct.pack("<B6s", market, code)   # market: 1字节, code: 6字节ASCII
```
- 固定头 14 字节，其中偏移 10-11 的 `0f 00` 即功能号 0x000f（KMSG_XDXRINFO）
- injoyai/tdx 实现：Frame Type=0x000f，payload=`0b 00 0b 00 0f 00 01 00` + market + code（对齐待 spike 实测）

## 响应
- 跳过前 9 字节 → uint16 LE 记录数 num
- 每条记录：market+code(7B) → 跳 1B → 日期时间(9 字节模式) → category(1B) → 16B 负载
- 负载按 category：
  - **1=除权除息**：fenhong(分红)、peigujia(配股价)、songzhuangu(送转股)、peigu(配股) = `<ffff`
  - 11/12=扩缩股/缩股：suogu
  - 13/14=权证：xingquanjia、fenshu
  - 其他（2-10）：panqianliutong/qianzongguben/panhouliutong/houzongguben = `<IIII`（经 get_volume 解码）

## 事件分类
1 除权除息 / 2 送配股上市 / 3 非流通股上市 / 4 未知股本变动 / 5 股本变化 / 6 增发新股 / 7 股份回购 / 8 增发新股上市 / 9 转配股上市 / 10 可转债上市 / 11 扩缩股 / 12 非流通股缩股 / 13 送认购权证 / 14 送认沽权证

## 缓存失效判定（M-B 用）
- 仅 category=1（除权除息）且事件日 ≤ 当日 → 缓存失效
- category=5/6（股本变化/增发）可作为 float_share 刷新信号（增强项）
