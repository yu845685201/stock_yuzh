# XDXR 协议参考（pytdx 逆向 + tdx-api Go 实测）

> 状态：**2026-09-09 spike 已通过**。Go 实现（`protocol/model_xdxr.go`）与 pytdx 逐条对照 4 只股票 288 条事件全部一致，单次请求 ~35ms。
> 来源：`/opt/anaconda3/lib/python3.12/site-packages/pytdx/parser/get_xdxr_info.py`
> 实测服务器：`115.238.90.165:7709`（pytdx 对照）、`124.71.187.122:7709`（Go 实测，来自 tdx.Hosts）

## 请求

```
pkg = bytes.fromhex('0c1f187600010b000b000f000100')
pkg += struct.pack("<B6s", market, code)   # market: 1字节, code: 6字节ASCII
```

- 固定头 14 字节，其中偏移 10-11 的 `0f 00` 即功能号 0x000f（KMSG_XDXRINFO）
- 对应 tdx-api `Frame{Control: Control01, Type: TypeXdxr(0x000f), Data: {0x01, 0x00, market, code6}}`
- 帧长字段 `0b00` = 11 = 数据域 9 字节 + 2

## 响应（**实测确认，已修正历史记载**）

- 跳过前 9 字节 → `uint16 LE` 记录数 num（总长 <11 时返回空列表，与 pytdx 一致）
- 每条记录 **29 字节**：`market+code(7B)` → 跳 1B → **日期 4B（uint32 LE，YYYYMMDD 整型）** → `category(1B)` → 16B 负载

> ⚠️ 历史记载"日期时间(9 字节模式)"是**误导**：pytdx `get_datetime(9, buf, pos)` 中的 `9` 是 `get_datetime` 的**模式选择参数**（≥4 时走 4 字节整型日期分支），不是字节数。实测 date 字段为 **4 字节 uint32 LE**，`year=zipday/10000`、`month=(zipday%10000)/100`、`day=zipday%100`。

- pytdx 解析时的一个既有 bug（Go 实现已修正）：它每条记录都从 `body_buf[:7]` 读 market/code，只有首条正确；tdx-api Go 实现按 pos 正常推进。

## 负载按 category

| category | 含义 | 负载格式 | 字段顺序 |
|---|---|---|---|
| 1 | 除权除息 | `<ffff` | fenhong(分红)、peigujia(配股价)、songzhuangu(送转股)、peigu(配股) |
| 11 / 12 | 扩缩股 / 非流通股缩股 | `<IIfI` | suogu 取第 3 个（float） |
| 13 / 14 | 送认购权证 / 送认沽权证 | `<fIfI` | xingquanjia(第1, float)、fenshu(第3, float) |
| 2-10 | 股本变动类 | `<IIII` | panqianliutong、qianzongguben、panhouliutong、houzongguben（经 `get_volume` 解码；原始值 0 时取 0） |

注意 payload 中股本字段顺序是"盘前流通、前总股本、盘后流通、后总股本"，不是前后配对顺序。

## 事件分类（14 类）

1 除权除息 / 2 送配股上市 / 3 非流通股上市 / 4 未知股本变动 / 5 股本变化 / 6 增发新股 / 7 股份回购 / 8 增发新股上市 / 9 转配股上市 / 10 可转债上市 / 11 扩缩股 / 12 非流通股缩股 / 13 送认购权证 / 14 送认沽权证

## 实测样本（2026-09-09）

| 股票 | 事件数 | 分类分布 | 耗时 |
|------|--------|----------|------|
| sz.000001 | 80 | 1:32 2:10 3:3 5:32 9:3 | 35ms |
| sh.600519 | 45 | 1:30 2:7 3:1 5:5 9:1 14:1 | 34ms |
| sz.300750 | 75 | 1:11 5:63 9:1 | 35ms |
| sh.600000 | 88 | 1:27 2:4 3:1 5:51 6:1 8:2 9:2 | 37ms |

## 缓存失效判定（M-B 用）

- 仅 `category=1`（除权除息）且**事件日 ≤ 当日** → 缓存失效（实现见 `protocol.XdxrResp.ExDividendSince(anchor, today)`）
- 公告在前、除权在后的"未来事件"不提前失效，避免公告日至除权日间每天无谓全量重拉
- `category=5/6`（股本变化/增发）可作为 float_share 刷新信号（增强项，未实现）
