# 技术指标纯函数库（indicators/）

> 口径来源：`mpquant/MyTT`（V3.3，本地克隆 `refrences/mpquant__MyTT/MyTT.py`），
> **通达信口径为默认**——A 股用户的指标语言来自通达信/同花顺，且本地有可对拍的
> 实现基线。TA-Lib 版（Wilder 递推 ATR、rolling-mean KDJ）作为后续可选第二口径，
> 落地须新开模块参数 `caliber: tdx|talib`，禁静默切换。

## 口径表（公式与 MyTT 逐字对应）

| 指标 | 公式 | MyTT 行号 | warmup | 禁忌 |
|---|---|---|---|---|
| MA | `rolling(N).mean()` | :63 | 60 | — |
| MACD | `DIF=EMA(C,12)-EMA(C,26)`；`DEA=EMA(DIF,9)`；`hist=(DIF-DEA)*2` | :144 | 130 | 柱不乘 2 是境外口径 |
| KDJ | `RSV=(C-LLV(L,9))/(HHV(H,9)-LLV(L,9))*100`；`K=EMA(RSV,5)`；`D=EMA(K,5)`；`J=3K-2D` | :149 | 60 | ta 库 rolling-mean 平滑不同源 |
| RSI | `SMA(MAX(DIF,0),N)/SMA(ABS(DIF),N)*100`，DIF=C-REF(C,1)，默认 N=24（三线 6/12/24） | :154 | 130 | ta 库 ewm 双轨不同源 |
| BOLL | `MID=MA(C,20)`；`UPPER/LOWER=MID±STD(C,20)*2`（STD 为 **ddof=0** 总体标准差） | :169/:43 | 60 | 样本标准差 ddof=1 不同源 |
| ATR | `TR=MAX(MAX(H-L,|REF(C,1)-H|),|REF(C,1)-L|)`；`ATR=MA(TR,20)` | :184 | 60 | **简单平均**，非 Wilder 递推 |
| OBV | `SUM(IF(C>REF(C,1),VOL,IF(C<REF(C,1),-VOL,0)),0)/10000` | :260 | 130 | 万手单位；平盘计 0 |

## 已知口径边界（诚实声明）

1. **warmup 精度**：EMA/SMA 系（MACD/RSI/OBV）按 MyTT 源码注释取 130 周期才与
   通达信数值精确一致（ewm 以首值播种）；短序列的最新值有播种误差——离散状态判定
   （金叉/死叉/超买）不受影响，对外数值列需 130 日回看。
2. **NaN 语义**：分母为 0（KDJ 一字板区间、RSI 全天平盘）输出 NaN，与 MyTT 一致，
   由调用方按「数据不足」处理——禁拿 0/默认值顶替（原则 P3）。
3. **展示层舍入**：MyTT 的 MACD/RSI/BOLL 出口包 RD（3 位）；本库计算层全精度，
   RD 仅展示用（先舍入再平均会对不上）。

## 对拍基线

`test/analysis/test_indicators_parity.py`：同一合成序列（固定 seed，含一字板/
涨跌停/平盘边界日）跑本库与 MyTT，末值容差 0.005（RD 舍入差 ≤0.0005）；
另设真实数据窗口用例（需数据库，skip 不 fail，模式同黄金样本）。
