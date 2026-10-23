# AShare-AI-Stock-Picker（stlin256）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：[github.com/stlin256/AShare-AI-Stock-Picker](https://github.com/stlin256/AShare-AI-Stock-Picker) · **分析依据**：本地克隆源码（`refrences/stlin256__AShare-AI-Stock-Picker`，git commit `6b296a8` 2025-05-15，v1.0）· **配套**：`../开源股票分析项目清单-20261010.md`
> **方法**：精读 `main.py`、`config/config.py`、`data/data_fetcher.py`、`features/feature_engineering.py`、`models/model_trainer.py` 与 README。

---

## 0 定位与使用形态

AShare-AI-Stock-Picker 是一个**基于 LightGBM 的 A 股「未来涨跌预测」选股系统**：对每只股票训练一个独立的 LightGBM 二分类模型，预测「未来 5 日收益率是否为正」，按概率打分评级（A/B/C）导出 Excel。

- 形态：CLI 单文件入口 `python main.py`，交互式选择是否加载缓存模型，逐股训练→预测→汇总出 Excel。
- 技术栈：Python 3.12（README 要求）；`akshare`（取数）、`talib`（指标）、`lightgbm`、`optuna`（调参）、`sklearn`、`openpyxl`（导出）。⚠️ `requirements.txt` 只列了 7 个（akshare/lightgbm/numpy/optuna/pandas/scikit_learn/tqdm），**缺 `talib` 和 `openpyxl`**（`talib` 靠仓库附带的 `TA_Lib-0.4.28-cp312-cp312-win_amd64.whl` 装）。
- 定位：个人学习 demo，README 双语（中英），代码体量极小（4 个核心 py）。

---

## 1 功能模块全景

| 模块 | 位置 | 职责 |
|---|---|---|
| 主程序 | `main.py` | 拉股票列表→过滤→逐股训练/加载→预测→评级→Excel |
| 配置 | `config/config.py` | `FEATURE_COLS`、`TARGET_DAYS`、`MODEL_DIR` |
| 取数 | `data/data_fetcher.py` | akshare 历史日线 + 调特征工程 |
| 特征工程 | `features/feature_engineering.py` | talib 算 7 项技术指标 + 目标 |
| 模型训练 | `models/model_trainer.py` | Optuna 调参 + LightGBM 训练/缓存 |
| 模型缓存 | `data/stock_models/*_model.txt` | 每票一个 LightGBM 文本模型 |

---

## 2 数据源来源与采集机制

| 源 | 接口 | 内容 | 预处理 |
|---|---|---|---|
| akshare | `ak.stock_zh_a_hist(symbol, period="daily", adjust="qfq")` | 单票全量日线（前复权） | 列名中→英（日期/开盘/收盘/最高/最低/成交量/涨跌幅），`date` 设索引 |
| akshare | `ak.stock_zh_a_spot_em()` | 全市场实时列表（代码/名称/最新价/涨跌幅） | 代码 `zfill(6)`，过滤 ST/退/300/688/8 |

**过滤规则**（`main.py`）：
- `~name.contains('ST|退')`（ST/*ST、退市）；
- `~code.startswith(('300','688','8'))`（创业 300、科创 688、北交所 8 全部排除——只留主板）。
- 数据不足 500 日的股票跳过（`len(df) < 500`）。

**缓存**：`{code}_model.txt` 每票一个模型文件；`main.py` 按最新 `mtime` 显示缓存更新时间；`force_retrain` 强制重训。

---

## 3 分析逻辑

### 3.1 特征工程（`feature_engineering.py`）

`FEATURE_COLS`（10 个）：
```
close, volume, pct_chg,           # 原始
EMA20_ratio = close / EMA(close,20),
RSI14 = RSI(close,14),
MACD = talib.MACD 的 DIF 线(12/26/9),
OBV, CCI(14), ATR(14), ADX(14)
```

**预测目标**：
```python
df['pct_chg_5d'] = df['close'].pct_change(periods=5).shift(-5)   # 未来5日收益率
y = (df['pct_chg_5d'] > 0).astype(int)                            # 二分类：涨=1 跌=0
```
- ⚠️ `config.py` 里 `TARGET_DAYS = 2` 是**死配置**——特征工程硬编码 `pct_chg_5d`（5 日）与 `shift(-5)`，`TARGET_DAYS` 未被任何代码引用；README 也写「5 日」。以代码为准是 **5 日**。

### 3.2 模型训练（`model_trainer.py`）—— 每票一个模型

- **per-stock 建模**：`train_and_save_model(code)` 对**每只股票**单独训练一个 LightGBM（不是全市场横截面建模）——这是本项目最核心的设计决策，也是「8C16T 跑 5 小时」的原因（每票都要跑一遍 50 次 Optuna 试验）。
- **Optuna 超参**（`optimize_hyperparameters`）：`n_trials=50`，搜索空间 `n_estimators`[100,500]、`learning_rate`[1e-3,0.3 log]、`max_depth`[3,9]、`subsample/colsample_bytree`[0.6,1.0]、`reg_alpha/reg_lambda`[1e-8,10]，`random_state=42`；目标 `maximize` 3 折 `TimeSeriesSplit`（时序切分，避免随机泄露）的 `roc_auc_score` 均值。
- **最终训练**：拿到 `best_params` 后对**全量** X 再 `fit` 一遍，`booster_.save_model` 存 `{code}_model.txt`。
- **数据切分**：`split_idx = 0.8` 时间顺序切训练/验证（用于调参），无独立测试集。
- 训练失败/数据不足返回 None 静默跳过。

### 3.3 预测与评级（`main.py`）

- 逐票：加载缓存模型（或现训）→ 取最新一根特征 → `booster.predict(latest_features)` 得上涨概率 `prob`。
- **评级**：`pd.cut(prob, bins=[0, 0.6, 0.75, 1], labels=['C','B','A'])`——`A≥0.75`、`0.6≤B<0.75`、`C<0.6`（与 README 一致）。
- **是否涨停**：`latest_pct >= 9.9`（统一 9.9%，由于 300/688/8 已被过滤，只剩主板 10%，但 **ST 5% 未单独处理**，过滤里只按名称含「ST」排掉了）。
- 结果字段：`代码/名称/是否涨停/预测概率/推荐评级/收盘价/更新日期`。

---

## 4 最终产物结构与格式

- **Excel**：`stock_recommend_YYYYMMDD.xlsx`（openpyxl），列为 `代码、名称、是否涨停、预测概率、推荐评级、收盘价、更新日期`，全市场预测结果一行一票。
- **控制台**：`result_df.head(10)` 打印前 10 行；无有效数据时提示「未找到有效股票数据，请检查数据源或过滤条件」。
- **模型缓存**：`data/stock_models/{code}_model.txt`（LightGBM 文本模型，可跨次复用）。

---

## 5 坑与注意事项（源码发现）

1. **`TARGET_DAYS=2` 是死配置**：`config.py` 声明 `TARGET_DAYS=2`，但特征工程硬编码 `pct_chg_5d`（5 日），该常量未被引用。想改预测周期只能改 `feature_engineering.py`，改 config 无效。
2. **依赖清单不完整**：`requirements.txt` 缺 `talib`（需另装仓库附带的 whl）与 `openpyxl`（导出会 ImportError）；README 还笔误写 `pip install requirements.txt -r`（正确是 `pip install -r requirements.txt`）。
3. **每票独立模型 = 5 小时训练**：README 明示「首次运行耗时约 5h（8C16T R9-7940HS）」。逐票 Optuna×50 是 O(5000票) 的暴力方案，无增量/共享；缓存模型后运行才快。
4. **大概率存在过拟合**：单票只有约 500+ 天样本、10 个特征，靠 Optuna 50 次调 8 个超参，容易在单票上过拟合；且调参用 3 折 TimeSeriesSplit 后又在全量数据重训，验证集结论与实际模型不完全一致。
5. **无独立测试集评估**：`X_valid` 只参与 Optuna 打分，最终模型用全量训练、无 holdout 报告，输出只有「预测概率」，用户无法判断模型真实 AUC。
6. **涨停判定一刀切 9.9%**：不区分 ST（5%）——虽然 ST 名称被过滤，但散户常改名的边缘股可能漏判；北交所/创业/科创已被整体排除，等于放弃了 20cm/30cm 标的。
7. **「是否涨停」字段语义漂移**：字段名是「是否涨停」，实际判断的只是「当日涨幅 ≥9.9%」，未核实收盘是否真封涨停价（差一分钱也算「涨停」）。
8. **目标存在未来数据窗口收缩**：`pct_chg_5d` 末尾 5 行必然为 NaN，`dropna` 后训练集少 5 行——正常，但预测用的「最新一根特征」其实与「最新标签」在时间上错位，逻辑上无误但需理解。
9. **`n_trials=50` 对每票都跑**：即便 `load_cache` 命中，新股票/缓存失效仍会触发全量 Optuna，单票也要几十秒到几分钟。

---

## 6 可借鉴的设计要点

1. **「预测概率 → 分级推荐」的产物设计**：把二分类概率映射成 A/B/C 三档（0.75/0.6 阈值），比裸概率更贴近用户决策，且阈值用 `pd.cut` 一行实现、易改。
2. **时序切分（TimeSeriesSplit）+ 目标最大化 AUC**：调参阶段用时间顺序切分而非随机切分，避免「用未来特征预测过去」的泄露，是时间序列建模的最小正确姿势。
3. **模型缓存 + mtime 提示**：`{code}_model.txt` 按票缓存、启动时显示最新缓存时间、`force_retrain` 开关，兼顾「跑得快」和「可强制重训」。
4. **过滤集中写在入口**：ST/退/300/688/8 的过滤一行 `str.startswith` + `str.contains` 搞定，README 明确「可在 main.py 调整」，可读性好。
5. **失败静默跳过 + 进度条**：逐票 try/except 不中断全市场跑批，`tqdm` 实时显示「正在处理：code」，是批量预测的工程底子。

---

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `main.py` | 入口：过滤/逐股训练或加载/预测/评级/Excel |
| `config/config.py` | FEATURE_COLS、TARGET_DAYS(死配置)、MODEL_DIR |
| `data/data_fetcher.py` | akshare 取数 + 列名规范化 |
| `features/feature_engineering.py` | talib 7 项指标 + `pct_chg_5d` 目标 |
| `models/model_trainer.py` | Optuna 调参 + LightGBM 训练 + 缓存 |
| `README.md` | 双语说明、A/B/C 评级、输出样例 |
| `TA_Lib-0.4.28-cp312-cp312-win_amd64.whl` | 附带的 talib Windows 轮子 |