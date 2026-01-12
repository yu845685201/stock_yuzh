# 5分钟K线数据采集功能代码逻辑检查报告

## 检查概览
- **检查对象**: `feature/data_sync` 分支下5分钟K线数据采集相关代码
- **主要文件**:
  - `backend/src/sync/sync_manager.py`
  - `backend/src/data_sources/pytdx_source.py`
  - `backend/src/utils/daily_kline_anomaly_detector.py`
  - `backend/src/sync/csv_writer.py`

## 检查结果总结
代码整体架构符合设计，但存在 **3处关键逻辑差异** 和 **1个潜在风险**，建议修复。

---

## 详细差异分析

### 1. 涨跌幅计算逻辑不一致 (严重)
- **产品设计要求**:
  > `change_rate`: (close-preclose)/preclose*100
  > `preclose`: **前一条5分钟数据**的close价格
- **代码实现**:
  - `SyncManager._post_process_5min_data` 方法中，代码逻辑强制获取**昨日收盘价** (`his_kline_day`表中昨日close) 并将其赋值给 `preclose`。
  - 代码片段: `data['preclose'] = yesterday_preclose`
- **问题**:
  - 这里的 `preclose` 变成了昨日收盘价，导致 `change_rate` 计算的是**相对昨日收盘的涨跌幅**（即日内涨跌幅），而不是产品文档要求的**5分钟级别涨跌幅**（Bar-to-Bar）。
  - `PytdxSource` 中原本已经正确计算了 Bar-to-Bar 的 `preclose`，但在 `SyncManager` 中被错误覆盖。

### 2. CSV文件数据源不符合"未经加工"要求 (中等)
- **产品设计要求**:
  > 数据源：ts_code加上.lc5文件中解析出来的**未经任何加工的数据**
- **代码实现**:
  - `SyncManager.sync_5min_data` 实际上是将 `enriched_data` (已加工数据) 传递给了 `csv_writer`。
  - `enriched_data` 包含了 `turnover_rate`, `fundamentals_disclosure_date` 等后续计算和关联字段。
- **对比**:
  - 日K线采集 (`sync_daily_data`) 专门维护了 `all_raw_data` 用于生成CSV，符合要求。
  - 5分钟K线采集未维护独立的原始数据列表。

### 3. ST股票涨跌幅校验逻辑缺失 (中等)
- **产品设计要求**:
  > ST股票涨跌幅限制为5.1%
- **代码实现**:
  - 异常检测器 `DailyKlineAnomalyDetector._detect_change_rate_anomalies` 依赖 `data.get('is_st')` 字段来判断是否使用 5.1% 的阈值。
  - 在5分钟数据处理流程 (`_enrich_minute_data_with_fundamentals`) 中，虽然关联了 `stock_name`，但**没有计算或设置 `is_st` 字段**。
- **后果**:
  - 检测器会默认 `is_st=False`，导致ST股票被错误地使用 10.1% 或 20.1% 的阈值进行校验，无法检出 5.1%~10.1% 之间的异常涨跌幅。

### 4. 采集范围过滤逻辑 (合规)
- **产品设计要求**:
  - 沪市: `sh60`, `sh688`
  - 深市: `sz0`, `sz300`, `sz301`
- **代码实现**:
  - `SyncManager._is_valid_stock_file` 方法中正确实现了该过滤逻辑。
  - 检查逻辑: `stock_code.startswith('60')` 等，符合设计。

---

## 修复建议

### 1. 修正Preclose及涨跌幅逻辑
修改 `SyncManager._post_process_5min_data`：
- **方案A (遵循文档)**: 删除覆盖 `preclose` 的逻辑，保留 `PytdxSource` 返回的 Bar-to-Bar 的 `preclose`。
- **方案B (如果意图是日内涨幅)**: 修改产品文档。
- **建议**: 采纳方案A。5分钟K线的 `preclose` 通常指上一根K线收盘价。

### 2. 完善ST标记
在 `SyncManager._enrich_minute_data_with_fundamentals` 方法中：
- 增加 `is_st` 字段的计算：
  ```python
  from ..utils.data_transformer import DataTransformer
  # ...
  enriched_record['is_st'] = DataTransformer.check_is_st(enriched_record['stock_name'])
  ```

### 3. 调整CSV输出数据源
在 `SyncManager.sync_5min_data` (及 `_process_5min_data_batch`) 中：
- 在数据 enrich 之前，先深拷贝一份 `raw_data` (仅含.lc5解析字段 + ts_code)。
- 将这份 `raw_data` 传递给 `csv_writer.write_his_kline_5min`。

## 任务清单
1. [ ] 修复 `SyncManager._post_process_5min_data` 中的 `preclose` 覆盖问题
2. [ ] 在 `SyncManager._enrich_minute_data_with_fundamentals` 中添加 `is_st` 字段
3. [ ] 修改 `SyncManager` 确保 5分钟CSV 使用原始数据
