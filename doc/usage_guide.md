# A股盘后静态分析系统使用指南

## 1. 快速开始

### 1.1 安装依赖

```bash
cd backend
pip install -r requirements.txt
```

### 1.2 初始化系统

```bash
# 初始化系统（包括数据库表）
python -m src.cli.main init --init-db

# 查看系统状态
python -m src.cli.main status
```

### 1.3 执行数据同步

```bash
# 同步所有数据
python -m src.cli.main sync-all

# 仅同步股票列表
python -m src.cli.main sync-stocks

# 同步指定日期的日K线数据
python -m src.cli.main sync-daily --start-date 2024-01-01 --end-date 2024-01-31

# 同步财务数据
python -m src.cli.main sync-financial --year 2023 --quarter 4
```

## 2. 命令详解

### 2.1 init命令 - 初始化系统

```bash
python -m src.cli.main init [OPTIONS]
```

**选项：**
- `--init-db`: 是否初始化数据库表

**功能：**
- 创建必要的数据目录
- 初始化数据库表结构
- 验证系统配置

**示例：**
```bash
# 完整初始化
python -m src.cli.main init --init-db

# 仅创建目录
python -m src.cli.main init
```

### 2.2 sync-all命令 - 同步所有数据

```bash
python -m src.cli.main sync-all [OPTIONS]
```

**选项：**
- `--csv`: 是否保存到CSV文件（默认True）
- `--db`: 是否保存到数据库（默认True）

**功能：**
- 同步股票列表
- 同步日K线数据（最近30天）
- 同步财务数据（最近一个季度）

**示例：**
```bash
# 完整同步
python -m src.cli.main sync-all

# 仅保存到CSV
python -m src.cli.main sync-all --csv --no-db

# 仅保存到数据库
python -m src.cli.main sync-all --no-csv --db
```

### 2.3 sync-stocks命令 - 同步股票列表

```bash
python -m src.cli.main sync-stocks [OPTIONS]
```

**选项：**
- `--csv`: 是否保存到CSV文件（默认True）
- `--db`: 是否保存到数据库（默认True）

**功能：**
- 从所有数据源获取最新股票列表
- 自动去重合并
- 更新股票基本信息

### 2.4 sync-daily命令 - 同步日K线数据

```bash
python -m src.cli.main sync-daily [OPTIONS]
```

**选项：**
- `--csv`: 是否保存到CSV文件（默认True）
- `--db`: 是否保存到数据库（默认True）
- `--start-date`: 开始日期（YYYY-MM-DD）
- `--end-date`: 结束日期（YYYY-MM-DD）
- `--codes`: 股票代码列表（逗号分隔）

**功能：**
- 同步指定时间范围的日K线数据
- 支持指定股票代码
- 自动处理数据缺失

**示例：**
```bash
# 同步最近30天所有股票
python -m src.cli.main sync-daily

# 同步指定日期范围
python -m src.cli.main sync-daily --start-date 2024-01-01 --end-date 2024-01-31

# 同步指定股票
python -m src.cli.main sync-daily --codes "000001,000002,600000"

# 同步单只股票指定日期
python -m src.cli.main sync-daily --codes "000001" --start-date 2024-01-01 --end-date 2024-01-31
```

### 2.5 sync-financial命令 - 同步财务数据

```bash
python -m src.cli.main sync-financial [OPTIONS]
```

**选项：**
- `--csv`: 是否保存到CSV文件（默认True）
- `--db`: 是否保存到数据库（默认True）
- `--year`: 年份
- `--quarter`: 季度（1-4）
- `--codes`: 股票代码列表（逗号分隔）

**功能：**
- 同步指定季度的财务数据
- 支持批量处理
- 自动处理数据更新

**示例：**
```bash
# 同步去年第四季度所有股票
python -m src.cli.main sync-financial

# 同步指定季度
python -m src.cli.main sync-financial --year 2023 --quarter 4

# 同步指定股票的财务数据
python -m src.cli.main sync-financial --codes "000001,000002" --year 2023 --quarter 4
```

### 2.6 status命令 - 查看系统状态

```bash
python -m src.cli.main status
```

**功能：**
- 显示当前环境配置
- 检查数据库连接状态
- 显示数据目录状态
- 统计已同步数据量

## 3. 环境配置

### 3.1 切换环境

```bash
# 使用测试环境
python -m src.cli.main --env uat [command]

# 使用生产环境
python -m src.cli.main --env prod [command]
```

### 3.2 配置文件说明

配置文件位于 `backend/config/config.yaml`，主要配置项：

```yaml
# 环境配置
env: uat  # uat 或 prod

# 数据库配置
database:
  uat:
    host: 127.0.0.1
    port: 5432
    user: postgres
    password: yuzh1234
    database: stock_analysis_uat

# 数据路径配置
data_paths:
  uat:
    csv: uat/data
    vipdoc: uat/vipdoc

# 数据源配置
data_sources:
  pytdx:
    enabled: true
    vipdoc_path: uat/vipdoc
  baostock:
    enabled: true

# 同步配置
sync:
  batch_size: 1000      # 批处理大小
  max_retries: 3        # 最大重试次数
  timeout: 30           # 超时时间（秒）
```

## 4. 数据目录结构

### 4.1 CSV数据目录

```
uat/data/
├── stocks/             # 股票列表
│   └── stocks_YYYYMMDD.csv
├── daily/              # 日K线数据
│   └── daily_YYYYMMDD.csv
└── financial/          # 财务数据
    └── financial_YYYYMMDD.csv
```

### 4.2 通达信数据目录

```
uat/vipdoc/
├── sh/                 # 上海交易所
│   ├── lday/           # 日K线
│   ├── minline/        # 分钟线
│   └── ...
├── sz/                 # 深圳交易所
│   ├── lday/
│   ├── minline/
│   └── ...
└── ds/                 # 大商所
    └── ...
```

## 5. 常见问题

### 5.1 数据库连接失败

**问题：** 提示数据库连接失败

**解决方案：**
1. 检查PostgreSQL服务是否启动
2. 验证配置文件中的数据库信息
3. 确认数据库用户权限

```bash
# 检查PostgreSQL状态
pg_ctl status

# 测试连接
psql -h 127.0.0.1 -p 5432 -U postgres -d stock_analysis_uat
```

### 5.2 数据同步中断

**问题：** 同步过程中意外中断

**解决方案：**
1. 查看日志文件了解错误原因
2. 重新执行同步命令（支持增量更新）
3. 使用更小的batch_size

```bash
# 查看日志
tail -f logs/sync_uat_YYYYMMDD.log

# 调整批处理大小
# 修改config.yaml中的sync.batch_size为500
```

### 5.3 内存不足

**问题：** 处理大量数据时内存不足

**解决方案：**
1. 减少batch_size
2. 分批处理股票代码
3. 增加系统内存

```bash
# 分批同步
python -m src.cli.main sync-daily --codes "000001,000002,...,000100"
```

## 6. 最佳实践

### 6.1 定期同步建议

```bash
# 每日收盘后执行
python -m src.cli.main sync-daily --start-date $(date -d '1 day ago' +%Y-%m-%d)

# 每季度更新财务数据
python -m src.cli.main sync-financial --year $(date +%Y) --quarter $((( ($(date +%m)-1)/3)+1))

# 每周更新股票列表
python -m src.cli.main sync-stocks
```

### 6.2 数据备份

```bash
# 备份CSV数据
tar -czf backup_$(date +%Y%m%d).tar.gz uat/data/

# 备份数据库
pg_dump -h 127.0.0.1 -p 5432 -U postgres stock_analysis_uat > backup_$(date +%Y%m%d).sql
```

### 6.3 性能优化

1. **合理设置batch_size**
   - 内存充足：1000-2000
   - 内存有限：500-1000

2. **使用SSD存储**
   - 提升I/O性能
   - 加快数据写入速度

3. **定期清理日志**
   - 避免日志文件过大
   - 保留最近30天日志

## 7. 故障排查

### 7.1 查看详细日志

```bash
# 实时查看同步日志
tail -f logs/sync_uat_$(date +%Y%m%d).log

# 查看错误日志
grep ERROR logs/sync_uat_*.log
```

### 7.2 检查数据完整性

```sql
-- 检查股票数量
SELECT COUNT(*) FROM stocks;

-- 检查日K线数据
SELECT code, COUNT(*) as days FROM daily_data GROUP BY code ORDER BY days DESC LIMIT 10;

-- 检查财务数据
SELECT year, quarter, COUNT(*) FROM financial_data GROUP BY year, quarter;
```

### 7.3 重新同步数据

```bash
# 删除错误数据后重新同步
python -m src.cli.main sync-daily --start-date 2024-01-01 --end-date 2024-01-31 --no-csv --db
```