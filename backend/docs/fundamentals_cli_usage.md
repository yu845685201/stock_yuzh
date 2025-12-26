# 股票基本面数据采集CLI使用指南

## 概述

股票基本面数据采集CLI工具用于从baostock获取A股市场的财务基本面数据，包括总股本、流通股本等关键指标。

## 命令格式

```bash
python run_cli.py sync-fundamentals [OPTIONS]
```

## 参数说明

### 基础参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `--batch-size` | INTEGER | 50 | 批次大小，符合baostock QPS限制 |
| `--dry-run` | FLAG | False | 试运行模式，不实际写入数据 |
| `--list-status` | CHOICE | L | 股票上市状态过滤 |

### 上市状态选项

| 选项 | 说明 |
|------|------|
| `L` | 上市股票（默认） |
| `D` | 退市股票 |
| `P` | 暂停上市股票 |

## 使用示例

### 1. 基本使用（默认参数）

```bash
python run_cli.py sync-fundamentals
```

**说明**：使用默认参数采集所有上市股票的基本面数据
- 批次大小：50只股票
- 状态过滤：仅上市股票
- 模式：实际写入数据

### 2. 试运行模式

```bash
python run_cli.py sync-fundamentals --dry-run
```

**说明**：试运行模式，不实际写入数据，用于测试和验证

### 3. 自定义批次大小

```bash
python run_cli.py sync-fundamentals --batch-size 30
```

**说明**：减小批次大小，降低QPS压力

### 4. 采集退市股票数据

```bash
python run_cli.py sync-fundamentals --list-status D
```

**说明**：采集已退市股票的基本面数据（用于历史数据分析）

### 5. 组合参数使用

```bash
python run_cli.py sync-fundamentals --batch-size 20 --dry-run --list-status L
```

**说明**：小批次试运行，仅采集上市股票

## 输出示例

```
开始同步股票基本面数据...
  - 批次大小: 50
  - 试运行模式: 否
  - 上市状态: L

✓ 基本面数据同步完成!
  - 总股票数量: 4852
  - 成功采集: 4235
  - 失败数量: 617
  - 处理批次数: 98
  - 耗时: 245.67 秒
  - 成功率: 87.28%
```

## 系统要求

### 环境配置

- Python 3.8+
- PostgreSQL 12+
- baostock库
- 网络连接（访问baostock API）

### 数据库要求

确保 `base_fundamentals_info` 表已创建：
```bash
python run_cli.py init --init-db
```

### 数据目录

确保CSV输出目录存在：
```
uat/data/base_fundamentals_info/
```

## 性能优化建议

### 1. QPS控制

- 默认批次大小50符合baostock QPS限制
- 如遇API限制，可减小批次大小至30-20
- 系统已内置每次调用间隔0.02秒的安全机制

### 2. 内存优化

- 大量数据采集时建议使用较小批次大小
- 可使用 `--dry-run` 预先测试系统负载

### 3. 网络优化

- 确保网络连接稳定
- 如遇网络中断，程序会自动跳过失败股票继续执行

## 错误处理

### 常见错误及解决方案

#### 1. 数据库连接失败

```
✗ 基本面数据同步失败: connection to server at "127.0.0.1", port 5432 failed
```

**解决方案**：
- 检查PostgreSQL服务是否启动
- 验证数据库连接配置
- 运行 `python run_cli.py status` 检查系统状态

#### 2. baostock API限制

```
✗ 基本面数据同步失败: Baostock登录失败
```

**解决方案**：
- 减小批次大小：`--batch-size 30`
- 稍后重试
- 检查网络连接

#### 3. 数据表不存在

```
✗ 基本面数据同步失败: relation "base_fundamentals_info" does not exist
```

**解决方案**：
- 初始化数据库表：`python run_cli.py init --init-db`

## 监控和日志

### 查看系统状态

```bash
python run_cli.py status
```

**输出示例**：
```
系统状态:
  - 环境: uat
  - 数据库连接: 正常
  - 股票数量: 4852
  - 基本面数据: 4235 条
  - csv目录: 存在 (/path/to/uat/data)
```

### 日志级别

使用详细模式查看更多日志信息：

```bash
# 注意：当前CLI使用click框架，如需详细日志，可使用独立的CLI工具
python -m src.cli.fundamentals_cli --verbose --dry-run
```

## 数据输出

### CSV文件

- **路径**：`{csv根目录}/base_fundamentals_info/`
- **命名**：`base_fundamentals_info_{yyyyMMdd}.csv`
- **字段**：ts_code, stock_code, stock_name, disclosure_date, report_date, total_share, float_share

### 数据库表

- **表名**：`base_fundamentals_info`
- **更新策略**：基于ts_code的upsert操作
- **索引**：ts_code唯一索引

## 最佳实践

### 1. 定期更新

建议每个季度更新一次基本面数据：
```bash
python run_cli.py sync-fundamentals --batch-size 30
```

### 2. 数据验证

更新后验证数据质量：
```bash
python run_cli.py status
```

### 3. 备份策略

重要数据更新前先备份：
- 备份CSV文件
- 导出数据库表

### 4. 监控告警

关注以下指标：
- 采集成功率应 > 85%
- 单次采集时间 < 10分钟
- 失败股票数量 < 500只

## 技术支持

如遇到问题，请检查：

1. **系统状态**：`python run_cli.py status`
2. **配置文件**：检查 `config/config.yaml`
3. **日志文件**：查看详细错误信息
4. **网络连接**：确保能访问baostock API

---

**版本**：1.0.0
**更新时间**：2025-12-26
**维护团队**：A股盘后静态分析系统