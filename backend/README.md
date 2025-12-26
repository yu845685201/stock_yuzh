# A股盘后静态分析系统 - 后端

## 项目简介

本系统是一个专注于A股市场的盘后静态数据分析系统，主要用于交易日结束后的数据分析和研究。系统包含两大核心模块：**数据同步**和**股票分析**，为投资决策提供数据支持。

## 功能特性

- 多数据源支持（Pytdx、Baostock）
- 双重存储（CSV文件 + PostgreSQL数据库）
- 命令行工具接口
- 模块化设计，易于扩展

## 快速开始

### 1. 环境要求

- Python 3.8+
- PostgreSQL 12+

### 2. 安装依赖

```bash
cd backend
pip install -r requirements.txt
```

或使用setup.py安装：

```bash
pip install -e .
```

### 3. 配置数据库

确保PostgreSQL服务已启动，并创建数据库：

```sql
-- 测试环境
CREATE DATABASE stock_analysis_uat;

-- 生产环境
CREATE DATABASE stock_analysis;
```

### 4. 初始化系统

```bash
# 初始化系统（包括创建数据库表）
python -m src.cli.main --env uat init --init-db
```

### 5. 同步数据

```bash
# 同步所有数据
python -m src.cli.main sync-all

# 只同步股票列表
python -m src.cli.main sync-stocks

# 同步指定日期范围的日K线数据
python -m src.cli.main sync-daily --start-date 2024-01-01 --end-date 2024-01-31

# 同步指定股票的财务数据
python -m src.cli.main sync-financial --year 2023 --quarter 4 --codes 000001,000002
```

## 项目结构

```
backend/
├── src/                    # 源代码目录
│   ├── cli/               # 命令行接口
│   ├── config/            # 配置管理
│   ├── database/          # 数据库操作
│   ├── data_sources/      # 数据源实现
│   └── sync/              # 数据同步
├── config/                # 配置文件
│   └── config.yaml        # 主配置文件
├── requirements.txt       # 依赖列表
├── setup.py              # 安装脚本
└── README.md             # 说明文档
```

## 配置说明

配置文件位于 `backend/config/config.yaml`，包含以下配置项：

- **env**: 运行环境（uat/prod）
- **database**: 数据库连接配置
- **data_paths**: 数据存储路径
- **data_sources**: 数据源开关和配置
- **sync**: 同步参数（批次大小、重试次数等）

## 命令行工具

系统提供了完整的命令行工具：

```bash
# 查看帮助
python -m src.cli.main --help

# 查看系统状态
python -m src.cli.main status

# 初始化系统
python -m src.cli.main init [--init-db]

# 数据同步命令
python -m src.cli.main sync-all [--csv] [--db]
python -m src.cli.main sync-stocks [--csv] [--db]
python -m src.cli.main sync-daily [--csv] [--db] [--start-date] [--end-date] [--codes]
python -m src.cli.main sync-financial [--csv] [--db] [--year] [--quarter] [--codes]
```

## 数据源说明

### Pytdx数据源
- 用于读取通达信数据文件（.day, .lc1, .lc5等）
- 支持日K线、分钟K线等行情数据
- 无需网络连接，直接读取本地文件

### Baostock数据源
- 提供免费、纯净的证券数据
- 支持股票列表、日K线、财务数据等
- 需要网络连接

## 开发指南

### 添加新的数据源

1. 继承 `DataSourceBase` 基类
2. 实现所有抽象方法
3. 在配置文件中添加数据源配置
4. 在 `SyncManager` 中注册新数据源

### 扩展数据模型

1. 在 `database/models.py` 中定义新模型
2. 在数据库连接中创建对应的表
3. 更新同步管理器以支持新数据类型

## 注意事项

1. 严格按照环境隔离使用资源
2. 禁止使用模拟数据，所有数据必须来自真实源
3. 支持的数据会如实同步，无法获取的数据留空处理

## 常见问题

### Q: 如何切换到生产环境？
A: 使用 `--env prod` 参数，或修改配置文件中的 `env` 字段。

### Q: 数据同步失败怎么办？
A: 1. 检查网络连接；2. 确认数据源配置正确；3. 查看错误日志。

### Q: 如何只同步CSV不存数据库？
A: 使用 `--csv --no-db` 参数。