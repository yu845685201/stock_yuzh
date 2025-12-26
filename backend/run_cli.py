#!/usr/bin/env python3
"""
A股盘后静态分析系统 - 命令行入口脚本
"""

import sys
import os

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

# 导入并运行主函数
from src.cli.main import main

if __name__ == "__main__":
    main()