"""股票技术分析链（Analysis Chain）。

方案：``doc/技术分析链方案设计.MD``（V1.1）。

模块职责：
- ``module_base``       抽象基类、ModuleResult、ChainContext
- ``module_registry``   模块自动发现 + README.MD 登记校验
- ``chain_config``      链配置解析（YAML）与启动即校验
- ``data_reader``       数据读取（股票池 / 锚定日 / COPY 流式加载 / 数据预检）
- ``limit_up_rules``    涨停制度分段阈值与向量化判定（M1/M2 共享）
- ``result_writer``     CSV + 运行元信息写出
- ``progress_tracker``  进度跟踪（stdout + ``.progress.json``）
- ``chain_engine``      编排引擎
"""

from .chain_config import ChainConfig, ModuleSpec, list_chains, load_chain, parse_modules_option
from .chain_engine import ChainEngine, EngineOptions, EngineResult, build_output_dir, options_from_config
from .data_reader import DataReader
from .lookback import Lookback
from .module_base import (
    DATA_INSUFFICIENT,
    AnalysisModule,
    ChainContext,
    ModuleResult,
    StockInfo,
)
from .module_registry import build, load_registry, registration_warnings
from .progress_tracker import ProgressTracker, format_progress, read_progress
from .result_writer import ResultWriter

__all__ = [
    "AnalysisModule",
    "ChainConfig",
    "ChainContext",
    "ChainEngine",
    "DATA_INSUFFICIENT",
    "DataReader",
    "EngineOptions",
    "EngineResult",
    "Lookback",
    "ModuleResult",
    "ModuleSpec",
    "ProgressTracker",
    "ResultWriter",
    "StockInfo",
    "build",
    "build_output_dir",
    "format_progress",
    "list_chains",
    "load_chain",
    "load_registry",
    "options_from_config",
    "parse_modules_option",
    "read_progress",
    "registration_warnings",
]
