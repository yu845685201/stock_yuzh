"""分析链核心抽象（方案 §3.3）。

本模块定义三件事：
- ``AnalysisModule``：原子分析模块的抽象基类；
- ``ModuleResult``：模块返回值（写入 CSV 的 value + 可选调试 detail）；
- ``ChainContext``：执行上下文（锚定日、股票信息、存疑原因、上游结果）。

模块开发规范（§3.3）：
1. 纯函数：同输入必同输出，禁止 randomness / 网络调用 / 全局状态；
2. 不抛异常：数据不足/异常一律返回 ``数据不足``，引擎层面兜底捕获；
3. 只用 pandas / numpy 原生算子，向量化实现，禁止逐行 Python 循环（性能红线）；
4. 每个模块配 pytest 用例；
5. 新模块必须同步登记 ``src/analysis/README.MD``。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, List, Optional

import pandas as pd

#: 数据不足时的统一返回值（§2.5：不允许空串或静默丢弃）
DATA_INSUFFICIENT = "数据不足"
#: 模块执行异常时的返回值（P3-3/C6：异常与"数据不足"区分，错误摘要进 detail——
#: 异常可见可查，不与正常数据不足混同）
DATA_ERROR = "错误"


@dataclass(frozen=True)
class StockInfo:
    """分析股票池中的一只股票。"""

    ts_code: str
    stock_code: str
    stock_name: str
    list_date: Optional[str] = None  # yyyyMMdd，用于「上市首日不判定」
    board: str = ""                  # 主板 / 创业板 / 科创板 / 北交所 / 未知

    def __str__(self) -> str:  # pragma: no cover - 仅日志用
        return f"{self.stock_code}({self.stock_name})"


@dataclass
class ModuleResult:
    """模块返回值。

    Attributes:
        value: 写入 CSV 的值。格式由模块自行定义并登记 README.MD。
        detail: 可选调试信息，MVP 不落盘。
    """

    value: Any
    detail: Optional[Dict[str, Any]] = None

    @classmethod
    def insufficient(cls, reason: Optional[str] = None) -> "ModuleResult":
        """构造 ``数据不足`` 结果。"""
        return cls(DATA_INSUFFICIENT, {"reason": reason} if reason else None)


@dataclass
class ChainContext:
    """链执行上下文。

    Attributes:
        as_of: 锚定日 yyyyMMdd。
        stock: 当前股票信息。
        suspect_reasons: 数据预检发现的存疑原因（供模块参考）。
        upstream: 前序模块结果 + 引擎装配的环境值（如外围域 fx_hkdcny，
                  整合方案 §4.2；键名与模块名不冲突）。逐股独立副本。
        expected_bars: 本股分析窗口应含的交易日数（P0-7，按
                  ``max(窗口起点, list_date)`` 起算，防次新股被误判缺日）；
                  ``None`` = 引擎未提供（外围域等），模块侧不做覆盖检查。
    """

    as_of: str
    stock: StockInfo
    suspect_reasons: List[str] = field(default_factory=list)
    upstream: Dict[str, Any] = field(default_factory=dict)
    expected_bars: Optional[int] = None

    @property
    def is_suspect(self) -> bool:
        return bool(self.suspect_reasons)


class AnalysisModule(ABC):
    """原子分析模块抽象基类。

    子类必须声明 ``name``（中文，即 CSV 列名）、``name_en``（英文标识）、
    ``description``（详细描述，进 README.MD）与 ``required_columns``，
    并实现 ``analyze``。
    """

    #: CSV 列名（中文）
    name: ClassVar[str] = ""
    #: 英文标识（README.MD 登记用，也是链 YAML 中引用的模块名）
    name_en: ClassVar[str] = ""
    #: 详细描述（进 README.MD）
    description: ClassVar[str] = ""
    #: 声明所需 K 线列，DataReader 按需取列
    required_columns: ClassVar[List[str]] = []
    #: 声明最大回看天数（0 = 全历史）
    max_lookback: ClassVar[int] = 0
    #: 模块参数默认值（可在链 YAML 的 params 中覆盖）
    default_params: ClassVar[Dict[str, Any]] = {}

    def __init__(self, params: Optional[Dict[str, Any]] = None):
        merged = dict(self.default_params)
        if params:
            unknown = sorted(set(params) - set(merged))
            if unknown:
                raise ValueError(
                    f"模块 {self.name_en} 不支持参数: {', '.join(unknown)}；"
                    f"可用参数: {', '.join(sorted(merged)) or '无'}"
                )
            merged.update(params)
        self.params: Dict[str, Any] = merged

    # ------------------------------------------------------------------ 参数
    def param(self, key: str) -> Any:
        return self.params[key]

    def effective_lookback(self) -> int:
        """有效回看交易日数（链参数优先于类默认值，0 = 全历史）。"""
        value = self.params.get("max_lookback", self.max_lookback)
        try:
            value = int(value)
        except (TypeError, ValueError):
            value = self.max_lookback
        return max(value, 0)

    # ------------------------------------------------------------------ 执行
    @abstractmethod
    def analyze(self, df: pd.DataFrame, ctx: ChainContext) -> ModuleResult:
        """分析单只股票。

        Args:
            df: 单只股票日 K，按 ``trade_date`` 升序（已按锚定日窗口裁剪）。
            ctx: 链上下文。

        Returns:
            ModuleResult。数据不足时返回 ``数据不足``，不要抛异常。
        """

    # ------------------------------------------------------------------ 命中统计
    #: 档位分桶命中值：命中统计按值分桶计数（如 ``["大涨", "大跌"]``）；
    #: 空 = 不分桶。档位类模块声明此项，布尔/数值类用 ``hit_rule``。
    hit_buckets: ClassVar[List[str]] = []
    #: 数值/理由串类命中规则（方案 §2.5）：
    #: ``positive``（>0，默认口径）/ ``signed``（正负分桶，如溢价）/
    #: ``threshold``（≥ ``hit_threshold`` 参数，默认 2.0，链 YAML 可覆盖）/
    #: ``sample``（可算样本数，连续比率类）/ ``reason``（理由串：非「否/数据不足」即命中）
    hit_rule: ClassVar[str] = "positive"

    #: 理由串类的非命中值（hit_rule="reason" 用）
    _NO_HIT_STRINGS = (DATA_INSUFFICIENT, DATA_ERROR, "否")

    def hit_keys(self, value: Any) -> List[str]:
        """命中统计的桶名列表（引擎按桶计数；空列表 = 不计入）。

        默认口径与历史行为一致：布尔原样 / ``是`` 计「命中」/ 数值 >0 计「命中」，
        其余字符串不计。声明 ``hit_buckets`` / ``hit_rule`` 的模块按 §2.5 规格分桶，
        未声明模块零行为变化。
        """
        if self.hit_rule == "reason":
            return [] if value in self._NO_HIT_STRINGS else ["命中"]
        if isinstance(value, bool):
            return ["命中"] if value else []
        if value == "是":
            return ["命中"]
        if isinstance(value, (int, float)):
            if self.hit_rule == "sample":
                return ["可算"]
            if self.hit_rule == "signed":
                if value > 0:
                    return ["正"]
                if value < 0:
                    return ["负"]
                return []
            if self.hit_rule == "threshold":
                threshold = float(self.params.get("hit_threshold", 2.0))
                return [f"≥{threshold}x"] if value >= threshold else []
            return ["命中"] if value > 0 else []
        if self.hit_buckets:
            return [str(value)] if value in self.hit_buckets else []
        return []

    def __repr__(self) -> str:  # pragma: no cover - 仅日志用
        return f"<{type(self).__name__} {self.name_en} params={self.params}>"
