"""模块注册表：扫描 ``modules/`` 目录自动发现分析模块（方案 §3.2 F1）。

发现规则：``src/analysis/modules/`` 下每个 ``*.py``（排除 ``_`` 开头）中，
所有 ``AnalysisModule`` 子类即为一个模块。注册键为 ``name_en``。

同时提供 README.MD 登记校验：未登记 README.MD 的模块在 ``analyze list-modules``
时给出告警（§3.3 规范 5）。
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Type

from .module_base import AnalysisModule

MODULES_PACKAGE = "src.analysis.modules"
ANALYSIS_DIR = Path(__file__).resolve().parent
README_PATH = ANALYSIS_DIR / "README.MD"


def _iter_module_classes() -> List[Type[AnalysisModule]]:
    package = importlib.import_module(MODULES_PACKAGE)
    found: List[Type[AnalysisModule]] = []
    for _, mod_name, _ in pkgutil.iter_modules(package.__path__):
        if mod_name.startswith("_"):
            continue
        module = importlib.import_module(f"{MODULES_PACKAGE}.{mod_name}")
        for _, obj in inspect.getmembers(module, inspect.isclass):
            if not issubclass(obj, AnalysisModule) or obj is AnalysisModule:
                continue
            if inspect.isabstract(obj):
                continue
            if obj.__module__ != module.__name__:  # 只认本文件定义的类
                continue
            found.append(obj)
    return found


def load_registry(validate: bool = True) -> Dict[str, Type[AnalysisModule]]:
    """返回 ``{name_en: ModuleClass}``。

    Args:
        validate: 是否校验类定义完整性（name/name_en/description/analyze 等）。
    """
    registry: Dict[str, Type[AnalysisModule]] = {}
    for cls in _iter_module_classes():
        if not cls.name_en:
            raise ValueError(f"模块 {cls.__name__} 未声明 name_en")
        if cls.name_en in registry:
            raise ValueError(
                f"模块英文名重复: {cls.name_en}（{cls.__name__} 与 {registry[cls.name_en].__name__}）"
            )
        if validate:
            missing = [
                attr for attr in ("name", "description")
                if not getattr(cls, attr, "")
            ]
            if missing:
                raise ValueError(f"模块 {cls.name_en} 缺少声明: {', '.join(missing)}")
            if inspect.isabstract(cls):
                raise ValueError(f"模块 {cls.name_en} 未实现 analyze()")
        registry[cls.name_en] = cls
    return registry


def build(name_en: str, params: Optional[dict] = None) -> AnalysisModule:
    """按 ``name_en`` 实例化模块。"""
    registry = load_registry()
    if name_en not in registry:
        available = ", ".join(sorted(registry)) or "（无）"
        raise ValueError(f"未知分析模块: {name_en}；可用模块: {available}")
    return registry[name_en](params)


def readme_registered_names(readme_path: Path = README_PATH) -> Tuple[set, bool]:
    """从 README.MD 的登记表中解析已登记的英文名集合。

    Returns:
        (已登记英文名集合, README 是否存在)
    """
    if not readme_path.exists():
        return set(), False
    text = readme_path.read_text(encoding="utf-8")
    names = set()
    for line in text.splitlines():
        if not line.strip().startswith("|"):
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 2 or cells[0] in {"英文名", ""} or set(cells[0]) <= {"-", ":", " "}:
            continue
        token = cells[0].strip("`")
        if re.fullmatch(r"[a-z][a-z0-9_]*", token):
            names.add(token)
    return names, True


def registration_warnings() -> List[str]:
    """返回「模块未登记 README.MD / README 中登记了不存在的模块」的告警。"""
    registry = load_registry(validate=False)
    registered, exists = readme_registered_names()
    warnings: List[str] = []
    if not exists:
        return ["未找到 src/analysis/README.MD，模块目录缺失（§3.3 规范 5）"]
    for name_en in sorted(set(registry) - registered):
        warnings.append(f"模块 {name_en} 未登记 README.MD（§3.3 规范 5 要求同步登记）")
    for name_en in sorted(registered - set(registry)):
        warnings.append(f"README.MD 中登记的模块 {name_en} 在代码中不存在")
    return warnings
