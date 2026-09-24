"""链配置解析（方案 §3.4）。

``chains/`` 下一个 YAML 一条链，**无默认链**——``analyze run`` 必须显式指定
``--chain`` 或 ``--modules``，否则报错退出。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from .lookback import Lookback

CHAINS_DIR = Path(__file__).resolve().parent / "chains"


@dataclass
class ModuleSpec:
    """链中的一个模块条目。"""

    name_en: str
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ChainConfig:
    """一条分析链。"""

    chain: str
    description: str = ""
    #: 分析窗口：锚定日往前加载多久的日 K 线。默认 1 年（见 lookback.py）
    lookback: Lookback = field(default_factory=Lookback.default)
    modules: List[ModuleSpec] = field(default_factory=list)
    source_path: Optional[Path] = None


def list_chain_files(chains_dir: Path = CHAINS_DIR) -> List[Path]:
    if not chains_dir.exists():
        return []
    return sorted(p for p in chains_dir.glob("*.yaml") if p.is_file())


def list_chains(chains_dir: Path = CHAINS_DIR) -> List[ChainConfig]:
    """加载 ``chains/`` 下全部链配置。"""
    return [load_chain(path.stem, chains_dir) for path in list_chain_files(chains_dir)]


def load_chain(name: str, chains_dir: Path = CHAINS_DIR) -> ChainConfig:
    """按链名加载 ``chains/<name>.yaml``。未知链名直接报错。"""
    if not name or not str(name).strip():
        raise ValueError("链名不能为空")
    path = chains_dir / f"{name}.yaml"
    if not path.exists():
        available = ", ".join(p.stem for p in list_chain_files(chains_dir)) or "（无）"
        raise ValueError(f"未知分析链: {name}；可用链: {available}")

    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    chain = ChainConfig(
        chain=str(raw.get("chain") or name),
        description=str(raw.get("description") or ""),
        lookback=Lookback.parse(raw["lookback"]) if "lookback" in raw else Lookback.default(),
        source_path=path,
    )

    modules = raw.get("modules")
    if not isinstance(modules, list) or not modules:
        raise ValueError(f"链配置 {path} 的 modules 必须是非空列表")

    for idx, item in enumerate(modules):
        if isinstance(item, str):
            chain.modules.append(ModuleSpec(name_en=item))
            continue
        if not isinstance(item, dict) or "name" not in item:
            raise ValueError(f"链配置 {path} 第 {idx + 1} 个模块条目格式非法: {item!r}")
        params = item.get("params") or {}
        if not isinstance(params, dict):
            raise ValueError(f"链配置 {path} 模块 {item['name']} 的 params 必须是映射")
        chain.modules.append(ModuleSpec(name_en=str(item["name"]), params=dict(params)))

    _validate(chain, chains_dir)
    return chain


def parse_modules_option(option: str) -> List[ModuleSpec]:
    """解析 ``--modules a,b`` 临时链。"""
    names = [n.strip() for n in str(option).split(",") if n.strip()]
    if not names:
        raise ValueError("--modules 不能为空")
    return [ModuleSpec(name_en=n) for n in names]


def _validate(chain: ChainConfig, chains_dir: Path) -> None:
    """启动即校验：链名/模块名/参数非法一律报错退出，不静默跳过（§3.4）。"""
    from .module_registry import build, load_registry

    registry = load_registry()
    seen = set()
    for spec in chain.modules:
        if spec.name_en in seen:
            raise ValueError(f"链 {chain.chain} 中模块 {spec.name_en} 重复配置")
        seen.add(spec.name_en)
        if spec.name_en not in registry:
            available = ", ".join(sorted(registry)) or "（无）"
            raise ValueError(
                f"链 {chain.chain} 引用了未知模块: {spec.name_en}；可用模块: {available}"
            )
        build(spec.name_en, spec.params)  # 参数校验（未知参数会抛错）


def available_chain_names(chains_dir: Path = CHAINS_DIR) -> List[str]:
    return [p.stem for p in list_chain_files(chains_dir)]
