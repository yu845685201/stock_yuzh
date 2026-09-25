"""链编排引擎（方案 §3.1 / §3.4）。

主流程::

    股票池 → 加载日K（按需取列 + 锚定日窗口）→ 数据预检 → 逐股执行模块 → 汇总 CSV

设计要点：
- 取列裁剪：全链各模块 ``required_columns`` 求并集；
- 加载窗口：``max(模块有效回看天数, 预检窗口)`` 个交易日（0 = 全历史）；
- 模块异常一律兜底为 ``数据不足``，绝不让单只股票中断整链；
- 全程输出进度（stdout + ``.progress.json``）。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from .chain_config import ChainConfig
from .data_reader import MISSING_MODE_GAP, DataReader
from .lookback import Lookback
from .module_base import DATA_INSUFFICIENT, ChainContext, ModuleResult, StockInfo
from .module_registry import build
from .progress_tracker import ProgressTracker
from .result_writer import RunMeta, ResultWriter, now_iso

logger = logging.getLogger(__name__)

#: 加载窗口相对模块回看天数的余量
WINDOW_BUFFER = 2


def _is_hit(value: Any) -> bool:
    """命中统计：布尔类模块值为「是」，数值类模块值 > 0，其余不计。"""
    if isinstance(value, bool):
        return value
    if value == "是":
        return True
    if isinstance(value, (int, float)):
        return value > 0
    return False


@dataclass
class EngineOptions:
    """引擎运行参数。"""

    as_of: Optional[str] = None
    #: 分析窗口覆盖（CLI --lookback）；None = 用链 YAML 配置
    lookback: Optional[Lookback] = None
    break_tolerance_pct: float = 0.6
    check_missing_day: bool = True
    missing_day_mode: str = MISSING_MODE_GAP
    progress_interval_stocks: int = 200
    progress_interval_percent: float = 5.0
    output_dir: Optional[Path] = None


@dataclass
class EngineResult:
    """引擎运行结果。"""

    chain: str
    as_of: str
    csv_path: Path
    progress_path: Path
    meta_path: Path
    stock_count: int
    suspect_count: int
    suspended_count: int
    module_names: List[str]
    duration_seconds: float
    hit_summary: Dict[str, int]
    lookback: str = ""
    window_start: Optional[str] = None
    loaded_trading_days: int = 0
    window_expanded: bool = False


class ChainEngine:
    """分析链执行引擎。"""

    def __init__(self, config_manager, options: EngineOptions):
        self.config_manager = config_manager
        self.options = options

    # ------------------------------------------------------------------ 入口
    def run(self, chain: ChainConfig) -> EngineResult:
        started = time.monotonic()
        started_at = now_iso()
        opts = self.options

        with DataReader(self.config_manager) as reader:
            as_of = reader.resolve_as_of(opts.as_of)
            universe = reader.load_stock_universe()
            if not universe:
                raise ValueError("股票池为空（base_stock_info.list_status='L' ∩ his_kline_day）")

            modules = [build(spec.name_en, spec.params) for spec in chain.modules]
            module_names = [m.name for m in modules]

            # 取列裁剪：各模块 required_columns 并集
            columns: List[str] = ["change_rate", "is_st"]
            for mod in modules:
                columns.extend(mod.required_columns)

            # 分析窗口：链 YAML 配置（默认 1y）；再与各模块回看需求取更早者
            lookback = opts.lookback or chain.lookback
            module_days = max([m.effective_lookback() for m in modules] or [0])
            window_expanded = False
            if lookback.is_all:
                window_start = None
            else:
                chain_start = reader.resolve_window_start(as_of, lookback)
                candidates = [c for c in [chain_start] if c]
                if module_days > 0:
                    needed = reader.nth_trading_day_back(as_of, module_days + WINDOW_BUFFER)
                    if needed:  # 交易日历不足则忽略，退化为链窗口
                        candidates.append(needed)
                        if chain_start and needed < chain_start:
                            window_expanded = True
                window_start = min(candidates) if candidates else None

            logger.info(
                "锚定日 %s | 股票池 %d 只 | 分析窗口 %s（起点 %s%s）| 模块 %s",
                as_of, len(universe), lookback, window_start or "全历史",
                "，因模块回看需求扩展" if window_expanded else "", ", ".join(module_names),
            )

            kdf = reader.load_kline(columns=columns, as_of=as_of, start_date=window_start)
            pre = reader.precheck(
                kdf,
                as_of=as_of,
                start_date=window_start,
                break_tolerance_pct=opts.break_tolerance_pct,
                check_missing_day=opts.check_missing_day,
                universe=universe,
                missing_day_mode=opts.missing_day_mode,
            )
            suspects = pre.suspects
            reader.close()

        loaded_trading_days = int(kdf["trade_date"].nunique()) if not kdf.empty else 0
        logger.info(
            "数据预检：前复权断裂 %d 处 / 计入存疑的缺日 %d 处 / 疑似停牌 %d 只 / 窗口内交易日 %d 天",
            pre.break_count, pre.missing_count, pre.suspended_stock_count, loaded_trading_days,
        )

        path_cfg = build_output_dir(self.config_manager)
        output_dir = Path(opts.output_dir) if opts.output_dir else path_cfg
        writer = ResultWriter(output_dir)
        paths = writer.resolve_paths(chain.chain, as_of)

        progress = ProgressTracker(
            chain=chain.chain,
            as_of=as_of,
            total=len(universe),
            path=paths["progress"],
            interval_stocks=opts.progress_interval_stocks,
            interval_percent=opts.progress_interval_percent,
        )

        rows: List[Dict[str, Any]] = []
        hit_summary: Dict[str, int] = {name: 0 for name in module_names}
        suspect_count = 0
        try:
            info_by_ts = {s.ts_code: s for s in universe}
            seen: set = set()
            if not kdf.empty:
                for ts_code, g in kdf.groupby("ts_code", sort=False):
                    stock = info_by_ts.get(ts_code)
                    if stock is None:  # 库中出现但不在股票池（如已退市）→ 跳过
                        continue
                    seen.add(ts_code)
                    reasons = suspects.get(ts_code, [])
                    if reasons:
                        suspect_count += 1
                    results = self._run_modules(modules, g, as_of, stock, reasons)
                    for name, res in results.items():
                        if _is_hit(res.value):
                            hit_summary[name] += 1
                    rows.append(self._row(stock, reasons, results, module_names))
                    progress.tick()
            # 股票池中在加载窗口内无任何 K 线的股票（如上市晚于锚定日、长期停牌）
            # 同样输出行并标 数据不足，不允许静默丢弃（§2.5）
            for stock in universe:
                if stock.ts_code in seen:
                    continue
                reasons = suspects.get(stock.ts_code, [])
                if reasons:
                    suspect_count += 1
                rows.append(self._row(stock, reasons, {}, module_names))
                progress.tick()
        except Exception as exc:
            progress.fail(repr(exc))
            raise

        rows.sort(key=lambda r: r["股票代码"])  # 固定行序，保证同锚定日重复运行逐字节一致
        csv_path = writer.write_csv(chain.chain, as_of, module_names, rows)
        duration = time.monotonic() - started
        writer.write_meta(
            chain.chain,
            as_of,
            RunMeta(
                chain=chain.chain,
                as_of=as_of,
                modules=module_names,
                started_at=started_at,
                finished_at=now_iso(),
                duration_seconds=duration,
                stock_count=len(rows),
                suspect_count=suspect_count,
                lookback=str(lookback),
                window_start=window_start or "",
                loaded_trading_days=loaded_trading_days,
                window_expanded=window_expanded,
                break_count=pre.break_count,
                missing_count=pre.missing_count,
                suspended_count=pre.suspended_stock_count,
                suspended_detail={k: v[:2] for k, v in list(pre.suspended.items())[:50]},
            ),
        )
        progress.warnings = suspect_count
        progress.finish(csv_path=str(csv_path), status="done")

        return EngineResult(
            chain=chain.chain,
            as_of=as_of,
            csv_path=csv_path,
            progress_path=paths["progress"],
            meta_path=paths["meta"],
            stock_count=len(rows),
            suspect_count=suspect_count,
            suspended_count=pre.suspended_stock_count,
            module_names=module_names,
            duration_seconds=duration,
            hit_summary=hit_summary,
            lookback=str(lookback),
            window_start=window_start,
            loaded_trading_days=loaded_trading_days,
            window_expanded=window_expanded,
        )

    # ------------------------------------------------------------------ 内部
    @staticmethod
    def _run_modules(
        modules: Sequence,
        group: "pd.DataFrame",
        as_of: str,
        stock: StockInfo,
        reasons: List[str],
    ) -> Dict[str, ModuleResult]:
        ctx = ChainContext(as_of=as_of, stock=stock, suspect_reasons=list(reasons), upstream={})
        results: Dict[str, ModuleResult] = {}
        for mod in modules:
            try:
                result = mod.analyze(group, ctx)
                if result is None:
                    result = ModuleResult.insufficient("模块返回 None")
            except Exception as exc:  # 单模块失败不中断整链
                logger.warning("模块 %s 在 %s 执行失败: %r", mod.name_en, stock.ts_code, exc)
                result = ModuleResult.insufficient(repr(exc))
            results[mod.name] = result
            ctx.upstream[mod.name] = result
        return results

    @staticmethod
    def _row(
        stock: StockInfo,
        reasons: List[str],
        results: Dict[str, ModuleResult],
        module_names: Sequence[str],
    ) -> Dict[str, Any]:
        row: Dict[str, Any] = {
            "股票代码": stock.stock_code,
            "股票名称": stock.stock_name,
            "是否存疑": "是" if reasons else "否",
            "存疑原因": ";".join(reasons),
        }
        for name in module_names:
            res = results.get(name)
            row[name] = res.value if res is not None else DATA_INSUFFICIENT
        return row


def build_output_dir(config_manager) -> Path:
    """解析输出目录：``analysis.output_dir`` 相对仓库根 ``stock_yuzh/``，或绝对路径。

    默认 ``uat/data/analysis`` → ``stock_yuzh/uat/data/analysis``（方案 §2.5）。
    """
    configured = config_manager.get("analysis.output_dir") if config_manager else None
    repo_root = Path(__file__).resolve().parents[3]  # analysis → src → backend → stock_yuzh
    if configured:
        candidate = Path(str(configured)).expanduser()
        return candidate if candidate.is_absolute() else (repo_root / candidate)
    return repo_root / "uat" / "data" / "analysis"


def options_from_config(config_manager) -> EngineOptions:
    """从 config.yaml 的 ``analysis`` 段读取默认引擎参数。

    注意：**分析窗口不在这里配置**——窗口由链 YAML 的 ``lookback`` 决定（默认 1y），
    这里只放与具体链无关的执行参数。
    """
    return EngineOptions(
        break_tolerance_pct=float(config_manager.get("analysis.break_tolerance_pct", 0.6)),
        check_missing_day=bool(config_manager.get("analysis.check_missing_day", True)),
        missing_day_mode=str(config_manager.get("analysis.missing_day_mode", MISSING_MODE_GAP) or MISSING_MODE_GAP),
        progress_interval_stocks=int(config_manager.get("analysis.progress_interval_stocks", 200)),
        progress_interval_percent=float(config_manager.get("analysis.progress_interval_percent", 5.0)),
    )
