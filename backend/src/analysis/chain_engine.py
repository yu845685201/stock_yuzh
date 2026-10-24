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
from bisect import bisect_left
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd

from .chain_config import ChainConfig
from .data_domain import WINDOW_BUFFER, get_domain
from .data_reader import MISSING_MODE_GAP, DataReader
from .env_assembly import assemble_env
from .lookback import Lookback
from .module_base import DATA_INSUFFICIENT, ChainContext, ModuleResult, StockInfo
from .module_registry import build
from .progress_tracker import ProgressTracker
from .result_writer import RunMeta, ResultWriter, now_iso

logger = logging.getLogger(__name__)


def flatten_hits(buckets: Dict[str, Dict[str, int]]) -> Dict[str, Any]:
    """把跨股累计的命中桶压平为展示结构（方案 §2.5）。

    单桶「命中」压平为 int（与历史显示兼容），多桶保留 ``{桶名: 数}``
    （如大涨/大跌分桶、正/负溢价分桶、可算样本数）。
    """
    summary: Dict[str, Any] = {}
    for name, b in buckets.items():
        if not b:
            summary[name] = 0
        elif set(b) == {"命中"}:
            summary[name] = b["命中"]
        else:
            summary[name] = b
    return summary


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
    hit_summary: Dict[str, Any]
    #: 各模块「数据不足」行数（样本数类模块展示用：可算 N（数据不足 M））
    insufficient_summary: Dict[str, int] = field(default_factory=dict)
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
        # 数据域分发（整合方案 §2）：股票池/K线加载/预检/锚定日按域策略，
        # 引擎主干（逐股跑模块 → CSV）零改动
        domain = get_domain(chain.domain)

        with DataReader(self.config_manager) as reader:
            as_of = domain.resolve_as_of(reader, opts.as_of)
            universe = domain.load_universe(reader, self.config_manager)
            if not universe:
                raise ValueError(domain.universe_empty_error)

            modules = [build(spec.name_en, spec.params) for spec in chain.modules]
            module_names = [m.name for m in modules]

            # 取列裁剪：各模块 required_columns 并集（域基列在前）
            columns: List[str] = list(domain.base_columns)
            for mod in modules:
                columns.extend(mod.required_columns)

            # 分析窗口 + 加载 + 预检均按域策略（A股=原逻辑搬运；外围=行数窗口/市场日历）
            module_days = max([m.effective_lookback() for m in modules] or [0])
            load = domain.load(reader, columns, as_of, chain, opts,
                               module_days, universe)
            kdf = load.kdf
            pre = domain.precheck(reader, kdf, as_of, load, universe, opts)
            suspects = pre.suspects
            lookback_desc = load.lookback_desc
            window_start = load.window_start
            window_expanded = load.window_expanded
            # P0-7：窗口交易日列表（升序）——模块侧输入完整性门控
            # （scan_stock_anchor expected_bars）按此逐股计算应含K数；
            # 必须在 reader 关闭前取好
            window_trading_days = (reader.trading_days(as_of, window_start)
                                   if window_start else [])
            # 截面环境装配（env_assembly；链未声明 env 时零行为变化；
            # env 为列表时依序装配合并，后者键覆盖同名键）
            for env_name in chain.env_list():
                load.env_extras.update(assemble_env(env_name, reader, as_of,
                                                    self.config_manager))
            reader.close()

        logger.info(
            "锚定日 %s | 股票池 %d 只 | 分析窗口 %s（起点 %s%s）| 模块 %s",
            as_of, len(universe), lookback_desc, window_start or "全历史",
            "，因模块回看需求扩展" if window_expanded else "", ", ".join(module_names),
        )

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
        hit_buckets: Dict[str, Dict[str, int]] = {name: {} for name in module_names}
        insufficient_summary: Dict[str, int] = {name: 0 for name in module_names}
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
                    results = self._run_modules(modules, g, as_of, stock, reasons,
                                                load.env_extras,
                                                window_trading_days=window_trading_days,
                                                window_start=window_start)
                    for mod in modules:
                        for key in mod.hit_keys(results[mod.name].value):
                            bucket = hit_buckets[mod.name]
                            bucket[key] = bucket.get(key, 0) + 1
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

        hit_summary = flatten_hits(hit_buckets)
        # 数据不足行数从最终 rows 统计（含「窗口内无 K 线」的第二批股票——
        # 它们同样写「数据不足」行，只在逐股循环里计数会漏掉）
        for name in module_names:
            insufficient_summary[name] = sum(
                1 for r in rows if r.get(name) == DATA_INSUFFICIENT)
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
                lookback=lookback_desc,
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
            insufficient_summary=insufficient_summary,
            lookback=lookback_desc,
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
        env_extras: Optional[Dict[str, Any]] = None,
        window_trading_days: Optional[List[str]] = None,
        window_start: Optional[str] = None,
    ) -> Dict[str, ModuleResult]:
        # env_extras：域装配的环境值（如 fx_hkdcny）预置进 upstream（方案 §4.2），
        # 模块只读；逐股拷贝，防模块写入相互泄漏
        # expected_bars（P0-7）：[max(窗口起点, list_date), as_of] 的交易日数，
        # 次新股按上市日起算，防止把"上市晚"误判成"缺日"
        expected_bars: Optional[int] = None
        if window_trading_days:
            eff_start = window_start or ""
            if stock.list_date and stock.list_date > eff_start:
                eff_start = stock.list_date
            idx = bisect_left(window_trading_days, eff_start)
            expected_bars = len(window_trading_days) - idx or None
        ctx = ChainContext(as_of=as_of, stock=stock, suspect_reasons=list(reasons),
                           upstream=dict(env_extras or {}),
                           expected_bars=expected_bars)
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

    默认 ``data/analysis`` → ``stock_yuzh/data/analysis``（方案 §2.5，R-16 后路径）。
    """
    configured = config_manager.get("analysis.output_dir") if config_manager else None
    repo_root = Path(__file__).resolve().parents[3]  # analysis → src → backend → stock_yuzh
    if configured:
        candidate = Path(str(configured)).expanduser()
        return candidate if candidate.is_absolute() else (repo_root / candidate)
    return repo_root / "data" / "analysis"


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
