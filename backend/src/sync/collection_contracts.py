"""采集闭环公共契约（每日复盘采集与分析闭环技术方案 V1.0 §3.1/§3.2，D01）

三个可追溯对象贯穿全链路（技术方案 §1）：

- 采集作业与分区：``job_id`` / ``partition_id`` —— 哪个市场、哪一天、哪些代码未到齐；
- 固定输入与数据修订：``snapshot_id`` / ``revision_id`` —— 计算用了哪些数据；
- 已发布批次：``batch_id`` —— 当时发布了什么（见 dao_review_publication）。

本模块只放纯契约（枚举 + dataclass + 纯函数），不依赖数据库连接，便于单测与
下游（编排器、manager、source_expectations）统一引用。

边界约定：

- 日期统一 ``YYYYMMDD`` VARCHAR(8)（与 his_kline_day 等业务表一致）；
  交易日历表 base_trade_calendar 为带横线格式，进本契约前先归一；
- 时间统一带时区 datetime（入库 TIMESTAMPTZ）；
- 分区键构造必须走 :func:`build_partition_key` 单一入口——分区结果、修订记录
  和 advisory lock 必须共享同一键（技术方案 §4.1）。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional


def new_id() -> str:
    """应用侧生成 UUID（技术方案 §4.1：不依赖数据库扩展）"""
    return str(uuid.uuid4())


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def normalize_ymd(value: Any) -> Optional[str]:
    """任意日期输入归一为 YYYYMMDD；无法解析返回 None。

    兼容 '2026-11-19' / '20261119' / date 对象。
    """
    if value is None:
        return None
    s = str(value).strip().replace('-', '').replace('/', '')
    if len(s) == 8 and s.isdigit():
        return s
    return None


class RunMode(str, Enum):
    """运行模式（技术方案 §3.1：目标业务日与运行模式解耦）"""

    DAILY = 'DAILY'          # 日常盘后/跨零点补跑：目标日取最近已收盘交易日
    BACKFILL = 'BACKFILL'    # 显式区间/指定日补采：每个交易日产生独立分区
    INIT = 'INIT'            # 全量重建


class PartitionStatus(str, Enum):
    """分区状态（技术方案 §3.2 表）。续跑与下游行为见方案原文。"""

    COMPLETE = 'COMPLETE'                    # 所需页/代码/字段验收完成
    CONFIRMED_EMPTY = 'CONFIRMED_EMPTY'      # 源已发布且有证据证明该分区为空
    NOT_DUE = 'NOT_DUE'                      # 截止时刻尚未应发布（不算采集失败）
    PARTIAL = 'PARTIAL'                      # 部分代码/分页/字段有效，可续跑
    FAILED = 'FAILED'                        # 请求/解析/校验失败，保存错误类别
    BLOCKED = 'BLOCKED'                      # 上游不可用或源风控，独立源继续
    UNRECOVERABLE = 'UNRECOVERABLE'          # 历史快照无法恢复，永久保留证据缺口
    NOT_APPLICABLE = 'NOT_APPLICABLE'        # 该数据集不适用，不进应到分母
    PLANNED = 'PLANNED'                      # dry-run 计划结果，无写库无源请求


class AvailabilityBasis(str, Enum):
    """可知性依据（技术方案 §3.1：历史补采不能倒签成当时已经可知）"""

    SOURCE_TIMESTAMP = 'SOURCE_TIMESTAMP'            # 源报价时刻 ≤ 截止，严格"当时已知"
    SYSTEM_OBSERVED = 'SYSTEM_OBSERVED'              # 系统首次观察时刻 ≤ 截止（无源时刻）
    PUBLIC_RECONSTRUCTION = 'PUBLIC_RECONSTRUCTION'  # 公开可得重建，仅限研究视图
    LEGACY_UNKNOWN = 'LEGACY_UNKNOWN'                # 迁移 baseline，不证明历史时点可知


class Recoverability(str, Enum):
    RECOVERABLE = 'RECOVERABLE'
    PARTIALLY_RECOVERABLE = 'PARTIALLY_RECOVERABLE'
    UNRECOVERABLE = 'UNRECOVERABLE'


class MetricStatus(str, Enum):
    """指标状态（与分区状态分开；技术方案 §3.2）"""

    VALID = 'VALID'
    PARTIAL = 'PARTIAL'
    UNAVAILABLE = 'UNAVAILABLE'
    NOT_APPLICABLE = 'NOT_APPLICABLE'


# 机器可读原因码（技术方案 §3.2/§5.5 给出示例；新增按需补充，保持大写蛇形）
REASON_SOURCE_STALE = 'SOURCE_STALE'
REASON_DATE_UNVERIFIED = 'DATE_UNVERIFIED'
REASON_TIME_UNVERIFIED = 'TIME_UNVERIFIED'
REASON_NO_PAIRED_SAMPLE = 'NO_PAIRED_SAMPLE'
REASON_COHORT_UNKNOWN = 'COHORT_UNKNOWN'
REASON_NOT_PUBLISHED_YET = 'NOT_PUBLISHED_YET'
REASON_SOURCE_EMPTY_CONFIRMED = 'SOURCE_EMPTY_CONFIRMED'
REASON_REQUEST_FAILED = 'REQUEST_FAILED'
REASON_RATE_LIMITED = 'RATE_LIMITED'
REASON_DEPENDENCY_BLOCKED = 'DEPENDENCY_BLOCKED'
REASON_MISSING_PAGE = 'MISSING_PAGE'
REASON_TRUNCATED_PAGE = 'TRUNCATED_PAGE'
REASON_CALENDAR_UNKNOWN = 'CALENDAR_UNKNOWN'


@dataclass
class RunContext:
    """一次采集作业的统一上下文（技术方案 §3.1 契约示意）"""

    job_id: str
    target_date: str                    # A 股报告锚定日，YYYYMMDD
    run_mode: RunMode = RunMode.DAILY
    parent_job_id: Optional[str] = None
    cutoff_at: Optional[datetime] = None    # 带时区；入库 TIMESTAMPTZ
    calendar_version: Optional[str] = None  # 交易日历版本（读日历时记录）
    dry_run: bool = False

    def to_row(self) -> Dict[str, Any]:
        d = asdict(self)
        d['run_mode'] = self.run_mode.value
        return d


@dataclass
class PartitionResult:
    """单数据集×市场×业务日的验收结果（技术方案 §3.1 契约示意）"""

    dataset: str
    source: str
    market: str
    partition_key: str
    expected_date: Optional[str] = None        # 应到业务日 YYYYMMDD
    source_business_date: Optional[str] = None  # 源实际业务日（响应内证据）
    quote_at: Optional[datetime] = None         # 源报价时刻
    source_published_at: Optional[datetime] = None
    first_observed_at: Optional[datetime] = None
    availability_basis: Optional[AvailabilityBasis] = None
    status: PartitionStatus = PartitionStatus.FAILED
    reason_codes: List[str] = field(default_factory=list)
    expected_codes: Optional[int] = None        # 应到代码/记录数
    observed_codes: Optional[int] = None        # 实到有效数
    invalid_codes: Optional[int] = None         # 无效数（空值/字段缺失）
    missing_codes: Optional[int] = None         # 缺失数（observed + invalid + missing = expected）
    page_evidence: Optional[Dict[str, Any]] = None   # 页数/总数/页哈希等源证据
    quality: Optional[Dict[str, Any]] = None         # 质量标注（如封板待核）
    recoverability: Optional[Recoverability] = None
    revision_id: Optional[str] = None
    # 以下为编排侧补充字段（不入 partition_key 身份）
    job_id: Optional[str] = None
    partition_id: Optional[str] = None
    error_detail: Optional[str] = None

    def to_row(self, job_id: Optional[str] = None) -> Dict[str, Any]:
        """转 sync_partition 行（JSONB 字段在此序列化为字符串由 DAO 处理）"""
        row = {
            'partition_id': self.partition_id or new_id(),
            'job_id': self.job_id or job_id,
            'dataset': self.dataset,
            'source': self.source,
            'market': self.market,
            'partition_key': self.partition_key,
            'expected_date': self.expected_date,
            'source_business_date': self.source_business_date,
            'quote_at': self.quote_at,
            'source_published_at': self.source_published_at,
            'first_observed_at': self.first_observed_at or now_utc(),
            'availability_basis': (self.availability_basis.value
                                   if self.availability_basis else None),
            'status': self.status.value,
            'reason_codes': self.reason_codes or [],
            'expected_codes': self.expected_codes,
            'observed_codes': self.observed_codes,
            'invalid_codes': self.invalid_codes,
            'missing_codes': self.missing_codes,
            'page_evidence': self.page_evidence,
            'quality': self.quality,
            'recoverability': (self.recoverability.value
                               if self.recoverability else None),
            'revision_id': self.revision_id,
            'error_detail': self.error_detail,
        }
        return row


@dataclass
class RecomputeTaskRow:
    """重算任务行（技术方案 §6.3：状态机 + 租约）"""

    module: str
    module_version: str
    anchor_start: str
    anchor_end: str
    codes: List[str] = field(default_factory=list)
    scope_hash: str = ''
    trigger_revisions: List[str] = field(default_factory=list)
    required_input_hash: Optional[str] = None
    task_id: str = field(default_factory=new_id)
    status: str = 'PENDING'      # PENDING/RUNNING/SUCCEEDED/FAILED
    attempts: int = 0

    def scope_key(self) -> str:
        """唯一键除任务字段外的 scope 标识（scope_hash 由调用方以有序代码集+日期范围计算）"""
        return f'{self.module}|{self.module_version}|{self.anchor_start}|{self.anchor_end}|{self.scope_hash}'


def build_partition_key(target_date: str, dataset: str, market: str,
                        exchange: Optional[str] = None,
                        sub_dataset: Optional[str] = None,
                        code: Optional[str] = None) -> str:
    """统一分区键构造器（技术方案 §4.1：分区结果、修订记录和锁必须调用同一构造器）。

    规范：业务日期|市场|交易所|数据集|子数据集[|代码]，逐标的行情才包含代码。
    段内禁止出现 '|'（构造时以 None 表达"不适用"，不越级拼接）。
    """
    td = normalize_ymd(target_date)
    if not td:
        raise ValueError(f'partition_key 目标日非法: {target_date!r}')
    segments = [td, market or '', exchange or '', dataset or '',
                sub_dataset or '']
    if code is not None:
        if '|' in code:
            raise ValueError(f'code 含分隔符: {code!r}')
        segments.append(code)
    for seg in segments[:-1] if code is not None else segments:
        if '|' in str(seg):
            raise ValueError(f'分区键段含分隔符: {seg!r}')
    return '|'.join(segments)


def parse_partition_key(key: str) -> Dict[str, Optional[str]]:
    """分区键解析（与 build_partition_key 对称；末段可能是代码）"""
    parts = key.split('|')
    base = {'target_date': parts[0], 'market': parts[1], 'exchange': parts[2],
            'dataset': parts[3], 'sub_dataset': parts[4] if len(parts) > 4 else None,
            'code': parts[5] if len(parts) > 5 else None}
    return base


def resolve_target_date(run_mode: RunMode,
                        explicit_target: Optional[str],
                        latest_closed: str) -> str:
    """目标日解析（技术方案 §3.1：DAILY 缺省取最近已收盘交易日；跨零点补跑同口径）。

    Args:
        run_mode: 运行模式
        explicit_target: 显式指定的业务日（BACKFILL 必填；DAILY 可空）
        latest_closed: 最近已收盘交易日（YYYYMMDD，由调用方查日历得到）

    Returns:
        YYYYMMDD

    Raises:
        ValueError: BACKFILL/INIT 未提供显式目标日，或显式日非法。
    """
    if run_mode == RunMode.DAILY:
        return normalize_ymd(latest_closed) or (
            normalize_ymd(explicit_target) or '')
    td = normalize_ymd(explicit_target)
    if not td:
        raise ValueError(f'{run_mode.value} 模式必须显式指定目标业务日')
    return td


def entry_to_partition_result(entry: Dict[str, Any], dataset: str, market: str,
                              target_date: str, source: Optional[str] = None,
                              exchange: Optional[str] = None,
                              as_of: Optional[datetime] = None) -> PartitionResult:
    """编排器 summary entry → PartitionResult（编排器最小接入用映射）。

    现行源状态（成功/失败/待补/跳过）映射到分区状态：
    - 成功 → COMPLETE（行数>0）/ CONFIRMED_EMPTY（行数=0 且源正常返回）
    - 失败 → FAILED（原因码 REQUEST_FAILED）
    - 待补 → PARTIAL（数据日滞后/风控冷却后仍失败 → RATE_LIMITED）
    - 跳过 → NOT_APPLICABLE（未注册/被门控）/ BLOCKED（日K门控未过）
    """
    status_raw = str(entry.get('status') or '')
    rows = int(entry.get('rows') or 0)
    detail = str(entry.get('detail') or '') or None
    reason_codes: List[str] = []
    if status_raw == '成功':
        status = PartitionStatus.COMPLETE if rows > 0 else PartitionStatus.CONFIRMED_EMPTY
        if rows == 0:
            reason_codes.append(REASON_SOURCE_EMPTY_CONFIRMED)
    elif status_raw == '失败':
        status = PartitionStatus.FAILED
        reason_codes.append(REASON_REQUEST_FAILED)
    elif status_raw == '待补':
        status = PartitionStatus.PARTIAL
        # 结构化 kind 优先（限流方案 §1）；'风控' 子串仅迁移期回退
        if entry.get('blocked_kind') or '风控' in (detail or ''):
            reason_codes.append(REASON_RATE_LIMITED)
        else:
            reason_codes.append(REASON_SOURCE_STALE)
    else:  # 跳过
        detail_l = detail or ''
        if '日K' in detail_l or '门控' in detail_l or '依赖' in detail_l:
            status = PartitionStatus.BLOCKED
            reason_codes.append(REASON_DEPENDENCY_BLOCKED)
        else:
            status = PartitionStatus.NOT_APPLICABLE
    return PartitionResult(
        dataset=dataset,
        source=source or dataset,
        market=market,
        partition_key=build_partition_key(target_date, dataset, market,
                                          exchange=exchange),
        expected_date=target_date,
        status=status,
        reason_codes=reason_codes,
        observed_codes=rows or None,
        error_detail=detail,
        first_observed_at=as_of or now_utc(),
        availability_basis=(AvailabilityBasis.SYSTEM_OBSERVED
                            if status in (PartitionStatus.COMPLETE,
                                          PartitionStatus.CONFIRMED_EMPTY,
                                          PartitionStatus.PARTIAL)
                            else None),
    )
