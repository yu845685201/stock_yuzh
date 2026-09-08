"""
基本面全量重刷 runner（可断点续传）

按 V3.0 方案 §5.3 设计：
- 任务全集：base_stock_info(type='1'，含退市股) × 各股上市以来全部报告期
- baostock 单会话串行（不支持并发），_execute_query_with_retry 自带断线重连
- manifest 文件断点（表清空后 DB 不能作断点），按股票粒度记录完成状态
- upsert 幂等（冲突键 ts_code+stat_date），失败重试后跳过、次轮可续
- 0 记录股票二次重试（区分无数据与调用失败）
"""
import json
import logging
import socket
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..config.config_manager import ConfigManager
from ..database.connection import DatabaseConnection
from ..data_sources.baostock_source import BaostockSource, BaostockBlacklistError


class BaostockNetworkDeadError(RuntimeError):
    """网络探针失败：连续无返回且探针无数据，判定 baostock 链路不可用"""
from ..utils.quarter_calculator import calculate_start_quarter, get_current_previous_quarter

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
DEFAULT_MANIFEST = ROOT / 'tmp' / 'fundamentals_rebuild_manifest.json'


class FundamentalsRebuildManager:
    """基本面全量重刷：串行 + manifest 断点 + 幂等 upsert"""

    def __init__(self, config_manager: ConfigManager, manifest_path: Optional[Path] = None):
        # 防 baostock/DB 挂死：必须在任何 socket 创建之前设置（连接池会预建连接，
        # 后创建的 socket 才继承该默认超时）——上次挂死正是补丁位置在连接池之后所致
        self.socket_timeout = int(config_manager.get('sync.fundamentals_rebuild_socket_timeout', 60))
        socket.setdefaulttimeout(self.socket_timeout)

        self.config_manager = config_manager
        self.config = config_manager.load_config()
        self.db = DatabaseConnection(config_manager)
        self.csv_writer = None  # 惰性创建，--no-csv 时不建
        self.baostock = BaostockSource(self.config.get('data_sources.baostock', {}))
        self.manifest_path = Path(manifest_path) if manifest_path else DEFAULT_MANIFEST
        self.sleep_per_stock = float(self.config.get('sync.fundamentals_rebuild_sleep', 0.02))
        self.batch_size = int(self.config.get('sync.fundamentals_rebuild_batch_size', 500))
        # 防 baostock 封禁：每次调用的间隔 + 每 N 次调用回收会话（重新 login）
        self.call_sleep = float(self.config.get('sync.fundamentals_rebuild_call_sleep', 0.3))
        self.recycle_calls = int(self.config.get('sync.fundamentals_rebuild_recycle_calls', 300))

    # ---------- manifest ----------

    def _load_manifest(self) -> Dict[str, Any]:
        if self.manifest_path.exists():
            try:
                return json.loads(self.manifest_path.read_text(encoding='utf-8'))
            except Exception as e:
                logger.warning(f"manifest 读取失败，重新开始: {e}")
        return {'completed': {}, 'failed': {}, 'started_at': None, 'updated_at': None}

    def _save_manifest(self, manifest: Dict[str, Any]):
        manifest['updated_at'] = datetime.now().isoformat(timespec='seconds')
        self.manifest_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.manifest_path.with_suffix('.tmp')
        tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=1), encoding='utf-8')
        tmp.replace(self.manifest_path)

    # ---------- 任务全集 ----------

    def build_task_list(self, ts_codes: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """base_stock_info(type=股票) × 上市以来全部报告期；退市股以 delist_date 封顶"""
        stocks = self.db.execute_query("""
            SELECT ts_code, stock_code, stock_name, list_date, delist_date
            FROM base_stock_info
            WHERE type = '1'
            ORDER BY ts_code""")
        if ts_codes:
            wanted = set(ts_codes)
            stocks = [s for s in stocks if s['ts_code'] in wanted]

        end_year, end_quarter = get_current_previous_quarter()
        tasks = []
        for s in stocks:
            start_year, start_quarter = calculate_start_quarter(s.get('list_date'))
            ey, eq = end_year, end_quarter
            delist_date = s.get('delist_date')
            if delist_date and isinstance(delist_date, str) and len(delist_date) == 8:
                try:
                    dy = int(delist_date[:4])
                    dq = (int(delist_date[4:6]) - 1) // 3 + 1
                    if (dy, dq) < (ey, eq):
                        ey, eq = dy, dq
                except ValueError:
                    pass
            quarters = []
            for y in range(start_year, ey + 1):
                qs = start_quarter if y == start_year else 1
                qe = eq if y == ey else 4
                for q in range(qs, qe + 1):
                    quarters.append((y, q))
            tasks.append({'stock': s, 'quarters': quarters})
        return tasks

    # ---------- 主流程 ----------

    def execute(self, ts_codes: Optional[List[str]] = None, resume: bool = True,
                save_to_csv: bool = True, save_to_db: bool = True) -> Dict[str, Any]:
        started = datetime.now()
        if save_to_csv:
            from ..sync.csv_writer import CsvWriter
            self.csv_writer = CsvWriter(self.config_manager)

        tasks = self.build_task_list(ts_codes)
        manifest = self._load_manifest() if resume else {
            'completed': {}, 'failed': {}, 'started_at': None, 'updated_at': None}
        manifest.setdefault('started_at', started.isoformat(timespec='seconds'))

        total_calls = sum(len(t['quarters']) for t in tasks)
        logger.info(f"任务全集：{len(tasks)} 只股票，{total_calls} 个季度调用；"
                    f"manifest 已完成 {len(manifest['completed'])}，跳过")

        if not self.baostock.connect():
            raise RuntimeError("baostock 连接失败")

        stats = {
            'stocks_done': 0, 'stocks_skipped': 0, 'stocks_zero_retry': 0,
            'calls': 0, 'records': 0, 'no_data_quarters': 0,
            'failed_stocks': 0, 'csv_flushes': 0,
        }
        csv_buffer: List[Dict[str, Any]] = []
        zero_record_stocks: List[Dict[str, Any]] = []
        result = {'success': False, 'stats': stats, 'report_path': None,
                  'errors': [], 'manifest_path': str(self.manifest_path)}

        try:
            for idx, task in enumerate(tasks):
                stock = task['stock']
                ts_code = stock['ts_code']
                if ts_code in manifest['completed']:
                    stats['stocks_skipped'] += 1
                    continue

                records, calls_used, no_data = self._collect_stock(stock, task['quarters'], stats)
                stats['calls'] += calls_used

                if records:
                    if save_to_db:
                        self.db.upsert_fundamentals_data(records)
                    stats['records'] += len(records)
                    if self.csv_writer is not None:
                        csv_buffer.extend(self._to_csv_rows(stock, records))
                elif calls_used > 0:
                    # 全部季度无返回：可能是调用失败而非真无数据，末尾重试
                    zero_record_stocks.append(task)

                manifest['completed'][ts_code] = {
                    'quarters': len(task['quarters']), 'records': len(records),
                    'no_data': no_data}
                manifest['failed'].pop(ts_code, None)

                if len(csv_buffer) >= self.batch_size and self.csv_writer is not None:
                    self.csv_writer.write_base_fundamentals_info(csv_buffer)
                    stats['csv_flushes'] += 1
                    csv_buffer = []

                self._save_manifest(manifest)
                stats['stocks_done'] += 1

                if stats['stocks_done'] % 25 == 0:
                    done = stats['stocks_done'] + stats['stocks_skipped']
                    elapsed = (datetime.now() - started).total_seconds()
                    eta_min = elapsed / done * (len(tasks) - done) / 60 if done else 0
                    logger.info(f"进度 {done}/{len(tasks)}，记录 {stats['records']}，"
                                f"调用 {stats['calls']}，ETA {eta_min:.0f} 分钟")

                time.sleep(self.sleep_per_stock)

            # 零记录股票二次重试
            if zero_record_stocks:
                logger.warning(f"零记录股票二次重试：{len(zero_record_stocks)} 只")
                for task in zero_record_stocks:
                    stock = task['stock']
                    ts_code = stock['ts_code']
                    try:
                        records, calls_used, no_data = self._collect_stock(stock, task['quarters'], stats)
                    except (BaostockBlacklistError, BaostockNetworkDeadError):
                        raise
                    stats['calls'] += calls_used
                    if records:
                        if save_to_db:
                            self.db.upsert_fundamentals_data(records)
                        stats['records'] += len(records)
                        if self.csv_writer is not None:
                            csv_buffer.extend(self._to_csv_rows(stock, records))
                        manifest['completed'][ts_code] = {
                            'quarters': len(task['quarters']), 'records': len(records),
                            'no_data': no_data}
                        stats['stocks_zero_retry'] += 1
                    else:
                        manifest['failed'][ts_code] = '全部季度无返回（重试后）'
                        stats['failed_stocks'] += 1
                    self._save_manifest(manifest)
                    time.sleep(self.sleep_per_stock)

            if csv_buffer and self.csv_writer is not None:
                self.csv_writer.write_base_fundamentals_info(csv_buffer)
                stats['csv_flushes'] += 1

            result['success'] = True
        except BaostockBlacklistError as e:
            # 封禁：立即停止（当前股票不标记完成），manifest 保留已完成进度
            logger.error(f"baostock 封禁，重刷急停（断点已保存，稍后可续跑）: {e}")
            result['errors'].append(f'baostock 封禁急停: {e}')
            result['blocked'] = True
        except BaostockNetworkDeadError as e:
            # 网络探针失败：链路不可用，急停交由监督循环稍后重启
            logger.error(f"baostock 链路不可用，重刷急停（断点已保存）: {e}")
            result['errors'].append(f'网络死亡急停: {e}')
            result['blocked'] = True
        except Exception as e:
            logger.exception(f"重刷异常终止: {e}")
            result['errors'].append(str(e))
        finally:
            self.baostock.disconnect()
            self._save_manifest(manifest)
            result['stats'] = stats
            result['duration'] = (datetime.now() - started).total_seconds()
            result['report_path'] = self._write_report(result, manifest, len(tasks))

        return result

    def _force_recycle(self, stats: Dict[str, Any]) -> None:
        """强制回收会话（断线重连）"""
        logger.warning("强制回收 baostock 会话")
        self.baostock.disconnect()
        if not self.baostock.connect():
            raise BaostockNetworkDeadError("会话回收后重连失败")
        stats['calls_since_recycle'] = 0
        stats['session_recycles'] = stats.get('session_recycles', 0) + 1

    def _probe_network(self) -> bool:
        """网络健康探针：查询已知必有数据的 (sz.000001, 2025Q3)"""
        try:
            return self.baostock.get_stock_fundamentals('sz.000001', year=2025, quarter=3) is not None
        except BaostockBlacklistError:
            raise
        except Exception:
            return False

    def _collect_stock(self, stock: Dict[str, Any], quarters: List[Tuple[int, int]],
                       stats: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int, int]:
        """采集单只股票全部季度；返回 (records, 调用次数, no_data 季度数)

        连续 3 个季度无返回 → 强制回收会话 + 网络探针：
        - 探针有数据 → 网络正常，重试当前季度（结果可信）
        - 探针无数据 → 链路不可用，抛出急停（由监督循环稍后重启）
        """
        ts_code = stock['ts_code']
        records: List[Dict[str, Any]] = []
        no_data = 0
        consecutive_none = 0
        for (y, q) in quarters:
            if self.call_sleep > 0:
                time.sleep(self.call_sleep)
            # 会话回收：防止单会话调用计数触发服务端封禁
            stats['calls_since_recycle'] = stats.get('calls_since_recycle', 0) + 1
            if self.recycle_calls > 0 and stats['calls_since_recycle'] >= self.recycle_calls:
                logger.info(f"会话回收（已 {stats['calls_since_recycle']} 次调用），重新登录")
                self._force_recycle(stats)
            try:
                fundamentals = self.baostock.get_stock_fundamentals(ts_code, year=y, quarter=q)
            except BaostockBlacklistError:
                raise
            except Exception as e:
                logger.warning(f"{ts_code} {y}Q{q} 调用异常: {e}")
                fundamentals = None

            if fundamentals is None:
                consecutive_none += 1
                no_data += 1
                if consecutive_none >= 3:
                    logger.warning(f"{ts_code} {y}Q{q} 连续 {consecutive_none} 个季度无返回，"
                                   f"强制回收会话 + 网络探针")
                    self._force_recycle(stats)
                    if not self._probe_network():
                        raise BaostockNetworkDeadError(
                            f"探针无数据（{ts_code} 连续无返回触发）")
                    # 探针证明链路正常，重试当前季度一次
                    logger.info("网络探针通过，重试当前季度")
                    try:
                        fundamentals = self.baostock.get_stock_fundamentals(ts_code, year=y, quarter=q)
                    except BaostockBlacklistError:
                        raise
                    except Exception:
                        fundamentals = None
                    consecutive_none = 0
            else:
                consecutive_none = 0

            if fundamentals:
                fundamentals['stock_name'] = stock.get('stock_name')
                records.append(fundamentals)
        return records, len(quarters), no_data

    @staticmethod
    def _to_csv_rows(stock: Dict[str, Any], records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows = []
        for r in records:
            rows.append({
                'ts_code': r.get('ts_code'),
                'stock_code': r.get('stock_code'),
                'stock_name': stock.get('stock_name'),
                'stat_date': r.get('stat_date'),
                'disclosure_date': r.get('disclosure_date'),
                'total_share': r.get('total_share'),
                'float_share': r.get('float_share'),
            })
        return rows

    def _write_report(self, result: Dict[str, Any], manifest: Dict[str, Any], total_tasks: int) -> Optional[str]:
        stats = result['stats']
        report_dir = ROOT.parent / 'doc' / 'reports'
        report_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        path = report_dir / f'fundamentals_rebuild_report_{ts}.md'
        duration_min = result.get('duration', 0) / 60
        path.write_text(f"""# 基本面全量重刷报告 {ts}

- 状态：{'成功' if result['success'] else '异常终止'}
- 任务：{total_tasks} 只股票（本轮完成 {stats['stocks_done']}，跳过 {stats['stocks_skipped']}）
- 季度调用：{stats['calls']}；upsert 记录：{stats['records']}
- 零记录二次重试找回：{stats['stocks_zero_retry']}；失败股票：{stats['failed_stocks']}
- manifest：{self.manifest_path}（已完成 {len(manifest['completed'])}，失败 {len(manifest['failed'])}）
- 耗时：{duration_min:.0f} 分钟
""", encoding='utf-8')
        return str(path)
