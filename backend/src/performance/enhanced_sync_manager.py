"""
增强的同步管理器
集成性能监控功能，用于分析基本面数据采集性能
"""

from typing import Dict, Any, List, Optional
from datetime import date, datetime
from ..config import ConfigManager
from ..sync.sync_manager import SyncManager
from ..data_sources.baostock_source import BaostockSource
from .performance_decorator import monitor_performance, monitor_api_calls, monitor_db_operations, PerformanceContext
from .performance_monitor import performance_monitor


class EnhancedBaostockSource(BaostockSource):
    """增强的Baostock数据源，集成性能监控"""

    @monitor_api_calls("baostock_login")
    def connect(self) -> bool:
        """连接baostock（带性能监控）"""
        return super().connect()

    @monitor_api_calls("baostock_logout")
    def disconnect(self) -> None:
        """断开baostock连接（带性能监控）"""
        return super().disconnect()

    @monitor_api_calls("baostock_stock_list")
    def get_stock_list(self) -> List[Dict[str, Any]]:
        """获取股票列表（带性能监控）"""
        result = super().get_stock_list()
        if result:
            performance_monitor.record_stock_processed()
            performance_monitor.record_data_records(len(result))
        return result

    @monitor_api_calls("baostock_financial_data")
    def get_stock_fundamentals(self, ts_code: str, year: int = None, quarter: int = None) -> Optional[Dict[str, Any]]:
        """获取基本面数据（带性能监控）"""
        result = super().get_stock_fundamentals(ts_code, year, quarter)
        if result:
            performance_monitor.record_stock_processed()
            performance_monitor.record_data_records(1)
        return result


class EnhancedSyncManager(SyncManager):
    """增强的同步管理器，集成性能监控"""

    def __init__(self, config_manager: ConfigManager = None):
        super().__init__(config_manager)
        # 替换为增强的数据源
        self.baostock_source = EnhancedBaostockSource({
            'data_path': self.config_manager.get_data_paths().get('csv'),
            'rate_limit': {
                'enabled': True,
                'calls_per_period': 50,
                'sleep_duration': 1.0
            }
        })

    @monitor_performance("sync_all_data")
    def sync_all(self, save_to_csv: bool = True, save_to_db: bool = True) -> Dict[str, Any]:
        """同步所有数据（带性能监控）"""
        return super().sync_all(save_to_csv, save_to_db)

    @monitor_performance("sync_stock_list")
    def sync_stocks(self, save_to_csv: bool = True, save_to_db: bool = True) -> List[Dict[str, Any]]:
        """同步股票列表（带性能监控）"""
        return super().sync_stocks(save_to_csv, save_to_db)

    @monitor_performance("sync_financial_data")
    def sync_financial_data(
        self,
        save_to_csv: bool = True,
        save_to_db: bool = True,
        year: int = None,
        quarter: int = None,
        codes: List[str] = None
    ) -> int:
        """同步基本面数据（带性能监控）"""
        return super().sync_financial_data(save_to_csv, save_to_db, year, quarter, codes)

    @monitor_db_operations("save_stocks_to_db")
    def _save_stocks_to_db(self, stocks: List[Dict[str, Any]]) -> None:
        """保存股票数据到数据库（带性能监控）"""
        super()._save_stocks_to_db(stocks)

    @monitor_db_operations("save_fundamentals_to_db")
    def _save_fundamentals_data_to_db(self, financial_data: List[Dict[str, Any]]) -> None:
        """保存基本面数据到数据库（带性能监控）"""
        super()._save_fundamentals_data_to_db(financial_data)

    def run_performance_test(self, test_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        运行性能测试

        Args:
            test_config: 测试配置
                - test_types: 测试类型列表 ['stocks', 'financial_data']
                - stock_count: 测试股票数量
                - environment: 环境名称

        Returns:
            测试结果
        """
        test_results = {
            'test_config': test_config,
            'start_time': datetime.now(),
            'results': {},
            'performance_metrics': {},
            'errors': []
        }

        try:
            test_types = test_config.get('test_types', ['financial_data'])
            stock_count = test_config.get('stock_count', 100)

            # 获取股票列表
            print(f"获取测试股票列表，限制数量：{stock_count}")
            stocks = self._get_test_stocks(stock_count)

            if not stocks:
                raise Exception("无法获取测试股票列表")

            print(f"实际测试股票数量：{len(stocks)}")

            # 执行各类测试
            for test_type in test_types:
                print(f"\n开始执行测试：{test_type}")
                try:
                    if test_type == 'stocks':
                        result = self._test_stock_list_sync()
                    elif test_type == 'financial_data':
                        result = self._test_financial_data_sync(stocks)
                    else:
                        print(f"❌ 不支持的测试类型：{test_type}")
                        continue

                    test_results['results'][test_type] = result

                    # 获取性能指标
                    metrics = performance_monitor.get_metrics_history(test_type, hours=1)
                    if metrics:
                        latest_metrics = metrics[-1]
                        test_results['performance_metrics'][test_type] = latest_metrics.to_dict()

                    print(f"✅ 测试 {test_type} 完成")

                except Exception as e:
                    error_msg = f"测试 {test_type} 失败：{str(e)}"
                    print(f"❌ {error_msg}")
                    test_results['errors'].append(error_msg)

        except Exception as e:
            error_msg = f"性能测试执行失败：{str(e)}"
            print(f"❌ {error_msg}")
            test_results['errors'].append(error_msg)

        test_results['end_time'] = datetime.now()
        test_results['total_test_duration'] = (
            test_results['end_time'] - test_results['start_time']
        ).total_seconds()

        return test_results

    def _get_test_stocks(self, count: int) -> List[Dict[str, Any]]:
        """获取测试用股票列表"""
        try:
            # 从数据库获取股票列表
            query = f"""
            SELECT ts_code, stock_code, stock_name
            FROM base_stock_info
            WHERE list_status = 'L'
            ORDER BY ts_code
            LIMIT {count}
            """
            stocks = self.db_conn.fetch_all(query)

            if not stocks:
                print("⚠️ 数据库中没有股票数据，尝试从Baostock获取")
                stocks = self.sync_stocks(False, False)
                stocks = stocks[:count]

            print(f"获取到 {len(stocks)} 只股票用于测试")
            return stocks

        except Exception as e:
            print(f"❌ 获取测试股票失败：{e}")
            return []

    def _test_stock_list_sync(self) -> Dict[str, Any]:
        """测试股票列表同步性能"""
        with PerformanceContext("test_stock_list") as metrics:
            stocks = self.sync_stocks(False, False)  # 不保存，只测试同步

            return {
                'stocks_count': len(stocks),
                'duration': metrics.duration,
                'success': len(stocks) > 0
            }

    def _test_financial_data_sync(self, stocks: List[Dict[str, Any]]) -> Dict[str, Any]:
        """测试基本面数据同步性能"""
        with PerformanceContext("test_financial_data") as metrics:
            codes = [stock['stock_code'] for stock in stocks]

            # 执行基本面数据同步
            data_count = self.sync_financial_data(
                save_to_csv=False,  # 不保存CSV，专注测试API性能
                save_to_db=False,   # 不保存DB，专注测试API性能
                codes=codes
            )

            return {
                'stocks_tested': len(codes),
                'data_records': data_count,
                'success_rate': data_count / len(codes) if codes else 0,
                'duration': metrics.duration,
                'throughput': len(codes) / max(metrics.duration, 0.001)  # 股票/秒
            }

    def generate_performance_dashboard(self) -> Dict[str, Any]:
        """生成性能仪表板数据"""
        dashboard = {
            'real_time_metrics': performance_monitor.get_real_time_metrics(),
            'recent_reports': {},
            'system_info': self._get_system_info(),
            'generated_at': datetime.now()
        }

        # 最近24小时的报告摘要
        operations = ['sync_stock_list', 'sync_financial_data', 'test_stock_list', 'test_financial_data']
        for op in operations:
            recent_metrics = performance_monitor.get_metrics_history(op, hours=24)
            if recent_metrics:
                total_executions = len(recent_metrics)
                avg_duration = sum(m.duration for m in recent_metrics) / total_executions
                total_stocks = sum(m.stocks_processed for m in recent_metrics)
                success_rate = sum(1 for m in recent_metrics if m.error_count == 0) / total_executions

                dashboard['recent_reports'][op] = {
                    'total_executions': total_executions,
                    'avg_duration': avg_duration,
                    'total_stocks': total_stocks,
                    'success_rate': success_rate
                }

        return dashboard

    def _get_system_info(self) -> Dict[str, Any]:
        """获取系统信息"""
        try:
            import psutil
            return {
                'cpu_count': psutil.cpu_count(),
                'memory_total': psutil.virtual_memory().total / 1024 / 1024 / 1024,  # GB
                'memory_available': psutil.virtual_memory().available / 1024 / 1024 / 1024,  # GB
                'disk_usage': psutil.disk_usage('/').percent
            }
        except Exception:
            return {
                'cpu_count': 'N/A',
                'memory_total': 'N/A',
                'memory_available': 'N/A',
                'disk_usage': 'N/A'
            }