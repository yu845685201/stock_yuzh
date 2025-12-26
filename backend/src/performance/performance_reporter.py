"""
性能报告生成器
生成详细的性能分析报告和优化建议
"""

import os
import json
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from .performance_monitor import PerformanceMetrics


@dataclass
class PerformanceSummary:
    """性能摘要数据类"""
    operation_name: str
    total_executions: int
    avg_duration: float
    min_duration: float
    max_duration: float
    total_stocks: int
    avg_throughput: float  # 股票/秒
    api_success_rate: float
    db_success_rate: float
    error_rate: float
    avg_memory_usage: float
    bottleneck_score: float  # 0-100，越高表示瓶颈越严重


class PerformanceReporter:
    """性能报告生成器"""

    def __init__(self, report_dir: str = "reports/performance"):
        self.report_dir = report_dir
        os.makedirs(report_dir, exist_ok=True)

    def generate_comprehensive_report(self, operation: str, hours: int = 24) -> Dict[str, Any]:
        """
        生成综合性能报告

        Args:
            operation: 操作名称
            hours: 分析时间范围（小时）

        Returns:
            性能报告
        """
        try:
            from ..performance_monitor import performance_monitor

            # 获取性能指标
            metrics_list = performance_monitor.get_metrics_history(operation, hours)

            if not metrics_list:
                return {
                    'error': f'没有找到操作 "{operation}" 在过去 {hours} 小时内的性能数据'
                }

            # 生成性能摘要
            summary = self._generate_performance_summary(operation, metrics_list)

            # 瓶颈分析
            bottleneck_analysis = self._analyze_bottlenecks(metrics_list)

            # 优化建议
            recommendations = self._generate_optimization_recommendations(summary, bottleneck_analysis)

            # 生成图表数据
            chart_data = self._generate_chart_data(metrics_list)

            report = {
                'report_info': {
                    'operation': operation,
                    'time_range_hours': hours,
                    'generated_at': datetime.now(),
                    'total_metrics': len(metrics_list)
                },
                'performance_summary': asdict(summary),
                'bottleneck_analysis': bottleneck_analysis,
                'optimization_recommendations': recommendations,
                'chart_data': chart_data,
                'detailed_metrics': [m.to_dict() for m in metrics_list]
            }

            # 保存报告
            self._save_report(report, operation)

            return report

        except Exception as e:
            return {
                'error': f'生成报告失败：{str(e)}'
            }

    def _generate_performance_summary(self, operation: str, metrics_list: List[PerformanceMetrics]) -> PerformanceSummary:
        """生成性能摘要"""
        total_executions = len(metrics_list)
        durations = [m.duration for m in metrics_list]
        stocks_processed = [m.stocks_processed for m in metrics_list]
        throughputs = [m.throughput for m in metrics_list if m.throughput > 0]

        api_success_rates = [m.api_success / max(m.api_calls, 1) for m in metrics_list if m.api_calls > 0]
        db_success_rates = [m.db_success / max(m.db_operations, 1) for m in metrics_list if m.db_operations > 0]
        error_rates = [m.error_count / max(m.stocks_processed, 1) for m in metrics_list if m.stocks_processed > 0]

        avg_memory_usage = [m.memory_peak for m in metrics_list if m.memory_peak > 0]

        summary = PerformanceSummary(
            operation_name=operation,
            total_executions=total_executions,
            avg_duration=sum(durations) / total_executions,
            min_duration=min(durations),
            max_duration=max(durations),
            total_stocks=sum(stocks_processed),
            avg_throughput=sum(throughputs) / len(throughputs) if throughputs else 0,
            api_success_rate=sum(api_success_rates) / len(api_success_rates) if api_success_rates else 1.0,
            db_success_rate=sum(db_success_rates) / len(db_success_rates) if db_success_rates else 1.0,
            error_rate=sum(error_rates) / len(error_rates) if error_rates else 0.0,
            avg_memory_usage=sum(avg_memory_usage) / len(avg_memory_usage) if avg_memory_usage else 0,
            bottleneck_score=self._calculate_bottleneck_score(metrics_list)
        )

        return summary

    def _calculate_bottleneck_score(self, metrics_list: List[PerformanceMetrics]) -> float:
        """计算瓶颈评分（0-100，越高表示瓶颈越严重）"""
        if not metrics_list:
            return 0

        scores = []

        # API成功率瓶颈（权重：30%）
        avg_api_success_rate = sum(m.api_success / max(m.api_calls, 1) for m in metrics_list if m.api_calls > 0) / len([m for m in metrics_list if m.api_calls > 0]) if [m for m in metrics_list if m.api_calls > 0] else 1.0
        api_score = (1.0 - avg_api_success_rate) * 30
        scores.append(api_score)

        # 吞吐量瓶颈（权重：25%）
        avg_throughput = sum(m.throughput for m in metrics_list if m.throughput > 0) / len([m for m in metrics_list if m.throughput > 0]) if [m for m in metrics_list if m.throughput > 0] else 0
        # 假设理想吞吐量是50股票/秒
        throughput_score = max(0, (50 - avg_throughput) / 50 * 25)
        scores.append(throughput_score)

        # 错误率瓶颈（权重：20%）
        avg_error_rate = sum(m.error_count / max(m.stocks_processed, 1) for m in metrics_list if m.stocks_processed > 0) / len([m for m in metrics_list if m.stocks_processed > 0]) if [m for m in metrics_list if m.stocks_processed > 0] else 0
        error_score = min(avg_error_rate * 100, 20)
        scores.append(error_score)

        # 内存使用瓶颈（权重：15%）
        avg_memory = sum(m.memory_peak for m in metrics_list if m.memory_peak > 0) / len([m for m in metrics_list if m.memory_peak > 0]) if [m for m in metrics_list if m.memory_peak > 0] else 0
        # 假设内存使用超过1GB为瓶颈
        memory_score = max(0, (avg_memory - 1000) / 1000 * 15)
        scores.append(memory_score)

        # 执行时间稳定性（权重：10%）
        durations = [m.duration for m in metrics_list]
        if len(durations) > 1:
            avg_duration = sum(durations) / len(durations)
            variance = sum((d - avg_duration) ** 2 for d in durations) / len(durations)
            cv = (variance ** 0.5) / avg_duration  # 变异系数
            stability_score = min(cv * 10, 10)
        else:
            stability_score = 0
        scores.append(stability_score)

        return min(sum(scores), 100)

    def _analyze_bottlenecks(self, metrics_list: List[PerformanceMetrics]) -> Dict[str, Any]:
        """分析性能瓶颈"""
        bottlenecks = []

        # API瓶颈分析
        api_calls = [m.api_calls for m in metrics_list]
        api_success_rates = [m.api_success / max(m.api_calls, 1) for m in metrics_list if m.api_calls > 0]
        avg_api_success_rate = sum(api_success_rates) / len(api_success_rates) if api_success_rates else 1.0

        if avg_api_success_rate < 0.95:
            bottlenecks.append({
                'type': 'API瓶颈',
                'severity': 'high' if avg_api_success_rate < 0.9 else 'medium',
                'description': f'API成功率仅为{avg_api_success_rate:.2%}，低于95%阈值',
                'impact': '直接影响数据采集完整性',
                'metrics': {
                    'avg_success_rate': avg_api_success_rate,
                    'total_calls': sum(api_calls),
                    'total_failures': sum(m.api_failures for m in metrics_list)
                }
            })

        # 吞吐量瓶颈分析
        throughputs = [m.throughput for m in metrics_list if m.throughput > 0]
        if throughputs:
            avg_throughput = sum(throughputs) / len(throughputs)
            if avg_throughput < 10:  # 假设10股票/秒为最低可接受吞吐量
                bottlenecks.append({
                    'type': '吞吐量瓶颈',
                    'severity': 'high' if avg_throughput < 5 else 'medium',
                    'description': f'平均吞吐量仅为{avg_throughput:.2f}股票/秒，性能偏低',
                    'impact': '影响大规模数据采集效率',
                    'metrics': {
                        'avg_throughput': avg_throughput,
                        'min_throughput': min(throughputs),
                        'max_throughput': max(throughputs)
                    }
                })

        # 内存使用瓶颈分析
        memory_peaks = [m.memory_peak for m in metrics_list if m.memory_peak > 0]
        if memory_peaks:
            avg_memory = sum(memory_peaks) / len(memory_peaks)
            max_memory = max(memory_peaks)
            if max_memory > 2000:  # 2GB
                bottlenecks.append({
                    'type': '内存瓶颈',
                    'severity': 'high' if max_memory > 4000 else 'medium',
                    'description': f'内存峰值使用{max_memory:.1f}MB，可能存在内存泄漏',
                    'impact': '影响系统稳定性，可能导致OOM',
                    'metrics': {
                        'avg_memory': avg_memory,
                        'max_memory': max_memory,
                        'memory_growth': max_memory - min(memory_peaks)
                    }
                })

        # 数据库瓶颈分析
        db_response_times = []
        for m in metrics_list:
            if m.db_operations > 0:
                db_response_times.append(m.db_response_time / m.db_operations)

        if db_response_times:
            avg_db_response_time = sum(db_response_times) / len(db_response_times)
            if avg_db_response_time > 1.0:  # 1秒
                bottlenecks.append({
                    'type': '数据库瓶颈',
                    'severity': 'high' if avg_db_response_time > 2.0 else 'medium',
                    'description': f'数据库平均响应时间{avg_db_response_time:.2f}秒，过慢',
                    'impact': '影响数据写入性能',
                    'metrics': {
                        'avg_response_time': avg_db_response_time,
                        'total_operations': sum(m.db_operations for m in metrics_list)
                    }
                })

        return {
            'bottlenecks': bottlenecks,
            'bottleneck_count': len(bottlenecks),
            'severity_distribution': self._analyze_severity_distribution(bottlenecks)
        }

    def _analyze_severity_distribution(self, bottlenecks: List[Dict[str, Any]]) -> Dict[str, int]:
        """分析瓶颈严重程度分布"""
        distribution = {'high': 0, 'medium': 0, 'low': 0}
        for bottleneck in bottlenecks:
            severity = bottleneck.get('severity', 'low')
            distribution[severity] += 1
        return distribution

    def _generate_optimization_recommendations(self, summary: PerformanceSummary, bottleneck_analysis: Dict[str, Any]) -> List[Dict[str, Any]]:
        """生成优化建议"""
        recommendations = []

        # API优化建议
        if summary.api_success_rate < 0.95:
            recommendations.append({
                'category': 'API优化',
                'priority': 'high',
                'recommendation': '优化API调用策略，增加重试机制和错误处理',
                'expected_improvement': '提升API成功率至98%以上',
                'implementation': {
                    'add_retry_logic': '实现指数退避重试机制',
                    'connection_pooling': '使用连接池减少连接开销',
                    'rate_limiting': '优化API限流策略'
                }
            })

        # 吞吐量优化建议
        if summary.avg_throughput < 20:
            recommendations.append({
                'category': '吞吐量优化',
                'priority': 'high',
                'recommendation': '实现并发处理，提升数据采集吞吐量',
                'expected_improvement': '吞吐量提升至30-50股票/秒',
                'implementation': {
                    'concurrent_processing': '使用线程池并发处理多只股票',
                    'batch_optimization': '优化批量处理大小',
                    'async_processing': '考虑使用异步I/O'
                }
            })

        # 内存优化建议
        if summary.avg_memory_usage > 1000:
            recommendations.append({
                'category': '内存优化',
                'priority': 'medium',
                'recommendation': '优化内存使用，避免内存泄漏',
                'expected_improvement': '内存使用降低50%以上',
                'implementation': {
                    'stream_processing': '使用流式处理替代批量加载',
                    'memory_monitoring': '增加内存监控和垃圾回收',
                    'data_structures': '优化数据结构减少内存占用'
                }
            })

        # 数据库优化建议
        db_bottlenecks = [b for b in bottleneck_analysis.get('bottlenecks', []) if b['type'] == '数据库瓶颈']
        if db_bottlenecks:
            recommendations.append({
                'category': '数据库优化',
                'priority': 'medium',
                'recommendation': '优化数据库操作性能',
                'expected_improvement': '数据库响应时间降低至500ms以内',
                'implementation': {
                    'batch_size': '调整批量插入大小至10000-50000',
                    'connection_pool': '使用数据库连接池',
                    'index_optimization': '优化数据库索引',
                    'async_db': '考虑使用异步数据库操作'
                }
            })

        # 通用优化建议
        if summary.bottleneck_score > 70:
            recommendations.append({
                'category': '架构优化',
                'priority': 'high',
                'recommendation': '考虑整体架构优化，引入分布式处理',
                'expected_improvement': '整体性能提升3-5倍',
                'implementation': {
                    'distributed_processing': '使用分布式任务队列',
                    'caching': '增加数据缓存层',
                    'load_balancing': '实现负载均衡'
                }
            })

        return recommendations

    def _generate_chart_data(self, metrics_list: List[PerformanceMetrics]) -> Dict[str, Any]:
        """生成图表数据"""
        timestamps = [m.start_time for m in metrics_list]
        durations = [m.duration for m in metrics_list]
        throughputs = [m.throughput for m in metrics_list]
        memory_usage = [m.memory_peak for m in metrics_list]

        return {
            'time_series': {
                'timestamps': [t.isoformat() for t in timestamps],
                'durations': durations,
                'throughputs': throughputs,
                'memory_usage': memory_usage
            },
            'distribution': {
                'duration_distribution': self._calculate_distribution(durations),
                'throughput_distribution': self._calculate_distribution(throughputs),
                'memory_distribution': self._calculate_distribution(memory_usage)
            }
        }

    def _calculate_distribution(self, values: List[float]) -> Dict[str, Any]:
        """计算数值分布"""
        if not values:
            return {}

        sorted_values = sorted(values)
        n = len(sorted_values)

        return {
            'min': sorted_values[0],
            'max': sorted_values[-1],
            'median': sorted_values[n // 2],
            'p25': sorted_values[n // 4],
            'p75': sorted_values[3 * n // 4],
            'p90': sorted_values[int(0.9 * n)],
            'p95': sorted_values[int(0.95 * n)],
            'mean': sum(sorted_values) / n
        }

    def _save_report(self, report: Dict[str, Any], operation: str):
        """保存报告到文件"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{operation}_performance_report_{timestamp}.json"
        filepath = os.path.join(self.report_dir, filename)

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)

        print(f"性能报告已保存到：{filepath}")