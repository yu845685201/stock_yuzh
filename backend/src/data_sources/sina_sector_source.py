"""
新浪财经板块数据源 —— 行业分类与概念板块成分（每日复盘缺失数据采集方案 V1.0 20260928 §3/§4）

接口事实（2026-09-28 实测锁定，探路记录见方案 §2.1 P1-P4）：
- GET http://vip.stock.finance.sina.com.cn/q/view/newSinaHy.php
  → GBK 文本 var S_Finance_bankuai_sinaindustry = {"new_blhy":"new_blhy,玻璃行业,19,...", ...}
  值字段：[0]节点代码(冗余) [1]行业名 [2]成员数 [3+]行情快照字段（采集不用）
- GET http://vip.stock.finance.sina.com.cn/q/view/newFLJK.php?param=class
  → 同构 var S_Finance_bankuai_class = {"gn_hwqc":"gn_hwqc,华为汽车,97,..."}（概念节点 gn_*）
- 成分翻页（翻页可用是选新浪为主源的关键，THS 成分翻页被 hexin-v 拦）：
  GET http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/
      Market_Center.getHQNodeData?page=N&num=40&sort=symbol&asc=1&node={node}&symbol=&_s_r_a=init
  → JSON 数组 [{symbol:"sh600176", code, name, ...}]；翻过末页返回 []
- 响应均为 GBK（headers 未标 charset），统一 get_content 后显式解码
- 必须带 Referer https://finance.sina.com.cn/（hq.sinajs.cn 403 先例，vip 域同样带上）

安全红线：串行限速、正常 UA/Referer、不破解任何技术措施、风控识别即退避冷却、仅自用不分发。
"""

import json
import logging
import re
import time
from typing import Any, Dict, List, Optional

from .http_collector_base import HttpCollectorBase, ReviewBlockedError

logger = logging.getLogger(__name__)

INDUSTRY_LIST_URL = 'http://vip.stock.finance.sina.com.cn/q/view/newSinaHy.php'
CONCEPT_LIST_URL = 'http://vip.stock.finance.sina.com.cn/q/view/newFLJK.php?param=class'
NODE_DATA_URL = ('http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/'
                 'Market_Center.getHQNodeData')
SINA_REFERER = 'https://finance.sina.com.cn/'

# 板块成员单页条数（实测可用值；翻过末页返回空数组作为终止条件）
NODE_PAGE_SIZE = 40
# 单节点翻页安全阀（40/页 × 30 页 = 1200 只，远超任何板块实际规模）
NODE_MAX_PAGES = 30
# 空页二次确认次数（expected_count 未达时，瞬时空响应不等于翻页结束）
NODE_EMPTY_RETRIES = 2

_SYMBOL_RE = re.compile(r'^(sh|sz|bj)(\d{6})$', re.IGNORECASE)


class SinaSectorBlockedError(ReviewBlockedError):
    """新浪板块源风控异常（4xx/非 JSON/结构变更）"""


class SinaSectorSource(HttpCollectorBase):
    """新浪板块：行业/概念清单 + 成分翻页（周频全量，各源内部串行限速）"""

    def __init__(self, config: Dict[str, Any]):
        review = (config or {}).get('review_sync', {}) or {}
        super().__init__(
            config, 'sina_sector',
            interval_seconds=float(review.get('sina_sector_interval', 2.0)),
            referer=SINA_REFERER,
            blocked_error_class=SinaSectorBlockedError,
        )

    # ---------- 清单 ----------

    def fetch_industry_list(self) -> Dict[str, Dict[str, Any]]:
        """行业清单 -> {node: {name, count}}"""
        content = self.get_content(INDUSTRY_LIST_URL)
        return self.parse_sina_var(content, 'S_Finance_bankuai_sinaindustry')

    def fetch_concept_list(self) -> Dict[str, Dict[str, Any]]:
        """概念清单 -> {node: {name, count}}"""
        content = self.get_content(CONCEPT_LIST_URL)
        return self.parse_sina_var(content, 'S_Finance_bankuai_class')

    # ---------- 成分 ----------

    def fetch_node_members(self, node: str,
                           expected_count: Optional[int] = None) -> List[Dict[str, str]]:
        """单板块全部成分（翻页至空页；单页解析失败记日志继续，连续空页终止）。

        expected_count：清单口径成员数——空页但未达预期时对该页二次确认
        （2026-09-29 实测盘中偶发瞬时空响应导致翻页提前终止、大节点被截成一页）。
        Returns: [{'ts_code': 'sh.600176', 'code6': '600176', 'name': '中国巨石'}]
        无前缀/非 A 股代码条目跳过并计数（由调用方体现在报告中）。
        """
        members: List[Dict[str, str]] = []
        page = 1
        empty_retries = 0
        while page <= NODE_MAX_PAGES:
            data = self.get_json(NODE_DATA_URL, params={
                'page': page, 'num': NODE_PAGE_SIZE, 'sort': 'symbol', 'asc': 1,
                'node': node, 'symbol': '', '_s_r_a': 'init',
            })
            batch = data if isinstance(data, list) else []
            if not batch:
                if (expected_count and len(members) < expected_count
                        and empty_retries < NODE_EMPTY_RETRIES):
                    empty_retries += 1
                    time.sleep(self.request_interval * 3)
                    continue  # 同页重试（疑似瞬时空响应）
                break
            for item in batch:
                parsed = self.symbol_to_ts_code(str(item.get('symbol') or ''))
                if parsed is None:
                    logger.debug(f'[sina_sector] 节点 {node} 跳过无法解析代码: {item}')
                    continue
                members.append({'ts_code': parsed, 'code6': parsed.split('.')[1],
                                'name': str(item.get('name') or '') or None})
            page += 1
        return members

    # ---------- 解析（staticmethod 便于单测） ----------

    @staticmethod
    def parse_sina_var(content: bytes, var_name: str) -> Dict[str, Dict[str, Any]]:
        """GBK 文本中提取 var {var_name} = {...}; -> {node: {name, count}}

        值为逗号分隔字符串：[0]节点(冗余) [1]名称 [2]成员数；解析失败/空数据抛 KeyError 语义
        （返回空 dict，由调用方判空报错——结构变更必须可见，不允许静默 0 行）。
        """
        text = content.decode('gbk', errors='replace')
        m = re.search(re.escape(var_name) + r'\s*=\s*(\{.*\})\s*;?', text, re.DOTALL)
        if not m:
            return {}
        try:
            raw = json.loads(m.group(1))
        except ValueError:
            return {}
        result: Dict[str, Dict[str, Any]] = {}
        for node, value in raw.items():
            fields = str(value).split(',')
            if len(fields) < 2:
                continue
            result[str(node)] = {
                'name': fields[1].strip(),
                'count': _to_int(fields[2]) if len(fields) > 2 else None,
            }
        return result

    @staticmethod
    def symbol_to_ts_code(symbol: str) -> Optional[str]:
        """sh600176 -> sh.600176；无法识别前缀/代码返回 None"""
        m = _SYMBOL_RE.match(str(symbol).strip())
        if not m:
            return None
        return f'{m.group(1).lower()}.{m.group(2)}'


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None
