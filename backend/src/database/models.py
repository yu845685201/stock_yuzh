"""
数据模型定义
"""

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional, Dict, Any


@dataclass
class BaseStockInfo:
    """股票基本信息模型 - 严格按照文档要求"""
    id: Optional[int] = None
    ts_code: Optional[str] = None
    stock_code: str = ""
    stock_name: Optional[str] = None
    cnspell: Optional[str] = None
    market_code: Optional[str] = None
    market_name: Optional[str] = None
    exchange_code: Optional[str] = None
    sector_code: Optional[str] = None
    sector_name: Optional[str] = None
    industry_code: Optional[str] = None
    industry_name: Optional[str] = None
    list_status: str = 'L'  # L-上市，D-退市，P-暂停上市
    list_date: Optional[date] = None
    delist_date: Optional[date] = None
    type: Optional[str] = None  # 1：股票，2：指数，3：其它，4：可转债，5：ETF
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseStockInfo':
        """从字典创建BaseStockInfo对象"""
        return cls(
            id=data.get('id'),
            ts_code=data.get('ts_code'),
            stock_code=data.get('stock_code') or data.get('code', ''),
            stock_name=data.get('stock_name') or data.get('name') or data.get('code_name'),
            cnspell=data.get('cnspell'),
            market_code=data.get('market_code'),
            market_name=data.get('market_name'),
            exchange_code=data.get('exchange_code'),
            sector_code=data.get('sector_code'),
            sector_name=data.get('sector_name'),
            industry_code=data.get('industry_code'),
            industry_name=data.get('industry_name') or data.get('industry'),
            list_status=data.get('list_status', 'L'),
            list_date=data.get('list_date'),
            delist_date=data.get('delist_date'),
            type=data.get('type'),
            create_time=data.get('create_time'),
            update_time=data.get('update_time')
        )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'id': self.id,
            'ts_code': self.ts_code,
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'cnspell': self.cnspell,
            'market_code': self.market_code,
            'market_name': self.market_name,
            'exchange_code': self.exchange_code,
            'sector_code': self.sector_code,
            'sector_name': self.sector_name,
            'industry_code': self.industry_code,
            'industry_name': self.industry_name,
            'list_status': self.list_status,
            'list_date': self.list_date,
            'delist_date': self.delist_date,
            'type': self.type,
            'create_time': self.create_time,
            'update_time': self.update_time
        }


# 为了向后兼容，保留Stock别名
Stock = BaseStockInfo


@dataclass
class HisKlineDay:
    """日线行情数据模型"""
    id: Optional[int] = None
    ts_code: str = ""
    trade_date: Optional[date] = None
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    pre_close: Optional[float] = None
    change: Optional[float] = None
    pct_chg: Optional[float] = None
    vol: Optional[float] = None
    amount: Optional[float] = None
    turnover_rate: Optional[float] = None
    volume_ratio: Optional[float] = None  # 量比
    pe: Optional[float] = None  # 市盈率
    pb: Optional[float] = None  # 市净率
    ps: Optional[float] = None  # 市销率
    pcf: Optional[float] = None  # 市现率
    total_share: Optional[float] = None  # 总股本
    float_share: Optional[float] = None  # 流通股本
    free_share: Optional[float] = None  # 自由流通股本
    total_mv: Optional[float] = None  # 总市值
    circ_mv: Optional[float] = None  # 流通市值
    adj_factor: Optional[float] = None  # 复权因子
    is_st: Optional[str] = None  # 是否ST
    trade_status: Optional[str] = None  # 交易状态
    create_time: Optional[datetime] = None
    update_time: Optional[datetime] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'HisKlineDay':
        """从字典创建HisKlineDay对象"""
        return cls(
            id=data.get('id'),
            ts_code=data.get('ts_code'),
            trade_date=data.get('trade_date'),
            open=data.get('open'),
            high=data.get('high'),
            low=data.get('low'),
            close=data.get('close'),
            pre_close=data.get('pre_close'),
            change=data.get('change'),
            pct_chg=data.get('pct_chg'),
            vol=data.get('vol'),
            amount=data.get('amount'),
            turnover_rate=data.get('turnover_rate'),
            volume_ratio=data.get('volume_ratio'),
            pe=data.get('pe'),
            pb=data.get('pb'),
            ps=data.get('ps'),
            pcf=data.get('pcf'),
            total_share=data.get('total_share'),
            float_share=data.get('float_share'),
            free_share=data.get('free_share'),
            total_mv=data.get('total_mv'),
            circ_mv=data.get('circ_mv'),
            adj_factor=data.get('adj_factor'),
            is_st=data.get('is_st'),
            trade_status=data.get('trade_status'),
            create_time=data.get('create_time'),
            update_time=data.get('update_time')
        )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'id': self.id,
            'ts_code': self.ts_code,
            'trade_date': self.trade_date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'pre_close': self.pre_close,
            'change': self.change,
            'pct_chg': self.pct_chg,
            'vol': self.vol,
            'amount': self.amount,
            'turnover_rate': self.turnover_rate,
            'volume_ratio': self.volume_ratio,
            'pe': self.pe,
            'pb': self.pb,
            'ps': self.ps,
            'pcf': self.pcf,
            'total_share': self.total_share,
            'float_share': self.float_share,
            'free_share': self.free_share,
            'total_mv': self.total_mv,
            'circ_mv': self.circ_mv,
            'adj_factor': self.adj_factor,
            'is_st': self.is_st,
            'trade_status': self.trade_status,
            'create_time': self.create_time,
            'update_time': self.update_time
        }
