-- 创建更新时间的触发器函数
CREATE OR REPLACE FUNCTION update_modtime()
RETURNS TRIGGER AS $$
BEGIN
    NEW.update_time = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- 创建股票基本信息表
CREATE TABLE base_stock_info (
    -- 主键
    id BIGSERIAL PRIMARY KEY,
    
    -- 股票代码信息
    ts_code VARCHAR(20) NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(20) NOT NULL,
    cnspell VARCHAR(10),
    
    -- 市场信息
    market_code VARCHAR(5),
    market_name VARCHAR(20),
    exchange_code VARCHAR(10),
    
    -- 板块和行业信息
    sector_code VARCHAR(20),
    sector_name VARCHAR(20),
    industry_code VARCHAR(20),
    industry_name VARCHAR(20),
    
    -- 上市状态
    list_status VARCHAR(2) DEFAULT 'L' CHECK (list_status IN ('L', 'D', 'P')),
    
    -- 日期信息（字符串格式存储）
    list_date VARCHAR(8),  -- 格式：yyyyMMdd
    delist_date VARCHAR(8),  -- 格式：yyyyMMdd
    
    -- 系统时间
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
CREATE INDEX idx_base_stock_ts_code ON base_stock_info(ts_code);
CREATE INDEX idx_base_stock_stock_code ON base_stock_info(stock_code);
CREATE INDEX idx_base_stock_market ON base_stock_info(market_code);
CREATE INDEX idx_base_stock_industry ON base_stock_info(industry_code);
CREATE INDEX idx_base_stock_sector ON base_stock_info(sector_code);
CREATE INDEX idx_base_stock_list_status ON base_stock_info(list_status);
CREATE INDEX idx_base_stock_list_date ON base_stock_info(list_date);
CREATE INDEX idx_base_stock_cnspell ON base_stock_info(cnspell);

-- 创建触发器
CREATE TRIGGER update_base_stock_info_modtime
    BEFORE UPDATE ON base_stock_info
    FOR EACH ROW
    EXECUTE FUNCTION update_modtime();

-- 添加注释
COMMENT ON TABLE base_stock_info IS '股票基本信息表';
COMMENT ON COLUMN base_stock_info.id IS '主键ID，自增';
COMMENT ON COLUMN base_stock_info.ts_code IS 'TS代码，唯一标识符';
COMMENT ON COLUMN base_stock_info.stock_code IS '股票代码';
COMMENT ON COLUMN base_stock_info.stock_name IS '股票名称';
COMMENT ON COLUMN base_stock_info.cnspell IS '拼音缩写';
COMMENT ON COLUMN base_stock_info.market_code IS '市场编码';
COMMENT ON COLUMN base_stock_info.market_name IS '市场名称';
COMMENT ON COLUMN base_stock_info.exchange_code IS '交易所编码';
COMMENT ON COLUMN base_stock_info.sector_code IS '板块编码';
COMMENT ON COLUMN base_stock_info.sector_name IS '板块名称';
COMMENT ON COLUMN base_stock_info.industry_code IS '行业编码';
COMMENT ON COLUMN base_stock_info.industry_name IS '行业名称';
COMMENT ON COLUMN base_stock_info.list_status IS '上市状态: L-上市，D-退市，P-暂停上市';
COMMENT ON COLUMN base_stock_info.list_date IS '上市日期，格式：yyyyMMdd';
COMMENT ON COLUMN base_stock_info.delist_date IS '退市日期，格式：yyyyMMdd';
COMMENT ON COLUMN base_stock_info.create_time IS '数据创建时间，自动生成';
COMMENT ON COLUMN base_stock_info.update_time IS '数据修改时间，自动更新';




-- 创建基本面信息表
CREATE TABLE base_fundamentals_info (
    -- 主键
    id BIGSERIAL PRIMARY KEY,
    
    -- 股票基本信息
    ts_code VARCHAR(20) NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(20) NOT NULL,
    
    -- 披露日期
    disclosure_date VARCHAR(8) NOT NULL,  -- 格式：yyyyMMdd
    
    -- 股本信息
    total_share NUMERIC(30, 8),  -- 总股本，30位精度，8位小数
    float_share NUMERIC(30, 8),  -- 流通股本，30位精度，8位小数
    
    -- 系统时间
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
CREATE INDEX idx_base_fundamentals_ts_code ON base_fundamentals_info(ts_code);
CREATE INDEX idx_base_fundamentals_stock_code ON base_fundamentals_info(stock_code);
CREATE INDEX idx_base_fundamentals_disclosure_date ON base_fundamentals_info(disclosure_date);
CREATE INDEX idx_base_fundamentals_stock_disclosure ON base_fundamentals_info(stock_code, disclosure_date);

-- 创建触发器
CREATE TRIGGER update_base_fundamentals_info_modtime
    BEFORE UPDATE ON base_fundamentals_info
    FOR EACH ROW
    EXECUTE FUNCTION update_modtime();

-- 添加表和字段注释
COMMENT ON TABLE base_fundamentals_info IS '基本面信息表';
COMMENT ON COLUMN base_fundamentals_info.id IS '主键ID，自增';
COMMENT ON COLUMN base_fundamentals_info.ts_code IS 'TS代码';
COMMENT ON COLUMN base_fundamentals_info.stock_code IS '股票编码';
COMMENT ON COLUMN base_fundamentals_info.stock_name IS '股票名称';
COMMENT ON COLUMN base_fundamentals_info.disclosure_date IS '信息披露日期，格式：yyyyMMdd';
COMMENT ON COLUMN base_fundamentals_info.total_share IS '总股本，精度：30位，8位小数';
COMMENT ON COLUMN base_fundamentals_info.float_share IS '流通股本，精度：30位，8位小数';
COMMENT ON COLUMN base_fundamentals_info.create_time IS '数据创建时间，自动生成';
COMMENT ON COLUMN base_fundamentals_info.update_time IS '数据修改时间，自动更新';




-- 创建复权因子信息表
CREATE TABLE base_adjust_factor_info (
    -- 主键
    id BIGSERIAL PRIMARY KEY,
    
    -- 股票基本信息
    ts_code VARCHAR(20) NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(20) NOT NULL,
    
    -- 除权除息日期
    divid_operateDate VARCHAR(8) NOT NULL,  -- 格式：yyyyMMdd
    
    -- 复权因子
    fore_adjust_factor NUMERIC(30, 8),  -- 向前复权因子，30位精度，8位小数
    back_adjust_factor NUMERIC(30, 8),  -- 向后复权因子，30位精度，8位小数
    adjust_factor NUMERIC(30, 8),       -- 本次复权因子，30位精度，8位小数
    
    -- 系统时间
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
CREATE INDEX idx_base_adjust_factor_ts_code ON base_adjust_factor_info(ts_code);
CREATE INDEX idx_base_adjust_factor_stock_code ON base_adjust_factor_info(stock_code);
CREATE INDEX idx_base_adjust_factor_divid_date ON base_adjust_factor_info(divid_operateDate);
CREATE INDEX idx_base_adjust_factor_ts_divid ON base_adjust_factor_info(ts_code, divid_operateDate);

-- 创建触发器
CREATE TRIGGER update_base_adjust_factor_info_modtime
    BEFORE UPDATE ON base_adjust_factor_info
    FOR EACH ROW
    EXECUTE FUNCTION update_modtime();

-- 添加表和字段注释
COMMENT ON TABLE base_adjust_factor_info IS '复权因子信息表';
COMMENT ON COLUMN base_adjust_factor_info.id IS '主键ID，自增';
COMMENT ON COLUMN base_adjust_factor_info.ts_code IS 'TS代码';
COMMENT ON COLUMN base_adjust_factor_info.stock_code IS '股票编码';
COMMENT ON COLUMN base_adjust_factor_info.stock_name IS '股票名称';
COMMENT ON COLUMN base_adjust_factor_info.divid_operateDate IS '除权除息日期，格式：yyyyMMdd';
COMMENT ON COLUMN base_adjust_factor_info.fore_adjust_factor IS '向前复权因子，除权除息日前一个交易日的收盘价/除权除息日最近的一个交易日的前收盘价，精度：30位，8位小数';
COMMENT ON COLUMN base_adjust_factor_info.back_adjust_factor IS '向后复权因子，除权除息日最近的一个交易日的前收盘价/除权除息日前一个交易日的收盘价，精度：30位，8位小数';
COMMENT ON COLUMN base_adjust_factor_info.adjust_factor IS '本次复权因子，精度：30位，8位小数';
COMMENT ON COLUMN base_adjust_factor_info.create_time IS '数据创建时间，自动生成';
COMMENT ON COLUMN base_adjust_factor_info.update_time IS '数据修改时间，自动更新';




-- 创建历史日K线数据表
CREATE TABLE his_kline_day (
    -- 主键
    id BIGSERIAL PRIMARY KEY,
    
    -- 股票基本信息
    ts_code VARCHAR(20) NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(20) NOT NULL,
    
    -- 交易日期
    trade_date VARCHAR(8) NOT NULL,  -- 格式：yyyyMMdd
    
    -- 价格数据（精度：小数点后8位）
    open NUMERIC(30, 8) DEFAULT 0.00000000,
    high NUMERIC(30, 8) DEFAULT 0.00000000,
    low NUMERIC(30, 8) DEFAULT 0.00000000,
    close NUMERIC(30, 8) DEFAULT 0.00000000,
    preclose NUMERIC(30, 8) DEFAULT 0.00000000,
    
    -- 成交量与成交额
    volume NUMERIC(30, 8) DEFAULT 0.00000000,  -- 成交量（单位：股）
    amount NUMERIC(30, 8) DEFAULT 0.00000000,  -- 成交额（单位：人民币元）
    
    -- 交易状态
    trade_status SMALLINT DEFAULT 1 CHECK (trade_status IN (0, 1)),  -- 1：正常交易，0：停牌
    
    -- 股票状态
    is_st BOOLEAN DEFAULT FALSE,  -- 是否ST股，1：是，0：否
    
    -- 涨跌幅（精度：小数点后8位）
    change_rate NUMERIC(30, 8) DEFAULT 0.00000000,
    
    -- 换手率（精度：小数点后8位）
    turnover_rate NUMERIC(30, 8) DEFAULT 0.00000000,
    
    -- 关联的基本面信息
    fundamentals_disclosure_date VARCHAR(8),  -- 关联的基本面信息披露日期，格式：yyyyMMdd
    total_share NUMERIC(30, 8),  -- 总股本
    float_share NUMERIC(30, 8),  -- 流通股本
    
    -- 估值指标
    pe_ttm NUMERIC(30, 8),  -- 滚动市盈率
    pb_rate NUMERIC(30, 8),  -- 市净率
    ps_ttm NUMERIC(30, 8),  -- 滚动市销率
    pcf_ttm NUMERIC(30, 8),  -- 滚动市现率
    
    -- 系统时间
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
CREATE INDEX idx_his_kline_day_ts_code ON his_kline_day(ts_code);
CREATE INDEX idx_his_kline_day_stock_code ON his_kline_day(stock_code);
CREATE INDEX idx_his_kline_day_trade_date ON his_kline_day(trade_date);
CREATE INDEX idx_his_kline_day_ts_trade_date ON his_kline_day(ts_code, trade_date);
CREATE INDEX idx_his_kline_day_stock_trade_date ON his_kline_day(stock_code, trade_date);
CREATE INDEX idx_his_kline_day_trade_status ON his_kline_day(trade_status);
CREATE INDEX idx_his_kline_day_is_st ON his_kline_day(is_st);
CREATE INDEX idx_his_kline_day_change_rate ON his_kline_day(change_rate);
CREATE INDEX idx_his_kline_day_fundamentals_date ON his_kline_day(fundamentals_disclosure_date);
CREATE INDEX idx_his_kline_day_valuation ON his_kline_day(pe_ttm, pb_rate);

-- 创建触发器
CREATE TRIGGER update_his_kline_day_modtime
    BEFORE UPDATE ON his_kline_day
    FOR EACH ROW
    EXECUTE FUNCTION update_modtime();

-- 添加表和字段注释
COMMENT ON TABLE his_kline_day IS '历史日K线数据表';
COMMENT ON COLUMN his_kline_day.id IS '主键ID，自增';
COMMENT ON COLUMN his_kline_day.ts_code IS 'TS代码';
COMMENT ON COLUMN his_kline_day.stock_code IS '股票编码';
COMMENT ON COLUMN his_kline_day.stock_name IS '股票名称';
COMMENT ON COLUMN his_kline_day.trade_date IS '交易日，格式：yyyyMMdd';
COMMENT ON COLUMN his_kline_day.open IS '今日开盘价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_day.high IS '最高价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_day.low IS '最低价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_day.close IS '今日收盘价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_day.preclose IS '昨日收盘价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_day.volume IS '成交量，单位：股，精度：小数点后8位';
COMMENT ON COLUMN his_kline_day.amount IS '成交额，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_day.trade_status IS '交易状态：1-正常交易，0-停牌';
COMMENT ON COLUMN his_kline_day.is_st IS '是否ST股：1-是，0-否';
COMMENT ON COLUMN his_kline_day.change_rate IS '涨跌幅，精度：小数点后8位，计算公式：[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%';
COMMENT ON COLUMN his_kline_day.turnover_rate IS '换手率，精度：小数点后8位，计算公式：[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%';
COMMENT ON COLUMN his_kline_day.fundamentals_disclosure_date IS '关联的基本面信息披露日期，格式：yyyyMMdd';
COMMENT ON COLUMN his_kline_day.total_share IS '总股本，精度：小数点后8位';
COMMENT ON COLUMN his_kline_day.float_share IS '流通股本，精度：小数点后8位';
COMMENT ON COLUMN his_kline_day.pe_ttm IS '滚动市盈率，精度：小数点后8位，计算公式：(指定交易日的股票收盘价/指定交易日的每股盈余TTM)=(指定交易日的股票收盘价*截至当日公司总股本)/归属母公司股东净利润TTM';
COMMENT ON COLUMN his_kline_day.pb_rate IS '市净率，精度：小数点后8位，计算公式：(指定交易日的股票收盘价/指定交易日的每股净资产)=总市值/(最近披露的归属母公司股东的权益-其他权益工具)';
COMMENT ON COLUMN his_kline_day.ps_ttm IS '滚动市销率，精度：小数点后8位，计算公式：(指定交易日的股票收盘价/指定交易日的每股销售额)=(指定交易日的股票收盘价*截至当日公司总股本)/营业总收入TTM';
COMMENT ON COLUMN his_kline_day.pcf_ttm IS '滚动市现率，精度：小数点后8位，计算公式：(指定交易日的股票收盘价/指定交易日的每股现金流TTM)=(指定交易日的股票收盘价*截至当日公司总股本)/现金以及现金等价物净增加额TTM';
COMMENT ON COLUMN his_kline_day.create_time IS '数据创建时间，自动生成';
COMMENT ON COLUMN his_kline_day.update_time IS '数据修改时间，自动更新';




-- 创建历史1分钟K线数据表（非分区表）
CREATE TABLE his_kline_1min (
    -- 主键
    id BIGSERIAL PRIMARY KEY,
    
    -- 股票基本信息
    ts_code VARCHAR(20) NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(20) NOT NULL,
    
    -- 交易日期和时间
    trade_date VARCHAR(8) NOT NULL,      -- 交易日，格式：yyyyMMdd
    trade_time VARCHAR(4) NOT NULL,      -- 交易时间，格式：hhmm
    trade_datetime VARCHAR(12) NOT NULL, -- 交易日期时间，格式：yyyyMMddhhmm
    
    -- 价格数据（精度：小数点后8位）
    open NUMERIC(30, 8) DEFAULT 0.00000000,
    high NUMERIC(30, 8) DEFAULT 0.00000000,
    low NUMERIC(30, 8) DEFAULT 0.00000000,
    close NUMERIC(30, 8) DEFAULT 0.00000000,
    preclose NUMERIC(30, 8) DEFAULT 0.00000000,
    
    -- 成交量与成交额
    volume NUMERIC(30, 8) DEFAULT 0.00000000,  -- 成交量（单位：股）
    amount NUMERIC(30, 8) DEFAULT 0.00000000,  -- 成交额（单位：人民币元）
    
    -- 涨跌幅（精度：小数点后8位）
    change_rate NUMERIC(30, 8) DEFAULT 0.00000000,
    
    -- 换手率（精度：小数点后8位）
    turnover_rate NUMERIC(30, 8) DEFAULT 0.00000000,
    
    -- 关联的基本面信息
    fundamentals_disclosure_date VARCHAR(8),  -- 关联的基本面信息披露日期，格式：yyyyMMdd
    total_share NUMERIC(30, 8),  -- 总股本
    float_share NUMERIC(30, 8),  -- 流通股本
    
    -- 系统时间
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
CREATE INDEX idx_his_kline_1min_ts_code ON his_kline_1min(ts_code);
CREATE INDEX idx_his_kline_1min_stock_code ON his_kline_1min(stock_code);
CREATE INDEX idx_his_kline_1min_trade_date ON his_kline_1min(trade_date);
CREATE INDEX idx_his_kline_1min_trade_time ON his_kline_1min(trade_time);
CREATE INDEX idx_his_kline_1min_trade_datetime ON his_kline_1min(trade_datetime);
CREATE INDEX idx_his_kline_1min_ts_trade_datetime ON his_kline_1min(ts_code, trade_datetime);
CREATE INDEX idx_his_kline_1min_stock_trade_datetime ON his_kline_1min(stock_code, trade_datetime);
CREATE INDEX idx_his_kline_1min_date_time_range ON his_kline_1min(trade_datetime DESC);
CREATE INDEX idx_his_kline_1min_fundamentals_date ON his_kline_1min(fundamentals_disclosure_date);
CREATE INDEX idx_his_kline_1min_price_volume ON his_kline_1min(close, volume);

-- 创建触发器
CREATE TRIGGER update_his_kline_1min_modtime
    BEFORE UPDATE ON his_kline_1min
    FOR EACH ROW
    EXECUTE FUNCTION update_modtime();

-- 添加表和字段注释
COMMENT ON TABLE his_kline_1min IS '历史1分钟K线数据表';
COMMENT ON COLUMN his_kline_1min.id IS '主键ID，自增';
COMMENT ON COLUMN his_kline_1min.ts_code IS 'TS代码';
COMMENT ON COLUMN his_kline_1min.stock_code IS '股票编码';
COMMENT ON COLUMN his_kline_1min.stock_name IS '股票名称';
COMMENT ON COLUMN his_kline_1min.trade_date IS '交易日，格式：yyyyMMdd';
COMMENT ON COLUMN his_kline_1min.trade_time IS '交易时间，格式：hhmm';
COMMENT ON COLUMN his_kline_1min.trade_datetime IS '交易日期时间，格式：yyyyMMddhhmm';
COMMENT ON COLUMN his_kline_1min.open IS '该1分钟开盘价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_1min.high IS '该1分钟最高价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_1min.low IS '该1分钟最低价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_1min.close IS '该1分钟收盘价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_1min.preclose IS '前一1分钟收盘价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_1min.volume IS '成交量，单位：股，精度：小数点后8位';
COMMENT ON COLUMN his_kline_1min.amount IS '成交额，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_1min.change_rate IS '涨跌幅，精度：小数点后8位，计算公式：[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%';
COMMENT ON COLUMN his_kline_1min.turnover_rate IS '换手率，精度：小数点后8位，计算公式：[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%';
COMMENT ON COLUMN his_kline_1min.fundamentals_disclosure_date IS '关联的基本面信息披露日期，格式：yyyyMMdd';
COMMENT ON COLUMN his_kline_1min.total_share IS '总股本，精度：小数点后8位';
COMMENT ON COLUMN his_kline_1min.float_share IS '流通股本，精度：小数点后8位';
COMMENT ON COLUMN his_kline_1min.create_time IS '数据创建时间，自动生成';
COMMENT ON COLUMN his_kline_1min.update_time IS '数据修改时间，自动更新';




-- 创建历史5分钟K线数据表
CREATE TABLE his_kline_5min (
    -- 主键
    id BIGSERIAL PRIMARY KEY,
    
    -- 股票基本信息
    ts_code VARCHAR(20) NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(20) NOT NULL,
    
    -- 交易日期和时间
    trade_date VARCHAR(8) NOT NULL,      -- 交易日，格式：yyyyMMdd
    trade_time VARCHAR(4) NOT NULL,      -- 交易时间，格式：hhmm
    trade_datetime VARCHAR(12) NOT NULL, -- 交易日期时间，格式：yyyyMMddhhmm
    
    -- 价格数据（精度：小数点后8位）
    open NUMERIC(30, 8) DEFAULT 0.00000000,
    high NUMERIC(30, 8) DEFAULT 0.00000000,
    low NUMERIC(30, 8) DEFAULT 0.00000000,
    close NUMERIC(30, 8) DEFAULT 0.00000000,
    preclose NUMERIC(30, 8) DEFAULT 0.00000000,
    
    -- 成交量与成交额
    volume NUMERIC(30, 8) DEFAULT 0.00000000,  -- 成交量（单位：股）
    amount NUMERIC(30, 8) DEFAULT 0.00000000,  -- 成交额（单位：人民币元）
    
    -- 涨跌幅（精度：小数点后8位）
    change_rate NUMERIC(30, 8) DEFAULT 0.00000000,
    
    -- 换手率（精度：小数点后8位）
    turnover_rate NUMERIC(30, 8) DEFAULT 0.00000000,
    
    -- 关联的基本面信息
    fundamentals_disclosure_date VARCHAR(8),  -- 关联的基本面信息披露日期，格式：yyyyMMdd
    total_share NUMERIC(30, 8),  -- 总股本
    float_share NUMERIC(30, 8),  -- 流通股本
    
    -- 系统时间
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
CREATE INDEX idx_his_kline_5min_ts_code ON his_kline_5min(ts_code);
CREATE INDEX idx_his_kline_5min_stock_code ON his_kline_5min(stock_code);
CREATE INDEX idx_his_kline_5min_trade_date ON his_kline_5min(trade_date);
CREATE INDEX idx_his_kline_5min_trade_time ON his_kline_5min(trade_time);
CREATE INDEX idx_his_kline_5min_trade_datetime ON his_kline_5min(trade_datetime);
CREATE INDEX idx_his_kline_5min_ts_trade_datetime ON his_kline_5min(ts_code, trade_datetime);
CREATE INDEX idx_his_kline_5min_stock_trade_datetime ON his_kline_5min(stock_code, trade_datetime);
CREATE INDEX idx_his_kline_5min_date_time_range ON his_kline_5min(trade_datetime DESC);
CREATE INDEX idx_his_kline_5min_fundamentals_date ON his_kline_5min(fundamentals_disclosure_date);
CREATE INDEX idx_his_kline_5min_price_volume ON his_kline_5min(close, volume);

-- 创建触发器
CREATE TRIGGER update_his_kline_5min_modtime
    BEFORE UPDATE ON his_kline_5min
    FOR EACH ROW
    EXECUTE FUNCTION update_modtime();

-- 添加表和字段注释
COMMENT ON TABLE his_kline_5min IS '历史5分钟K线数据表';
COMMENT ON COLUMN his_kline_5min.id IS '主键ID，自增';
COMMENT ON COLUMN his_kline_5min.ts_code IS 'TS代码';
COMMENT ON COLUMN his_kline_5min.stock_code IS '股票编码';
COMMENT ON COLUMN his_kline_5min.stock_name IS '股票名称';
COMMENT ON COLUMN his_kline_5min.trade_date IS '交易日，格式：yyyyMMdd';
COMMENT ON COLUMN his_kline_5min.trade_time IS '交易时间，格式：hhmm';
COMMENT ON COLUMN his_kline_5min.trade_datetime IS '交易日期时间，格式：yyyyMMddhhmm';
COMMENT ON COLUMN his_kline_5min.open IS '该5分钟开盘价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_5min.high IS '该5分钟最高价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_5min.low IS '该5分钟最低价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_5min.close IS '该5分钟收盘价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_5min.preclose IS '前一5分钟收盘价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_5min.volume IS '成交量，单位：股，精度：小数点后8位';
COMMENT ON COLUMN his_kline_5min.amount IS '成交额，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN his_kline_5min.change_rate IS '涨跌幅，精度：小数点后8位，计算公式：[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%';
COMMENT ON COLUMN his_kline_5min.turnover_rate IS '换手率，精度：小数点后8位，计算公式：[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%';
COMMENT ON COLUMN his_kline_5min.fundamentals_disclosure_date IS '关联的基本面信息披露日期，格式：yyyyMMdd';
COMMENT ON COLUMN his_kline_5min.total_share IS '总股本，精度：小数点后8位';
COMMENT ON COLUMN his_kline_5min.float_share IS '流通股本，精度：小数点后8位';
COMMENT ON COLUMN his_kline_5min.create_time IS '数据创建时间，自动生成';
COMMENT ON COLUMN his_kline_5min.update_time IS '数据修改时间，自动更新';




-- 创建立体K线数据表 - 涨跌幅25%预设分析
CREATE TABLE anal_kline_rise_25pre (
    -- 主键
    id BIGSERIAL PRIMARY KEY,
    
    -- 股票基本信息
    ts_code VARCHAR(20) NOT NULL,
    stock_code VARCHAR(20) NOT NULL,
    stock_name VARCHAR(20) NOT NULL,
    
    -- 交易日期和时间
    trade_date VARCHAR(8) NOT NULL,      -- 交易日，格式：yyyyMMdd
    trade_time VARCHAR(4) NOT NULL,      -- 交易时间，格式：hhmm
    trade_datetime VARCHAR(12) NOT NULL, -- 交易日期时间，格式：yyyyMMddhhmm
    
    -- 价格数据（精度：小数点后8位）
    open NUMERIC(30, 8) DEFAULT 0.00000000,
    high NUMERIC(30, 8) DEFAULT 0.00000000,
    low NUMERIC(30, 8) DEFAULT 0.00000000,
    close NUMERIC(30, 8) DEFAULT 0.00000000,
    
    -- 成交量与成交额
    volume NUMERIC(30, 8) DEFAULT 0.00000000,  -- 成交量（单位：股）
    amount NUMERIC(30, 8) DEFAULT 0.00000000,  -- 成交额（单位：人民币元）
    
    -- 涨跌幅（精度：小数点后8位）
    change_rate NUMERIC(30, 8) DEFAULT 0.00000000,
    
    -- 换手率（精度：小数点后8位）
    turnover_rate NUMERIC(30, 8) DEFAULT 0.00000000,
    
    -- 系统时间
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
CREATE INDEX idx_anal_kline_rise_ts_code ON anal_kline_rise_25pre(ts_code);
CREATE INDEX idx_anal_kline_rise_stock_code ON anal_kline_rise_25pre(stock_code);
CREATE INDEX idx_anal_kline_rise_trade_date ON anal_kline_rise_25pre(trade_date);
CREATE INDEX idx_anal_kline_rise_trade_time ON anal_kline_rise_25pre(trade_time);
CREATE INDEX idx_anal_kline_rise_trade_datetime ON anal_kline_rise_25pre(trade_datetime);
CREATE INDEX idx_anal_kline_rise_ts_trade_datetime ON anal_kline_rise_25pre(ts_code, trade_datetime);
CREATE INDEX idx_anal_kline_rise_stock_trade_datetime ON anal_kline_rise_25pre(stock_code, trade_datetime);
CREATE INDEX idx_anal_kline_rise_date_time_range ON anal_kline_rise_25pre(trade_datetime DESC);
CREATE INDEX idx_anal_kline_rise_change_rate ON anal_kline_rise_25pre(change_rate DESC);
CREATE INDEX idx_anal_kline_rise_price_volume ON anal_kline_rise_25pre(close, volume);

-- 创建触发器
CREATE TRIGGER update_anal_kline_rise_25pre_modtime
    BEFORE UPDATE ON anal_kline_rise_25pre
    FOR EACH ROW
    EXECUTE FUNCTION update_modtime();

-- 添加表和字段注释
COMMENT ON TABLE anal_kline_rise_25pre IS '立体K线数据表 - 涨跌幅25%预设分析';
COMMENT ON COLUMN anal_kline_rise_25pre.id IS '主键ID，自增';
COMMENT ON COLUMN anal_kline_rise_25pre.ts_code IS 'TS代码';
COMMENT ON COLUMN anal_kline_rise_25pre.stock_code IS '股票编码';
COMMENT ON COLUMN anal_kline_rise_25pre.stock_name IS '股票名称';
COMMENT ON COLUMN anal_kline_rise_25pre.trade_date IS '交易日，格式：yyyyMMdd';
COMMENT ON COLUMN anal_kline_rise_25pre.trade_time IS '交易时间，格式：hhmm';
COMMENT ON COLUMN anal_kline_rise_25pre.trade_datetime IS '交易日期时间，格式：yyyyMMddhhmm';
COMMENT ON COLUMN anal_kline_rise_25pre.open IS '该周期开盘价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN anal_kline_rise_25pre.high IS '该周期最高价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN anal_kline_rise_25pre.low IS '该周期最低价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN anal_kline_rise_25pre.close IS '该周期收盘价，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN anal_kline_rise_25pre.volume IS '成交量，单位：股，精度：小数点后8位';
COMMENT ON COLUMN anal_kline_rise_25pre.amount IS '成交额，精度：小数点后8位，单位：人民币元';
COMMENT ON COLUMN anal_kline_rise_25pre.change_rate IS '涨跌幅，精度：小数点后8位，计算公式：[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%';
COMMENT ON COLUMN anal_kline_rise_25pre.turnover_rate IS '换手率，精度：小数点后8位，计算公式：[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%';
COMMENT ON COLUMN anal_kline_rise_25pre.create_time IS '数据创建时间，自动生成';
COMMENT ON COLUMN anal_kline_rise_25pre.update_time IS '数据修改时间，自动更新';