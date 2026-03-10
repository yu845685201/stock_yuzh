-- 创建触发器函数：用于在更新记录时自动更新update_time字段
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    -- 在更新操作前，将update_time字段设置为当前时间戳
    NEW.update_time = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- 创建股票基本信息表
CREATE TABLE base_stock_info (
    -- 主键，自增
    id BIGSERIAL PRIMARY KEY,
    -- TS代码，用于标识股票在Tushare中的唯一编码
    ts_code VARCHAR(20),
    -- 股票编码，公司上市股票代码
    stock_code VARCHAR(20),
    -- 股票名称
    stock_name VARCHAR(20),
    -- 拼音缩写，股票名称的拼音首字母
    cnspell VARCHAR(10),
    -- 市场编码，如: SH, SZ
    market_code VARCHAR(5),
    -- 市场名称，如: 上海交易所, 深圳交易所
    market_name VARCHAR(20),
    -- 交易所编码
    exchange_code VARCHAR(10),
    -- 板块编码
    sector_code VARCHAR(20),
    -- 板块名称
    sector_name VARCHAR(20),
    -- 行业编码
    industry_code VARCHAR(20),
    -- 行业名称
    industry_name VARCHAR(20),
    -- 上市状态：L-上市，D-退市，P-暂停上市
    list_status VARCHAR(2),
    -- 上市日期
    list_date VARCHAR(8),
    -- 退市日期
    delist_date VARCHAR(8),
    -- 数据创建时间，插入记录时自动设置当前时间
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 数据修改时间，更新记录时自动更新为当前时间
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
-- TS代码索引，常用于股票查询
CREATE INDEX idx_base_stock_info_ts_code ON base_stock_info (ts_code);
-- 股票编码索引，常用于按股票代码查询
CREATE INDEX idx_base_stock_info_stock_code ON base_stock_info (stock_code);
-- 市场编码索引，常用于按市场筛选
CREATE INDEX idx_base_stock_info_market_code ON base_stock_info (market_code);
-- 上市状态索引，常用于筛选上市/退市股票
CREATE INDEX idx_base_stock_info_list_status ON base_stock_info (list_status);
-- 行业编码索引，常用于按行业筛选
CREATE INDEX idx_base_stock_info_industry_code ON base_stock_info (industry_code);

-- 表注释
COMMENT ON TABLE base_stock_info IS '股票基本信息表';

-- 字段注释
COMMENT ON COLUMN base_stock_info.id IS '主键，自增ID';
COMMENT ON COLUMN base_stock_info.ts_code IS 'TS代码，Tushare系统中的唯一标识';
COMMENT ON COLUMN base_stock_info.stock_code IS '股票代码，交易所公布的股票编码';
COMMENT ON COLUMN base_stock_info.stock_name IS '股票名称';
COMMENT ON COLUMN base_stock_info.cnspell IS '股票名称的拼音缩写';
COMMENT ON COLUMN base_stock_info.market_code IS '市场编码，如SH(上海)、SZ(深圳)';
COMMENT ON COLUMN base_stock_info.market_name IS '市场名称';
COMMENT ON COLUMN base_stock_info.exchange_code IS '交易所编码';
COMMENT ON COLUMN base_stock_info.sector_code IS '板块编码';
COMMENT ON COLUMN base_stock_info.sector_name IS '板块名称';
COMMENT ON COLUMN base_stock_info.industry_code IS '行业分类编码';
COMMENT ON COLUMN base_stock_info.industry_name IS '行业分类名称';
COMMENT ON COLUMN base_stock_info.list_status IS '上市状态：L-上市，D-退市，P-暂停上市';
COMMENT ON COLUMN base_stock_info.list_date IS '上市日期，格式为yyyyMMdd的字符串';
COMMENT ON COLUMN base_stock_info.delist_date IS '退市日期，格式为yyyyMMdd的字符串';
COMMENT ON COLUMN base_stock_info.create_time IS '数据创建时间，记录插入时间';
COMMENT ON COLUMN base_stock_info.update_time IS '数据修改时间，记录最后更新时间';

-- 创建触发器：在base_stock_info表更新时自动调用上面的函数
CREATE TRIGGER update_base_stock_info_modtime 
BEFORE UPDATE ON base_stock_info 
FOR EACH ROW 
EXECUTE FUNCTION update_modified_column();


-- 基本面信息表
CREATE TABLE base_fundamentals_info (
    -- 主键，自增
    id BIGSERIAL PRIMARY KEY,
    -- TS代码，关联股票基本信息
    ts_code VARCHAR(20),
    -- 股票编码，用于业务查询
    stock_code VARCHAR(20),
    -- 股票名称
    stock_name VARCHAR(20),
    -- 信息披露日期，财报发布日期
    disclosure_date TIMESTAMP,
    -- 总股本，单位：股
    total_share NUMERIC(20, 4),
    -- 流通股本，单位：股
    float_share NUMERIC(20, 4),
    -- 数据创建时间，插入时自动设置
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 数据修改时间，更新时自动更新
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
-- TS代码索引
CREATE INDEX idx_base_fundamentals_info_ts_code ON base_fundamentals_info (ts_code);
-- 股票编码索引
CREATE INDEX idx_base_fundamentals_info_stock_code ON base_fundamentals_info (stock_code);
-- 信息披露日期索引
CREATE INDEX idx_base_fundamentals_info_disclosure_date ON base_fundamentals_info (disclosure_date);

-- 表注释
COMMENT ON TABLE base_fundamentals_info IS '基本面信息表';

-- 字段注释
COMMENT ON COLUMN base_fundamentals_info.id IS '主键，自增ID';
COMMENT ON COLUMN base_fundamentals_info.ts_code IS 'TS代码，关联base_stock_info.ts_code';
COMMENT ON COLUMN base_fundamentals_info.stock_code IS '股票编码，关联base_stock_info.stock_code';
COMMENT ON COLUMN base_fundamentals_info.stock_name IS '股票名称';
COMMENT ON COLUMN base_fundamentals_info.disclosure_date IS '信息披露日期，财报发布日期';
COMMENT ON COLUMN base_fundamentals_info.total_share IS '总股本，单位：股，精确到4位小数';
COMMENT ON COLUMN base_fundamentals_info.float_share IS '流通股本，单位：股，精确到4位小数';
COMMENT ON COLUMN base_fundamentals_info.create_time IS '数据创建时间，记录插入时间';
COMMENT ON COLUMN base_fundamentals_info.update_time IS '数据修改时间，记录最后更新时间';

-- 创建触发器：在base_fundamentals_info表更新时自动调用上面的函数
CREATE TRIGGER update_base_fundamentals_info_modtime 
BEFORE UPDATE ON base_fundamentals_info 
FOR EACH ROW 
EXECUTE FUNCTION update_modified_column();



-- 交易日历表
CREATE TABLE base_trade_calendar (
    -- 主键，自增
    id BIGSERIAL PRIMARY KEY,
    -- 自然日日期，格式：yyyy-mm-dd
    calendar_date VARCHAR(10) NOT NULL,
    -- 是否交易日：0-否，1-是
    is_trading_day SMALLINT NOT NULL
);

-- 创建索引以提高查询性能
-- 自然日日期索引，唯一约束确保日期不重复
CREATE UNIQUE INDEX idx_base_trade_calendar_calendar_date ON base_trade_calendar (calendar_date);
-- 是否交易日索引，用于快速筛选交易日或非交易日
CREATE INDEX idx_base_trade_calendar_is_trading_day ON base_trade_calendar (is_trading_day);

-- 表注释
COMMENT ON TABLE base_trade_calendar IS '交易日历表';

-- 字段注释
COMMENT ON COLUMN base_trade_calendar.id IS '主键，自增ID';
COMMENT ON COLUMN base_trade_calendar.calendar_date IS '自然日日期，格式为yyyy-mm-dd';
COMMENT ON COLUMN base_trade_calendar.is_trading_day IS '是否交易日：0-否，1-是';


-- 指数数据表
CREATE TABLE trade_index_info (
    -- 主键，自增
    id BIGSERIAL PRIMARY KEY,
    -- TS代码，用于标识指数在Tushare中的唯一编码
    ts_code VARCHAR(20),
    -- 指数编码，交易所公布的指数代码
    index_code VARCHAR(20),
    -- 指数名称
    index_name VARCHAR(20),
    -- 交易日，格式：yyyyMMdd
    trade_date VARCHAR(8),
    -- 今日开盘价，精度：小数点后4位；单位：人民币元
    open NUMERIC(20, 4),
    -- 最高价，精度：小数点后4位；单位：人民币元
    high NUMERIC(20, 4),
    -- 最低价，精度：小数点后4位；单位：人民币元
    low NUMERIC(20, 4),
    -- 今日收盘价，精度：小数点后4位；单位：人民币元
    close NUMERIC(20, 4),
    -- 昨日收盘价，精度：小数点后4位；单位：人民币元
    preclose NUMERIC(20, 4),
    -- 成交量，单位：股
    volume NUMERIC(20, 0),
    -- 成交额，精度：小数点后4位；单位：人民币元
    amount NUMERIC(20, 4),
    -- 涨跌幅，精度：小数点后6位
    change_rate NUMERIC(10, 6),
    -- 数据创建时间，插入记录时自动设置当前时间
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 数据修改时间，更新记录时自动更新为当前时间
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
-- TS代码索引
CREATE INDEX idx_trade_index_info_ts_code ON trade_index_info (ts_code);
-- 指数编码索引
CREATE INDEX idx_trade_index_info_index_code ON trade_index_info (index_code);
-- 交易日索引
CREATE INDEX idx_trade_index_info_trade_date ON trade_index_info (trade_date);
-- 组合索引：指数编码+交易日，常用于查询特定指数的历史数据
CREATE INDEX idx_trade_index_info_index_code_trade_date ON trade_index_info (index_code, trade_date);
-- 组合索引：TS代码+交易日，常用于查询特定TS代码的历史数据
CREATE INDEX idx_trade_index_info_ts_code_trade_date ON trade_index_info (ts_code, trade_date);

-- 表注释
COMMENT ON TABLE trade_index_info IS '指数数据表';

-- 字段注释
COMMENT ON COLUMN trade_index_info.id IS '主键，自增ID';
COMMENT ON COLUMN trade_index_info.ts_code IS 'TS代码，Tushare系统中的唯一标识';
COMMENT ON COLUMN trade_index_info.index_code IS '指数编码，交易所公布的指数代码';
COMMENT ON COLUMN trade_index_info.index_name IS '指数名称';
COMMENT ON COLUMN trade_index_info.trade_date IS '交易日，格式为yyyyMMdd的字符串';
COMMENT ON COLUMN trade_index_info.open IS '今日开盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN trade_index_info.high IS '最高价，单位：元，精确到4位小数';
COMMENT ON COLUMN trade_index_info.low IS '最低价，单位：元，精确到4位小数';
COMMENT ON COLUMN trade_index_info.close IS '今日收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN trade_index_info.preclose IS '昨日收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN trade_index_info.volume IS '成交量，单位：股，整数';
COMMENT ON COLUMN trade_index_info.amount IS '成交额，单位：元，精确到4位小数';
COMMENT ON COLUMN trade_index_info.change_rate IS '涨跌幅，百分比，精确到6位小数';
COMMENT ON COLUMN trade_index_info.create_time IS '数据创建时间，记录插入时间';
COMMENT ON COLUMN trade_index_info.update_time IS '数据修改时间，记录最后更新时间';

-- 创建触发器：在trade_index_info表更新时自动调用上面的函数
CREATE TRIGGER update_trade_index_info_modtime 
BEFORE UPDATE ON trade_index_info 
FOR EACH ROW 
EXECUTE FUNCTION update_modified_column();


-- 历史日K线数据表
CREATE TABLE his_kline_day (
    -- 主键，自增
    id BIGSERIAL PRIMARY KEY,
    -- TS代码，Tushare系统中的唯一标识
    ts_code VARCHAR(20),
    -- 股票编码
    stock_code VARCHAR(20),
    -- 股票名称
    stock_name VARCHAR(20),
    -- 交易日，格式：yyyyMMdd
    trade_date VARCHAR(8),
    -- 今日开盘价，精度：小数点后4位；单位：人民币元
    open NUMERIC(20, 4),
    -- 最高价，精度：小数点后4位；单位：人民币元
    high NUMERIC(20, 4),
    -- 最低价，精度：小数点后4位；单位：人民币元
    low NUMERIC(20, 4),
    -- 今日收盘价，精度：小数点后4位；单位：人民币元
    close NUMERIC(20, 4),
    -- 昨日收盘价，精度：小数点后4位；单位：人民币元
    preclose NUMERIC(20, 4),
    -- 成交量，单位：股
    volume NUMERIC(20, 0),
    -- 成交额，精度：小数点后4位；单位：人民币元
    amount NUMERIC(20, 4),
    -- 交易状态：1-正常交易，0-停牌
    trade_status SMALLINT,
    -- 是否ST股：1-是，0-否
    is_st BOOLEAN,
    -- 复权状态：1-后复权，2-前复权，3-不复权
    adjust_flag SMALLINT,
    -- 涨跌幅，精度：小数点后6位
    change_rate NUMERIC(10, 6),
    -- 换手率，百分比
    turnover_rate NUMERIC(10, 6),
    -- 滚动市盈率，TTM算法
    pe_ttm NUMERIC(20, 6),
    -- 市净率
    pb_rate NUMERIC(20, 6),
    -- 滚动市销率，TTM算法
    ps_ttm NUMERIC(20, 6),
    -- 滚动市现率，TTM算法
    pcf_ttm NUMERIC(20, 6),
    -- 数据创建时间，插入记录时自动设置当前时间
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 数据修改时间，更新记录时自动更新为当前时间
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
-- TS代码索引
CREATE INDEX idx_his_kline_day_ts_code ON his_kline_day (ts_code);
-- 股票编码索引
CREATE INDEX idx_his_kline_day_stock_code ON his_kline_day (stock_code);
-- 交易日索引
CREATE INDEX idx_his_kline_day_trade_date ON his_kline_day (trade_date);
-- 组合索引：TS代码+交易日
CREATE INDEX idx_his_kline_day_ts_code_trade_date ON his_kline_day (ts_code, trade_date);
-- 组合索引：股票编码+交易日
CREATE INDEX idx_his_kline_day_stock_code_trade_date ON his_kline_day (stock_code, trade_date);
-- 组合索引：TS代码+复权状态
CREATE INDEX idx_his_kline_day_ts_code_adjust_flag ON his_kline_day (ts_code, adjust_flag);
-- 交易状态索引
CREATE INDEX idx_his_kline_day_trade_status ON his_kline_day (trade_status);
-- 是否ST股索引
CREATE INDEX idx_his_kline_day_is_st ON his_kline_day (is_st);
-- 复权状态索引
CREATE INDEX idx_his_kline_day_adjust_flag ON his_kline_day (adjust_flag);

-- 表注释
COMMENT ON TABLE his_kline_day IS '历史日K线数据表';

-- 字段注释
COMMENT ON COLUMN his_kline_day.id IS '主键，自增ID';
COMMENT ON COLUMN his_kline_day.ts_code IS 'TS代码，Tushare系统中的唯一标识';
COMMENT ON COLUMN his_kline_day.stock_code IS '股票编码，交易所公布的股票代码';
COMMENT ON COLUMN his_kline_day.stock_name IS '股票名称';
COMMENT ON COLUMN his_kline_day.trade_date IS '交易日，格式为yyyyMMdd的字符串';
COMMENT ON COLUMN his_kline_day.open IS '今日开盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_day.high IS '最高价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_day.low IS '最低价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_day.close IS '今日收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_day.preclose IS '昨日收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_day.volume IS '成交量，单位：股，整数';
COMMENT ON COLUMN his_kline_day.amount IS '成交额，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_day.trade_status IS '交易状态：1-正常交易，0-停牌';
COMMENT ON COLUMN his_kline_day.is_st IS '是否ST股：1-是，0-否';
COMMENT ON COLUMN his_kline_day.adjust_flag IS '复权状态：1-后复权，2-前复权，3-不复权';
COMMENT ON COLUMN his_kline_day.change_rate IS '涨跌幅，百分比，精确到6位小数。计算方式：[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%';
COMMENT ON COLUMN his_kline_day.turnover_rate IS '换手率，百分比。计算方式：[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%';
COMMENT ON COLUMN his_kline_day.pe_ttm IS '滚动市盈率，TTM算法。计算方式：(指定交易日的股票收盘价/指定交易日的每股盈余TTM)=(指定交易日的股票收盘价*截至当日公司总股本)/归属母公司股东净利润TTM';
COMMENT ON COLUMN his_kline_day.pb_rate IS '市净率。计算方式：(指定交易日的股票收盘价/指定交易日的每股净资产)=总市值/(最近披露的归属母公司股东的权益-其他权益工具)';
COMMENT ON COLUMN his_kline_day.ps_ttm IS '滚动市销率，TTM算法。计算方式：(指定交易日的股票收盘价/指定交易日的每股销售额)=(指定交易日的股票收盘价*截至当日公司总股本)/营业总收入TTM';
COMMENT ON COLUMN his_kline_day.pcf_ttm IS '滚动市现率，TTM算法。计算方式：(指定交易日的股票收盘价/指定交易日的每股现金流TTM)=(指定交易日的股票收盘价*截至当日公司总股本)/现金以及现金等价物净增加额TTM';
COMMENT ON COLUMN his_kline_day.create_time IS '数据创建时间，记录插入时间';
COMMENT ON COLUMN his_kline_day.update_time IS '数据修改时间，记录最后更新时间';

-- 创建触发器：在his_kline_day表更新时自动调用上面的函数
CREATE TRIGGER update_his_kline_day_modtime 
BEFORE UPDATE ON his_kline_day 
FOR EACH ROW 
EXECUTE FUNCTION update_modified_column();


-- 历史周K线数据表
CREATE TABLE his_kline_week (
    -- 主键，自增
    id BIGSERIAL PRIMARY KEY,
    -- TS代码，Tushare系统中的唯一标识
    ts_code VARCHAR(20),
    -- 股票编码
    stock_code VARCHAR(20),
    -- 股票名称
    stock_name VARCHAR(20),
    -- 交易日，格式：yyyyMMdd，表示该周最后一个交易日
    trade_date VARCHAR(8),
    -- 本周开盘价，精度：小数点后4位；单位：人民币元
    open NUMERIC(20, 4),
    -- 本周最高价，精度：小数点后4位；单位：人民币元
    high NUMERIC(20, 4),
    -- 本周最低价，精度：小数点后4位；单位：人民币元
    low NUMERIC(20, 4),
    -- 本周收盘价，精度：小数点后4位；单位：人民币元
    close NUMERIC(20, 4),
    -- 上周周收盘价，精度：小数点后4位；单位：人民币元
    preclose NUMERIC(20, 4),
    -- 本周成交量，单位：股
    volume NUMERIC(20, 0),
    -- 本周成交额，精度：小数点后4位；单位：人民币元
    amount NUMERIC(20, 4),
    -- 复权状态：1-后复权，2-前复权，3-不复权
    adjust_flag SMALLINT,
    -- 周涨跌幅，精度：小数点后6位
    change_rate NUMERIC(10, 6),
    -- 周换手率，百分比
    turnover_rate NUMERIC(10, 6),
    -- 数据创建时间，插入记录时自动设置当前时间
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 数据修改时间，更新记录时自动更新为当前时间
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
-- TS代码索引
CREATE INDEX idx_his_kline_week_ts_code ON his_kline_week (ts_code);
-- 股票编码索引
CREATE INDEX idx_his_kline_week_stock_code ON his_kline_week (stock_code);
-- 交易日索引
CREATE INDEX idx_his_kline_week_trade_date ON his_kline_week (trade_date);
-- 组合索引：TS代码+交易日
CREATE INDEX idx_his_kline_week_ts_code_trade_date ON his_kline_week (ts_code, trade_date);
-- 组合索引：股票编码+交易日
CREATE INDEX idx_his_kline_week_stock_code_trade_date ON his_kline_week (stock_code, trade_date);
-- 组合索引：TS代码+复权状态
CREATE INDEX idx_his_kline_week_ts_code_adjust_flag ON his_kline_week (ts_code, adjust_flag);
-- 复权状态索引
CREATE INDEX idx_his_kline_week_adjust_flag ON his_kline_week (adjust_flag);

-- 表注释
COMMENT ON TABLE his_kline_week IS '历史周K线数据表';

-- 字段注释
COMMENT ON COLUMN his_kline_week.id IS '主键，自增ID';
COMMENT ON COLUMN his_kline_week.ts_code IS 'TS代码，Tushare系统中的唯一标识';
COMMENT ON COLUMN his_kline_week.stock_code IS '股票编码，交易所公布的股票代码';
COMMENT ON COLUMN his_kline_week.stock_name IS '股票名称';
COMMENT ON COLUMN his_kline_week.trade_date IS '交易日，格式为yyyyMMdd的字符串，表示该周最后一个交易日';
COMMENT ON COLUMN his_kline_week.open IS '本周开盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_week.high IS '本周最高价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_week.low IS '本周最低价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_week.close IS '本周收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_week.preclose IS '上周收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_week.volume IS '本周成交量，单位：股，整数';
COMMENT ON COLUMN his_kline_week.amount IS '本周成交额，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_week.adjust_flag IS '复权状态：1-后复权，2-前复权，3-不复权';
COMMENT ON COLUMN his_kline_week.change_rate IS '周涨跌幅，百分比，精确到6位小数。计算方式：[(本周收盘价-上周收盘价)/上周收盘价]*100%';
COMMENT ON COLUMN his_kline_week.turnover_rate IS '周换手率，百分比。计算方式：[本周成交量(股)/股票的流通股总股数(股)]*100%';
COMMENT ON COLUMN his_kline_week.create_time IS '数据创建时间，记录插入时间';
COMMENT ON COLUMN his_kline_week.update_time IS '数据修改时间，记录最后更新时间';

-- 创建触发器：在his_kline_week表更新时自动调用上面的函数
CREATE TRIGGER update_his_kline_week_modtime 
BEFORE UPDATE ON his_kline_week 
FOR EACH ROW 
EXECUTE FUNCTION update_modified_column();


-- 历史月K线数据表
CREATE TABLE his_kline_month (
    -- 主键，自增
    id BIGSERIAL PRIMARY KEY,
    -- TS代码，Tushare系统中的唯一标识
    ts_code VARCHAR(20),
    -- 股票编码
    stock_code VARCHAR(20),
    -- 股票名称
    stock_name VARCHAR(20),
    -- 交易日，格式：yyyyMMdd，表示该月最后一个交易日
    trade_date VARCHAR(8),
    -- 本月开盘价，精度：小数点后4位；单位：人民币元
    open NUMERIC(20, 4),
    -- 本月最高价，精度：小数点后4位；单位：人民币元
    high NUMERIC(20, 4),
    -- 本月最低价，精度：小数点后4位；单位：人民币元
    low NUMERIC(20, 4),
    -- 本月收盘价，精度：小数点后4位；单位：人民币元
    close NUMERIC(20, 4),
    -- 上月收盘价，精度：小数点后4位；单位：人民币元
    preclose NUMERIC(20, 4),
    -- 本月成交量，单位：股
    volume NUMERIC(20, 0),
    -- 本月成交额，精度：小数点后4位；单位：人民币元
    amount NUMERIC(20, 4),
    -- 复权状态：1-后复权，2-前复权，3-不复权
    adjust_flag SMALLINT,
    -- 月涨跌幅，精度：小数点后6位
    change_rate NUMERIC(10, 6),
    -- 月换手率，百分比
    turnover_rate NUMERIC(10, 6),
    -- 数据创建时间，插入记录时自动设置当前时间
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 数据修改时间，更新记录时自动更新为当前时间
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
-- TS代码索引
CREATE INDEX idx_his_kline_month_ts_code ON his_kline_month (ts_code);
-- 股票编码索引
CREATE INDEX idx_his_kline_month_stock_code ON his_kline_month (stock_code);
-- 交易日索引
CREATE INDEX idx_his_kline_month_trade_date ON his_kline_month (trade_date);
-- 组合索引：TS代码+交易日
CREATE INDEX idx_his_kline_month_ts_code_trade_date ON his_kline_month (ts_code, trade_date);
-- 组合索引：股票编码+交易日
CREATE INDEX idx_his_kline_month_stock_code_trade_date ON his_kline_month (stock_code, trade_date);
-- 组合索引：TS代码+复权状态
CREATE INDEX idx_his_kline_month_ts_code_adjust_flag ON his_kline_month (ts_code, adjust_flag);
-- 复权状态索引
CREATE INDEX idx_his_kline_month_adjust_flag ON his_kline_month (adjust_flag);

-- 表注释
COMMENT ON TABLE his_kline_month IS '历史月K线数据表';

-- 字段注释
COMMENT ON COLUMN his_kline_month.id IS '主键，自增ID';
COMMENT ON COLUMN his_kline_month.ts_code IS 'TS代码，Tushare系统中的唯一标识';
COMMENT ON COLUMN his_kline_month.stock_code IS '股票编码，交易所公布的股票代码';
COMMENT ON COLUMN his_kline_month.stock_name IS '股票名称';
COMMENT ON COLUMN his_kline_month.trade_date IS '交易日，格式为yyyyMMdd的字符串，表示该月最后一个交易日';
COMMENT ON COLUMN his_kline_month.open IS '本月开盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_month.high IS '本月最高价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_month.low IS '本月最低价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_month.close IS '本月收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_month.preclose IS '上月收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_month.volume IS '本月成交量，单位：股，整数';
COMMENT ON COLUMN his_kline_month.amount IS '本月成交额，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_month.adjust_flag IS '复权状态：1-后复权，2-前复权，3-不复权';
COMMENT ON COLUMN his_kline_month.change_rate IS '月涨跌幅，百分比，精确到6位小数。计算方式：[(本月收盘价-上月收盘价)/上月收盘价]*100%';
COMMENT ON COLUMN his_kline_month.turnover_rate IS '月换手率，百分比。计算方式：[本月成交量(股)/股票的流通股总股数(股)]*100%';
COMMENT ON COLUMN his_kline_month.create_time IS '数据创建时间，记录插入时间';
COMMENT ON COLUMN his_kline_month.update_time IS '数据修改时间，记录最后更新时间';

-- 创建触发器：在his_kline_month表更新时自动调用上面的函数
CREATE TRIGGER update_his_kline_month_modtime 
BEFORE UPDATE ON his_kline_month 
FOR EACH ROW 
EXECUTE FUNCTION update_modified_column();


-- 历史1分钟K线数据表
CREATE TABLE his_kline_1min (
    -- 主键，自增
    id BIGSERIAL PRIMARY KEY,
    -- TS代码，Tushare系统中的唯一标识
    ts_code VARCHAR(20),
    -- 股票编码
    stock_code VARCHAR(20),
    -- 股票名称
    stock_name VARCHAR(20),
    -- 交易日，格式：yyyyMMdd
    trade_date VARCHAR(8),
    -- 交易时间，格式：hhmm，表示该分钟的开始时间
    trade_time VARCHAR(4),
    -- 交易时间，格式：yyyyMMddhhmm，表示该分钟的开始时间
    trade_datetime VARCHAR(12),
    -- 该分钟开盘价，精度：小数点后4位；单位：人民币元
    open NUMERIC(20, 4),
    -- 该分钟最高价，精度：小数点后4位；单位：人民币元
    high NUMERIC(20, 4),
    -- 该分钟最低价，精度：小数点后4位；单位：人民币元
    low NUMERIC(20, 4),
    -- 该分钟收盘价，精度：小数点后4位；单位：人民币元
    close NUMERIC(20, 4),
    -- 前一分钟分钟收盘价，精度：小数点后4位；单位：人民币元
    preclose NUMERIC(20, 4),
    -- 该分钟成交量，单位：股
    volume NUMERIC(20, 0),
    -- 该分钟成交额，精度：小数点后4位；单位：人民币元
    amount NUMERIC(20, 4),
    -- 复权状态：1-后复权，2-前复权，3-不复权
    adjust_flag SMALLINT,
    -- 该分钟涨跌幅，精度：小数点后6位
    change_rate NUMERIC(10, 6),
    -- 该分钟换手率，百分比
    turnover_rate NUMERIC(10, 6),
    -- 数据创建时间，插入记录时自动设置当前时间
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 数据修改时间，更新记录时自动更新为当前时间
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建唯一约束，确保同一股票同一分钟仅一条记录
ALTER TABLE his_kline_1min ADD CONSTRAINT uk_his_kline_1min_code_date_time UNIQUE (ts_code, trade_date, trade_time);

-- 创建索引以提高查询性能
-- TS代码索引
CREATE INDEX idx_his_kline_1min_ts_code ON his_kline_1min (ts_code);
-- 股票编码索引
CREATE INDEX idx_his_kline_1min_stock_code ON his_kline_1min (stock_code);
-- 交易日索引
CREATE INDEX idx_his_kline_1min_trade_date ON his_kline_1min (trade_date);
-- 交易时间索引
CREATE INDEX idx_his_kline_1min_trade_time ON his_kline_1min (trade_time);
-- 组合索引：股票编码+交易日+交易时间
CREATE INDEX idx_his_kline_1min_stock_code_trade_date_trade_time ON his_kline_1min (stock_code, trade_date, trade_time);
-- 复权状态索引
CREATE INDEX idx_his_kline_1min_adjust_flag ON his_kline_1min (adjust_flag);

-- 表注释
COMMENT ON TABLE his_kline_1min IS '历史1分钟K线数据表';

-- 字段注释
COMMENT ON COLUMN his_kline_1min.id IS '主键，自增ID';
COMMENT ON COLUMN his_kline_1min.ts_code IS 'TS代码，Tushare系统中的唯一标识';
COMMENT ON COLUMN his_kline_1min.stock_code IS '股票编码，交易所公布的股票代码';
COMMENT ON COLUMN his_kline_1min.stock_name IS '股票名称';
COMMENT ON COLUMN his_kline_1min.trade_date IS '交易日，格式为yyyyMMdd的字符串';
COMMENT ON COLUMN his_kline_1min.trade_time IS '交易时间，格式为hhmm的字符串，表示该分钟的开始时间';
COMMENT ON COLUMN his_kline_1min.trade_datetime IS '交易时间，格式为yyyyMMddhhmm的字符串，表示该分钟的开始时间';
COMMENT ON COLUMN his_kline_1min.open IS '该分钟开盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_1min.high IS '该分钟最高价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_1min.low IS '该分钟最低价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_1min.close IS '该分钟收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_1min.preclose IS '前一分钟收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_1min.volume IS '该分钟成交量，单位：股，整数';
COMMENT ON COLUMN his_kline_1min.amount IS '该分钟成交额，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_1min.adjust_flag IS '复权状态：1-后复权，2-前复权，3-不复权';
COMMENT ON COLUMN his_kline_1min.change_rate IS '该分钟涨跌幅，百分比，精确到6位小数。计算方式：[(该分钟收盘价-前一分钟收盘价)/前一分钟收盘价]*100%';
COMMENT ON COLUMN his_kline_1min.turnover_rate IS '该分钟换手率，百分比。计算方式：[该分钟成交量(股)/股票的流通股总股数(股)]*100%';
COMMENT ON COLUMN his_kline_1min.create_time IS '数据创建时间，记录插入时间';
COMMENT ON COLUMN his_kline_1min.update_time IS '数据修改时间，记录最后更新时间';

-- 创建触发器：在his_kline_1min表更新时自动调用上面的函数
CREATE TRIGGER update_his_kline_1min_modtime 
BEFORE UPDATE ON his_kline_1min 
FOR EACH ROW 
EXECUTE FUNCTION update_modified_column();


-- 历史5分钟K线数据表
CREATE TABLE his_kline_5min (
    -- 主键，自增
    id BIGSERIAL PRIMARY KEY,
    -- TS代码，Tushare系统中的唯一标识
    ts_code VARCHAR(20),
    -- 股票编码
    stock_code VARCHAR(20),
    -- 股票名称
    stock_name VARCHAR(20),
    -- 交易日，格式：yyyyMMdd
    trade_date VARCHAR(8),
    -- 交易时间，格式：hhmm，表示该5分钟时段的开始时间
    trade_time VARCHAR(4),
    -- 交易时间，格式：yyyyMMddhhmm，表示该5分钟时段的开始时间
    trade_datetime VARCHAR(12),
    -- 该5分钟时段开盘价，精度：小数点后4位；单位：人民币元
    open NUMERIC(20, 4),
    -- 该5分钟时段最高价，精度：小数点后4位；单位：人民币元
    high NUMERIC(20, 4),
    -- 该5分钟时段最低价，精度：小数点后4位；单位：人民币元
    low NUMERIC(20, 4),
    -- 该5分钟时段收盘价，精度：小数点后4位；单位：人民币元
    close NUMERIC(20, 4),
    -- 前一5分钟时段收盘价，精度：小数点后4位；单位：人民币元
    preclose NUMERIC(20, 4),
    -- 该5分钟时段成交量，单位：股
    volume NUMERIC(20, 0),
    -- 该5分钟时段成交额，精度：小数点后4位；单位：人民币元
    amount NUMERIC(20, 4),
    -- 复权状态：1-后复权，2-前复权，3-不复权
    adjust_flag SMALLINT,
    -- 该5分钟时段涨跌幅，精度：小数点后6位
    change_rate NUMERIC(10, 6),
    -- 该5分钟时段换手率，百分比
    turnover_rate NUMERIC(10, 6),
    -- 数据创建时间，插入记录时自动设置当前时间
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 数据修改时间，更新记录时自动更新为当前时间
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
-- TS代码索引
CREATE INDEX idx_his_kline_5min_ts_code ON his_kline_5min (ts_code);
-- 股票编码索引
CREATE INDEX idx_his_kline_5min_stock_code ON his_kline_5min (stock_code);
-- 交易日索引
CREATE INDEX idx_his_kline_5min_trade_date ON his_kline_5min (trade_date);
-- 交易时间索引
CREATE INDEX idx_his_kline_5min_trade_time ON his_kline_5min (trade_time);
-- 组合索引：TS代码+交易日+交易时间，用于按股票和时间范围查询
CREATE INDEX idx_his_kline_5min_ts_code_trade_date_trade_time ON his_kline_5min (ts_code, trade_date, trade_time);
-- 组合索引：股票编码+交易日+交易时间
CREATE INDEX idx_his_kline_5min_stock_code_trade_date_trade_time ON his_kline_5min (stock_code, trade_date, trade_time);
-- 复权状态索引
CREATE INDEX idx_his_kline_5min_adjust_flag ON his_kline_5min (adjust_flag);

-- 表注释
COMMENT ON TABLE his_kline_5min IS '历史5分钟K线数据表';

-- 字段注释
COMMENT ON COLUMN his_kline_5min.id IS '主键，自增ID';
COMMENT ON COLUMN his_kline_5min.ts_code IS 'TS代码，Tushare系统中的唯一标识';
COMMENT ON COLUMN his_kline_5min.stock_code IS '股票编码，交易所公布的股票代码';
COMMENT ON COLUMN his_kline_5min.stock_name IS '股票名称';
COMMENT ON COLUMN his_kline_5min.trade_date IS '交易日，格式为yyyyMMdd的字符串';
COMMENT ON COLUMN his_kline_5min.trade_time IS '交易时间，格式为hhmm的字符串，表示该5分钟时段的开始时间';
COMMENT ON COLUMN his_kline_5min.trade_datetime IS '交易时间，格式为yyyyMMddhhmm的字符串，表示该5分钟时段的开始时间';
COMMENT ON COLUMN his_kline_5min.open IS '该5分钟时段开盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_5min.high IS '该5分钟时段最高价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_5min.low IS '该5分钟时段最低价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_5min.close IS '该5分钟时段收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_5min.preclose IS '前一5分钟时段收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_5min.volume IS '该5分钟时段成交量，单位：股，整数';
COMMENT ON COLUMN his_kline_5min.amount IS '该5分钟时段成交额，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_5min.adjust_flag IS '复权状态：1-后复权，2-前复权，3-不复权';
COMMENT ON COLUMN his_kline_5min.change_rate IS '该5分钟时段涨跌幅，百分比，精确到6位小数。计算方式：[(该时段收盘价-前一时段收盘价)/前一时段收盘价]*100%';
COMMENT ON COLUMN his_kline_5min.turnover_rate IS '该5分钟时段换手率，百分比。计算方式：[该时段成交量(股)/股票的流通股总股数(股)]*100%';
COMMENT ON COLUMN his_kline_5min.create_time IS '数据创建时间，记录插入时间';
COMMENT ON COLUMN his_kline_5min.update_time IS '数据修改时间，记录最后更新时间';

-- 创建触发器：在his_kline_5min表更新时自动调用上面的函数
CREATE TRIGGER update_his_kline_5min_modtime 
BEFORE UPDATE ON his_kline_5min 
FOR EACH ROW 
EXECUTE FUNCTION update_modified_column();


-- 历史15分钟K线数据表
CREATE TABLE his_kline_15min (
    -- 主键，自增
    id BIGSERIAL PRIMARY KEY,
    -- TS代码，Tushare系统中的唯一标识
    ts_code VARCHAR(20),
    -- 股票编码
    stock_code VARCHAR(20),
    -- 股票名称
    stock_name VARCHAR(20),
    -- 交易日，格式：yyyyMMdd
    trade_date VARCHAR(8),
    -- 交易时间，格式：hhmm，表示该15分钟时段的开始时间
    trade_time VARCHAR(4),
    -- 交易时间，格式：yyyyMMddhhmm，表示该15分钟时段的开始时间
    trade_datetime VARCHAR(12),
    -- 该15分钟时段开盘价，精度：小数点后4位；单位：人民币元
    open NUMERIC(20, 4),
    -- 该15分钟时段最高价，精度：小数点后4位；单位：人民币元
    high NUMERIC(20, 4),
    -- 该15分钟时段最低价，精度：小数点后4位；单位：人民币元
    low NUMERIC(20, 4),
    -- 该15分钟时段收盘价，精度：小数点后4位；单位：人民币元
    close NUMERIC(20, 4),
    -- 前一15分钟时段收盘价，精度：小数点后4位；单位：人民币元
    preclose NUMERIC(20, 4),
    -- 该15分钟时段成交量，单位：股
    volume NUMERIC(20, 0),
    -- 该15分钟时段成交额，精度：小数点后4位；单位：人民币元
    amount NUMERIC(20, 4),
    -- 复权状态：1-后复权，2-前复权，3-不复权
    adjust_flag SMALLINT,
    -- 该15分钟时段涨跌幅，精度：小数点后6位
    change_rate NUMERIC(10, 6),
    -- 该15分钟时段换手率，百分比
    turnover_rate NUMERIC(10, 6),
    -- 数据创建时间，插入记录时自动设置当前时间
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 数据修改时间，更新记录时自动更新为当前时间
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
-- TS代码索引
CREATE INDEX idx_his_kline_15min_ts_code ON his_kline_15min (ts_code);
-- 股票编码索引
CREATE INDEX idx_his_kline_15min_stock_code ON his_kline_15min (stock_code);
-- 交易日索引
CREATE INDEX idx_his_kline_15min_trade_date ON his_kline_15min (trade_date);
-- 交易时间索引
CREATE INDEX idx_his_kline_15min_trade_time ON his_kline_15min (trade_time);
-- 组合索引：TS代码+交易日+交易时间，用于按股票和时间范围查询
CREATE INDEX idx_his_kline_15min_ts_code_trade_date_trade_time ON his_kline_15min (ts_code, trade_date, trade_time);
-- 组合索引：股票编码+交易日+交易时间
CREATE INDEX idx_his_kline_15min_stock_code_trade_date_trade_time ON his_kline_15min (stock_code, trade_date, trade_time);
-- 复权状态索引
CREATE INDEX idx_his_kline_15min_adjust_flag ON his_kline_15min (adjust_flag);

-- 表注释
COMMENT ON TABLE his_kline_15min IS '历史15分钟K线数据表';

-- 字段注释
COMMENT ON COLUMN his_kline_15min.id IS '主键，自增ID';
COMMENT ON COLUMN his_kline_15min.ts_code IS 'TS代码，Tushare系统中的唯一标识';
COMMENT ON COLUMN his_kline_15min.stock_code IS '股票编码，交易所公布的股票代码';
COMMENT ON COLUMN his_kline_15min.stock_name IS '股票名称';
COMMENT ON COLUMN his_kline_15min.trade_date IS '交易日，格式为yyyyMMdd的字符串';
COMMENT ON COLUMN his_kline_15min.trade_time IS '交易时间，格式为hhmm的字符串，表示该15分钟时段的开始时间';
COMMENT ON COLUMN his_kline_15min.trade_datetime IS '交易时间，格式为yyyyMMddhhmm的字符串，表示该15分钟时段的开始时间';
COMMENT ON COLUMN his_kline_15min.open IS '该15分钟时段开盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_15min.high IS '该15分钟时段最高价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_15min.low IS '该15分钟时段最低价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_15min.close IS '该15分钟时段收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_15min.close IS '前一15分钟时段收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_15min.volume IS '该15分钟时段成交量，单位：股，整数';
COMMENT ON COLUMN his_kline_15min.amount IS '该15分钟时段成交额，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_15min.adjust_flag IS '复权状态：1-后复权，2-前复权，3-不复权';
COMMENT ON COLUMN his_kline_15min.change_rate IS '该15分钟时段涨跌幅，百分比，精确到6位小数。计算方式：[(该时段收盘价-前一时段收盘价)/前一时段收盘价]*100%';
COMMENT ON COLUMN his_kline_15min.turnover_rate IS '该15分钟时段换手率，百分比。计算方式：[该时段成交量(股)/股票的流通股总股数(股)]*100%';
COMMENT ON COLUMN his_kline_15min.create_time IS '数据创建时间，记录插入时间';
COMMENT ON COLUMN his_kline_15min.update_time IS '数据修改时间，记录最后更新时间';

-- 创建触发器：在his_kline_15min表更新时自动调用上面的函数
CREATE TRIGGER update_his_kline_15min_modtime 
BEFORE UPDATE ON his_kline_15min 
FOR EACH ROW 
EXECUTE FUNCTION update_modified_column();


-- 历史30分钟K线数据表
CREATE TABLE his_kline_30min (
    -- 主键，自增
    id BIGSERIAL PRIMARY KEY,
    -- TS代码，Tushare系统中的唯一标识
    ts_code VARCHAR(20),
    -- 股票编码
    stock_code VARCHAR(20),
    -- 股票名称
    stock_name VARCHAR(20),
    -- 交易日，格式：yyyyMMdd
    trade_date VARCHAR(8),
    -- 交易时间，格式：hhmm，表示该30分钟时段的开始时间
    trade_time VARCHAR(4),
    -- 交易时间，格式：yyyyMMddhhmm，表示该30分钟时段的开始时间
    trade_datetime VARCHAR(12),
    -- 该30分钟时段开盘价，精度：小数点后4位；单位：人民币元
    open NUMERIC(20, 4),
    -- 该30分钟时段最高价，精度：小数点后4位；单位：人民币元
    high NUMERIC(20, 4),
    -- 该30分钟时段最低价，精度：小数点后4位；单位：人民币元
    low NUMERIC(20, 4),
    -- 该30分钟时段收盘价，精度：小数点后4位；单位：人民币元
    close NUMERIC(20, 4),
    -- 前一30分钟时段收盘价，精度：小数点后4位；单位：人民币元
    preclose NUMERIC(20, 4),
    -- 该30分钟时段成交量，单位：股
    volume NUMERIC(20, 0),
    -- 该30分钟时段成交额，精度：小数点后4位；单位：人民币元
    amount NUMERIC(20, 4),
    -- 复权状态：1-后复权，2-前复权，3-不复权
    adjust_flag SMALLINT,
    -- 该30分钟时段涨跌幅，精度：小数点后6位
    change_rate NUMERIC(10, 6),
    -- 该30分钟时段换手率，百分比
    turnover_rate NUMERIC(10, 6),
    -- 数据创建时间，插入记录时自动设置当前时间
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 数据修改时间，更新记录时自动更新为当前时间
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
-- TS代码索引
CREATE INDEX idx_his_kline_30min_ts_code ON his_kline_30min (ts_code);
-- 股票编码索引
CREATE INDEX idx_his_kline_30min_stock_code ON his_kline_30min (stock_code);
-- 交易日索引
CREATE INDEX idx_his_kline_30min_trade_date ON his_kline_30min (trade_date);
-- 交易时间索引
CREATE INDEX idx_his_kline_30min_trade_time ON his_kline_30min (trade_time);
-- 组合索引：TS代码+交易日+交易时间，用于按股票和时间范围查询
CREATE INDEX idx_his_kline_30min_ts_code_trade_date_trade_time ON his_kline_30min (ts_code, trade_date, trade_time);
-- 组合索引：股票编码+交易日+交易时间
CREATE INDEX idx_his_kline_30min_stock_code_trade_date_trade_time ON his_kline_30min (stock_code, trade_date, trade_time);
-- 复权状态索引
CREATE INDEX idx_his_kline_30min_adjust_flag ON his_kline_30min (adjust_flag);

-- 表注释
COMMENT ON TABLE his_kline_30min IS '历史30分钟K线数据表';

-- 字段注释
COMMENT ON COLUMN his_kline_30min.id IS '主键，自增ID';
COMMENT ON COLUMN his_kline_30min.ts_code IS 'TS代码，Tushare系统中的唯一标识';
COMMENT ON COLUMN his_kline_30min.stock_code IS '股票编码，交易所公布的股票代码';
COMMENT ON COLUMN his_kline_30min.stock_name IS '股票名称';
COMMENT ON COLUMN his_kline_30min.trade_date IS '交易日，格式为yyyyMMdd的字符串';
COMMENT ON COLUMN his_kline_30min.trade_time IS '交易时间，格式为hhmm的字符串，表示该30分钟时段的开始时间';
COMMENT ON COLUMN his_kline_30min.trade_datetime IS '交易时间，格式为yyyyMMddhhmm的字符串，表示该30分钟时段的开始时间';
COMMENT ON COLUMN his_kline_30min.open IS '该30分钟时段开盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_30min.high IS '该30分钟时段最高价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_30min.low IS '该30分钟时段最低价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_30min.close IS '该30分钟时段收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_30min.preclose IS '前一30分钟时段收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_30min.volume IS '该30分钟时段成交量，单位：股，整数';
COMMENT ON COLUMN his_kline_30min.amount IS '该30分钟时段成交额，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_30min.adjust_flag IS '复权状态：1-后复权，2-前复权，3-不复权';
COMMENT ON COLUMN his_kline_30min.change_rate IS '该30分钟时段涨跌幅，百分比，精确到6位小数。计算方式：[(该时段收盘价-前一时段收盘价)/前一时段收盘价]*100%';
COMMENT ON COLUMN his_kline_30min.turnover_rate IS '该30分钟时段换手率，百分比。计算方式：[该时段成交量(股)/股票的流通股总股数(股)]*100%';
COMMENT ON COLUMN his_kline_30min.create_time IS '数据创建时间，记录插入时间';
COMMENT ON COLUMN his_kline_30min.update_time IS '数据修改时间，记录最后更新时间';

-- 创建触发器：在his_kline_30min表更新时自动调用上面的函数
CREATE TRIGGER update_his_kline_30min_modtime 
BEFORE UPDATE ON his_kline_30min 
FOR EACH ROW 
EXECUTE FUNCTION update_modified_column();


-- 历史60分钟K线数据表
CREATE TABLE his_kline_60min (
    -- 主键，自增
    id BIGSERIAL PRIMARY KEY,
    -- TS代码，Tushare系统中的唯一标识
    ts_code VARCHAR(20),
    -- 股票编码
    stock_code VARCHAR(20),
    -- 股票名称
    stock_name VARCHAR(20),
    -- 交易日，格式：yyyyMMdd
    trade_date VARCHAR(8),
    -- 交易时间，格式：hhmm，表示该60分钟时段的开始时间
    trade_time VARCHAR(4),
    -- 交易时间，格式：yyyyMMddhhmm，表示该60分钟时段的开始时间
    trade_datetime VARCHAR(12),
    -- 该60分钟时段开盘价，精度：小数点后4位；单位：人民币元
    open NUMERIC(20, 4),
    -- 该60分钟时段最高价，精度：小数点后4位；单位：人民币元
    high NUMERIC(20, 4),
    -- 该60分钟时段最低价，精度：小数点后4位；单位：人民币元
    low NUMERIC(20, 4),
    -- 该60分钟时段收盘价，精度：小数点后4位；单位：人民币元
    close NUMERIC(20, 4),
    -- 前一60分钟时段收盘价，精度：小数点后4位；单位：人民币元
    preclose NUMERIC(20, 4),
    -- 该60分钟时段成交量，单位：股
    volume NUMERIC(20, 0),
    -- 该60分钟时段成交额，精度：小数点后4位；单位：人民币元
    amount NUMERIC(20, 4),
    -- 复权状态：1-后复权，2-前复权，3-不复权
    adjust_flag SMALLINT,
    -- 该60分钟时段涨跌幅，精度：小数点后6位
    change_rate NUMERIC(10, 6),
    -- 该60分钟时段换手率，百分比
    turnover_rate NUMERIC(10, 6),
    -- 数据创建时间，插入记录时自动设置当前时间
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 数据修改时间，更新记录时自动更新为当前时间
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
-- TS代码索引
CREATE INDEX idx_his_kline_60min_ts_code ON his_kline_60min (ts_code);
-- 股票编码索引
CREATE INDEX idx_his_kline_60min_stock_code ON his_kline_60min (stock_code);
-- 交易日索引
CREATE INDEX idx_his_kline_60min_trade_date ON his_kline_60min (trade_date);
-- 交易时间索引
CREATE INDEX idx_his_kline_60min_trade_time ON his_kline_60min (trade_time);
-- 组合索引：TS代码+交易日+交易时间，用于按股票和时间范围查询
CREATE INDEX idx_his_kline_60min_ts_code_trade_date_trade_time ON his_kline_60min (ts_code, trade_date, trade_time);
-- 组合索引：股票编码+交易日+交易时间
CREATE INDEX idx_his_kline_60min_stock_code_trade_date_trade_time ON his_kline_60min (stock_code, trade_date, trade_time);
-- 复权状态索引
CREATE INDEX idx_his_kline_60min_adjust_flag ON his_kline_60min (adjust_flag);

-- 表注释
COMMENT ON TABLE his_kline_60min IS '历史60分钟K线数据表';

-- 字段注释
COMMENT ON COLUMN his_kline_60min.id IS '主键，自增ID';
COMMENT ON COLUMN his_kline_60min.ts_code IS 'TS代码，Tushare系统中的唯一标识';
COMMENT ON COLUMN his_kline_60min.stock_code IS '股票编码，交易所公布的股票代码';
COMMENT ON COLUMN his_kline_60min.stock_name IS '股票名称';
COMMENT ON COLUMN his_kline_60min.trade_date IS '交易日，格式为yyyyMMdd的字符串';
COMMENT ON COLUMN his_kline_60min.trade_time IS '交易时间，格式为hhmm的字符串，表示该60分钟时段的开始时间';
COMMENT ON COLUMN his_kline_60min.trade_datetime IS '交易时间，格式为yyyyMMddhhmm的字符串，表示该60分钟时段的开始时间';
COMMENT ON COLUMN his_kline_60min.open IS '该60分钟时段开盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_60min.high IS '该60分钟时段最高价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_60min.low IS '该60分钟时段最低价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_60min.close IS '该60分钟时段收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_60min.preclose IS '前一60分钟时段收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_60min.volume IS '该60分钟时段成交量，单位：股，整数';
COMMENT ON COLUMN his_kline_60min.amount IS '该60分钟时段成交额，单位：元，精确到4位小数';
COMMENT ON COLUMN his_kline_60min.adjust_flag IS '复权状态：1-后复权，2-前复权，3-不复权';
COMMENT ON COLUMN his_kline_60min.change_rate IS '该60分钟时段涨跌幅，百分比，精确到6位小数。计算方式：[(该时段收盘价-前一时段收盘价)/前一时段收盘价]*100%';
COMMENT ON COLUMN his_kline_60min.turnover_rate IS '该60分钟时段换手率，百分比。计算方式：[该时段成交量(股)/股票的流通股总股数(股)]*100%';
COMMENT ON COLUMN his_kline_60min.create_time IS '数据创建时间，记录插入时间';
COMMENT ON COLUMN his_kline_60min.update_time IS '数据修改时间，记录最后更新时间';

-- 创建触发器：在his_kline_60min表更新时自动调用上面的函数
CREATE TRIGGER update_his_kline_60min_modtime 
BEFORE UPDATE ON his_kline_60min 
FOR EACH ROW 
EXECUTE FUNCTION update_modified_column();


-- 立体K线数据表
CREATE TABLE anal_kline_rise_25pre (
    -- 主键，自增
    id BIGSERIAL PRIMARY KEY,
    -- TS代码，Tushare系统中的唯一标识
    ts_code VARCHAR(20),
    -- 股票编码
    stock_code VARCHAR(20),
    -- 股票名称
    stock_name VARCHAR(20),
    -- 交易日，格式：yyyyMMdd
    trade_date VARCHAR(8),
    -- 交易时间，格式：hhmm，表示该交易时段的开始时间
    trade_time VARCHAR(4),
    -- 交易时间，格式：yyyyMMddhhmm，表示该交易时段的开始时间
    trade_datetime VARCHAR(12),
    -- 该周期开盘价，精度：小数点后4位；单位：人民币元
    open NUMERIC(20, 4),
    -- 该周期最高价，精度：小数点后4位；单位：人民币元
    high NUMERIC(20, 4),
    -- 该周期最低价，精度：小数点后4位；单位：人民币元
    low NUMERIC(20, 4),
    -- 该周期收盘价，精度：小数点后4位；单位：人民币元
    close NUMERIC(20, 4),
    -- 前一周期收盘价，精度：小数点后4位；单位：人民币元
    preclose NUMERIC(20, 4),
    -- 成交量，单位：股
    volume NUMERIC(20, 0),
    -- 成交额，精度：小数点后4位；单位：人民币元
    amount NUMERIC(20, 4),
    -- 复权状态：1-后复权，2-前复权，3-不复权
    adjust_flag SMALLINT,
    -- 涨跌幅，精度：小数点后6位
    change_rate NUMERIC(10, 6),
    -- 换手率，百分比
    turnover_rate NUMERIC(10, 6),
    -- 数据创建时间，插入记录时自动设置当前时间
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- 数据修改时间，更新记录时自动更新为当前时间
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引以提高查询性能
-- TS代码索引
CREATE INDEX idx_anal_kline_rise_25pre_ts_code ON anal_kline_rise_25pre (ts_code);
-- 股票编码索引
CREATE INDEX idx_anal_kline_rise_25pre_stock_code ON anal_kline_rise_25pre (stock_code);
-- 交易日索引
CREATE INDEX idx_anal_kline_rise_25pre_trade_date ON anal_kline_rise_25pre (trade_date);
-- 交易时间索引
CREATE INDEX idx_anal_kline_rise_25pre_trade_time ON anal_kline_rise_25pre (trade_time);
-- 组合索引：TS代码+交易日+交易时间，用于按股票和时间范围查询
CREATE INDEX idx_anal_kline_rise_25pre_ts_code_trade_date_trade_time ON anal_kline_rise_25pre (ts_code, trade_date, trade_time);
-- 组合索引：股票编码+交易日+交易时间
CREATE INDEX idx_anal_kline_rise_25pre_stock_code_trade_date_trade_time ON anal_kline_rise_25pre (stock_code, trade_date, trade_time);
-- 复权状态索引
CREATE INDEX idx_anal_kline_rise_25pre_adjust_flag ON anal_kline_rise_25pre (adjust_flag);

-- 表注释
COMMENT ON TABLE anal_kline_rise_25pre IS '立体K线数据表（25%涨跌幅预处理）';

-- 字段注释
COMMENT ON COLUMN anal_kline_rise_25pre.id IS '主键，自增ID';
COMMENT ON COLUMN anal_kline_rise_25pre.ts_code IS 'TS代码，Tushare系统中的唯一标识';
COMMENT ON COLUMN anal_kline_rise_25pre.stock_code IS '股票编码，交易所公布的股票代码';
COMMENT ON COLUMN anal_kline_rise_25pre.stock_name IS '股票名称';
COMMENT ON COLUMN anal_kline_rise_25pre.trade_date IS '交易日，格式为yyyyMMdd的字符串';
COMMENT ON COLUMN anal_kline_rise_25pre.trade_time IS '交易时间，格式为hhmm的字符串，表示该交易时段的开始时间';
COMMENT ON COLUMN anal_kline_rise_25pre.trade_datetime IS '交易时间，格式为yyyyMMddhhmm的字符串，表示该交易时段的开始时间';
COMMENT ON COLUMN anal_kline_rise_25pre.open IS '该周期开盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN anal_kline_rise_25pre.high IS '该周期最高价，单位：元，精确到4位小数';
COMMENT ON COLUMN anal_kline_rise_25pre.low IS '该周期最低价，单位：元，精确到4位小数';
COMMENT ON COLUMN anal_kline_rise_25pre.close IS '该周期收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN anal_kline_rise_25pre.preclose IS '前一周期收盘价，单位：元，精确到4位小数';
COMMENT ON COLUMN anal_kline_rise_25pre.volume IS '成交量，单位：股，整数';
COMMENT ON COLUMN anal_kline_rise_25pre.amount IS '成交额，单位：元，精确到4位小数';
COMMENT ON COLUMN anal_kline_rise_25pre.adjust_flag IS '复权状态：1-后复权，2-前复权，3-不复权';
COMMENT ON COLUMN anal_kline_rise_25pre.change_rate IS '涨跌幅，百分比，精确到6位小数。计算方式：[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%';
COMMENT ON COLUMN anal_kline_rise_25pre.turnover_rate IS '换手率，百分比。计算方式：[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%';
COMMENT ON COLUMN anal_kline_rise_25pre.create_time IS '数据创建时间，记录插入时间';
COMMENT ON COLUMN anal_kline_rise_25pre.update_time IS '数据修改时间，记录最后更新时间';

-- 创建触发器：在anal_kline_rise_25pre表更新时自动调用上面的函数
CREATE TRIGGER update_anal_kline_rise_25pre_modtime 
BEFORE UPDATE ON anal_kline_rise_25pre 
FOR EACH ROW 
EXECUTE FUNCTION update_modified_column();