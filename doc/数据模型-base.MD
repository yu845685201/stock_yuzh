# 股票分析系统数据库设计

## 基础数据

1. **股票基本信息 - base_stock_info**
   
字段名 | 字段类型 | 描述 | 说明
-- | -- | -- | --
id | bigint | 主键 | 自增
ts_code | varchar(20) | ts代码
stock_code | varchar(20) | 股票编码
stock_name | varchar(20) | 股票名称
cnspell | varchar(10) | 拼音缩写
market_code | varchar(5) | 市场编码
market_name | varchar(20) | 市场名称
exchange_code | varchar(10) | 交易所编码
sector_code | varchar(20) | 板块编码
sector_name | varchar(20) | 板块名称
industry_code | varchar(20) | 行业编码
industry_name | varchar(20) | 行业名称
list_status | varchar(2) | 上市状态 | L-上市，D-退市，P-暂停上市
list_date | varchar(8) | 上市日期，yyyyMMdd
delist_date | varchar(8) | 退市日期，yyyyMMdd
create_time | datetime | 数据创建时间 | 数据创建时自动使用当前时间赋值
update_time | datetime | 数据修改时间 | 数据更新时自动使用当前时间赋值

2. **基本面信息 - base_fundamentals_info**

字段名 | 字段类型 | 描述 | 说明
-- | -- | -- | --
id | bigint | 主键 | 自增
ts_code | varchar(20) | ts代码
stock_code | varchar(20) | 股票编码
stock_name | varchar(20) | 股票名称
disclosure_date | datetime | 信息披露日期
total_share | numeric | 总股本
float_share | numeric | 流通股本
create_time | datetime | 数据创建时间 | 数据创建时自动使用当前时间赋值
update_time | datetime | 数据修改时间 | 数据更新时自动使用当前时间赋值

## 指数数据
1. **指数数据 - trade_index_info**

字段名 | 字段类型 | 描述 | 说明
-- | -- | -- | --
id | bigint | 主键 | 自增
ts_code | varchar(20) | ts代码
index_code | varchar(20) | 指数编码
index_name | varchar(20) | 指数名称
trade_date | varchar(8) | 交易日，yyyyMMdd
open | numeric | 今日开盘价 | 精度：小数点后4位；单位：人民币元
high | numeric | 最高价 | 精度：小数点后4位；单位：人民币元
low | numeric | 最低价 | 精度：小数点后4位；单位：人民币元
close | numeric | 今日收盘价 | 精度：小数点后4位；单位：人民币元
preclose | numeric | 昨日收盘价 | 精度：小数点后4位；单位：人民币元
volume | numeric | 成交量 | 单位：股
amount | numeric | 成交额 | 精度：小数点后4位；单位：人民币元
change_rate | numeric | 涨跌幅 | 精度：小数点后6位
create_time | datetime | 数据创建时间 | 数据创建时自动使用当前时间赋值
update_time | datetime | 数据修改时间 | 数据更新时自动使用当前时间赋值

## 历史k线数据
1. **历史日k线数据 - his_kline_day**

字段名 | 字段类型 | 描述 | 说明
-- | -- | -- | --
id | bigint | 主键 | 自增
ts_code | varchar(20) | ts代码
stock_code | varchar(20) | 指数编码
stock_name | varchar(20) | 指数名称
trade_date | varchar(8) | 交易日，yyyyMMdd
open | numeric | 今日开盘价 | 精度：小数点后4位；单位：人民币元
high | numeric | 最高价 | 精度：小数点后4位；单位：人民币元
low | numeric | 最低价 | 精度：小数点后4位；单位：人民币元
close | numeric | 今日收盘价 | 精度：小数点后4位；单位：人民币元
preclose | numeric | 昨日收盘价 | 精度：小数点后4位；单位：人民币元
volume | numeric | 成交量 | 单位：股
amount | numeric | 成交额 | 精度：小数点后4位；单位：人民币元
trade_status | smallint | 交易状态 | 1：正常交易，0：停牌
is_st | boolean | 是否ST股 | 1：是，0：否
adjust_flag | smallint | 复权状态 | 1：后复权， 2：前复权，3：不复权
change_rate | numeric | 涨跌幅 | 精度：小数点后6位<Br/>日涨跌幅=[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%
turnover_rate | numeric | 换手率 | 换手率=[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%
pe_ttm | numeric | 滚动市盈率 | (指定交易日的股票收盘价/指定交易日的每股盈余TTM)=(指定交易日的股票收盘价*截至当日公司总股本)/归属母公司股东净利润TTM
pb_rate | numeric | 市净率 | (指定交易日的股票收盘价/指定交易日的每股净资产)=总市值/(最近披露的归属母公司股东的权益-其他权益工具)
ps_ttm | numeric | 滚动市销率 | (指定交易日的股票收盘价/指定交易日的每股销售额)=(指定交易日的股票收盘价*截至当日公司总股本)/营业总收入TTM
pcf_ttm | numeric | 滚动市现率 | (指定交易日的股票收盘价/指定交易日的每股现金流TTM)=(指定交易日的股票收盘价*截至当日公司总股本)/现金以及现金等价物净增加额TTM
create_time | datetime | 数据创建时间 | 数据创建时自动使用当前时间赋值
update_time | datetime | 数据修改时间 | 数据更新时自动使用当前时间赋值

2. **历史周k线数据 - his_kline_week**

字段名 | 字段类型 | 描述 | 说明
-- | -- | -- | --
id | bigint | 主键 | 自增
ts_code | varchar(20) | ts代码
stock_code | varchar(20) | 指数编码
stock_name | varchar(20) | 指数名称
trade_date | varchar(8) | 交易日，yyyyMMdd
open | numeric | 该周开盘价 | 精度：小数点后4位；单位：人民币元
high | numeric | 该周最高价 | 精度：小数点后4位；单位：人民币元
low | numeric | 该周最低价 | 精度：小数点后4位；单位：人民币元
close | numeric | 该周今日收盘价 | 精度：小数点后4位；单位：人民币元
preclose | numeric | 上周收盘价 | 精度：小数点后4位；单位：人民币元
volume | numeric | 成交量 | 单位：股
amount | numeric | 成交额 | 精度：小数点后4位；单位：人民币元
adjust_flag | smallint | 复权状态 | 1：后复权， 2：前复权，3：不复权
change_rate | numeric | 涨跌幅 | 精度：小数点后6位<br/>日涨跌幅=[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%
turnover_rate | numeric | 换手率 | 换手率=[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%
create_time | datetime | 数据创建时间 | 数据创建时自动使用当前时间赋值
update_time | datetime | 数据修改时间 | 数据更新时自动使用当前时间赋值

3. **历史月k线数据 - his_kline_month**

字段名 | 字段类型 | 描述 | 说明
-- | -- | -- | --
id | bigint | 主键 | 自增
ts_code | varchar(20) | ts代码
stock_code | varchar(20) | 指数编码
stock_name | varchar(20) | 指数名称
trade_date | varchar(8) | 交易日，yyyyMMdd
open | numeric | 该月开盘价 | 精度：小数点后4位；单位：人民币元
high | numeric | 该月最高价 | 精度：小数点后4位；单位：人民币元
low | numeric | 该月最低价 | 精度：小数点后4位；单位：人民币元
close | numeric | 该月收盘价 | 精度：小数点后4位；单位：人民币元
preclose | numeric | 上一月收盘价 | 精度：小数点后4位；单位：人民币元
volume | numeric | 成交量 | 单位：股
amount | numeric | 成交额 | 精度：小数点后4位；单位：人民币元
adjust_flag | smallint | 复权状态 | 1：后复权， 2：前复权，3：不复权
change_rate | numeric | 涨跌幅 | 精度：小数点后6位<br/>日涨跌幅=[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%
turnover_rate | numeric | 换手率 | 换手率=[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%
create_time | datetime | 数据创建时间 | 数据创建时自动使用当前时间赋值
update_time | datetime | 数据修改时间 | 数据更新时自动使用当前时间赋值

4. **历史1分钟k线数据 - his_kline_1min**

字段名 | 字段类型 | 描述 | 说明
-- | -- | -- | --
id | bigint | 主键 | 自增
ts_code | varchar(20) | ts代码
stock_code | varchar(20) | 指数编码
stock_name | varchar(20) | 指数名称
trade_date | varchar(8) | 交易日，yyyyMMdd
trade_time | varchar(4) | 交易时间，hhmm
open | numeric | 该1分钟开盘价 | 精度：小数点后4位；单位：人民币元
high | numeric | 该1分钟最高价 | 精度：小数点后4位；单位：人民币元
low | numeric | 该1分钟最低价 | 精度：小数点后4位；单位：人民币元
close | numeric | 该1分钟收盘价 | 精度：小数点后4位；单位：人民币元
preclose | numeric | 前一1分钟收盘价 | 精度：小数点后4位；单位：人民币元
volume | numeric | 成交量 | 单位：股
amount | numeric | 成交额 | 精度：小数点后4位；单位：人民币元
adjust_flag | smallint | 复权状态 | 1：后复权， 2：前复权，3：不复权
change_rate | numeric | 涨跌幅 | 精度：小数点后6位<br/>日涨跌幅=[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%
turnover_rate | numeric | 换手率 | 换手率=[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%
create_time | datetime | 数据创建时间 | 数据创建时自动使用当前时间赋值
update_time | datetime | 数据修改时间 | 数据更新时自动使用当前时间赋值

5. **历史5分钟k线数据 - his_kline_5min**

字段名 | 字段类型 | 描述 | 说明
-- | -- | -- | --
id | bigint | 主键 | 自增
ts_code | varchar(20) | ts代码
stock_code | varchar(20) | 指数编码
stock_name | varchar(20) | 指数名称
trade_date | varchar(8) | 交易日，yyyyMMdd
trade_time | varchar(4) | 交易时间，hhmm
open | numeric | 该5分钟开盘价 | 精度：小数点后4位；单位：人民币元
high | numeric | 该5分钟最高价 | 精度：小数点后4位；单位：人民币元
low | numeric | 该5分钟最低价 | 精度：小数点后4位；单位：人民币元
close | numeric | 该5分钟收盘价 | 精度：小数点后4位；单位：人民币元
preclose | numeric | 前一5分钟收盘价 | 精度：小数点后4位；单位：人民币元
volume | numeric | 成交量 | 单位：股
amount | numeric | 成交额 | 精度：小数点后4位；单位：人民币元
adjust_flag | smallint | 复权状态 | 1：后复权， 2：前复权，3：不复权
change_rate | numeric | 涨跌幅 | 精度：小数点后6位<br/>日涨跌幅=[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%
turnover_rate | numeric | 换手率 | 换手率=[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%
create_time | datetime | 数据创建时间 | 数据创建时自动使用当前时间赋值
update_time | datetime | 数据修改时间 | 数据更新时自动使用当前时间赋值

5. **历史15分钟k线数据 - his_kline_15min**

字段名 | 字段类型 | 描述 | 说明
-- | -- | -- | --
id | bigint | 主键 | 自增
ts_code | varchar(20) | ts代码
stock_code | varchar(20) | 指数编码
stock_name | varchar(20) | 指数名称
trade_date | varchar(8) | 交易日，yyyyMMdd
trade_time | varchar(4) | 交易时间，hhmm
open | numeric | 该15分钟开盘价 | 精度：小数点后4位；单位：人民币元
high | numeric | 该15分钟最高价 | 精度：小数点后4位；单位：人民币元
low | numeric | 该15分钟最低价 | 精度：小数点后4位；单位：人民币元
close | numeric | 该15分钟收盘价 | 精度：小数点后4位；单位：人民币元
preclose | numeric | 前一15分钟收盘价 | 精度：小数点后4位；单位：人民币元
volume | numeric | 成交量 | 单位：股
amount | numeric | 成交额 | 精度：小数点后4位；单位：人民币元
adjust_flag | smallint | 复权状态 | 1：后复权， 2：前复权，3：不复权
change_rate | numeric | 涨跌幅 | 精度：小数点后6位<br/>日涨跌幅=[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%
turnover_rate | numeric | 换手率 | 换手率=[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%
create_time | datetime | 数据创建时间 | 数据创建时自动使用当前时间赋值
update_time | datetime | 数据修改时间 | 数据更新时自动使用当前时间赋值

6. **历史30分钟k线数据 - his_kline_30min**

字段名 | 字段类型 | 描述 | 说明
-- | -- | -- | --
id | bigint | 主键 | 自增
ts_code | varchar(20) | ts代码
stock_code | varchar(20) | 指数编码
stock_name | varchar(20) | 指数名称
trade_date | varchar(8) | 交易日，yyyyMMdd
trade_time | varchar(4) | 交易时间，hhmm
open | numeric | 该30分钟开盘价 | 精度：小数点后4位；单位：人民币元
high | numeric | 该30分钟最高价 | 精度：小数点后4位；单位：人民币元
low | numeric | 该30分钟最低价 | 精度：小数点后4位；单位：人民币元
close | numeric | 该30分钟收盘价 | 精度：小数点后4位；单位：人民币元
preclose | numeric | 前一30分钟收盘价 | 精度：小数点后4位；单位：人民币元
volume | numeric | 成交量 | 单位：股
amount | numeric | 成交额 | 精度：小数点后4位；单位：人民币元
adjust_flag | smallint | 复权状态 | 1：后复权， 2：前复权，3：不复权
change_rate | numeric | 涨跌幅 | 精度：小数点后6位<br/>日涨跌幅=[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%
turnover_rate | numeric | 换手率 | 换手率=[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%
create_time | datetime | 数据创建时间 | 数据创建时自动使用当前时间赋值
update_time | datetime | 数据修改时间 | 数据更新时自动使用当前时间赋值

7. **历史60分钟k线数据 - his_kline_60min**

字段名 | 字段类型 | 描述 | 说明
-- | -- | -- | --
id | bigint | 主键 | 自增
ts_code | varchar(20) | ts代码
stock_code | varchar(20) | 指数编码
stock_name | varchar(20) | 指数名称
trade_date | varchar(8) | 交易日，yyyyMMdd
trade_time | varchar(4) | 交易时间，hhmm
open | numeric | 该60分钟开盘价 | 精度：小数点后4位；单位：人民币元
high | numeric | 该60分钟最高价 | 精度：小数点后4位；单位：人民币元
low | numeric | 该60分钟最低价 | 精度：小数点后4位；单位：人民币元
close | numeric | 该60分钟收盘价 | 精度：小数点后4位；单位：人民币元
preclose | numeric | 前一60分钟收盘价 | 精度：小数点后4位；单位：人民币元
volume | numeric | 成交量 | 单位：股
amount | numeric | 成交额 | 精度：小数点后4位；单位：人民币元
adjust_flag | smallint | 复权状态 | 1：后复权， 2：前复权，3：不复权
change_rate | numeric | 涨跌幅 | 精度：小数点后6位<br/>日涨跌幅=[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%
turnover_rate | numeric | 换手率 | 换手率=[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%
create_time | datetime | 数据创建时间 | 数据创建时自动使用当前时间赋值
update_time | datetime | 数据修改时间 | 数据更新时自动使用当前时间赋值


## 分析数据
1. **立体k线数据 - anal_kline_rise_25pre**

字段名 | 字段类型 | 描述 | 说明
-- | -- | -- | --
id | bigint | 主键 | 自增
ts_code | varchar(20) | ts代码
stock_code | varchar(20) | 指数编码
stock_name | varchar(20) | 指数名称
trade_date | varchar(8) | 交易日，yyyyMMdd
trade_time | varchar(4) | 交易时间，hhmm
open | numeric | 该周期开盘价 | 精度：小数点后4位；单位：人民币元
high | numeric | 该周期最高价 | 精度：小数点后4位；单位：人民币元
low | numeric | 该周期最低价 | 精度：小数点后4位；单位：人民币元
close | numeric | 该周期收盘价 | 精度：小数点后4位；单位：人民币元
preclose | numeric | 前一周期收盘价 | 精度：小数点后4位；单位：人民币元
volume | numeric | 成交量 | 单位：股
amount | numeric | 成交额 | 精度：小数点后4位；单位：人民币元
adjust_flag | smallint | 复权状态 | 1：后复权， 2：前复权，3：不复权
change_rate | numeric | 涨跌幅 | 精度：小数点后6位<br/>日涨跌幅=[(指定交易日的收盘价-指定交易日前收盘价)/指定交易日前收盘价]*100%
turnover_rate | numeric | 换手率 | 换手率=[指定交易日的成交量(股)/指定交易日的股票的流通股总股数(股)]*100%
create_time | datetime | 数据创建时间 | 数据创建时自动使用当前时间赋值
update_time | datetime | 数据修改时间 | 数据更新时自动使用当前时间赋值



