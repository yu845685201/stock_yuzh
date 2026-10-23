# AStock-Trading（meiqinggao）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：[github.com/meiqinggao/AStock-Trading](https://github.com/meiqinggao/AStock-Trading) · **分析依据**：本地克隆源码（`refrences/meiqinggao__AStock-Trading`，git commit `3ac0524` 2020-05-17）· **配套**：`../开源股票分析项目清单-20261010.md`
> **方法**：精读 `utils/StockDataRetriever.java`、`StockParser.java`、`StockUtils.java`、`RealtimeDataParser.java`、`scheduler/RealtimeDataRetrieveScheduler.java`、`controller/SearchController.java`、`schema.sql` 与 README。

---

## 0 定位与使用形态

AStock-Trading 是一个 **Java/Spring Boot 的 A 股「涨停追踪 + 概念搜索」工具**（2020 年的老项目）。核心：定时爬取同花顺概念板块，盘中定时拉新浪实时行情识别涨停股，按涨停股反查其所属概念，聚合出「涨停概念热度榜」，并支持按概念搜索近期强势股。

- 形态：Spring Boot 2.1.8 Web（thymeleaf 模板页）+ MySQL；`@Scheduled` 定时器。
- 技术栈：Java 8、Spring Data JPA、jsoup（HTML 解析）、gson、httpclient、joda-time、lombok。
- README 定位：「老版本只支持搜索概念/板块，新版本更多功能合适时机再开放」。

---

## 1 功能模块全景

| 模块 | 位置 | 职责 |
|---|---|---|
| 定时爬概念/股票 | `scheduler/RefreshDailyStockScheduler.java`、`StockUtils.refreshAllStockCodeAndName/refreshAllStockConcept/refreshAllStockField` | 每日同步股票名单与同花顺概念/行业 |
| 盘中实时涨停 | `scheduler/RealtimeDataRetrieveScheduler.java`、`RealtimeDataParser.java` | 定时拉新浪实时识别涨停、聚合涨停概念 |
| 涨停判定 | `StockUtils.isZhangTingForCode` | 分板块涨停判定（主板 10%/科创 20%） |
| 搜索 | `controller/SearchController.java` | 按概念/多概念交集搜近期强势股 |
| 展示 | `controller/` + `templates/*.html` | 涨停列表 / 涨停概念榜 / home |

---

## 2 数据源来源与采集机制

| 源 | 接口（URL，见 `StockURL.java`） | 内容 | 采集 |
|---|---|---|---|
| 新浪 | `http://hq.sinajs.cn/list=%s` | 实时行情（价格/涨跌幅/量） | `StockDataRetriever` 每批 500 只拼接，`sb` 预分配 250×n 字符 |
| 新浪 | `vip.stock.finance.sina.com.cn/.../Market_Center.getHQNodeData?num=80&...&page=%s` | 上一交易日收盘全表（分页 50 页） | 同步股票名单/改名 |
| 同花顺 | `basic.10jqka.com.cn/{code}/concept.html`、`/field.html` | 个股概念 / 所属行业 | jsoup 解析，GBK 编码，逐股 `sleep` |
| 同花顺 | `q.10jqka.com.cn/gn/`、`thshy/...` | 概念/行业涨幅榜 | - |
| Tushare | `api.waditu.com`（`daily`） | 历史日线（涨停日期回填/5日均量） | `ConnectionUtils.getDailyStockDetailedInfo` |
| 网易 | `quotes.money.163.com/cjmx/...xls` | 逐笔成交 | - |

---

## 3 分析逻辑

### 3.1 涨停判定（`StockUtils.isZhangTingForCode`，分板块精确到分）

```java
if (pct_chg < 9.85) return false;              // 涨幅 <9.85% 快速剪枝
// 688 科创板涨幅 20%、其余主板/创业 10%
factor = code.startsWith("688") ? 1.2 : 1.1;
zhangtingPrice = pre_close×factor（BigDecimal setScale(2, HALF_EVEN)）
return zhangtingPrice.compareTo(close) == 0;   // 收盘价 精确等于 涨停价（保留两位）
```
即：先按涨跌幅粗筛，再**按「收盘价 == 昨收×1.1（或1.2）四舍五入到分」严格判涨停**（差一分钱不算）。仅区分科创 20% 与主板 10%，未区分 ST 5%/北交所（2020 年北交所尚未设立）。

### 3.2 盘中涨停追踪（`RealtimeDataRetrieveScheduler`）

定时（`zone=CTT`，北京时间）：
- `0 24 9 ? * 1-5`（09:24）：清空当日涨停池 `zhangTingStocks/zhangTingConcepts`，并重新算「连续涨停」计数 `initConsecutiveZhangTings`。
- `*/30 25-59 9 / 0-31 11 / * 10,13,14 / 30 0 15`：盘中每 30 秒拉一次新浪实时（09:25-09:59、10:00-15:00 各时段）。

`RealtimeDataParser.retrieveAndParseStockData`：
1. `retrieveAllSinaRealtimeStocksData(所有代码)` 分批拉实时；
2. `StockParser.parseSourceDataToStocks` 解析，按 `isZhangTingForCode` 判涨停；
3. 涨停股 → 查该股概念（`stockConceptRepository`）→ 聚合进 `zhangTingConcepts`（概念→涨停股集合）；排除 `excludeConcepts`（标普道琼斯A股 / 富时罗素概念股 / 机构重仓 / 参股新三板）；
4. 已开板（不再涨停）的票从涨停池和概念集合中移除。

### 3.3 概念/强势股搜索（`SearchController`）

- `/zt`：涨停股列表（按封板时间倒序）。
- `/ztc`：涨停概念热度榜——按概念内涨停股数量倒序取前 15。
- `/search/{concept}`：某概念下「近期涨停」的股票，按最近涨停日倒序，超 10 只截 10。
- `/search`（POST）：支持 `&` 分隔多概念交集（`searchStockCodesByConcepts` 用 `retainAll` 取交集）；输入股票名提示「不支持，请搜索概念/行业」；模糊建议 `getSuggestedConcepts`（`contains` 前缀匹配前 10）。

### 3.4 数据模型（`schema.sql`）

- `stock_list`：`code`(唯一)、`stock_name`、`recent_zt`（最近涨停日）、`recent_touch_zt`（最近触板日）、`market_value`、`turnover`、`average_volume`（5 日均量）。
- `stock_concept`：`stock_code`→`concept`，`concept_type ∈ {concept, first, second, third}`（概念/一级/二级/三级行业），`source`（THS 等），唯一键 `(stock_code, concept, concept_type)`。

---

## 4 最终产物结构与格式

- **Web 页面**（thymeleaf）：`home.html`（搜索+涨停一览）、`zhangting.html`（当日涨停股列表）、`zhangtingconcept.html`（涨停概念榜前 15）、`concept.html`（概念搜索结果）。
- **数据库**：MySQL 双表（`stock_list` + `stock_concept`），涨停日期/5 日均量等均落库供搜索查询。
- **涨停数据结构**（`ZhangTingStock`）：`code/name/time(首次封板时间 HH:mm)/连板次数`。

---

## 5 坑与注意事项

1. **⚠️ 硬编码 Tushare Token 泄露**：`config/constant/StockURL.java` 里 `TUSHARE_PRO_Daily_BODY` 直接写死了 `"token": "57b24baa..."`——属于凭证 git 提交泄露，应视为失效并轮换。
2. **涨停判定只区分科创/主板**：`688` 用 1.2、其余 1.1，未处理 ST 5%（`pct_chg<9.85` 快剪会把 ST 涨停误判为非涨停）、未处理北交所（2020 年未设立的地理时间差）；创业 300 当时也是 10%，现在创业板已是 20%，跑今日需改。
3. **新浪 `hq.sinajs.cn` 需 Referer/限频**：老接口，大量请求可能封 IP；代码无重试退避（仅靠 500/批拆分）。
4. **概念爬取逐股 sleep**：`refreshAllStockConcept` 全市场逐股拉同花顺 concept 页 + `sleepInMillis`，几千只要跑很久（README「老版本」定位、无并发）。
5. **`refreshStockDate_ZT` 依赖 Tushare 日线回填**：从当天往前逐交易日拉全市场日线补涨停日期，交易日判定只靠 `dayOfWeek 1-5`（不排法定节假日）。
6. **概念搜索只支持概念/行业**：`POST /search` 里输入股票名会提示「暂不支持」，README 也说「老版本只支持搜索概念/板块」。
7. **`getPreviousDayStocksInfoForPage(page)` 固定翻 50 页找股票名单**：`PAGE_NUM=50`，若新上市股票在 50×80 之外可能漏掉。

---

## 6 可借鉴的设计要点

1. **涨停判定「按分取整 + BigDecimal HALF_EVEN」**：`pre_close×1.1` 精确到分、收盘价精确相等才算涨停，避免了「涨幅≥9.9% 就算涨停」的一刀切（差一分不算、四舍五入规则明确），是严谨的涨停口径。
2. **涨停 → 概念反向聚合**：从「涨停股」反查「所属概念」→ 聚合「概念热度榜」，用板块涨停家数表征题材强度，是打板复盘的核心视角。
3. **盘中 30s 定时 + 早盘清空**：`resetOnMorning` 每天 09:24 清空、盘中每 30s 增量更新「当前仍在涨停状态的股票」，并实时移除开板股，形成「涨停-开板」的动态数据集。
4. **多概念交集检索**：`&` 分隔概念 → `retainAll` 取股票交集，快速圈定「同时属于某几个题材」的票，配合涨停榜使用是短线选股利器。
5. **概念分类三档**（concept/first/second/third）+ 唯一键，把「题材名」与「行业层级」结构化，避免同花顺概念口径混杂。

---

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `utils/StockUtils.java` | 涨停判定、概念/行业/涨停日期同步、连续涨停统计 |
| `utils/StockDataRetriever.java` | 新浪实时行情分批拉取 |
| `utils/StockParser.java` | 新浪实时字符串解析成 RealtimeStock |
| `utils/RealtimeDataParser.java` | 涨停聚合 + 涨停概念场更新 |
| `scheduler/RealtimeDataRetrieveScheduler.java` | 盘中 30s 定时 + 早盘清空 |
| `scheduler/RefreshDailyStockScheduler.java` | 每日股票名单/概念更新定时 |
| `controller/SearchController.java` | 概念搜索/涨停榜/概念榜 |
| `constant/StockURL.java` | 数据源 URL + （⚠️硬编码 token） |
| `utils/HtmlParser.java` | jsoup 解析同花顺概念/行业页 |
| `resources/schema.sql` | stock_list / stock_concept 两表结构 |