# RL-Stock（wangshub/RL-Stock）深度分析

> **分析日期**：2026-10-11 · **仓库 URL**：[github.com/wangshub/RL-Stock](https://github.com/wangshub/RL-Stock) · **分析依据**：本地克隆源码（`refrences/wangshub__RL-Stock`，git commit `22d2cbf` 2020-03-28）· **配套**：`../开源股票分析项目清单-20261010.md`
> **方法**：精读 `rlenv/StockTradingEnv0.py`（Gym 环境）、`main.py`（PPO2 训练/推理）、`get_stock_data.py`（baostock 取数）与 README，全部结论来自源码实现。

---

## 0 定位与使用形态

RL-Stock 是一个**教学演示型项目**：用 OpenAI Gym 封装一个 A 股单只股票交易环境，用 stable-baselines 的 PPO2 训练，验证「深度强化学习能否自动炒股」这一命题。README 明确自述「俺完全是股票没入门的新手」「数据和方法皆来源于网络，无法保证有效性，**Just For Fun**」。

- 形态：单文件环境 + 单文件训练入口 + 取数脚本 + 可视化 notebook（`vis.ipynb`）；无 Web、无回测框架、无实盘。
- 技术栈（requirements.txt 锁死在 2020 年）：`gym==0.17.1`、`stable-baselines==2.10.0`、`tensorflow-gpu==1.15.2`、`tensorboard==1.14.0`、`baostock==0.8.8`、`pandas/numpy/matplotlib`（注意是 TF1/stable-baselines 2.x 老版本，已不兼容新环境）。
- 数据：baostock 免费日 K，本地 CSV（`stockdata/train|test/{code}.{name}.csv`）。

---

## 1 功能模块全景

| 模块 | 位置 | 职责 |
|---|---|---|
| 数据下载 | `get_stock_data.py` | baostock 拉全市场历史日 K，按 train/test 切成 CSV |
| Gym 环境 | `rlenv/StockTradingEnv0.py` | 观测/动作/奖励/step/reset/render |
| 训练+推理 | `main.py` | `stock_trade()`（PPO2 训练 1e4 步 + 逐日 predict）、`test_a_stock_trade()`（单股画图）、`multi_stock_trade()`（批量跑并 pickle） |
| 可视化 | `vis.ipynb` | 训练曲线/盈亏分布/饼图统计 |

---

## 2 数据源来源与采集机制

| 源 | 接口（函数） | 内容 | 落盘格式 |
|---|---|---|---|
| baostock | `bs.query_all_stock(date)` | 某日全市场代码+名称列表 | 内存 DataFrame |
| baostock | `bs.query_history_k_data_plus(code, fields, start_date, end_date)` | 单票日 K | `stockdata/{train,test}/{code}.{name}.csv` |

`get_stock_data.py` 拉取字段（`Downloader.fields`）：
```
date,code,open,high,low,close,volume,amount,adjustflag,turn,
tradestatus,pctChg,peTTM,pbMRQ,psTTM,pcfNcfTTM,isST
```
窗口划分（README 明确）：训练 `1990-01-01 ~ 2019-11-29`，测试 `2019-12-01 ~ 2019-12-31`（末尾 1 个月）。默认 `date_end='2020-03-23'` 硬编码（下载脚本里按此切分）。

---

## 3 分析逻辑（策略 = 强化学习环境）

### 3.1 观测（Observation，19 维 Box [0,1]）

`_next_observation()` 归一化到 [0,1]（README 强调「必须归一化到 [-1,1] 区间内」以利于收敛），实际 19 维：

| 索引 | 来源字段 | 归一化 | 含义 |
|---|---|---|---|
| 0–3 | open/high/low/close | ÷ 5000 (`MAX_SHARE_PRICE`) | 价格 |
| 4 | volume | ÷ 100e8 (`MAX_VOLUME`) | 成交量 |
| 5 | amount | ÷ 3e10 (`MAX_AMOUNT`) | 成交额 |
| 6 | adjustflag | ÷ 10 | 复权状态 |
| 7 | tradestatus | ÷ 1 | 交易状态 |
| 8 | pctChg | ÷ 100 | 涨跌幅 |
| 9 | peTTM | ÷ 1e4 | 滚动市盈率 |
| 10 | pbMRQ | ÷ 100 | 市净率 |
| 11 | psTTM | ÷ 100 | 市销率 |
| 12 | pctChg | ÷ 1e3 | （**pctChg 重复出现**） |
| 13 | balance | ÷ MAX_ACCOUNT_BALANCE | 现金 |
| 14 | max_net_worth | ÷ MAX_ACCOUNT_BALANCE | 历史峰值净值 |
| 15 | shares_held | ÷ MAX_NUM_SHARES | 持仓股数 |
| 16 | cost_basis | ÷ 5000 | 持仓成本 |
| 17 | total_shares_sold | ÷ MAX_NUM_SHARES | 累计卖出 |
| 18 | total_sales_value | ÷ (MAX_NUM_SHARES×5000) | 累计卖出额 |

> 发现一处实现瑕疵：索引 8 和 12 都是 `pctChg`（一个 ÷100、一个 ÷1e3），`pcfNcfTTM`（滚动市现率）和 `isST` 字段虽被下载却未进入观测；README 观测表里列了 `preclose`（昨收）但源码观测里**没有** preclose。即：19 维观测里含一个重复维度、缺昨天收盘与市现率。

### 3.2 动作（Action，Box [0,3]×[0,1]）

`action[0]` 取值分段解释：
- `action[0] < 1` → **买入** `action[1]` 比例的可用资金（`int(balance/price × amount)` 股）；
- `1 ≤ action[0] < 2` → **卖出** `action[1]` 比例的持仓；
- `action[0] ≥ 2` → **持有**（README 写动作类型 3，实测是 ≥2 的连续段）。

**成交价**：`current_price = random.uniform(当日 open, 当日 close)`（在开收盘价之间随机取一点），作为当日唯一的撮合价——这是唯一的「滑点」近似。

**持仓成本**：加权平均，`cost_basis = (prev_cost + additional_cost) / (shares_held + shares_bought)`；清仓后 `cost_basis = 0`。

### 3.3 奖励（Reward）

```python
reward = self.net_worth - INITIAL_ACCOUNT_BALANCE   # INITIAL_ACCOUNT_BALANCE = 10000
reward = 1 if reward > 0 else -100
```
即：盈利给 +1 的稀疏正反馈，亏损统一给 -100 的大惩罚（README 解释：为了让网络更快学到盈利策略）。net_worth = 现金 + 持仓市值。

### 3.4 训练循环与终止（`main.py: stock_trade`）

- `DummyVecEnv` 包装单环境；`PPO2(MlpPolicy, env, tensorboard_log='./log')`，`model.learn(total_timesteps=1e4)`（1 万步）。
- **循环训练**：`step()` 里 `current_step` 到末尾时 `current_step = 0` 回卷（`# done = True` 被注释掉），只有 `net_worth <= 0` 才 `done = True`。即训练在数据上无限滚动。
- 测试：`obs = env.reset()` 后逐日 `model.predict(obs)` → `env.step(action)` → `env.render()` 收集每日 profit；测试集 `len(df_test)-1` 天。
- `multi_stock_trade`：从 `600000` 起遍历 3000 个代码（`find_file` 在 `stockdata/train` 里按代码名找文件），批量跑并 `pickle.dump` 结果。

---

## 4 最终产物结构与格式

- **单股结果**：`main.py: test_a_stock_trade('sh.600036')` 生成 `img/sh.600036.png`——横轴 step、纵轴 profit 的点线图（`render()` 逐日打印 Step/Balance/Shares/Net worth/Profit 到 stdout）。
- **批量结果**：`code-600000-603000.pkl`（pickle 的利润序列列表）。
- **可视化**（`vis.ipynb`）：README 展示的训练曲线、`pie.png`（1002 只股票的盈亏分布：盈利 44.5% / 不亏不赚 46.5% / 亏损 9.0%）、`hist.png`（利润直方图）。
- **README 声称结果**：单股招商银行 `sh.600036` 初始本金 10000、模拟 20 天盈利约 400；多股 1002 只的盈亏占比如上。

---

## 5 坑与注意事项

1. **完全无交易成本与 A 股规则**：没有佣金/印花税、没有 T+1 限制（当天买当天卖）、没有涨跌停约束（跌停也能卖）。A 股 T+1 与涨跌停是硬约束，本环境完全未建模——这也是 README 自嘲「Just For Fun」的原因。
2. **环境版本已严重过时**：依赖锁在 `gym 0.17` / `stable-baselines 2.10` / `TF 1.15`（Python 3.6 时代），新环境（Python 3.8+/新版 stable-baselines3）无法直接运行；`gym.Env` 的 `render(mode=...)` 旧签名也与新版 gym 不兼容。
3. **奖励函数过于粗糙**：`1 / -100` 二值化丢掉利润额信息，无法区分赚 1 元和赚 1 万；`net_worth<=0` 才会终止，回卷训练导致 `current_step` 状态与真实步对齐困难。
4. **观测有重复维度**：`pctChg` 出现两次（÷100 与 ÷1e3），`pcfNcfTTM`、`isST`、`preclose` 下载了却没用进观测（见 §3.1），观测维度有冗余与缺失。
5. **随机成交价引入不确定性但不可复现**：`random.uniform(open, close)` 未设随机种子，同一策略两次运行结果不同；且它不是真实滑点模型。
6. **`action[0]` 分段阈值靠 Box 连续值**：通过 `<1`/`<2` 判断买/卖/持有，训练初期动作分布未收敛时买卖倾向取决于网络输出，README 也提示「network 在训练过程会慢慢学到 action[1] 无意义」。
7. **代码匹配脆弱**：`find_file` 用 `if name in fname` 子串匹配，`600036` 会同时命中 `sh.600036xxx` 之类的错误文件（代码族相邻时）；`multi_stock_trade` 从 600000 开始遍历会漏掉深市（000xxx/300xxx）代码。

---

## 6 可借鉴的设计要点

1. **把交易抽象成标准 Gym 接口**是教育价值所在：`reset/step/render` 三段式清晰，观测/动作/奖励注释+表格齐全，是理解「RL 如何接交易」的最低门槛样例。
2. **归一化常量的固化做法**：`MAX_SHARE_PRICE=5000`、`MAX_VOLUME=100e8` 等把各量纲统一压到 [0,1]，README 单独辟一章讲归一化的必要性——对新手是很好的示范。
3. **训练/测试窗口切开 + 逐日 render 收集 profit**：虽然简单，但「训练集历史 + 末尾 1 个月测试」的划分和逐日 profit 曲线，是复现 RL 交易实验的最小闭环。
4. **README 的「踩坑叙事」**：从「抄底被套→决定用 RL 验证」的动机写起，配合多只股票的盈亏饼图/直方图，让读者直观看到「大部分票不亏不赚、少数大亏」的真实分布，破除「RL 稳赚」的幻想。

---

## 7 主要源文件索引

| 文件 | 内容 |
|---|---|
| `rlenv/StockTradingEnv0.py` | Gym 交易环境：19 维观测、买卖动作、1/-100 奖励、循环训练 |
| `main.py` | PPO2 训练与推理、单股/多股实验、matplotlib 绘制 |
| `get_stock_data.py` | baostock 取数与 train/test CSV 落盘 |
| `vis.ipynb` | 训练结果统计可视化（盈亏饼图/直方图） |
| `requirements.txt` | 2020 年版本锁（gym/stable-baselines/TF1.15） |
| `README.md` | 原理讲解（观测/动作/奖励/PPO）+ 实验结果 |