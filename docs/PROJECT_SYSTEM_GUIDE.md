# 当前项目完整说明

本文档面向后续策略优化和系统维护，描述当前项目的核心模块、运行方式、选股逻辑、择时逻辑、AI Agent 作用，以及新增短线情绪交易模块的接入方式。

## 1. 项目定位

当前项目不是单纯的回测仓库，而是一套 A 股短线交易与监控系统，主要覆盖：

- 股票池 / ETF 池管理
- 盘中实时扫描与信号池
- 持仓管理与复盘
- 看板展示
- TACO / TACO-OIL 等热点驱动策略
- AI Agent 辅助决策
- 回测、调优和实验

核心入口：

- `./main.py`
- `./docker_start.py`
- `./dashboard.py`
- `./trading/realtime_monitor.py`

## 2. 新增短线情绪系统接入结果

本轮新增的模块没有独立悬空，而是已经接入现有策略链路。

新增模块位置：

- 情绪模块：`strategy/analysis/emotion/`
- 龙头模型：`strategy/alpha/leader_model.py`
- Top 风险模型：`strategy/ml/top_model.py`
- 情绪回测策略：`backtest/strategy.py`
- 情绪回测引擎：`backtest/engine.py`

新增接入层：

- `./strategy/analysis/emotion/emotion_ensemble.py`

这个组合分析器统一输出两类数据：

1. 市场级上下文 `EmotionMarketContext`
- 市场周期
- 空间分 `space_score`
- 过热度 `overheat`
- 主线板块
- 建议仓位 `recommended_exposure`

2. 个股级画像 `EmotionStockProfile`
- 个股情绪分
- 概念强度
- 龙头分与龙头排名
- Top 风险概率 `top_prob`
- 组合情绪分 `composite_score`

这些画像已经接入 `./trading/realtime_monitor.py`，直接影响 A 股信号放行、排序和理由生成。

## 3. 当前运行入口

### 3.1 常用 CLI

```bash
python3 main.py --help
python3 main.py --mode realtime --once
python3 main.py --mode review
python3 main.py --mode emotion-scan
python3 main.py --mode backtest --strategy taco
python3 main.py --mode taco-monitor --strategy taco
python3 dashboard.py
python3 docker_start.py
```

### 3.2 主要 mode

- `pool`：查看池子
- `pool-update`：更新股票池
- `realtime`：盘中实时扫描
- `review`：生成复盘
- `emotion-scan`：查看情绪扫描
- `backtest`：单策略回测
- `compare`：策略对比
- `tune-review` / `tune-experiments`：调优建议与实验
- `taco-monitor` / `taco-compare`：TACO 系列扫描与比较

## 4. 股票池、信号池、盘中盘后更新逻辑

### 4.1 股票池

股票池真表是 `stock_pool`。

刷新逻辑在：

- `./docker_start.py`
- `./data/stock_pool_generator.py`

当前交易日刷新时间点：

- `08:30`
- `09:25`
- `09:30`
- `09:40`
- `09:50`
- `10:00`
- `10:15`
- `10:30`
- `13:00`
- `13:15`
- `13:30`
- `14:00`
- `14:15`
- `14:30`
- `14:50`
- `14:57`

特殊逻辑：

- `08:30` 批次标记为 `pre_market_us_news`
- 这一轮会结合前夜美股表现和新闻上下文，优先构建盘前候选池

### 4.2 信号池

信号池真表是 `signal_pool`。

当前逻辑不是单独定时刷新，而是：

1. 股票池刷新
2. 回调实时扫描
3. 扫描结果写入信号池

所以现在信号池从属于股票池刷新事件，不再是独立轮询任务。

### 4.3 盘中和盘后

盘中：

- 以动态股票池为候选
- 调用 `RealtimeMonitor`
- 生成买入 / 卖出 / 观望信号
- 更新信号池

盘后：

- 主要做新闻收集、AI 判断、复盘和调优
- 不再额外维护一套完全独立的盘后信号池

## 5. 当前选股逻辑

当前选股已经不是单一技术信号，而是多层过滤：

### 5.1 第一层：基础技术信号

在 `./trading/realtime_monitor.py` 中，先生成基础信号：

- `PriceAction + MACD`
- `WeakToStrongTimingStrategy`

如果两个同时为正，会被标记为双重信号。

### 5.2 第二层：市场门控

基础信号不是直接买入，还要经过市场门控：

- 大盘情绪
- 空间强度
- 指数平均涨跌
- 最弱指数表现
- ETF 类型分流
- 弱市防守模式豁免

### 5.3 第三层：增强情绪画像

这是本轮新增的核心增强。

对于 A 股候选，系统现在会额外计算：

- `space_score`
  公式：`0.4 * cycle + 0.3 * sector + 0.3 * intraday`
- `overheat`
  组成：连板高度、涨停拥挤度、成交量异常、一致性、板块集中度
- `leader_score`
  龙头识别分
- `top_prob`
  未来 1~3 天出现较大回撤的风险概率
- `composite_score`
  组合情绪分

这些指标会影响最终买入决策：

- `leader_score` 高，会给买入信号加分
- `top_prob` 高，会抑制买入甚至直接打回观望
- `overheat` 过高且不是核心龙头，会直接限制追涨
- `recommended_exposure` 过低时，普通个股信号会被主动压分

### 5.4 ETF / A 股差异

- TACO / TACO-OIL 仍然是 ETF/LOF 优先
- 普通短线扫描对 A 股使用增强情绪画像更重
- 弱市下 ETF 会按宽基 / 主题 / 防御 / 海外映射分别处理

## 6. 当前择时逻辑

择时主要分成三个层面：

### 6.1 入场择时

入场由技术信号触发：

- `PA + MACD`
- `WeakToStrong`

然后叠加：

- 市场门控
- 运行模式过滤
- 龙头 / Top 风险过滤

### 6.2 市场模式择时

运行模式有三种：

- `normal`
- `defense`
- `golden_pit`

当前不是纯规则，也不是纯 AI，而是：

- 先用规则根据指数、情绪、空间推导候选模式
- 再由 AI 判断是否覆盖
- AI 置信度足够高时，可覆盖规则结果

### 6.3 持仓择时

持仓卖出和管理主要依赖：

- 止损 / 止盈
- 跟踪止盈
- 时间止损
- 封板强度 / 炸板风险
- FCF 资金一致性
- 市场情绪退潮
- AI 持仓执行决策

## 7. AIAgent 的作用

AI Agent 不是替代量化，而是叠加在量化链路上做二次判断。

核心实现位置：

- `./agents/quant_agent.py`

当前 AI 的作用主要有四类：

### 7.1 市场模式判断

AI 会基于：

- 指数涨跌
- 情绪分
- 空间分
- 当前规则模式

输出：

- `mode`
- `reason`
- `confidence`

### 7.2 候选信号二次审核

AI 可以对候选股票做：

- `buy`
- `watch`
- `skip`

这一步更适合后续继续加大使用，目前项目里已经具备接口，但还可以进一步放到更多盘中信号上。

### 7.3 买入执行决策

AI 会综合：

- 当日信号
- 当前持仓
- 市场情绪

决定：

- 买哪些
- 哪些跳过
- 是否允许加仓

### 7.4 持仓执行决策

AI 会综合持仓信号和市场环境，给出：

- 卖出
- 减仓
- 加仓
- 继续持有

### 7.5 新闻与调优辅助

AI 还用于：

- 新闻摘要和新闻判断
- 热点聚类摘要
- 策略调优分析和实验建议

## 8. 各策略当前定位

### 8.1 PA + MACD

定位：

- 基础趋势择时
- 泛用型技术触发器

优点：

- 直观
- 低成本
- 容易解释

缺点：

- 对题材接力和情绪峰值不敏感

### 8.2 WeakToStrong

定位：

- 短线接力与分歧转一致

优点：

- 对强势股二次启动更敏感

缺点：

- 弱市容易被假修复骗线

### 8.3 TACO / TACO-OIL

定位：

- 宏观热点 / 事件修复 / 主题轮动

特点：

- ETF/LOF 优先
- 跟踪特朗普、关税、油气、芯片、AI、防务等热点

### 8.4 短线情绪增强层

定位：

- 不是独立替代现有策略
- 而是作为“二次因子层”增强现有策略

作用：

- 提升情绪识别
- 抑制高位接力回撤
- 识别核心龙头
- 提高 A 股信号质量

## 9. 本轮策略优化结果

基于当前接入，系统比原先多了这些优化：

### 9.1 情绪指标更完整

原先偏重：

- 市场情绪
- 空间分
- 个股情绪

现在新增：

- 盘中承接
- 过热度
- 龙头识别
- Top 风险概率
- 建议仓位

### 9.2 选股排序更合理

原先更多按基础信号和分数排序。

现在 A 股会进一步按：

- 综合信号分
- 组合情绪分
- 龙头分
- Top 风险

进行排序。

### 9.3 追高风险抑制更强

现在高位追涨会多一道拦截：

- 如果 Top 风险很高且不是核心龙头，直接降为观望
- 如果市场过热且不是核心龙头，也会被压制

### 9.4 龙头溢价被显式建模

过去“主线龙头”更多体现在人读理由。

现在已经有结构化字段：

- `leader_score`
- `leader_rank`
- `is_core_leader`

这会直接影响选股和后续看板展示扩展。

## 10. 回测系统如何使用

新回测模块包括：

- `./backtest/strategy.py`
- `./backtest/engine.py`

### 10.1 策略规则

默认规则：

- BUY: `space_score > 0.6 and top_prob < 0.4`
- SELL: `top_prob > 0.6`

### 10.2 回测输入

需要两类数据：

- `price_data`
- `feature_rows`

`feature_rows` 至少包含：

- `trade_date`
- `symbol`
- `space_score`
- `overheat`
- `acc`
- `zt_diff`
- `eff_diff`
- `leader_ret`

### 10.3 当前状态

这套回测层已经可本地运行，但还没有完全挂到现有 `main.py --mode backtest` 主路径里。后续如果要进一步产品化，建议：

1. 增加新的 strategy 名称
2. 增加特征构造器
3. 把回测输入自动从历史行情和历史情绪代理中生成

## 11. 后续建议优化方向

### 11.1 优先做的

- 将增强情绪画像接入看板 API，直接展示 `leader_score / top_prob / overheat`
- 把 AI 的 `review_signal_with_regime()` 接到盘中候选审核
- 给 Top 模型增加真实历史样本训练，不再只依赖冷启动先验模型

### 11.2 中期建议

- 对不同策略分开建模，而不是共用一套 Top 风险
- 将特朗普、关税、出口管制、联储、地缘冲突等事件映射为独立因子
- 对 ETF 和 A 股分别建立不同的情绪画像逻辑

### 11.3 长期建议

- 建立统一特征仓库
- 用复盘结果反哺 Top 风险训练集
- 让 AI 不只做解释，也参与“是否该调参”的闭环决策

## 12. 自动运行与人工触发清单

### 12.1 部署后默认自动运行

前提：

- `quant-stock-bot` 容器正常启动
- `quant-stock-dashboard` 容器正常启动
- 两个容器都配置了 `restart: unless-stopped`

默认自动运行的能力：

- 交易日股票池多时间点刷新
- 股票池刷新后自动回调信号池刷新
- 信号扫描、推荐写库、自动买入
- 看板行情缓存刷新
- 看板择时参数试验缓存刷新
- 新闻报告、盘中诱多/诱空推送、外围简报
- 15:30 每日自动优化

### 12.2 仍然属于人工触发

- 看板顶部手动按钮
- `python3 main.py --mode backtest`
- `python3 main.py --mode compare`
- `python3 main.py --mode taco-compare`
- `python3 main.py --mode tune-review`
- `python3 main.py --mode tune-experiments`
- 任何临时诊断、一次性复盘和参数试验命令

### 12.3 上线后建议核对

- `quant-stock-bot` 是否常驻
- `quant-stock-dashboard` 是否常驻
- 股票池是否在交易日自动更新时间点落库
- 信号池是否跟随股票池刷新，而不是独立乱跳
- 看板 `/api/health` 是否正常
- `15:30` 自动优化结果是否写入数据库

## 13. 关键文件总览

- 统一入口：`./main.py`
- 调度中心：`./docker_start.py`
- 看板后端：`./dashboard.py`
- 看板前端：`./dashboard/index.html`
- 实时扫描：`./trading/realtime_monitor.py`
- 持仓/推荐写库：`./trading/recommend_recorder.py`
- 数据源：`./data/data_source.py`
- 情绪组合分析：`./strategy/analysis/emotion/emotion_ensemble.py`
- 龙头模型：`./strategy/alpha/leader_model.py`
- Top 模型：`./strategy/ml/top_model.py`
- 回测策略：`./backtest/strategy.py`
- 回测引擎：`./backtest/engine.py`
