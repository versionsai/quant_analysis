# 项目上下文信息

最后更新: 2026-03-17

## 项目概述

A股量化交易系统，专注于ETF/LOF产品的量化投资策略。

## 项目结构

```
sai/
├── backtest/           # 回测引擎
│   ├── engine.py        # BacktestEngine, SelectorBacktestEngine
│   └── analyzer.py      # 绩效分析
├── config/              # 配置文件
├── context/             # 项目上下文
├── data/                # 数据源和股票池
│   ├── data_source.py   # 原始akshare数据源
│   ├── data_source_v2.py   # 多数据源适配器 [新增]
│   │                     # 优先级: Baostock -> Tushare Pro -> akshare
│   └── stock_pool.py    # StockPool
├── strategy/            # 策略模块
│   ├── base.py          # 择时策略基类
│   ├── examples/        # 择时策略实现
│   └── selectors/       # 选股策略
├── trading/             # 交易接口
├── utils/              # 工具类
├── main.py             # 主程序
└── requirements.txt     # 依赖
```

## 策略分类

### 选股策略 (Selectors)
从股票池中选择优质股票

| 策略 | 类名 | 功能 |
|------|------|------|
| 动量选股 | MomentumSelector | 基于N日涨幅排序 |
| 双动量选股 | DualMomentumSelector | 短期+长期动量 |
| 多因子选股 | FactorSelector | 动量+波动+成交量+趋势 |
| 质量选股 | QualitySelector | 盈利能力+稳定性 |
| 综合选股 | CompositeSelector | 动量40%+质量30%+趋势30% |

### 择时策略 (Timing)
决定买入卖出时机

| 策略 | 类名 | 功能 |
|------|------|------|
| PriceAction | PriceActionStrategy | 价格行为（突破、Pin Bar） |
| MACD | MACDStrategy | MACD金叉死叉、背离 |
| PA+MACD | PriceActionMACDStrategy | 组合策略 |
| Breakout | BreakoutStrategy | 区间突破 |

## 运行模式

```bash
# 获取ETF/LOF股票池
python main.py --mode pool

# 纯择时回测
python main.py --mode backtest

# 策略对比
python main.py --mode compare

# 选股测试
python main.py --mode select

# 选股+择时组合回测
python main.py --mode select_backtest
```

## 当前状态

### 已完成
- [x] 策略分类：选股 vs 择时
- [x] 选股策略实现 (Momentum, DualMomentum, Factor, Composite)
- [x] SelectorBacktestEngine 组合回测引擎
- [x] main.py 选股功能
- [x] data_source.py 添加重试机制 + 默认ETF/LOF列表

### 待完成
- [ ] 优化选股因子参数
- [ ] 添加更多选股策略（如价值选股）
- [ ] 完善回测绩效分析
- [ ] 添加实盘交易接口

## 已知问题

### akshare 网络连接问题
东方财富接口不稳定，经常报 `RemoteDisconnected` 错误。

**解决方案**:
1. ✅ 已集成 `akshare-proxy-patch` 代理补丁（推荐方案）
2. ✅ 已添加重试机制
3. ✅ 已添加默认ETF/LOF列表

### 数据源状态
| 数据源 | 状态 |
|--------|------|
| akshare + proxy patch | ⚠️ 需测试 |
| Tushare Pro | ⚠️ 需权限 |
| Baostock | ⚠️ 不支持ETF |
| 默认列表 | ✅ 可用 |

### akshare-proxy-patch 使用说明
安装：`pip install akshare-proxy-patch==0.2.13`

代码已自动集成代理补丁，初始化DataSource时自动启用。

## 常用ETF/LOF代码

| 代码 | 名称 |
|------|------|
| 511880 | 银华日利 |
| 511010 | 易方达上证50ETF |
| 510300 | 华夏沪深300ETF |
| 510500 | 南方中证500ETF |
| 512880 | 证券ETF |
| 513050 | 中概互联网ETF |
| 513100 | 纳指ETF |
| 159919 | 券商ETF |
| 515790 | 光伏ETF |

## 技术栈

- Python 3.14
- pandas, numpy
- akshare (数据源)
- scikit-learn, scipy
- plotly (可视化)

## 注意事项

1. akshare 需要网络访问，国内数据可能不稳定（已添加重试机制）
2. 选股结果仅供参考，实际交易需谨慎
3. 回测结果不代表未来收益
