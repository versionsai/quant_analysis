# -*- coding: utf-8 -*-
"""
A股量化交易配置
"""

# 数据源配置
DATA_CONFIG = {
    "source": "akshare",
    "cache_dir": "./data/cache",
    "cache_expire_hours": 24,
}

# 回测配置
BACKTEST_CONFIG = {
    "initial_capital": 1000000,  # 初始资金 100万
    "commission_rate": 0.0003,   # 佣金万三
    "stamp_tax": 0.001,          # 印花税千一（卖出）
    "slippage": 0.001,           # 滑点千一
    "min_commission": 5,         # 最低佣金5元
}

# 交易配置
TRADING_CONFIG = {
    "broker": "default",
    "max_position": 0.2,         # 单只股票最大仓位20%
    "max_stocks": 10,            # 最多持仓10只
}

# 策略通用参数
STRATEGY_CONFIG = {
    "rebalance_freq": "daily",   # 调仓频率
    "stop_loss": -0.07,          # 止损线 -7%
    "take_profit": 0.15,         # 止盈线 15%
}
