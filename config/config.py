# -*- coding: utf-8 -*-
"""
A股量化交易配置
"""

# 数据源配置
DATA_CONFIG = {
    "source": "akshare",
    "cache_dir": "./runtime/data/cache",
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
    "stop_loss": -0.05,          # 止损线 -5% (短线更贴合情绪流)
    "take_profit": 0.15,         # 止盈线 15%
    "trailing_stop": 0.06,       # 跟踪止盈：从最高点回撤 6% 卖出
    "max_hold_days": 3,          # 最长持仓 3 个交易日
    "time_stop_days": 2,         # 时间止损：持仓满 2 天仍不涨则卖
    "time_stop_min_return": 0.0, # 时间止损触发阈值：涨幅<=0 视为“不涨”
    "min_hold_days_before_sell": 1,  # A股T+1：买入当日不卖（回测/风控均遵守）

    # 情绪风控（全市场辅助 + 个股优先）
    "emotion_enabled": True,             # 是否启用情绪退出
    "market_emotion_stop_score": 40.0,   # 大盘情绪分（0-100）低于该值，触发“退潮”风控
    "stock_emotion_override_score": 75.0,# 个股强势/抱团分（0-100）高于该值，可在弱市中“坚定持有”（跳过情绪/时间退出）
    "concept_override_score": 0.70,      # 所属概念强度（0-1）高于该值，认定为主线抱团

    # 抱团股增强：更宽回撤 + 分批止盈
    "override_trailing_stop": 0.08,      # 抱团股跟踪止盈：从最高点回撤 8%
    "scale_out_enabled": True,           # 是否启用分批止盈
    "scale_out_levels": [0.10, 0.20],    # 分批止盈阈值（收益率）
    "scale_out_ratios": [0.50, 1.00],    # 每档卖出比例（第2档通常为清仓）

    # 结构止损：跌破买入日低点（不及预期）
    "entry_low_stop_enabled": True,
    "entry_low_stop_buffer": 0.0,        # 缓冲（如 0.001 表示低点下方 0.1% 才触发）

    # 涨停封板强度/炸板风险止盈（实盘/模拟：使用 ak.stock_zt_pool_em 的 封板资金/炸板次数）
    "limit_up_seal_exit_enabled": True,
    "seal_ratio_sell_all": 0.10,         # 封板资金/成交额 < 10%：清仓
    "seal_ratio_sell_half": 0.20,        # 封板资金/成交额 < 20%：卖一半
    "break_count_sell_all": 3,           # 炸板次数 >= 3：清仓
    "break_count_sell_half": 1,          # 炸板次数 >= 1 且封板偏弱：卖一半（见代码兜底条件）

    # 回测近似（无封板资金/炸板）：用收盘强度 close_strength 代替
    "limit_up_close_strength_sell_all": 0.10,
    "limit_up_close_strength_sell_half": 0.20,

    # 资金一致性因子（FCF）
    "fcf_enabled": True,
    "fcf_buy_threshold": 0.0,     # 买入过滤：FCF > 0 才允许
    "fcf_sell_threshold": 0.0,    # 卖出：FCF < 0 触发
    "fcf_down_days": 2,           # 卖出：FCF 连续下降天数
    "fcf_death_turnover": 50.0,   # “死亡换手率”阈值（%）

    # 盘中诱多/诱空（上证 + 中证500 + 中证1000）
    "intraday_trap_enabled": True,
    "intraday_trap_threshold": 0.65,
    "intraday_trap_spread": 0.12,
    "intraday_trap_weight_sh": 0.30,
    "intraday_trap_weight_csi500": 0.35,
    "intraday_trap_weight_csi1000": 0.35,
}
