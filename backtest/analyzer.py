# -*- coding: utf-8 -*-
"""
绩效分析
"""
import pandas as pd
import numpy as np
from typing import Dict, List
from dataclasses import dataclass


@dataclass
class PerformanceMetrics:
    """绩效指标"""
    total_return: float
    annual_return: float
    sharpe_ratio: float
    sortino_ratio: float
    max_drawdown: float
    calmar_ratio: float
    win_rate: float
    profit_loss_ratio: float
    trade_count: int


class PerformanceAnalyzer:
    """绩效分析器"""
    
    @staticmethod
    def analyze(daily_values: pd.DataFrame, trades: List = None) -> PerformanceMetrics:
        """分析绩效"""
        if daily_values.empty:
            return PerformanceMetrics(0, 0, 0, 0, 0, 0, 0, 0, 0)
        
        returns = daily_values["total_value"].pct_change().dropna()
        
        total_return = (daily_values["total_value"].iloc[-1] / daily_values["total_value"].iloc[0]) - 1
        
        days = len(daily_values)
        annual_return = (1 + total_return) ** (252 / max(days, 1)) - 1
        
        sharpe_ratio = returns.mean() / returns.std() * np.sqrt(252) if returns.std() > 0 else 0
        
        downside = returns[returns < 0]
        sortino_ratio = returns.mean() / downside.std() * np.sqrt(252) if len(downside) > 0 and downside.std() > 0 else 0
        
        cummax = daily_values["total_value"].cummax()
        drawdown = (daily_values["total_value"] - cummax) / cummax
        max_drawdown = drawdown.min()
        
        calmar_ratio = abs(annual_return / max_drawdown) if max_drawdown != 0 else 0
        
        return PerformanceMetrics(
            total_return=total_return,
            annual_return=annual_return,
            sharpe_ratio=sharpe_ratio,
            sortino_ratio=sortino_ratio,
            max_drawdown=max_drawdown,
            calmar_ratio=calmar_ratio,
            win_rate=0,
            profit_loss_ratio=0,
            trade_count=len(trades) if trades else 0
        )
    
    @staticmethod
    def generate_report(result) -> str:
        """生成报告"""
        metrics = PerformanceAnalyzer.analyze(result.daily_values, result.trades)
        
        report = f"""
========================================
           回 测 报 告
========================================
总收益率:     {metrics.total_return:>10.2%}
年化收益率:   {metrics.annual_return:>10.2%}
夏普比率:     {metrics.sharpe_ratio:>10.2f}
索提诺比率:   {metrics.sortino_ratio:>10.2f}
最大回撤:     {metrics.max_drawdown:>10.2%}
卡尔玛比率:   {metrics.calmar_ratio:>10.2f}
交易次数:     {metrics.trade_count:>10d}
========================================
"""
        return report
