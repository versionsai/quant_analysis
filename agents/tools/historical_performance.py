# -*- coding: utf-8 -*-
"""
历史表现查询工具
查询历史信号表现，辅助决策
"""
from typing import Dict, List
from langchain_core.tools import tool

from data.recommend_db import get_db, SignalQualityDB
from utils.logger import get_logger

logger = get_logger(__name__)


@tool
def get_signal_performance(
    signal_source: str = "",
    lookback_days: int = 30,
) -> Dict:
    """
    查询历史信号表现
    
    Args:
        signal_source: 信号来源 (weak_strong/taco/pa_macd/combined)，为空则查全部
        lookback_days: 查询天数
    
    Returns:
        dict: {
            win_rate: float,
            avg_pnl_pct: float,
            total_trades: int,
            best_source: str,
            source_details: dict
        }
    """
    sq_db = SignalQualityDB()
    
    if signal_source:
        perf = sq_db.get_performance_by_source(lookback_days)
        source_data = perf.get(signal_source, {})
        return {
            "win_rate": source_data.get("win_rate", 0),
            "avg_pnl_pct": source_data.get("avg_pnl", 0),
            "total_trades": source_data.get("total", 0),
            "source": signal_source,
        }
    
    perf = sq_db.get_performance_by_source(lookback_days)
    if not perf:
        return {
            "win_rate": 0,
            "avg_pnl_pct": 0,
            "total_trades": 0,
            "best_source": "",
            "source_details": {},
        }
    
    best_source = max(perf.keys(), key=lambda k: perf[k].get("win_rate", 0))
    
    return {
        "win_rate": sum(p.get("wins", 0) for p in perf.values()) / 
                    max(sum(p.get("total", 0) for p in perf.values()), 1) * 100,
        "avg_pnl_pct": sum(p.get("avg_pnl", 0) for p in perf.values()) / max(len(perf), 1),
        "total_trades": sum(p.get("total", 0) for p in perf.values()),
        "best_source": best_source,
        "source_details": perf,
    }


@tool
def get_agent_performance(
    lookback_days: int = 30,
) -> Dict:
    """
    按决策Agent查询胜率
    
    Args:
        lookback_days: 查询天数
    
    Returns:
        dict: {
            agent_performance: dict,
            best_agent: str,
            recommended_weights: dict
        }
    """
    sq_db = SignalQualityDB()
    perf = sq_db.get_performance_by_agent(lookback_days)
    
    if not perf:
        return {
            "agent_performance": {},
            "best_agent": "",
            "recommended_weights": {},
        }
    
    best_agent = max(perf.keys(), key=lambda k: perf[k].get("win_rate", 0))
    
    total_wins = sum(p.get("wins", 0) for p in perf.values())
    total_trades = sum(p.get("total", 0) for p in perf.values())
    
    weights = {}
    for agent, data in perf.items():
        if total_trades > 0:
            win_rate = data.get("win_rate", 0)
            weight = win_rate / 100 * data.get("total", 0) / total_trades
            weights[agent] = round(weight, 3)
    
    return {
        "agent_performance": perf,
        "best_agent": best_agent,
        "recommended_weights": weights,
    }


@tool
def get_recent_trades(
    limit: int = 20,
    only_wins: bool = False,
) -> List[Dict]:
    """
    获取最近的交易记录
    
    Args:
        limit: 返回条数
        only_wins: 只看盈利交易
    
    Returns:
        list: 交易记录列表
    """
    db = get_db()
    trades = db.get_trade_history(days=30)
    
    if only_wins:
        trades = [t for t in trades if float(t.get("pnl", 0) or 0) > 0]
    
    return trades[:limit]


@tool
def get_daily_summary(
    date: str = "",
) -> Dict:
    """
    获取指定日期的交易摘要
    
    Args:
        date: 日期 (YYYY-MM-DD)，为空则查最近一天
    
    Returns:
        dict: {
            date: str,
            total_trades: int,
            win_trades: int,
            win_rate: float,
            total_pnl: float,
            avg_pnl: float
        }
    """
    db = get_db()
    
    if not date:
        from datetime import datetime, timedelta
        date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    trades = db.get_trade_history(days=1)
    date_trades = [t for t in trades if str(t.get("date", "")) == date]
    
    if not date_trades:
        return {
            "date": date,
            "total_trades": 0,
            "win_trades": 0,
            "win_rate": 0,
            "total_pnl": 0,
            "avg_pnl": 0,
        }
    
    win_trades = sum(1 for t in date_trades if float(t.get("pnl", 0) or 0) > 0)
    total_pnl = sum(float(t.get("pnl", 0) or 0) for t in date_trades)
    
    return {
        "date": date,
        "total_trades": len(date_trades),
        "win_trades": win_trades,
        "win_rate": win_trades / len(date_trades) * 100 if date_trades else 0,
        "total_pnl": total_pnl,
        "avg_pnl": total_pnl / len(date_trades),
    }
