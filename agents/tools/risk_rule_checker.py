# -*- coding: utf-8 -*-
"""
风控规则检查工具
检查信号是否触发风控规则
"""
from typing import Dict, List, Optional
from langchain_core.tools import tool

from config.config import STRATEGY_CONFIG, TRADING_CONFIG
from data.recommend_db import get_db
from utils.logger import get_logger

logger = get_logger(__name__)


@tool
def check_risk_rules(
    signal_code: str,
    signal_type: str,
    entry_price: float,
    current_price: float,
    market_emotion_score: float = 50.0,
    market_regime: str = "normal",
    holding_days: int = 0,
    position_count: int = 0,
) -> Dict:
    """
    检查信号是否触发风控规则
    
    Args:
        signal_code: 股票代码
        signal_type: 信号类型 (买入/卖出/观望)
        entry_price: 买入价格（持仓时）
        current_price: 当前价格
        market_emotion_score: 大盘情绪评分 (0-100)
        market_regime: 市场模式 (golden_pit/defense/normal)
        holding_days: 持仓天数
        position_count: 当前持仓数量
    
    Returns:
        dict: {
            passed: bool,           # 是否通过风控
            veto_reason: str,       # 否决原因（如果有）
            warnings: list,          # 警告列表
            checks: dict            # 各检查项结果
        }
    """
    checks = {}
    warnings = []
    veto_reason = None
    
    cfg = STRATEGY_CONFIG
    trad_cfg = TRADING_CONFIG
    
    if signal_type != "买入":
        checks["signal_type"] = {"passed": True, "detail": "非买入信号，跳过风控"}
        return {"passed": True, "warnings": [], "checks": checks, "veto_reason": None}
    
    checks["signal_type"] = {"passed": True, "detail": "买入信号，执行风控检查"}
    
    if position_count >= trad_cfg.get("max_stocks", 10):
        veto_reason = f"已达最大持仓数 {trad_cfg.get('max_stocks', 10)}"
        checks["max_positions"] = {"passed": False, "detail": veto_reason}
        return {"passed": False, "warnings": warnings, "checks": checks, "veto_reason": veto_reason}
    checks["max_positions"] = {"passed": True, "detail": f"当前持仓{position_count}/{trad_cfg.get('max_stocks', 10)}"}
    
    if cfg.get("emotion_enabled", True):
        min_emotion = cfg.get("market_emotion_stop_score", 40.0)
        if market_emotion_score < min_emotion:
            override_score = cfg.get("stock_emotion_override_score", 75.0)
            if market_emotion_score < min_emotion - 10:
                veto_reason = f"大盘情绪过低 ({market_emotion_score:.0f} < {min_emotion:.0f})"
                checks["market_emotion"] = {"passed": False, "detail": veto_reason}
            else:
                warnings.append(f"大盘情绪偏低 ({market_emotion_score:.0f})")
                checks["market_emotion"] = {"passed": True, "detail": f"情绪偏低但放行"}
        else:
            checks["market_emotion"] = {"passed": True, "detail": f"情绪正常 ({market_emotion_score:.0f})"}
    
    stop_loss = cfg.get("stop_loss", -0.05)
    if entry_price > 0 and current_price > 0:
        pnl_pct = (current_price - entry_price) / entry_price
        if pnl_pct <= stop_loss:
            veto_reason = f"触发止损 ({pnl_pct*100:.1f}% <= {stop_loss*100:.1f}%)"
            checks["stop_loss"] = {"passed": False, "detail": veto_reason}
        else:
            checks["stop_loss"] = {"passed": True, "detail": f"未触发止损 ({pnl_pct*100:.1f}%)"}
    
    time_stop_days = cfg.get("time_stop_days", 2)
    time_stop_min_return = cfg.get("time_stop_min_return", 0.0)
    if holding_days >= time_stop_days:
        if current_price > 0 and entry_price > 0:
            pnl_pct = (current_price - entry_price) / entry_price
            if pnl_pct <= time_stop_min_return:
                veto_reason = f"时间止损触发 (持仓{holding_days}天不涨)"
                checks["time_stop"] = {"passed": False, "detail": veto_reason}
            else:
                checks["time_stop"] = {"passed": True, "detail": f"持仓{holding_days}天但有盈利"}
        else:
            checks["time_stop"] = {"passed": True, "detail": f"新入场，跳过时间检查"}
    
    max_hold_days = cfg.get("max_hold_days", 3)
    if holding_days >= max_hold_days:
        warnings.append(f"临近最长持仓天数 ({holding_days}/{max_hold_days})")
        checks["max_hold_days"] = {"passed": True, "detail": f"即将超限，提醒关注"}
    
    if market_regime == "normal":
        gate_min = cfg.get("market_gate_min_score", 42.0)
        if market_emotion_score < gate_min:
            veto_reason = f"市场门控未通过 ({market_emotion_score:.0f} < {gate_min:.0f})"
            checks["market_gate"] = {"passed": False, "detail": veto_reason}
        else:
            checks["market_gate"] = {"passed": True, "detail": "市场门控通过"}
    else:
        checks["market_gate"] = {"passed": True, "detail": f"市场模式{market_regime}，门控放宽"}
    
    passed = veto_reason is None
    
    return {
        "passed": passed,
        "veto_reason": veto_reason,
        "warnings": warnings,
        "checks": checks,
    }


@tool
def check_position_risk(code: str) -> Dict:
    """
    检查持仓风险
    
    Args:
        code: 股票代码
    
    Returns:
        dict: {
            has_position: bool,
            risk_level: str (high/medium/low),
            warnings: list,
            actions: list
        }
    """
    db = get_db()
    holdings = db.get_holdings_aggregated()
    
    holding = None
    for h in holdings:
        if str(h.get("code", "")) == str(code):
            holding = h
            break
    
    if not holding:
        return {
            "has_position": False,
            "risk_level": "none",
            "warnings": [],
            "actions": ["无持仓"],
        }
    
    pnl_pct = float(holding.get("total_pnl_pct", 0) or 0)
    highest_price = float(holding.get("highest_price") or 0)
    current_price = float(holding.get("avg_current_price") or 0)
    entry_price = float(holding.get("avg_buy_price") or 0)
    
    warnings = []
    actions = []
    risk_level = "low"
    
    cfg = STRATEGY_CONFIG
    stop_loss = cfg.get("stop_loss", -0.05)
    trailing_stop = cfg.get("trailing_stop", 0.06)
    take_profit = cfg.get("take_profit", 0.15)
    
    if pnl_pct <= stop_loss:
        risk_level = "high"
        warnings.append(f"触发止损线 ({pnl_pct*100:.1f}%)")
        actions.append("建议卖出")
    elif pnl_pct >= take_profit:
        risk_level = "medium"
        warnings.append(f"达到止盈线 ({pnl_pct*100:.1f}%)")
        actions.append("可考虑分批止盈")
    
    if highest_price > 0 and current_price > 0:
        drawdown = (highest_price - current_price) / highest_price
        if drawdown >= trailing_stop:
            risk_level = "high"
            warnings.append(f"触发跟踪止盈 (从高点回撤 {drawdown*100:.1f}%)")
            actions.append("建议卖出")
    
    return {
        "has_position": True,
        "risk_level": risk_level,
        "warnings": warnings,
        "actions": actions,
        "pnl_pct": pnl_pct,
    }
