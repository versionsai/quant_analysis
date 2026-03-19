# -*- coding: utf-8 -*-
"""
盈亏分析器

分析持仓盈亏情况，计算胜率、盈亏比等统计指标
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from strategy.analysis.review.portfolio_tracker import Position, PortfolioTracker
from utils.logger import get_logger

logger = get_logger(__name__)


@dataclass
class PnLRecord:
    """盈亏记录"""
    symbol: str
    name: str
    buy_date: str
    sell_date: str = ""
    
    buy_price: float = 0.0
    buy_shares: int = 0
    sell_price: float = 0.0
    sell_shares: int = 0
    
    cost: float = 0.0
    revenue: float = 0.0
    realized_pnl: float = 0.0
    realized_pnl_pct: float = 0.0
    
    hold_days: int = 0
    max_drawdown: float = 0.0
    max_gain: float = 0.0
    
    exit_reason: str = ""
    notes: str = ""
    
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "name": self.name,
            "buy_date": self.buy_date,
            "sell_date": self.sell_date,
            "buy_price": self.buy_price,
            "buy_shares": self.buy_shares,
            "sell_price": self.sell_price,
            "sell_shares": self.sell_shares,
            "cost": self.cost,
            "revenue": self.revenue,
            "realized_pnl": self.realized_pnl,
            "realized_pnl_pct": self.realized_pnl_pct,
            "hold_days": self.hold_days,
            "exit_reason": self.exit_reason,
            "notes": self.notes,
        }


class PnLAnalyzer:
    """盈亏分析器"""
    
    def __init__(self):
        self.closed_trades: List[PnLRecord] = []
        self.trade_stats: Dict = {}
    
    def analyze_position(
        self,
        position: Position,
        current_date: str = None,
    ) -> Dict:
        """分析单个持仓盈亏"""
        if current_date is None:
            current_date = datetime.now().strftime("%Y%m%d")
        
        result = {
            "symbol": position.symbol,
            "name": position.name,
            "shares": position.shares,
            "avg_cost": position.avg_cost,
            "current_price": position.current_price,
            "current_value": position.current_value,
            "unrealized_pnl": position.unrealized_pnl,
            "unrealized_pnl_pct": position.unrealized_pnl_pct,
            "hold_days": position.hold_days,
        }
        
        if position.buy_history:
            first_buy = position.buy_history[0]
            result["first_buy_date"] = first_buy["date"]
            
            total_cost = sum(b["shares"] * b["price"] for b in position.buy_history)
            total_shares = sum(b["shares"] for b in position.buy_history)
            result["total_cost"] = total_cost
            result["total_shares"] = total_shares
            
            if position.sell_history:
                total_revenue = sum(s["shares"] * s["price"] for s in position.sell_history)
                total_sold = sum(s["shares"] for s in position.sell_history)
                result["total_revenue"] = total_revenue
                result["total_sold"] = total_sold
                
                realized = sum(s.get("realized_pnl", 0) for s in position.sell_history)
                result["realized_pnl"] = realized
        
        return result
    
    def analyze_portfolio(
        self,
        tracker: PortfolioTracker,
        current_date: str = None,
    ) -> Dict:
        """分析整个组合盈亏"""
        if current_date is None:
            current_date = datetime.now().strftime("%Y%m%d")
        
        positions = list(tracker.positions.values())
        
        if not positions:
            return {
                "date": current_date,
                "total_value": tracker.total_value,
                "total_cost": 0,
                "unrealized_pnl": 0,
                "win_rate": 0,
                "positions_count": 0,
            }
        
        total_cost = tracker.total_cost
        unrealized_pnl = tracker.total_unrealized_pnl
        total_return = tracker.total_return
        
        winning_positions = [p for p in positions if p.unrealized_pnl > 0]
        losing_positions = [p for p in positions if p.unrealized_pnl < 0]
        
        avg_win = np.mean([p.unrealized_pnl_pct for p in winning_positions]) if winning_positions else 0
        avg_loss = np.mean([p.unrealized_pnl_pct for p in losing_positions]) if losing_positions else 0
        
        result = {
            "date": current_date,
            "total_value": tracker.total_value,
            "cash": tracker.cash,
            "total_cost": total_cost,
            "unrealized_pnl": unrealized_pnl,
            "total_return": total_return,
            "win_count": len(winning_positions),
            "loss_count": len(losing_positions),
            "positions_count": len(positions),
            "win_rate": len(winning_positions) / len(positions) * 100 if positions else 0,
            "avg_win_pct": avg_win,
            "avg_loss_pct": avg_loss,
            "profit_loss_ratio": abs(avg_win / avg_loss) if avg_loss != 0 else 0,
            "largest_win": max((p.unrealized_pnl_pct for p in positions), default=0),
            "largest_loss": min((p.unrealized_pnl_pct for p in positions), default=0),
        }
        
        return result
    
    def analyze_trade_history(
        self,
        tracker: PortfolioTracker,
    ) -> Dict:
        """分析历史交易统计"""
        if not tracker.trade_history:
            return {"total_trades": 0, "realized_pnl": 0}
        
        df = pd.DataFrame(tracker.trade_history)
        
        sell_trades = df[df["action"] == "卖出"]
        
        realized_pnl = 0
        if "realized_pnl" in sell_trades.columns:
            realized_pnl = sell_trades["realized_pnl"].sum()
        
        buy_trades = df[df["action"] == "买入"]
        unique_symbols = df["symbol"].nunique()
        
        return {
            "total_trades": len(df),
            "buy_trades": len(buy_trades),
            "sell_trades": len(sell_trades),
            "unique_symbols": unique_symbols,
            "realized_pnl": realized_pnl,
            "avg_holding_days": self._calc_avg_holding_days(sell_trades),
        }
    
    def _calc_avg_holding_days(self, sell_trades: pd.DataFrame) -> float:
        """计算平均持仓天数"""
        if sell_trades.empty:
            return 0
        
        holding_days = []
        for _, trade in sell_trades.iterrows():
            if "hold_days" in trade:
                holding_days.append(trade["hold_days"])
        
        return np.mean(holding_days) if holding_days else 0
    
    def calculate_drawdown(
        self,
        prices: List[float],
    ) -> Tuple[float, int, int]:
        """计算最大回撤"""
        if not prices:
            return 0, 0, 0
        
        peak = prices[0]
        max_drawdown = 0
        peak_idx = 0
        drawdown_idx = 0
        
        for i, price in enumerate(prices):
            if price > peak:
                peak = price
                peak_idx = i
            
            drawdown = (peak - price) / peak * 100 if peak > 0 else 0
            if drawdown > max_drawdown:
                max_drawdown = drawdown
                drawdown_idx = i
        
        return max_drawdown, peak_idx, drawdown_idx
    
    def generate_analysis_report(
        self,
        tracker: PortfolioTracker,
        current_date: str = None,
    ) -> str:
        """生成盈亏分析报告"""
        if current_date is None:
            current_date = datetime.now().strftime("%Y%m%d")
        
        portfolio_stats = self.analyze_portfolio(tracker, current_date)
        trade_stats = self.analyze_trade_history(tracker)
        
        lines = [
            f"【盈亏分析报告 {current_date}】",
            f"",
            f"--- 持仓概况 ---",
            f"总资产: {portfolio_stats['total_value']:,.2f}",
            f"持仓市值: {portfolio_stats['total_value'] - portfolio_stats['cash']:,.2f}",
            f"现金: {portfolio_stats['cash']:,.2f}",
            f"浮盈亏: {portfolio_stats['unrealized_pnl']:,.2f} ({portfolio_stats['total_return']:.2f}%)",
            f"",
            f"--- 盈亏分布 ---",
            f"持仓数: {portfolio_stats['positions_count']}",
            f"盈利持仓: {portfolio_stats['win_count']} ({portfolio_stats['win_rate']:.1f}%)",
            f"亏损持仓: {portfolio_stats['loss_count']}",
            f"平均盈利: {portfolio_stats['avg_win_pct']:.2f}%",
            f"平均亏损: {portfolio_stats['avg_loss_pct']:.2f}%",
            f"盈亏比: {portfolio_stats['profit_loss_ratio']:.2f}",
            f"最大盈利: {portfolio_stats['largest_win']:.2f}%",
            f"最大亏损: {portfolio_stats['largest_loss']:.2f}%",
            f"",
            f"--- 交易统计 ---",
            f"总交易次数: {trade_stats['total_trades']}",
            f"买入次数: {trade_stats['buy_trades']}",
            f"卖出次数: {trade_stats['sell_trades']}",
            f"已实现盈亏: {trade_stats['realized_pnl']:,.2f}",
            f"平均持仓: {trade_stats['avg_holding_days']:.1f}天",
        ]
        
        positions = list(tracker.positions.values())
        if positions:
            lines.append("")
            lines.append("--- 持仓明细 ---")
            for pos in sorted(positions, key=lambda x: -x.unrealized_pnl):
                lines.append(
                    f"{pos.symbol} {pos.name}: "
                    f"{pos.unrealized_pnl:+.2f}({pos.unrealized_pnl_pct:+.2f}%) "
                    f"持仓{pos.hold_days}天"
                )
        
        return "\n".join(lines)
