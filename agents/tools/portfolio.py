# -*- coding: utf-8 -*-
"""
持仓分析工具
分析当前持仓状态和盈亏情况
"""
from datetime import datetime
from typing import Dict
from langchain_core.tools import tool

from agents.tools.stock_analysis import get_stock_fundamental_summary
from data.recommend_db import get_db
from trading.report_formatter import HoldingReportRow, ReviewTradeRow, format_holdings_section, format_review_section
from utils.logger import get_logger

logger = get_logger(__name__)


@tool
def analyze_portfolio() -> str:
    """
    分析当前持仓情况，包括持仓明细、盈亏状态和风险评估。

    Returns:
        str: 持仓分析报告，包含持仓列表、盈亏情况和风险提示
    """
    try:
        db = get_db()
        holdings = db.get_holdings_aggregated()
        stats = db.get_statistics()
        raw_trades = db.get_trade_history(days=5)
        trades = [
            ReviewTradeRow(
                date=str(t.get("date", "")),
                code=str(t.get("code", "")),
                direction=str(t.get("direction", "")),
                price=float(t.get("price", 0) or 0.0),
                pnl=float(t.get("pnl", 0) or 0.0),
            )
            for t in raw_trades
        ]

        result = f"【持仓分析报告】\n时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"

        if not holdings:
            result += format_holdings_section([])
            result += "\n\n"
            result += format_review_section(stats=stats, trades=trades, proxy_diff_rows=None)
            return result

        total_value = 0
        total_cost = 0
        holding_rows = []

        for h in holdings:
            code = h.get("code", "")
            name = h.get("name", "")
            avg_buy_price = h.get("avg_buy_price", 0)
            avg_current_price = h.get("avg_current_price", avg_buy_price) or avg_buy_price
            total_quantity = h.get("total_quantity", 0)
            total_pnl = h.get("total_pnl", 0) or 0
            total_pnl_pct = h.get("total_pnl_pct", 0) or 0

            cost = avg_buy_price * total_quantity
            value = avg_current_price * total_quantity
            total_cost += cost
            total_value += value

            pnl_str = f"+{total_pnl:.2f}" if total_pnl >= 0 else f"{total_pnl:.2f}"
            pnl_pct_str = f"+{total_pnl_pct:.2f}%" if total_pnl_pct >= 0 else f"{total_pnl_pct:.2f}%"

            holding_rows.append(
                HoldingReportRow(
                    code=code,
                    name=name,
                    latest_price=float(avg_current_price or 0.0),
                    pnl_pct=float(total_pnl_pct or 0.0),
                    target_price=float(h.get("target_price") or 0.0),
                    stop_loss=float(h.get("stop_loss") or 0.0),
                    factor_text=f"仓位: {int(total_quantity)}股 | 盈亏额 {pnl_str}元 ({pnl_pct_str})",
                    fundamental_text=get_stock_fundamental_summary(code),
                    tech_text=f"成本/现价: {float(avg_buy_price or 0.0):.2f}/{float(avg_current_price or 0.0):.2f}",
                    fund_text=f"持仓市值: {float(value):.2f}元",
                    emotion_text=f"持仓周期: {h.get('first_buy_date', '-')} -> {h.get('last_buy_date', '-')}",
                )
            )

        total_pnl = total_value - total_cost
        total_pnl_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0

        result += format_holdings_section(holding_rows)
        result += "\n\n"
        result += "【持仓汇总】\n"
        result += f"总市值: {total_value:.2f}元\n"
        result += f"总成本: {total_cost:.2f}元\n"
        result += f"总盈亏: {total_pnl:+.2f}元 ({total_pnl_pct:+.2f}%)\n\n"
        result += format_review_section(stats=stats, trades=trades, proxy_diff_rows=None)

        if len(holdings) >= 3:
            result += "\n\n【风险提示】\n持仓已满(3只)，建议暂不新增买入\n"

        return result

    except Exception as e:
        logger.error(f"持仓分析失败: {e}")
        return f"持仓分析失败: {str(e)}"
