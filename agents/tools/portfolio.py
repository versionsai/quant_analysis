# -*- coding: utf-8 -*-
"""
持仓分析工具
分析当前持仓状态和盈亏情况
"""
from datetime import datetime
from typing import Dict
from langchain_core.tools import tool

from data.recommend_db import get_db
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

        result = "【持仓分析报告】\n\n"
        result += f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"

        if not holdings:
            result += "【当前状态】: 空仓\n\n"
            result += "【历史统计】\n"
            result += f"  总交易次数: {stats['total_trades']}\n"
            result += f"  胜率: {stats['win_rate']:.1f}%\n"
            result += f"  总收益: {stats['total_pnl']:.2f}元\n"
            return result

        result += f"【持仓明细】({len(holdings)}只)\n"
        result += "-" * 50 + "\n"

        total_value = 0
        total_cost = 0

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

            result += f"• {code} {name}\n"
            result += f"  均价: {avg_buy_price:.2f} | 现价: {avg_current_price:.2f}\n"
            result += f"  数量: {total_quantity} | 市值: {value:.2f}元\n"
            result += f"  盈亏: {pnl_str}元 ({pnl_pct_str})\n\n"

        total_pnl = total_value - total_cost
        total_pnl_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0

        result += "-" * 50 + "\n"
        result += f"【汇总】\n"
        result += f"  总市值: {total_value:.2f}元\n"
        result += f"  总成本: {total_cost:.2f}元\n"
        result += f"  总盈亏: {total_pnl:+.2f}元 ({total_pnl_pct:+.2f}%)\n\n"

        result += f"【历史统计】\n"
        result += f"  总交易次数: {stats['total_trades']}\n"
        result += f"  盈利次数: {stats['win_trades']}\n"
        result += f"  亏损次数: {stats['loss_trades']}\n"
        result += f"  胜率: {stats['win_rate']:.1f}%\n"
        result += f"  累计收益: {stats['total_pnl']:.2f}元\n"

        if len(holdings) >= 3:
            result += "\n【风险提示】: 持仓已满(3只)，建议暂不新增买入\n"

        return result

    except Exception as e:
        logger.error(f"持仓分析失败: {e}")
        return f"持仓分析失败: {str(e)}"
