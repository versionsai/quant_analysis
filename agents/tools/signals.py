# -*- coding: utf-8 -*-
"""
量化信号工具
获取量化策略产生的交易信号
"""
from datetime import datetime
from langchain_core.tools import tool

from trading.realtime_monitor import RealtimeMonitor
from utils.logger import get_logger

logger = get_logger(__name__)


@tool
def check_quant_signals() -> str:
    """
    获取量化策略产生的最新交易信号，包括ETF和A股的买入/卖出/观望信号。

    Returns:
        str: 量化信号报告，包含所有扫描股票的信号详情
    """
    try:
        logger.info("开始获取量化信号...")

        monitor = RealtimeMonitor(etf_count=5, stock_count=5)
        results = monitor.scan_market()

        result = "【量化信号报告】\n\n"
        result += f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"

        etf_signals = results.get("etf", [])
        stock_signals = results.get("stock", [])

        buy_etf = [s for s in etf_signals if s.signal_type == "买入"]
        buy_stock = [s for s in stock_signals if s.signal_type == "买入"]

        result += "【ETF/LOF 信号】\n"
        if etf_signals:
            for s in etf_signals[:5]:
                signal_emoji = "📈" if s.signal_type == "买入" else ("📉" if s.signal_type == "卖出" else "⏸️")
                result += f"{signal_emoji} {s.code} {s.name}\n"
                result += f"   价格: {s.price:.2f} 涨跌幅: {s.change_pct:+.2f}%\n"
                result += f"   信号: {s.signal_type}\n"
                if s.target_price and s.stop_loss:
                    profit_pct = (s.target_price / s.price - 1) * 100
                    loss_pct = (s.stop_loss / s.price - 1) * 100
                    result += f"   目标: {s.target_price:.2f}(+{profit_pct:.1f}%) "
                    result += f"止损: {s.stop_loss:.2f}({loss_pct:.1f}%)\n"
                result += f"   理由: {s.reason}\n\n"
        else:
            result += "  暂无数据\n"
            result += "\n"

        result += "【A股信号】\n"
        if stock_signals:
            for s in stock_signals[:5]:
                signal_emoji = "📈" if s.signal_type == "买入" else ("📉" if s.signal_type == "卖出" else "⏸️")
                result += f"{signal_emoji} {s.code} {s.name}\n"
                result += f"   价格: {s.price:.2f} 涨跌幅: {s.change_pct:+.2f}%\n"
                result += f"   信号: {s.signal_type}\n"
                if s.target_price and s.stop_loss:
                    profit_pct = (s.target_price / s.price - 1) * 100
                    loss_pct = (s.stop_loss / s.price - 1) * 100
                    result += f"   目标: {s.target_price:.2f}(+{profit_pct:.1f}%) "
                    result += f"止损: {s.stop_loss:.2f}({loss_pct:.1f}%)\n"
                result += f"   理由: {s.reason}\n\n"
        else:
            result += "  暂无数据\n"
            result += "\n"

        result += "【信号汇总】\n"
        result += f"  ETF买入信号: {len(buy_etf)}只\n"
        result += f"  A股买入信号: {len(buy_stock)}只\n"

        if buy_etf or buy_stock:
            result += "\n【建议操作】\n"
            if buy_etf:
                result += "ETF推荐:\n"
                for s in buy_etf[:2]:
                    result += f"  ✅ {s.code} {s.name} @ {s.price:.2f}\n"
            if buy_stock:
                result += "A股推荐:\n"
                for s in buy_stock[:2]:
                    result += f"  ✅ {s.code} {s.name} @ {s.price:.2f}\n"
        else:
            result += "\n【建议操作】: 暂无买入信号，建议观望\n"

        return result

    except Exception as e:
        logger.error(f"获取量化信号失败: {e}")
        return f"获取量化信号失败: {str(e)}"
