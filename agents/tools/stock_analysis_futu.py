# -*- coding: utf-8 -*-
"""
股票分析工具 - 富途版
使用富途 API 获取实时行情和技术分析
"""
from datetime import datetime
from typing import Optional, Dict, List
from langchain_core.tools import tool

import pandas as pd
import numpy as np

from utils.logger import get_logger

logger = get_logger(__name__)


@tool
def analyze_stock_futu(symbol: str) -> str:
    """
    使用富途API深度分析指定股票。

    Args:
        symbol: 股票代码，如 "600036"

    Returns:
        str: 股票综合分析报告
    """
    try:
        from data.futu_source import get_futu_source

        futu = get_futu_source()
        if not futu.is_connected():
            return "富途数据源未连接"

        result = f"【股票分析】{symbol}\n\n"
        result += f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"

        code = futu._normalize_code(symbol)
        quotes = futu.get_batch_quote([symbol])
        if quotes:
            q = quotes[0]
            price = q.get("price", 0)
            prev = q.get("prev_close", price)
            change_pct = ((price - prev) / prev * 100) if prev > 0 else 0

            result += "【行情】\n"
            result += f"  现价: {price:.3f}\n"
            result += f"  昨收: {prev:.3f}\n"
            result += f"  涨跌: {change_pct:+.2f}%\n"
            result += f"  成交量: {q.get('volume', 0):,}\n\n"

        kline = futu.get_kline(symbol, count=60)
        if kline is not None and not kline.empty:
            close = kline["close"].values

            ma5 = close[-5:].mean() if len(close) >= 5 else close.mean()
            ma10 = close[-10:].mean() if len(close) >= 10 else close.mean()
            ma20 = close[-20:].mean() if len(close) >= 20 else close.mean()
            ma60 = close[-60:].mean() if len(close) >= 60 else close.mean()

            delta = np.diff(close)
            gain = np.where(delta > 0, delta, 0)
            loss = np.where(delta < 0, -delta, 0)
            avg_gain = np.mean(gain[-14:])
            avg_loss = np.mean(loss[-14:])
            rs = avg_gain / avg_loss if avg_loss > 0 else 100
            rsi = 100 - (100 / (1 + rs))

            ema12 = _calc_ema(close, 12)
            ema26 = _calc_ema(close, 26)
            macd = ema12 - ema26
            signal = _calc_ema(close[-9:], 9) if len(close) >= 9 else ema12
            macd_hist = macd - signal

            result += "【技术指标】\n"
            result += f"  MA5:  {ma5:.3f}\n"
            result += f"  MA10: {ma10:.3f}\n"
            result += f"  MA20: {ma20:.3f}\n"
            result += f"  MA60: {ma60:.3f}\n"
            result += f"  MACD: {macd:.4f} (hist: {macd_hist:.4f})\n"
            result += f"  RSI:  {rsi:.1f}\n\n"

            price = close[-1]
            trend = "上涨" if price > ma20 else "下跌"
            mid_trend = "上涨" if price > ma60 else "下跌"

            result += "【趋势判断】\n"
            result += f"  短期: {trend}\n"
            result += f"  中期: {mid_trend}\n"
            result += f"  支撑: {ma20 * 0.97:.2f}\n"
            result += f"  压力: {ma20 * 1.03:.2f}\n\n"

            suggestions = []
            if change_pct > 5:
                suggestions.append("⚠️ 涨幅较大，谨慎追高")
            elif change_pct < -5:
                suggestions.append("📉 跌幅较大，注意止损")

            if macd_hist > 0:
                suggestions.append("✅ MACD 金叉，看多")
            else:
                suggestions.append("📉 MACD 死叉，谨慎")

            if rsi > 70:
                suggestions.append("⚠️ RSI 超买")
            elif rsi < 30:
                suggestions.append("📈 RSI 超卖，可能反弹")

            result += "【建议】\n"
            for s in suggestions:
                result += f"  {s}\n"

        else:
            result += "【K线数据】\n"
            result += "  暂无数据（请在 OpenD 中下载历史K线）\n"

        return result

    except Exception as e:
        logger.error(f"股票分析失败 {symbol}: {e}")
        return f"分析失败: {str(e)}"


def _calc_ema(prices: np.ndarray, period: int) -> float:
    """计算 EMA"""
    multiplier = 2 / (period + 1)
    ema = float(prices[0])
    for price in prices[1:]:
        ema = (float(price) - ema) * multiplier + ema
    return ema
