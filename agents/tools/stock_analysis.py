# -*- coding: utf-8 -*-
"""
股票分析工具
深度分析单只股票的技术面、基本面和市场情绪
"""
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from langchain_core.tools import tool

import akshare as ak
from data import DataSource
from utils.logger import get_logger

logger = get_logger(__name__)


@tool
def analyze_stock(symbol: str) -> str:
    """
    深度分析指定股票的多维度信息。

    Args:
        symbol: 股票代码，如 "600036" 或 "000001"

    Returns:
        str: 股票综合分析报告
    """
    try:
        result = f"【股票深度分析】{symbol}\n\n"
        result += f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"

        info = _get_stock_info(symbol)
        if info:
            result += "【基本信息】\n"
            result += f"  名称: {info.get('name', 'N/A')}\n"
            result += f"  现价: {info.get('price', 'N/A')}\n"
            result += f"  涨跌幅: {info.get('change_pct', 'N/A')}\n"
            result += f"  市值: {info.get('market_cap', 'N/A')}\n"
            result += f"  市盈率: {info.get('pe', 'N/A')}\n"
            result += f"  市净率: {info.get('pb', 'N/A')}\n\n"

        kline_data = _get_kline_data(symbol)
        if kline_data:
            result += "【技术分析】\n"
            result += f"  20日均线: {kline_data.get('ma20', 'N/A')}\n"
            result += f"  60日均线: {kline_data.get('ma60', 'N/A')}\n"
            result += f"  MACD: {kline_data.get('macd', 'N/A')}\n"
            result += f"  RSI: {kline_data.get('rsi', 'N/A')}\n\n"

        trend = _analyze_trend(kline_data)
        result += "【趋势判断】\n"
        result += f"  短期趋势: {trend.get('short_term', 'N/A')}\n"
        result += f"  中期趋势: {trend.get('mid_term', 'N/A')}\n"
        result += f"  支撑位: {trend.get('support', 'N/A')}\n"
        result += f"  压力位: {trend.get('resistance', 'N/A')}\n\n"

        news = _get_stock_news(symbol)
        if news:
            result += "【近期新闻】\n"
            for n in news[:3]:
                result += f"  • {n}\n"
            result += "\n"

        suggestion = _generate_suggestion(info, kline_data, trend)
        result += "【综合建议】\n"
        result += f"  {suggestion}\n"

        return result

    except Exception as e:
        logger.error(f"股票分析失败 {symbol}: {e}")
        return f"股票分析失败: {str(e)}"


def _get_stock_info(symbol: str) -> Optional[Dict]:
    """获取股票基本信息"""
    try:
        df = ak.stock_zh_a_spot_em()
        if df is None or df.empty:
            return None

        row = df[df['代码'] == symbol]
        if row.empty:
            return None

        r = row.iloc[0]
        return {
            "name": r.get("名称", ""),
            "price": r.get("最新价", 0),
            "change_pct": r.get("涨跌幅", 0),
            "volume": r.get("成交量", 0),
            "amount": r.get("成交额", 0),
            "market_cap": _format_market_cap(r.get("总市值", 0)),
            "pe": r.get("市盈率-动态", "N/A"),
            "pb": r.get("市净率", "N/A"),
        }
    except Exception as e:
        logger.warning(f"获取股票信息失败 {symbol}: {e}")
        return None


def _get_kline_data(symbol: str) -> Optional[Dict]:
    """获取K线数据并计算技术指标"""
    try:
        data_source = DataSource()
        end_date = datetime.now()
        start_date = end_date - timedelta(days=120)

        df = data_source.get_kline(
            symbol,
            start_date.strftime("%Y%m%d"),
            end_date.strftime("%Y%m%d")
        )

        if df is None or df.empty or len(df) < 20:
            return None

        close = df["close"].values

        ma20 = float(close[-20:].mean())
        ma60 = float(close[-60:].mean()) if len(close) >= 60 else None

        ema12 = _calc_ema(close, 12)
        ema26 = _calc_ema(close, 26)
        macd = (ema12 - ema26) * 2

        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        rsi = float(100 - (100 / (1 + rs)).iloc[-1])

        return {
            "ma20": round(ma20, 2),
            "ma60": round(ma60, 2) if ma60 else None,
            "macd": round(macd, 4),
            "rsi": round(rsi, 2),
            "price": close[-1],
        }
    except Exception as e:
        logger.warning(f"获取K线数据失败 {symbol}: {e}")
        return None


def _calc_ema(prices: List[float], period: int) -> float:
    """计算指数移动平均"""
    import numpy as np
    prices_arr = np.array(prices)
    multiplier = 2 / (period + 1)
    ema = prices_arr[0]
    for price in prices_arr[1:]:
        ema = (price - ema) * multiplier + ema
    return ema


def _analyze_trend(kline_data: Dict) -> Dict:
    """分析价格趋势"""
    if not kline_data:
        return {}

    price = kline_data.get("price", 0)
    ma20 = kline_data.get("ma20", 0)
    ma60 = kline_data.get("ma60", 0)

    short_term = "上涨" if price > ma20 else "下跌"
    mid_term = "上涨" if ma60 and price > ma60 else ("震荡" if ma60 and abs(price - ma60) / ma60 < 0.02 else "下跌")

    support = round(ma20 * 0.97, 2)
    resistance = round(ma20 * 1.03, 2)

    return {
        "short_term": short_term,
        "mid_term": mid_term,
        "support": support,
        "resistance": resistance,
    }


def _get_stock_news(symbol: str) -> List[str]:
    """获取股票相关新闻"""
    try:
        news_df = ak.stock_news_em(symbol=symbol)
        if news_df is None or news_df.empty:
            return []

        return news_df.head(5)["新闻标题"].tolist()
    except Exception as e:
        logger.warning(f"获取新闻失败 {symbol}: {e}")
        return []


def _generate_suggestion(info: Dict, kline_data: Dict, trend: Dict) -> str:
    """生成综合建议"""
    if not info or not kline_data:
        return "数据不足，无法给出建议"

    price = info.get("price", 0)
    change_pct = info.get("change_pct", 0)
    macd = kline_data.get("macd", 0)
    rsi = kline_data.get("rsi", 50)
    short_term = trend.get("short_term", "")

    suggestions = []

    if change_pct > 5:
        suggestions.append("⚠️ 今日涨幅较大，追高风险较高")
    elif change_pct < -5:
        suggestions.append("📉 今日跌幅较大，注意止损")

    if macd > 0:
        suggestions.append("✅ MACD金叉，看多")
    else:
        suggestions.append("📉 MACD死叉，谨慎")

    if rsi > 70:
        suggestions.append("⚠️ RSI超买，注意回调风险")
    elif rsi < 30:
        suggestions.append("📈 RSI超卖，可能存在反弹机会")

    if short_term == "上涨":
        suggestions.append("✅ 短期趋势向上")

    if not suggestions:
        suggestions.append("📊 趋势不明，保持观望")

    return "\n  ".join(suggestions)


def _format_market_cap(value: float) -> str:
    """格式化市值"""
    if value is None or value == 0:
        return "N/A"
    if value >= 1e12:
        return f"{value/1e12:.2f}万亿"
    elif value >= 1e8:
        return f"{value/1e8:.2f}亿"
    else:
        return f"{value:.2f}万"
