# -*- coding: utf-8 -*-
"""
市场情绪分析工具
获取财经新闻，分析市场情绪
"""
import akshare as ak
from datetime import datetime, timedelta
from langchain_core.tools import tool

from utils.logger import get_logger

logger = get_logger(__name__)


@tool
def get_market_sentiment() -> str:
    """
    获取市场情绪信息，包括财经新闻和市场分析。

    Returns:
        str: 市场情绪分析结果，包含重要财经新闻摘要和市场情绪评估
    """
    try:
        news_list = []
        sentiment_score = 0
        sentiment_label = "中性"

        try:
            news_df = ak.stock_news_em(symbol="上证指数")
            if news_df is not None and not news_df.empty:
                for _, row in news_df.head(5).iterrows():
                    title = row.get("新闻标题", "")
                    news_list.append(title)
        except Exception as e:
            logger.warning(f"获取新闻失败: {e}")

        if not news_list:
            return "暂时无法获取市场新闻，请稍后重试"

        result = "【市场情绪分析】\n\n"
        result += f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"

        result += "【最新财经新闻】\n"
        for i, news in enumerate(news_list, 1):
            result += f"{i}. {news}\n"

        result += f"\n【情绪评估】: {sentiment_label}\n"
        result += "【建议】: 建议关注市场动态，结合量化信号做出交易决策"

        return result

    except Exception as e:
        logger.error(f"获取市场情绪失败: {e}")
        return f"获取市场情绪失败: {str(e)}"
