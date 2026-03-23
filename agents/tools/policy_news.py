# -*- coding: utf-8 -*-
"""
A股政策新闻工具
"""
from datetime import datetime

from langchain_core.tools import tool

from agents.tools.news_router import build_market_news_digest
from utils.logger import get_logger

logger = get_logger(__name__)


@tool
def get_policy_news() -> str:
    """
    获取A股市场政策相关新闻与公告。

    Returns:
        政策、宏观与市场热点摘要
    """
    try:
        query = "A股最新政策、证监会、央行、国务院、交易所、指数异动、板块轮动、海外市场影响"
        digest = build_market_news_digest(query=query, limit=6)
        lines = [
            "【A股市场政策资讯】",
            "",
            f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "",
            digest or "暂无最新政策与市场资讯",
        ]
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"获取A股政策资讯失败: {e}")
        return f"获取A股政策资讯失败: {e}"
