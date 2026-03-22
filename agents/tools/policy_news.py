# -*- coding: utf-8 -*-
"""
A股政策新闻工具
"""
from datetime import datetime

from langchain_core.tools import tool

from utils.logger import get_logger
from utils.miaoxiang_client import search_financial_news

logger = get_logger(__name__)


@tool
def get_policy_news() -> str:
    """
    获取A股市场政策相关新闻与公告。

    Returns:
        政策、宏观与市场热点摘要
    """
    try:
        result = "【A股市场政策资讯】\n\n"
        result += f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"

        policy_text = search_financial_news(
            "A股最新政策、证监会、央行、国务院、交易所、IPO、注册制相关新闻"
        )
        market_text = search_financial_news(
            "A股最新市场热点、指数异动、北向资金、ETF资金流向、板块轮动"
        )

        if not policy_text and not market_text:
            result += "【暂无最新资讯】\n"
            result += "建议稍后重试妙想资讯查询。\n"
            return result

        result += "【重大政策 / 宏观】\n"
        result += f"{policy_text[:1800] if policy_text else '暂无明显政策面新增信息'}\n\n"

        result += "【市场热点新闻】\n"
        result += f"{market_text[:1800] if market_text else '暂无明显市场热点新增信息'}\n\n"

        result += "【说明】\n"
        result += "  - 当前优先使用妙想资讯搜索补充政策、公告与市场动态\n"
        result += "  - akshare 主要保留给情绪、概念、涨停池与分钟级辅助分析\n"
        return result
    except Exception as e:
        logger.error(f"获取A股政策资讯失败: {e}")
        return f"获取A股政策资讯失败: {e}"
