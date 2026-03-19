# -*- coding: utf-8 -*-
"""
综合新闻报告推送工具
整合全球金融、政策和个股资讯，统一推送
"""
from datetime import datetime
from langchain_core.tools import tool

from trading import get_pusher
from utils.logger import get_logger

logger = get_logger(__name__)


@tool
def push_news_report(report_content: str = "") -> str:
    """
    推送综合新闻报告到手机通知。

    Args:
        report_content: 报告内容，如果不提供则生成简要报告

    Returns:
        str: 推送结果
    """
    try:
        pusher = get_pusher()

        if not report_content:
            report_content = f"""
【综合新闻报告】{datetime.now().strftime('%Y-%m-%d %H:%M')}

请使用 AI Agent 执行新闻分析任务获取完整报告。
"""

        title = f"📰 每日资讯 ({datetime.now().strftime('%m-%d %H:%M')})"

        success = pusher.push(title, report_content)

        if success:
            logger.info("新闻报告推送成功")
            return "新闻报告推送成功"
        else:
            logger.warning("新闻报告推送失败")
            return "推送失败，请检查 Bark 配置"

    except Exception as e:
        logger.error(f"推送新闻报告失败: {e}")
        return f"推送失败: {str(e)}"
