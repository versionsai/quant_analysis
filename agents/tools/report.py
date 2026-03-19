# -*- coding: utf-8 -*-
"""
报告推送工具
生成并推送交易报告
"""
from datetime import datetime
from langchain_core.tools import tool

from trading.simulate_trading import get_trader
from trading import get_pusher
from utils.logger import get_logger

logger = get_logger(__name__)


@tool
def push_report(report_content: str = "") -> str:
    """
    推送交易报告到手机通知。

    Args:
        report_content: 报告内容，如果不提供则自动生成

    Returns:
        str: 推送结果
    """
    try:
        pusher = get_pusher()

        if not report_content:
            trader = get_trader()
            report_content = trader.get_report()

        title = f"📊 交易报告 ({datetime.now().strftime('%Y-%m-%d %H:%M')})"

        success = pusher.push(title, report_content)

        if success:
            logger.info("报告推送成功")
            return "报告推送成功"
        else:
            logger.warning("报告推送失败")
            return "报告推送失败，请检查 Bark 配置"

    except Exception as e:
        logger.error(f"推送报告失败: {e}")
        return f"推送报告失败: {str(e)}"
