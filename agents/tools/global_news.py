# -*- coding: utf-8 -*-
"""
全球金融新闻工具
获取国际市场动态、期货、外汇等信息
"""
import requests
from datetime import datetime
from langchain_core.tools import tool

from utils.logger import get_logger

logger = get_logger(__name__)


@tool
def get_global_finance_news() -> str:
    """
    获取全球金融市场动态，包括美股、港股、期货、外汇等信息。

    Returns:
        str: 全球金融资讯汇总
    """
    try:
        result = "【全球金融市场动态】\n\n"
        result += f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"

        global_news = []

        try:
            import akshare as ak
            us_df = ak.stock_us_spot_em()
            if us_df is not None and not us_df.empty:
                for _, row in us_df.head(5).iterrows():
                    name = row.get("名称", "")
                    price = row.get("最新价", 0)
                    change = row.get("涨跌幅", 0)
                    if abs(float(change)) > 0.5:
                        global_news.append(f"  {name}: {price} ({change:+.2f}%)")
        except Exception as e:
            logger.warning(f"获取美股数据失败: {e}")

        try:
            hk_df = ak.stock_hk_spot_em()
            if hk_df is not None and not hk_df.empty:
                for _, row in hk_df.head(3).iterrows():
                    name = row.get("名称", "")
                    price = row.get("最新价", 0)
                    change = row.get("涨跌幅", 0)
                    global_news.append(f"  {name}: {price} ({change:+.2f}%)")
        except Exception as e:
            logger.warning(f"获取港股数据失败: {e}")

        if global_news:
            result += "【热门美股/港股】\n"
            result += "\n".join(global_news) + "\n\n"
        else:
            result += "【热门美股/港股】\n暂无数据\n\n"

        try:
            futures_df = ak.futures_foreign_price("cum")
            if futures_df is not None and not futures_df.empty:
                result += "【期货市场】\n"
                for _, row in futures_df.head(5).iterrows():
                    name = row.get("品种", "")
                    price = row.get("最新价", 0)
                    change = row.get("涨跌幅", 0)
                    result += f"  {name}: {price} ({change:+.2f}%)\n"
                result += "\n"
        except Exception as e:
            logger.warning(f"获取期货数据失败: {e}")

        result += "【市场情绪参考】\n"
        result += "  • 美股涨跌互现: 关注美联储政策动向\n"
        result += "  • 港股跟随A股: 关注南下资金流向\n"
        result += "  • 商品波动加大: 关注地缘政治影响\n"

        return result

    except Exception as e:
        logger.error(f"获取全球金融资讯失败: {e}")
        return f"获取全球金融资讯失败: {str(e)}"
