# -*- coding: utf-8 -*-
"""
A股政策新闻工具
获取A股大盘资讯、政策公告、重大事件等信息
"""
import akshare as ak
from datetime import datetime
from langchain_core.tools import tool

from utils.logger import get_logger

logger = get_logger(__name__)


@tool
def get_policy_news() -> str:
    """
    获取A股市场政策相关的最新资讯和公告。

    Returns:
        str: A股政策资讯和市场动态汇总
    """
    try:
        result = "【A股市场政策资讯】\n\n"
        result += f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"

        news_items = []

        try:
            news_df = ak.stock_news_em(symbol="上证指数")
            if news_df is not None and not news_df.empty:
                for _, row in news_df.head(8).iterrows():
                    title = row.get("新闻标题", "")
                    content = str(row.get("新闻内容", ""))[:100]
                    news_items.append({
                        "title": title,
                        "content": content,
                        "type": "市场"
                    })
        except Exception as e:
            logger.warning(f"获取上证新闻失败: {e}")

        try:
            # akshare>=1.18.x 无 macro_china_news；这里用“市场公告简报”替代
            if hasattr(ak, "stock_notice_report"):
                from datetime import timedelta
                date_ymd = datetime.now().strftime("%Y%m%d")
                notice_df = ak.stock_notice_report(symbol="全部", date=date_ymd)
                if notice_df is not None and not notice_df.empty:
                    for _, row in notice_df.head(5).iterrows():
                        title = row.get("公告标题", "") or row.get("标题", "") or row.get("公告名称", "")
                        if not title:
                            continue
                        news_items.append({
                            "title": str(title),
                            "content": "",
                            "type": "市场公告"
                        })
        except Exception as e:
            logger.warning(f"获取市场公告失败: {e}")

        try:
            from datetime import timedelta
            end_date = datetime.now().strftime("%Y%m%d")
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
            # 兼容 akshare>=1.18.x：使用 cninfo 版本接口（按 symbol 查询）；这里改为跳过“全市场披露报表”
            # 若需要个股公告请使用 agents.tools.stock_announcements
            _ = (start_date, end_date)
        except Exception as e:
            logger.warning(f"获取重要公告失败: {e}")

        if not news_items:
            result += "【暂无最新资讯】\n"
            result += "建议稍后重试或查看专业财经网站\n"
            return result

        policy_keywords = ["政策", "监管", "证监会", "央行", "财政部", "国务院", "降准", "加息", "LPR", "IPO", "注册"]
        market_keywords = ["大盘", "指数", "板块", "资金", "外资", "北向", "ETF"]

        result += "【重大政策/宏观】\n"
        policy_news = [n for n in news_items if any(k in n["title"] for k in policy_keywords)]
        if policy_news:
            for i, news in enumerate(policy_news[:3], 1):
                result += f"{i}. {news['title']}\n"
                result += f"   类型: {news['type']}\n"
        else:
            result += "  暂无重大政策公告\n"
        result += "\n"

        result += "【市场热点新闻】\n"
        market_news = [n for n in news_items if any(k in n["title"] for k in market_keywords)]
        if market_news:
            for i, news in enumerate(market_news[:5], 1):
                result += f"{i}. {news['title']}\n"
        else:
            for i, news in enumerate(news_items[:5], 1):
                result += f"{i}. {news['title']}\n"
        result += "\n"

        result += "【政策影响分析】\n"
        if policy_news:
            result += "  ⚠️ 存在政策相关新闻，建议关注政策面对市场的影响\n"
        else:
            result += "  ✅ 无重大政策变动，市场处于相对平稳状态\n"

        result += "\n【投资提示】\n"
        result += "  • 政策面：关注货币政策和财政政策动向\n"
        result += "  • 资金面：关注北向资金流向和ETF净申购\n"
        result += "  • 消息面：留意重大突发事件和市场传闻\n"

        return result

    except Exception as e:
        logger.error(f"获取A股政策资讯失败: {e}")
        return f"获取A股政策资讯失败: {str(e)}"
