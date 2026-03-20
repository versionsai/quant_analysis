# -*- coding: utf-8 -*-
"""
个股公告工具
获取持仓个股的最新公告、新闻和分析
"""
import akshare as ak
from datetime import datetime, timedelta
from typing import List, Dict
from langchain_core.tools import tool

from data.recommend_db import get_db
from utils.logger import get_logger

logger = get_logger(__name__)


@tool
def get_holding_announcements() -> str:
    """
    获取持仓个股的最新公告、新闻和重要信息。

    Returns:
        str: 持仓个股相关资讯汇总及影响分析
    """
    try:
        db = get_db()
        holdings = db.get_holdings()

        result = "【持仓个股最新资讯】\n\n"
        result += f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"

        if not holdings:
            result += "【当前持仓】: 空仓\n"
            result += "无需获取个股资讯\n"
            return result

        result += f"【持仓列表】({len(holdings)}只)\n"
        result += "-" * 50 + "\n"

        all_announcements: List[Dict] = []

        for holding in holdings:
            code = holding.get("code", "")
            name = holding.get("name", "")
            buy_price = holding.get("buy_price", 0)
            current_price = holding.get("current_price", buy_price)

            result += f"\n📊 {code} {name}\n"
            result += f"   买入价: {buy_price:.2f} | 现价: {current_price:.2f}\n"

            stock_news = []
            stock_notices = []

            try:
                news_df = ak.stock_news_em(symbol=code)
                if news_df is not None and not news_df.empty:
                    for _, row in news_df.head(3).iterrows():
                        title = row.get("新闻标题", "")
                        stock_news.append(title)
            except Exception as e:
                logger.warning(f"获取{code}新闻失败: {e}")

            try:
                end_date = datetime.now().strftime("%Y%m%d")
                start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
                if hasattr(ak, "stock_zh_a_disclosure_report_cninfo"):
                    notice_df = ak.stock_zh_a_disclosure_report_cninfo(
                        symbol=code,
                        start_date=start_date,
                        end_date=end_date,
                    )
                elif hasattr(ak, "stock_notice_report"):
                    notice_df = ak.stock_notice_report(symbol=code, date=end_date)
                else:
                    notice_df = None
                if notice_df is not None and not notice_df.empty:
                    for _, row in notice_df.head(3).iterrows():
                        title = row.get("公告标题", "") or row.get("标题", "") or row.get("公告名称", "")
                        notice_date = row.get("公告日期", "") or row.get("公告时间", "") or row.get("公告发布时间", "")
                        stock_notices.append({"title": title, "date": notice_date})
            except Exception as e:
                logger.warning(f"获取{code}公告失败: {e}")

            if stock_news:
                result += f"   【近期新闻】({len(stock_news)}条)\n"
                for news in stock_news[:2]:
                    result += f"     • {news[:40]}\n"
                    all_announcements.append({
                        "code": code,
                        "name": name,
                        "type": "新闻",
                        "title": news
                    })
            else:
                result += "   【近期新闻】: 暂无\n"

            if stock_notices:
                result += f"   【重要公告】({len(stock_notices)}条)\n"
                for notice in stock_notices[:2]:
                    result += f"     • [{notice['date']}] {notice['title'][:30]}\n"
                    all_announcements.append({
                        "code": code,
                        "name": name,
                        "type": "公告",
                        "title": notice["title"]
                    })
            else:
                result += "   【重要公告】: 暂无\n"

        result += "\n" + "=" * 50 + "\n"
        result += "【个股资讯影响分析】\n\n"

        if not all_announcements:
            result += "  暂无重要个股资讯\n\n"
        else:
            positive_keywords = ["增长", "盈利", "突破", "合作", "订单", "中标", "增持", "回购", "业绩预增", "利润分配"]
            negative_keywords = ["减持", "亏损", "预警", "风险", "调查", "处罚", "诉讼", "商誉减值", "业绩预减"]

            positive = []
            negative = []
            neutral = []

            for ann in all_announcements:
                title = ann["title"]
                if any(k in title for k in positive_keywords):
                    positive.append(ann)
                elif any(k in title for k in negative_keywords):
                    negative.append(ann)
                else:
                    neutral.append(ann)

            if positive:
                result += "✅ 【利好个股】\n"
                for p in positive[:3]:
                    result += f"  • {p['name']}({p['code']}): {p['title'][:35]}\n"
                result += "\n"

            if negative:
                result += "⚠️ 【利空个股】\n"
                for n in negative[:3]:
                    result += f"  • {n['name']}({n['code']}): {n['title'][:35]}\n"
                result += "\n"

            if neutral:
                result += "📌 【中性资讯】\n"
                for n in neutral[:2]:
                    result += f"  • {n['name']}({n['code']}): {n['title'][:35]}\n"
                result += "\n"

        result += "【交易建议】\n"
        if holdings:
            if negative:
                result += "  ⚠️ 存在利空消息，建议关注持仓风险\n"
                result += "  如持仓触及止损位，考虑减仓\n"
            elif positive:
                result += "  ✅ 存在利好消息，可适度持有\n"
                result += "  关注个股后续走势，适时止盈\n"
            else:
                result += "  📊 个股暂无重大资讯，维持现有策略\n"

        return result

    except Exception as e:
        logger.error(f"获取持仓个股资讯失败: {e}")
        return f"获取持仓个股资讯失败: {str(e)}"
