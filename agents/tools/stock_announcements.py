# -*- coding: utf-8 -*-
"""
持仓个股公告与资讯工具
"""
from datetime import datetime
from typing import Dict, List

from langchain_core.tools import tool

from agents.tools.news_router import build_watchlist_news_digest
from data.recommend_db import get_db
from utils.logger import get_logger

logger = get_logger(__name__)


@tool
def get_holding_announcements() -> str:
    """
    获取持仓个股的最新公告、新闻和研报摘要。

    Returns:
        持仓个股资讯汇总及影响分析
    """
    try:
        db = get_db()
        holdings = db.get_holdings_aggregated()

        result = "【持仓个股最新资讯】\n\n"
        result += f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"

        if not holdings:
            result += "【当前持仓】空仓\n"
            result += "无需获取个股资讯\n"
            return result

        result += f"【持仓列表】{len(holdings)}只\n"
        result += "-" * 50 + "\n"

        all_items: List[Dict[str, str]] = []
        digest = build_watchlist_news_digest(holdings, limit=max(6, len(holdings) * 2))
        digest_lines = [line.strip() for line in str(digest or "").splitlines() if line.strip()]

        for holding in holdings:
            code = str(holding.get("code", "") or "").strip()
            name = str(holding.get("name", "") or "").strip()
            buy_price = float(holding.get("avg_buy_price", 0) or 0)
            current_price = float(holding.get("avg_current_price", buy_price) or buy_price)

            result += f"\n📳 {code} {name}\n"
            result += f"   买入价: {buy_price:.2f} | 现价: {current_price:.2f}\n"

            stock_news: List[str] = []
            stock_notices: List[Dict[str, str]] = []
            related_lines = [
                line for line in digest_lines
                if (code and code in line) or (name and name in line)
            ]
            for line in related_lines[:8]:
                cleaned = line.strip(" -•\t")
                if any(keyword in cleaned for keyword in ["公告", "披露", "年报", "季报", "问询", "回复函", "巨潮公告"]):
                    stock_notices.append({"title": cleaned, "date": ""})
                else:
                    stock_news.append(cleaned)

            if stock_news:
                result += f"   【近期新闻】{len(stock_news)}条\n"
                for news in stock_news[:2]:
                    result += f"     - {news[:60]}\n"
                    all_items.append({"code": code, "name": name, "type": "新闻", "title": news})
            else:
                result += "   【近期新闻】暂无\n"

            if stock_notices:
                result += f"   【重要公告】{len(stock_notices)}条\n"
                for notice in stock_notices[:2]:
                    result += f"     - {notice['title'][:60]}\n"
                    all_items.append({"code": code, "name": name, "type": "公告", "title": notice["title"]})
            else:
                result += "   【重要公告】暂无\n"

        result += "\n" + "=" * 50 + "\n"
        result += "【个股资讯影响分析】\n\n"

        if not all_items:
            result += "  暂无重要个股资讯\n\n"
        else:
            positive_keywords = ["增长", "盈利", "突破", "合作", "订单", "中标", "增持", "回购", "业绩预增", "分红"]
            negative_keywords = ["减持", "亏损", "预警", "风险", "调查", "处罚", "诉讼", "商誉减值", "业绩预减"]

            positive = []
            negative = []
            neutral = []
            for item in all_items:
                title = item["title"]
                if any(keyword in title for keyword in positive_keywords):
                    positive.append(item)
                elif any(keyword in title for keyword in negative_keywords):
                    negative.append(item)
                else:
                    neutral.append(item)

            if positive:
                result += "✅ 【利好个股】\n"
                for item in positive[:3]:
                    result += f"  - {item['name']}({item['code']}): {item['title'][:60]}\n"
                result += "\n"

            if negative:
                result += "⚠️ 【利空个股】\n"
                for item in negative[:3]:
                    result += f"  - {item['name']}({item['code']}): {item['title'][:60]}\n"
                result += "\n"

            if neutral:
                result += "📚 【中性资讯】\n"
                for item in neutral[:2]:
                    result += f"  - {item['name']}({item['code']}): {item['title'][:60]}\n"
                result += "\n"

        result += "【交易建议】\n"
        if any(item for item in all_items if any(keyword in item["title"] for keyword in ["减持", "亏损", "预警", "风险", "处罚"])):
            result += "  ⚠️ 存在利空消息，建议关注持仓风险\n"
            result += "  如持仓触及止损位，考虑减仓\n"
        elif any(item for item in all_items if any(keyword in item["title"] for keyword in ["增长", "盈利", "订单", "增持", "回购"])):
            result += "  ✅ 存在利好消息，可适度持有\n"
            result += "  关注个股后续走势，适时止盈\n"
        else:
            result += "  📳 个股暂无重大资讯，维持现有策略\n"

        return result
    except Exception as e:
        logger.error(f"获取持仓个股资讯失败: {e}")
        return f"获取持仓个股资讯失败: {e}"
