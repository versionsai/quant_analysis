# -*- coding: utf-8 -*-
"""
资讯路由工具
统一封装财联社、全球市场与公告摘要能力。
"""
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Sequence

import akshare as ak
from langchain_core.tools import tool

from agents.tools.cls_news import _fetch_cls_rows
from agents.tools.global_news import get_global_finance_news
from utils.logger import get_logger

logger = get_logger(__name__)

POSITIVE_KEYWORDS = [
    "中标", "签约", "订单", "回购", "增持", "分红", "业绩预增", "扭亏", "增长",
    "突破", "合作", "获批", "投产", "涨停", "连板", "修复", "超预期",
]

NEGATIVE_KEYWORDS = [
    "减持", "预亏", "亏损", "下滑", "处罚", "问询", "诉讼", "调查", "违约",
    "跌停", "大跌", "跳水", "破位", "终止", "取消", "停产", "减值",
]

RISK_KEYWORDS = [
    "风险", "波动", "地缘", "冲突", "关税", "通胀", "监管", "停牌", "退市",
    "质押", "减持计划", "业绩预警", "流动性", "高位", "回撤", "不确定性",
]


def _clean_text(value: Any) -> str:
    """清洗文本。"""
    text = str(value or "").replace("\r", "\n")
    text = " ".join(part.strip() for part in text.splitlines() if part.strip())
    while "  " in text:
        text = text.replace("  ", " ")
    return text.strip()


def _extract_keywords(query: str) -> List[str]:
    """从查询文本中提取关键词。"""
    text = str(query or "").strip()
    if not text:
        return []

    keywords = re.split(r"[\s,，、;；|]+", text)
    result: List[str] = []
    for keyword in keywords:
        item = keyword.strip()
        if len(item) < 2:
            continue
        if item not in result:
            result.append(item)
    return result[:12]


def _normalize_symbol_entries(entries: Sequence[Dict[str, Any]]) -> List[Dict[str, str]]:
    """规范化标的列表。"""
    normalized: List[Dict[str, str]] = []
    seen = set()
    for item in entries or []:
        code = str(item.get("code", "") or "").strip()
        name = _clean_text(item.get("name", ""))
        if not code and not name:
            continue
        cache_key = f"{code}|{name}"
        if cache_key in seen:
            continue
        normalized.append({"code": code, "name": name})
        seen.add(cache_key)
    return normalized


def _parse_symbols_text(symbols: str) -> List[Dict[str, str]]:
    """解析字符串形式的标的列表。"""
    text = str(symbols or "").strip()
    if not text:
        return []

    entries: List[Dict[str, str]] = []
    for part in re.split(r"[\n,，;；|]+", text):
        raw = part.strip()
        if not raw:
            continue
        code_match = re.search(r"\b(\d{6})\b", raw)
        code = code_match.group(1) if code_match else ""
        name = raw.replace(code, "").strip() if code else raw
        entries.append({"code": code, "name": name})
    return _normalize_symbol_entries(entries)


def _pick_row_value(row: Dict[str, Any], candidates: List[str]) -> str:
    """从公告行中提取字段。"""
    for key in candidates:
        value = row.get(key)
        cleaned = _clean_text(value)
        if cleaned:
            return cleaned
    return ""


def _fetch_cninfo_notices(code: str, limit: int = 3, days: int = 7) -> List[Dict[str, str]]:
    """获取巨潮公告摘要。"""
    if not code:
        return []
    if str(code).startswith(("1", "5")):
        return []

    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")

    try:
        data_frame = ak.stock_zh_a_disclosure_report_cninfo(
            symbol=code,
            market="沪深京",
            start_date=start_date,
            end_date=end_date,
        )
        if data_frame is None or data_frame.empty:
            return []

        notices: List[Dict[str, str]] = []
        for _, row in data_frame.head(limit).iterrows():
            row_dict = row.to_dict()
            title = _pick_row_value(row_dict, ["公告标题", "标题", "title", "announcementTitle"])
            if not title:
                continue
            notices.append(
                {
                    "source": "巨潮公告",
                    "title": title,
                    "date": _pick_row_value(row_dict, ["公告时间", "公告日期", "时间", "date"]),
                    "content": _pick_row_value(row_dict, ["摘要", "内容", "description"]),
                    "category": _pick_row_value(row_dict, ["公告类型", "类型", "category"]),
                }
            )
        return notices
    except Exception as e:
        logger.debug(f"获取巨潮公告失败 {code}: {e}")
        return []


def _fetch_cls_market_items(query: str = "", limit: int = 6) -> List[Dict[str, str]]:
    """获取市场级财联社资讯。"""
    rows = _fetch_cls_rows(symbol="全部", limit=max(limit * 4, 20))
    keywords = _extract_keywords(query)
    result: List[Dict[str, str]] = []

    for row in rows:
        category = str(row.get("category", "")).strip()
        level = str(row.get("level", "")).strip()
        title = _clean_text(row.get("title", ""))
        content = _clean_text(row.get("content", ""))
        haystack = f"{title} {content}"
        if keywords and not any(keyword in haystack for keyword in keywords):
            if category not in {"macro", "regulation", "overseas", "industry"}:
                continue
        if not keywords and category not in {"macro", "regulation", "overseas", "industry"}:
            continue
        if level == "normal" and category not in {"macro", "overseas"}:
            continue

        result.append(
            {
                "source": f"财联社/{row.get('category_label', '其他')}",
                "title": title,
                "date": f"{row.get('publish_date', '')} {row.get('publish_time', '')}".strip(),
                "content": content,
            }
        )
        if len(result) >= limit:
            break

    return result


def _fetch_cls_symbol_items(entries: Sequence[Dict[str, str]], limit: int = 4) -> List[Dict[str, str]]:
    """获取与标的相关的财联社资讯。"""
    normalized = _normalize_symbol_entries(entries)
    if not normalized:
        return []

    keywords: List[str] = []
    for item in normalized:
        code = str(item.get("code", "")).strip()
        name = str(item.get("name", "")).strip()
        if code:
            keywords.append(code)
        if name:
            keywords.append(name)

    rows = _fetch_cls_rows(symbol="全部", limit=max(limit * 8, 30))
    result: List[Dict[str, str]] = []
    seen = set()
    for row in rows:
        title = _clean_text(row.get("title", ""))
        content = _clean_text(row.get("content", ""))
        haystack = f"{title} {content}"
        if not any(keyword in haystack for keyword in keywords):
            continue

        cache_key = f"{title}|{row.get('publish_date', '')}|{row.get('publish_time', '')}"
        if cache_key in seen:
            continue
        seen.add(cache_key)

        result.append(
            {
                "source": f"财联社/{row.get('category_label', '其他')}",
                "title": title,
                "date": f"{row.get('publish_date', '')} {row.get('publish_time', '')}".strip(),
                "content": content,
            }
        )
        if len(result) >= limit:
            break
    return result


def _format_news_items(title: str, items: List[Dict[str, str]], limit: int = 6) -> str:
    """格式化资讯条目。"""
    lines = [f"【{title}】"]
    for index, item in enumerate((items or [])[:limit], 1):
        item_title = _clean_text(item.get("title", "")) or "未命名资讯"
        source = _clean_text(item.get("source", "")) or "未知来源"
        date_text = _clean_text(item.get("date", ""))[:19]
        content = _clean_text(item.get("content", ""))
        category = _clean_text(item.get("category", ""))

        meta_parts = [source]
        if category:
            meta_parts.append(category)
        if date_text:
            meta_parts.append(date_text)

        lines.append(f"{index}. {item_title} ({' | '.join(meta_parts)})")
        if content:
            lines.append(f"   {content}")

    if len(lines) == 1:
        lines.append("暂无新增资讯")
    return "\n".join(lines)


def _classify_news_item(item: Dict[str, str]) -> str:
    """按关键词对资讯进行粗分类。"""
    text = f"{_clean_text(item.get('title', ''))} {_clean_text(item.get('content', ''))}"
    if any(keyword in text for keyword in RISK_KEYWORDS):
        return "risk"
    if any(keyword in text for keyword in NEGATIVE_KEYWORDS):
        return "negative"
    if any(keyword in text for keyword in POSITIVE_KEYWORDS):
        return "positive"
    return "neutral"


def _build_action_conclusion(items: List[Dict[str, str]], title: str) -> List[str]:
    """根据资讯分布生成简短结论。"""
    counts = {"positive": 0, "negative": 0, "risk": 0, "neutral": 0}
    for item in items:
        counts[_classify_news_item(item)] += 1

    conclusions: List[str] = []
    if counts["risk"] > 0 or counts["negative"] > counts["positive"]:
        conclusions.append(f"- {title} 当前偏谨慎，先核对利空与风险提示，再决定是否追价。")
    elif counts["positive"] > 0 and counts["negative"] == 0:
        conclusions.append(f"- {title} 存在正向催化，可结合量价与盘口做二次确认。")
    else:
        conclusions.append(f"- {title} 暂无单边结论，优先结合量化信号和盘面承接判断。")

    if counts["risk"] > 0:
        conclusions.append(f"- 风险类信息 {counts['risk']} 条，盘中操作宜降低预期收益并收紧止损。")
    if counts["positive"] > 0:
        conclusions.append(f"- 利好类信息 {counts['positive']} 条，若价格强于指数可提升关注级别。")
    return conclusions


def _format_structured_news(title: str, items: List[Dict[str, str]], limit: int = 8) -> str:
    """将资讯整理成利好/利空/风险/结论结构。"""
    lines = [f"【{title}】"]
    if not items:
        lines.append("暂无新增资讯")
        return "\n".join(lines)

    grouped = {
        "positive": [],
        "negative": [],
        "risk": [],
        "neutral": [],
    }
    for item in items[:limit]:
        grouped[_classify_news_item(item)].append(item)

    section_meta = [
        ("positive", "利好"),
        ("negative", "利空"),
        ("risk", "风险提示"),
        ("neutral", "中性"),
    ]
    for key, label in section_meta:
        rows = grouped[key]
        if not rows:
            continue
        lines.append(f"【{label}】")
        for index, item in enumerate(rows[:3], 1):
            item_title = _clean_text(item.get("title", "")) or "未命名资讯"
            source = _clean_text(item.get("source", "")) or "未知来源"
            date_text = _clean_text(item.get("date", ""))[:19]
            content = _clean_text(item.get("content", ""))
            meta = " | ".join([part for part in [source, date_text] if part])
            lines.append(f"{index}. {item_title}" if not meta else f"{index}. {item_title} ({meta})")
            if content:
                lines.append(f"   {content}")

    lines.append("【结论】")
    lines.extend(_build_action_conclusion(items[:limit], title))
    return "\n".join(lines)


def build_market_news_digest(query: str = "", limit: int = 6) -> str:
    """构建市场资讯摘要。"""
    sections: List[str] = []

    try:
        global_text = str(get_global_finance_news.invoke({}) or "").strip()
        if global_text:
            sections.append(global_text)
    except Exception as e:
        logger.warning(f"获取全球市场摘要失败: {e}")

    market_items = _fetch_cls_market_items(query=query, limit=limit)
    if market_items:
        sections.append(_format_news_items("财联社市场资讯", market_items, limit=limit))

    return "\n\n".join([section for section in sections if section]).strip()


def build_watchlist_news_digest(
    entries: Sequence[Dict[str, Any]],
    limit: int = 6,
    title: str = "持仓/信号池资讯",
) -> str:
    """构建标的资讯摘要。"""
    normalized = _normalize_symbol_entries(entries)
    if not normalized:
        return ""

    items: List[Dict[str, str]] = []
    for item in normalized[:6]:
        code = str(item.get("code", "")).strip()
        name = str(item.get("name", "")).strip()
        for notice in _fetch_cninfo_notices(code, limit=2):
            notice_item = dict(notice)
            if name and name not in notice_item["title"]:
                notice_item["title"] = f"{name}: {notice_item['title']}"
            items.append(notice_item)

    items.extend(_fetch_cls_symbol_items(normalized[:6], limit=max(2, limit // 2)))
    return _format_structured_news(title, items, limit=limit)


def build_intraday_news_digest(rows: Sequence[Dict[str, Any]], limit: int = 6) -> str:
    """构建盘中关注标的资讯摘要。"""
    normalized = _normalize_symbol_entries(rows)
    if not normalized:
        return "【重点标的资讯】\n暂无新增资讯"

    news_text = build_watchlist_news_digest(normalized, limit=limit, title="重点标的资讯")
    if not news_text:
        return "【重点标的资讯】\n暂无新增资讯"
    return news_text


@tool
def get_market_news_digest(query: str = "", limit: int = 6) -> str:
    """
    获取市场级资讯摘要，优先使用财联社和全球市场缓存。
    """
    return build_market_news_digest(query=query, limit=limit)


@tool
def get_symbol_news_digest(symbols: str, limit: int = 6) -> str:
    """
    获取标的相关资讯摘要，优先聚合巨潮公告与财联社个股事件。
    """
    entries = _parse_symbols_text(symbols)
    if not entries:
        return "【标的资讯】\n未提供有效标的"
    text = build_watchlist_news_digest(entries, limit=limit, title="标的资讯")
    return text or "【标的资讯】\n暂无新增资讯"


@tool
def search_market_context(query: str, limit: int = 8) -> str:
    """
    面向市场、板块、行业与外围冲击的定向资讯搜索。
    适合用于解释指数波动、题材催化、宏观事件和市场情绪变化。
    """
    items = _fetch_cls_market_items(query=query, limit=limit)
    if not items:
        return "【市场搜索】\n暂无相关资讯"
    return _format_structured_news("市场搜索", items, limit=limit)


@tool
def search_symbol_context(symbols: str, limit: int = 8) -> str:
    """
    面向个股、ETF、持仓或信号池标的的定向资讯搜索。
    优先返回巨潮公告和财联社个股事件。
    """
    entries = _parse_symbols_text(symbols)
    if not entries:
        return "【标的搜索】\n未提供有效标的"
    text = build_watchlist_news_digest(entries, limit=limit, title="标的搜索")
    if not text:
        return "【标的搜索】\n暂无相关资讯"
    return text


@tool
def search_policy_context(query: str = "A股政策 监管 宏观", limit: int = 8) -> str:
    """
    面向政策、监管与宏观事件的定向资讯搜索。
    适合盘前、收盘复盘和风险提示归因。
    """
    items = _fetch_cls_market_items(query=query, limit=limit)
    if not items:
        return "【政策搜索】\n暂无相关资讯"
    return _format_structured_news("政策搜索", items, limit=limit)
