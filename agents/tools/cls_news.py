# -*- coding: utf-8 -*-
"""
财联社快讯工具。
"""
import hashlib
import json
import os
from datetime import datetime
from typing import Dict, List

import akshare as ak
from langchain_core.tools import tool

from utils.logger import get_logger

logger = get_logger(__name__)

CLS_NEWS_LEVEL_LABELS: Dict[str, str] = {
    "critical": "高优先级",
    "important": "重点",
    "normal": "普通",
}

CLS_NEWS_KEYWORDS: Dict[str, List[str]] = {
    "critical": [
        "证监会",
        "国务院",
        "国务院常务会议",
        "央行",
        "人民银行",
        "金监总局",
        "沪深交易所",
        "停牌",
        "复牌",
        "并购重组",
        "重大资产重组",
        "业绩预增",
        "业绩预亏",
        "业绩预告",
        "地缘冲突",
        "空袭",
        "战争",
        "制裁",
        "霍尔木兹海峡",
    ],
    "important": [
        "降准",
        "降息",
        "LPR",
        "社融",
        "CPI",
        "PPI",
        "出口",
        "关税",
        "算力",
        "人工智能",
        "机器人",
        "半导体",
        "新能源",
        "光伏",
        "军工",
        "医药",
        "黄金",
        "原油",
        "稀土",
        "贴息",
        "补贴",
        "数据中心",
    ],
}


def _get_cls_cache_path() -> str:
    """获取财联社快讯缓存路径。"""
    cache_dir = os.environ.get("QUANT_CACHE_DIR", "./runtime/data")
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, "cls_telegraph_cache.json")


def _load_cls_cache() -> Dict[str, Dict]:
    """读取财联社快讯缓存。"""
    cache_path = _get_cls_cache_path()
    if not os.path.exists(cache_path):
        return {}

    try:
        with open(cache_path, "r", encoding="utf-8") as file:
            data = json.load(file)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning(f"读取财联社快讯缓存失败: {e}")
        return {}


def _save_cls_cache(cache: Dict[str, Dict]) -> None:
    """保存财联社快讯缓存。"""
    cache_path = _get_cls_cache_path()
    try:
        with open(cache_path, "w", encoding="utf-8") as file:
            json.dump(cache, file, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"保存财联社快讯缓存失败: {e}")


def _build_news_id(item: Dict) -> str:
    """生成快讯唯一标识。"""
    raw_text = "|".join(
        [
            str(item.get("标题", "")),
            str(item.get("内容", "")),
            str(item.get("发布日期", "")),
            str(item.get("发布时间", "")),
        ]
    )
    return hashlib.md5(raw_text.encode("utf-8")).hexdigest()


def classify_cls_news(item: Dict) -> Dict[str, object]:
    """对财联社快讯进行关键词分级。"""
    title = str(item.get("title", "")).strip()
    content = str(item.get("content", "")).strip()
    full_text = f"{title} {content}"

    matched_keywords: List[str] = []
    level = "normal"

    for candidate in CLS_NEWS_KEYWORDS["critical"]:
        if candidate in full_text:
            matched_keywords.append(candidate)
    if matched_keywords:
        level = "critical"
    else:
        for candidate in CLS_NEWS_KEYWORDS["important"]:
            if candidate in full_text:
                matched_keywords.append(candidate)
        if matched_keywords:
            level = "important"

    enriched = dict(item)
    enriched["level"] = level
    enriched["level_label"] = CLS_NEWS_LEVEL_LABELS.get(level, level)
    enriched["matched_keywords"] = matched_keywords[:5]
    return enriched


def filter_cls_news_by_level(items: List[Dict], min_level: str = "important") -> List[Dict]:
    """按级别过滤财联社快讯。"""
    rank_map = {"normal": 1, "important": 2, "critical": 3}
    threshold = rank_map.get(str(min_level or "important"), 2)
    result: List[Dict] = []
    for item in items or []:
        level = str(item.get("level", "normal"))
        if rank_map.get(level, 1) >= threshold:
            result.append(item)
    return result


def format_cls_alert(items: List[Dict], limit: int = 3) -> str:
    """格式化高优先级财联社提醒。"""
    lines = ["【财联社快讯预警】"]
    for item in (items or [])[:limit]:
        time_text = f"{item.get('publish_date', '')} {item.get('publish_time', '')}".strip()
        title = str(item.get("title", "")).strip()
        level_label = str(item.get("level_label", "普通"))
        keywords = item.get("matched_keywords", []) or []
        content = str(item.get("content", "")).replace("\n", " ").strip()
        if len(content) > 80:
            content = f"{content[:80]}..."
        lines.append(f"- [{level_label}] {time_text} {title}")
        if keywords:
            lines.append(f"  关键词: {', '.join([str(keyword) for keyword in keywords[:4]])}")
        if content:
            lines.append(f"  {content}")

    if len(lines) == 1:
        lines.append("暂无高优先级快讯")
    return "\n".join(lines)


def _fetch_cls_rows(symbol: str = "全部", limit: int = 10) -> List[Dict]:
    """获取财联社快讯原始记录。"""
    try:
        data_frame = ak.stock_info_global_cls(symbol=symbol)
        if data_frame is None or data_frame.empty:
            return []

        rows: List[Dict] = []
        for _, row in data_frame.head(limit).iterrows():
            item = {
                "title": str(row.get("标题", "")).strip(),
                "content": str(row.get("内容", "")).strip(),
                "publish_date": str(row.get("发布日期", "")).strip(),
                "publish_time": str(row.get("发布时间", "")).strip(),
            }
            if not item["title"]:
                continue
            item["id"] = _build_news_id(
                {
                    "标题": item["title"],
                    "内容": item["content"],
                    "发布日期": item["publish_date"],
                    "发布时间": item["publish_time"],
                }
            )
            rows.append(classify_cls_news(item))
        return rows
    except Exception as e:
        logger.error(f"获取财联社快讯失败: {e}")
        return []


def poll_cls_telegraph(symbol: str = "全部", limit: int = 20) -> List[Dict]:
    """轮询财联社快讯并返回新增记录。"""
    rows = _fetch_cls_rows(symbol=symbol, limit=limit)
    if not rows:
        return []

    cache = _load_cls_cache()
    new_items: List[Dict] = []
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for item in rows:
        news_id = item["id"]
        if news_id in cache:
            continue
        cache[news_id] = {
            "title": item["title"],
            "content": item["content"],
            "publish_date": item["publish_date"],
            "publish_time": item["publish_time"],
            "first_seen_at": now_text,
            "level": item.get("level", "normal"),
            "level_label": item.get("level_label", "普通"),
            "matched_keywords": item.get("matched_keywords", []),
        }
        new_items.append(item)

    if cache:
        ordered_items = sorted(
            cache.items(),
            key=lambda pair: (
                str(pair[1].get("publish_date", "")),
                str(pair[1].get("publish_time", "")),
                str(pair[1].get("first_seen_at", "")),
            ),
            reverse=True,
        )
        trimmed_cache = dict(ordered_items[:200])
        _save_cls_cache(trimmed_cache)

    return new_items


def format_cls_news(items: List[Dict], limit: int = 5) -> str:
    """格式化财联社快讯文本。"""
    lines = ["【财联社快讯】"]
    for item in (items or [])[:limit]:
        time_text = f"{item.get('publish_date', '')} {item.get('publish_time', '')}".strip()
        title = str(item.get("title", "")).strip()
        level_label = str(item.get("level_label", "普通"))
        keywords = item.get("matched_keywords", []) or []
        content = str(item.get("content", "")).replace("\n", " ").strip()
        if len(content) > 120:
            content = f"{content[:120]}..."
        lines.append(f"- [{level_label}] {time_text} {title}")
        if keywords:
            lines.append(f"  关键词: {', '.join([str(keyword) for keyword in keywords[:4]])}")
        if content:
            lines.append(f"  {content}")

    if len(lines) == 1:
        lines.append("暂无最新财联社快讯")
    return "\n".join(lines)


@tool
def get_cls_telegraph_news(symbol: str = "全部", limit: int = 8) -> str:
    """
    获取财联社最新电报快讯。

    Args:
        symbol: 可选“全部”或“重点”
        limit: 返回条数

    Returns:
        财联社快讯文本
    """
    rows = _fetch_cls_rows(symbol=symbol, limit=limit)
    return format_cls_news(rows, limit=limit)
