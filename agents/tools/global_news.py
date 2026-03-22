# -*- coding: utf-8 -*-
"""
全球金融新闻工具
优先使用财联社快讯与本地缓存，避免外部行情接口阻塞
"""
import json
import os
from datetime import datetime
from typing import Any, Dict, List

from langchain_core.tools import tool

from agents.tools.cls_news import _fetch_cls_rows
from utils.logger import get_logger

logger = get_logger(__name__)


def _get_us_market_cache_path() -> str:
    """
    获取美股缓存路径
    """
    cache_dir = os.environ.get("QUANT_CACHE_DIR", "./runtime/data")
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, "us_market_cache.json")


def _load_us_market_cache() -> Dict[str, Any]:
    """
    读取美股缓存
    """
    cache_path = _get_us_market_cache_path()
    if not os.path.exists(cache_path):
        return {}
    try:
        with open(cache_path, "r", encoding="utf-8") as file:
            data = json.load(file)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning(f"读取美股缓存失败: {e}")
        return {}


def _format_cached_us_market(cache: Dict[str, Any]) -> List[str]:
    """
    格式化缓存的美股信息
    """
    lines: List[str] = []
    items = cache.get("data", []) if isinstance(cache, dict) else []
    for item in items[:4]:
        code = str(item.get("code", "")).strip()
        price = item.get("price", "")
        change_pct = item.get("change_pct", "")
        if not code:
            continue
        if isinstance(change_pct, (int, float)):
            lines.append(f"  {code}: {price} ({float(change_pct):+.2f}%)")
        else:
            lines.append(f"  {code}: {price}")
    return lines


@tool
def get_global_finance_news() -> str:
    """
    获取全球金融市场动态，优先走财联社快讯和本地缓存。

    Returns:
        str: 全球金融资讯汇总
    """
    try:
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M")
        lines: List[str] = ["【全球金融市场动态】", "", f"时间: {now_text}", ""]

        us_cache = _load_us_market_cache()
        us_lines = _format_cached_us_market(us_cache)
        if us_lines:
            lines.append("【隔夜美股缓存】")
            lines.extend(us_lines)
            fetch_time = str(us_cache.get("fetch_time", "")).strip()
            if fetch_time:
                lines.append(f"  缓存时间: {fetch_time}")
            lines.append("")

        cls_rows = _fetch_cls_rows(symbol="全部", limit=12)
        overseas_rows = [row for row in cls_rows if str(row.get("category", "")) == "overseas"]
        macro_rows = [row for row in cls_rows if str(row.get("category", "")) in {"macro", "regulation"}]

        if overseas_rows:
            lines.append("【财联社·海外冲击】")
            for item in overseas_rows[:4]:
                time_text = f"{item.get('publish_date', '')} {item.get('publish_time', '')}".strip()
                title = str(item.get("title", "")).strip()
                level_label = str(item.get("level_label", "普通")).strip()
                content = str(item.get("content", "")).replace("\n", " ").strip()
                lines.append(f"- [{level_label}] {time_text} {title}")
                if content:
                    lines.append(f"  {content}")
            lines.append("")

        if macro_rows:
            lines.append("【财联社·宏观监管】")
            for item in macro_rows[:3]:
                time_text = f"{item.get('publish_date', '')} {item.get('publish_time', '')}".strip()
                category_label = str(item.get("category_label", "其他")).strip()
                title = str(item.get("title", "")).strip()
                lines.append(f"- [{category_label}] {time_text} {title}")
            lines.append("")

        if len(lines) <= 4:
            lines.extend([
                "【全球资讯补充】",
                "- 暂无新的财联社海外快讯，建议结合盘中指数与商品波动判断风险偏好。",
            ])

        return "\n".join(lines)
    except Exception as e:
        logger.error(f"获取全球金融资讯失败: {e}")
        return f"获取全球金融资讯失败: {str(e)}"
