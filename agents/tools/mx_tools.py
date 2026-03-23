# -*- coding: utf-8 -*-
"""
妙想数据工具
"""
import json
from pathlib import Path
from typing import Any, Dict, List

from langchain_core.tools import tool

from agents.tools.mx_common import ensure_mx_api_key, load_mx_module, run_async
from utils.logger import get_logger

logger = get_logger(__name__)


def _clean_text(value: Any) -> str:
    """
    清洗文本，去掉多余换行和连续空白。
    """
    text = str(value or "").replace("\r", "\n")
    text = " ".join(part.strip() for part in text.splitlines() if part.strip())
    while "  " in text:
        text = text.replace("  ", " ")
    return text.strip()


def _extract_news_items(value: Any) -> List[Dict[str, Any]]:
    """
    从 JSON 结构中提取资讯列表。
    """
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if not isinstance(value, dict):
        return []

    for key in ("data", "result", "list", "items"):
        nested = value.get(key)
        if isinstance(nested, list):
            return [item for item in nested if isinstance(item, dict)]
        if isinstance(nested, dict):
            nested_items = _extract_news_items(nested)
            if nested_items:
                return nested_items
    return []


def _format_news_items(items: List[Dict[str, Any]], max_items: int = 6) -> str:
    """
    将资讯列表格式化为摘要条目。
    """
    lines: List[str] = []
    for index, item in enumerate(items[:max_items], 1):
        title = _clean_text(item.get("title", "")) or "未命名资讯"
        source = _clean_text(item.get("source", "") or item.get("insName", "")) or "未知来源"
        date_text = _clean_text(item.get("date", ""))[:19]
        content = _clean_text(item.get("content", ""))

        meta = " | ".join([part for part in [source, date_text] if part])
        lines.append(f"{index}. {title}" if not meta else f"{index}. {title} ({meta})")
        if content:
            lines.append(f"   {content}")
    return "\n".join(lines).strip()


def summarize_mx_news_text(raw_text: str, max_items: int = 6) -> str:
    """
    将妙想返回的原始文本尽量提炼为可读摘要。
    兼容直接返回 JSON 字符串或 data/result 包裹结构。
    """
    text = str(raw_text or "").strip()
    if not text:
        return ""

    stripped = text.strip()
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            parsed = json.loads(stripped)
            items = _extract_news_items(parsed)
            if items:
                formatted = _format_news_items(items, max_items=max_items)
                if formatted:
                    return formatted
        except Exception:
            pass

    return text


def _require_mx_key() -> None:
    """
    校验妙想 API Key 是否可用。
    """
    if not ensure_mx_api_key():
        raise ValueError("EM_API_KEY 未配置，无法使用妙想 Skills")


@tool
def mx_search_financial_news(query: str) -> str:
    """
    使用妙想搜索最新公告、研报、新闻与政策信息。
    """
    try:
        _require_mx_key()
        module = load_mx_module(
            "mx_finance_search_runtime",
            "skills/mx-finance-search/scripts/get_data.py",
        )
        result = run_async(
            module.query_financial_news(
                query=query,
                output_dir=Path("runtime/mx_finance_search_agent"),
                save_to_file=True,
            )
        )
        if "error" in result:
            return f"妙想资讯搜索失败: {result['error']}"

        content = summarize_mx_news_text(str(result.get("content", "") or "").strip())
        if not content:
            return "暂无相关妙想资讯"
        return content[:4000]
    except Exception as e:
        logger.error(f"妙想资讯搜索失败: {e}")
        return f"妙想资讯搜索失败: {e}"


@tool
def mx_query_financial_data(query: str) -> str:
    """
    使用妙想查询结构化金融数据。
    """
    try:
        _require_mx_key()
        module = load_mx_module(
            "mx_finance_data_runtime",
            "skills/mx-finance-data/scripts/get_data.py",
        )
        result = run_async(
            module.query_mx_finance_data(
                query=query,
                output_dir=Path("runtime/mx_finance_data_agent"),
            )
        )
        if "error" in result:
            return f"妙想金融数据查询失败: {result['error']}"

        lines = [
            "【妙想金融数据】",
            f"查询: {query}",
            f"数据文件: {result.get('file_path') or result.get('csv_path')}",
            f"说明文件: {result.get('description_path')}",
            f"行数: {result.get('row_count', 0)}",
        ]
        if result.get("message"):
            lines.append(f"提示: {result['message']}")
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"妙想金融数据查询失败: {e}")
        return f"妙想金融数据查询失败: {e}"


@tool
def mx_query_macro_data(query: str) -> str:
    """
    使用妙想查询宏观经济数据。
    """
    try:
        _require_mx_key()
        module = load_mx_module(
            "mx_macro_data_runtime",
            "skills/mx-macro-data/scripts/get_data.py",
        )
        result = run_async(
            module.query_mx_macro_data(
                query=query,
                output_dir=Path("runtime/mx_macro_data_agent"),
            )
        )
        if "error" in result:
            return f"妙想宏观数据查询失败: {result['error']}"

        csv_paths = result.get("csv_paths") or []
        lines = [
            "【妙想宏观数据】",
            f"查询: {query}",
            f"数据文件: {', '.join([str(path) for path in csv_paths]) if csv_paths else '无'}",
            f"说明文件: {result.get('description_path')}",
            f"行数统计: {result.get('row_counts', {})}",
        ]
        if result.get("message"):
            lines.append(f"提示: {result['message']}")
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"妙想宏观数据查询失败: {e}")
        return f"妙想宏观数据查询失败: {e}"


@tool
def mx_screen_securities(query: str, select_type: str) -> str:
    """
    使用妙想执行选股/选ETF/选板块筛选。
    """
    try:
        _require_mx_key()
        module = load_mx_module(
            "mx_stocks_screener_runtime",
            "skills/mx-stocks-screener/scripts/get_data.py",
        )
        result = run_async(
            module.query_mx_stocks_screener(
                query=query,
                selectType=select_type,
                output_dir=Path("runtime/mx_stocks_screener_agent"),
            )
        )
        if "error" in result:
            return f"妙想选股筛选失败: {result['error']}"

        return "\n".join(
            [
                "【妙想选股筛选】",
                f"查询: {query}",
                f"类型: {select_type}",
                f"数据文件: {result.get('csv_path')}",
                f"说明文件: {result.get('description_path')}",
                f"结果数量: {result.get('row_count', 0)}",
            ]
        )
    except Exception as e:
        logger.error(f"妙想选股筛选失败: {e}")
        return f"妙想选股筛选失败: {e}"
