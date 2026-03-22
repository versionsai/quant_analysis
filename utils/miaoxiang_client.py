# -*- coding: utf-8 -*-
"""
妙想技能数据访问客户端
"""
import asyncio
import importlib.util
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from utils.logger import get_logger

logger = get_logger(__name__)


def ensure_mx_api_key() -> str:
    """
    确保妙想 API Key 已加载到环境变量。
    """
    for env_name in ("EM_API_KEY", "MIAOXIANG_EM_API_KEY", "GITEA_SECRET_EM_API_KEY", "GITEA_EM_API_KEY"):
        value = str(os.environ.get(env_name, "") or "").strip()
        if value:
            os.environ["EM_API_KEY"] = value
            return value

    root = Path(__file__).resolve().parents[1]
    for env_name in (".env.local", ".env"):
        env_path = root / env_name
        if not env_path.exists():
            continue
        try:
            for raw_line in env_path.read_text(encoding="utf-8").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                if key.strip() != "EM_API_KEY":
                    continue
                value = value.strip().strip('"').strip("'")
                if value:
                    os.environ["EM_API_KEY"] = value
                    return value
        except Exception as e:
            logger.warning(f"读取妙想环境配置失败 {env_path}: {e}")

    return ""


def load_mx_module(module_name: str, relative_script_path: str) -> Any:
    """
    按脚本路径动态加载妙想模块。
    """
    ensure_mx_api_key()
    root = Path(__file__).resolve().parents[1]
    module_path = root / relative_script_path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载妙想脚本: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_async(coro: Any) -> Any:
    """
    在同步上下文中运行异步协程。
    """
    return asyncio.run(coro)


def _load_excel_first_sheet(file_path: str) -> pd.DataFrame:
    """
    读取妙想结构化数据输出的首个 Sheet。
    """
    try:
        excel_file = pd.ExcelFile(file_path)
        if not excel_file.sheet_names:
            return pd.DataFrame()
        return pd.read_excel(file_path, sheet_name=excel_file.sheet_names[0])
    except Exception as e:
        logger.warning(f"读取妙想 Excel 失败 {file_path}: {e}")
        return pd.DataFrame()


def _sheet_to_key_value_dict(df: pd.DataFrame) -> Dict[str, str]:
    """
    将两列表格尽量转换为键值对字典。
    """
    if df is None or df.empty or len(df.columns) < 2:
        return {}

    key_col = df.columns[0]
    value_col = df.columns[1]
    result: Dict[str, str] = {}

    for _, row in df.iterrows():
        key = str(row.get(key_col, "") or "").strip()
        value = str(row.get(value_col, "") or "").strip()
        if key:
            result[key] = value
    return result


def _find_column(df: pd.DataFrame, keywords: List[str]) -> Optional[str]:
    """
    根据关键词模糊匹配列名。
    """
    if df is None or df.empty:
        return None

    columns = [str(column).strip() for column in df.columns]
    for keyword in keywords:
        for column in columns:
            if keyword in column:
                return column
    return None


def search_financial_news(query: str, output_dir: str = "runtime/mx_finance_search_service") -> str:
    """
    使用妙想搜索金融资讯并返回文本结果。
    """
    try:
        if not ensure_mx_api_key():
            return ""
        module = load_mx_module(
            "mx_finance_search_service_runtime",
            "skills/mx-finance-search/scripts/get_data.py",
        )
        result = run_async(
            module.query_financial_news(
                query=query,
                output_dir=Path(output_dir),
                save_to_file=False,
            )
        )
        if not isinstance(result, dict) or result.get("error"):
            return ""
        return str(result.get("content", "") or "").strip()
    except Exception as e:
        logger.warning(f"妙想资讯搜索失败 {query}: {e}")
        return ""


def query_financial_data_dict(
    query: str,
    output_dir: str = "runtime/mx_finance_data_service",
) -> Dict[str, str]:
    """
    使用妙想查询结构化金融数据并尽量转换为键值对。
    """
    try:
        if not ensure_mx_api_key():
            return {}
        module = load_mx_module(
            "mx_finance_data_service_runtime",
            "skills/mx-finance-data/scripts/get_data.py",
        )
        result = run_async(
            module.query_mx_finance_data(
                query=query,
                output_dir=Path(output_dir),
            )
        )
        if not isinstance(result, dict) or result.get("error"):
            return {}

        file_path = str(result.get("file_path") or result.get("csv_path") or "").strip()
        if not file_path:
            return {}

        df = _load_excel_first_sheet(file_path)
        return _sheet_to_key_value_dict(df)
    except Exception as e:
        logger.warning(f"妙想结构化数据查询失败 {query}: {e}")
        return {}


def query_financial_data_frame(
    query: str,
    output_dir: str = "runtime/mx_finance_data_service",
) -> pd.DataFrame:
    """
    使用妙想查询结构化金融数据并返回首个 Sheet DataFrame。
    """
    try:
        if not ensure_mx_api_key():
            return pd.DataFrame()
        module = load_mx_module(
            "mx_finance_data_frame_runtime",
            "skills/mx-finance-data/scripts/get_data.py",
        )
        result = run_async(
            module.query_mx_finance_data(
                query=query,
                output_dir=Path(output_dir),
            )
        )
        if not isinstance(result, dict) or result.get("error"):
            return pd.DataFrame()

        file_path = str(result.get("file_path") or result.get("csv_path") or "").strip()
        if not file_path:
            return pd.DataFrame()

        return _load_excel_first_sheet(file_path)
    except Exception as e:
        logger.warning(f"妙想结构化数据 DataFrame 查询失败 {query}: {e}")
        return pd.DataFrame()


def screen_securities_frame(
    query: str,
    select_type: str,
    output_dir: str = "runtime/mx_stocks_screener_service",
) -> pd.DataFrame:
    """
    使用妙想筛股并返回 CSV 结果。
    """
    try:
        if not ensure_mx_api_key():
            return pd.DataFrame()
        module = load_mx_module(
            "mx_stocks_screener_service_runtime",
            "skills/mx-stocks-screener/scripts/get_data.py",
        )
        result = run_async(
            module.query_mx_stocks_screener(
                query=query,
                selectType=select_type,
                output_dir=Path(output_dir),
            )
        )
        if not isinstance(result, dict) or result.get("error"):
            return pd.DataFrame()

        csv_path = str(result.get("csv_path") or "").strip()
        if not csv_path:
            return pd.DataFrame()

        return pd.read_csv(csv_path)
    except Exception as e:
        logger.warning(f"妙想筛股失败 {query}: {e}")
        return pd.DataFrame()


def parse_price_fields(df: pd.DataFrame) -> Dict[str, Optional[float]]:
    """
    从妙想筛股结果中提取常见价格字段。
    """
    if df is None or df.empty:
        return {}

    row = df.iloc[0]
    price_col = _find_column(df, ["最新价", "现价", "收盘价"])
    change_col = _find_column(df, ["涨跌幅", "涨幅"])
    market_cap_col = _find_column(df, ["总市值", "流通市值"])
    pe_col = _find_column(df, ["市盈率"])
    pb_col = _find_column(df, ["市净率"])

    return {
        "price": row.get(price_col) if price_col else None,
        "change_pct": row.get(change_col) if change_col else None,
        "market_cap": row.get(market_cap_col) if market_cap_col else None,
        "pe": row.get(pe_col) if pe_col else None,
        "pb": row.get(pb_col) if pb_col else None,
    }
