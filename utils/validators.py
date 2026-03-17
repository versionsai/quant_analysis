# -*- coding: utf-8 -*-
"""
数据验证工具
"""
import pandas as pd
from typing import List, Optional


def validate_stock_code(code: str) -> bool:
    """验证A股股票代码"""
    if not code:
        return False
    code = str(code).zfill(6)
    return code.isdigit() and (code.startswith("6") or 
                                code.startswith("0") or 
                                code.startswith("3"))


def validate_date_range(start_date: str, end_date: str) -> bool:
    """验证日期范围"""
    try:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        return start <= end
    except:
        return False


def validate_price(price: float) -> bool:
    """验证价格"""
    return price > 0 and price < 10000


def validate_volume(volume: int) -> bool:
    """验证成交量"""
    return volume >= 0


def check_dataframe_columns(df: pd.DataFrame, required_cols: List[str]) -> bool:
    """检查DataFrame必要列"""
    return all(col in df.columns for col in required_cols)
