# -*- coding: utf-8 -*-
"""
动态参数工具
读取和设置动态参数
"""
from typing import Dict, Optional
from langchain_core.tools import tool

from data.recommend_db import DynamicParamsDB
from utils.logger import get_logger

logger = get_logger(__name__)


@tool
def get_current_params() -> Dict:
    """
    读取当前动态参数
    
    Returns:
        dict: 所有动态参数及其值
    """
    dp_db = DynamicParamsDB()
    params = dp_db.get_all_params()
    
    result = {}
    for key, data in params.items():
        result[key] = data.get("value")
    
    return result


@tool
def get_param(key: str, default: Optional[float] = None) -> float:
    """
    读取单个动态参数
    
    Args:
        key: 参数名
        default: 默认值
    
    Returns:
        float: 参数值
    """
    dp_db = DynamicParamsDB()
    return dp_db.get_param(key, default)


@tool
def set_param(
    key: str,
    value: float,
    reason: str = "",
    source: str = "manual",
) -> Dict:
    """
    设置动态参数
    
    Args:
        key: 参数名
        value: 参数值
        reason: 变更原因
        source: 来源 (manual/optimizer)
    
    Returns:
        dict: {
            success: bool,
            key: str,
            old_value: float,
            new_value: float
        }
    """
    dp_db = DynamicParamsDB()
    old_value = dp_db.get_param(key)
    
    success = dp_db.set_param(key, value, reason, source)
    
    return {
        "success": success,
        "key": key,
        "old_value": old_value,
        "new_value": value,
        "reason": reason,
        "source": source,
    }


@tool
def get_param_history(key: str, limit: int = 10) -> list:
    """
    读取参数调整历史
    
    Args:
        key: 参数名
        limit: 返回条数
    
    Returns:
        list: 参数历史
    """
    from data.recommend_db import get_db
    import json
    
    db = get_db()
    conn = db._get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT param_value, change_reason, source, updated_at
        FROM dynamic_params
        WHERE param_key = ?
        ORDER BY updated_at DESC
        LIMIT ?
    """, (key, limit))
    
    results = []
    for row in cursor.fetchall():
        results.append({
            "value": row["param_value"],
            "reason": row["change_reason"],
            "source": row["source"],
            "updated_at": row["updated_at"],
        })
    
    conn.close()
    return results


@tool
def reset_params_to_default() -> Dict:
    """
    重置所有参数到默认值
    
    Returns:
        dict: 重置结果
    """
    dp_db = DynamicParamsDB()
    defaults = DynamicParamsDB.DEFAULT_PARAMS
    
    results = {}
    for key, value in defaults.items():
        dp_db.set_param(key, value, "重置到默认值", "system")
        results[key] = value
    
    return {
        "success": True,
        "reset_params": results,
    }
