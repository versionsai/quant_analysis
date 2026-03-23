# -*- coding: utf-8 -*-
"""
运行时策略模式配置
"""
import os
from datetime import datetime
from typing import Dict, Optional

from data.recommend_db import RecommendDB

RUNTIME_SETTINGS_CACHE_KEY = "runtime_settings"
DEFAULT_MARKET_REGIME_MODE = "auto"
MARKET_REGIME_MODE_OPTIONS = {
    "auto": {
        "label": "自动",
        "description": "根据指数与情绪自动在正常、防守、黄金坑之间切换。",
    },
    "normal": {
        "label": "正常",
        "description": "按常规信号运行，适合正常波动环境。",
    },
    "defense": {
        "label": "防守",
        "description": "只保留抱团核心和更强承接，明显收紧买入。",
    },
    "golden_pit": {
        "label": "黄金坑",
        "description": "只保留恐慌后放量修复的机会，防止把普通反抽误判成机会。",
    },
}


def normalize_market_regime_mode(mode: str) -> str:
    """
    规范化市场运行模式。
    """
    raw_mode = str(mode or "").strip().lower()
    if raw_mode in MARKET_REGIME_MODE_OPTIONS:
        return raw_mode
    return str(os.environ.get("MARKET_REGIME_MODE", DEFAULT_MARKET_REGIME_MODE) or DEFAULT_MARKET_REGIME_MODE).strip().lower() \
        if str(os.environ.get("MARKET_REGIME_MODE", DEFAULT_MARKET_REGIME_MODE) or "").strip().lower() in MARKET_REGIME_MODE_OPTIONS \
        else DEFAULT_MARKET_REGIME_MODE


def get_runtime_settings(db_path: Optional[str] = None) -> Dict[str, str]:
    """
    获取运行时配置。
    """
    path = db_path or os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db")
    default_mode = normalize_market_regime_mode(os.environ.get("MARKET_REGIME_MODE", DEFAULT_MARKET_REGIME_MODE))
    default_option = MARKET_REGIME_MODE_OPTIONS.get(default_mode, MARKET_REGIME_MODE_OPTIONS[DEFAULT_MARKET_REGIME_MODE])
    try:
        db = RecommendDB(path)
        payload = db.get_dashboard_cache(RUNTIME_SETTINGS_CACHE_KEY) or {}
    except Exception:
        payload = {}

    mode = normalize_market_regime_mode(payload.get("market_regime_mode", default_mode))
    option = MARKET_REGIME_MODE_OPTIONS.get(mode, default_option)
    updated_at = str(payload.get("updated_at", "") or "")
    return {
        "market_regime_mode": mode,
        "market_regime_label": option.get("label", mode),
        "market_regime_description": option.get("description", ""),
        "updated_at": updated_at,
    }


def save_runtime_settings(db_path: Optional[str], market_regime_mode: str) -> Dict[str, str]:
    """
    保存运行时配置。
    """
    path = db_path or os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db")
    mode = normalize_market_regime_mode(market_regime_mode)
    payload = {
        "market_regime_mode": mode,
        "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    db = RecommendDB(path)
    db.set_dashboard_cache(RUNTIME_SETTINGS_CACHE_KEY, payload)
    return get_runtime_settings(path)
