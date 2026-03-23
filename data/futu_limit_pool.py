# -*- coding: utf-8 -*-
"""
Futu 近似涨跌停池工具
"""
from typing import Callable, Dict, Tuple

import pandas as pd


def get_limit_pct(code: str, name: str = "") -> float:
    """估算 A 股涨跌停比例"""
    text_code = str(code or "").strip()
    text_name = str(name or "").upper()
    if "ST" in text_name:
        return 0.05
    if text_code.startswith(("300", "301", "688", "689")):
        return 0.20
    if text_code.startswith(("4", "8")):
        return 0.30
    return 0.10


def get_recent_limit_streak(
    symbol: str,
    limit_pct: float,
    current_is_limit: bool,
    get_daily_bars: Callable[[str, int], pd.DataFrame],
) -> int:
    """根据日线近似估算连续涨停天数"""
    try:
        df = get_daily_bars(symbol, 15)
        if df is None or df.empty:
            return 1 if current_is_limit else 0

        work_df = df.copy()
        work_df.columns = [str(c).lower() for c in work_df.columns]
        if "change_rate" not in work_df.columns:
            return 1 if current_is_limit else 0

        streak = 1 if current_is_limit else 0
        threshold = limit_pct * 100 - 0.35
        for change in reversed(pd.to_numeric(work_df["change_rate"], errors="coerce").tolist()):
            if pd.notna(change) and float(change) >= threshold:
                streak += 1
            else:
                break
        return streak
    except Exception:
        return 1 if current_is_limit else 0


def build_limit_pool(
    snapshot: pd.DataFrame,
    get_streak: Callable[[str, float, bool], int],
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """基于快照近似构建涨停池、跌停池、炸板池"""
    if snapshot is None or snapshot.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df = snapshot.copy()
    if "code" not in df.columns or "last_price" not in df.columns or "prev_close_price" not in df.columns:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df["code"] = df["code"].astype(str)
    df["name"] = df.get("name", "").astype(str)
    df["change_rate"] = pd.to_numeric(df.get("change_rate"), errors="coerce")
    df["last_price"] = pd.to_numeric(df.get("last_price"), errors="coerce")
    df["prev_close_price"] = pd.to_numeric(df.get("prev_close_price"), errors="coerce")
    df["high_price"] = pd.to_numeric(df.get("high_price"), errors="coerce")
    df["turnover"] = pd.to_numeric(df.get("turnover"), errors="coerce").fillna(0.0)

    zt_records = []
    dt_records = []
    zb_records = []

    for _, row in df.iterrows():
        code = str(row.get("code", "") or "")
        name = str(row.get("name", "") or "")
        prev_close = float(row.get("prev_close_price", 0) or 0)
        last_price = float(row.get("last_price", 0) or 0)
        high_price = float(row.get("high_price", 0) or 0)
        change_rate = float(row.get("change_rate", 0) or 0)
        turnover = float(row.get("turnover", 0) or 0)
        if not code or prev_close <= 0 or last_price <= 0:
            continue

        limit_pct = get_limit_pct(code, name)
        up_limit = prev_close * (1 + limit_pct)
        down_limit = prev_close * (1 - limit_pct)
        up_hit = change_rate >= limit_pct * 100 - 0.35 or last_price >= up_limit * 0.997
        down_hit = change_rate <= -(limit_pct * 100 - 0.35) or last_price <= down_limit * 1.003
        break_hit = (high_price >= up_limit * 0.997) and (last_price < up_limit * 0.995)

        if up_hit:
            zt_records.append({
                "代码": code,
                "名称": name,
                "连板数": get_streak(code, limit_pct, True),
                "成交额": turnover,
                "炸板次数": 0,
            })
        if down_hit:
            dt_records.append({
                "代码": code,
                "名称": name,
                "成交额": turnover,
            })
        if break_hit:
            zb_records.append({
                "代码": code,
                "名称": name,
                "成交额": turnover,
            })

    return pd.DataFrame(zt_records), pd.DataFrame(dt_records), pd.DataFrame(zb_records)


def build_limit_status(
    row: pd.Series,
    get_order_book: Callable[[str, int], Dict[str, object]],
    get_streak: Callable[[str, float, bool], int],
) -> Dict[str, float]:
    """获取单只标的的涨停状态、封单强度和炸板近似值"""
    code = str(row.get("code", "") or "")
    name = str(row.get("name", "") or "")
    prev_close = float(pd.to_numeric(row.get("prev_close_price", 0), errors="coerce") or 0)
    last_price = float(pd.to_numeric(row.get("last_price", 0), errors="coerce") or 0)
    high_price = float(pd.to_numeric(row.get("high_price", 0), errors="coerce") or 0)
    turnover = float(pd.to_numeric(row.get("turnover", 0), errors="coerce") or 0)
    change_rate = float(pd.to_numeric(row.get("change_rate", 0), errors="coerce") or 0)
    if prev_close <= 0 or last_price <= 0:
        return {}

    limit_pct = get_limit_pct(code, name)
    up_limit = prev_close * (1 + limit_pct)
    is_limit_up = change_rate >= limit_pct * 100 - 0.35 or last_price >= up_limit * 0.997
    break_count = 1 if (high_price >= up_limit * 0.997 and last_price < up_limit * 0.995) else 0

    seal_amount = 0.0
    if is_limit_up:
        order_book = get_order_book(code, 5)
        bids = order_book.get("bid", []) if isinstance(order_book, dict) else []
        seal_amount = sum(
            float(item.get("price", 0) or 0) * float(item.get("volume", 0) or 0)
            for item in bids
        )

    seal_ratio = seal_amount / turnover if turnover > 0 else 0.0
    return {
        "is_limit_up": float(1 if is_limit_up else 0),
        "continuous_limit_days": float(get_streak(code, limit_pct, is_limit_up)),
        "seal_amount": float(seal_amount),
        "turnover": float(turnover),
        "break_count": float(break_count),
        "seal_ratio": float(seal_ratio),
    }
