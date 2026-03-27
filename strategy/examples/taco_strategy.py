# -*- coding: utf-8 -*-
"""
TACO 事件修复策略
"""
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd

from data.recommend_db import RecommendDB
from strategy.base import BaseStrategy, Signal


FIXED_EVENT_WINDOW_DAYS = 30

TACO_KEYWORDS: Dict[str, float] = {
    "特朗普": 1.9,
    "Trump": 1.9,
    "trump": 1.9,
    "关税": 1.7,
    "tariff": 1.7,
    "trade war": 1.5,
    "贸易战": 1.5,
    "对华": 1.3,
    "中美": 1.3,
    "制裁": 1.2,
    "豁免": 1.1,
    "谈判": 1.0,
    "加征": 1.2,
    "升级": 1.0,
    "缓和": 0.9,
    "暂缓": 0.9,
    "对等关税": 1.8,
    "reciprocal tariff": 1.8,
    "行政令": 1.2,
    "executive order": 1.2,
    "出口管制": 1.4,
    "export control": 1.4,
    "301调查": 1.2,
    "制裁清单": 1.3,
    "豁免清单": 1.2,
    "停火": 1.0,
    "ceasefire": 1.0,
    "truth social": 1.5,
    "海湖庄园": 1.2,
    "降息施压": 1.0,
    "fed pressure": 1.0,
}

HOT_TOPIC_KEYWORDS: Dict[str, float] = {
    "原油": 1.4,
    "油价": 1.3,
    "石油": 1.2,
    "中东冲突": 1.8,
    "霍尔木兹海峡": 2.2,
    "Hormuz": 2.2,
    "Strait of Hormuz": 2.4,
    "Brent": 1.5,
    "WTI": 1.4,
    "OPEC+": 1.4,
    "伊朗": 1.5,
    "Iran": 1.5,
    "以色列": 1.4,
    "Israel": 1.4,
    "地缘风险": 1.3,
    "supply disruption": 1.6,
    "crude oil": 1.5,
    "半导体": 1.2,
    "芯片": 1.2,
    "AI": 1.0,
    "人工智能": 1.0,
    "算力": 1.0,
    "稀土": 1.2,
    "军工": 1.1,
    "黄金": 1.1,
    "航运": 1.0,
    "医药": 0.9,
}

TACO_OIL_KEYWORDS: Dict[str, float] = {
    **HOT_TOPIC_KEYWORDS,
    **TACO_KEYWORDS,
    "Middle East": 1.8,
    "oil spike": 1.7,
    "oil surge": 1.7,
    "Brent crude": 1.8,
    "WTI crude": 1.6,
    "油气": 1.2,
    "Red Sea": 1.2,
    "shipping lane": 1.3,
    "tanker": 1.2,
    "pipeline": 1.1,
    "refinery": 1.1,
    "missile strike": 1.6,
    "drone strike": 1.5,
    "geopolitical risk": 1.4,
}

CLS_LEVEL_WEIGHT: Dict[str, float] = {
    "critical": 1.6,
    "important": 1.15,
    "normal": 0.7,
}

CLS_CATEGORY_WEIGHT: Dict[str, float] = {
    "overseas": 1.25,
    "macro": 1.15,
    "regulation": 1.0,
    "industry": 0.95,
    "stock_event": 0.7,
    "other": 0.6,
}

VARIANT_SETTINGS: Dict[str, Dict[str, object]] = {
    "taco": {
        "display_name": "TACO",
        "keywords": {**TACO_KEYWORDS, **HOT_TOPIC_KEYWORDS},
        "calendar_path": "./config/taco_event_calendar.json",
        "event_score_threshold": 0.20,
        "shock_drop_pct": -0.035,
        "volume_ratio_threshold": 1.05,
        "live_news_lookback_days": FIXED_EVENT_WINDOW_DAYS,
        "live_news_decay_days": FIXED_EVENT_WINDOW_DAYS,
        "event_window_days": FIXED_EVENT_WINDOW_DAYS,
        "prefer_funds": True,
    },
    "taco_oil": {
        "display_name": "TACO-OIL",
        "keywords": TACO_OIL_KEYWORDS,
        "calendar_path": "./config/taco_oil_event_calendar.json",
        "event_score_threshold": 0.24,
        "shock_drop_pct": -0.025,
        "volume_ratio_threshold": 1.02,
        "live_news_lookback_days": FIXED_EVENT_WINDOW_DAYS,
        "live_news_decay_days": FIXED_EVENT_WINDOW_DAYS,
        "event_window_days": FIXED_EVENT_WINDOW_DAYS,
        "prefer_funds": True,
    },
}


@dataclass
class TacoEvent:
    """事件锚点"""

    label: str
    anchor_date: str
    impact_score: float = 1.0
    base_window_days: int = FIXED_EVENT_WINDOW_DAYS
    max_window_days: int = FIXED_EVENT_WINDOW_DAYS
    pre_window_days: int = 0
    keywords: List[str] = field(default_factory=list)
    note: str = ""


@dataclass
class TacoEventSignal:
    """事件信号"""

    score: float = 0.0
    label: str = ""
    source: str = ""
    window_days: int = 0
    matched_keywords: List[str] = field(default_factory=list)
    reason: str = ""


@dataclass
class TacoStrategyParams:
    """TACO 策略参数"""

    variant: str = "taco"
    display_name: str = "TACO"
    shock_drop_pct: float = -0.035
    rebound_lookback: int = 5
    panic_lookback: int = 8
    volume_ratio_threshold: float = 1.15
    ema_fast: int = 5
    ema_slow: int = 10
    profit_target_pct: float = 0.06
    stop_loss_pct: float = -0.03
    news_filter_enabled: bool = True
    event_score_threshold: float = 0.25
    live_news_lookback_days: int = FIXED_EVENT_WINDOW_DAYS
    live_news_decay_days: int = FIXED_EVENT_WINDOW_DAYS
    event_window_days: int = FIXED_EVENT_WINDOW_DAYS
    calendar_path: str = "./config/taco_event_calendar.json"
    db_path: str = "./runtime/data/recommend.db"
    cls_cache_path: str = "./runtime/data/cls_telegraph_cache.json"
    prefer_funds: bool = True
    keywords: Dict[str, float] = field(default_factory=lambda: dict(TACO_KEYWORDS))


def build_taco_params(variant: str = "taco", overrides: Optional[Dict[str, object]] = None) -> TacoStrategyParams:
    """
    构建指定变体参数
    """
    settings = dict(VARIANT_SETTINGS.get(str(variant or "taco"), VARIANT_SETTINGS["taco"]))
    params = TacoStrategyParams(
        variant=str(variant or "taco"),
        display_name=str(settings.get("display_name", "TACO")),
        shock_drop_pct=float(settings.get("shock_drop_pct", -0.035)),
        volume_ratio_threshold=float(settings.get("volume_ratio_threshold", 1.15)),
        event_score_threshold=float(settings.get("event_score_threshold", 0.25)),
        live_news_lookback_days=int(settings.get("live_news_lookback_days", FIXED_EVENT_WINDOW_DAYS)),
        live_news_decay_days=int(settings.get("live_news_decay_days", FIXED_EVENT_WINDOW_DAYS)),
        event_window_days=int(settings.get("event_window_days", FIXED_EVENT_WINDOW_DAYS)),
        calendar_path=str(settings.get("calendar_path", "./config/taco_event_calendar.json")),
        prefer_funds=bool(settings.get("prefer_funds", True)),
        keywords=dict(settings.get("keywords", TACO_KEYWORDS)),
    )
    for key, value in (overrides or {}).items():
        if hasattr(params, key):
            setattr(params, key, value)
    return params


class TacoNewsEventFilter:
    """事件过滤器"""

    def __init__(self, params: TacoStrategyParams):
        self.params = params
        self._calendar_cache: List[TacoEvent] = []
        self._calendar_mtime: float = -1.0
        self._cls_items_cache: Optional[List[Dict[str, object]]] = None
        self._news_brief_items_cache: Optional[List[Dict[str, object]]] = None

    def get_event_signal(self, trade_date: datetime) -> TacoEventSignal:
        """
        生成事件信号
        """
        if not self.params.news_filter_enabled:
            return TacoEventSignal(score=1.0, label="disabled", source="disabled", reason="事件过滤已关闭")

        calendar_signal = self._compute_calendar_signal(trade_date)
        live_signal = self._compute_live_news_signal(trade_date)
        dominant = live_signal if live_signal.score >= calendar_signal.score else calendar_signal
        follower = calendar_signal if dominant is live_signal else live_signal
        merged_score = min(dominant.score + follower.score * 0.35, 3.0)
        return TacoEventSignal(
            score=merged_score,
            label=dominant.label or follower.label,
            source=dominant.source or follower.source,
            window_days=max(dominant.window_days, follower.window_days),
            matched_keywords=dominant.matched_keywords or follower.matched_keywords,
            reason=dominant.reason or follower.reason,
        )

    def get_hot_topics(self, trade_date: datetime, limit: int = 8) -> List[Dict[str, object]]:
        """
        获取最近窗口内的热点摘要
        """
        topic_scores: Dict[str, Dict[str, object]] = {}
        self._ensure_calendar_loaded()

        for event in self._calendar_cache:
            anchor_dt = self._parse_date(event.anchor_date)
            if anchor_dt is None:
                continue
            days_delta = (trade_date.date() - anchor_dt.date()).days
            if days_delta < -int(event.pre_window_days) or days_delta > int(self.params.event_window_days):
                continue
            score = float(event.impact_score) * max(0.2, 1.0 - max(days_delta, 0) / max(int(self.params.event_window_days), 1))
            label = str(event.label or "calendar")
            topic_scores[label] = {
                "name": label,
                "score": round(score, 3),
                "source": "calendar",
                "date": anchor_dt.strftime("%Y-%m-%d"),
                "keywords": list(event.keywords)[:6],
                "reason": str(event.note or "calendar event"),
            }

        for item in self._collect_live_news_items()[:80]:
            published_at = self._parse_datetime(str(item.get("published_at", "") or ""))
            if published_at is None:
                continue
            days_delta = (trade_date.date() - published_at.date()).days
            if days_delta < 0 or days_delta > int(self.params.live_news_lookback_days):
                continue
            keyword_info = self._calc_keyword_score(item)
            score = float(keyword_info.get("score", 0.0) or 0.0)
            if score <= 0:
                continue
            recency_factor = max(0.25, 1.0 - days_delta / max(int(self.params.live_news_decay_days), 1))
            level_weight = CLS_LEVEL_WEIGHT.get(str(item.get("level", "normal")).lower(), 0.8)
            category_weight = CLS_CATEGORY_WEIGHT.get(str(item.get("category", "other")).lower(), 0.7)
            final_score = round(score * recency_factor * level_weight * category_weight, 3)
            title = str(item.get("title", "") or "").strip() or "live_news"
            topic_scores[title] = {
                "name": title[:80],
                "score": final_score,
                "source": "live_news",
                "date": published_at.strftime("%Y-%m-%d"),
                "keywords": list(keyword_info.get("matched_keywords", []))[:6],
                "reason": str(item.get("content", "") or "")[:120],
            }

        rows = sorted(topic_scores.values(), key=lambda item: float(item.get("score", 0.0) or 0.0), reverse=True)
        return rows[: max(1, int(limit))]

    def _compute_calendar_signal(self, trade_date: datetime) -> TacoEventSignal:
        self._ensure_calendar_loaded()
        best_signal = TacoEventSignal()

        for event in self._calendar_cache:
            anchor_dt = self._parse_date(event.anchor_date)
            if anchor_dt is None:
                continue

            days_delta = (trade_date.date() - anchor_dt.date()).days
            if days_delta < -int(event.pre_window_days) or days_delta > int(self.params.event_window_days):
                continue

            decay = max(0.2, 1.0 - max(days_delta, 0) / max(int(self.params.event_window_days), 1))
            score = float(event.impact_score) * decay
            if score > best_signal.score:
                best_signal = TacoEventSignal(
                    score=score,
                    label=event.label,
                    source="calendar",
                    window_days=int(self.params.event_window_days),
                    matched_keywords=list(event.keywords),
                    reason=f"事件锚点 {event.label} 仍在 30 天窗口内",
                )
        return best_signal

    def _compute_live_news_signal(self, trade_date: datetime) -> TacoEventSignal:
        items = self._collect_live_news_items()
        if not items:
            return TacoEventSignal()

        total_score = 0.0
        top_label = ""
        top_reason = ""
        top_keywords: List[str] = []
        top_score = 0.0

        for item in items[:80]:
            published_at = self._parse_datetime(str(item.get("published_at", "") or ""))
            if published_at is None:
                continue

            days_delta = (trade_date.date() - published_at.date()).days
            if days_delta < 0 or days_delta > int(self.params.live_news_lookback_days):
                continue

            keyword_info = self._calc_keyword_score(item)
            keyword_score = float(keyword_info.get("score", 0.0) or 0.0)
            if keyword_score <= 0:
                continue

            recency_factor = max(0.25, 1.0 - days_delta / max(int(self.params.live_news_decay_days), 1))
            level_weight = CLS_LEVEL_WEIGHT.get(str(item.get("level", "normal")).lower(), 0.8)
            category_weight = CLS_CATEGORY_WEIGHT.get(str(item.get("category", "other")).lower(), 0.7)
            item_score = keyword_score * recency_factor * level_weight * category_weight
            total_score += item_score

            if item_score > top_score:
                title = str(item.get("title", "") or "").strip()
                top_label = title[:80] if title else str(item.get("label", "") or "live_news")
                top_keywords = list(keyword_info.get("matched_keywords", []))
                top_reason = f"实时新闻命中 {', '.join(top_keywords[:5])}" if top_keywords else "实时新闻事件"
                top_score = item_score

        if total_score <= 0:
            return TacoEventSignal()

        return TacoEventSignal(
            score=min(total_score, 3.0),
            label=top_label or "live_news",
            source="live_news",
            window_days=int(self.params.event_window_days),
            matched_keywords=top_keywords,
            reason=top_reason or "实时新闻事件仍在 30 天窗口内",
        )

    def _calc_keyword_score(self, item: Dict[str, object]) -> Dict[str, object]:
        title = str(item.get("title", "") or "")
        content = str(item.get("content", "") or "")
        haystack = f"{title}\n{content}".lower()

        score = 0.0
        matched_keywords: List[str] = []
        for keyword, weight in self.params.keywords.items():
            if str(keyword).lower() in haystack:
                score += float(weight)
                matched_keywords.append(str(keyword))

        topic_bonus = self._calc_topic_bonus(title, content)
        score += float(topic_bonus.get("bonus", 0.0))
        matched_keywords.extend(topic_bonus.get("matched_keywords", []))

        context_bonus = self._calc_context_bonus(title, content)
        score += float(context_bonus.get("bonus", 0.0))
        matched_keywords.extend(context_bonus.get("matched_keywords", []))

        unique_keywords: List[str] = []
        for keyword in matched_keywords:
            if keyword not in unique_keywords:
                unique_keywords.append(keyword)

        return {"score": score, "matched_keywords": unique_keywords}

    @staticmethod
    def _calc_context_bonus(title: str, content: str) -> Dict[str, object]:
        """
        对特朗普/关税/油气/地缘等组合事件给予额外加分。
        """
        haystack = f"{title}\n{content}".lower()
        combos = [
            (("trump", "tariff"), 0.75, ["Trump+Tariff"]),
            (("特朗普", "关税"), 0.75, ["特朗普+关税"]),
            (("truth social", "tariff"), 0.65, ["TruthSocial+Tariff"]),
            (("制裁", "原油"), 0.55, ["制裁+原油"]),
            (("伊朗", "原油"), 0.55, ["伊朗+原油"]),
            (("以色列", "原油"), 0.45, ["以色列+原油"]),
            (("ceasefire", "oil"), 0.40, ["停火+油价"]),
            (("出口管制", "芯片"), 0.50, ["出口管制+芯片"]),
            (("fed", "trump"), 0.35, ["Trump+Fed"]),
        ]
        bonus = 0.0
        matched: List[str] = []
        for keywords, weight, labels in combos:
            if all(keyword in haystack for keyword in keywords):
                bonus += float(weight)
                matched.extend(labels)
        return {"bonus": min(bonus, 1.5), "matched_keywords": matched[:6]}

    def _calc_topic_bonus(self, title: str, content: str) -> Dict[str, object]:
        haystack = f"{title}\n{content}".lower()
        matched_keywords: List[str] = []
        bonus = 0.0
        for keyword, weight in HOT_TOPIC_KEYWORDS.items():
            if str(keyword).lower() in haystack:
                matched_keywords.append(str(keyword))
                bonus += float(weight) * 0.25
        return {"bonus": min(bonus, 1.2), "matched_keywords": matched_keywords[:6]}

    def _collect_live_news_items(self) -> List[Dict[str, object]]:
        items: List[Dict[str, object]] = []
        items.extend(self._load_cls_cache_items())
        items.extend(self._load_news_brief_items())
        return items

    def _load_cls_cache_items(self) -> List[Dict[str, object]]:
        if self._cls_items_cache is not None:
            return self._cls_items_cache

        cache_path = str(self.params.cls_cache_path or "").strip()
        if not cache_path or not os.path.exists(cache_path):
            self._cls_items_cache = []
            return self._cls_items_cache

        try:
            with open(cache_path, "r", encoding="utf-8") as file:
                payload = json.load(file)
        except Exception:
            self._cls_items_cache = []
            return self._cls_items_cache

        rows: List[Dict[str, object]] = []
        iterable = payload.values() if isinstance(payload, dict) else payload if isinstance(payload, list) else []
        for item in iterable:
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "title": str(item.get("title", "") or ""),
                    "content": str(item.get("content", "") or ""),
                    "published_at": str(item.get("published_at") or item.get("publish_date") or ""),
                    "level": str(item.get("level", "important") or "important"),
                    "category": str(item.get("category", "overseas") or "overseas"),
                    "matched_keywords": item.get("matched_keywords", []),
                }
            )
        self._cls_items_cache = rows
        return self._cls_items_cache

    def _load_news_brief_items(self) -> List[Dict[str, object]]:
        if self._news_brief_items_cache is not None:
            return self._news_brief_items_cache

        db_path = str(self.params.db_path or "").strip()
        if not db_path or not os.path.exists(db_path):
            self._news_brief_items_cache = []
            return self._news_brief_items_cache

        try:
            payload = RecommendDB(db_path).get_dashboard_cache("news_briefs") or {}
        except Exception:
            self._news_brief_items_cache = []
            return self._news_brief_items_cache

        rows: List[Dict[str, object]] = []
        if isinstance(payload, dict):
            for block in payload.get("blocks", []):
                if isinstance(block, dict):
                    content = str(block.get("content", "") or "")
                    source = str(block.get("source", "") or "")
                    if source == "cls":
                        rows.extend(self._parse_cls_json_content(content))
                    else:
                        rows.extend(self._extract_items_from_brief_content(content))

        self._news_brief_items_cache = rows
        return self._news_brief_items_cache

    def _parse_cls_json_content(self, content: str) -> List[Dict[str, object]]:
        """解析CLS JSON格式的快讯内容"""
        rows: List[Dict[str, object]] = []
        try:
            item = json.loads(content)
            if isinstance(item, dict):
                rows.append({
                    "title": item.get("title", ""),
                    "content": item.get("content", ""),
                    "published_at": f"{item.get('publish_date', '')} {item.get('publish_time', '')}",
                    "level": item.get("level", "normal"),
                    "category": item.get("category", "other"),
                    "matched_keywords": item.get("matched_keywords", []),
                })
        except Exception:
            pass
        return rows

    def _extract_items_from_brief_content(self, content: str) -> List[Dict[str, object]]:
        text = str(content or "")
        if not text.strip():
            return []

        json_match = re.search(r'(\{\s*"data"\s*:\s*\[.*\]\s*\})', text, re.S)
        if not json_match:
            return []

        try:
            payload = json.loads(json_match.group(1))
        except Exception:
            return []

        rows: List[Dict[str, object]] = []
        for item in payload.get("data", []):
            if not isinstance(item, dict):
                continue
            rows.append(
                {
                    "title": str(item.get("title", "") or ""),
                    "content": str(item.get("content", "") or ""),
                    "published_at": str(item.get("date", "") or ""),
                    "level": "important",
                    "category": "overseas",
                    "matched_keywords": [],
                }
            )
        return rows

    def _ensure_calendar_loaded(self) -> None:
        calendar_path = str(self.params.calendar_path or "").strip()
        if not calendar_path or not os.path.exists(calendar_path):
            self._calendar_cache = []
            self._calendar_mtime = -1.0
            return

        current_mtime = os.path.getmtime(calendar_path)
        if current_mtime == self._calendar_mtime and self._calendar_cache:
            return

        try:
            with open(calendar_path, "r", encoding="utf-8") as file:
                payload = json.load(file)
        except Exception:
            self._calendar_cache = []
            self._calendar_mtime = current_mtime
            return

        events: List[TacoEvent] = []
        for item in payload.get("events", []) if isinstance(payload, dict) else []:
            if not isinstance(item, dict):
                continue
            try:
                events.append(
                    TacoEvent(
                        label=str(item.get("label", "") or ""),
                        anchor_date=str(item.get("anchor_date", "") or ""),
                        impact_score=float(item.get("impact_score", 1.0)),
                        base_window_days=int(item.get("base_window_days", FIXED_EVENT_WINDOW_DAYS)),
                        max_window_days=int(item.get("max_window_days", FIXED_EVENT_WINDOW_DAYS)),
                        pre_window_days=int(item.get("pre_window_days", 0)),
                        keywords=list(item.get("keywords", []) or []),
                        note=str(item.get("note", "") or ""),
                    )
                )
            except Exception:
                continue

        self._calendar_cache = events
        self._calendar_mtime = current_mtime

    @staticmethod
    def _parse_date(value: str) -> Optional[datetime]:
        try:
            return pd.Timestamp(value).to_pydatetime()
        except Exception:
            return None

    @staticmethod
    def _parse_datetime(value: str) -> Optional[datetime]:
        try:
            return pd.Timestamp(value).to_pydatetime()
        except Exception:
            return None


class TACOStrategy(BaseStrategy):
    """TACO / TACO-OIL 事件修复策略"""

    def __init__(self, params: Optional[TacoStrategyParams] = None):
        actual_params = params or build_taco_params("taco")
        super().__init__(actual_params.display_name)
        self.params = actual_params
        self.news_filter = TacoNewsEventFilter(self.params)

    def on_bar(self, symbol: str, df: pd.DataFrame) -> Optional[Signal]:
        """
        逐 bar 输出信号
        """
        min_bars = max(self.params.ema_slow + 2, self.params.panic_lookback + 3)
        if df is None or df.empty or len(df) < min_bars:
            return None

        data = self._calc_indicators(df.copy())
        latest = data.iloc[-1]
        prev = data.iloc[-2]
        trade_date = self._resolve_trade_date(data)
        event_signal = self.news_filter.get_event_signal(trade_date)
        market_context = self.get_market_context()
        buy_score = self._calc_buy_score(data, event_signal, market_context)
        candidate_score = min(max(buy_score / 6.0, 0.0), 1.0)

        if self._is_buy_setup(data, event_signal, market_context, buy_score=buy_score):
            return Signal(
                symbol=symbol,
                date=trade_date,
                signal=1,
                weight=self._calc_buy_weight(symbol, latest, event_signal, buy_score),
                candidate_score=candidate_score,
                gate_passed=True,
                gate_reason=event_signal.reason,
            )

        if self._is_sell_setup(data):
            return Signal(
                symbol=symbol,
                date=trade_date,
                signal=-1,
                weight=self._calc_sell_weight(latest, prev),
                candidate_score=candidate_score,
                gate_passed=True,
                gate_reason="触发事件修复卖出结构",
            )
        return Signal(
            symbol=symbol,
            date=trade_date,
            signal=0,
            weight=0.0,
            candidate_score=candidate_score,
            gate_passed=False,
            gate_reason=event_signal.reason or "事件分数或价格结构未满足",
        )

    def describe_trade_date(self, trade_date: datetime) -> Dict[str, object]:
        """
        输出事件诊断快照
        """
        event_signal = self.news_filter.get_event_signal(trade_date)
        active = bool(event_signal.score >= float(self.params.event_score_threshold))
        return {
            "variant": self.params.variant,
            "display_name": self.params.display_name,
            "date": trade_date.strftime("%Y-%m-%d"),
            "event_score": round(float(event_signal.score), 3),
            "window_days": int(event_signal.window_days),
            "threshold": float(self.params.event_score_threshold),
            "active": active,
            "status_label": "已激活" if active else "未激活",
            "source": event_signal.source or "-",
            "label": event_signal.label or "-",
            "matched_keywords": list(event_signal.matched_keywords),
            "reason": event_signal.reason or ("事件分数未达到阈值" if not active else "事件分数已达到阈值"),
            "prefer_funds": bool(self.params.prefer_funds),
        }

    def _calc_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df["open"] = df.get("open", df["close"])
        df["high"] = df.get("high", df["close"])
        df["low"] = df.get("low", df["close"])
        df["volume"] = df.get("volume", 0.0)
        df["ema_fast"] = df["close"].ewm(span=self.params.ema_fast, adjust=False).mean()
        df["ema_slow"] = df["close"].ewm(span=self.params.ema_slow, adjust=False).mean()
        df["ret_1d"] = df["close"].pct_change()
        df["panic_low"] = df["low"].rolling(self.params.panic_lookback).min()
        df["recent_high"] = df["high"].rolling(self.params.rebound_lookback).max()
        df["volume_ma"] = df["volume"].rolling(self.params.rebound_lookback).mean()
        df["volume_ratio"] = df["volume"] / df["volume_ma"].replace(0, pd.NA)
        df["rebound_from_panic"] = df["close"] / df["panic_low"].replace(0, pd.NA) - 1.0
        df["distance_to_recent_high"] = df["close"] / df["recent_high"].replace(0, pd.NA) - 1.0
        df["intraday_reversal"] = (df["close"] - df["low"]) / (df["high"] - df["low"] + 1e-6)
        return df

    def _calc_buy_score(
        self,
        df: pd.DataFrame,
        event_signal: TacoEventSignal,
        market_context: Optional[Dict[str, object]] = None,
    ) -> float:
        """计算 TACO 买入分。"""
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        volume_ratio = self._safe_float(latest.get("volume_ratio"), 0.0)
        rebound_from_panic = self._safe_float(latest.get("rebound_from_panic"), 0.0)
        intraday_reversal = self._safe_float(latest.get("intraday_reversal"), 0.0)
        regime = str((market_context or {}).get("regime", "normal") or "normal")

        had_panic = bool((df["ret_1d"].tail(self.params.panic_lookback) <= self.params.shock_drop_pct).any())
        reclaim_fast_ma = bool(prev["close"] <= prev["ema_fast"] and latest["close"] > latest["ema_fast"])
        above_slow_ma = bool(latest["close"] >= latest["ema_slow"] * 0.99)
        volume_confirm = bool(volume_ratio >= self.params.volume_ratio_threshold)
        rebound_started = bool(rebound_from_panic >= 0.006)
        close_strong = bool(intraday_reversal >= 0.52)
        event_active = bool(event_signal.score >= float(self.params.event_score_threshold))
        score = 0.0
        if had_panic:
            score += 1.3
        if reclaim_fast_ma:
            score += 1.0
        if above_slow_ma:
            score += 0.9
        if volume_confirm:
            score += 0.8
        elif volume_ratio >= max(0.9, self.params.volume_ratio_threshold - 0.12):
            score += 0.4
        if rebound_started:
            score += 0.8
        if close_strong:
            score += 0.7
        elif intraday_reversal >= 0.46:
            score += 0.3
        if event_active:
            score += 1.4
        elif event_signal.score >= max(0.12, float(self.params.event_score_threshold) * 0.75):
            score += 0.8
        if regime == "golden_pit":
            score += 0.35
        elif regime == "defense":
            score -= 0.25
        return score

    def _is_buy_setup(
        self,
        df: pd.DataFrame,
        event_signal: TacoEventSignal,
        market_context: Optional[Dict[str, object]] = None,
        buy_score: Optional[float] = None,
    ) -> bool:
        latest = df.iloc[-1]
        regime = str((market_context or {}).get("regime", "normal") or "normal")
        actual_buy_score = float(buy_score if buy_score is not None else self._calc_buy_score(df, event_signal, market_context))
        event_floor = max(0.10, float(self.params.event_score_threshold) * 0.70)
        if float(event_signal.score or 0.0) < event_floor:
            return False
        if latest["close"] <= 0:
            return False
        if regime == "defense":
            return bool(actual_buy_score >= 4.8 and latest["close"] >= latest["ema_slow"])
        if regime == "golden_pit":
            return bool(actual_buy_score >= 4.0)
        return bool(actual_buy_score >= 4.4)

    def _is_sell_setup(self, df: pd.DataFrame) -> bool:
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        rebound_from_panic = self._safe_float(latest.get("rebound_from_panic"), 0.0)
        distance_to_recent_high = self._safe_float(latest.get("distance_to_recent_high"), -1.0)
        intraday_reversal = self._safe_float(latest.get("intraday_reversal"), 0.0)
        rebound_fail = bool(prev["close"] >= prev["ema_fast"] and latest["close"] < latest["ema_fast"])
        ma_trend_lost = bool(latest["ema_fast"] < latest["ema_slow"] and latest["close"] < latest["ema_slow"])
        hit_profit_target = bool(rebound_from_panic >= self.params.profit_target_pct)
        break_panic_low = bool(rebound_from_panic <= self.params.stop_loss_pct)
        near_recent_high = bool(distance_to_recent_high >= -0.01 and intraday_reversal < 0.42)
        return any([rebound_fail, ma_trend_lost, hit_profit_target, break_panic_low, near_recent_high])

    def _calc_buy_weight(self, symbol: str, latest: pd.Series, event_signal: TacoEventSignal, buy_score: float) -> float:
        rebound = self._safe_float(latest.get("rebound_from_panic"), 0.0)
        reversal = self._safe_float(latest.get("intraday_reversal"), 0.0)
        volume_ratio = self._safe_float(latest.get("volume_ratio"), 0.0)
        event_bonus = min(max(event_signal.score, 0.0), 2.5) * 0.10
        fund_bonus = 0.12 if self.params.prefer_funds and self._is_etf_lof_symbol(symbol) else 0.0
        raw_score = (
            0.24
            + min(max(rebound, 0.0), 0.08) * 3.5
            + max(reversal - 0.5, 0.0) * 0.35
            + min(volume_ratio, 2.0) * 0.08
            + event_bonus
            + fund_bonus
            + min(max(buy_score - 4.0, 0.0), 1.8) * 0.08
        )
        return max(0.18, min(raw_score, 0.72))

    def _calc_sell_weight(self, latest: pd.Series, prev: pd.Series) -> float:
        latest_close = self._safe_float(latest.get("close"), 0.0)
        latest_ema_fast = self._safe_float(latest.get("ema_fast"), latest_close)
        prev_close = self._safe_float(prev.get("close"), latest_close)
        distance = abs(latest_close / latest_ema_fast - 1.0) if latest_ema_fast > 0 else 0.0
        fade = abs(latest_close / prev_close - 1.0) if prev_close > 0 else 0.0
        raw_score = 0.5 + min(distance, 0.04) * 6.0 + min(fade, 0.05) * 4.0
        return max(0.4, min(raw_score, 1.0))

    @staticmethod
    def _is_etf_lof_symbol(symbol: str) -> bool:
        code = str(symbol or "").zfill(6)
        return code.startswith(("15", "16", "18", "50", "51", "56", "58"))

    @staticmethod
    def _safe_float(value: object, default: float = 0.0) -> float:
        try:
            if pd.isna(value):
                return default
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _resolve_trade_date(df: pd.DataFrame) -> datetime:
        try:
            return pd.Timestamp(df.index[-1]).to_pydatetime()
        except Exception:
            return datetime.now()


class TACOOilStrategy(TACOStrategy):
    """热点扩展版 TACO"""

    def __init__(self, params: Optional[TacoStrategyParams] = None):
        super().__init__(params or build_taco_params("taco_oil"))


def build_taco_snapshot(
    variant: str = "taco",
    trade_date: Optional[datetime] = None,
    overrides: Optional[Dict[str, object]] = None,
) -> Dict[str, object]:
    """
    构建 TACO 诊断快照
    """
    actual_date = trade_date or datetime.now()
    if variant == "taco_oil":
        strategy = TACOOilStrategy(build_taco_params("taco_oil", overrides))
    else:
        strategy = TACOStrategy(build_taco_params("taco", overrides))
    return strategy.describe_trade_date(actual_date)


def build_taco_hot_topics(
    variant: str = "taco",
    trade_date: Optional[datetime] = None,
    overrides: Optional[Dict[str, object]] = None,
    limit: int = 8,
) -> List[Dict[str, object]]:
    """
    构建 TACO 热点摘要
    """
    actual_date = trade_date or datetime.now()
    if variant == "taco_oil":
        strategy = TACOOilStrategy(build_taco_params("taco_oil", overrides))
    else:
        strategy = TACOStrategy(build_taco_params("taco", overrides))
    return strategy.news_filter.get_hot_topics(actual_date, limit=limit)
