# -*- coding: utf-8 -*-
"""
轻量级量化看板服务
"""
import argparse
import json
import os
import sqlite3
import threading
import time
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv

from data import DataSource
from data.recommend_db import RecommendDB
from strategy import build_taco_hot_topics, build_taco_snapshot
from docker_start import ScheduledPusher
from trading.review_report import build_runtime_review_report
from trading.runtime_config import (
    MARKET_REGIME_MODE_OPTIONS,
    get_runtime_settings,
    save_runtime_settings,
)
from utils.logger import get_logger

load_dotenv()
load_dotenv(".env.local", override=True)

logger = get_logger(__name__)

BASE_DIR = Path(__file__).resolve().parent
HTML_PATH = BASE_DIR / "dashboard" / "index.html"
DASHBOARD_PORT = 18675
ACTIVE_ACTION_NAMES = {
    "refresh_market_cache",
    "refresh_pool",
    "refresh_signal_pool",
    "refresh_timing_experiments",
    "push_once",
    "push_intraday_alert",
}


def _mask_secret(value: str) -> str:
    """
    对敏感配置做脱敏展示。
    """
    text = str(value or "").strip()
    if not text:
        return "未配置"
    if len(text) <= 8:
        return "*" * len(text)
    return f"{text[:4]}***{text[-4:]}"

class DashboardService:
    """看板数据服务。"""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = self._resolve_db_path(db_path or os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db"))
        self.db = RecommendDB(self.db_path)
        self._action_lock = threading.Lock()
        self._action_state: Dict[str, Dict] = self._load_action_state()

    def _load_action_state(self) -> Dict[str, Dict]:
        """
        从缓存表恢复操作状态。
        """
        payload = self.db.get_dashboard_cache("action_state")
        if not isinstance(payload, dict):
            return {}
        actions = payload.get("actions", {})
        if not isinstance(actions, dict):
            return {}
        return self._sanitize_action_state(actions)

    def _sanitize_action_state(self, actions: Dict[str, Dict]) -> Dict[str, Dict]:
        """
        过滤废弃动作，并清理长时间未结束的 running 状态。
        """
        sanitized: Dict[str, Dict] = {}
        for action_name, item in actions.items():
            if action_name not in ACTIVE_ACTION_NAMES:
                continue
            row = dict(item or {})
            status = str(row.get("status", "") or "").strip()
            updated_at = self._parse_datetime_text(str(row.get("updated_at", "") or ""))
            if status == "running" and updated_at is not None:
                if (datetime.now() - updated_at).total_seconds() > 15 * 60:
                    row["status"] = "failed"
                    row["message"] = "历史执行状态已过期，已自动清理"
            sanitized[action_name] = row
        return sanitized

    def _resolve_db_path(self, preferred_path: str) -> str:
        """
        解析数据库路径，兼容旧版 data 目录挂载。
        """
        raw_preferred = str(preferred_path or "").strip()
        if raw_preferred and Path(raw_preferred).exists():
            return raw_preferred

        candidates = []
        for fallback in [
            "./runtime/data/recommend.db",
            "/app/runtime/data/recommend.db",
            "./data/recommend.db",
            "/app/data/recommend.db",
        ]:
            if fallback not in candidates and Path(fallback).exists():
                candidates.append(fallback)

        if not candidates:
            return raw_preferred or "./runtime/data/recommend.db"

        best_path = candidates[0]
        best_score = -1
        for path in candidates:
            score = self._score_db_path(path)
            if score > best_score:
                best_score = score
                best_path = path

        if best_path != raw_preferred and raw_preferred:
            logger.info(f"看板数据库路径回退: {raw_preferred} -> {best_path}")
        return best_path

    @staticmethod
    def _score_db_path(db_path: str) -> int:
        """
        根据数据库可用性和数据量打分。
        """
        path = Path(db_path)
        if not path.exists():
            return -1

        try:
            conn = sqlite3.connect(str(path))
            cursor = conn.cursor()
            total = 0
            for table_name in ["positions", "signal_pool", "recommends", "trade_points", "trades"]:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    total += int(cursor.fetchone()[0] or 0)
                except Exception:
                    continue
            conn.close()
            return total
        except Exception:
            return 0

    def _get_conn(self) -> sqlite3.Connection:
        """
        获取 SQLite 连接。
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def get_overview(self) -> Dict:
        """
        获取看板总览数据。
        """
        holdings = self.db.get_holdings_aggregated()
        signal_pool = self.db.get_signal_pool(limit=100)
        signal_pool_all = self.db.get_signal_pool_multi_status(["active", "holding", "inactive"], limit=200)
        signal_pool_display = self._filter_and_sort_signal_pool_rows(
            [self._decorate_signal_pool_row(row) for row in signal_pool]
        )
        signal_pool_all_display = self._filter_and_sort_signal_pool_rows(
            [self._decorate_signal_pool_row(row) for row in signal_pool_all]
        )
        signal_pool_counts = {
            "active": len(signal_pool_display),
            "holding": len(
                self._filter_and_sort_signal_pool_rows(
                    [self._decorate_signal_pool_row(row) for row in self.db.get_signal_pool(status="holding", limit=100)]
                )
            ),
            "inactive": len(
                self._filter_and_sort_signal_pool_rows(
                    [self._decorate_signal_pool_row(row) for row in self.db.get_signal_pool(status="inactive", limit=100)]
                )
            ),
        }
        stock_pool = self.get_stock_pool(limit=100)
        stats = self.db.get_statistics()
        recommendations = self.get_recent_recommends(limit=20)
        trade_points = self.db.get_trade_points(limit=100)
        action_state = self.get_action_state()
        latest_actions = {
            "refresh_market_cache_at": self._get_action_updated_at(action_state, "refresh_market_cache"),
            "refresh_pool_at": self._get_action_updated_at(action_state, "refresh_pool"),
            "refresh_signal_pool_at": self._get_action_updated_at(action_state, "refresh_signal_pool"),
            "refresh_timing_experiments_at": self._get_action_updated_at(action_state, "refresh_timing_experiments"),
            "push_once_at": self._get_action_updated_at(action_state, "push_once"),
            "push_intraday_alert_at": self._get_action_updated_at(action_state, "push_intraday_alert"),
        }
        timing_experiments = self.get_timing_experiments()
        strategy_tuning = self.get_strategy_tuning()
        runtime_settings = self.get_runtime_settings()
        taco_diagnostics = self.get_taco_diagnostics()
        taco_hot_topics = self.get_taco_hot_topics()
        freshness = {
            "market_cache_freshness": self._calc_freshness(self.get_market_cards().get("generated_at", ""), fresh_minutes=5, stale_minutes=20),
            "stock_pool_freshness": self._calc_freshness(stock_pool[0].get("updated_at", "") if stock_pool else "", fresh_minutes=720, stale_minutes=1440),
            "signal_pool_freshness": self._calc_freshness(signal_pool[0].get("updated_at", "") if signal_pool else "", fresh_minutes=240, stale_minutes=720),
            "timing_experiments_freshness": self._calc_freshness(timing_experiments.get("generated_at", ""), fresh_minutes=240, stale_minutes=720),
            "strategy_tuning_freshness": self._calc_freshness(strategy_tuning.get("generated_at", ""), fresh_minutes=240, stale_minutes=720),
        }
        tuning_recommendations = list(strategy_tuning.get("recommended", []) or [])
        tuning_summary = dict(strategy_tuning.get("summary", {}) or {})

        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "database_path": self.db_path,
            "summary": {
                "holding_count": len(holdings),
                "signal_pool_count": len(signal_pool_display),
                "signal_pool_active_count": int(signal_pool_counts.get("active", 0)),
                "signal_pool_holding_count": int(signal_pool_counts.get("holding", 0)),
                "signal_pool_inactive_count": int(signal_pool_counts.get("inactive", 0)),
                "stock_pool_count": len(stock_pool),
                "recommend_count": len(recommendations),
                "trade_event_count": len(trade_points),
                "sell_trade_count": int(stats.get("total_trades", 0) or 0),
                "win_rate": float(stats.get("win_rate", 0.0) or 0.0),
                "total_pnl": float(stats.get("total_pnl", 0.0) or 0.0),
                "strategy_tuning_strategy": str(strategy_tuning.get("strategy_name", "") or ""),
                "strategy_tuning_experiment_count": int(tuning_summary.get("experiment_count", 0) or 0),
                "strategy_tuning_recommended_count": len(tuning_recommendations),
                "strategy_tuning_best_name": str((tuning_recommendations[0] or {}).get("name", "") if tuning_recommendations else ""),
                **latest_actions,
                **freshness,
            },
            "features": self.get_feature_status(),
            "runtime_settings": runtime_settings,
            "strategy_tuning": strategy_tuning,
            "taco_diagnostics": taco_diagnostics,
            "taco_hot_topics": taco_hot_topics,
            "background_health": self.get_background_health(),
            "latest": {
                "recommend": recommendations[0] if recommendations else None,
                "trade_point": trade_points[0] if trade_points else None,
                "signal_pool": signal_pool_display[0] if signal_pool_display else None,
                "signal_pool_any": self._pick_latest_signal_pool_row(signal_pool_all_display),
                "stock_pool": stock_pool[0] if stock_pool else None,
                "strategy_tuning": strategy_tuning,
                "taco_diagnostics": taco_diagnostics,
                "taco_hot_topics": taco_hot_topics,
            },
        }

    @staticmethod
    def _get_action_updated_at(action_state: Dict[str, Dict], action_name: str) -> str:
        """
        获取指定操作最近执行时间。
        """
        item = action_state.get(action_name, {})
        return str(item.get("updated_at", "") or "")

    @staticmethod
    def _calc_freshness(timestamp_text: str, fresh_minutes: int, stale_minutes: int) -> Dict[str, str]:
        """
        计算数据新鲜度状态。
        """
        text = str(timestamp_text or "").strip()
        if not text:
            return {"status": "empty", "label": "暂无数据"}

        parsed_at: Optional[datetime] = None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                parsed_at = datetime.strptime(text[:19] if "T" in text else text, fmt)
                break
            except Exception:
                continue

        if parsed_at is None:
            return {"status": "unknown", "label": text}

        age_minutes = max(0.0, (datetime.now() - parsed_at).total_seconds() / 60.0)
        if age_minutes <= fresh_minutes:
            return {"status": "fresh", "label": f"最新（{int(age_minutes)}分钟前）"}
        if age_minutes <= stale_minutes:
            return {"status": "aging", "label": f"需关注（{int(age_minutes)}分钟前）"}
        return {"status": "stale", "label": f"已过期（{int(age_minutes)}分钟前）"}

    def get_market_cards(self) -> Dict:
        """
        获取盘中实时行情卡片数据。
        """
        payload = self.db.get_dashboard_cache("market_cards")
        if payload:
            return payload
        return {
            "generated_at": "",
            "indices": [],
            "etfs": [],
            "holdings": [],
        }

    def get_timing_experiments(self) -> Dict[str, object]:
        """
        获取择时参数试验缓存。
        """
        payload = self.db.get_dashboard_cache("timing_experiments")
        if payload:
            return payload
        return {
            "generated_at": "",
            "conclusion": {
                "title": "暂无试验结果",
                "summary": "请先刷新信号池或手动执行一次择时参数试验。",
                "recommendation": "当前无法给出参数建议",
            },
            "scenarios": [],
        }

    def get_strategy_tuning(self) -> Dict[str, object]:
        """
        获取最近一次策略调优结果与推荐配置。
        """
        report_dir = BASE_DIR / "runtime" / "reports" / "tuning"
        if not report_dir.exists():
            return self._build_empty_strategy_tuning()

        report_files = sorted(
            report_dir.glob("experiment_report_*.json"),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
        if not report_files:
            return self._build_empty_strategy_tuning()

        try:
            payload = json.loads(report_files[0].read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"读取策略调优报告失败: {e}")
            return self._build_empty_strategy_tuning()

        strategy_name = str(payload.get("strategy_name", "") or "")
        experiments = list(payload.get("experiments", []) or [])
        recommended = list(payload.get("recommended", []) or [])
        review = dict(payload.get("review", {}) or {})
        generated_at = str(payload.get("generated_at", "") or "")

        snapshot = self._load_strategy_snapshot(report_dir, strategy_name)
        best_candidate = dict(snapshot.get("best_candidate", {}) or {})
        if not best_candidate and recommended:
            best_candidate = dict(recommended[0] or {})

        summary = {
            "experiment_count": len(experiments),
            "recommended_count": len(recommended),
            "primary_benchmark_code": str(payload.get("primary_benchmark_code", "399001") or "399001"),
            "primary_benchmark_name": str(payload.get("primary_benchmark_name", "深证成指") or "深证成指"),
            "best_name": str(best_candidate.get("name", "") or ""),
            "best_total_return": float(best_candidate.get("total_return", 0.0) or 0.0),
            "best_excess_return": float(best_candidate.get("primary_excess_return", 0.0) or 0.0),
            "best_max_drawdown": float(best_candidate.get("max_drawdown", 0.0) or 0.0),
            "best_sharpe_ratio": float(best_candidate.get("sharpe_ratio", 0.0) or 0.0),
            "gate_pass_rate": float((((best_candidate.get("signal_summary", {}) or {}).get("gate_pass_rate", 0.0)) or 0.0)),
        }

        return {
            "generated_at": generated_at,
            "strategy_name": strategy_name,
            "summary": summary,
            "review": review,
            "best_candidate": best_candidate,
            "recommended": recommended[:3],
            "experiments": experiments[:8],
            "snapshot": {
                "generated_at": str(snapshot.get("generated_at", "") or ""),
                "json_path": str(snapshot.get("json_path", "") or ""),
            },
            "source_files": {
                "report_json_path": str(report_files[0]),
            },
        }

    @staticmethod
    def _build_empty_strategy_tuning() -> Dict[str, object]:
        """
        构建空的策略调优数据。
        """
        return {
            "generated_at": "",
            "strategy_name": "",
            "summary": {
                "experiment_count": 0,
                "recommended_count": 0,
                "primary_benchmark_code": "399001",
                "primary_benchmark_name": "深证成指",
                "best_name": "",
                "best_total_return": 0.0,
                "best_excess_return": 0.0,
                "best_max_drawdown": 0.0,
                "best_sharpe_ratio": 0.0,
                "gate_pass_rate": 0.0,
            },
            "review": {
                "summary": "暂无策略调优结果，请先运行 `python3 main.py --mode tune-experiments --strategy taco`。",
                "suggestions": [],
            },
            "best_candidate": {},
            "recommended": [],
            "experiments": [],
            "snapshot": {
                "generated_at": "",
                "json_path": "",
            },
            "source_files": {
                "report_json_path": "",
            },
        }

    @staticmethod
    def _load_strategy_snapshot(report_dir: Path, strategy_name: str) -> Dict[str, object]:
        """
        读取最新的推荐配置快照。
        """
        if not strategy_name:
            return {}
        pattern = f"best_config_{strategy_name}_*.json"
        snapshot_files = sorted(
            report_dir.glob(pattern),
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
        if not snapshot_files:
            return {}
        try:
            payload = json.loads(snapshot_files[0].read_text(encoding="utf-8"))
            payload["json_path"] = str(snapshot_files[0])
            return payload
        except Exception as e:
            logger.warning(f"读取推荐配置快照失败: {e}")
            return {}

    def get_taco_diagnostics(self) -> List[Dict[str, object]]:
        """
        获取 TACO / TACO-OIL 今日事件诊断。
        """
        current_date = datetime.now()
        rows: List[Dict[str, object]] = []
        for variant in ["taco", "taco_oil"]:
            try:
                rows.append(build_taco_snapshot(variant=variant, trade_date=current_date))
            except Exception as e:
                rows.append(
                    {
                        "variant": variant,
                        "display_name": variant.upper(),
                        "date": current_date.strftime("%Y-%m-%d"),
                        "event_score": 0.0,
                        "window_days": 0,
                        "threshold": 0.0,
                        "active": False,
                        "status_label": "加载失败",
                        "source": "-",
                        "label": "-",
                        "matched_keywords": [],
                        "reason": f"诊断失败: {e}",
                    }
                )
        return rows

    def get_taco_hot_topics(self) -> List[Dict[str, object]]:
        """
        获取 TACO 追踪热点
        """
        current_date = datetime.now()
        rows: List[Dict[str, object]] = []
        for variant in ["taco", "taco_oil"]:
            try:
                topics = build_taco_hot_topics(variant=variant, trade_date=current_date, limit=6)
                for topic in topics:
                    item = dict(topic or {})
                    item["variant"] = variant
                    item["display_name"] = "TACO-OIL" if variant == "taco_oil" else "TACO"
                    item["topic_group"] = self._classify_taco_topic(item)
                    rows.append(item)
            except Exception as e:
                rows.append(
                    {
                        "variant": variant,
                        "display_name": "TACO-OIL" if variant == "taco_oil" else "TACO",
                        "name": "load_failed",
                        "score": 0.0,
                        "source": "error",
                        "date": current_date.strftime("%Y-%m-%d"),
                        "keywords": [],
                        "reason": str(e),
                        "topic_group": "other",
                    }
                )
        rows.sort(key=lambda item: float(item.get("score", 0.0) or 0.0), reverse=True)
        return rows

    @staticmethod
    def _classify_taco_topic(item: Dict[str, object]) -> str:
        """
        归类 TACO 热点主题
        """
        text = " ".join(
            [
                str(item.get("name", "") or ""),
                str(item.get("reason", "") or ""),
                " ".join([str(keyword) for keyword in item.get("keywords", []) or []]),
            ]
        ).lower()
        if any(keyword in text for keyword in ["特朗普", "trump", "关税", "tariff", "贸易战", "trade war"]):
            return "trump_tariff"
        if any(keyword in text for keyword in ["原油", "油价", "石油", "brent", "wti", "opec"]):
            return "oil"
        if any(keyword in text for keyword in ["中东", "霍尔木兹", "hormuz", "伊朗", "iran", "以色列", "israel", "地缘"]):
            return "middle_east"
        if any(keyword in text for keyword in ["ai", "人工智能", "算力", "芯片", "半导体"]):
            return "ai_chip"
        if any(keyword in text for keyword in ["军工", "导弹", "航运", "黄金", "稀土"]):
            return "defense_supply"
        return "other"

    def _load_market_cards(self) -> Dict:
        """
        实际加载行情卡片数据。
        """
        index_items = [
            {"code": "000001", "name": "上证指数", "group": "index"},
            {"code": "399001", "name": "深证成指", "group": "index"},
            {"code": "399006", "name": "创业板指", "group": "index"},
            {"code": "000688", "name": "科创50", "group": "index"},
            {"code": "000905", "name": "中证500", "group": "index"},
            {"code": "000852", "name": "中证1000", "group": "index"},
        ]

        holdings = self.db.get_holdings_aggregated()
        signal_pool = self.db.get_signal_pool(limit=12)
        etf_items = []
        seen_codes = set()
        for item in signal_pool:
            code = str(item.get("code", "")).strip()
            if not code or code in seen_codes:
                continue
            if code.startswith(("5", "1")):
                etf_items.append({"code": code, "name": str(item.get("name", "")).strip(), "group": "etf"})
                seen_codes.add(code)
            if len(etf_items) >= 6:
                break

        holding_items = []
        for item in holdings[:8]:
            code = str(item.get("code", "")).strip()
            if not code:
                continue
            holding_items.append(
                {
                    "code": code,
                    "name": str(item.get("name", "")).strip(),
                    "group": "holding",
                    "avg_buy_price": float(item.get("avg_buy_price") or 0.0),
                    "total_pnl_pct": float(item.get("total_pnl_pct") or 0.0),
                }
            )

        symbols = [item["code"] for item in index_items + etf_items + holding_items]
        quotes_map: Dict[str, Dict] = {}
        try:
            data_source = DataSource()
            try:
                df = data_source.get_market_snapshots(symbols)
                if df is not None and not df.empty:
                    df.columns = [str(col).lower() for col in df.columns]
                    for _, row in df.iterrows():
                        code = str(row.get("code", "")).strip()
                        if code:
                            quotes_map[code[-6:]] = {
                                "code": code[-6:],
                                "name": str(row.get("name", "")).strip(),
                                "last_price": float(row.get("last_price", 0) or 0.0),
                                "change_rate": float(row.get("change_rate", 0) or 0.0),
                                "turnover": float(row.get("turnover", 0) or 0.0),
                                "volume": float(row.get("volume", 0) or 0.0),
                            }
            finally:
                data_source.close()
        except Exception as e:
            logger.warning(f"获取看板实时行情失败: {e}")

        def enrich(items: List[Dict]) -> List[Dict]:
            result: List[Dict] = []
            for item in items:
                quote = quotes_map.get(str(item.get("code", "")).zfill(6), {})
                merged = dict(item)
                merged.update(quote)
                if item.get("group") == "holding":
                    avg_buy_price = float(item.get("avg_buy_price", 0.0) or 0.0)
                    last_price = float(merged.get("last_price", 0.0) or 0.0)
                    if avg_buy_price > 0 and last_price > 0:
                        merged["intraday_pnl_pct"] = (last_price - avg_buy_price) / avg_buy_price * 100
                    else:
                        merged["intraday_pnl_pct"] = 0.0
                result.append(merged)
            return result

        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "indices": enrich(index_items),
            "etfs": enrich(etf_items),
            "holdings": enrich(holding_items),
        }

    def refresh_market_cache(self) -> Dict:
        """
        刷新并写入看板行情缓存。
        """
        result = self._load_market_cards()
        self.db.set_dashboard_cache("market_cards", result)
        return result

    @staticmethod
    def _format_signal_pool_refresh_message(result: Dict[str, object], prefix: str = "信号池刷新完成") -> str:
        """
        格式化信号池刷新与自动买入结果摘要。
        """
        payload = dict(result or {})
        buy_count = int(payload.get("buy_count", 0) or 0)
        saved_count = int(payload.get("saved_count", 0) or 0)
        recommend_count = int(payload.get("recommend_count", 0) or 0)
        auto_buy_count = int(payload.get("auto_buy_count", 0) or 0)
        auto_buy_result = payload.get("auto_buy_result", {}) if isinstance(payload.get("auto_buy_result", {}), dict) else {}
        auto_buy_reason = str(auto_buy_result.get("reason", "") or "").strip()
        ai_reason = str(auto_buy_result.get("ai_reason", "") or "").strip()

        message = (
            f"{prefix}：共写入 {saved_count} 条，"
            f"买入信号 {buy_count} 条，"
            f"新增荐股 {recommend_count} 条，"
            f"自动买入 {auto_buy_count} 条"
        )
        if buy_count <= 0:
            return f"{message}，当前无买入信号"
        if auto_buy_count > 0:
            return message
        if ai_reason:
            return f"{message}，AI 决策未放行：{ai_reason}"
        if auto_buy_reason:
            reason_map = {
                "no_buy_signals": "当前无买入信号",
                "no_recommends": "当前无可买荐股",
            }
            return f"{message}，未买入原因：{reason_map.get(auto_buy_reason, auto_buy_reason)}"
        return f"{message}，未买入原因：规则或仓位限制未通过"

    def run_action(self, action: str) -> Dict:
        """
        执行看板操作。
        """
        action_name = str(action or "").strip()
        if not action_name:
            return {"ok": False, "message": "缺少 action 参数"}

        allowed_actions = {"refresh_pool", "refresh_signal_pool", "refresh_market_cache", "refresh_timing_experiments", "push_once", "push_intraday_alert"}
        if action_name not in allowed_actions:
            return {"ok": False, "message": f"未知操作: {action_name}"}

        with self._action_lock:
            running = self._action_state.get(action_name, {}).get("status") == "running"
            if running:
                return {"ok": True, "message": f"{action_name} 正在执行中，请稍后刷新查看结果"}
            self._action_state[action_name] = {
                "status": "running",
                "message": "任务已提交，正在后台执行",
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

        thread = threading.Thread(target=self._execute_action, args=(action_name,), daemon=True)
        thread.start()
        return {"ok": True, "message": f"{action_name} 已提交，正在后台执行"}

    def _execute_action(self, action_name: str):
        """
        后台执行操作。
        """
        pusher: Optional[ScheduledPusher] = None
        try:
            if action_name == "refresh_market_cache":
                market = self.refresh_market_cache()
                message = (
                    f"行情缓存刷新完成：指数{len(market.get('indices', []))}项，"
                    f"ETF{len(market.get('etfs', []))}项，持仓{len(market.get('holdings', []))}项"
                )
                self._set_action_state(action_name, "success", message)
                return
            if action_name == "refresh_timing_experiments":
                result = self.refresh_timing_experiments()
                message = (
                    f"择时参数试验刷新完成：方案{len(result.get('scenarios', []))}个，"
                    f"结论：{result.get('conclusion', {}).get('title', '无')}"
                )
                self._set_action_state(action_name, "success", message)
                return

            pusher = ScheduledPusher()
            if action_name == "refresh_pool":
                pusher.update_stock_pool(merge_existing=False)
                self._set_action_state(action_name, "success", "股票池刷新完成")
                return
            if action_name == "refresh_signal_pool":
                result = pusher.refresh_signal_pool(etf_count=5, stock_count=5, reload_pool=True)
                self._set_action_state(
                    action_name,
                    "success",
                    self._format_signal_pool_refresh_message(result),
                )
                return
            if action_name == "push_once":
                success = bool(pusher.push_once())
                self._set_action_state(action_name, "success" if success else "failed", "外围简报推送完成" if success else "外围简报推送失败")
                return
            if action_name == "push_intraday_alert":
                success = bool(pusher.push_intraday_trap_signal())
                self._set_action_state(action_name, "success" if success else "failed", "盘中预警执行完成" if success else "盘中预警执行失败")
                return
            self._set_action_state(action_name, "failed", f"未知操作: {action_name}")
        except Exception as e:
            logger.error(f"执行看板操作失败 {action_name}: {e}")
            self._set_action_state(action_name, "failed", f"执行失败: {e}")
        finally:
            try:
                if pusher and getattr(pusher, "data_source", None):
                    pusher.data_source.close()
            except Exception:
                pass

    def _set_action_state(self, action_name: str, status: str, message: str):
        """
        更新操作状态。
        """
        with self._action_lock:
            self._action_state[action_name] = {
                "status": status,
                "message": message,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            self.db.set_dashboard_cache(
                "action_state",
                {
                    "actions": self._action_state,
                },
            )

    def get_action_state(self) -> Dict[str, Dict]:
        """
        获取操作状态。
        """
        with self._action_lock:
            self._action_state = self._sanitize_action_state(self._action_state)
            return dict(self._action_state)

    def mark_action_state(self, action_name: str, status: str, message: str) -> None:
        """
        对外暴露操作状态更新方法，便于后台定时任务复用。
        """
        self._set_action_state(action_name, status, message)

    def get_feature_status(self) -> List[Dict]:
        """
        获取功能开关和配置状态。
        """
        bark_key = str(os.environ.get("BARK_KEY", "") or "").strip()
        serverchan_sendkey = str(os.environ.get("SERVERCHAN_SENDKEY", "") or "").strip()
        silicon_key = str(os.environ.get("SILICONFLOW_API_KEY", "") or "").strip()
        future_host = str(os.environ.get("FUTU_HOST", "127.0.0.1") or "127.0.0.1").strip()
        future_port = str(os.environ.get("FUTU_PORT", "11111") or "11111").strip()

        runtime_settings = self.get_runtime_settings()
        return [
            {
                "name": "市场模式",
                "enabled": True,
                "detail": (
                    f"{runtime_settings.get('market_regime_label', '自动')} | "
                    f"{runtime_settings.get('market_regime_description', '')}"
                ),
            },
            {
                "name": "AI Agent",
                "enabled": str(os.environ.get("ENABLE_AI_AGENT", "false")).lower() == "true",
                "detail": f"模型: {os.environ.get('SILICONFLOW_MODEL', '未配置')}",
            },
            {
                "name": "大模型密钥",
                "enabled": bool(silicon_key),
                "detail": _mask_secret(silicon_key),
            },
            {
                "name": "资讯路由",
                "enabled": True,
                "detail": "财联社 + 全球市场缓存 + 巨潮公告",
            },
            {
                "name": "Bark 推送",
                "enabled": bool(bark_key) and bark_key != "changeme",
                "detail": _mask_secret(bark_key),
            },
            {
                "name": "Server酱 推送",
                "enabled": bool(serverchan_sendkey) and serverchan_sendkey != "changeme",
                "detail": _mask_secret(serverchan_sendkey),
            },
            {
                "name": "Futu OpenD",
                "enabled": bool(future_host and future_port),
                "detail": f"{future_host}:{future_port}",
            },
            {
                "name": "数据库",
                "enabled": Path(self.db_path).exists(),
                "detail": self.db_path,
            },
        ]

    def get_runtime_settings(self) -> Dict[str, object]:
        """
        获取运行时市场模式配置。
        """
        settings = get_runtime_settings(self.db_path)
        options = []
        for mode, item in MARKET_REGIME_MODE_OPTIONS.items():
            options.append(
                {
                    "value": mode,
                    "label": str(item.get("label", mode)),
                    "description": str(item.get("description", "") or ""),
                }
            )
        settings["options"] = options
        return settings

    def update_runtime_settings(self, market_regime_mode: str) -> Dict[str, object]:
        """
        更新运行时市场模式配置。
        """
        save_runtime_settings(self.db_path, market_regime_mode)
        return self.get_runtime_settings()

    def get_background_health(self) -> List[Dict]:
        """
        获取后台任务健康状态。
        """
        action_state = self.get_action_state()
        health_items = [
            self._build_background_health_item(
                action_name="refresh_market_cache",
                display_name="后台行情缓存",
                refresh_sec=max(30, int(os.environ.get("DASHBOARD_MARKET_REFRESH_SEC", "120") or "120")),
            ),
            self._build_background_health_item(
                action_name="refresh_signal_pool",
                display_name="后台信号池",
                refresh_sec=max(120, int(os.environ.get("DASHBOARD_SIGNAL_POOL_REFRESH_SEC", "900") or "900")),
            ),
        ]
        for item in health_items:
            state = action_state.get(item["action_name"], {})
            item["message"] = str(state.get("message", "") or "")
        return health_items

    def _build_background_health_item(self, action_name: str, display_name: str, refresh_sec: int) -> Dict[str, str]:
        """
        构建单个后台任务健康状态。
        """
        action_state = self.get_action_state()
        item = action_state.get(action_name, {})
        updated_at = str(item.get("updated_at", "") or "")
        status = str(item.get("status", "") or "")

        if not updated_at:
            return {
                "action_name": action_name,
                "name": display_name,
                "status": "empty",
                "label": "未运行",
                "detail": f"尚未写入状态，期望间隔 {refresh_sec} 秒",
                "message": "",
            }

        freshness = self._calc_freshness(updated_at, fresh_minutes=max(1, refresh_sec // 60 * 2), stale_minutes=max(2, refresh_sec // 60 * 6))
        if status == "running":
            label = "执行中"
            ui_status = "warning"
        elif status == "success" and freshness.get("status") in {"fresh", "aging"}:
            label = "正常"
            ui_status = "healthy" if freshness.get("status") == "fresh" else "warning"
        elif status == "failed":
            label = "失败"
            ui_status = "failed"
        else:
            label = "过期"
            ui_status = "failed"

        return {
            "action_name": action_name,
            "name": display_name,
            "status": ui_status,
            "label": label,
            "detail": f"最近更新: {updated_at} | 刷新间隔 {refresh_sec} 秒",
            "message": "",
        }

    def get_signal_pool(self, limit: int = 50) -> List[Dict]:
        """
        获取信号池。
        """
        return self.db.get_signal_pool(limit=limit)

    def get_signal_pool_all(self, limit: int = 100) -> Dict[str, object]:
        """
        获取按状态分组的信号池。
        """
        active_rows = self._filter_and_sort_signal_pool_rows(
            [self._decorate_signal_pool_row(row) for row in self.db.get_signal_pool(status="active", limit=limit)]
        )
        holding_rows = self._filter_and_sort_signal_pool_rows(
            [self._decorate_signal_pool_row(row) for row in self.db.get_signal_pool(status="holding", limit=limit)]
        )
        inactive_rows = self._filter_and_sort_signal_pool_rows(
            [self._decorate_signal_pool_row(row) for row in self.db.get_signal_pool_inactive_recent(limit=limit, days=3)]
        )
        counts = {
            "active": len(active_rows),
            "holding": len(holding_rows),
            "inactive": len(inactive_rows),
        }
        recent_changes = sorted(
            active_rows + holding_rows + inactive_rows,
            key=lambda item: str(item.get("updated_at", "") or ""),
            reverse=True,
        )[:8]
        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "counts": counts,
            "recent_changes": recent_changes,
            "groups": {
                "active": active_rows,
                "holding": holding_rows,
                "inactive": inactive_rows,
            },
        }

    def _filter_and_sort_signal_pool_rows(self, rows: List[Dict]) -> List[Dict]:
        """
        调整信号池展示顺序，并过滤掉无持仓支撑的卖出信号。
        """
        holding_codes = {
            str(item.get("code", "")).strip()
            for item in self.db.get_holdings_aggregated()
            if str(item.get("code", "")).strip()
        }

        filtered_rows = []
        for row in rows:
            signal_type = str(row.get("signal_type", "") or "").strip()
            code = str(row.get("code", "") or "").strip()
            if signal_type == "卖出" and code not in holding_codes:
                continue
            filtered_rows.append(row)

        return sorted(
            filtered_rows,
            key=lambda item: (
                self._signal_type_priority(str(item.get("signal_type", "") or "")),
                -float(item.get("score", 0.0) or 0.0),
                str(item.get("updated_at", "") or ""),
            ),
        )

    @staticmethod
    def _pick_latest_signal_pool_row(rows: List[Dict]) -> Optional[Dict]:
        """
        获取最近更新的一条信号池记录，用于 overview.latest.signal_pool_any。
        """
        if not rows:
            return None
        return sorted(
            rows,
            key=lambda item: (
                str(item.get("updated_at", "") or ""),
                str(item.get("created_at", "") or ""),
                int(item.get("id", 0) or 0),
            ),
            reverse=True,
        )[0]

    @staticmethod
    def _signal_type_priority(signal_type: str) -> int:
        """
        信号类型优先级，越小越靠前。
        """
        priority_map = {"买入": 0, "观望": 1, "卖出": 2}
        return priority_map.get(str(signal_type or "").strip(), 9)

    def _decorate_signal_pool_row(self, row: Dict) -> Dict:
        """
        为信号池记录补充展示字段。
        """
        item = dict(row)
        news_items = self._get_symbol_news_context_items()
        code = str(item.get("code", "") or "").strip()
        news_row = news_items.get(code, {}) if isinstance(news_items, dict) else {}
        status = str(item.get("status", "") or "").strip()
        created_at = self._parse_datetime_text(str(item.get("created_at", "") or ""))
        updated_at = self._parse_datetime_text(str(item.get("updated_at", "") or ""))
        age_minutes = None
        if updated_at is not None:
            age_minutes = max(0, int((datetime.now() - updated_at).total_seconds() // 60))

        status_label_map = {
            "active": "活跃跟踪",
            "holding": "已转持仓",
            "inactive": "已失效",
        }
        if status == "active":
            if created_at and updated_at and abs((updated_at - created_at).total_seconds()) <= 180:
                change_label = "新进池"
            elif age_minutes is not None and age_minutes <= 120:
                change_label = "近期更新"
            else:
                change_label = "持续跟踪"
        elif status == "holding":
            change_label = "转持仓"
        elif status == "inactive":
            change_label = "已失效"
        else:
            change_label = "状态未知"

        item["status_label"] = status_label_map.get(status, status or "-")
        item["change_label"] = change_label
        item["updated_minutes"] = age_minutes
        item["updated_label"] = self._format_recent_age(age_minutes)
        item["news_summary"] = str(news_row.get("news_summary", "") or "")
        item["news_text"] = str(news_row.get("news_text", "") or "")
        return item

    def _get_symbol_news_context_items(self) -> Dict[str, Dict]:
        """
        获取单票结构化资讯缓存。
        """
        payload = self.db.get_dashboard_cache("symbol_news_contexts") or {}
        items = payload.get("items", {}) if isinstance(payload, dict) else {}
        return items if isinstance(items, dict) else {}

    @staticmethod
    def _parse_datetime_text(text: str) -> Optional[datetime]:
        """
        解析日期时间文本。
        """
        raw_text = str(text or "").strip()
        if not raw_text:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                value = raw_text[:19] if "T" in raw_text else raw_text
                return datetime.strptime(value, fmt)
            except Exception:
                continue
        return None

    @staticmethod
    def _format_recent_age(age_minutes: Optional[int]) -> str:
        """
        格式化最近更新时间。
        """
        if age_minutes is None:
            return "-"
        if age_minutes < 1:
            return "刚刚更新"
        if age_minutes < 60:
            return f"{age_minutes} 分钟前"
        hours = age_minutes // 60
        if hours < 24:
            return f"{hours} 小时前"
        days = hours // 24
        return f"{days} 天前"

    def get_holdings(self) -> List[Dict]:
        """
        获取当前持仓。
        """
        rows = self.db.get_holdings_aggregated()
        ai_hints = self.db.get_dashboard_cache("position_ai_hints") or {}
        hint_items = ai_hints.get("items", {}) if isinstance(ai_hints, dict) else {}
        news_items = self._get_symbol_news_context_items()
        result: List[Dict] = []
        for row in rows:
            item = dict(row)
            code = str(item.get("code", "") or "").strip()
            hint_row = hint_items.get(code, {}) if isinstance(hint_items, dict) else {}
            news_row = news_items.get(code, {}) if isinstance(news_items, dict) else {}
            item["ai_hint"] = str(hint_row.get("ai_hint", "") or "")
            item["add_hint"] = str(hint_row.get("add_hint", "") or "")
            item["ai_hint_updated_at"] = str(hint_row.get("updated_at", "") or "")
            item["news_summary"] = str(news_row.get("news_summary", "") or "")
            item["news_text"] = str(news_row.get("news_text", "") or "")
            result.append(item)
        return result

    def get_recent_recommends(self, limit: int = 30) -> List[Dict]:
        """
        获取最近荐股记录。
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM recommends
            ORDER BY date DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
        records = [dict(row) for row in cursor.fetchall()]
        conn.close()
        if records:
            return records
        return self._build_fallback_recommends(limit=limit)

    def _build_fallback_recommends(self, limit: int = 30) -> List[Dict]:
        """
        当 recommends 为空时，回退为当前信号池中的买入/观望标的。
        """
        rows = [self._decorate_signal_pool_row(row) for row in self.db.get_signal_pool(status="active", limit=max(limit * 3, 50))]
        preferred_rows = [row for row in rows if str(row.get("signal_type", "")).strip() in {"买入", "观望"}]
        sorted_rows = sorted(
            preferred_rows,
            key=lambda item: (
                self._signal_type_priority(str(item.get("signal_type", "") or "")),
                -float(item.get("score", 0.0) or 0.0),
                str(item.get("updated_at", "") or ""),
            ),
        )

        results: List[Dict] = []
        for row in sorted_rows[:limit]:
            results.append(
                {
                    "id": row.get("id"),
                    "date": row.get("date"),
                    "code": row.get("code"),
                    "name": row.get("name"),
                    "price": float(row.get("price", 0.0) or 0.0),
                    "target_price": float(row.get("target_price", 0.0) or 0.0),
                    "stop_loss": float(row.get("stop_loss", 0.0) or 0.0),
                    "reason": row.get("reason", ""),
                    "signal_type": row.get("signal_type", ""),
                    "created_at": row.get("updated_at", "") or row.get("created_at", ""),
                    "source": "signal_pool_fallback",
                }
            )
        return results

    def get_stock_pool(self, limit: int = 50) -> List[Dict]:
        """
        获取当前股票池。
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM stock_pool
            ORDER BY score DESC, updated_at DESC, id DESC
            LIMIT ?
            """,
            (limit,),
        )
        records = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return records

    def get_trade_points(self, limit: int = 50) -> List[Dict]:
        """
        获取最近交易事件。
        """
        return self.db.get_trade_points(limit=limit)

    def get_signal_review(self, limit: int = 50) -> Dict[str, object]:
        """
        获取信号质量复盘统计。
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                r.id,
                r.date,
                r.code,
                r.name,
                r.price,
                r.target_price,
                r.stop_loss,
                r.reason,
                r.signal_type,
                t.date AS sell_date,
                t.price AS sell_price,
                t.pnl,
                t.pnl_pct,
                t.status AS trade_status,
                p.status AS position_status,
                p.buy_date AS position_buy_date
            FROM recommends r
            LEFT JOIN trades t
                ON t.recommend_id = r.id
               AND t.direction = 'sell'
            LEFT JOIN positions p
                ON (
                    p.recommend_id = r.id
                    OR (p.recommend_id IS NULL AND p.code = r.code AND p.status = 'holding')
                )
               AND p.status = 'holding'
            ORDER BY r.date DESC, r.id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()

        review_rows: List[Dict[str, object]] = []
        group_stats: Dict[str, Dict[str, float]] = {
            "etf": {"label": "ETF/LOF", "count": 0, "closed_count": 0, "win_count": 0, "total_pnl": 0.0, "total_pnl_pct": 0.0, "holding_days_sum": 0.0},
            "stock": {"label": "A股", "count": 0, "closed_count": 0, "win_count": 0, "total_pnl": 0.0, "total_pnl_pct": 0.0, "holding_days_sum": 0.0},
        }
        total_count = 0
        closed_count = 0
        open_count = 0
        win_count = 0
        total_pnl = 0.0
        total_pnl_pct = 0.0
        holding_days_sum = 0.0

        for row in rows:
            code = str(row.get("code", "") or "").strip()
            group_key = "etf" if code.startswith(("1", "5")) else "stock"
            group_item = group_stats[group_key]
            total_count += 1
            group_item["count"] += 1

            created_at = self._parse_datetime_text(str(row.get("date", "") or ""))
            sold_at = self._parse_datetime_text(str(row.get("sell_date", "") or ""))
            holding_days = 0
            effective_buy_at = self._parse_datetime_text(str(row.get("position_buy_date", "") or "")) or created_at
            if effective_buy_at and sold_at:
                holding_days = max(0, int((sold_at - effective_buy_at).total_seconds() // 86400))
            elif effective_buy_at and str(row.get("position_status", "") or "").strip() == "holding":
                holding_days = max(0, int((datetime.now() - effective_buy_at).total_seconds() // 86400))

            pnl = float(row.get("pnl", 0) or 0.0)
            pnl_pct = float(row.get("pnl_pct", 0) or 0.0)
            if sold_at:
                result_label = "已完成"
                closed_count += 1
                group_item["closed_count"] += 1
                total_pnl += pnl
                total_pnl_pct += pnl_pct
                holding_days_sum += holding_days
                group_item["total_pnl"] += pnl
                group_item["total_pnl_pct"] += pnl_pct
                group_item["holding_days_sum"] += holding_days
                if pnl > 0:
                    win_count += 1
                    group_item["win_count"] += 1
            elif str(row.get("position_status", "") or "").strip() == "holding":
                result_label = "持有中"
                open_count += 1
            else:
                result_label = "待触发"

            review_rows.append({
                "date": str(row.get("date", "") or ""),
                "code": code,
                "name": str(row.get("name", "") or ""),
                "pool_type": group_item["label"],
                "signal_type": str(row.get("signal_type", "") or ""),
                "buy_price": float(row.get("price", 0) or 0.0),
                "sell_price": float(row.get("sell_price", 0) or 0.0),
                "target_price": float(row.get("target_price", 0) or 0.0),
                "stop_loss": float(row.get("stop_loss", 0) or 0.0),
                "holding_days": holding_days,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "result_label": result_label,
                "reason": str(row.get("reason", "") or ""),
            })

        group_rows = []
        for group_key in ["etf", "stock"]:
            item = group_stats[group_key]
            closed_num = int(item["closed_count"])
            group_rows.append({
                "group": item["label"],
                "count": int(item["count"]),
                "closed_count": closed_num,
                "win_rate": (float(item["win_count"]) / closed_num * 100) if closed_num > 0 else 0.0,
                "avg_pnl": (float(item["total_pnl"]) / closed_num) if closed_num > 0 else 0.0,
                "avg_pnl_pct": (float(item["total_pnl_pct"]) / closed_num) if closed_num > 0 else 0.0,
                "avg_holding_days": (float(item["holding_days_sum"]) / closed_num) if closed_num > 0 else 0.0,
            })

        summary = {
            "total_count": total_count,
            "closed_count": closed_count,
            "open_count": open_count,
            "win_rate": (win_count / closed_count * 100) if closed_count > 0 else 0.0,
            "avg_pnl": (total_pnl / closed_count) if closed_count > 0 else 0.0,
            "avg_pnl_pct": (total_pnl_pct / closed_count) if closed_count > 0 else 0.0,
            "avg_holding_days": (holding_days_sum / closed_count) if closed_count > 0 else 0.0,
        }
        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "summary": summary,
            "groups": group_rows,
            "records": review_rows,
        }

    def get_timing_review(self, limit: int = 100) -> Dict[str, object]:
        """
        获取择时卖出复盘统计。
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                tp.date,
                tp.code,
                tp.name,
                tp.reason,
                tp.status,
                tp.metadata,
                t.price AS sell_price,
                t.pnl,
                t.pnl_pct,
                r.price AS buy_price,
                r.date AS recommend_date
            FROM trade_points tp
            LEFT JOIN trades t
                ON t.code = tp.code
               AND t.direction = 'sell'
               AND t.date = tp.date
            LEFT JOIN recommends r
                ON r.id = tp.recommend_id
            WHERE tp.event_type IN ('sell', 'scale_out')
            ORDER BY tp.date DESC, tp.id DESC
            LIMIT ?
            """,
            (limit,),
        )
        rows = [dict(row) for row in cursor.fetchall()]
        conn.close()

        summary = {
            "total_count": 0,
            "win_rate": 0.0,
            "avg_pnl": 0.0,
            "avg_pnl_pct": 0.0,
        }
        reason_stats: Dict[str, Dict[str, float]] = {}
        records: List[Dict[str, object]] = []
        total_pnl = 0.0
        total_pnl_pct = 0.0
        win_count = 0

        for row in rows:
            reason_text = str(row.get("reason", "") or "").strip() or "未分类"
            pnl = float(row.get("pnl", 0) or 0.0)
            pnl_pct = float(row.get("pnl_pct", 0) or 0.0)
            summary["total_count"] += 1
            total_pnl += pnl
            total_pnl_pct += pnl_pct
            if pnl > 0:
                win_count += 1

            reason_item = reason_stats.setdefault(
                reason_text,
                {"count": 0, "win_count": 0, "total_pnl": 0.0, "total_pnl_pct": 0.0},
            )
            reason_item["count"] += 1
            reason_item["total_pnl"] += pnl
            reason_item["total_pnl_pct"] += pnl_pct
            if pnl > 0:
                reason_item["win_count"] += 1

            records.append({
                "date": str(row.get("date", "") or ""),
                "code": str(row.get("code", "") or ""),
                "name": str(row.get("name", "") or ""),
                "reason": reason_text,
                "sell_price": float(row.get("sell_price", 0) or 0.0),
                "buy_price": float(row.get("buy_price", 0) or 0.0),
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "status": str(row.get("status", "") or ""),
            })

        total_count = int(summary["total_count"])
        if total_count > 0:
            summary["win_rate"] = win_count / total_count * 100
            summary["avg_pnl"] = total_pnl / total_count
            summary["avg_pnl_pct"] = total_pnl_pct / total_count

        groups = []
        for reason_text, item in sorted(reason_stats.items(), key=lambda kv: (-kv[1]["count"], kv[0])):
            count = int(item["count"])
            groups.append({
                "reason": reason_text,
                "count": count,
                "win_rate": (float(item["win_count"]) / count * 100) if count > 0 else 0.0,
                "avg_pnl": (float(item["total_pnl"]) / count) if count > 0 else 0.0,
                "avg_pnl_pct": (float(item["total_pnl_pct"]) / count) if count > 0 else 0.0,
            })

        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "summary": summary,
            "groups": groups,
            "records": records,
        }

    @staticmethod
    def _build_timing_experiment_conclusion(rows: List[Dict[str, object]]) -> Dict[str, str]:
        """
        根据择时参数试验结果生成结论。
        """
        if not rows:
            return {
                "title": "暂无试验结果",
                "summary": "当前还没有可用的择时参数试验数据。",
                "recommendation": "请先刷新信号池和择时试验缓存。",
            }

        valid_rows = [row for row in rows if not row.get("error")]
        if not valid_rows:
            return {
                "title": "试验执行失败",
                "summary": "所有择时参数方案都未能正常给出结果。",
                "recommendation": "优先检查持仓数据、信号池和第三方行情接口。",
            }

        dominant_reasons: Dict[str, int] = {}
        best_row: Optional[Dict[str, object]] = None
        best_score: Optional[tuple] = None
        stable_row: Optional[Dict[str, object]] = None
        stable_sell_count: Optional[int] = None

        for row in valid_rows:
            sell_count = int(row.get("sell_count", 0) or 0)
            hold_count = int(row.get("hold_count", 0) or 0)
            avg_sell_pnl_pct = float(row.get("avg_sell_pnl_pct", 0.0) or 0.0)
            score = (hold_count, avg_sell_pnl_pct, -sell_count)
            if best_score is None or score > best_score:
                best_score = score
                best_row = row
            if stable_sell_count is None or sell_count < stable_sell_count:
                stable_sell_count = sell_count
                stable_row = row

            for reason_text, count in dict(row.get("reason_counts", {})).items():
                dominant_reasons[str(reason_text)] = dominant_reasons.get(str(reason_text), 0) + int(count or 0)

        dominant_reason = ""
        if dominant_reasons:
            dominant_reason = max(dominant_reasons.items(), key=lambda item: item[1])[0]

        best_name = str((best_row or {}).get("name", "") or "")
        stable_name = str((stable_row or {}).get("name", "") or "")

        if dominant_reason:
            summary = f"当前持仓在多套择时参数下，主导的卖出原因是“{dominant_reason}”。"
        else:
            summary = "当前持仓在多套择时参数下，卖出原因还不够集中。"

        if best_name and stable_name and best_name != stable_name:
            recommendation = f"综合对比看，“{best_name}”更偏向保留收益空间，“{stable_name}”更偏向稳定控制卖出数量，可优先围绕这两套参数继续回测。"
        elif best_name:
            recommendation = f"当前可优先关注“{best_name}”方案，它在持有数量、建议卖出数量和收益空间之间最平衡。"
        else:
            recommendation = "当前样本还不够充分，建议先继续积累卖出样本，再决定参数优化方向。"

        return {
            "title": "择时参数对比结论",
            "summary": summary,
            "recommendation": recommendation,
        }

    def refresh_timing_experiments(self) -> Dict[str, object]:
        """
        刷新择时参数试验缓存。
        """
        from trading.simulate_trading import SimulateTrader

        scenarios = [
            {
                "name": "当前参数",
                "description": "保持当前默认择时参数",
                "overrides": {},
            },
            {
                "name": "保守止损",
                "description": "更紧的止损与更短时间止损，优先降低回撤",
                "overrides": {
                    "trailing_stop": 0.04,
                    "time_stop_days": 1,
                    "time_stop_min_return": 0.01,
                    "entry_low_stop_buffer": 0.0,
                },
            },
            {
                "name": "趋势持有",
                "description": "放宽跟踪止盈和时间止损，争取大波段",
                "overrides": {
                    "trailing_stop": 0.08,
                    "time_stop_days": 3,
                    "time_stop_min_return": -0.01,
                    "override_trailing_stop": 0.10,
                },
            },
            {
                "name": "平衡止盈",
                "description": "更早分批止盈，兼顾胜率和收益兑现",
                "overrides": {
                    "scale_out_levels": [0.08, 0.16],
                    "trailing_stop": 0.05,
                    "time_stop_days": 2,
                    "time_stop_min_return": 0.0,
                },
            },
        ]

        rows: List[Dict[str, object]] = []
        for item in scenarios:
            trader: Optional[SimulateTrader] = None
            try:
                trader = SimulateTrader(db_path=self.db_path, risk_overrides=item["overrides"])
                result = trader.preview_timing_decisions(limit=20)
                rows.append({
                    "name": item["name"],
                    "description": item["description"],
                    "holding_count": int(result["summary"].get("holding_count", 0)),
                    "sell_count": int(result["summary"].get("sell_count", 0)),
                    "hold_count": int(result["summary"].get("hold_count", 0)),
                    "avg_sell_pnl_pct": float(result["summary"].get("avg_sell_pnl_pct", 0.0)),
                    "reason_counts": result.get("reason_counts", {}),
                    "decisions": result.get("decisions", []),
                })
            except Exception as e:
                rows.append({
                    "name": item["name"],
                    "description": item["description"],
                    "holding_count": 0,
                    "sell_count": 0,
                    "hold_count": 0,
                    "avg_sell_pnl_pct": 0.0,
                    "reason_counts": {"????": 1},
                    "decisions": [],
                    "error": str(e),
                })
            finally:
                try:
                    if trader and getattr(trader, "data_source", None):
                        trader.data_source.close()
                except Exception:
                    pass

        result = {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "conclusion": self._build_timing_experiment_conclusion(rows),
            "scenarios": rows,
        }
        self.db.set_dashboard_cache("timing_experiments", result)
        return result

    def get_timeline(self, limit: int = 100) -> List[Dict]:
        """
        获取时间线。
        """
        return self.db.get_trade_timeline(limit=limit)

    def get_recent_logs(self, limit: int = 80) -> List[str]:
        """
        获取最近日志。
        """
        log_dir = BASE_DIR / "logs"
        if not log_dir.exists():
            return []

        log_files = sorted(log_dir.glob("*.log"), key=lambda item: item.stat().st_mtime, reverse=True)
        if not log_files:
            return []

        try:
            lines = log_files[0].read_text(encoding="utf-8", errors="ignore").splitlines()
            return lines[-limit:]
        except Exception as e:
            logger.warning(f"读取日志失败: {e}")
            return []

    def get_review_report(self) -> Dict[str, object]:
        """
        获取综合复盘报告。
        """
        report = build_runtime_review_report(self)
        self.db.set_dashboard_cache("review_report", report)
        return report

    def get_dynamic_params(self) -> Dict[str, object]:
        """获取动态参数"""
        try:
            from data.recommend_db import DynamicParamsDB
            dp_db = DynamicParamsDB()
            return {"ok": True, "params": dp_db.get_all_params()}
        except Exception as e:
            logger.warning(f"获取动态参数失败: {e}")
            return {"ok": False, "error": str(e)}

    def set_dynamic_param(self, payload: Dict) -> Dict:
        """设置动态参数"""
        try:
            key = payload.get("param_key")
            value = payload.get("param_value")
            reason = payload.get("reason", "看板调整")
            if not key or value is None:
                return {"ok": False, "error": "缺少 param_key 或 param_value"}
            
            from data.recommend_db import DynamicParamsDB
            dp_db = DynamicParamsDB()
            old_value = dp_db.get_param(key)
            dp_db.set_param(key, float(value), reason, "manual")
            return {"ok": True, "param_key": key, "old_value": old_value, "new_value": value}
        except Exception as e:
            logger.warning(f"设置动态参数失败: {e}")
            return {"ok": False, "error": str(e)}

    def get_override_history(self, limit: int = 20) -> Dict:
        """获取人工干预历史"""
        try:
            from data.recommend_db import ManualOverrideDB
            override_db = ManualOverrideDB()
            overrides = override_db.get_overrides(limit=limit)
            return {"ok": True, "overrides": overrides}
        except Exception as e:
            logger.warning(f"获取干预历史失败: {e}")
            return {"ok": False, "error": str(e)}

    def get_daily_optimization(self) -> Dict:
        """获取每日优化结果"""
        try:
            from data.recommend_db import DailyOptimizationDB
            opt_db = DailyOptimizationDB()
            latest = opt_db.get_latest_optimization()
            history = opt_db.get_optimization_history(limit=5)
            
            if not latest:
                return {
                    "ok": True,
                    "has_data": False,
                    "message": "暂无优化结果，将在15:30自动执行"
                }
            
            return {
                "ok": True,
                "has_data": True,
                "latest": latest,
                "history": history,
            }
        except Exception as e:
            logger.warning(f"获取优化结果失败: {e}")
            return {"ok": False, "error": str(e)}

    def get_etf_pool_status(self) -> Dict:
        """获取ETF池状态"""
        try:
            from data.data_source import get_etf_pool_cache
            cache = get_etf_pool_cache()
            status = cache.get_status()
            return {
                "ok": True,
                "status": status
            }
        except Exception as e:
            logger.warning(f"获取ETF池状态失败: {e}")
            return {"ok": False, "error": str(e)}

    def add_override(self, payload: Dict) -> Dict:
        """添加人工干预"""
        try:
            signal_id = payload.get("signal_id")
            action = payload.get("action")
            reason = payload.get("reason", "看板干预")
            if not signal_id or not action:
                return {"ok": False, "error": "缺少 signal_id 或 action"}
            
            from data.recommend_db import ManualOverrideDB
            override_db = ManualOverrideDB()
            result = override_db.add_override(
                signal_id=signal_id,
                original_action="ai_decision",
                override_action=action,
                override_reason=reason,
                operator="human_dashboard"
            )
            return {"ok": True, "override_id": result, "signal_id": signal_id, "action": action}
        except Exception as e:
            logger.warning(f"添加干预失败: {e}")
            return {"ok": False, "error": str(e)}


class DashboardHandler(BaseHTTPRequestHandler):
    """HTTP 请求处理器。"""

    service = DashboardService()

    def do_GET(self):
        """
        处理 GET 请求。
        """
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/":
            return self._serve_html()
        if path == "/api/health":
            return self._send_json({"ok": True, "time": datetime.now().isoformat()})
        if path == "/api/overview":
            return self._send_json(self.service.get_overview())
        if path == "/api/runtime-config":
            return self._send_json(self.service.get_runtime_settings())
        if path == "/api/market":
            return self._send_json(self.service.get_market_cards())
        if path == "/api/action-status":
            return self._send_json(self.service.get_action_state())
        if path == "/api/signal-pool":
            limit = self._parse_limit(query, default_value=50)
            return self._send_json(self.service.get_signal_pool(limit=limit))
        if path == "/api/signal-pool-all":
            limit = self._parse_limit(query, default_value=100)
            return self._send_json(self.service.get_signal_pool_all(limit=limit))
        if path == "/api/holdings":
            return self._send_json(self.service.get_holdings())
        if path == "/api/stock-pool":
            limit = self._parse_limit(query, default_value=50)
            return self._send_json(self.service.get_stock_pool(limit=limit))
        if path == "/api/recommends":
            limit = self._parse_limit(query, default_value=30)
            return self._send_json(self.service.get_recent_recommends(limit=limit))
        if path == "/api/signal-review":
            limit = self._parse_limit(query, default_value=50)
            return self._send_json(self.service.get_signal_review(limit=limit))
        if path == "/api/timing-review":
            limit = self._parse_limit(query, default_value=100)
            return self._send_json(self.service.get_timing_review(limit=limit))
        if path == "/api/timing-experiments":
            return self._send_json(self.service.get_timing_experiments())
        if path == "/api/strategy-tuning":
            return self._send_json(self.service.get_strategy_tuning())
        if path == "/api/trade-points":
            limit = self._parse_limit(query, default_value=50)
            return self._send_json(self.service.get_trade_points(limit=limit))
        if path == "/api/timeline":
            limit = self._parse_limit(query, default_value=100)
            return self._send_json(self.service.get_timeline(limit=limit))
        if path == "/api/logs":
            limit = self._parse_limit(query, default_value=80)
            return self._send_json(self.service.get_recent_logs(limit=limit))
        if path == "/api/review-report":
            return self._send_json(self.service.get_review_report())
        if path == "/api/dynamic-params":
            return self._send_json(self.service.get_dynamic_params())
        if path == "/api/override-history":
            limit = self._parse_limit(query, default_value=20)
            return self._send_json(self.service.get_override_history(limit=limit))
        if path == "/api/daily-optimization":
            return self._send_json(self.service.get_daily_optimization())
        if path == "/api/etf-pool-status":
            return self._send_json(self.service.get_etf_pool_status())

        self._send_json({"ok": False, "error": f"未知路径: {path}"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self):
        """
        处理 POST 请求。
        """
        parsed = urlparse(self.path)
        if parsed.path == "/api/runtime-config":
            content_length = int(self.headers.get("Content-Length", "0") or 0)
            raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"
            try:
                payload = json.loads(raw_body.decode("utf-8") or "{}")
            except Exception:
                payload = {}
            market_regime_mode = str(payload.get("market_regime_mode", "")).strip()
            if not market_regime_mode:
                return self._send_json({"ok": False, "error": "缺少 market_regime_mode"}, status=HTTPStatus.BAD_REQUEST)
            settings = self.service.update_runtime_settings(market_regime_mode)
            return self._send_json({"ok": True, "message": f"市场模式已切换为 {settings.get('market_regime_label', market_regime_mode)}", "settings": settings})

        if parsed.path == "/api/set-param":
            content_length = int(self.headers.get("Content-Length", "0") or 0)
            raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"
            try:
                payload = json.loads(raw_body.decode("utf-8") or "{}")
            except Exception:
                payload = {}
            result = self.service.set_dynamic_param(payload)
            return self._send_json(result)

        if parsed.path == "/api/add-override":
            content_length = int(self.headers.get("Content-Length", "0") or 0)
            raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"
            try:
                payload = json.loads(raw_body.decode("utf-8") or "{}")
            except Exception:
                payload = {}
            result = self.service.add_override(payload)
            return self._send_json(result)

        if parsed.path != "/api/action":
            return self._send_json({"ok": False, "error": f"未知路径: {parsed.path}"}, status=HTTPStatus.NOT_FOUND)

        content_length = int(self.headers.get("Content-Length", "0") or 0)
        raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            payload = json.loads(raw_body.decode("utf-8") or "{}")
        except Exception:
            payload = {}

        action = str(payload.get("action", "")).strip()
        result = self.service.run_action(action)
        status = HTTPStatus.OK if result.get("ok") else HTTPStatus.BAD_REQUEST
        return self._send_json(result, status=status)

    def log_message(self, format_text: str, *args):
        """
        接管默认 HTTP 访问日志。
        """
        logger.info("%s - %s" % (self.address_string(), format_text % args))

    def _serve_html(self):
        """
        返回首页。
        """
        if not HTML_PATH.exists():
            self.send_error(HTTPStatus.NOT_FOUND, "dashboard 页面文件不存在")
            return

        try:
            content = HTML_PATH.read_text(encoding="utf-8")
            payload = content.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self._send_no_cache_headers()
            self.end_headers()
            self.wfile.write(payload)
        except Exception as e:
            logger.error(f"读取页面失败: {e}")
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, f"读取页面失败: {e}")

    def _send_json(self, data, status: HTTPStatus = HTTPStatus.OK):
        """
        返回 JSON。
        """
        payload = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self._send_no_cache_headers()
        self.end_headers()
        self.wfile.write(payload)

    def _send_no_cache_headers(self):
        """
        发送防缓存响应头。
        """
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")

    @staticmethod
    def _parse_limit(query: Dict[str, List[str]], default_value: int) -> int:
        """
        解析分页数量。
        """
        raw_value = str((query.get("limit") or [default_value])[0]).strip()
        try:
            value = int(raw_value)
            return max(1, min(value, 500))
        except Exception:
            return default_value


class DashboardBackgroundUpdater:
    """看板后台定时更新器。"""

    def __init__(self, service: DashboardService):
        self.service = service
        self.market_refresh_sec = max(30, int(os.environ.get("DASHBOARD_MARKET_REFRESH_SEC", "120") or "120"))
        self.signal_pool_refresh_sec = max(120, int(os.environ.get("DASHBOARD_SIGNAL_POOL_REFRESH_SEC", "900") or "900"))
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        """
        启动后台更新线程。
        """
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="dashboard-background-updater", daemon=True)
        self._thread.start()
        logger.info(
            f"看板后台更新器已启动，行情缓存刷新间隔: {self.market_refresh_sec} 秒，"
            f"信号池刷新间隔: {self.signal_pool_refresh_sec} 秒"
        )

    def stop(self) -> None:
        """
        停止后台更新线程。
        """
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)

    def _run(self) -> None:
        """
        定时刷新看板缓存。
        """
        self._refresh_market_cache(initial_run=True)
        self._refresh_signal_pool(initial_run=True)
        self._refresh_timing_experiments(initial_run=True)
        next_market_at = time.time() + self.market_refresh_sec
        next_signal_pool_at = time.time() + self.signal_pool_refresh_sec
        next_timing_experiments_at = time.time() + self.signal_pool_refresh_sec

        while not self._stop_event.wait(1):
            now_ts = time.time()
            if now_ts >= next_market_at:
                self._refresh_market_cache(initial_run=False)
                next_market_at = now_ts + self.market_refresh_sec
            if now_ts >= next_signal_pool_at:
                self._refresh_signal_pool(initial_run=False)
                next_signal_pool_at = now_ts + self.signal_pool_refresh_sec
            if now_ts >= next_timing_experiments_at:
                self._refresh_timing_experiments(initial_run=False)
                next_timing_experiments_at = now_ts + self.signal_pool_refresh_sec

    def _refresh_market_cache(self, initial_run: bool) -> None:
        """
        刷新行情缓存并写入状态。
        """
        try:
            market = self.service.refresh_market_cache()
            message = (
                f"后台定时刷新完成：指数{len(market.get('indices', []))}项，"
                f"ETF{len(market.get('etfs', []))}项，持仓{len(market.get('holdings', []))}项"
            )
            self.service.mark_action_state("refresh_market_cache", "success", message)
            logger.info(message if not initial_run else f"看板启动预热完成：{message}")
        except Exception as e:
            logger.warning(f"后台定时刷新行情缓存失败: {e}")
            self.service.mark_action_state("refresh_market_cache", "failed", f"后台刷新失败: {e}")

    def _refresh_signal_pool(self, initial_run: bool) -> None:
        """
        刷新信号池并写入状态。
        """
        pusher: Optional[ScheduledPusher] = None
        try:
            pusher = ScheduledPusher()
            result = pusher.refresh_signal_pool(etf_count=5, stock_count=5, reload_pool=True)
            message = self.service._format_signal_pool_refresh_message(result, prefix="后台定时刷新信号池完成")
            self.service.mark_action_state("refresh_signal_pool", "success", message)
            logger.info(message if not initial_run else f"看板信号池预热完成：{message}")
        except Exception as e:
            logger.warning(f"后台定时刷新信号池失败: {e}")
            self.service.mark_action_state("refresh_signal_pool", "failed", f"后台刷新失败: {e}")
        finally:
            try:
                if pusher and getattr(pusher, "data_source", None):
                    pusher.data_source.close()
            except Exception:
                pass

    def _refresh_timing_experiments(self, initial_run: bool) -> None:
        """
        刷新择时参数试验缓存并写入状态。
        """
        try:
            result = self.service.refresh_timing_experiments()
            scenarios = result.get("scenarios", []) if isinstance(result, dict) else []
            message = f"后台定时刷新择时参数试验完成：共 {len(scenarios)} 组方案"
            self.service.mark_action_state("refresh_timing_experiments", "success", message)
            logger.info(message if not initial_run else f"看板择时试验预热完成：{message}")
        except Exception as e:
            logger.warning(f"后台定时刷新择时参数试验失败: {e}")
            self.service.mark_action_state("refresh_timing_experiments", "failed", f"后台刷新失败: {e}")


def main():
    """
    启动看板服务。
    """
    parser = argparse.ArgumentParser(description="量化信号与持仓看板")
    parser.add_argument("--host", default="0.0.0.0", help="监听地址，默认 0.0.0.0")
    parser.add_argument(
        "--db-path",
        default=os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db"),
        help="SQLite 数据库路径",
    )
    args = parser.parse_args()

    DashboardHandler.service = DashboardService(db_path=args.db_path)
    background_updater = DashboardBackgroundUpdater(DashboardHandler.service)
    background_updater.start()
    server = ThreadingHTTPServer((args.host, DASHBOARD_PORT), DashboardHandler)

    logger.info(f"看板服务启动: http://{args.host}:{DASHBOARD_PORT}")
    logger.info(f"数据库路径(传入): {args.db_path}")
    logger.info(f"数据库路径(实际): {DashboardHandler.service.db_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("收到停止信号，准备退出看板服务")
    finally:
        background_updater.stop()
        server.server_close()


if __name__ == "__main__":
    main()
