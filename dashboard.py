# -*- coding: utf-8 -*-
"""
轻量级量化看板服务
"""
import argparse
import json
import os
import sqlite3
import threading
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from dotenv import load_dotenv

from data import DataSource
from data.recommend_db import RecommendDB
from docker_start import ScheduledPusher
from utils.logger import get_logger

load_dotenv()
load_dotenv(".env.local", override=True)

logger = get_logger(__name__)

BASE_DIR = Path(__file__).resolve().parent
HTML_PATH = BASE_DIR / "dashboard" / "index.html"
DASHBOARD_PORT = 18675


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
        return actions if isinstance(actions, dict) else {}

    def _resolve_db_path(self, preferred_path: str) -> str:
        """
        解析数据库路径，兼容旧版 data 目录挂载。
        """
        candidates = []
        raw_preferred = str(preferred_path or "").strip()
        if raw_preferred:
            candidates.append(raw_preferred)

        for fallback in [
            "./runtime/data/recommend.db",
            "./data/recommend.db",
            "/app/runtime/data/recommend.db",
            "/app/data/recommend.db",
        ]:
            if fallback not in candidates:
                candidates.append(fallback)

        best_path = raw_preferred or candidates[0]
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
        stock_pool = self.get_stock_pool(limit=100)
        stats = self.db.get_statistics()
        recommendations = self.get_recent_recommends(limit=20)
        trade_points = self.db.get_trade_points(limit=100)
        action_state = self.get_action_state()
        latest_actions = {
            "refresh_market_cache_at": self._get_action_updated_at(action_state, "refresh_market_cache"),
            "refresh_pool_at": self._get_action_updated_at(action_state, "refresh_pool"),
            "push_once_at": self._get_action_updated_at(action_state, "push_once"),
            "push_intraday_alert_at": self._get_action_updated_at(action_state, "push_intraday_alert"),
        }
        freshness = {
            "market_cache_freshness": self._calc_freshness(self.get_market_cards().get("generated_at", ""), fresh_minutes=5, stale_minutes=20),
            "stock_pool_freshness": self._calc_freshness(stock_pool[0].get("updated_at", "") if stock_pool else "", fresh_minutes=720, stale_minutes=1440),
            "signal_pool_freshness": self._calc_freshness(signal_pool[0].get("updated_at", "") if signal_pool else "", fresh_minutes=240, stale_minutes=720),
        }

        return {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "database_path": self.db_path,
            "summary": {
                "holding_count": len(holdings),
                "signal_pool_count": len(signal_pool),
                "stock_pool_count": len(stock_pool),
                "recommend_count": len(recommendations),
                "trade_event_count": len(trade_points),
                "sell_trade_count": int(stats.get("total_trades", 0) or 0),
                "win_rate": float(stats.get("win_rate", 0.0) or 0.0),
                "total_pnl": float(stats.get("total_pnl", 0.0) or 0.0),
                **latest_actions,
                **freshness,
            },
            "features": self.get_feature_status(),
            "latest": {
                "recommend": recommendations[0] if recommendations else None,
                "trade_point": trade_points[0] if trade_points else None,
                "signal_pool": signal_pool[0] if signal_pool else None,
                "stock_pool": stock_pool[0] if stock_pool else None,
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

    def run_action(self, action: str) -> Dict:
        """
        执行看板操作。
        """
        action_name = str(action or "").strip()
        if not action_name:
            return {"ok": False, "message": "缺少 action 参数"}

        allowed_actions = {"refresh_pool", "refresh_market_cache", "push_once", "push_intraday_alert"}
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

            pusher = ScheduledPusher()
            if action_name == "refresh_pool":
                pusher.update_stock_pool(merge_existing=False)
                self._set_action_state(action_name, "success", "股票池刷新完成")
                return
            if action_name == "push_once":
                success = bool(pusher.push_once())
                self._set_action_state(action_name, "success" if success else "failed", "完整推送完成" if success else "完整推送执行失败")
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
            return dict(self._action_state)

    def get_feature_status(self) -> List[Dict]:
        """
        获取功能开关和配置状态。
        """
        bark_key = str(os.environ.get("BARK_KEY", "") or "").strip()
        silicon_key = str(os.environ.get("SILICONFLOW_API_KEY", "") or "").strip()
        em_key = str(os.environ.get("EM_API_KEY", "") or "").strip()
        future_host = str(os.environ.get("FUTU_HOST", "127.0.0.1") or "127.0.0.1").strip()
        future_port = str(os.environ.get("FUTU_PORT", "11111") or "11111").strip()

        return [
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
                "name": "妙想技能",
                "enabled": bool(em_key),
                "detail": _mask_secret(em_key),
            },
            {
                "name": "Bark 推送",
                "enabled": bool(bark_key) and bark_key != "changeme",
                "detail": _mask_secret(bark_key),
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

    def get_signal_pool(self, limit: int = 50) -> List[Dict]:
        """
        获取信号池。
        """
        return self.db.get_signal_pool(limit=limit)

    def get_holdings(self) -> List[Dict]:
        """
        获取当前持仓。
        """
        return self.db.get_holdings_aggregated()

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
        return records

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
        if path == "/api/market":
            return self._send_json(self.service.get_market_cards())
        if path == "/api/action-status":
            return self._send_json(self.service.get_action_state())
        if path == "/api/signal-pool":
            limit = self._parse_limit(query, default_value=50)
            return self._send_json(self.service.get_signal_pool(limit=limit))
        if path == "/api/holdings":
            return self._send_json(self.service.get_holdings())
        if path == "/api/stock-pool":
            limit = self._parse_limit(query, default_value=50)
            return self._send_json(self.service.get_stock_pool(limit=limit))
        if path == "/api/recommends":
            limit = self._parse_limit(query, default_value=30)
            return self._send_json(self.service.get_recent_recommends(limit=limit))
        if path == "/api/trade-points":
            limit = self._parse_limit(query, default_value=50)
            return self._send_json(self.service.get_trade_points(limit=limit))
        if path == "/api/timeline":
            limit = self._parse_limit(query, default_value=100)
            return self._send_json(self.service.get_timeline(limit=limit))
        if path == "/api/logs":
            limit = self._parse_limit(query, default_value=80)
            return self._send_json(self.service.get_recent_logs(limit=limit))

        self._send_json({"ok": False, "error": f"未知路径: {path}"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self):
        """
        处理 POST 请求。
        """
        parsed = urlparse(self.path)
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
    server = ThreadingHTTPServer((args.host, DASHBOARD_PORT), DashboardHandler)

    logger.info(f"看板服务启动: http://{args.host}:{DASHBOARD_PORT}")
    logger.info(f"数据库路径: {args.db_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("收到停止信号，准备退出看板服务")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
