# -*- coding: utf-8 -*-
"""
荐股数据库模块
使用SQLite存储荐股记录和模拟交易数据
SQLite是Python内置的，无需单独安装
"""
import os
import json
import sqlite3
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List, Optional
from utils.logger import get_logger

logger = get_logger(__name__)


def resolve_db_path(db_path: Optional[str] = None) -> str:
    """解析统一的数据库路径。"""
    normalized_path = str(db_path or "").strip()
    if normalized_path:
        return normalized_path
    return os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db")


@dataclass
class RecommendRecord:
    """荐股记录"""
    id: Optional[int] = None
    date: str = ""  # 推荐日期 YYYY-MM-DD
    code: str = ""   # 股票代码
    name: str = ""   # 股票名称
    price: float = 0.0  # 买入价格
    target_price: float = 0.0  # 目标价格
    stop_loss: float = 0.0  # 止损价格
    reason: str = ""  # 推荐理由
    signal_type: str = "买入"  # 信号类型
    created_at: str = ""


@dataclass
class TradeRecord:
    """交易记录"""
    id: Optional[int] = None
    recommend_id: int = 0  # 关联的荐股记录ID
    date: str = ""  # 交易日期
    code: str = ""  # 股票代码
    name: str = ""  # 股票名称
    direction: str = ""  # buy/sell
    price: float = 0.0  # 成交价格
    quantity: int = 0  # 成交数量
    amount: float = 0.0  # 成交金额
    commission: float = 0.0  # 手续费
    pnl: float = 0.0  # 收益(仅卖出时)
    pnl_pct: float = 0.0  # 收益率(%)
    status: str = "holding"  # holding/sold
    created_at: str = ""


@dataclass
class TradePointRecord:
    """买卖点/信号事件记录"""
    id: Optional[int] = None
    recommend_id: int = 0
    date: str = ""
    code: str = ""
    name: str = ""
    event_type: str = ""  # recommend/buy/sell/scale_out/skip
    signal_type: str = ""  # 买入/卖出/观望
    price: float = 0.0
    target_price: float = 0.0
    stop_loss: float = 0.0
    quantity: int = 0
    reason: str = ""
    source: str = ""
    status: str = ""
    metadata: str = ""
    created_at: str = ""


@dataclass
class SignalPoolRecord:
    """信号池记录"""
    id: Optional[int] = None
    date: str = ""
    code: str = ""
    name: str = ""
    pool_type: str = ""
    signal_type: str = ""
    price: float = 0.0
    target_price: float = 0.0
    stop_loss: float = 0.0
    reason: str = ""
    score: float = 0.0
    source: str = ""
    status: str = "active"
    metadata: str = ""
    created_at: str = ""
    updated_at: str = ""


class RecommendDB:
    """荐股数据库"""
    
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = resolve_db_path(db_path)
        os.makedirs(os.path.dirname(self.db_path) if os.path.dirname(self.db_path) else "./data", exist_ok=True)
        self._init_db()
    
    def _get_conn(self) -> sqlite3.Connection:
        """获取数据库连接"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def _init_db(self):
        """初始化数据库表"""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        # 荐股记录表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS recommends (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT,
                price REAL,
                target_price REAL,
                stop_loss REAL,
                reason TEXT,
                signal_type TEXT DEFAULT '买入',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 交易记录表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recommend_id INTEGER,
                date TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT,
                direction TEXT NOT NULL,
                price REAL NOT NULL,
                quantity INTEGER,
                amount REAL,
                commission REAL DEFAULT 0,
                pnl REAL DEFAULT 0,
                pnl_pct REAL DEFAULT 0,
                status TEXT DEFAULT 'holding',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (recommend_id) REFERENCES recommends(id)
            )
        """)
        
        # 持仓记录表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recommend_id INTEGER,
                code TEXT NOT NULL,
                name TEXT,
                buy_date TEXT NOT NULL,
                buy_price REAL NOT NULL,
                quantity INTEGER NOT NULL,
                target_price REAL,
                stop_loss REAL,
                current_price REAL,
                highest_price REAL,
                entry_low REAL,
                tp_stage INTEGER DEFAULT 0,
                pnl REAL DEFAULT 0,
                pnl_pct REAL DEFAULT 0,
                status TEXT DEFAULT 'holding',
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (recommend_id) REFERENCES recommends(id)
            )
        """)

        # 买卖点/信号事件表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_points (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                recommend_id INTEGER,
                date TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT,
                event_type TEXT NOT NULL,
                signal_type TEXT DEFAULT '',
                price REAL DEFAULT 0,
                target_price REAL DEFAULT 0,
                stop_loss REAL DEFAULT 0,
                quantity INTEGER DEFAULT 0,
                reason TEXT DEFAULT '',
                source TEXT DEFAULT '',
                status TEXT DEFAULT '',
                metadata TEXT DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (recommend_id) REFERENCES recommends(id)
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trade_points_code ON trade_points(code)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trade_points_date ON trade_points(date)")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signal_pool (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT,
                pool_type TEXT DEFAULT '',
                signal_type TEXT DEFAULT '',
                price REAL DEFAULT 0,
                target_price REAL DEFAULT 0,
                stop_loss REAL DEFAULT 0,
                reason TEXT DEFAULT '',
                score REAL DEFAULT 0,
                source TEXT DEFAULT '',
                status TEXT DEFAULT 'active',
                metadata TEXT DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self._rebuild_signal_pool_indexes(cursor)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_pool_date ON signal_pool(date)")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dashboard_cache (
                cache_key TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_dashboard_cache_updated_at ON dashboard_cache(updated_at)")

        # 信号质量追踪表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS signal_quality (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT UNIQUE,
                signal_source TEXT,
                signal_params TEXT,
                decision_agent TEXT,
                market_regime TEXT,
                entry_date TEXT,
                entry_price REAL,
                exit_date TEXT,
                exit_price REAL,
                holding_days INTEGER,
                pnl_pct REAL,
                outcome TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_quality_source ON signal_quality(signal_source)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_quality_outcome ON signal_quality(outcome)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_quality_date ON signal_quality(entry_date)")

        # 动态参数表（自优化）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dynamic_params (
                param_key TEXT PRIMARY KEY,
                param_value REAL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                change_reason TEXT,
                source TEXT DEFAULT 'optimizer'
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_dynamic_params_updated ON dynamic_params(updated_at)")

        # 人工干预记录表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS manual_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id TEXT,
                original_action TEXT,
                override_action TEXT,
                override_reason TEXT,
                operator TEXT DEFAULT 'human',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_manual_overrides_signal ON manual_overrides(signal_id)")

        # Walk-Forward分析结果表（防过拟合）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS wfa_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                window_start TEXT,
                window_end TEXT,
                train_return REAL,
                test_return REAL,
                params_used TEXT,
                stability_score REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_wfa_results_window ON wfa_results(window_start, window_end)")

        # 每日优化结果表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_optimization (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL,
                daily_summary TEXT,
                performance TEXT,
                suggestions TEXT,
                applied_changes TEXT,
                rejected_changes TEXT,
                stability_score REAL,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_daily_optimization_date ON daily_optimization(date)")

        # 兼容旧库：增量补齐字段
        self._ensure_column(cursor, "positions", "highest_price", "highest_price REAL")
        self._ensure_column(cursor, "positions", "tp_stage", "tp_stage INTEGER DEFAULT 0")
        self._ensure_column(cursor, "positions", "entry_low", "entry_low REAL")
        
        conn.commit()
        conn.close()
        logger.info(f"数据库初始化完成: {self.db_path}")

    def _rebuild_signal_pool_indexes(self, cursor: sqlite3.Cursor) -> None:
        """重建信号池索引，按 code+date+status 合并同日记录并保留跨日历史。"""
        try:
            cursor.execute("DROP INDEX IF EXISTS idx_signal_pool_code_status")
        except Exception as e:
            logger.debug(f"删除旧信号池索引失败: {e}")
        cursor.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_pool_code_date_status "
            "ON signal_pool(code, date, status)"
        )

    def _ensure_column(self, cursor: sqlite3.Cursor, table: str, column: str, ddl: str):
        """为旧表补齐字段（幂等）"""
        try:
            cursor.execute(f"PRAGMA table_info({table})")
            rows = cursor.fetchall()
            cols = []
            for r in rows:
                try:
                    cols.append(r["name"])
                except Exception:
                    cols.append(r[1])
            if column not in cols:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")
        except Exception as e:
            logger.warning(f"补齐字段失败 {table}.{column}: {e}")
    
    def add_recommend(self, record: RecommendRecord) -> int:
        """添加荐股记录"""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO recommends (date, code, name, price, target_price, stop_loss, reason, signal_type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (record.date, record.code, record.name, record.price, 
              record.target_price, record.stop_loss, record.reason, record.signal_type))
        
        recommend_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        logger.info(f"添加荐股记录: {record.code} {record.name} @ {record.price}")
        return recommend_id
    
    def get_recommends_by_date(self, date: str) -> List[RecommendRecord]:
        """获取指定日期的荐股记录"""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM recommends WHERE date = ?
        """, (date,))
        
        records = []
        for row in cursor.fetchall():
            records.append(RecommendRecord(
                id=row["id"],
                date=row["date"],
                code=row["code"],
                name=row["name"],
                price=row["price"],
                target_price=row["target_price"],
                stop_loss=row["stop_loss"],
                reason=row["reason"],
                signal_type=row["signal_type"],
                created_at=row["created_at"]
            ))
        
        conn.close()
        return records
    
    def add_trade(self, record: TradeRecord) -> int:
        """添加交易记录"""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO trades (recommend_id, date, code, name, direction, price, quantity, amount, commission, pnl, pnl_pct, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (record.recommend_id, record.date, record.code, record.name, 
              record.direction, record.price, record.quantity, record.amount, 
              record.commission, record.pnl, record.pnl_pct, record.status))
        
        trade_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return trade_id

    def add_trade_point(self, record: TradePointRecord) -> int:
        """添加买卖点/信号事件记录"""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO trade_points (
                recommend_id, date, code, name, event_type, signal_type,
                price, target_price, stop_loss, quantity, reason, source, status, metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            record.recommend_id,
            record.date,
            record.code,
            record.name,
            record.event_type,
            record.signal_type,
            record.price,
            record.target_price,
            record.stop_loss,
            record.quantity,
            record.reason,
            record.source,
            record.status,
            record.metadata,
        ))

        trade_point_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return trade_point_id

    def upsert_signal_pool(self, record: SignalPoolRecord) -> int:
        """新增或更新信号池记录"""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id
            FROM signal_pool
            WHERE code = ? AND date = ? AND status = ?
            ORDER BY id DESC
            LIMIT 1
        """, (record.code, record.date, record.status))
        existing = cursor.fetchone()

        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if existing:
            cursor.execute("""
                UPDATE signal_pool
                SET date = ?,
                    name = ?,
                    pool_type = ?,
                    signal_type = ?,
                    price = ?,
                    target_price = ?,
                    stop_loss = ?,
                    reason = ?,
                    score = ?,
                    source = ?,
                    metadata = ?,
                    updated_at = ?
                WHERE id = ?
            """, (
                record.date,
                record.name,
                record.pool_type,
                record.signal_type,
                record.price,
                record.target_price,
                record.stop_loss,
                record.reason,
                record.score,
                record.source,
                record.metadata,
                now_text,
                existing["id"],
            ))
            signal_pool_id = existing["id"]
        else:
            cursor.execute("""
                INSERT INTO signal_pool (
                    date, code, name, pool_type, signal_type, price, target_price,
                    stop_loss, reason, score, source, status, metadata, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record.date,
                record.code,
                record.name,
                record.pool_type,
                record.signal_type,
                record.price,
                record.target_price,
                record.stop_loss,
                record.reason,
                record.score,
                record.source,
                record.status,
                record.metadata,
                now_text,
                now_text,
            ))
            signal_pool_id = cursor.lastrowid

        conn.commit()
        conn.close()
        return signal_pool_id

    def update_signal_pool_status(self, code: str, status: str) -> None:
        """更新信号池状态"""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE signal_pool
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE code = ? AND status != ?
        """, (status, code, status))
        conn.commit()
        conn.close()

    def clear_signal_pool_by_status(self, status: str = "active", next_status: str = "inactive") -> int:
        """批量清理指定状态的信号池记录"""
        conn = self._get_conn()
        try:
            cursor = conn.cursor()
            if next_status != status:
                cursor.execute(
                    """
                    DELETE FROM signal_pool
                    WHERE status = ?
                      AND code IN (
                          SELECT code
                          FROM signal_pool
                          WHERE status = ?
                      )
                    """,
                    (next_status, status),
                )
            cursor.execute("""
                UPDATE signal_pool
                SET status = ?, updated_at = CURRENT_TIMESTAMP
                WHERE status = ?
            """, (next_status, status))
            affected = int(cursor.rowcount or 0)
            conn.commit()
            return affected
        finally:
            conn.close()
    
    def add_position(self, recommend_id: int, code: str, name: str, 
                    buy_date: str, buy_price: float, quantity: int,
                    target_price: float, stop_loss: float) -> int:
        """添加持仓记录（兼容旧接口，统一走单表持仓仓储）"""
        return self.upsert_position(
            code=code,
            name=name,
            buy_price=buy_price,
            quantity=quantity,
            target_price=target_price,
            stop_loss=stop_loss,
            buy_date=buy_date,
            recommend_id=recommend_id,
            entry_low=buy_price,
        )

    def _list_positions(
        self,
        statuses: Optional[List[str]] = None,
        aggregated: bool = False,
        limit: Optional[int] = None,
    ) -> List[Dict]:
        """统一读取持仓表，支持原始记录和按 code 聚合视图。"""
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            status_list = [str(item).strip() for item in (statuses or ["holding"]) if str(item).strip()]
            placeholders = ",".join(["?"] * len(status_list))

            if aggregated:
                sql = f"""
                    SELECT
                        code,
                        MAX(name) AS name,
                        SUM(quantity) AS total_quantity,
                        SUM(quantity * buy_price) / NULLIF(SUM(quantity), 0) AS avg_buy_price,
                        MIN(buy_date) AS first_buy_date,
                        MAX(buy_date) AS last_buy_date,
                        MAX(target_price) AS target_price,
                        MIN(stop_loss) AS stop_loss,
                        SUM(quantity * current_price) / NULLIF(SUM(quantity), 0) AS avg_current_price,
                        SUM(quantity * (current_price - buy_price)) AS total_pnl,
                        SUM(quantity * (current_price - buy_price)) / NULLIF(SUM(quantity * buy_price), 0) * 100 AS total_pnl_pct,
                        MAX(updated_at) AS updated_at,
                        MAX(highest_price) AS highest_price,
                        MIN(entry_low) AS entry_low,
                        MAX(tp_stage) AS tp_stage
                    FROM positions
                    WHERE status IN ({placeholders})
                    GROUP BY code
                    ORDER BY MAX(updated_at) DESC, code ASC
                """
            else:
                sql = f"""
                    SELECT *
                    FROM positions
                    WHERE status IN ({placeholders})
                    ORDER BY updated_at DESC, id DESC
                """

            params: List[object] = list(status_list)
            if limit is not None and int(limit) > 0:
                sql += " LIMIT ?"
                params.append(int(limit))

            cursor.execute(sql, tuple(params))
            return [dict(row) for row in cursor.fetchall()]
        finally:
            conn.close()

    def list_positions(
        self,
        statuses: Optional[List[str]] = None,
        aggregated: bool = False,
        limit: Optional[int] = None,
    ) -> List[Dict]:
        """统一持仓查询入口。"""
        return self._list_positions(statuses=statuses, aggregated=aggregated, limit=limit)

    def get_position(self, code: str, aggregated: bool = True, status: str = "holding") -> Optional[Dict]:
        """按代码获取单只持仓。"""
        code_text = str(code or "").strip()
        if not code_text:
            return None
        rows = self._list_positions(statuses=[status], aggregated=aggregated, limit=None)
        for row in rows:
            if str(row.get("code", "") or "").strip() == code_text:
                return row
        return None

    def _normalize_position_rows(self, cursor: sqlite3.Cursor, code: str) -> Optional[Dict]:
        """
        归并同 code 的多条 holding 记录，确保单票只保留一条主记录。
        """
        cursor.execute("""
            SELECT *
            FROM positions
            WHERE code = ? AND status = 'holding'
            ORDER BY buy_date ASC, id ASC
        """, (code,))
        rows = [dict(row) for row in cursor.fetchall()]
        if not rows:
            return None
        if len(rows) == 1:
            return rows[0]

        primary = rows[0]
        total_quantity = sum(int(row.get("quantity") or 0) for row in rows)
        total_cost = sum(float(row.get("buy_price") or 0.0) * int(row.get("quantity") or 0) for row in rows)
        avg_buy_price = total_cost / total_quantity if total_quantity > 0 else float(primary.get("buy_price") or 0.0)
        current_price = float(primary.get("current_price") or avg_buy_price)
        highest_price = max(float(row.get("highest_price") or row.get("buy_price") or 0.0) for row in rows)
        entry_low_candidates = [float(row.get("entry_low") or 0.0) for row in rows if float(row.get("entry_low") or 0.0) > 0]
        entry_low = min(entry_low_candidates) if entry_low_candidates else avg_buy_price
        target_price = max(float(row.get("target_price") or 0.0) for row in rows)
        stop_loss_candidates = [float(row.get("stop_loss") or 0.0) for row in rows if float(row.get("stop_loss") or 0.0) > 0]
        stop_loss = min(stop_loss_candidates) if stop_loss_candidates else 0.0
        pnl = (current_price - avg_buy_price) * total_quantity
        pnl_pct = ((current_price - avg_buy_price) / avg_buy_price * 100) if avg_buy_price > 0 else 0.0
        tp_stage = max(int(row.get("tp_stage") or 0) for row in rows)
        latest_name = next((str(row.get("name", "") or "").strip() for row in reversed(rows) if str(row.get("name", "") or "").strip()), str(primary.get("name", "") or ""))

        cursor.execute("""
            UPDATE positions
            SET name = ?,
                buy_date = ?,
                buy_price = ?,
                quantity = ?,
                target_price = ?,
                stop_loss = ?,
                current_price = ?,
                highest_price = ?,
                entry_low = ?,
                tp_stage = ?,
                pnl = ?,
                pnl_pct = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (
            latest_name,
            primary.get("buy_date"),
            avg_buy_price,
            total_quantity,
            target_price,
            stop_loss,
            current_price,
            highest_price,
            entry_low,
            tp_stage,
            pnl,
            pnl_pct,
            primary["id"],
        ))

        extra_ids = [int(row["id"]) for row in rows[1:]]
        placeholders = ",".join(["?"] * len(extra_ids))
        cursor.execute(f"DELETE FROM positions WHERE id IN ({placeholders})", tuple(extra_ids))
        primary.update({
            "name": latest_name,
            "buy_price": avg_buy_price,
            "quantity": total_quantity,
            "target_price": target_price,
            "stop_loss": stop_loss,
            "current_price": current_price,
            "highest_price": highest_price,
            "entry_low": entry_low,
            "tp_stage": tp_stage,
            "pnl": pnl,
            "pnl_pct": pnl_pct,
        })
        logger.info(f"持仓归并完成: {code} 合并 {len(rows)} 条为 1 条")
        return primary

    def _normalize_all_holding_positions(self) -> None:
        """全量归并 holding 持仓，避免旧数据导致多口径不一致。"""
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT code
                FROM positions
                WHERE status = 'holding'
                GROUP BY code
                HAVING COUNT(*) > 1
            """)
            duplicated_codes = [str(row["code"] or "").strip() for row in cursor.fetchall()]
            if not duplicated_codes:
                return
            for code in duplicated_codes:
                self._normalize_position_rows(cursor, code)
            conn.commit()
        finally:
            conn.close()

    def upsert_position(
        self,
        code: str,
        name: str,
        buy_price: float,
        quantity: int,
        target_price: float,
        stop_loss: float,
        buy_date: str,
        recommend_id: int = 0,
        entry_low: Optional[float] = None,
    ) -> int:
        """统一开仓/加仓入口，保证同 code 在 positions 中仅保留一条 holding 记录。"""
        code_text = str(code or "").strip()
        if not code_text or int(quantity or 0) <= 0:
            raise ValueError("无效的持仓参数")

        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            existing = self._normalize_position_rows(cursor, code_text)
            entry_low_val = float(entry_low) if entry_low is not None else float(buy_price)
            if existing:
                current_qty = int(existing.get("quantity") or 0)
                total_qty = current_qty + int(quantity)
                existing_buy_price = float(existing.get("buy_price") or 0.0)
                avg_buy_price = (
                    (existing_buy_price * current_qty + float(buy_price) * int(quantity)) / total_qty
                    if total_qty > 0 else float(buy_price)
                )
                current_price = float(existing.get("current_price") or buy_price or avg_buy_price)
                highest_price = max(float(existing.get("highest_price") or current_price or avg_buy_price), float(buy_price))
                merged_target = max(float(existing.get("target_price") or 0.0), float(target_price or 0.0))
                stop_candidates = [float(val) for val in [existing.get("stop_loss"), stop_loss] if float(val or 0.0) > 0]
                merged_stop = min(stop_candidates) if stop_candidates else 0.0
                merged_entry_low = min(
                    [float(val) for val in [existing.get("entry_low"), entry_low_val] if float(val or 0.0) > 0]
                )
                pnl = (current_price - avg_buy_price) * total_qty
                pnl_pct = ((current_price - avg_buy_price) / avg_buy_price * 100) if avg_buy_price > 0 else 0.0
                cursor.execute("""
                    UPDATE positions
                    SET recommend_id = CASE WHEN recommend_id IS NULL OR recommend_id = 0 THEN ? ELSE recommend_id END,
                        name = ?,
                        buy_date = CASE WHEN buy_date <= ? THEN buy_date ELSE ? END,
                        buy_price = ?,
                        quantity = ?,
                        target_price = ?,
                        stop_loss = ?,
                        current_price = ?,
                        highest_price = ?,
                        entry_low = ?,
                        pnl = ?,
                        pnl_pct = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (
                    recommend_id,
                    name,
                    buy_date,
                    buy_date,
                    avg_buy_price,
                    total_qty,
                    merged_target,
                    merged_stop,
                    current_price,
                    highest_price,
                    merged_entry_low,
                    pnl,
                    pnl_pct,
                    existing["id"],
                ))
                position_id = int(existing["id"])
            else:
                cursor.execute("""
                    INSERT INTO positions (
                        recommend_id, code, name, buy_date, buy_price, quantity,
                        target_price, stop_loss, current_price, highest_price,
                        entry_low, tp_stage, pnl, pnl_pct, status, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'holding', CURRENT_TIMESTAMP)
                """, (
                    recommend_id,
                    code_text,
                    name,
                    buy_date,
                    buy_price,
                    quantity,
                    target_price,
                    stop_loss,
                    buy_price,
                    buy_price,
                    entry_low_val,
                    0,
                    0.0,
                    0.0,
                ))
                position_id = int(cursor.lastrowid)
            conn.commit()
            return position_id
        finally:
            conn.close()
    
    def update_position_price(self, code: str, current_price: float):
        """更新持仓现价和盈亏（兼容旧接口）"""
        conn = self._get_conn()
        cursor = conn.cursor()
        self._normalize_position_rows(cursor, str(code or "").strip())
        cursor.execute("""
            UPDATE positions 
            SET current_price = ?, 
                highest_price = CASE
                    WHEN highest_price IS NULL THEN ?
                    WHEN ? > highest_price THEN ?
                    ELSE highest_price
                END,
                pnl = (?-buy_price)*quantity,
                pnl_pct = ((?-buy_price)/buy_price)*100,
                updated_at = CURRENT_TIMESTAMP
            WHERE code = ? AND status = 'holding'
        """, (current_price, current_price, current_price, current_price, current_price, current_price, code))
        
        conn.commit()
        conn.close()

    def update_position(self, code: str, **fields) -> bool:
        """统一修改持仓字段。"""
        allowed = {
            "name", "buy_date", "buy_price", "quantity", "target_price", "stop_loss",
            "current_price", "highest_price", "entry_low", "tp_stage", "status",
        }
        updates = []
        params: List[object] = []
        for key, value in fields.items():
            if key not in allowed:
                continue
            updates.append(f"{key} = ?")
            params.append(value)
        if not updates:
            return False

        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            self._normalize_position_rows(cursor, str(code or "").strip())
            sql = f"UPDATE positions SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP WHERE code = ? AND status = 'holding'"
            params.append(str(code or "").strip())
            cursor.execute(sql, tuple(params))
            conn.commit()
            return int(cursor.rowcount or 0) > 0
        finally:
            conn.close()

    def delete_position(self, code: str, status: Optional[str] = None) -> int:
        """删除持仓记录，供人工修正或回滚使用。"""
        conn = self._get_conn()
        cursor = conn.cursor()
        try:
            if status:
                cursor.execute("DELETE FROM positions WHERE code = ? AND status = ?", (code, status))
            else:
                cursor.execute("DELETE FROM positions WHERE code = ?", (code,))
            affected = int(cursor.rowcount or 0)
            conn.commit()
            return affected
        finally:
            conn.close()
    
    def close_position(self, code: str, sell_price: float, sell_date: str, reason: str = ""):
        """平仓"""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        # 获取持仓信息
        cursor.execute("""
            SELECT * FROM positions WHERE code = ? AND status = 'holding'
        """, (code,))
        
        row = cursor.fetchone()
        if not row:
            conn.close()
            return None
        
        pnl = (sell_price - row["buy_price"]) * row["quantity"]
        pnl_pct = (sell_price - row["buy_price"]) / row["buy_price"] * 100
        
        # 更新持仓状态
        cursor.execute("""
            UPDATE positions SET status = 'sold', current_price = ?, pnl = ?, pnl_pct = ?, updated_at = CURRENT_TIMESTAMP
            WHERE code = ? AND status = 'holding'
        """, (sell_price, pnl, pnl_pct, code))
        
        # 添加交易记录
        cursor.execute("""
            INSERT INTO trades (recommend_id, date, code, name, direction, price, quantity, amount, pnl, pnl_pct, status)
            VALUES (?, ?, ?, ?, 'sell', ?, ?, ?, ?, ?, 'sold')
        """, (row["recommend_id"], sell_date, code, row["name"], sell_price, 
              row["quantity"], row["quantity"]*sell_price, pnl, pnl_pct))

        cursor.execute("""
            INSERT INTO trade_points (
                recommend_id, date, code, name, event_type, signal_type, price,
                target_price, stop_loss, quantity, reason, source, status, metadata
            )
            VALUES (?, ?, ?, ?, 'sell', '卖出', ?, ?, ?, ?, ?, 'simulate_trading', 'sold', ?)
        """, (
            row["recommend_id"],
            sell_date,
            code,
            row["name"],
            sell_price,
            row["target_price"] or 0,
            row["stop_loss"] or 0,
            row["quantity"],
            reason,
            json.dumps({"pnl": pnl, "pnl_pct": pnl_pct}, ensure_ascii=False),
        ))

        cursor.execute("""
            UPDATE signal_pool
            SET status = 'sold', updated_at = CURRENT_TIMESTAMP
            WHERE code = ?
        """, (code,))
        
        conn.commit()
        conn.close()
        
        return pnl

    def sell_partial(
        self,
        code: str,
        sell_price: float,
        sell_date: str,
        sell_quantity: int,
        tp_stage: Optional[int] = None,
        reason: str = "",
    ):
        """部分平仓（用于分批止盈）"""
        if sell_quantity <= 0:
            return None

        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM positions WHERE code = ? AND status = 'holding'
        """, (code,))

        row = cursor.fetchone()
        if not row:
            conn.close()
            return None

        current_qty = int(row["quantity"] or 0)
        if current_qty <= 0:
            conn.close()
            return None

        sell_qty = int(sell_quantity)
        if sell_qty >= current_qty:
            conn.close()
            return self.close_position(code, sell_price, sell_date)

        pnl = (sell_price - row["buy_price"]) * sell_qty
        pnl_pct = (sell_price - row["buy_price"]) / row["buy_price"] * 100 if row["buy_price"] else 0.0

        new_qty = current_qty - sell_qty
        new_status = "holding" if new_qty > 0 else "sold"

        if tp_stage is None:
            tp_stage = int(row["tp_stage"] or 0)

        cursor.execute("""
            UPDATE positions
            SET quantity = ?,
                current_price = ?,
                highest_price = CASE
                    WHEN highest_price IS NULL THEN ?
                    WHEN ? > highest_price THEN ?
                    ELSE highest_price
                END,
                tp_stage = ?,
                pnl = (? - buy_price) * ?,
                pnl_pct = ((? - buy_price) / buy_price) * 100,
                status = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (
            new_qty,
            sell_price,
            sell_price,
            sell_price,
            sell_price,
            tp_stage,
            sell_price,
            new_qty,
            sell_price,
            new_status,
            row["id"],
        ))

        cursor.execute("""
            INSERT INTO trades (recommend_id, date, code, name, direction, price, quantity, amount, pnl, pnl_pct, status)
            VALUES (?, ?, ?, ?, 'sell', ?, ?, ?, ?, ?, ?)
        """, (
            row["recommend_id"],
            sell_date,
            code,
            row["name"],
            sell_price,
            sell_qty,
            sell_qty * sell_price,
            pnl,
            pnl_pct,
            new_status,
        ))

        cursor.execute("""
            INSERT INTO trade_points (
                recommend_id, date, code, name, event_type, signal_type, price,
                target_price, stop_loss, quantity, reason, source, status, metadata
            )
            VALUES (?, ?, ?, ?, 'scale_out', '卖出', ?, ?, ?, ?, ?, 'simulate_trading', ?, ?)
        """, (
            row["recommend_id"],
            sell_date,
            code,
            row["name"],
            sell_price,
            row["target_price"] or 0,
            row["stop_loss"] or 0,
            sell_qty,
            reason,
            new_status,
            json.dumps({"pnl": pnl, "pnl_pct": pnl_pct, "tp_stage": tp_stage}, ensure_ascii=False),
        ))

        conn.commit()
        conn.close()
        return pnl
    
    def get_holdings(self) -> List[Dict]:
        """获取当前持仓原始记录（统一持仓接口）"""
        self._normalize_all_holding_positions()
        return self.list_positions(statuses=["holding"], aggregated=False)

    def get_signal_pool(self, status: str = "active", limit: int = 50) -> List[Dict]:
        """获取当前信号池"""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT *
            FROM signal_pool
            WHERE status = ?
            ORDER BY score DESC, updated_at DESC, id DESC
            LIMIT ?
        """, (status, limit))

        records = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return records

    def get_signal_pool_multi_status(self, statuses: List[str], limit: int = 100) -> List[Dict]:
        """按多个状态获取信号池"""
        status_list = [str(status).strip() for status in statuses if str(status).strip()]
        if not status_list:
            return []

        conn = self._get_conn()
        cursor = conn.cursor()
        placeholders = ",".join(["?"] * len(status_list))
        cursor.execute(
            f"""
            SELECT *
            FROM signal_pool
            WHERE status IN ({placeholders})
            ORDER BY
                CASE status
                    WHEN 'active' THEN 1
                    WHEN 'holding' THEN 2
                    WHEN 'inactive' THEN 3
                    ELSE 9
                END,
                score DESC,
                updated_at DESC,
                id DESC
            LIMIT ?
            """,
            (*status_list, limit),
        )
        records = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return records

    def get_signal_pool_inactive_recent(self, limit: int = 30, days: int = 3) -> List[Dict]:
        """获取最近失效的信号池记录。"""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM signal_pool
            WHERE status = 'inactive'
              AND date >= date('now', ?)
            ORDER BY updated_at DESC, id DESC
            LIMIT ?
            """,
            (f"-{max(int(days), 0)} day", limit),
        )
        records = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return records

    def get_signal_pool_status_counts(self) -> Dict[str, int]:
        """获取信号池各状态数量"""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT status, COUNT(*) AS cnt
            FROM signal_pool
            GROUP BY status
            """
        )
        counts = {"active": 0, "holding": 0, "inactive": 0}
        for row in cursor.fetchall():
            counts[str(row["status"] or "").strip() or "unknown"] = int(row["cnt"] or 0)
        conn.close()
        return counts

    def get_holdings_aggregated(self) -> List[Dict]:
        """获取聚合后的持仓（按 code 合并，统一持仓接口）"""
        self._normalize_all_holding_positions()
        return self.list_positions(statuses=["holding"], aggregated=True)

    def add_position_merged(
        self,
        code: str,
        name: str,
        buy_price: float,
        quantity: int,
        target_price: float,
        stop_loss: float,
        buy_date: str,
        entry_low: Optional[float] = None,
    ) -> int:
        """追加持仓（兼容旧接口，统一走 upsert_position）"""
        return self.upsert_position(
            code=code,
            name=name,
            buy_price=buy_price,
            quantity=quantity,
            target_price=target_price,
            stop_loss=stop_loss,
            buy_date=buy_date,
            entry_low=entry_low,
        )
    
    def get_trade_history(self, days: int = 30) -> List[Dict]:
        """获取交易历史"""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM trades ORDER BY date DESC LIMIT ?
        """, (days,))
        
        trades = []
        for row in cursor.fetchall():
            trades.append(dict(row))
        
        conn.close()
        return trades

    def get_trade_points(self, limit: int = 50) -> List[Dict]:
        """获取最近买卖点/信号事件"""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM trade_points ORDER BY id DESC LIMIT ?
        """, (limit,))

        records = []
        for row in cursor.fetchall():
            records.append(dict(row))

        conn.close()
        return records

    def get_trade_timeline(self, limit: int = 100) -> List[Dict]:
        """获取按标的和时间排序的交易时间线"""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT *
            FROM trade_points
            ORDER BY date DESC, id DESC
            LIMIT ?
        """, (limit,))

        records = []
        for row in cursor.fetchall():
            records.append(dict(row))

        conn.close()
        return records

    def set_dashboard_cache(self, cache_key: str, payload: Dict) -> None:
        """写入看板缓存数据"""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dashboard_cache (cache_key, payload, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(cache_key) DO UPDATE SET
                payload = excluded.payload,
                updated_at = CURRENT_TIMESTAMP
        """, (
            cache_key,
            json.dumps(payload, ensure_ascii=False),
        ))
        conn.commit()
        conn.close()

    def get_dashboard_cache(self, cache_key: str) -> Optional[Dict]:
        """读取看板缓存数据"""
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT payload, updated_at
            FROM dashboard_cache
            WHERE cache_key = ?
            LIMIT 1
        """, (cache_key,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None

        try:
            payload = json.loads(row["payload"] or "{}")
        except Exception:
            payload = {}

        if isinstance(payload, dict):
            payload.setdefault("generated_at", row["updated_at"])
        return payload
    
    def get_statistics(self, days: int = 30) -> Dict:
        """获取统计数据"""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        # 总交易次数
        cursor.execute("SELECT COUNT(*) as total FROM trades WHERE direction = 'sell'")
        total_trades = cursor.fetchone()["total"]
        
        # 盈利次数
        cursor.execute("SELECT COUNT(*) as wins FROM trades WHERE direction = 'sell' AND pnl > 0")
        win_trades = cursor.fetchone()["wins"]
        
        # 总收益
        cursor.execute("SELECT SUM(pnl) as total_pnl FROM trades WHERE direction = 'sell'")
        total_pnl = cursor.fetchone()["total_pnl"] or 0
        
        # 平均收益
        avg_pnl = total_pnl / total_trades if total_trades > 0 else 0
        
        # 胜率
        win_rate = win_trades / total_trades * 100 if total_trades > 0 else 0
        
        conn.close()
        
        return {
            "total_trades": total_trades,
            "win_trades": win_trades,
            "loss_trades": total_trades - win_trades,
            "win_rate": win_rate,
            "total_pnl": total_pnl,
            "avg_pnl": avg_pnl,
        }


# 全局数据库实例
_db_instances: Dict[str, RecommendDB] = {}


def get_db(db_path: Optional[str] = None) -> RecommendDB:
    """获取数据库实例"""
    global _db_instances
    normalized_path = resolve_db_path(db_path)
    if normalized_path not in _db_instances:
        _db_instances[normalized_path] = RecommendDB(normalized_path)
    return _db_instances[normalized_path]


class SignalQualityDB:
    """信号质量追踪数据库（继承RecommendDB功能）"""

    def __init__(self, db_path: Optional[str] = None):
        self.db = get_db(db_path)

    def add_signal_quality(
        self,
        signal_id: str,
        signal_source: str,
        signal_params: Dict,
        decision_agent: str,
        market_regime: str,
        entry_date: str,
        entry_price: float,
        exit_date: Optional[str] = None,
        exit_price: Optional[float] = None,
        holding_days: Optional[int] = None,
        pnl_pct: Optional[float] = None,
        outcome: Optional[str] = None,
    ) -> int:
        """添加信号质量记录"""
        conn = self.db._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO signal_quality (
                signal_id, signal_source, signal_params, decision_agent, market_regime,
                entry_date, entry_price, exit_date, exit_price, holding_days, pnl_pct, outcome
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            signal_id, signal_source, json.dumps(signal_params, ensure_ascii=False),
            decision_agent, market_regime, entry_date, entry_price,
            exit_date, exit_price, holding_days, pnl_pct, outcome
        ))
        conn.commit()
        conn.close()
        return cursor.lastrowid

    def update_signal_outcome(
        self,
        signal_id: str,
        exit_date: str,
        exit_price: float,
        holding_days: int,
        pnl_pct: float,
        outcome: str,
    ) -> bool:
        """更新信号结果（出场时调用）"""
        conn = self.db._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE signal_quality SET
                exit_date = ?, exit_price = ?, holding_days = ?, pnl_pct = ?, outcome = ?
            WHERE signal_id = ?
        """, (exit_date, exit_price, holding_days, pnl_pct, outcome, signal_id))
        conn.commit()
        conn.close()
        return cursor.rowcount > 0

    def get_performance_by_source(self, lookback_days: int = 30) -> Dict:
        """按信号来源查询胜率"""
        conn = self.db._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT signal_source, COUNT(*) as total,
                   SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) as wins,
                   AVG(pnl_pct) as avg_pnl
            FROM signal_quality
            WHERE outcome IS NOT NULL
              AND entry_date >= date('now', '-' || ? || ' days')
            GROUP BY signal_source
        """, (lookback_days,))
        results = {}
        for row in cursor.fetchall():
            source = row["signal_source"]
            total = row["total"]
            wins = row["wins"] or 0
            results[source] = {
                "total": total,
                "wins": wins,
                "losses": total - wins,
                "win_rate": wins / total * 100 if total > 0 else 0,
                "avg_pnl": row["avg_pnl"] or 0,
            }
        conn.close()
        return results

    def get_performance_by_agent(self, lookback_days: int = 30) -> Dict:
        """按决策Agent查询胜率"""
        conn = self.db._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT decision_agent, COUNT(*) as total,
                   SUM(CASE WHEN outcome = 'win' THEN 1 ELSE 0 END) as wins,
                   AVG(pnl_pct) as avg_pnl
            FROM signal_quality
            WHERE outcome IS NOT NULL
              AND entry_date >= date('now', '-' || ? || ' days')
            GROUP BY decision_agent
        """, (lookback_days,))
        results = {}
        for row in cursor.fetchall():
            agent = row["decision_agent"]
            total = row["total"]
            wins = row["wins"] or 0
            results[agent] = {
                "total": total,
                "wins": wins,
                "losses": total - wins,
                "win_rate": wins / total * 100 if total > 0 else 0,
                "avg_pnl": row["avg_pnl"] or 0,
            }
        conn.close()
        return results


class DynamicParamsDB:
    """动态参数数据库"""

    DEFAULT_PARAMS = {
        "gate_threshold": 0.58,
        "ws_shrink_ratio": 0.65,
        "ws_volume_multiple": 1.5,
        "taco_event_threshold": 0.24,
        "stop_loss": -0.05,
        "trailing_stop": 0.05,
        "max_position": 0.18,
        "max_hold_days": 2,
        "optimist_weight": 0.30,
        "pessimist_weight": 0.25,
    }

    def __init__(self, db_path: Optional[str] = None):
        self.db = get_db(db_path)
        self._ensure_defaults()

    def _ensure_defaults(self):
        """确保默认参数存在"""
        conn = self.db._get_conn()
        cursor = conn.cursor()
        for key, value in self.DEFAULT_PARAMS.items():
            cursor.execute("""
                INSERT OR IGNORE INTO dynamic_params (param_key, param_value, source)
                VALUES (?, ?, 'system')
            """, (key, value))
        conn.commit()
        conn.close()

    def get_param(self, key: str, default: Optional[float] = None) -> Optional[float]:
        """读取动态参数"""
        conn = self.db._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT param_value FROM dynamic_params WHERE param_key = ?", (key,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return row["param_value"]
        return default

    def set_param(self, key: str, value: float, reason: str = "", source: str = "optimizer") -> bool:
        """设置动态参数"""
        conn = self.db._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dynamic_params (param_key, param_value, change_reason, source, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(param_key) DO UPDATE SET
                param_value = excluded.param_value,
                change_reason = excluded.change_reason,
                source = excluded.source,
                updated_at = CURRENT_TIMESTAMP
        """, (key, value, reason, source))
        conn.commit()
        conn.close()
        return cursor.rowcount > 0

    def get_all_params(self) -> Dict:
        """读取所有动态参数"""
        conn = self.db._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT param_key, param_value, updated_at FROM dynamic_params")
        results = {}
        for row in cursor.fetchall():
            results[row["param_key"]] = {
                "value": row["param_value"],
                "updated_at": row["updated_at"],
            }
        conn.close()
        return results


class ManualOverrideDB:
    """人工干预数据库"""

    def __init__(self, db_path: Optional[str] = None):
        self.db = get_db(db_path)

    def add_override(
        self,
        signal_id: str,
        original_action: str,
        override_action: str,
        override_reason: str,
        operator: str = "human",
    ) -> int:
        """添加人工干预记录"""
        conn = self.db._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO manual_overrides (
                signal_id, original_action, override_action, override_reason, operator
            ) VALUES (?, ?, ?, ?, ?)
        """, (signal_id, original_action, override_action, override_reason, operator))
        conn.commit()
        conn.close()
        return cursor.lastrowid

    def get_overrides(self, limit: int = 50) -> List[Dict]:
        """获取人工干预记录"""
        conn = self.db._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM manual_overrides ORDER BY created_at DESC LIMIT ?
        """, (limit,))
        results = [dict(row) for row in cursor.fetchall()]
        conn.close()
        return results


class WFADB:
    """Walk-Forward分析数据库"""

    def __init__(self, db_path: Optional[str] = None):
        self.db = get_db(db_path)

    def add_result(
        self,
        window_start: str,
        window_end: str,
        train_return: float,
        test_return: float,
        params_used: Dict,
        stability_score: float,
    ) -> int:
        """添加WFA结果"""
        conn = self.db._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO wfa_results (
                window_start, window_end, train_return, test_return, params_used, stability_score
            ) VALUES (?, ?, ?, ?, ?, ?)
        """, (
            window_start, window_end, train_return, test_return,
            json.dumps(params_used, ensure_ascii=False), stability_score
        ))
        conn.commit()
        conn.close()
        return cursor.lastrowid

    def get_results(self, limit: int = 20) -> List[Dict]:
        """获取WFA结果"""
        conn = self.db._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM wfa_results ORDER BY window_end DESC LIMIT ?
        """, (limit,))
        results = []
        for row in cursor.fetchall():
            r = dict(row)
            if r.get("params_used"):
                try:
                    r["params_used"] = json.loads(r["params_used"])
                except Exception:
                    pass
            results.append(r)
        conn.close()
        return results

    def get_latest_stability_score(self) -> Optional[float]:
        """获取最新的稳定性分数"""
        conn = self.db._get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT stability_score FROM wfa_results ORDER BY window_end DESC LIMIT 1")
        row = cursor.fetchone()
        conn.close()
        return row["stability_score"] if row else None


class DailyOptimizationDB:
    """每日优化结果数据库"""

    def __init__(self, db_path: Optional[str] = None):
        self.db = get_db(db_path)

    def add_optimization(
        self,
        date: str,
        daily_summary: Dict,
        performance: Dict,
        suggestions: List[Dict],
        applied_changes: List[Dict],
        rejected_changes: List[Dict],
        stability_score: Optional[float],
    ) -> int:
        """添加每日优化结果"""
        conn = self.db._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO daily_optimization (
                date, daily_summary, performance, suggestions, 
                applied_changes, rejected_changes, stability_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            date,
            json.dumps(daily_summary, ensure_ascii=False),
            json.dumps(performance, ensure_ascii=False),
            json.dumps(suggestions, ensure_ascii=False),
            json.dumps(applied_changes, ensure_ascii=False),
            json.dumps(rejected_changes, ensure_ascii=False),
            stability_score,
        ))
        conn.commit()
        conn.close()
        return cursor.lastrowid

    def get_latest_optimization(self) -> Optional[Dict]:
        """获取最新一次优化结果"""
        conn = self.db._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM daily_optimization ORDER BY created_at DESC LIMIT 1
        """)
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return None
        
        result = dict(row)
        for key in ["daily_summary", "performance", "suggestions", "applied_changes", "rejected_changes"]:
            if result.get(key):
                try:
                    result[key] = json.loads(result[key])
                except Exception:
                    pass
        return result

    def get_optimization_history(self, limit: int = 10) -> List[Dict]:
        """获取优化历史"""
        conn = self.db._get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT date, stability_score, created_at,
                   applied_changes, rejected_changes
            FROM daily_optimization 
            ORDER BY created_at DESC 
            LIMIT ?
        """, (limit,))
        
        results = []
        for row in cursor.fetchall():
            r = dict(row)
            try:
                r["applied_changes"] = json.loads(r.get("applied_changes", "[]") or "[]")
                r["rejected_changes"] = json.loads(r.get("rejected_changes", "[]") or "[]")
            except Exception:
                pass
            results.append(r)
        
        conn.close()
        return results
