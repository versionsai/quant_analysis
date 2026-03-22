# -*- coding: utf-8 -*-
"""
荐股数据库模块
使用SQLite存储荐股记录和模拟交易数据
SQLite是Python内置的，无需单独安装
"""
import sqlite3
import os
import json
from datetime import datetime
from typing import List, Optional, Dict
from dataclasses import dataclass
from utils.logger import get_logger

logger = get_logger(__name__)


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
    
    def __init__(self, db_path: str = "./data/recommend.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else "./data", exist_ok=True)
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
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_signal_pool_code_status ON signal_pool(code, status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_signal_pool_date ON signal_pool(date)")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dashboard_cache (
                cache_key TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_dashboard_cache_updated_at ON dashboard_cache(updated_at)")

        # 兼容旧库：增量补齐字段
        self._ensure_column(cursor, "positions", "highest_price", "highest_price REAL")
        self._ensure_column(cursor, "positions", "tp_stage", "tp_stage INTEGER DEFAULT 0")
        self._ensure_column(cursor, "positions", "entry_low", "entry_low REAL")
        
        conn.commit()
        conn.close()
        logger.info(f"数据库初始化完成: {self.db_path}")

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
            WHERE code = ? AND status = ?
            ORDER BY id DESC
            LIMIT 1
        """, (record.code, record.status))
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
        """添加持仓记录"""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO positions (recommend_id, code, name, buy_date, buy_price, quantity, target_price, stop_loss, current_price, highest_price, entry_low, tp_stage)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (recommend_id, code, name, buy_date, buy_price, quantity, target_price, stop_loss, buy_price, buy_price, buy_price, 0))
        
        position_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return position_id
    
    def update_position_price(self, code: str, current_price: float):
        """更新持仓现价和盈亏"""
        conn = self._get_conn()
        cursor = conn.cursor()
        
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
        """获取当前持仓"""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM positions WHERE status = 'holding'
        """)
        
        holdings = []
        for row in cursor.fetchall():
            holdings.append(dict(row))
        
        conn.close()
        return holdings

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
        """获取聚合后的持仓（按code合并）"""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT
                code,
                name,
                SUM(quantity) as total_quantity,
                SUM(quantity * buy_price) / SUM(quantity) as avg_buy_price,
                MIN(buy_date) as first_buy_date,
                MAX(buy_date) as last_buy_date,
                MAX(target_price) as target_price,
                MIN(stop_loss) as stop_loss,
                SUM(quantity * current_price) / SUM(quantity) as avg_current_price,
                SUM(quantity * (current_price - buy_price)) as total_pnl,
                SUM(quantity * (current_price - buy_price)) / SUM(quantity * buy_price) * 100 as total_pnl_pct
            FROM positions
            WHERE status = 'holding'
            GROUP BY code
        """)
        
        holdings = []
        for row in cursor.fetchall():
            holdings.append(dict(row))
        
        conn.close()
        return holdings

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
        """追加持仓（同code累加数量）"""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id FROM positions WHERE code = ? AND status = 'holding'
        """, (code,))
        existing = cursor.fetchone()
        
        if existing:
            cursor.execute("""
                UPDATE positions
                SET quantity = quantity + ?,
                    buy_price = (quantity * buy_price + ? * ?) / (quantity + ?),
                    updated_at = ?
                WHERE id = ?
            """, (quantity, quantity, buy_price, quantity, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), existing["id"]))
            pos_id = existing["id"]
        else:
            entry_low_val = float(entry_low) if entry_low is not None else float(buy_price)
            cursor.execute("""
                INSERT INTO positions (code, name, buy_date, buy_price, quantity, target_price, stop_loss, current_price, highest_price, entry_low, tp_stage, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                code,
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
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            ))
            pos_id = cursor.lastrowid
        
        conn.commit()
        conn.close()
        return pos_id
    
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
            ORDER BY code ASC, date ASC, id ASC
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


def get_db(db_path: str = "./data/recommend.db") -> RecommendDB:
    """获取数据库实例"""
    global _db_instances
    normalized_path = str(db_path or "./data/recommend.db")
    if normalized_path not in _db_instances:
        _db_instances[normalized_path] = RecommendDB(normalized_path)
    return _db_instances[normalized_path]
