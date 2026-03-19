# -*- coding: utf-8 -*-
"""
荐股数据库模块
使用SQLite存储荐股记录和模拟交易数据
SQLite是Python内置的，无需单独安装
"""
import sqlite3
import os
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
                pnl REAL DEFAULT 0,
                pnl_pct REAL DEFAULT 0,
                status TEXT DEFAULT 'holding',
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (recommend_id) REFERENCES recommends(id)
            )
        """)
        
        conn.commit()
        conn.close()
        logger.info(f"数据库初始化完成: {self.db_path}")
    
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
    
    def add_position(self, recommend_id: int, code: str, name: str, 
                    buy_date: str, buy_price: float, quantity: int,
                    target_price: float, stop_loss: float) -> int:
        """添加持仓记录"""
        conn = self._get_conn()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO positions (recommend_id, code, name, buy_date, buy_price, quantity, target_price, stop_loss, current_price)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (recommend_id, code, name, buy_date, buy_price, quantity, target_price, stop_loss, buy_price))
        
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
                pnl = (?-buy_price)*quantity,
                pnl_pct = ((?-buy_price)/buy_price)*100,
                updated_at = CURRENT_TIMESTAMP
            WHERE code = ? AND status = 'holding'
        """, (current_price, current_price, current_price, code))
        
        conn.commit()
        conn.close()
    
    def close_position(self, code: str, sell_price: float, sell_date: str):
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

    def add_position_merged(self, code: str, name: str, buy_price: float, quantity: int,
                           target_price: float, stop_loss: float, buy_date: str) -> int:
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
            cursor.execute("""
                INSERT INTO positions (code, name, buy_date, buy_price, quantity, target_price, stop_loss, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (code, name, buy_date, buy_price, quantity, target_price, stop_loss, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
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
_db_instance: Optional[RecommendDB] = None


def get_db(db_path: str = "./data/recommend.db") -> RecommendDB:
    """获取数据库实例"""
    global _db_instance
    if _db_instance is None:
        _db_instance = RecommendDB(db_path)
    return _db_instance
