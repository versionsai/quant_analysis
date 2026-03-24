# -*- coding: utf-8 -*-
"""
A股数据源 - baostock历史K线 + futu实时行情 + akshare辅助数据
- 历史K线: baostock (支持股票+ETF)
- 实时行情: futu-api (Futu OpenD)
- ETF/LOF列表: futu-api
"""
import os
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import akshare as ak
import baostock as bs
import pandas as pd

from .futu_limit_pool import build_limit_pool, build_limit_status, get_limit_pct, get_recent_limit_streak
from utils.logger import get_logger

logger = get_logger(__name__)

_bs_logged_in = False
_a_share_codes_cache: List[str] = []
_a_share_codes_cache_time: Optional[datetime] = None


def _ensure_bs_login():
    """确保baostock已登录"""
    global _bs_logged_in
    if not _bs_logged_in:
        lg = bs.login()
        if lg.error_code == '0':
            _bs_logged_in = True
            logger.info("Baostock登录成功")
        else:
            logger.warning(f"Baostock登录失败: {lg.error_msg}")


DEFAULT_ETF_LIST = [
    {"code": "515220", "name": "煤炭ETF", "type": "ETF"},
    {"code": "159985", "name": "豆粕ETF", "type": "ETF"},
    {"code": "512070", "name": "证券保险ETF易方达", "type": "ETF"},
    {"code": "513310", "name": "中韩半导体华泰柏瑞", "type": "ETF"},
    {"code": "562590", "name": "半导体设备ETF华夏", "type": "ETF"},
    {"code": "159667", "name": "工业母机ETF国泰", "type": "ETF"},
    {"code": "512660", "name": "军工ETF国泰", "type": "ETF"},
    {"code": "159326", "name": "电网设备ETF", "type": "ETF"},
    {"code": "512400", "name": "有色金属ETF", "type": "ETF"},
    {"code": "501018", "name": "南方原油LOF", "type": "LOF"},
]

INDEX_FUTU_MAP = {
    "000001": "SH.000001",  # 上证指数
    "000905": "SH.000905",  # 中证500
    "000852": "SH.000852",  # 中证1000
    "000300": "SH.000300",  # 沪深300
    "399001": "SZ.399001",  # 深证成指
    "399006": "SZ.399006",  # 创业板指
    "000688": "SH.000688",  # 科创50
}


def _symbol_to_baostock(symbol: str) -> str:
    """将6位代码转换为baostock格式 (sh.600000 / sz.000001)"""
    symbol = str(symbol).zfill(6)
    if symbol.startswith(("6", "5", "9")):
        return f"sh.{symbol}"
    return f"sz.{symbol}"


def _adjust_flag(adjust: str) -> str:
    """将adjust参数映射到baostock adjustflag"""
    if adjust == "qfq":
        return "2"
    if adjust == "hfq":
        return "1"
    return "3"


def _normalize_plain_code(code: str) -> str:
    """将 Futu 代码标准化为 6 位纯代码"""
    return str(code or "").strip().upper().replace("SH.", "").replace("SZ.", "").zfill(6)


class DataSource:
    _shared_futu_ctx = None
    _shared_futu_connected = False
    _shared_futu_signature = ""
    _shared_subscribed: Set[str] = set()
    _shared_lock = threading.Lock()
    
    def __init__(self, cache_dir: str = None):
        if cache_dir is None:
            cache_dir = os.environ.get("QUANT_CACHE_DIR", "./runtime/data/cache")
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        
        self._futu_ctx = None
        self._futu_host = os.environ.get("FUTU_HOST", "127.0.0.1")
        self._futu_port = int(os.environ.get("FUTU_PORT", 11111))
        self._futu_connected = False
    
    def _init_futu(self) -> bool:
        """初始化Futu连接 (用于实时行情)"""
        signature = f"{self._futu_host}:{self._futu_port}"
        with self.__class__._shared_lock:
            if (
                self.__class__._shared_futu_connected
                and self.__class__._shared_futu_ctx is not None
                and self.__class__._shared_futu_signature == signature
            ):
                self._futu_ctx = self.__class__._shared_futu_ctx
                self._futu_connected = True
                return True

            futu_import_error = None
            try:
                from futu import OpenQuoteContext
                self._futu_ctx = OpenQuoteContext(host=self._futu_host, port=self._futu_port)
                self._futu_connected = True
                self.__class__._shared_futu_ctx = self._futu_ctx
                self.__class__._shared_futu_connected = True
                self.__class__._shared_futu_signature = signature
                logger.info(f"Futu连接成功: {self._futu_host}:{self._futu_port} (futu-api, shared)")
                return True
            except ImportError as e:
                futu_import_error = e
                try:
                    from futuquant import OpenQuoteContext
                    self._futu_ctx = OpenQuoteContext(host=self._futu_host, port=self._futu_port)
                    self._futu_connected = True
                    self.__class__._shared_futu_ctx = self._futu_ctx
                    self.__class__._shared_futu_connected = True
                    self.__class__._shared_futu_signature = signature
                    logger.info(f"Futu连接成功: {self._futu_host}:{self._futu_port} (futuquant, shared)")
                    return True
                except Exception as inner_e:
                    if futu_import_error is not None:
                        logger.warning(
                            f"Futu连接失败: futu-api 未安装或不可用 ({futu_import_error}); "
                            f"旧版 futuquant 也不可用 ({inner_e})"
                        )
                    else:
                        logger.warning(f"Futu连接失败: {inner_e}")
                    self._futu_connected = False
                    return False
            except Exception as e:
                logger.warning(f"Futu连接失败: {e}")
                self._futu_connected = False
                return False
    
    def close(self):
        """释放当前实例对共享 Futu 连接的引用"""
        self._futu_ctx = None
        self._futu_connected = False

    @classmethod
    def close_shared_futu(cls):
        """显式关闭进程内共享 Futu 连接"""
        with cls._shared_lock:
            if cls._shared_futu_ctx is not None:
                try:
                    cls._shared_futu_ctx.close()
                except Exception:
                    pass
            cls._shared_futu_ctx = None
            cls._shared_futu_connected = False
            cls._shared_futu_signature = ""
            cls._shared_subscribed = set()
    
    def _futu_normalize(self, symbol: str) -> str:
        """Futu代码标准化"""
        symbol = str(symbol).strip().upper().zfill(6)
        if symbol.startswith(("SH.", "SZ.")):
            return symbol
        if symbol.startswith(("6", "5", "9")):
            return f"SH.{symbol}"
        return f"SZ.{symbol}"
    
    def _futu_index_normalize(self, symbol: str) -> str:
        """Futu指数代码标准化"""
        raw = str(symbol).strip().upper()
        if raw.startswith(("SH.", "SZ.")):
            return raw
        symbol = raw.zfill(6)
        if symbol in INDEX_FUTU_MAP:
            return INDEX_FUTU_MAP[symbol]
        return self._futu_normalize(symbol)

    def _ensure_futu_sub(self, codes: List[str], sub_types: Optional[List] = None):
        """确保Futu订阅"""
        try:
            from futu import SubType
        except ImportError:
            from futuquant import SubType

        actual_sub_types = sub_types or [SubType.QUOTE]
        need = []
        for code in codes:
            subscribed_all = True
            for sub_type in actual_sub_types:
                cache_key = f"{code}:{str(sub_type)}"
                if cache_key not in self.__class__._shared_subscribed:
                    subscribed_all = False
                    break
            if not subscribed_all:
                need.append(code)

        if need:
            try:
                ret, data = self._futu_ctx.subscribe(need, actual_sub_types, subscribe_push=False)
                if ret == 0:
                    for code in need:
                        for sub_type in actual_sub_types:
                            self.__class__._shared_subscribed.add(f"{code}:{str(sub_type)}")
                else:
                    logger.warning(
                        f"Futu订阅返回失败: codes={need}, sub_types={actual_sub_types}, ret={ret}, data={data}"
                    )
            except Exception as e:
                logger.warning(f"Futu订阅失败: {e}")

    def get_index_minute_bars(self, symbol: str, count: int = 60) -> pd.DataFrame:
        """获取指数 1 分钟级别行情（Futu 优先）"""
        if not self._init_futu():
            return pd.DataFrame()

        try:
            try:
                from futu import KLType, SubType
            except ImportError:
                from futuquant import KLType, SubType

            code = self._futu_index_normalize(symbol)
            self._ensure_futu_sub([code], [SubType.K_1M])

            ret, data = self._futu_ctx.get_cur_kline(code, num=count, ktype=KLType.K_1M)
            if ret == 0 and data is not None and not data.empty:
                df = data.copy()
                df.columns = [str(c).lower() for c in df.columns]
                if "time_key" in df.columns:
                    df["datetime"] = pd.to_datetime(df["time_key"], errors="coerce")
                if "code" in df.columns:
                    df["code"] = df["code"].astype(str)
                return df
        except Exception as e:
            logger.warning(f"Futu获取指数分钟K线失败 {symbol}: {e}")

        try:
            code = self._futu_index_normalize(symbol)
            self._ensure_futu_sub([code], [SubType.RT_DATA])
            ret, data = self._futu_ctx.get_rt_data(code)
            if ret == 0 and data is not None and not data.empty:
                df = data.copy()
                df.columns = [str(c).lower() for c in df.columns]
                time_col = "time" if "time" in df.columns else ("time_key" if "time_key" in df.columns else "")
                if time_col:
                    today = datetime.now().strftime("%Y-%m-%d")
                    df["datetime"] = pd.to_datetime(today + " " + df[time_col].astype(str), errors="coerce")
                if "cur_price" in df.columns and "close" not in df.columns:
                    df["close"] = pd.to_numeric(df["cur_price"], errors="coerce")
                if "last_close" in df.columns and "open" not in df.columns:
                    df["open"] = pd.to_numeric(df["last_close"], errors="coerce")
                if "close" in df.columns:
                    df["high"] = pd.to_numeric(df.get("high", df["close"]), errors="coerce").fillna(df["close"])
                    df["low"] = pd.to_numeric(df.get("low", df["close"]), errors="coerce").fillna(df["close"])
                if "volume" not in df.columns:
                    df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0.0)
                if "turnover" in df.columns and "amount" not in df.columns:
                    df["amount"] = pd.to_numeric(df["turnover"], errors="coerce").fillna(0.0)
                return df
        except Exception as e:
            logger.warning(f"Futu获取指数分时失败 {symbol}: {e}")

        return pd.DataFrame()

    def _normalize_snapshot_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化 Futu 市场快照字段"""
        if df is None or df.empty:
            return pd.DataFrame()

        result = df.copy()
        result.columns = [str(c).lower() for c in result.columns]

        if "code" in result.columns:
            result["code"] = (
                result["code"].astype(str).str.replace("SH.", "", regex=False).str.replace("SZ.", "", regex=False)
            )

        numeric_cols = [
            "last_price",
            "open_price",
            "high_price",
            "low_price",
            "prev_close_price",
            "volume",
            "turnover",
            "turnover_rate",
            "amplitude",
            "total_market_val",
            "pe_ttm_ratio",
            "pb_ratio",
        ]
        for col in numeric_cols:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors="coerce")

        if "change_rate" not in result.columns and {"last_price", "prev_close_price"}.issubset(result.columns):
            prev_close = pd.to_numeric(result["prev_close_price"], errors="coerce").replace(0, float("nan"))
            last_price = pd.to_numeric(result["last_price"], errors="coerce")
            result["change_rate"] = ((last_price - prev_close) / prev_close * 100).astype(float)

        return result

    def _get_a_share_codes(self) -> List[str]:
        """获取 A 股证券列表（Futu 静态信息，带缓存）"""
        global _a_share_codes_cache
        global _a_share_codes_cache_time

        if _a_share_codes_cache and _a_share_codes_cache_time:
            if datetime.now() - _a_share_codes_cache_time < timedelta(hours=12):
                return list(_a_share_codes_cache)

        if not self._init_futu():
            return []

        try:
            try:
                from futu import Market, SecurityType
            except ImportError:
                from futuquant import Market, SecurityType

            codes: List[str] = []
            for market in [Market.SH, Market.SZ]:
                ret, data = self._futu_ctx.get_stock_basicinfo(market=market, stock_type=SecurityType.STOCK)
                if ret == 0 and data is not None and not data.empty and "code" in data.columns:
                    codes.extend(data["code"].astype(str).tolist())

            unique_codes = sorted(list(set(codes)))
            _a_share_codes_cache = unique_codes
            _a_share_codes_cache_time = datetime.now()
            return unique_codes
        except Exception as e:
            logger.warning(f"Futu获取 A 股列表失败: {e}")
            return []

    def get_market_snapshots(self, symbols: List[str]) -> pd.DataFrame:
        """获取指定标的的市场快照（Futu 优先）"""
        if not symbols:
            return pd.DataFrame()
        if not self._init_futu():
            return pd.DataFrame()

        codes = [
            self._futu_index_normalize(s) if str(s).zfill(6) in INDEX_FUTU_MAP else self._futu_normalize(s)
            for s in symbols
        ]
        frames: List[pd.DataFrame] = []

        try:
            batch_size = 400
            for i in range(0, len(codes), batch_size):
                batch = codes[i:i + batch_size]
                ret, data = self._futu_ctx.get_market_snapshot(batch)
                if ret == 0 and data is not None and not data.empty:
                    frames.append(data.copy())
            if frames:
                merged = pd.concat(frames, ignore_index=True, sort=False)
                return self._normalize_snapshot_df(merged)
        except Exception as e:
            logger.warning(f"Futu获取市场快照失败: {e}")

        return pd.DataFrame()

    def _get_stock_basicinfo_frame(self, stock_type: str) -> pd.DataFrame:
        """获取沪深市场指定证券类型静态列表（Futu）"""
        if not self._init_futu():
            return pd.DataFrame()

        frames: List[pd.DataFrame] = []
        try:
            try:
                from futu import Market
            except ImportError:
                from futuquant import Market

            for market in [Market.SH, Market.SZ]:
                ret, data = self._futu_ctx.get_stock_basicinfo(market=market, stock_type=stock_type)
                if ret == 0 and data is not None and not data.empty:
                    frames.append(data.copy())
        except Exception as e:
            logger.warning(f"Futu获取静态证券列表失败 {stock_type}: {e}")
            return pd.DataFrame()

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames, ignore_index=True, sort=False)
        result.columns = [str(c).lower() for c in result.columns]
        if "code" in result.columns:
            result["code"] = result["code"].astype(str).map(_normalize_plain_code)
        return result

    @staticmethod
    def _is_lof_code(code: str, name: str = "") -> bool:
        """按代码和名称近似判断 LOF"""
        text_code = _normalize_plain_code(code)
        text_name = str(name or "").upper()
        if "LOF" in text_name:
            return True
        return text_code.startswith(("16", "50", "501", "502", "505", "506"))

    def _build_fund_spot_frame(self, prefer_lof: bool) -> pd.DataFrame:
        """使用 Futu 静态列表 + 快照构建基金列表"""
        base_df = self._get_stock_basicinfo_frame("ETF")
        if base_df is None or base_df.empty:
            return pd.DataFrame()

        base_df = base_df.copy()
        base_df["is_lof"] = base_df.apply(
            lambda row: self._is_lof_code(row.get("code", ""), row.get("name", "")),
            axis=1,
        )
        base_df = base_df[base_df["is_lof"] == bool(prefer_lof)]
        if base_df.empty:
            return pd.DataFrame()

        snapshot = self.get_market_snapshots(base_df["code"].astype(str).tolist())
        snapshot_map = {}
        if snapshot is not None and not snapshot.empty and "code" in snapshot.columns:
            snapshot_map = {
                _normalize_plain_code(row.get("code", "")): row
                for _, row in snapshot.iterrows()
            }

        records = []
        for _, row in base_df.iterrows():
            code = _normalize_plain_code(row.get("code", ""))
            snap = snapshot_map.get(code, {})
            turnover = float(pd.to_numeric(getattr(snap, "get", lambda *_: 0)("turnover", 0), errors="coerce") or 0.0)
            last_price = float(pd.to_numeric(getattr(snap, "get", lambda *_: 0)("last_price", 0), errors="coerce") or 0.0)
            outstanding_units = float(pd.to_numeric(getattr(snap, "get", lambda *_: 0)("trust_outstanding_units", 0), errors="coerce") or 0.0)
            liquidity_proxy = turnover
            if liquidity_proxy <= 0 and outstanding_units > 0 and last_price > 0:
                # 盘前阶段 Futu 当日成交额常为 0，这里退回到“基金规模近似流动性”代理值
                liquidity_proxy = outstanding_units * last_price * 0.001
            records.append({
                "代码": code,
                "名称": str(row.get("name", "") or ""),
                "成交额": float(liquidity_proxy),
                "涨跌幅": float(pd.to_numeric(getattr(snap, "get", lambda *_: 0)("change_rate", 0), errors="coerce") or 0.0),
                "最新价": float(last_price),
                "更新时间": str(getattr(snap, "get", lambda *_: "")("update_time", "") or ""),
            })

        result = pd.DataFrame(records)
        if not result.empty:
            result = result.sort_values(["成交额", "代码"], ascending=[False, True]).reset_index(drop=True)
        return result

    def get_owner_plates(self, symbol: str) -> pd.DataFrame:
        """获取证券所属板块（Futu）"""
        if not self._init_futu():
            return pd.DataFrame()
        try:
            code = self._futu_index_normalize(symbol) if _normalize_plain_code(symbol) in INDEX_FUTU_MAP else self._futu_normalize(symbol)
            ret, data = self._futu_ctx.get_owner_plate([code])
            if ret == 0 and data is not None and not data.empty:
                result = data.copy()
                result.columns = [str(c).lower() for c in result.columns]
                if "code" in result.columns:
                    result["code"] = result["code"].astype(str).map(_normalize_plain_code)
                return result
        except Exception as e:
            logger.warning(f"Futu获取所属板块失败 {symbol}: {e}")
        return pd.DataFrame()

    def get_plate_list(self, plate_type: str) -> pd.DataFrame:
        """获取板块列表（Futu）"""
        if not self._init_futu():
            return pd.DataFrame()
        try:
            try:
                from futu import Market, Plate
            except ImportError:
                from futuquant import Market, Plate

            plate_class = getattr(Plate, str(plate_type or "").upper(), None)
            if plate_class is None:
                return pd.DataFrame()

            frames: List[pd.DataFrame] = []
            seen_codes: Set[str] = set()
            for market in [Market.SH, Market.SZ]:
                ret, data = self._futu_ctx.get_plate_list(market, plate_class)
                if ret != 0 or data is None or data.empty:
                    continue
                frame = data.copy()
                frame.columns = [str(c).lower() for c in frame.columns]
                if "code" in frame.columns:
                    frame = frame[~frame["code"].astype(str).isin(seen_codes)]
                    seen_codes |= set(frame["code"].astype(str).tolist())
                frames.append(frame)
            if frames:
                return pd.concat(frames, ignore_index=True, sort=False)
        except Exception as e:
            logger.warning(f"Futu获取板块列表失败 {plate_type}: {e}")
        return pd.DataFrame()

    def get_plate_stocks(self, plate_name: str, plate_type: str) -> pd.DataFrame:
        """按名称获取板块成分股（Futu）"""
        plate_code = self._resolve_plate_code(plate_name, plate_type)
        return self._get_plate_members(plate_code)

    def _resolve_plate_code(self, plate_name: str, plate_type: str) -> str:
        """根据板块名称解析 Futu 板块代码"""
        if not self._init_futu():
            return ""

        target = str(plate_name or "").strip().lower()
        if not target:
            return ""

        try:
            try:
                from futu import Market, Plate
            except ImportError:
                from futuquant import Market, Plate

            plate_class = getattr(Plate, plate_type.upper(), None)
            if plate_class is None:
                return ""

            candidates: Dict[str, str] = {}
            for market in [Market.SH, Market.SZ]:
                ret, data = self._futu_ctx.get_plate_list(market, plate_class)
                if ret != 0 or data is None or data.empty:
                    continue
                for _, row in data.iterrows():
                    name = str(row.get("plate_name", "") or "").strip()
                    code = str(row.get("code", "") or "").strip()
                    if name and code:
                        candidates[name.lower()] = code
                if target in candidates:
                    return candidates[target]

            for name, code in candidates.items():
                if target in name or name in target:
                    return code
        except Exception as e:
            logger.warning(f"Futu解析板块代码失败 {plate_name}/{plate_type}: {e}")

        return ""

    def _get_plate_members(self, plate_code: str) -> pd.DataFrame:
        """获取板块成分股（Futu）"""
        if not self._init_futu() or not plate_code:
            return pd.DataFrame()
        try:
            ret, data = self._futu_ctx.get_plate_stock(plate_code)
            if ret == 0 and data is not None and not data.empty:
                result = data.copy()
                result.columns = [str(c).lower() for c in result.columns]
                if "code" in result.columns:
                    result["code"] = result["code"].astype(str).map(_normalize_plain_code)
                return result
        except Exception as e:
            logger.warning(f"Futu获取板块成分股失败 {plate_code}: {e}")
        return pd.DataFrame()

    def _get_recent_limit_streak(
        self,
        symbol: str,
        limit_pct: float,
        current_is_limit: bool = False,
    ) -> int:
        """根据日线近似估算连续涨停天数"""
        if not self._init_futu():
            return 1 if current_is_limit else 0

        def _get_daily_bars(target_symbol: str, max_count: int) -> pd.DataFrame:
            try:
                try:
                    from futu import KLType
                except ImportError:
                    from futuquant import KLType

                code = self._futu_normalize(target_symbol)
                ret, data, _ = self._futu_ctx.request_history_kline(code, ktype=KLType.K_DAY, max_count=max_count)
                if ret == 0 and data is not None and not data.empty:
                    return data.copy()
            except Exception:
                pass
            return pd.DataFrame()

        return get_recent_limit_streak(
            symbol=symbol,
            limit_pct=limit_pct,
            current_is_limit=current_is_limit,
            get_daily_bars=_get_daily_bars,
        )

    def get_limit_pool(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """基于 Futu 快照近似构建涨停池、跌停池、炸板池"""
        snapshot = self.get_a_share_market_snapshot()
        if snapshot is not None and not snapshot.empty:
            snapshot = snapshot.copy()
            snapshot["code"] = snapshot["code"].astype(str).map(_normalize_plain_code)
        return build_limit_pool(
            snapshot=snapshot,
            get_streak=lambda symbol, limit_pct, current_is_limit: self._get_recent_limit_streak(symbol, limit_pct, current_is_limit),
        )

    def get_limit_status(self, symbol: str) -> Dict[str, float]:
        """获取单只标的的涨停状态、封单强度和炸板近似值"""
        snapshot = self.get_market_snapshots([symbol])
        if snapshot is None or snapshot.empty:
            return {}

        row = snapshot.iloc[0]
        row = row.copy()
        row["code"] = _normalize_plain_code(row.get("code", symbol))
        return build_limit_status(
            row=row,
            get_order_book=lambda target_symbol, depth: self.get_order_book(target_symbol, depth),
            get_streak=lambda target_symbol, limit_pct, current_is_limit: self._get_recent_limit_streak(target_symbol, limit_pct, current_is_limit),
        )

    def get_individual_capital_flow(self, symbol: str) -> Dict[str, float]:
        """获取个股资金流近似值（Futu 优先）"""
        if not self._init_futu():
            return {}

        code = self._futu_normalize(symbol)
        try:
            ret, data = self._futu_ctx.get_capital_flow(stock_code=code)
            if ret == 0 and data is not None and not data.empty:
                df = data.copy()
                df.columns = [str(c).lower() for c in df.columns]
                last_row = df.iloc[-1]
                amount = 0.0
                for column in ["net_inflow", "capital_inflow", "main_net_inflow"]:
                    if column in df.columns:
                        amount = float(pd.to_numeric(last_row.get(column, 0), errors="coerce") or 0.0)
                        break
                if amount != 0.0:
                    snapshot = self.get_market_snapshots([symbol])
                    turnover = float(pd.to_numeric(snapshot.iloc[0].get("turnover", 0), errors="coerce") or 0.0) if snapshot is not None and not snapshot.empty else 0.0
                    return {
                        "main_net_inflow": amount,
                        "main_net_ratio": amount / turnover * 100 if turnover > 0 else 0.0,
                    }
        except Exception as e:
            logger.debug(f"Futu获取资金流失败 {symbol}: {e}")

        try:
            ret, data = self._futu_ctx.get_capital_distribution(stock_code=code)
            if ret == 0 and data is not None and not data.empty:
                row = data.iloc[-1]
                inflow = sum(
                    float(pd.to_numeric(row.get(col, 0), errors="coerce") or 0.0)
                    for col in ["capital_in_super", "capital_in_big", "capital_in_mid", "capital_in_small"]
                )
                outflow = sum(
                    float(pd.to_numeric(row.get(col, 0), errors="coerce") or 0.0)
                    for col in ["capital_out_super", "capital_out_big", "capital_out_mid", "capital_out_small"]
                )
                net = inflow - outflow
                snapshot = self.get_market_snapshots([symbol])
                turnover = float(pd.to_numeric(snapshot.iloc[0].get("turnover", 0), errors="coerce") or 0.0) if snapshot is not None and not snapshot.empty else 0.0
                return {
                    "main_net_inflow": net,
                    "main_net_ratio": net / turnover * 100 if turnover > 0 else 0.0,
                }
        except Exception as e:
            logger.debug(f"Futu获取资金分布失败 {symbol}: {e}")

        return {}

    def get_order_book(self, symbol: str, depth: int = 5) -> Dict[str, object]:
        """
        获取单只标的五档盘口（Futu）

        Args:
            symbol: 标的代码，如 600036、513310
            depth: 档位深度，默认 5

        Returns:
            盘口字典，失败时返回空字典
        """
        if not self._init_futu():
            return {}

        try:
            try:
                from futu import SubType
            except ImportError:
                from futuquant import SubType

            normalized_symbol = str(symbol).zfill(6)
            code = self._futu_index_normalize(normalized_symbol) if normalized_symbol in INDEX_FUTU_MAP else self._futu_normalize(normalized_symbol)
            self._ensure_futu_sub([code], [SubType.ORDER_BOOK])

            ret, data = self._futu_ctx.get_order_book(code, num=depth)
            if ret != 0 or not isinstance(data, dict):
                logger.warning(f"Futu获取盘口失败 {symbol}: ret={ret}, data={data}")
                return {}

            bid_rows = []
            ask_rows = []

            for index, item in enumerate(data.get("Bid", [])[:depth], 1):
                bid_rows.append(self._normalize_order_book_row(item, index))

            for index, item in enumerate(data.get("Ask", [])[:depth], 1):
                ask_rows.append(self._normalize_order_book_row(item, index))

            return {
                "code": str(data.get("code", code)).replace("SH.", "").replace("SZ.", ""),
                "name": str(data.get("name", "")),
                "bid": bid_rows,
                "ask": ask_rows,
                "bid_time": str(data.get("svr_recv_time_bid", "")),
                "ask_time": str(data.get("svr_recv_time_ask", "")),
            }
        except Exception as e:
            logger.warning(f"Futu获取盘口异常 {symbol}: {e}")
            return {}

    @staticmethod
    def _normalize_order_book_row(item: object, level: int) -> Dict[str, object]:
        """
        标准化五档盘口单行
        """
        if not isinstance(item, (list, tuple)) or len(item) < 3:
            return {
                "level": level,
                "price": 0.0,
                "volume": 0.0,
                "order_count": 0,
                "raw": item,
            }

        price = pd.to_numeric(item[0], errors="coerce")
        volume = pd.to_numeric(item[1], errors="coerce")
        order_count = int(pd.to_numeric(item[2], errors="coerce") or 0)

        return {
            "level": level,
            "price": float(price) if pd.notna(price) else 0.0,
            "volume": float(volume) if pd.notna(volume) else 0.0,
            "order_count": order_count,
            "raw": item[3] if len(item) > 3 else {},
        }

    def get_a_share_market_snapshot(self) -> pd.DataFrame:
        """获取全市场 A 股快照（Futu 优先）"""
        codes = self._get_a_share_codes()
        if not codes:
            return pd.DataFrame()
        return self.get_market_snapshots(codes)
    
    def get_kline(self, symbol: str, start_date: str, end_date: str,
                   adjust: str = "qfq") -> pd.DataFrame:
        """
        获取K线数据 (历史 → baostock, 股票+ETF均支持)
        
        Args:
            symbol: 代码 (如: 000001, 512050)
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            adjust: 复权类型 qfq/hfq/None
        """
        symbol = str(symbol).zfill(6)
        if symbol.startswith(("51", "15", "16", "50", "56", "58")):
            return self._get_etf_kline(symbol, start_date, end_date, adjust)
        return self._get_kline_baostock(symbol, start_date, end_date, adjust)
    
    def _get_kline_baostock(self, symbol: str, start_date: str,
                             end_date: str, adjust: str = "qfq") -> pd.DataFrame:
        """使用baostock获取股票/ETF历史K线"""
        _ensure_bs_login()
        
        start_str = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}" if len(start_date) == 8 else start_date
        end_str = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}" if len(end_date) == 8 else end_date
        
        bs_code = _symbol_to_baostock(symbol)
        flag = _adjust_flag(adjust)
        
        try:
            rs = bs.query_history_k_data_plus(
                bs_code,
                "date,open,high,low,close,volume,amount,pctChg",
                start_date=start_str,
                end_date=end_str,
                frequency="d",
                adjustflag=flag
            )
            
            if rs.error_code != '0':
                logger.error(f"Baostock K线查询失败 {symbol}: {rs.error_msg}")
                return pd.DataFrame()
            
            data_list = []
            while rs.next():
                data_list.append(rs.get_row_data())
            
            if not data_list:
                return pd.DataFrame()
            
            df = pd.DataFrame(data_list, columns=rs.fields)
            df["date"] = pd.to_datetime(df["date"])
            
            for col in ["open", "high", "low", "close", "volume", "amount", "pctChg"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            
            df = df.rename(columns={"pctChg": "pct_change"})
            return df
        except Exception as e:
            logger.error(f"Baostock获取K线失败 {symbol}: {e}")
            return pd.DataFrame()
    
    def _get_etf_kline(self, symbol: str, start_date: str,
                        end_date: str, adjust: str = "qfq") -> pd.DataFrame:
        """获取ETF/LOF K线 (baostock)"""
        return self._get_kline_baostock(symbol, start_date, end_date, adjust)
    
    def get_realtime_quotes(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """
        获取实时行情 (Futu-only)
        
        Returns DataFrame with columns:
            code, name, last_price, change_rate, volume, turnover, ...
        """
        df = self._get_quotes_futu(symbols)
        if df is not None and not df.empty:
            return df
        
        logger.warning("Futu实时行情不可用，返回空结果")
        return self._get_quotes_akshare(symbols)
    
    def _get_quotes_futu(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """使用Futu获取实时行情"""
        try:
            normalized_symbols = symbols or []
            snapshot_df = self.get_market_snapshots(normalized_symbols)
            if snapshot_df is not None and not snapshot_df.empty:
                return snapshot_df

            if not normalized_symbols or not self._init_futu():
                return pd.DataFrame()

            try:
                try:
                    from futu import SubType
                except ImportError:
                    from futuquant import SubType

                code_list = [
                    self._futu_index_normalize(symbol) if str(symbol).zfill(6) in INDEX_FUTU_MAP else self._futu_normalize(symbol)
                    for symbol in normalized_symbols
                ]
                self._ensure_futu_sub(code_list, [SubType.QUOTE])
                ret, data = self._futu_ctx.get_stock_quote(code_list)
                if ret == 0 and data is not None and not data.empty:
                    result = data.copy()
                    result.columns = [str(c).lower() for c in result.columns]
                    if "data_date" in result.columns and "data_time" in result.columns:
                        result["update_time"] = (
                            result["data_date"].astype(str).str.strip() + " " + result["data_time"].astype(str).str.strip()
                        )
                    return self._normalize_snapshot_df(result)
            except Exception as inner_e:
                logger.debug(f"Futu报价接口获取失败: {inner_e}")
            return pd.DataFrame()
        except Exception as e:
            logger.warning(f"Futu获取实时行情失败: {e}")
            return pd.DataFrame()
    
    def _get_quotes_akshare(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """保留旧方法名以兼容调用，当前不再使用 akshare 兜底"""
        return pd.DataFrame()
    
    def get_stock_info(self, symbol: str) -> dict:
        """获取股票基本信息（Futu 优先，akshare 回退）"""
        try:
            snapshot = self.get_market_snapshots([symbol])
            if snapshot is not None and not snapshot.empty:
                row = snapshot.iloc[0]
                return {
                    "证券代码": str(row.get("code", "") or "").strip(),
                    "证券简称": str(row.get("name", "") or "").strip(),
                    "最新价": float(pd.to_numeric(row.get("last_price", 0), errors="coerce") or 0.0),
                    "涨跌幅": float(pd.to_numeric(row.get("change_rate", 0), errors="coerce") or 0.0),
                    "总市值": float(pd.to_numeric(row.get("total_market_val", 0), errors="coerce") or 0.0),
                    "流通市值": float(pd.to_numeric(row.get("circular_market_val", 0), errors="coerce") or 0.0),
                    "成交量": float(pd.to_numeric(row.get("volume", 0), errors="coerce") or 0.0),
                    "成交额": float(pd.to_numeric(row.get("turnover", 0), errors="coerce") or 0.0),
                    "市盈率": float(pd.to_numeric(row.get("pe_ratio", 0), errors="coerce") or 0.0),
                    "市盈率TTM": float(pd.to_numeric(row.get("pe_ttm_ratio", 0), errors="coerce") or 0.0),
                    "市净率": float(pd.to_numeric(row.get("pb_ratio", 0), errors="coerce") or 0.0),
                    "每股收益": float(pd.to_numeric(row.get("earning_per_share", 0), errors="coerce") or 0.0),
                    "每股净资产": float(pd.to_numeric(row.get("net_asset_per_share", 0), errors="coerce") or 0.0),
                    "更新时间": str(row.get("update_time", "") or "").strip(),
                }
        except Exception as e:
            logger.warning(f"Futu获取股票信息失败 {symbol}: {e}")

        try:
            df = ak.stock_individual_info_em(symbol=str(symbol).zfill(6))
            return dict(zip(df["item"], df["value"]))
        except Exception as e:
            logger.error(f"获取股票信息失败 {symbol}: {e}")
            return {}

    def get_index_daily(self, symbol: str = "000300") -> pd.DataFrame:
        """获取指数日线 (akshare)"""
        try:
            code = str(symbol or "").zfill(6)
            market_prefix = "sh" if code.startswith(("0", "5", "6", "9")) and not code.startswith("399") else "sz"
            df = ak.stock_zh_index_daily(symbol=f"{market_prefix}{code}")
            df["date"] = pd.to_datetime(df["date"])
            return df
        except Exception as e:
            logger.error(f"获取指数日线失败 {symbol}: {e}")
            return pd.DataFrame()
    
    def get_industry_stocks(self, industry: str) -> List[str]:
        """获取行业成分股 (Futu)"""
        try:
            plate_code = self._resolve_plate_code(industry, "INDUSTRY")
            df = self._get_plate_members(plate_code)
            if df is not None and not df.empty and "code" in df.columns:
                return df["code"].astype(str).tolist()
        except Exception as e:
            logger.error(f"获取行业成分股失败 {industry}: {e}")
        return []
    
    def get_concept_stocks(self, concept: str) -> List[str]:
        """获取概念成分股 (Futu)"""
        try:
            plate_code = self._resolve_plate_code(concept, "CONCEPT")
            df = self._get_plate_members(plate_code)
            if df is not None and not df.empty and "code" in df.columns:
                return df["code"].astype(str).tolist()
        except Exception as e:
            logger.error(f"获取概念成分股失败 {concept}: {e}")
        return []
    
    def get_financial_data(self, symbol: str, type_: str = "balancesheet") -> pd.DataFrame:
        """获取财务数据（优先新浪财报接口）"""
        try:
            symbol_map = {
                "balancesheet": "资产负债表",
                "income": "利润表",
                "cashflow": "现金流量表",
            }
            report_symbol = symbol_map.get(type_)
            if report_symbol:
                market_prefix = "sh" if str(symbol).zfill(6).startswith(("5", "6", "9")) else "sz"
                report_df = ak.stock_financial_report_sina(
                    stock=f"{market_prefix}{str(symbol).zfill(6)}",
                    symbol=report_symbol,
                )
                if report_df is not None and not report_df.empty:
                    return report_df
        except Exception as e:
            logger.warning(f"新浪财报获取失败 {symbol}/{type_}: {e}")

        try:
            if type_ == "income":
                df = ak.stock_financial_abstract(symbol=str(symbol).zfill(6))
                if df is not None and not df.empty:
                    return df
        except Exception as e:
            logger.error(f"获取财务数据失败 {symbol}: {e}")
        return pd.DataFrame()

    def get_etf_list(self) -> pd.DataFrame:
        """获取ETF列表 (Futu)"""
        try:
            df = self._build_fund_spot_frame(prefer_lof=False)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logger.warning(f"获取ETF列表失败: {e}")
        return self._get_default_etf_list()
    
    def get_lof_list(self) -> pd.DataFrame:
        """获取LOF列表 (Futu近似分类)"""
        try:
            df = self._build_fund_spot_frame(prefer_lof=True)
            if df is not None and not df.empty:
                return df
        except Exception as e:
            logger.warning(f"获取LOF列表失败: {e}")
        return self._get_default_lof_list()
    
    def _get_default_etf_list(self) -> pd.DataFrame:
        df = pd.DataFrame([i for i in DEFAULT_ETF_LIST if i["type"] == "ETF"])
        if not df.empty:
            df = df.rename(columns={"code": "代码", "name": "名称"})
            df["成交额"] = 1_000_000_000
        return df
    
    def _get_default_lof_list(self) -> pd.DataFrame:
        df = pd.DataFrame([i for i in DEFAULT_ETF_LIST if i["type"] == "LOF"])
        if not df.empty:
            df = df.rename(columns={"code": "代码", "name": "名称"})
            df["成交额"] = 1_000_000_000
        return df
    
    def get_default_pool(self) -> List[dict]:
        """默认ETF/LOF股票池"""
        products = []
        for item in DEFAULT_ETF_LIST:
            products.append({
                "code": item["code"],
                "name": item["name"],
                "amount": 1_000_000_000,
                "type": item["type"],
                "t0": item["code"].startswith(("51", "15"))
            })
        products.sort(key=lambda x: (-x["amount"], not x["t0"]))
        return products
    
    def get_etf_lof_pool(self, min_amount: float = 300_000_000,
                         prefer_t0: bool = True) -> List[dict]:
        """获取ETF/LOF股票池"""
        etf_list, lof_list = [], []
        
        try:
            df = self.get_etf_list()
            if not df.empty:
                df.columns = [c.lower() for c in df.columns]
                if "成交额" in df.columns:
                    df = df[df["成交额"] >= min_amount]
                    etf_list = df.to_dict("records")
        except Exception as e:
            logger.warning(f"获取ETF列表失败: {e}")
        
        try:
            df = self.get_lof_list()
            if not df.empty:
                df.columns = [c.lower() for c in df.columns]
                if "成交额" in df.columns:
                    df = df[df["成交额"] >= min_amount]
                    lof_list = df.to_dict("records")
        except Exception as e:
            logger.warning(f"获取LOF列表失败: {e}")
        
        if not etf_list and not lof_list:
            logger.warning("ETF/LOF列表获取失败，使用默认池")
            return self.get_default_pool()
        
        all_products = []
        for item in etf_list:
            code = str(item.get("代码", "")).zfill(6)
            all_products.append({
                "code": code,
                "name": str(item.get("名称", "")),
                "amount": item.get("成交额", 0),
                "type": "ETF",
                "t0": code.startswith(("51", "15"))
            })
        for item in lof_list:
            code = str(item.get("代码", "")).zfill(6)
            all_products.append({
                "code": code,
                "name": str(item.get("名称", "")),
                "amount": item.get("成交额", 0),
                "type": "LOF",
                "t0": code.startswith(("16", "15"))
            })
        
        if prefer_t0:
            all_products.sort(key=lambda x: (not x["t0"], -x["amount"]))
        else:
            all_products.sort(key=lambda x: -x["amount"])
        
        return all_products
