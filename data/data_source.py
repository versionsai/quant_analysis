# -*- coding: utf-8 -*-
"""
A股数据源 - baostock历史K线 + futu实时行情 + akshare辅助数据
- 历史K线: baostock (支持股票+ETF)
- 实时行情: futu-api (Futu OpenD)
- ETF/LOF列表: akshare
"""
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import akshare as ak
import baostock as bs
import pandas as pd

from utils.logger import get_logger
from utils.miaoxiang_client import query_financial_data_dict, query_financial_data_frame

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


class DataSource:
    
    def __init__(self, cache_dir: str = None):
        if cache_dir is None:
            cache_dir = os.environ.get("QUANT_CACHE_DIR", "./runtime/data/cache")
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)
        
        self._futu_ctx = None
        self._futu_host = os.environ.get("FUTU_HOST", "127.0.0.1")
        self._futu_port = int(os.environ.get("FUTU_PORT", 11111))
        self._futu_connected = False
        self._subscribed: set = set()
    
    def _init_futu(self) -> bool:
        """初始化Futu连接 (用于实时行情)"""
        if self._futu_connected and self._futu_ctx is not None:
            return True
        futu_import_error = None
        try:
            from futu import OpenQuoteContext
            self._futu_ctx = OpenQuoteContext(host=self._futu_host, port=self._futu_port)
            self._futu_connected = True
            logger.info(f"Futu连接成功: {self._futu_host}:{self._futu_port} (futu-api)")
            return True
        except ImportError as e:
            futu_import_error = e
            try:
                from futuquant import OpenQuoteContext
                self._futu_ctx = OpenQuoteContext(host=self._futu_host, port=self._futu_port)
                self._futu_connected = True
                logger.info(f"Futu连接成功: {self._futu_host}:{self._futu_port} (futuquant)")
                return True
            except Exception as e:
                if futu_import_error is not None:
                    logger.warning(
                        f"Futu连接失败: futu-api 未安装或不可用 ({futu_import_error}); "
                        f"旧版 futuquant 也不可用 ({e})"
                    )
                else:
                    logger.warning(f"Futu连接失败: {e}")
                self._futu_connected = False
                return False
        except Exception as e:
            logger.warning(f"Futu连接失败: {e}")
            self._futu_connected = False
            return False
    
    def close(self):
        """关闭Futu连接"""
        if self._futu_ctx:
            try:
                self._futu_ctx.close()
            except Exception:
                pass
            self._futu_ctx = None
            self._futu_connected = False
    
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
                if cache_key not in self._subscribed:
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
                            self._subscribed.add(f"{code}:{str(sub_type)}")
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
        获取实时行情 (Futu优先, akshare兜底)
        
        Returns DataFrame with columns:
            code, name, last_price, change_rate, volume, turnover, ...
        """
        df = self._get_quotes_futu(symbols)
        if df is not None and not df.empty:
            return df
        
        logger.warning("Futu实时行情失败，尝试akshare")
        return self._get_quotes_akshare(symbols)
    
    def _get_quotes_futu(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """使用Futu获取实时行情"""
        try:
            return self.get_market_snapshots(symbols or [])
        except Exception as e:
            logger.warning(f"Futu获取实时行情失败: {e}")
            return pd.DataFrame()
    
    def _get_quotes_akshare(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """akshare实时行情兜底"""
        try:
            code_list = [str(s).zfill(6) for s in symbols] if symbols else []
            frames = []

            need_stock = True
            need_fund = True
            if code_list:
                need_stock = any(not code.startswith(("51", "15", "16", "50", "56")) for code in code_list)
                need_fund = any(code.startswith(("51", "15", "16", "50", "56")) for code in code_list)

            if need_stock:
                stock_df = ak.stock_zh_a_spot_em()
                if stock_df is not None and not stock_df.empty:
                    frames.append(stock_df)

            if need_fund:
                for fetcher in [ak.fund_etf_spot_em, ak.fund_lof_spot_em]:
                    try:
                        fund_df = fetcher()
                        if fund_df is not None and not fund_df.empty:
                            frames.append(fund_df)
                    except Exception:
                        continue

            if frames:
                df = pd.concat(frames, ignore_index=True, sort=False)
                if symbols and "代码" in df.columns:
                    df = df[df["代码"].astype(str).isin(code_list)]
                return df
        except Exception as e:
            logger.error(f"akshare获取实时行情失败: {e}")
        return pd.DataFrame()
    
    def get_stock_info(self, symbol: str) -> dict:
        """获取股票基本信息（妙想优先，akshare 回退）"""
        mx_info = query_financial_data_dict(
            f"查询{str(symbol).zfill(6)} 最新价、涨跌幅、总市值、市盈率、市净率、证券简称等基本信息",
            output_dir="runtime/mx_finance_data_datasource",
        )
        if mx_info:
            return mx_info

        try:
            df = ak.stock_individual_info_em(symbol=str(symbol).zfill(6))
            return dict(zip(df["item"], df["value"]))
        except Exception as e:
            logger.error(f"获取股票信息失败 {symbol}: {e}")
            return {}

    def get_index_daily(self, symbol: str = "000300") -> pd.DataFrame:
        """获取指数日线 (akshare)"""
        try:
            df = ak.stock_zh_index_daily(symbol=f"sh{symbol}")
            df["date"] = pd.to_datetime(df["date"])
            return df
        except Exception as e:
            logger.error(f"获取指数日线失败 {symbol}: {e}")
            return pd.DataFrame()
    
    def get_industry_stocks(self, industry: str) -> List[str]:
        """获取行业成分股 (akshare)"""
        try:
            df = ak.stock_board_industry_name_em()
            code = df[df["板块名称"] == industry]["板块代码"].values[0]
            df = ak.stock_board_industry_cons_em(symbol=code)
            return df["代码"].tolist()
        except Exception as e:
            logger.error(f"获取行业成分股失败 {industry}: {e}")
            return []
    
    def get_concept_stocks(self, concept: str) -> List[str]:
        """获取概念成分股 (akshare)"""
        try:
            df = ak.stock_board_concept_name_em()
            code = df[df["板块名称"] == concept]["板块代码"].values[0]
            df = ak.stock_board_concept_cons_em(symbol=code)
            return df["代码"].tolist()
        except Exception as e:
            logger.error(f"获取概念成分股失败 {concept}: {e}")
            return []
    
    def get_financial_data(self, symbol: str, type_: str = "balancesheet") -> pd.DataFrame:
        """获取财务数据（妙想优先，akshare 回退）"""
        query_map = {
            "balancesheet": f"查询{str(symbol).zfill(6)} 资产负债表主要字段",
            "income": f"查询{str(symbol).zfill(6)} 利润表主要字段",
            "cashflow": f"查询{str(symbol).zfill(6)} 现金流量表主要字段",
        }
        if type_ in query_map:
            mx_df = query_financial_data_frame(
                query_map[type_],
                output_dir="runtime/mx_finance_data_datasource",
            )
            if mx_df is not None and not mx_df.empty:
                return mx_df

        try:
            func_map = {
                "balancesheet": ak.stock_balance_sheet,
                "income": ak.stock_income,
                "cashflow": ak.stock_cashflow
            }
            df = func_map[type_](symbol=str(symbol).zfill(6))
            return df
        except Exception as e:
            logger.error(f"获取财务数据失败 {symbol}: {e}")
            return pd.DataFrame()

    def get_etf_list(self) -> pd.DataFrame:
        """获取ETF列表 (akshare)"""
        try:
            return ak.fund_etf_spot_em()
        except Exception as e:
            logger.warning(f"获取ETF列表失败: {e}")
            return self._get_default_etf_list()
    
    def get_lof_list(self) -> pd.DataFrame:
        """获取LOF列表 (akshare)"""
        try:
            return ak.fund_lof_spot_em()
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
