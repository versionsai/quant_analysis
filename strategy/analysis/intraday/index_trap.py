# -*- coding: utf-8 -*-
"""
盘中诱多/诱空识别

基于上证指数、中证500、中证1000的 1 分钟结构、
成交量、VWAP 和跨指数共振，判断盘中更偏诱多还是诱空。
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from config.config import STRATEGY_CONFIG
from strategy.analysis.base_analyzer import BaseAnalyzer
from utils.logger import get_logger

logger = get_logger(__name__)


INDEX_MAP = {
    "000001": "上证指数",
    "000905": "中证500",
    "000852": "中证1000",
}


@dataclass
class IndexMinuteSnapshot:
    """单个指数的盘中快照"""

    code: str
    name: str
    latest_price: float = 0.0
    return_1m: float = 0.0
    return_3m: float = 0.0
    return_5m: float = 0.0
    volume_ratio: float = 1.0
    vwap_bias: float = 0.0
    breakout_fail: float = 0.0
    breakdown_fail: float = 0.0
    slope_score: float = 0.0
    fake_up_score: float = 0.0
    fake_down_score: float = 0.0
    comment: str = ""


@dataclass
class IntradayTrapSignal:
    """盘中诱多/诱空结果"""

    as_of: str
    trap_type: str = "neutral"
    fake_up_score: float = 0.0
    fake_down_score: float = 0.0
    data_ready: bool = True
    breadth_comment: str = ""
    summary: str = ""
    snapshots: List[IndexMinuteSnapshot] = field(default_factory=list)

    def to_message(self) -> str:
        """转为推送文本"""
        lines = ["【盘中诱多/诱空监控】", f"时间: {self.as_of}"]
        lines.append(f"数据状态: {'完整' if self.data_ready else '不足'}")
        lines.append(
            f"判定: {self.trap_type} | 诱多分 {self.fake_up_score:.2f} | "
            f"诱空分 {self.fake_down_score:.2f}"
        )
        if self.breadth_comment:
            lines.append(f"共振: {self.breadth_comment}")
        if self.summary:
            lines.append(f"结论: {self.summary}")
        lines.append("-" * 24)
        for item in self.snapshots:
            lines.append(
                f"{item.name}({item.code}) | "
                f"1m {item.return_1m:+.2f}% | "
                f"5m {item.return_5m:+.2f}% | "
                f"量比 {item.volume_ratio:.2f} | "
                f"VWAP偏离 {item.vwap_bias:+.2f}%"
            )
            lines.append(
                f"诱多 {item.fake_up_score:.2f} | 诱空 {item.fake_down_score:.2f} | {item.comment}"
            )
        return "\n".join(lines)


class IntradayTrapAnalyzer(BaseAnalyzer):
    """盘中诱多/诱空分析器"""

    def __init__(self):
        super().__init__("IntradayTrap")
        self._cache_ttl = 30
        self._patch_tried = False

    def analyze(self, **kwargs) -> IntradayTrapSignal:
        """执行盘中诱多/诱空分析"""
        return self.analyze_market_intraday(as_of=kwargs.get("as_of"))

    def analyze_market_intraday(self, as_of: Optional[datetime] = None) -> IntradayTrapSignal:
        """抓取分钟数据并分析"""
        ts = as_of or datetime.now()
        cache_key = f"intraday_trap_{ts.strftime('%Y%m%d_%H%M')}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        frames: Dict[str, pd.DataFrame] = {}
        data_source = None
        try:
            from data.data_source import DataSource

            data_source = DataSource()
            for code in INDEX_MAP:
                df = self._fetch_index_minute(code, ts, data_source=data_source)
                if df is None or df.empty:
                    logger.warning(f"盘中指数分钟数据为空: {code}")
                    continue
                frames[code] = df
        finally:
            if data_source is not None:
                try:
                    data_source.close()
                except Exception:
                    pass

        result = self.analyze_from_frames(frames, as_of=ts)
        self._set_cache(cache_key, result)
        return result

    def analyze_from_frames(self, frames: Dict[str, pd.DataFrame], as_of: Optional[datetime] = None) -> IntradayTrapSignal:
        """基于给定分钟数据进行分析，便于离线验证"""
        ts = as_of or datetime.now()
        snapshots: List[IndexMinuteSnapshot] = []
        for code, frame in frames.items():
            try:
                snapshots.append(self._analyze_single_index(code, frame))
            except Exception as e:
                logger.warning(f"盘中指数分析失败 {code}: {e}")

        fake_up_score, fake_down_score, breadth_comment = self._combine_snapshots(snapshots)
        trap_type, summary = self._classify(fake_up_score, fake_down_score, snapshots, breadth_comment)
        data_ready = len(snapshots) >= len(INDEX_MAP)
        return IntradayTrapSignal(
            as_of=ts.strftime("%Y-%m-%d %H:%M"),
            trap_type=trap_type,
            fake_up_score=fake_up_score,
            fake_down_score=fake_down_score,
            data_ready=data_ready,
            breadth_comment=breadth_comment,
            summary=summary,
            snapshots=snapshots,
        )

    def _install_patch(self):
        """安装 akshare 代理补丁"""
        if self._patch_tried:
            return
        self._patch_tried = True
        try:
            import akshare_proxy_patch

            akshare_proxy_patch.install_patch(
                "101.201.173.125",
                auth_token="",
                retry=30,
                hook_domains=[
                    "push2.eastmoney.com",
                    "push2his.eastmoney.com",
                    "push2ex.eastmoney.com",
                ],
            )
        except Exception as e:
            logger.debug(f"盘中诱多诱空补丁安装失败: {e}")

    def _fetch_index_minute(self, code: str, as_of: datetime, data_source=None) -> pd.DataFrame:
        """获取指数 1 分钟数据"""
        try:
            if data_source is not None:
                df = data_source.get_index_minute_bars(code, count=60)
                if df is not None and not df.empty:
                    return self._normalize_minute_df(df)
        except Exception as e:
            logger.warning(f"Futu分钟数据获取失败 {code}: {e}")

        try:
            import akshare as ak

            self._install_patch()

            start_dt = as_of.replace(hour=9, minute=30, second=0, microsecond=0)
            end_dt = as_of.replace(second=0, microsecond=0)
            df = ak.index_zh_a_hist_min_em(
                symbol=code,
                period="1",
                start_date=start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                end_date=end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            )
            return self._normalize_minute_df(df)
        except Exception as e:
            logger.warning(f"获取指数分钟数据失败 {code}: {e}")
            return pd.DataFrame()

    def _normalize_minute_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """标准化分钟数据"""
        if df is None or df.empty:
            return pd.DataFrame()

        result = df.copy()
        result.columns = [str(c).strip().lower() for c in result.columns]
        mapping = {
            "日期": "datetime",
            "时间": "datetime",
            "day": "datetime",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "volume",
            "成交额": "amount",
        }
        result = result.rename(columns={k.lower(): v for k, v in mapping.items()})

        if "datetime" not in result.columns:
            return pd.DataFrame()

        result["datetime"] = pd.to_datetime(result["datetime"], errors="coerce")
        result = result.dropna(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)

        for col in ["open", "high", "low", "close", "volume", "amount"]:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors="coerce")

        if "close" not in result.columns or result["close"].dropna().empty:
            return pd.DataFrame()

        result["close"] = result["close"].ffill()
        if "high" not in result.columns:
            result["high"] = result["close"]
        if "low" not in result.columns:
            result["low"] = result["close"]
        if "open" not in result.columns:
            result["open"] = result["close"]
        if "volume" not in result.columns:
            result["volume"] = 0.0
        if "amount" not in result.columns:
            result["amount"] = 0.0

        result["vwap"] = self._calc_vwap(result)
        return result

    def _calc_vwap(self, df: pd.DataFrame) -> pd.Series:
        """计算 VWAP"""
        volume = pd.to_numeric(df.get("volume"), errors="coerce").fillna(0.0)
        amount = pd.to_numeric(df.get("amount"), errors="coerce").fillna(0.0)
        close = pd.to_numeric(df.get("close"), errors="coerce").ffill()
        typical_price = (
            pd.to_numeric(df.get("high"), errors="coerce").fillna(close) +
            pd.to_numeric(df.get("low"), errors="coerce").fillna(close) +
            close
        ) / 3

        if amount.sum() > 0 and volume.sum() > 0:
            cum_amount = amount.cumsum()
            cum_volume = volume.replace(0, np.nan).cumsum()
            return (cum_amount / cum_volume).replace([np.inf, -np.inf], np.nan).fillna(close)

        proxy_amount = (typical_price * volume).cumsum()
        proxy_volume = volume.replace(0, np.nan).cumsum()
        return (proxy_amount / proxy_volume).replace([np.inf, -np.inf], np.nan).fillna(close)

    def _analyze_single_index(self, code: str, df: pd.DataFrame) -> IndexMinuteSnapshot:
        """分析单个指数分钟结构"""
        close = df["close"].astype(float)
        high = df["high"].astype(float)
        low = df["low"].astype(float)
        volume = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
        vwap = pd.to_numeric(df["vwap"], errors="coerce").ffill().fillna(close)

        latest_price = float(close.iloc[-1])
        return_1m = self._pct_change(close, 1)
        return_3m = self._pct_change(close, 3)
        return_5m = self._pct_change(close, 5)
        volume_ratio = self._latest_volume_ratio(volume)
        vwap_bias = ((latest_price / float(vwap.iloc[-1])) - 1.0) * 100 if float(vwap.iloc[-1]) > 0 else 0.0
        slope_score = self._calc_slope_score(close.tail(5))
        breakout_fail = self._calc_breakout_fail(close, high, vwap)
        breakdown_fail = self._calc_breakdown_fail(close, low, vwap)

        fast_up = 1.0 if (return_1m >= 0.35 or return_3m >= 0.60) else 0.0
        fast_down = 1.0 if (return_1m <= -0.35 or return_3m <= -0.60) else 0.0
        low_volume_rise = np.clip((1.05 - volume_ratio) / 0.6, 0.0, 1.0) if return_3m > 0 else 0.0
        low_volume_drop = np.clip((1.05 - volume_ratio) / 0.6, 0.0, 1.0) if return_3m < 0 else 0.0
        vwap_loss = np.clip((-vwap_bias) / 0.30, 0.0, 1.0) if return_3m > 0 else 0.0
        vwap_reclaim = np.clip(vwap_bias / 0.30, 0.0, 1.0) if return_3m < 0 else 0.0
        steep_up = np.clip(slope_score / 0.8, 0.0, 1.0) if return_3m > 0 else 0.0
        steep_down = np.clip((-slope_score) / 0.8, 0.0, 1.0) if return_3m < 0 else 0.0

        fake_up_score = float(np.clip(
            0.22 * fast_up +
            0.18 * low_volume_rise +
            0.20 * vwap_loss +
            0.25 * breakout_fail +
            0.15 * steep_up,
            0.0,
            1.0,
        ))
        fake_down_score = float(np.clip(
            0.22 * fast_down +
            0.18 * low_volume_drop +
            0.20 * vwap_reclaim +
            0.25 * breakdown_fail +
            0.15 * steep_down,
            0.0,
            1.0,
        ))

        comment = self._build_index_comment(return_3m, volume_ratio, vwap_bias, breakout_fail, breakdown_fail)
        return IndexMinuteSnapshot(
            code=code,
            name=INDEX_MAP.get(code, code),
            latest_price=latest_price,
            return_1m=return_1m,
            return_3m=return_3m,
            return_5m=return_5m,
            volume_ratio=volume_ratio,
            vwap_bias=vwap_bias,
            breakout_fail=breakout_fail,
            breakdown_fail=breakdown_fail,
            slope_score=slope_score,
            fake_up_score=fake_up_score,
            fake_down_score=fake_down_score,
            comment=comment,
        )

    def _combine_snapshots(self, snapshots: List[IndexMinuteSnapshot]) -> Tuple[float, float, str]:
        """跨指数合成总分"""
        if not snapshots:
            return 0.0, 0.0, "分钟数据不足"

        weights = {
            "000001": float(STRATEGY_CONFIG.get("intraday_trap_weight_sh", 0.30)),
            "000905": float(STRATEGY_CONFIG.get("intraday_trap_weight_csi500", 0.35)),
            "000852": float(STRATEGY_CONFIG.get("intraday_trap_weight_csi1000", 0.35)),
        }
        total_weight = sum(weights.get(item.code, 0.0) for item in snapshots) or 1.0

        fake_up = sum(item.fake_up_score * weights.get(item.code, 0.0) for item in snapshots) / total_weight
        fake_down = sum(item.fake_down_score * weights.get(item.code, 0.0) for item in snapshots) / total_weight

        up_count = sum(1 for item in snapshots if item.return_5m > 0)
        down_count = sum(1 for item in snapshots if item.return_5m < 0)
        weak_follow = sum(1 for item in snapshots if item.fake_up_score >= 0.55)
        strong_reclaim = sum(1 for item in snapshots if item.fake_down_score >= 0.55)

        breadth_parts = [
            f"上行{up_count}/{len(snapshots)}",
            f"下行{down_count}/{len(snapshots)}",
            f"疑似诱多{weak_follow}个",
            f"疑似诱空{strong_reclaim}个",
        ]
        return float(fake_up), float(fake_down), " | ".join(breadth_parts)

    def _classify(
        self,
        fake_up_score: float,
        fake_down_score: float,
        snapshots: List[IndexMinuteSnapshot],
        breadth_comment: str,
    ) -> Tuple[str, str]:
        """综合分类"""
        threshold = float(STRATEGY_CONFIG.get("intraday_trap_threshold", 0.65))
        spread = float(STRATEGY_CONFIG.get("intraday_trap_spread", 0.12))

        if len(snapshots) < len(INDEX_MAP):
            return "no_data", "分钟数据不足，跳过本次盘中诱多/诱空判断。"

        if fake_up_score >= threshold and fake_up_score >= fake_down_score + spread:
            trap_type = "fake_up"
            summary = "拉升偏快但扩散不足，倾向诱多；不宜追高，更适合等回踩确认。"
        elif fake_down_score >= threshold and fake_down_score >= fake_up_score + spread:
            trap_type = "fake_down"
            summary = "下杀偏急但承接尚可，倾向诱空；可观察 VWAP 站回后的低吸机会。"
        elif fake_up_score >= 0.55 and fake_down_score >= 0.55:
            trap_type = "chaotic"
            summary = "指数分化较大，拉砸都快，博弈混乱；更适合轻仓与等待二次确认。"
        else:
            positive = sum(1 for item in snapshots if item.return_5m > 0)
            negative = sum(1 for item in snapshots if item.return_5m < 0)
            if positive >= 2 and fake_up_score < 0.55:
                trap_type = "true_break"
                summary = "三指数多数同向走强，且诱多特征不明显，偏真突破。"
            elif negative >= 2 and fake_down_score < 0.55:
                trap_type = "true_drop"
                summary = "三指数多数同向走弱，且诱空特征不明显，偏真实走弱。"
            else:
                trap_type = "neutral"
                summary = f"暂无明确诱多/诱空优势，保持观察。{breadth_comment}"
        return trap_type, summary

    def _build_index_comment(
        self,
        return_3m: float,
        volume_ratio: float,
        vwap_bias: float,
        breakout_fail: float,
        breakdown_fail: float,
    ) -> str:
        """生成单指数说明"""
        parts: List[str] = []
        if return_3m > 0.6 and volume_ratio < 1.0:
            parts.append("急拉缺量")
        if return_3m < -0.6 and volume_ratio < 1.0:
            parts.append("急跌缩量")
        if vwap_bias < -0.2:
            parts.append("失守VWAP")
        elif vwap_bias > 0.2:
            parts.append("站上VWAP")
        if breakout_fail >= 0.6:
            parts.append("突破回落")
        if breakdown_fail >= 0.6:
            parts.append("跌破回收")
        return " / ".join(parts) if parts else "结构中性"

    def _pct_change(self, series: pd.Series, periods: int) -> float:
        """计算涨跌幅"""
        if series is None or len(series) <= periods:
            return 0.0
        base = float(series.iloc[-periods - 1] or 0.0)
        latest = float(series.iloc[-1] or 0.0)
        if base <= 0:
            return 0.0
        return (latest / base - 1.0) * 100

    def _latest_volume_ratio(self, volume: pd.Series) -> float:
        """最新分钟量比"""
        if volume is None or len(volume) < 6:
            return 1.0
        base = float(volume.iloc[-6:-1].replace(0, np.nan).mean() or 0.0)
        latest = float(volume.iloc[-1] or 0.0)
        if base <= 0:
            return 1.0
        return latest / base

    def _calc_slope_score(self, close: pd.Series) -> float:
        """计算分时斜率"""
        if close is None or len(close) < 3:
            return 0.0
        values = close.astype(float).values
        x = np.arange(len(values))
        slope = np.polyfit(x, values, 1)[0]
        base = float(np.mean(values)) if float(np.mean(values)) != 0 else 1.0
        return float((slope / base) * 1000)

    def _calc_breakout_fail(self, close: pd.Series, high: pd.Series, vwap: pd.Series) -> float:
        """计算假突破强度"""
        lookback = min(20, len(close) - 1)
        if lookback < 5:
            return 0.0
        prev_high = float(high.iloc[-lookback - 1:-1].max() or 0.0)
        last_high = float(high.iloc[-1] or 0.0)
        last_close = float(close.iloc[-1] or 0.0)
        last_vwap = float(vwap.iloc[-1] or 0.0)
        if prev_high <= 0:
            return 0.0
        breakout = last_high >= prev_high * 1.0005
        failed = last_close < prev_high and last_close < last_vwap
        if breakout and failed:
            return float(np.clip((prev_high - last_close) / prev_high * 200, 0.0, 1.0))
        return 0.0

    def _calc_breakdown_fail(self, close: pd.Series, low: pd.Series, vwap: pd.Series) -> float:
        """计算假跌破强度"""
        lookback = min(20, len(close) - 1)
        if lookback < 5:
            return 0.0
        prev_low = float(low.iloc[-lookback - 1:-1].min() or 0.0)
        last_low = float(low.iloc[-1] or 0.0)
        last_close = float(close.iloc[-1] or 0.0)
        last_vwap = float(vwap.iloc[-1] or 0.0)
        if prev_low <= 0:
            return 0.0
        breakdown = last_low <= prev_low * 0.9995
        recovered = last_close > prev_low and last_close > last_vwap
        if breakdown and recovered:
            return float(np.clip((last_close - prev_low) / prev_low * 200, 0.0, 1.0))
        return 0.0
