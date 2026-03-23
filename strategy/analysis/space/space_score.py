# -*- coding: utf-8 -*-
"""
连板空间评分（Space_Score）模型

将“盘感 + 题材 + 氛围”结构化为可计算、可回测、可推送的评分:

- 盘面强度（TapeStrength）: 封板成功率、炸板率(反向)、承接强度
- 题材强度（ThemeStrength）: 概念板块的涨停数、连板数、龙头高度（概念优先）
- 市场情绪（MarketEmotion）: 涨停/跌停/连板/流动性等综合情绪分
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd

from strategy.analysis.base_analyzer import BaseAnalyzer, ScoreResult
from strategy.analysis.emotion.market_emotion import MarketEmotionAnalyzer
from utils.logger import get_logger

logger = get_logger(__name__)

_PATCH_STATE = {"tried": False, "ok": False}


class SpaceLevel(str, Enum):
    """空间等级"""

    LOW = "≤2板"
    MID = "3~4板"
    HIGH = "5~7板"
    EXTREME = "≥8板(高风险)"


@dataclass
class SpaceScore:
    """空间评分结果（单日）"""

    date: str
    tape_strength: float  # 0~1
    theme_strength: float  # 0~1
    market_emotion: float  # 0~1
    space_score: float  # 0~1
    level: SpaceLevel

    def to_dict(self) -> dict:
        return {
            "date": self.date,
            "tape_strength": self.tape_strength,
            "theme_strength": self.theme_strength,
            "market_emotion": self.market_emotion,
            "space_score": self.space_score,
            "level": self.level.value,
        }


class SpaceScoreAnalyzer(BaseAnalyzer):
    """连板空间评分分析器（概念板块版本）"""

    def __init__(self):
        super().__init__("SpaceScore")
        self._cache_ttl = 600
        self._market_analyzer = MarketEmotionAnalyzer()
        # date -> symbol -> (concept_score, concept_name)
        self._concept_member_score: Dict[str, Dict[str, Tuple[float, str]]] = {}

    def analyze(self, **kwargs) -> ScoreResult:
        date = kwargs.get("date")
        top_concepts = int(kwargs.get("top_concepts", 30))
        return self.analyze_space(date=date, top_concepts=top_concepts)

    def analyze_space(self, date: Optional[str] = None, top_concepts: int = 30) -> ScoreResult:
        """
        计算单日 Space_Score（概念板块优先）。

        Args:
            date: 交易日 YYYYMMDD；为空默认上一自然日（与情绪分析保持一致）
            top_concepts: 仅对排名靠前的概念板块做映射，降低成本
        """
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

        cache_key = f"space_score_{date}_{top_concepts}"
        cached = self._get_cache(cache_key)
        if cached:
            return cached

        result = ScoreResult()
        try:
            tape_strength, tape_raw = self._calc_tape_strength(date)
            theme_strength, theme_raw = self._calc_theme_strength_by_concept(date, top_concepts=top_concepts)
            market_emotion = self._calc_market_emotion(date)

            space_score = 0.35 * tape_strength + 0.35 * theme_strength + 0.30 * market_emotion
            level = self._to_level(space_score)

            score_obj = SpaceScore(
                date=date,
                tape_strength=float(tape_strength),
                theme_strength=float(theme_strength),
                market_emotion=float(market_emotion),
                space_score=float(space_score),
                level=level,
            )

            result.score = float(space_score * 100)
            result.raw_data = {
                "space": score_obj.to_dict(),
                "tape_raw": tape_raw,
                "theme_raw": theme_raw,
            }
            result.signals = [f"空间:{level.value} | Space={space_score:.2f}"]
            result.success = True
        except Exception as e:
            result.success = False
            result.error_msg = str(e)
            logger.error(f"SpaceScore 计算失败: {e}")

        self._set_cache(cache_key, result)
        return result

    def get_symbol_concept_strength(
        self,
        symbol: str,
        date: Optional[str] = None,
        top_concepts: int = 30,
    ) -> Tuple[float, str]:
        """
        获取个股“概念板块强度”：
        - 返回该股所属概念中，位于 top_concepts 内的最高概念得分
        - 仅用于辅助（择时/风控/抱团识别），避免全量计算
        """
        if not symbol:
            return 0.0, ""
        symbol = str(symbol).strip()
        if date is None:
            date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

        cache = self._concept_member_score.get(date)
        if cache is None:
            _ = self.analyze_space(date=date, top_concepts=top_concepts)
            cache = self._concept_member_score.get(date, {})

        item = cache.get(symbol)
        if not item:
            return 0.0, ""
        return float(item[0]), str(item[1])

    def _to_level(self, space_score: float) -> SpaceLevel:
        if space_score < 0.30:
            return SpaceLevel.LOW
        if space_score < 0.60:
            return SpaceLevel.MID
        if space_score < 0.80:
            return SpaceLevel.HIGH
        return SpaceLevel.EXTREME

    def _calc_market_emotion(self, date: str) -> float:
        market = self._market_analyzer.get_market_emotion(date)
        if market is None:
            return 0.5
        return float(np.clip(float(market.normalized_score) / 100.0, 0.0, 1.0))

    def _install_patch(self):
        # 代理补丁可能在部分运行环境（如沙盒/无外网）触发大量授权请求日志；
        # 这里做进程内幂等与失败短路，避免重复尝试造成噪声。
        if _PATCH_STATE["tried"]:
            return
        _PATCH_STATE["tried"] = True
        try:
            import os

            token = str(os.environ.get("AKSHARE_PROXY_TOKEN", "")).strip()
            if not token:
                _PATCH_STATE["ok"] = False
                return

            import akshare_proxy_patch

            akshare_proxy_patch.install_patch(
                "101.201.173.125",
                auth_token=token,
                retry=2,
                hook_domains=[
                    "fund.eastmoney.com",
                    "push2.eastmoney.com",
                    "push2ex.eastmoney.com",
                ],
            )
            _PATCH_STATE["ok"] = True
        except Exception:
            _PATCH_STATE["ok"] = False

    def _calc_tape_strength(self, date: str) -> Tuple[float, dict]:
        """
        盘面强度 = 0.4*封板成功率 + 0.3*(1-炸板率) + 0.3*承接强度
        """
        try:
            from data.data_source import DataSource

            data_source = DataSource()
            try:
                zt_df, _, zb_df = data_source.get_limit_pool()
            finally:
                data_source.close()

            zt_count = int(len(zt_df)) if zt_df is not None else 0
            zb_count = int(len(zb_df)) if zb_df is not None else 0
            attempt = zt_count + zb_count
            seal_success = (zt_count / attempt) if attempt > 0 else 0.0
            break_rate = (zb_count / attempt) if attempt > 0 else 0.0

            # 承接强度：对涨停+炸板票的 spot 计算 (最新-最低)/(最高-最低)
            accept_strength = 0.5
            try:
                codes: Set[str] = set()
                if zt_df is not None and not zt_df.empty and "代码" in zt_df.columns:
                    codes |= set(zt_df["代码"].astype(str).tolist())
                if zb_df is not None and not zb_df.empty and "代码" in zb_df.columns:
                    codes |= set(zb_df["代码"].astype(str).tolist())

                if codes:
                    data_source = DataSource()
                    try:
                        spot_df = data_source.get_market_snapshots(sorted(list(codes)))
                    finally:
                        data_source.close()

                    if spot_df is not None and (not spot_df.empty):
                        h = pd.to_numeric(spot_df.get("high_price"), errors="coerce")
                        l = pd.to_numeric(spot_df.get("low_price"), errors="coerce")
                        c = pd.to_numeric(spot_df.get("last_price"), errors="coerce")
                        rng = (h - l).replace(0, np.nan)
                        s = ((c - l) / rng).replace([np.inf, -np.inf], np.nan).dropna()
                        if len(s) > 0:
                            accept_strength = float(np.clip(float(s.mean()), 0.0, 1.0))
            except Exception:
                pass

            tape = 0.4 * seal_success + 0.3 * (1.0 - break_rate) + 0.3 * accept_strength
            tape = float(np.clip(tape, 0.0, 1.0))
            raw = {
                "zt_count": zt_count,
                "zb_count": zb_count,
                "attempt": attempt,
                "seal_success": float(seal_success),
                "break_rate": float(break_rate),
                "accept_strength": float(accept_strength),
            }
            return tape, raw
        except Exception as e:
            logger.warning(f"盘面强度计算失败({date}): {e}")
            return 0.5, {"error": str(e)}

    def _pick_sort_column(self, df: pd.DataFrame) -> Optional[str]:
        if df is None or df.empty:
            return None
        candidates = [
            "涨跌幅",
            "今日涨跌幅",
            "主力净流入-净额",
            "今日主力净流入-净额",
            "成交额",
        ]
        for c in candidates:
            if c in df.columns:
                return c
        # 兜底：选第一个数值列
        for c in df.columns:
            try:
                if pd.api.types.is_numeric_dtype(df[c]):
                    return c
            except Exception:
                continue
        return None

    def _calc_theme_strength_by_concept(self, date: str, top_concepts: int) -> Tuple[float, dict]:
        """
        题材强度（概念板块）:
          0.5 * 概念涨停数 +
          0.3 * 概念连板数 +
          0.2 * 概念龙头高度

        为避免全量概念映射耗时，仅对 top_concepts 个热门概念做成分映射。
        """
        try:
            from data.data_source import DataSource

            data_source = DataSource()
            try:
                zt_df, _, _ = data_source.get_limit_pool()
            finally:
                data_source.close()

            if zt_df is None or zt_df.empty:
                return 0.0, {"reason": "no_zt_pool"}

            zt_codes = set(zt_df["代码"].astype(str).tolist()) if "代码" in zt_df.columns else set()
            if not zt_codes:
                return 0.0, {"reason": "empty_zt_codes"}

            zt_lb = {}
            if "连板数" in zt_df.columns:
                for _, row in zt_df.iterrows():
                    code = str(row.get("代码", "")).strip()
                    if not code:
                        continue
                    try:
                        zt_lb[code] = int(row.get("连板数", 0) or 0)
                    except Exception:
                        zt_lb[code] = 0

            concept_stats = []
            concept_members: Dict[str, Set[str]] = {}
            concept_hits: Dict[str, Dict[str, object]] = {}
            data_source = DataSource()
            try:
                for code in zt_codes:
                    plates = data_source.get_owner_plates(code)
                    if plates is None or plates.empty:
                        continue
                    for _, row in plates.iterrows():
                        plate_type = str(row.get("plate_type", "") or "").strip().upper()
                        if plate_type != "CONCEPT":
                            continue
                        concept_name = str(row.get("plate_name", "") or "").strip()
                        plate_code = str(row.get("plate_code", "") or "").strip()
                        if not concept_name:
                            continue
                        item = concept_hits.setdefault(
                            concept_name,
                            {"code": plate_code, "members": set(), "zt_count": 0, "lb_count": 0, "lb_max": 0},
                        )
                        item["members"].add(code)
                        item["zt_count"] = int(item["zt_count"]) + 1
                        if zt_lb.get(code, 0) > 1:
                            item["lb_count"] = int(item["lb_count"]) + 1
                        item["lb_max"] = max(int(item["lb_max"]), int(zt_lb.get(code, 0)))
            finally:
                data_source.close()

            if concept_hits:
                ranked = sorted(
                    concept_hits.items(),
                    key=lambda item: (-int(item[1]["zt_count"]), -int(item[1]["lb_count"]), -int(item[1]["lb_max"])),
                )[:top_concepts]
                for concept_name, payload in ranked:
                    members = set(str(code).strip() for code in payload.get("members", set()) if str(code).strip())
                    concept_members[concept_name] = members
                    concept_stats.append({
                        "concept": concept_name,
                        "code": str(payload.get("code", "") or ""),
                        "zt_count": int(payload.get("zt_count", 0) or 0),
                        "lb_count": int(payload.get("lb_count", 0) or 0),
                        "lb_max": int(payload.get("lb_max", 0) or 0),
                    })

            if not concept_stats:
                return 0.5, {"reason": "no_concept_stats"}

            max_zt = max(s["zt_count"] for s in concept_stats) or 1
            max_lb = max(s["lb_count"] for s in concept_stats) or 1
            max_h = max(s["lb_max"] for s in concept_stats) or 1

            for s in concept_stats:
                s["score"] = (
                    0.5 * (s["zt_count"] / max_zt) +
                    0.3 * (s["lb_count"] / max_lb) +
                    0.2 * (s["lb_max"] / max_h)
                )

            # 写入“概念->个股”强度映射缓存，供个股情绪/抱团识别使用
            try:
                member_score: Dict[str, Tuple[float, str]] = {}
                for s in concept_stats:
                    cname = str(s.get("concept", "")).strip()
                    cscore = float(s.get("score", 0.0))
                    members = concept_members.get(cname, set())
                    for code in members:
                        code = str(code).strip()
                        if not code:
                            continue
                        prev = member_score.get(code)
                        if prev is None or cscore > float(prev[0]):
                            member_score[code] = (cscore, cname)
                self._concept_member_score[date] = member_score
            except Exception:
                pass

            # 题材强度用“最强概念”代表当日主线题材强度
            concept_stats.sort(key=lambda x: -x["score"])
            theme = float(np.clip(float(concept_stats[0]["score"]), 0.0, 1.0))
            raw = {
                "top_concepts": concept_stats[:10],
                "sort_col": "futu_owner_plate_limit_hits",
                "concept_member_count": int(len(self._concept_member_score.get(date, {}))),
            }
            return theme, raw
        except Exception as e:
            logger.warning(f"题材强度(概念)计算失败({date}): {e}")
            return 0.5, {"error": str(e)}
