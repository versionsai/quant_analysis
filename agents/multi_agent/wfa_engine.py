# -*- coding: utf-8 -*-
"""
Walk-Forward分析引擎
防止参数过拟合
"""
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from data.recommend_db import get_db, SignalQualityDB, WFADB, DynamicParamsDB
from utils.logger import get_logger

logger = get_logger(__name__)


class WFAEngine:
    """Walk-Forward分析引擎"""

    def __init__(self, db_path: str = None):
        self.sq_db = SignalQualityDB(db_path)
        self.wfa_db = WFADB(db_path)
        self.dp_db = DynamicParamsDB(db_path)

    def run_wfa_analysis(
        self,
        train_window_days: int = 180,
        test_window_days: int = 30,
        lookback_days: int = 365,
    ) -> Dict:
        """
        执行Walk-Forward分析

        Args:
            train_window_days: 训练窗口天数
            test_window_days: 测试窗口天数
            lookback_days: 回看天数

        Returns:
            dict: WFA分析结果
        """
        db = get_db()
        conn = db._get_conn()
        cursor = conn.cursor()

        end_date = datetime.now()
        start_date = end_date - timedelta(days=lookback_days)

        results = []
        current_date = start_date

        while current_date + timedelta(days=test_window_days) <= end_date:
            train_end = current_date + timedelta(days=train_window_days)
            test_end = train_end + timedelta(days=test_window_days)

            cursor.execute("""
                SELECT signal_source, COUNT(*) as total,
                       SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) as wins,
                       AVG(pnl_pct) as avg_pnl
                FROM signal_quality
                WHERE entry_date >= ? AND entry_date < ?
                GROUP BY signal_source
            """, (
                current_date.strftime("%Y-%m-%d"),
                train_end.strftime("%Y-%m-%d")
            ))

            train_perf = {}
            for row in cursor.fetchall():
                source = row["signal_source"]
                total = row["total"] or 0
                wins = row["wins"] or 0
                train_perf[source] = {
                    "total": total,
                    "win_rate": wins / total * 100 if total > 0 else 0,
                    "avg_pnl": row["avg_pnl"] or 0,
                }

            cursor.execute("""
                SELECT signal_source, COUNT(*) as total,
                       SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) as wins,
                       AVG(pnl_pct) as avg_pnl
                FROM signal_quality
                WHERE entry_date >= ? AND entry_date < ?
                GROUP BY signal_source
            """, (
                train_end.strftime("%Y-%m-%d"),
                test_end.strftime("%Y-%m-%d")
            ))

            test_perf = {}
            for row in cursor.fetchall():
                source = row["signal_source"]
                total = row["total"] or 0
                wins = row["wins"] or 0
                test_perf[source] = {
                    "total": total,
                    "win_rate": wins / total * 100 if total > 0 else 0,
                    "avg_pnl": row["avg_pnl"] or 0,
                }

            if train_perf and test_perf:
                train_return = sum(p.get("avg_pnl", 0) * p.get("total", 0) for p in train_perf.values()) / max(sum(p.get("total", 0) for p in train_perf.values()), 1)
                test_return = sum(p.get("avg_pnl", 0) * p.get("total", 0) for p in test_perf.values()) / max(sum(p.get("total", 0) for p in test_perf.values()), 1)

                stability = self._calc_stability(train_perf, test_perf)

                self.wfa_db.add_result(
                    window_start=current_date.strftime("%Y-%m-%d"),
                    window_end=test_end.strftime("%Y-%m-%d"),
                    train_return=train_return,
                    test_return=test_return,
                    params_used=self.dp_db.get_all_params(),
                    stability_score=stability,
                )

                results.append({
                    "window_start": current_date.strftime("%Y-%m-%d"),
                    "window_end": test_end.strftime("%Y-%m-%d"),
                    "train_return": train_return,
                    "test_return": test_return,
                    "stability": stability,
                })

            current_date += timedelta(days=test_window_days)

        conn.close()

        return {
            "windows": results,
            "avg_stability": sum(r.get("stability", 0) for r in results) / max(len(results), 1),
            "avg_test_return": sum(r.get("test_return", 0) for r in results) / max(len(results), 1),
        }

    def _calc_stability(self, train_perf: Dict, test_perf: Dict) -> float:
        """
        计算稳定性分数

        稳定性 = 1 - |train_win_rate - test_win_rate| / 100
        """
        if not train_perf or not test_perf:
            return 0.5

        train_win_rates = [p.get("win_rate", 0) for p in train_perf.values()]
        test_win_rates = [p.get("win_rate", 0) for p in test_perf.values()]

        if not train_win_rates or not test_win_rates:
            return 0.5

        train_avg = sum(train_win_rates) / len(train_win_rates)
        test_avg = sum(test_win_rates) / len(test_win_rates)

        diff = abs(train_avg - test_avg) / 100
        stability = max(0, 1 - diff)

        return stability

    def is_param_change_safe(self, param_key: str, new_value: float) -> bool:
        """
        检查参数变更是否安全（防过拟合）

        Args:
            param_key: 参数名
            new_value: 新值

        Returns:
            bool: 是否安全
        """
        old_value = self.dp_db.get_param(param_key)
        if old_value is None:
            return True

        change_pct = abs(new_value - old_value) / abs(old_value) if old_value != 0 else 1

        if change_pct > 0.05:
            logger.warning(f"参数 {param_key} 变化超过5%: {old_value} -> {new_value}")
            return False

        latest_stability = self.wfa_db.get_latest_stability_score()
        if latest_stability is not None and latest_stability < 0.6:
            logger.warning(f"稳定性分数过低 ({latest_stability:.2f})，不建议调整参数")
            return False

        return True

    def get_stable_param_region(self, param_key: str, candidates: List[float]) -> Optional[float]:
        """
        获取稳定参数区域（ plateau）

        Args:
            param_key: 参数名
            candidates: 候选值列表

        Returns:
            float: 稳定区域内的最佳值
        """
        wfa_results = self.wfa_db.get_results(limit=10)
        if not wfa_results:
            return candidates[len(candidates) // 2] if candidates else None

        best_stability = max(r.get("stability_score", 0) for r in wfa_results)
        threshold = best_stability * 0.9

        stable_results = [r for r in wfa_results if r.get("stability_score", 0) >= threshold]

        if not stable_results:
            return candidates[len(candidates) // 2] if candidates else None

        return candidates[len(candidates) // 2] if candidates else None
