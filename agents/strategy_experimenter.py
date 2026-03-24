# -*- coding: utf-8 -*-
"""
策略实验批跑器
"""
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from backtest import BacktestEngine, SelectorBacktestEngine
from data import DataSource
from strategy import (
    BreakoutStrategy,
    MACDStrategy,
    PriceActionMACDStrategy,
    PriceActionStrategy,
    TACOOilStrategy,
    TACOStrategy,
    TacoWeakStrongParams,
    TacoWeakStrongSelector,
    TacoWeakStrongTimingStrategy,
    WeakToStrongSelector,
    build_taco_params,
)
from strategy.selectors.weak_to_strong import WeakToStrongParams, WeakToStrongTimingStrategy


DEFAULT_SYMBOLS = [
    "600000", "600036", "600519", "601318", "600887",
    "000001", "000002", "000858", "000333", "000651",
    "300750", "300059", "300015", "002594", "002415",
    "601012", "601166", "600030", "600900", "600028",
]

PRIMARY_BENCHMARK_CODE = "399001"
PRIMARY_BENCHMARK_NAME = "深证成指"
TARGET_TOTAL_RETURN = 0.20
TARGET_WIN_RATE = 0.55
TARGET_MAX_DRAWDOWN = -0.08


def build_strategy_for_experiment(strategy_name: str, overrides: Optional[Dict[str, Any]] = None):
    """构建实验用策略实例。"""
    params = dict(overrides or {})
    if strategy_name == "macd":
        return MACDStrategy(
            fast=int(params.get("fast", 12)),
            slow=int(params.get("slow", 26)),
            signal=int(params.get("signal", 9)),
            use_divergence=bool(params.get("use_divergence", True)),
        )
    if strategy_name == "pa":
        return PriceActionStrategy(
            lookback=int(params.get("lookback", 20)),
            atr_period=int(params.get("atr_period", 14)),
            atr_multiplier=float(params.get("atr_multiplier", 2.0)),
            min_body_ratio=float(params.get("min_body_ratio", 0.5)),
        )
    if strategy_name == "breakout":
        return BreakoutStrategy(
            lookback=int(params.get("lookback", 20)),
            volume_ratio=float(params.get("volume_ratio", 1.5)),
        )
    if strategy_name == "weak_strong":
        ws_params = WeakToStrongParams(
            limit_up_window=int(params.get("limit_up_window", 15)),
            shrink_ratio=float(params.get("shrink_ratio", 0.5)),
            pullback_days=int(params.get("pullback_days", 7)),
            volume_multiple=float(params.get("volume_multiple", 2.0)),
            min_rally_pct=float(params.get("min_rally_pct", 2.0)),
            total_window=int(params.get("total_window", 30)),
        )
        return WeakToStrongTimingStrategy(params=ws_params)
    if strategy_name == "taco":
        return TACOStrategy(build_taco_params("taco", params))
    if strategy_name == "taco_oil":
        return TACOOilStrategy(build_taco_params("taco_oil", params))
    return PriceActionMACDStrategy(
        lookback=int(params.get("lookback", 20)),
        macd_fast=int(params.get("macd_fast", 12)),
        macd_slow=int(params.get("macd_slow", 26)),
        macd_signal=int(params.get("macd_signal", 9)),
        require_confirmation=bool(params.get("require_confirmation", True)),
    )


def build_selector_pair_for_experiment(strategy_name: str, overrides: Optional[Dict[str, Any]] = None):
    """构建选股+择时实验策略。"""
    params = dict(overrides or {})
    if strategy_name == "weak_strong":
        weak_params = WeakToStrongParams(
            limit_up_window=int(params.get("limit_up_window", 20)),
            shrink_ratio=float(params.get("shrink_ratio", 0.75)),
            pullback_days=int(params.get("pullback_days", 10)),
            volume_multiple=float(params.get("volume_multiple", 1.35)),
            min_rally_pct=float(params.get("min_rally_pct", 1.2)),
            total_window=int(params.get("total_window", 40)),
            breakdown_drop_pct=float(params.get("breakdown_drop_pct", -5.5)),
            breakdown_volume_multiple=float(params.get("breakdown_volume_multiple", 2.2)),
            breakdown_lookback=int(params.get("breakdown_lookback", 3)),
            max_pullback_pct=float(params.get("max_pullback_pct", 18.0)),
            require_confirm_open_above_prev_close=bool(params.get("require_confirm_open_above_prev_close", False)),
            max_confirm_gap_pct=float(params.get("max_confirm_gap_pct", 9.0)),
            min_close_position_ratio=float(params.get("min_close_position_ratio", 0.45)),
            prior_weak_upper_shadow_ratio=float(params.get("prior_weak_upper_shadow_ratio", 0.55)),
            prior_weak_volume_multiple=float(params.get("prior_weak_volume_multiple", 1.6)),
        )
        return WeakToStrongSelector(weak_params), WeakToStrongTimingStrategy(params=weak_params)
    if strategy_name == "taco_weak_strong":
        weak_params = WeakToStrongParams(
            limit_up_window=int(params.get("limit_up_window", 20)),
            shrink_ratio=float(params.get("shrink_ratio", 0.75)),
            pullback_days=int(params.get("pullback_days", 10)),
            volume_multiple=float(params.get("volume_multiple", 1.35)),
            min_rally_pct=float(params.get("min_rally_pct", 1.2)),
            total_window=int(params.get("total_window", 40)),
            breakdown_drop_pct=float(params.get("breakdown_drop_pct", -5.5)),
            breakdown_volume_multiple=float(params.get("breakdown_volume_multiple", 2.2)),
            breakdown_lookback=int(params.get("breakdown_lookback", 3)),
            max_pullback_pct=float(params.get("max_pullback_pct", 18.0)),
            require_confirm_open_above_prev_close=bool(params.get("require_confirm_open_above_prev_close", False)),
            max_confirm_gap_pct=float(params.get("max_confirm_gap_pct", 9.0)),
            min_close_position_ratio=float(params.get("min_close_position_ratio", 0.45)),
            prior_weak_upper_shadow_ratio=float(params.get("prior_weak_upper_shadow_ratio", 0.55)),
            prior_weak_volume_multiple=float(params.get("prior_weak_volume_multiple", 1.6)),
        )
        combo_params = TacoWeakStrongParams(
            weak_params=weak_params,
            taco_variant=str(params.get("taco_variant", "taco")),
            taco_candidate_threshold=float(params.get("taco_candidate_threshold", 0.16)),
            taco_buy_score_threshold=float(params.get("taco_buy_score_threshold", 0.36)),
            combined_score_threshold=float(params.get("combined_score_threshold", 0.44)),
            selector_stage_floor=int(params.get("selector_stage_floor", 3)),
            selector_score_floor=float(params.get("selector_score_floor", 26.0)),
            stage4_buy_score_floor=float(params.get("stage4_buy_score_floor", 0.50)),
            stage3_buy_score_floor=float(params.get("stage3_buy_score_floor", 0.56)),
        )
        return TacoWeakStrongSelector(combo_params), TacoWeakStrongTimingStrategy(params=combo_params)
    return None


def run_strategy_experiments(
    strategy_name: str,
    experiments: List[Dict[str, Any]],
    start_date: str = "20250101",
    end_date: str = "20260318",
    symbols: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """批量运行调优实验。"""
    actual_symbols = list(symbols or DEFAULT_SYMBOLS)
    data_source = DataSource()
    rows: List[Dict[str, Any]] = []

    for item in experiments:
        name = str(item.get("name", "experiment"))
        overrides = dict(item.get("overrides", {}) or {})
        risk_overrides = dict(item.get("risk_overrides", {}) or {})
        candidate_gate_threshold = item.get("candidate_gate_threshold")
        selector_pair = build_selector_pair_for_experiment(strategy_name, overrides)
        if selector_pair is not None:
            selector, timing_strategy = selector_pair
            engine = SelectorBacktestEngine(
                selector=selector,
                timing_strategy=timing_strategy,
                initial_capital=1000000,
                risk_overrides=risk_overrides,
                candidate_gate_threshold=float(candidate_gate_threshold) if candidate_gate_threshold is not None else None,
            )
            result = engine.run(
                pool_symbols=actual_symbols,
                start_date=start_date,
                end_date=end_date,
                data_source=data_source,
                select_top_n=min(8, len(actual_symbols)),
                rebalance_freq=15,
            )
        else:
            strategy = build_strategy_for_experiment(strategy_name, overrides)
            engine = BacktestEngine(
                strategy=strategy,
                initial_capital=1000000,
                risk_overrides=risk_overrides,
                candidate_gate_threshold=float(candidate_gate_threshold) if candidate_gate_threshold is not None else None,
            )
            result = engine.run(
                symbols=actual_symbols,
                start_date=start_date,
                end_date=end_date,
                data_source=data_source,
            )
        row = {
            "name": name,
            "goal": str(item.get("goal", "") or ""),
            "overrides": overrides,
            "risk_overrides": risk_overrides,
            "candidate_gate_threshold": (
                float(candidate_gate_threshold) if candidate_gate_threshold is not None else None
            ),
            "total_return": float(result.total_return),
            "annual_return": float(result.annual_return),
            "sharpe_ratio": float(result.sharpe_ratio),
            "max_drawdown": float(result.max_drawdown),
            "win_rate": float(result.win_rate),
            "trade_count": int(len(result.trades)),
            "signal_summary": dict(result.signal_summary or {}),
            "benchmark_metrics": dict(result.benchmark_metrics or {}),
            "phase_metrics": list(result.phase_metrics or []),
        }
        rows.append(row)
    rows.sort(key=lambda item: (item["total_return"], item["sharpe_ratio"]), reverse=True)
    return rows


def rank_experiment_candidates(
    experiment_rows: List[Dict[str, Any]],
    max_drawdown_limit: float = TARGET_MAX_DRAWDOWN,
    top_k: int = 3,
) -> List[Dict[str, Any]]:
    """按高胜率、低回撤、20%收益目标优先筛选候选最优配置。"""
    ranked_rows: List[Dict[str, Any]] = []
    for item in experiment_rows:
        benchmark_metrics = dict(item.get("benchmark_metrics", {}) or {})
        primary = dict(benchmark_metrics.get(PRIMARY_BENCHMARK_CODE, {}) or {})
        excess_return = float(primary.get("excess_return", 0.0) or 0.0)
        total_return = float(item.get("total_return", 0.0) or 0.0)
        sharpe_ratio = float(item.get("sharpe_ratio", 0.0) or 0.0)
        max_drawdown = float(item.get("max_drawdown", 0.0) or 0.0)
        win_rate = float(item.get("win_rate", 0.0) or 0.0)

        drawdown_penalty = 0.0
        if max_drawdown < max_drawdown_limit:
            drawdown_penalty = abs(max_drawdown - max_drawdown_limit) * 2.5

        return_gap_penalty = max(TARGET_TOTAL_RETURN - total_return, 0.0)
        win_rate_penalty = max(TARGET_WIN_RATE - win_rate, 0.0)

        score = (
            excess_return * 85.0
            + total_return * 32.0
            + sharpe_ratio * 6.0
            + win_rate * 30.0
            - drawdown_penalty * 10.0
            - return_gap_penalty * 45.0
            - win_rate_penalty * 35.0
        )

        ranked_item = dict(item)
        ranked_item["primary_excess_return"] = excess_return
        ranked_item["primary_benchmark_code"] = PRIMARY_BENCHMARK_CODE
        ranked_item["primary_benchmark_name"] = PRIMARY_BENCHMARK_NAME
        ranked_item["ranking_score"] = float(score)
        ranked_item["drawdown_penalty"] = float(drawdown_penalty)
        ranked_item["return_gap_penalty"] = float(return_gap_penalty)
        ranked_item["win_rate_penalty"] = float(win_rate_penalty)
        ranked_rows.append(ranked_item)

    ranked_rows.sort(
        key=lambda item: (
            item["ranking_score"],
            item["primary_excess_return"],
            item["sharpe_ratio"],
        ),
        reverse=True,
    )
    return ranked_rows[: max(1, int(top_k))]


def save_experiment_report(
    strategy_name: str,
    review: Dict[str, Any],
    experiment_rows: List[Dict[str, Any]],
    recommended_rows: Optional[List[Dict[str, Any]]] = None,
    output_dir: Optional[str] = None,
) -> Dict[str, str]:
    """保存实验对比报告。"""
    base_dir = Path(output_dir or "./runtime/reports/tuning")
    base_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = base_dir / f"experiment_report_{strategy_name}_{ts}.json"
    md_path = base_dir / f"experiment_report_{strategy_name}_{ts}.md"

    payload = {
        "strategy_name": strategy_name,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "primary_benchmark_code": PRIMARY_BENCHMARK_CODE,
        "primary_benchmark_name": PRIMARY_BENCHMARK_NAME,
        "review": review,
        "experiments": experiment_rows,
        "recommended": list(recommended_rows or []),
    }
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    lines = [
        f"# 策略实验报告 - {strategy_name}",
        "",
        f"- 生成时间: {payload['generated_at']}",
        "",
        "## 调优摘要",
        "",
        str(review.get("summary", "") or ""),
        "",
        "## 实验结果",
        "",
    ]
    if recommended_rows:
        lines.extend(["## 推荐候选", ""])
        for index, item in enumerate(recommended_rows, 1):
            lines.append(
                f"{index}. {item['name']} | {PRIMARY_BENCHMARK_NAME}超额 {item.get('primary_excess_return', 0.0):+.2%} | "
                f"收益 {item['total_return']:.2%} | 夏普 {item['sharpe_ratio']:.2f} | "
                f"回撤 {item['max_drawdown']:.2%} | 排名分 {item.get('ranking_score', 0.0):.2f}"
            )
        lines.append("")
    for index, item in enumerate(experiment_rows, 1):
        lines.append(
            f"{index}. {item['name']} | 收益 {item['total_return']:.2%} | "
            f"夏普 {item['sharpe_ratio']:.2f} | 回撤 {item['max_drawdown']:.2%} | 交易 {item['trade_count']}"
        )
        if item.get("goal"):
            lines.append(f"   goal={item['goal']}")
        if item.get("overrides"):
            lines.append(f"   overrides={json.dumps(item['overrides'], ensure_ascii=False)}")
        if item.get("risk_overrides"):
            lines.append(f"   risk_overrides={json.dumps(item['risk_overrides'], ensure_ascii=False)}")
        if item.get("candidate_gate_threshold") is not None:
            lines.append(f"   candidate_gate_threshold={item['candidate_gate_threshold']}")

    with md_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines).strip() + "\n")

    return {
        "json_path": str(json_path),
        "markdown_path": str(md_path),
    }


def save_best_config_snapshot(
    strategy_name: str,
    recommended_rows: List[Dict[str, Any]],
    output_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """保存推荐候选配置快照。"""
    base_dir = Path(output_dir or "./runtime/reports/tuning")
    base_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = base_dir / f"best_config_{strategy_name}_{ts}.json"

    top_rows = [dict(item) for item in list(recommended_rows or [])[:3]]
    payload = {
        "strategy_name": strategy_name,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "best_candidate": top_rows[0] if top_rows else {},
        "recommended": top_rows,
    }

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return {
        "json_path": str(json_path),
        "generated_at": str(payload["generated_at"]),
        "best_name": str((payload["best_candidate"] or {}).get("name", "") or ""),
        "recommended_count": len(top_rows),
    }
