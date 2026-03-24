# -*- coding: utf-8 -*-
"""
策略实验批跑器
"""
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from backtest import BacktestEngine
from data import DataSource
from strategy import (
    BreakoutStrategy,
    MACDStrategy,
    PriceActionMACDStrategy,
    PriceActionStrategy,
    TACOOilStrategy,
    TACOStrategy,
    build_taco_params,
)
from strategy.selectors.weak_to_strong import WeakToStrongParams, WeakToStrongTimingStrategy


DEFAULT_SYMBOLS = [
    "600000", "600036", "600519", "601318", "600887",
    "000001", "000002", "000858", "000333", "000651",
    "300750", "300059", "300015", "002594", "002415",
    "601012", "601166", "600030", "600900", "600028",
]

PRIMARY_BENCHMARK_CODE = "000300"


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
    max_drawdown_limit: float = -0.18,
    top_k: int = 3,
) -> List[Dict[str, Any]]:
    """按超额收益优先、回撤约束、夏普兜底筛选候选最优配置。"""
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

        score = (
            excess_return * 100.0
            + total_return * 25.0
            + sharpe_ratio * 5.0
            + win_rate * 8.0
            - drawdown_penalty * 10.0
        )

        ranked_item = dict(item)
        ranked_item["primary_excess_return"] = excess_return
        ranked_item["ranking_score"] = float(score)
        ranked_item["drawdown_penalty"] = float(drawdown_penalty)
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
                f"{index}. {item['name']} | 沪深300超额 {item.get('primary_excess_return', 0.0):+.2%} | "
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
