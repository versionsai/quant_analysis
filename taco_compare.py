# -*- coding: utf-8 -*-
"""
TACO 批量比较工具
"""
import argparse
import json
from typing import Dict, List, Optional

from backtest import BacktestEngine
from data import DataSource
from strategy import TACOOilStrategy, TACOStrategy, build_taco_params


DEFAULT_SYMBOLS = [
    "600000", "600036", "600519", "601318", "600887",
    "000001", "000002", "000858", "000333", "000651",
    "300750", "300059", "300015", "002594", "002415",
    "601012", "601166", "600030", "600900", "600028",
]


def _parse_keyword_overrides(items: Optional[List[str]]) -> Dict[str, float]:
    """
    解析关键词权重覆盖
    """
    overrides: Dict[str, float] = {}
    for item in items or []:
        text = str(item or "").strip()
        if not text or "=" not in text:
            continue
        key, value = text.split("=", 1)
        try:
            overrides[key.strip()] = float(value.strip())
        except Exception:
            continue
    return overrides


def _merge_keywords(base_keywords: Dict[str, float], override_keywords: Dict[str, float]) -> Dict[str, float]:
    """
    合并关键词权重
    """
    merged = dict(base_keywords or {})
    for key, value in (override_keywords or {}).items():
        merged[str(key)] = float(value)
    return merged


def _build_compare_cases(
    variants: List[str],
    thresholds: Optional[List[float]] = None,
    keyword_overrides: Optional[Dict[str, float]] = None,
) -> List[Dict[str, object]]:
    """
    构建比较场景
    """
    actual_thresholds = thresholds or []
    overrides = keyword_overrides or {}
    cases: List[Dict[str, object]] = []

    for variant in variants:
        cases.append({"name": f"{variant}_base", "variant": variant, "overrides": {}})
        for threshold in actual_thresholds:
            cases.append(
                {
                    "name": f"{variant}_threshold_{str(threshold).replace('.', '_')}",
                    "variant": variant,
                    "overrides": {"event_score_threshold": float(threshold)},
                }
            )
        if overrides:
            cases.append(
                {
                    "name": f"{variant}_keyword_override",
                    "variant": variant,
                    "overrides": {"keywords": dict(overrides)},
                }
            )
    return cases


def _build_strategy(variant: str, overrides: Dict[str, object]):
    """
    构建待比较策略
    """
    actual_overrides = dict(overrides or {})
    base_params = build_taco_params(variant)
    if "keywords" in actual_overrides and isinstance(actual_overrides["keywords"], dict):
        actual_overrides["keywords"] = _merge_keywords(base_params.keywords, actual_overrides["keywords"])
    params = build_taco_params(variant, actual_overrides)
    if variant == "taco_oil":
        return TACOOilStrategy(params)
    return TACOStrategy(params)


def run_taco_compare(
    start_date: str = "20250101",
    end_date: str = "20260318",
    symbols: Optional[List[str]] = None,
    variants: Optional[List[str]] = None,
    thresholds: Optional[List[float]] = None,
    keyword_overrides: Optional[Dict[str, float]] = None,
) -> List[Dict[str, object]]:
    """
    运行批量比较
    """
    actual_symbols = list(symbols or DEFAULT_SYMBOLS)
    actual_variants = list(variants or ["taco", "taco_oil"])
    cases = _build_compare_cases(actual_variants, thresholds, keyword_overrides)

    print("=" * 72)
    print("TACO / TACO-OIL 参数批量比较")
    print("=" * 72)
    print(f"date range: {start_date} -> {end_date}")
    print(f"symbols: {len(actual_symbols)}")
    if keyword_overrides:
        print(f"keyword overrides: {json.dumps(keyword_overrides, ensure_ascii=False)}")

    data_source = DataSource()
    results: List[Dict[str, object]] = []

    for case in cases:
        strategy = _build_strategy(str(case.get("variant", "taco")), dict(case.get("overrides", {})))
        engine = BacktestEngine(strategy=strategy, initial_capital=1000000)
        result = engine.run(
            symbols=actual_symbols,
            start_date=start_date,
            end_date=end_date,
            data_source=data_source,
        )
        row = {
            "name": str(case.get("name", "")),
            "variant": str(case.get("variant", "")),
            "total_return": float(result.total_return),
            "annual_return": float(result.annual_return),
            "sharpe_ratio": float(result.sharpe_ratio),
            "max_drawdown": float(result.max_drawdown),
            "win_rate": float(result.win_rate),
            "trade_count": int(len(result.trades)),
        }
        results.append(row)
        print(
            f"{row['name']:<28} "
            f"variant={row['variant']:<8} "
            f"total={row['total_return']:.2%} "
            f"annual={row['annual_return']:.2%} "
            f"sharpe={row['sharpe_ratio']:.2f} "
            f"mdd={row['max_drawdown']:.2%} "
            f"win={row['win_rate']:.2%} "
            f"trades={row['trade_count']}"
        )

    print("-" * 72)
    print("按总收益排序")
    for row in sorted(results, key=lambda item: item["total_return"], reverse=True):
        print(f"{row['name']}: {row['total_return']:.2%} / sharpe {row['sharpe_ratio']:.2f} / trades {row['trade_count']}")
    return results


def main() -> None:
    """
    命令行入口
    """
    parser = argparse.ArgumentParser(description="TACO compare tool")
    parser.add_argument("--start-date", default="20250101", help="start date, e.g. 20250101")
    parser.add_argument("--end-date", default="20260318", help="end date, e.g. 20260318")
    parser.add_argument("--symbols", nargs="+", help="stock symbols")
    parser.add_argument("--variants", nargs="+", choices=["taco", "taco_oil"], help="variants to compare")
    parser.add_argument("--thresholds", nargs="+", type=float, help="threshold overrides")
    parser.add_argument(
        "--keyword-override",
        action="append",
        help="keyword weight override, e.g. --keyword-override Hormuz=2.6",
    )
    args = parser.parse_args()

    run_taco_compare(
        start_date=args.start_date,
        end_date=args.end_date,
        symbols=args.symbols,
        variants=args.variants,
        thresholds=args.thresholds,
        keyword_overrides=_parse_keyword_overrides(args.keyword_override),
    )


if __name__ == "__main__":
    main()
