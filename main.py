# -*- coding: utf-8 -*-
"""
A 股量化交易主程序
ETF/LOF + Price Action + MACD + Weak-to-Strong strategies
"""
import os

from dotenv import load_dotenv
import pandas as pd

load_dotenv()
load_dotenv(".env.local", override=True)
from data import DataSource, get_st_pool, get_dynamic_pool, get_pool_generator
from strategy import (
    PriceActionMACDStrategy,
    MACDStrategy,
    PriceActionStrategy,
    BreakoutStrategy,
    TACOStrategy,
    TACOOilStrategy,
    WeakToStrongSelector,
    WeakToStrongTimingStrategy,
    WeakToStrongParams,
)
from backtest import BacktestEngine, PerformanceAnalyzer
from trading.review_report import build_runtime_review_report, save_runtime_review_report
from agents import (
    rank_experiment_candidates,
    save_best_config_snapshot,
    StrategyTuningAdvisor,
    run_strategy_experiments,
    save_experiment_report,
    save_tuning_review,
)
from utils.logger import get_logger

logger = get_logger(__name__)


def get_taco_fund_candidates(data_source: DataSource, timeout_sec: float = 15.0) -> list:
    """获取 TACO 默认基金池，超时则回退到内置 ETF/LOF 池。"""
    try:
        pool = get_dynamic_pool(pool_type="etf_lof", limit=30, db_path=os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db"))
        rows = pool.get_t0_products_first()[:30]
        if rows:
            return rows
    except Exception as e:
        logger.warning(f"TACO ETF/LOF dynamic pool load failed, fallback to default pool: {e}")

    try:
        return list(data_source.get_default_pool())[:30]
    except Exception:
        return []


def get_backtest_symbols(strategy_name: str, data_source: DataSource) -> list:
    """根据策略返回默认回测标的"""
    if strategy_name in {"taco", "taco_oil"}:
        products = get_taco_fund_candidates(data_source, timeout_sec=15.0)[:20]
        symbols = [str(item.get("code", "")).zfill(6) for item in products if item.get("code")]
        if symbols:
            return symbols
    return [
        "600000", "600036", "600519", "601318", "600887",
        "000001", "000002", "000858", "000333", "000651",
        "300750", "300059", "300015", "002594", "002415",
        "601012", "601166", "600030", "600900", "600028",
    ]


def build_strategy(strategy_name: str):
    """根据策略名称构建策略实例"""
    if strategy_name == "macd":
        return MACDStrategy(fast=12, slow=26, signal=9)
    if strategy_name == "pa":
        return PriceActionStrategy(lookback=20)
    if strategy_name == "breakout":
        return BreakoutStrategy(lookback=20)
    if strategy_name == "taco":
        return TACOStrategy()
    if strategy_name == "taco_oil":
        return TACOOilStrategy()
    return PriceActionMACDStrategy(
        lookback=20,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        require_confirmation=True,
    )


def get_strategy_display_name(strategy_name: str) -> str:
    """获取策略展示名称"""
    mapping = {
        "pa_macd": "PriceAction + MACD",
        "macd": "MACD",
        "pa": "PriceAction",
        "breakout": "Breakout",
        "taco": "TACO Event Recovery",
        "taco_oil": "TACO-OIL Strategy",
    }
    return mapping.get(strategy_name, strategy_name)


def get_etf_lof_pool():
    """获取 ETF/LOF 股票池"""
    print("=" * 50)
    print("获取 ETF/LOF 股票池...")
    print("=" * 50)
    
    import os
    pool = get_st_pool("etf_lof", DataSource(cache_dir=os.environ.get("QUANT_CACHE_DIR", "./runtime/data/cache")))
    
    print(f"\n共获取 {len(pool)} 只 ETF/LOF 产品")
    
    t0_products = [p for p in pool.get_t0_products_first() if p.get("t0", False)]
    print(f"T+0 products: {len(t0_products)}")
    
    print("\n成交额 TOP 10 产品:")
    top_products = sorted(pool.get_t0_products_first(), key=lambda x: -x.get("amount", 0))[:10]
    for i, p in enumerate(top_products, 1):
        t0_flag = " T+0" if p.get("t0") else ""
        print(f"  {i}. {p.get('code')} {p.get('name')} - 成交额 {p.get('amount', 0)/1e8:.2f} 亿{t0_flag}")
    
    return pool


def update_stock_pool():
    """更新每日股票池"""
    print("=" * 50)
    print("更新每日股票池...")
    print("=" * 50)
    
    import os
    db_path = os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db")
    generator = get_pool_generator(db_path)
    
    result = generator.update_daily()
    
    print("\nETF/LOF 池（T+0 优先）:")
    for i, p in enumerate(result["etf_lof"][:10], 1):
        t0_flag = " T+0" if p.t0 else ""
        print(f"  {i}. {p.code} {p.name} - 成交额 {p.amount/1e8:.2f} 亿 评分: {p.score:.0f}{t0_flag}")
    
    print("\n热点股票池（中高风险优先）:")
    for i, p in enumerate(result["stock"][:10], 1):
        risk_emoji = {"high": "[HIGH]", "medium_high": "[MEDIUM_HIGH]", "medium": "[MEDIUM]"}.get(p.risk_level, "[INFO]")
        print(f"  {i}. {p.code} {p.name} - 涨幅: {p.change_pct:+.2f}% 风险: {p.risk_level} 评分: {p.score:.0f} {risk_emoji} {p.reason}")
    
    summary = generator.get_pool_summary()
    print(f"\n股票池摘要: 总计 {summary['total']} 只, 已更新 {summary['updated']}")
    for ptype, cnt in summary["by_type"].items():
        print(f"  {ptype}: {cnt}")
    
    return result


def run_weak_strong_scan():
    """运行弱转强选股扫描"""
    import os
    print("=" * 60)
    print("弱转强选股扫描（双策略：PA+MACD + Weak-to-Strong）")
    print("=" * 60)
    
    db_path = os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db")
    
    from trading import RealtimeMonitor
    monitor = RealtimeMonitor(etf_count=5, stock_count=10, db_path=db_path)
    
    results = monitor.scan_market()
    
    print("\nETF 推荐:")
    for r in results["etf"]:
        print(f"  {r.code} {r.name} - {r.signal_type} @ {r.price:.4f} ({r.reason})")
    
    print("\nA 股推荐（PA+MACD + Weak-to-Strong）:")
    dual_signals = [s for s in results["stock"] if s.dual_signal]
    print(f"  双重信号（{len(dual_signals)} 只）:")
    for s in dual_signals[:5]:
        print(f"    * {s.code} {s.name} - {s.signal_type} @ {s.price:.4f} ({s.reason})")
    
    print(f"\n  单信号（{len(results['stock']) - len(dual_signals)} 只）:")
    for s in results["stock"]:
        if not s.dual_signal:
            ws_tag = f"[WS {s.ws_stage}/4]" if s.ws_stage > 0 else ""
            print(f"    {s.code} {s.name} - {s.signal_type} @ {s.price:.4f} {ws_tag}({s.reason})")
    
    return results


def run_backtest_with_trades(strategy_name: str = "pa_macd"):
    """运行回测并展示详细交易记录"""
    print("\n" + "=" * 60)
    print("A 股量化选股 + 择时回测")
    print("=" * 60)
    
    import os
    data_source = DataSource(cache_dir=os.environ.get("QUANT_CACHE_DIR", "./runtime/data/cache"))
    
    # 使用 A 股主板/创业板股票
    pool_symbols = get_backtest_symbols(strategy_name, data_source)
    
    code_to_name = {
        "600000": "浦发银行", "600036": "招商银行", "600519": "贵州茅台",
        "601318": "中国平安", "600887": "伊利股份", "000001": "平安银行",
        "000002": "万科A", "000858": "五粮液", "000333": "美的集团",
        "000651": "格力电器", "300750": "宁德时代", "300059": "东方财富",
        "300015": "爱尔眼科", "002594": "比亚迪", "002415": "海康威视",
        "601012": "隆基绿能", "601166": "兴业银行", "600030": "中信证券",
        "600900": "长江电力", "600028": "中国石化",
    }
    
    print(f"\n股票池: {len(pool_symbols)} 只")
    print(f"标的: {', '.join(pool_symbols[:5])}...")
    
    strategy = build_strategy(strategy_name)
    
    print(f"\n策略: {get_strategy_display_name(strategy_name)}")
    print("回测区间: 20250101 ~ 20260318")
    print(f"Initial capital: 1000000")
    
    engine = BacktestEngine(
        strategy=strategy,
        initial_capital=1000000,
    )
    
    result = engine.run(
        symbols=pool_symbols,
        start_date="20250101",
        end_date="20260318",
        data_source=data_source,
    )
    
    print("\n" + "=" * 60)
    print("回测结果")
    print("=" * 60)
    print(f"  总收益: {result.total_return:.2%}")
    print(f"  年化收益: {result.annual_return:.2%}")
    print(f"  夏普比率: {result.sharpe_ratio:.2f}")
    print(f"  最大回撤: {result.max_drawdown:.2%}")
    print(f"  胜率: {result.win_rate:.2%}")
    print(f"  交易次数: {len(result.trades)}")
    if result.benchmark_metrics:
        print("\n基准对比:")
        for item in result.benchmark_metrics.values():
            print(
                f"  {item['name']}: 收益 {item['total_return']:.2%} | "
                f"超额 {item['excess_return']:+.2%} | 回撤 {item['max_drawdown']:.2%}"
            )
    if result.phase_metrics:
        print("\n分阶段验证:")
        for item in result.phase_metrics:
            print(
                f"  {item['phase']} {item['start']}~{item['end']} "
                f"({item['days']}天): 策略 {item['total_return']:.2%} | "
                f"基准 {item['benchmark_return']:.2%} | 超额 {item['excess_return']:+.2%}"
            )
    if result.signal_summary:
        print("\n信号摘要:")
        print(
            f"  候选 {int(result.signal_summary.get('candidate_count', 0))} | "
            f"放行 {int(result.signal_summary.get('gated_count', 0))} | "
            f"放行率 {result.signal_summary.get('gate_pass_rate', 0.0):.2%} | "
            f"平均候选分 {result.signal_summary.get('avg_candidate_score', 0.0):.2f}"
        )
    
    print("\n" + "=" * 60)
    print("详细交易记录")
    print("=" * 60)
    print("Date         Code       Name            Action Price      Qty        Fee")
    print("-" * 80)
    
    buy_count = 0
    sell_count = 0
    for trade in result.trades:
        name = code_to_name.get(trade.symbol, "")[:12]
        direction = "买入" if trade.direction == "buy" else "卖出"
        print(f"{trade.date.strftime('%Y-%m-%d'):<12} {trade.symbol:<10} {name:<15} {direction:<6} {trade.price:<10.4f} {trade.quantity:<10} {trade.commission:.2f}")
        if trade.direction == "buy":
            buy_count += 1
        else:
            sell_count += 1
    
    print(f"\n买入次数: {buy_count}, 卖出次数: {sell_count}")
    
    print("\n" + "=" * 60)
    print("持仓情况（期末）")
    print("=" * 60)
    final_positions = [(sym, pos) for sym, pos in engine.portfolio.positions.items()]
    if final_positions:
        print("Code       Name            Qty        Cost       Current    PnL")
        print("-" * 70)
        for sym, pos in final_positions:
            name = code_to_name.get(sym, "")[:12]
            pnl = (pos.current_price - pos.cost) / pos.cost * 100
            print(f"{sym:<10} {name:<15} {pos.quantity:<10} {pos.cost:<10.4f} {pos.current_price:<10.4f} {pnl:+.2f}%")
    else:
        print("  无持仓")
    
    return result


def run_taco_priority_scan(strategy_name: str = "taco"):
    """运行 TACO ETF/LOF 优先扫描"""
    print("\n" + "=" * 60)
    print("TACO ETF/LOF Priority Scan")
    print("=" * 60)

    data_source = DataSource(cache_dir=os.environ.get("QUANT_CACHE_DIR", "./runtime/data/cache"))
    strategy = build_strategy(strategy_name)
    products = get_taco_fund_candidates(data_source, timeout_sec=15.0)
    signals = []

    for item in products:
        symbol = str(item.get("code", "")).zfill(6)
        name = str(item.get("name", "") or "")
        if not symbol:
            continue
        df = data_source.get_kline(symbol, "20250101", "20260331")
        if df is None or df.empty:
            continue
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
        signal = strategy.on_bar(symbol, df)
        if signal and (signal.signal != 0 or float(signal.candidate_score or 0.0) > 0):
            signals.append(
                {
                    "code": symbol,
                    "name": name,
                    "signal": "BUY" if signal.signal > 0 else "SELL",
                    "weight": float(signal.weight or 0.0),
                    "candidate_score": float(signal.candidate_score or 0.0),
                    "gate_passed": bool(signal.gate_passed),
                    "gate_reason": str(signal.gate_reason or ""),
                }
            )

    signals = sorted(signals, key=lambda item: (-item["candidate_score"], -item["weight"], item["code"]))
    print(f"strategy: {get_strategy_display_name(strategy_name)}")
    print(f"etf/lof candidates: {len(products)}")
    print(f"signals: {len(signals)}")
    for row in signals[:10]:
        gate_tag = "PASS" if row["gate_passed"] else "BLOCK"
        print(
            f"{row['code']} {row['name']} {row['signal']} "
            f"candidate={row['candidate_score']:.2f} weight={row['weight']:.2f} {gate_tag}"
        )
    return signals


def run_strategy_comparison():
    """策略对比"""
    print("\n" + "=" * 50)
    print("策略对比测试")
    print("=" * 50)
    
    import os
    data_source = DataSource(cache_dir=os.environ.get("QUANT_CACHE_DIR", "./runtime/data/cache"))
    
    pool = get_st_pool("etf_lof", data_source)
    products = pool.get_t0_products_first()[:15]
    symbols = [p["code"] for p in products]
    
    strategies = {
        "PriceAction+MACD": PriceActionMACDStrategy(lookback=20),
        "MACD": MACDStrategy(fast=12, slow=26, signal=9),
        "PriceAction": PriceActionStrategy(lookback=20),
        "Breakout": BreakoutStrategy(lookback=20),
        "TACO": TACOStrategy(),
        "TACO-OIL": TACOOilStrategy(),
    }
    
    results = {}
    
    for name, strategy in strategies.items():
        print(f"\n测试策略: {name}")
        
        engine = BacktestEngine(strategy=strategy, initial_capital=1000000)
        
        result = engine.run(
            symbols=symbols,
            start_date="20240101",
            end_date="20240630",
            data_source=data_source,
        )
        
        results[name] = result
        print(f"  总收益: {result.total_return:.2%}")
    
    print("\n" + "=" * 50)
    print("策略对比结果")
    print("=" * 50)
    for name, result in sorted(results.items(), key=lambda x: -x[1].total_return):
        print(f"  {name}: {result.total_return:.2%}")


def run_realtime():
    """运行实时选股推送"""
    from trading import run_realtime_scan, run_scheduler
    
    import argparse
    parser = argparse.ArgumentParser(description="实时选股推送")
    parser.add_argument("--once", action="store_true", help="run once only")
    parser.add_argument("--schedule", action="store_true", help="start scheduler")
    parser.add_argument("--bark-key", type=str, default="", help="Bark key")
    
    args = parser.parse_args([])
    
    if args.bark_key:
        from trading import set_pusher_key
        set_pusher_key(args.bark_key)
    
    if args.schedule:
        print("启动定时调度器...")
        run_scheduler()
    else:
        print("运行单次实时扫描...")
        run_realtime_scan()


def run_emotion_scan():
    """运行情绪扫描"""
    print("=" * 60)
    print("市场情绪扫描")
    print("=" * 60)
    
    from strategy.analysis.multi_analyzer import MultiDimensionalAnalyzer
    
    analyzer = MultiDimensionalAnalyzer()
    
    market_emotion = analyzer.market_analyzer.get_market_emotion()
    if market_emotion:
        print("\n" + market_emotion.summary())
    
    sector_emotion = analyzer.sector_analyzer.analyze_sectors()
    if sector_emotion.success:
        print(f"\n板块情绪评分: {sector_emotion.score:.1f}")
        hot = sector_emotion.raw_data.get("hot_sectors", [])
        if hot:
            print(f"热门板块: {', '.join(hot[:5])}")
    
    return {"market": market_emotion, "sector": sector_emotion}


def run_review():
    """运行每日复盘"""
    print("=" * 60)
    print("每日复盘")
    print("=" * 60)
    from dashboard import DashboardService

    db_path = os.environ.get("DATABASE_PATH", "./runtime/data/recommend.db")
    service = DashboardService(db_path=db_path)
    report = build_runtime_review_report(service)
    saved = save_runtime_review_report(report)

    print("\n" + str(report.get("report_text", "")))
    print("\n" + "=" * 60)
    print("报告已生成")
    print(f"Markdown: {saved['markdown_path']}")
    print(f"JSON: {saved['json_path']}")

    return {
        "report": report,
        "saved": saved,
    }


def run_tuning_review(strategy_name: str = "pa_macd"):
    """运行回测并生成调优建议"""
    result = run_backtest_with_trades(strategy_name)
    advisor = StrategyTuningAdvisor()
    review = advisor.review_backtest(strategy_name, result)
    saved = save_tuning_review(review)

    print("\n" + "=" * 60)
    print("策略调优建议")
    print("=" * 60)
    print(str(review.get("summary", "") or ""))
    for index, item in enumerate(review.get("suggestions", []) or [], 1):
        print(f"{index}. [{item.get('priority', '')}] {item.get('target', '')}: {item.get('reason', '')}")
    print(f"\nMarkdown: {saved['markdown_path']}")
    print(f"JSON: {saved['json_path']}")
    return {"result": result, "review": review, "saved": saved}


def run_tuning_experiments(strategy_name: str = "pa_macd"):
    """运行调优建议并自动批量实验"""
    result = run_backtest_with_trades(strategy_name)
    advisor = StrategyTuningAdvisor()
    review = advisor.review_backtest(strategy_name, result)
    experiments = list(review.get("experiments", []) or [])
    if not experiments:
        print("\n暂无可运行的实验建议。")
        return {"result": result, "review": review, "experiments": []}

    rows = run_strategy_experiments(strategy_name, experiments)
    recommended = rank_experiment_candidates(rows)
    saved = save_experiment_report(strategy_name, review, rows, recommended_rows=recommended)
    snapshot = save_best_config_snapshot(strategy_name, recommended)

    print("\n" + "=" * 60)
    print("批量实验结果")
    print("=" * 60)
    for index, item in enumerate(rows, 1):
        print(
            f"{index}. {item['name']} | 收益 {item['total_return']:.2%} | "
            f"夏普 {item['sharpe_ratio']:.2f} | 回撤 {item['max_drawdown']:.2%} | 交易 {item['trade_count']}"
        )
    if recommended:
        print("\n推荐候选:")
        for index, item in enumerate(recommended, 1):
            print(
                f"{index}. {item['name']} | 沪深300超额 {item.get('primary_excess_return', 0.0):+.2%} | "
                f"收益 {item['total_return']:.2%} | 夏普 {item['sharpe_ratio']:.2f} | "
                f"回撤 {item['max_drawdown']:.2%} | 排名分 {item.get('ranking_score', 0.0):.2f}"
            )
    if snapshot.get("json_path"):
        print(f"\nBest Config: {snapshot['json_path']}")
    print(f"\nMarkdown: {saved['markdown_path']}")
    print(f"JSON: {saved['json_path']}")
    return {
        "result": result,
        "review": review,
        "experiments": rows,
        "recommended": recommended,
        "saved": saved,
        "snapshot": snapshot,
    }


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="A股量化交易回测")
    parser.add_argument("--mode", choices=["pool", "backtest", "compare", "realtime", "pool-update", "weak-strong", "emotion-scan", "review", "taco-compare", "taco-monitor", "tune-review", "tune-experiments"],
                       default="backtest", help="run mode")
    parser.add_argument("--symbols", nargs="+", help="stock symbols")
    parser.add_argument("--strategy", default="pa_macd", 
                       choices=["pa_macd", "macd", "pa", "breakout", "weak_strong", "taco", "taco_oil"],
                       help="strategy name")
    parser.add_argument("--once", action="store_true", help="实时模式: 只运行一次")
    parser.add_argument("--schedule", action="store_true", help="实时模式: 启动定时调度")
    parser.add_argument("--bark-key", type=str, default="", help="Bark key")
    
    args = parser.parse_args()
    
    if args.mode == "pool":
        get_etf_lof_pool()
    elif args.mode == "pool-update":
        update_stock_pool()
    elif args.mode == "weak-strong":
        run_weak_strong_scan()
    elif args.mode == "compare":
        run_strategy_comparison()
    elif args.mode == "emotion-scan":
        run_emotion_scan()
    elif args.mode == "review":
        run_review()
    elif args.mode == "tune-review":
        run_tuning_review(args.strategy)
    elif args.mode == "tune-experiments":
        run_tuning_experiments(args.strategy)
    elif args.mode == "taco-compare":
        from taco_compare import run_taco_compare
        run_taco_compare()
    elif args.mode == "taco-monitor":
        run_taco_priority_scan(args.strategy if args.strategy in {"taco", "taco_oil"} else "taco")
    elif args.mode == "realtime":
        from trading import set_pusher_key
        if args.bark_key:
            set_pusher_key(args.bark_key)
        
        if args.schedule:
            print("启动定时调度器...")
            from trading import run_scheduler
            run_scheduler()
        else:
            print("运行实时选股扫描...")
            from trading import run_realtime_scan
            run_realtime_scan()
    else:
        run_backtest_with_trades(args.strategy)


if __name__ == "__main__":
    main()
