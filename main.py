# -*- coding: utf-8 -*-
"""
A股量化交易主程序
ETF/LOF + Price Action + MACD + 弱转强策略
"""
from dotenv import load_dotenv

load_dotenv()
load_dotenv(".env.local", override=True)
from data import DataSource, get_st_pool, get_dynamic_pool, get_pool_generator
from strategy import (
    PriceActionMACDStrategy,
    MACDStrategy,
    PriceActionStrategy,
    BreakoutStrategy,
    WeakToStrongSelector,
    WeakToStrongTimingStrategy,
    WeakToStrongParams,
)
from backtest import BacktestEngine, PerformanceAnalyzer
from utils.logger import get_logger

logger = get_logger(__name__)


def get_etf_lof_pool():
    """获取ETF/LOF股票池"""
    print("=" * 50)
    print("获取ETF/LOF股票池...")
    print("=" * 50)
    
    import os
    db_path = os.environ.get("QUANT_CACHE_DIR", "./runtime/data/recommend.db")
    
    pool = get_st_pool("etf_lof", DataSource(cache_dir=os.environ.get("QUANT_CACHE_DIR", "./runtime/data/cache")))
    
    print(f"\n总共获取到 {len(pool)} 只ETF/LOF产品")
    
    t0_products = [p for p in pool.get_t0_products_first() if p.get("t0", False)]
    print(f"其中 T+0 产品: {len(t0_products)} 只")
    
    print("\n成交额TOP 10产品:")
    top_products = sorted(pool.get_t0_products_first(), key=lambda x: -x.get("amount", 0))[:10]
    for i, p in enumerate(top_products, 1):
        t0_flag = "✓T+0" if p.get("t0") else ""
        print(f"  {i}. {p.get('code')} {p.get('name')} - 成交额: {p.get('amount', 0)/1e8:.2f}亿 {t0_flag}")
    
    return pool


def update_stock_pool():
    """更新每日股票池"""
    print("=" * 50)
    print("更新每日股票池...")
    print("=" * 50)
    
    import os
    db_path = os.environ.get("DATABASE_PATH", "./data/recommend.db")
    generator = get_pool_generator(db_path)
    
    result = generator.update_daily()
    
    print("\nETF/LOF 池 (T+0 优先):")
    for i, p in enumerate(result["etf_lof"][:10], 1):
        t0_flag = "✓T+0" if p.t0 else ""
        print(f"  {i}. {p.code} {p.name} - 成交额: {p.amount/1e8:.2f}亿 评分: {p.score:.0f} {t0_flag}")
    
    print(f"\n热点股票池 (中高风险优先):")
    for i, p in enumerate(result["stock"][:10], 1):
        risk_emoji = {"high": "🔴", "medium_high": "🟠", "medium": "🟡"}.get(p.risk_level, "⚪")
        print(f"  {i}. {p.code} {p.name} - 涨幅: {p.change_pct:+.2f}% 风险: {p.risk_level} 评分: {p.score:.0f} {risk_emoji} {p.reason}")
    
    summary = generator.get_pool_summary()
    print(f"\n股票池摘要: 总计 {summary['total']} 只, 更新于 {summary['updated']}")
    for ptype, cnt in summary["by_type"].items():
        print(f"  {ptype}: {cnt} 只")
    
    return result


def run_weak_strong_scan():
    """运行弱转强选股扫描"""
    import os
    print("=" * 60)
    print("弱转强选股扫描 (双策略: PA+MACD + 弱转强)")
    print("=" * 60)
    
    db_path = os.environ.get("DATABASE_PATH", "./data/recommend.db")
    
    from trading import RealtimeMonitor
    monitor = RealtimeMonitor(etf_count=5, stock_count=10, db_path=db_path)
    
    results = monitor.scan_market()
    
    print("\nETF推荐:")
    for r in results["etf"]:
        print(f"  {r.code} {r.name} - {r.signal_type} @ {r.price:.4f} ({r.reason})")
    
    print(f"\nA股推荐 (PA+MACD + 弱转强):")
    dual_signals = [s for s in results["stock"] if s.dual_signal]
    print(f"  双重信号({len(dual_signals)}只):")
    for s in dual_signals[:5]:
        print(f"    ⭐ {s.code} {s.name} - {s.signal_type} @ {s.price:.4f} ({s.reason})")
    
    print(f"\n  单信号({len(results['stock']) - len(dual_signals)}只):")
    for s in results["stock"]:
        if not s.dual_signal:
            ws_tag = f"[弱转强{s.ws_stage}/4]" if s.ws_stage > 0 else ""
            print(f"    {s.code} {s.name} - {s.signal_type} @ {s.price:.4f} {ws_tag}({s.reason})")
    
    return results


def run_backtest_with_trades():
    """运行回测并展示详细交易记录"""
    print("\n" + "=" * 60)
    print("A股量化选股+择时回测")
    print("=" * 60)
    
    import os
    data_source = DataSource(cache_dir=os.environ.get("QUANT_CACHE_DIR", "./runtime/data/cache"))
    
    # 使用A股主板/创业板股票
    pool_symbols = [
        "600000", "600036", "600519", "601318", "600887",
        "000001", "000002", "000858", "000333", "000651",
        "300750", "300059", "300015", "002594", "002415",
        "601012", "601166", "600030", "600900", "600028"
    ]
    
    code_to_name = {
        "600000": "浦发银行", "600036": "招商银行", "600519": "贵州茅台",
        "601318": "中国平安", "600887": "伊利股份", "000001": "平安银行",
        "000002": "万科A", "000858": "五粮液", "000333": "美的集团",
        "000651": "格力电器", "300750": "宁德时代", "300059": "东方财富",
        "300015": "爱尔眼科", "002594": "比亚迪", "002415": "海康威视",
        "601012": "隆基绿能", "601166": "兴业银行", "600030": "中信证券",
        "600900": "长江电力", "600028": "中国石化",
    }
    
    print(f"\n股票池: {len(pool_symbols)} 只 (A股主板/创业板)")
    print(f"标的: {', '.join(pool_symbols[:5])}...")
    
    strategy = PriceActionMACDStrategy(
        lookback=20,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        require_confirmation=True,
    )
    
    print("\n策略: PriceAction + MACD 复合策略")
    print(f"回测区间: 20250101 ~ 20260318")
    print(f"初始资金: 100万")
    
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
    
    print("\n" + "=" * 60)
    print("详细交易记录")
    print("=" * 60)
    print(f"{'日期':<12} {'代码':<10} {'名称':<15} {'操作':<6} {'价格':<10} {'数量':<10} {'手续费'}")
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
    print("持仓情况 (期末)")
    print("=" * 60)
    final_positions = [(sym, pos) for sym, pos in engine.portfolio.positions.items()]
    if final_positions:
        print(f"{'代码':<10} {'名称':<15} {'数量':<10} {'成本价':<10} {'当前价':<10} {'盈亏'}")
        print("-" * 70)
        for sym, pos in final_positions:
            name = code_to_name.get(sym, "")[:12]
            pnl = (pos.current_price - pos.cost) / pos.cost * 100
            print(f"{sym:<10} {name:<15} {pos.quantity:<10} {pos.cost:<10.4f} {pos.current_price:<10.4f} {pnl:+.2f}%")
    else:
        print("  无持仓")
    
    return result


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
    parser.add_argument("--once", action="store_true", help="只运行一次，不持续监控")
    parser.add_argument("--schedule", action="store_true", help="启动定时调度器")
    parser.add_argument("--bark-key", type=str, default="", help="Bark推送Key(可选；为空则不调用Bark)")
    
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
    
    from strategy.analysis.review.portfolio_tracker import PortfolioTracker
    from strategy.analysis.review.pnl_analyzer import PnLAnalyzer
    from strategy.analysis.review.report_generator import ReportGenerator
    
    tracker = PortfolioTracker(initial_capital=1000000)
    
    tracker.add_position("510300", "沪深300ETF", 10000, 3.85)
    tracker.add_position("159919", "沪深300ETF", 5000, 3.90)
    tracker.add_position("515000", "科技ETF", 20000, 1.25)
    
    tracker.update_prices({
        "510300": 3.90,
        "159919": 3.95,
        "515000": 1.20,
    })
    
    analyzer = PnLAnalyzer()
    report_text = analyzer.generate_analysis_report(tracker)
    print("\n" + report_text)
    
    generator = ReportGenerator()
    report = generator.generate_daily_report(tracker)
    
    title, body = generator.format_report_for_push(report)
    print(f"\n推送预览:\n{title}\n{body}")
    
    return {"tracker": tracker, "analyzer": analyzer, "report": report}


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="A股量化交易回测")
    parser.add_argument("--mode", choices=["pool", "backtest", "compare", "realtime", "pool-update", "weak-strong", "emotion-scan", "review"],
                       default="backtest", help="运行模式")
    parser.add_argument("--symbols", nargs="+", help="指定股票代码")
    parser.add_argument("--strategy", default="pa_macd", 
                       choices=["pa_macd", "macd", "pa", "breakout", "weak_strong"],
                       help="选择策略")
    parser.add_argument("--once", action="store_true", help="实时模式: 只运行一次")
    parser.add_argument("--schedule", action="store_true", help="实时模式: 启动定时调度器")
    parser.add_argument("--bark-key", type=str, default="", help="Bark推送Key(可选；为空则不调用Bark)")
    
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
        run_backtest_with_trades()


if __name__ == "__main__":
    main()
