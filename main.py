# -*- coding: utf-8 -*-
"""
A股量化交易主程序
ETF/LOF + Price Action + MACD 策略
"""
from data import DataSource, get_st_pool
from strategy import (
    PriceActionMACDStrategy,
    MACDStrategy,
    PriceActionStrategy,
    BreakoutStrategy,
)
from backtest import BacktestEngine, PerformanceAnalyzer
from utils.logger import get_logger

logger = get_logger(__name__)


def get_etf_lof_pool():
    """获取ETF/LOF股票池"""
    print("=" * 50)
    print("获取ETF/LOF股票池...")
    print("=" * 50)
    
    data_source = DataSource(cache_dir="./data/cache")
    
    pool = get_st_pool("etf_lof", data_source)
    
    print(f"\n总共获取到 {len(pool)} 只ETF/LOF产品")
    
    t0_products = [p for p in pool.get_t0_products_first() if p.get("t0", False)]
    print(f"其中 T+0 产品: {len(t0_products)} 只")
    
    print("\n成交额TOP 10产品:")
    top_products = sorted(pool.get_t0_products_first(), key=lambda x: -x.get("amount", 0))[:10]
    for i, p in enumerate(top_products, 1):
        t0_flag = "✓T+0" if p.get("t0") else ""
        print(f"  {i}. {p.get('code')} {p.get('name')} - 成交额: {p.get('amount', 0)/1e8:.2f}亿 {t0_flag}")
    
    return pool


def run_backtest_example():
    """运行回测示例"""
    print("\n" + "=" * 50)
    print("开始回测...")
    print("=" * 50)
    
    data_source = DataSource(cache_dir="./data/cache")
    
    pool = get_st_pool("etf_lof", data_source)
    
    products = pool.get_t0_products_first()[:20]
    symbols = [p["code"] for p in products]
    
    print(f"\n选择股票池: {len(symbols)} 只")
    print(f"标的: {symbols[:5]}... (仅显示前5)")
    
    strategy = PriceActionMACDStrategy(
        lookback=20,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        require_confirmation=True,
    )
    
    engine = BacktestEngine(
        strategy=strategy,
        initial_capital=1000000,
    )
    
    result = engine.run(
        symbols=symbols,
        start_date="20240101",
        end_date="20240630",
        data_source=data_source,
    )
    
    report = PerformanceAnalyzer.generate_report(result)
    print("\n" + report)
    
    return result


def run_strategy_comparison():
    """策略对比"""
    print("\n" + "=" * 50)
    print("策略对比测试")
    print("=" * 50)
    
    data_source = DataSource(cache_dir="./data/cache")
    
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


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="A股量化交易回测")
    parser.add_argument("--mode", choices=["pool", "backtest", "compare"], 
                       default="backtest", help="运行模式")
    parser.add_argument("--symbols", nargs="+", help="指定股票代码")
    parser.add_argument("--strategy", default="pa_macd", 
                       choices=["pa_macd", "macd", "pa", "breakout"],
                       help="选择策略")
    
    args = parser.parse_args()
    
    if args.mode == "pool":
        get_etf_lof_pool()
    elif args.mode == "compare":
        run_strategy_comparison()
    else:
        run_backtest_example()


if __name__ == "__main__":
    main()
