# -*- coding: utf-8 -*-
"""
A股量化交易主程序
ETF/LOF + Price Action + MACD 策略

支持两种策略类型:
- 选股策略 (Selector): 从股票池中选择优质股票
- 择时策略 (Timing): 决定买入卖出时机
"""
from data import DataSource, get_st_pool
from strategy import (
    PriceActionMACDStrategy,
    MACDStrategy,
    PriceActionStrategy,
    BreakoutStrategy,
    MomentumSelector,
    DualMomentumSelector,
    FactorSelector,
    CompositeSelector,
)
from backtest import BacktestEngine, PerformanceAnalyzer, SelectorBacktestEngine
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


def run_selector_example():
    """运行选股策略示例"""
    print("\n" + "=" * 50)
    print("选股策略测试 - 推荐入选")
    print("=" * 50)
    
    data_source = DataSource(cache_dir="./data/cache")
    
    pool = get_st_pool("etf_lof", data_source)
    products = pool.get_t0_products_first()[:50]
    
    if not products:
        print("\n警告: 无法获取在线数据，使用默认ETF/LOF列表")
        products = _get_default_etf_lof_list()
    
    symbols = [p["code"] for p in products]
    code_to_name = {p["code"]: p.get("name", "") for p in products}
    
    print(f"\n股票池: {len(symbols)} 只")
    
    selector = CompositeSelector(
        momentum_weight=0.4,
        quality_weight=0.3,
        trend_weight=0.3,
        period=20,
    )
    
    print("\n正在加载数据...")
    for symbol in symbols:
        df = data_source.get_kline(symbol, "20240101", "20240630")
        if df is not None and not df.empty:
            selector.load_data(symbol, df)
    
    result = selector.select(
        symbols=symbols,
        start_date="20240101",
        end_date="20240630",
        top_n=10,
    )
    
    print("\n" + "=" * 50)
    print("综合选股结果 - 推荐买入")
    print("=" * 50)
    print(f"{'排名':<4} {'代码':<10} {'名称':<20} {'评分理由'}")
    print("-" * 60)
    for s in result.stocks[:10]:
        name = code_to_name.get(s.symbol, "")
        print(f"{s.rank:<4} {s.symbol:<10} {name:<20} {s.reason}")
    
    print("\n" + "=" * 50)
    print("生产验证建议")
    print("=" * 50)
    print("建议关注以下ETF/LOF:")
    for s in result.stocks[:5]:
        print(f"  - {s.symbol} ({code_to_name.get(s.symbol, '')})")
    
    return result.stocks


def _get_default_etf_lof_list():
    """获取默认ETF/LOF列表（当在线数据获取失败时使用）"""
    return [
        {"code": "511880", "name": "银华日利ETF"},
        {"code": "511880", "name": "银华日利"},
        {"code": "511010", "name": "易方达上证50ETF"},
        {"code": "510500", "name": "南方中证500ETF"},
        {"code": "512880", "name": "证券ETF"},
        {"code": "512880", "name": "证券ETF"},
        {"code": "515000", "name": "智能制造ETF"},
        {"code": "515000", "name": "智能制造"},
        {"code": "512690", "name": "消费ETF"},
        {"code": "512690", "name": "消费ETF"},
        {"code": "515790", "name": "光伏ETF"},
        {"code": "515790", "name": "光伏ETF"},
        {"code": "511660", "name": "货币ETF"},
        {"code": "511660", "name": "建信货币ETF"},
        {"code": "513050", "name": "中概互联网ETF"},
        {"code": "513050", "name": "中概互联网"},
        {"code": "513100", "name": "纳指ETF"},
        {"code": "513100", "name": "纳指ETF"},
        {"code": "510300", "name": "沪深300ETF"},
        {"code": "510300", "name": "沪深300ETF"},
        {"code": "159919", "name": "券商ETF"},
        {"code": "159919", "name": "券商ETF"},
        {"code": "159995", "name": "券商ETF"},
        {"code": "159995", "name": "券商ETF"},
        {"code": "510500", "name": "中证500ETF"},
    ]


def run_selector_backtest():
    """运行选股+择时组合回测"""
    print("\n" + "=" * 50)
    print("选股+择时组合回测")
    print("=" * 50)
    
    data_source = DataSource(cache_dir="./data/cache")
    
    pool = get_st_pool("etf_lof", data_source)
    products = pool.get_t0_products_first()[:50]
    pool_symbols = [p["code"] for p in products]
    
    print(f"\n股票池: {len(pool_symbols)} 只")
    
    selector = CompositeSelector(
        momentum_weight=0.4,
        quality_weight=0.3,
        trend_weight=0.3,
        period=20,
    )
    
    timing_strategy = PriceActionMACDStrategy(
        lookback=20,
        macd_fast=12,
        macd_slow=26,
        macd_signal=9,
        require_confirmation=True,
    )
    
    print("\n选股策略: 综合选股")
    print("择时策略: PriceAction+MACD")
    print(f"回测区间: 20240101 ~ 20240630")
    print(f"初始资金: 100万")
    
    engine = SelectorBacktestEngine(
        selector=selector,
        timing_strategy=timing_strategy,
        initial_capital=1000000,
    )
    
    result = engine.run(
        pool_symbols=pool_symbols,
        start_date="20240101",
        end_date="20240630",
        data_source=data_source,
        select_top_n=10,
        rebalance_freq=20,
    )
    
    print("\n" + "=" * 50)
    print("回测结果")
    print("=" * 50)
    print(f"  总收益: {result.total_return:.2%}")
    print(f"  年化收益: {result.annual_return:.2%}")
    print(f"  夏普比率: {result.sharpe_ratio:.2f}")
    print(f"  最大回撤: {result.max_drawdown:.2%}")
    print(f"  胜率: {result.win_rate:.2%}")
    print(f"  交易次数: {len(result.trades)}")
    
    print("\n" + "=" * 50)
    print("入选股票详情")
    print("=" * 50)
    for i, symbol in enumerate(engine.selected_stocks, 1):
        print(f"  {i}. {symbol}")
    
    return result, engine.selected_stocks


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description="A股量化交易回测")
    parser.add_argument("--mode", choices=["pool", "backtest", "compare", "select", "select_backtest"], 
                       default="backtest", help="运行模式")
    parser.add_argument("--symbols", nargs="+", help="指定股票代码")
    parser.add_argument("--strategy", default="pa_macd", 
                       choices=["pa_macd", "macd", "pa", "breakout"],
                       help="选择择时策略")
    parser.add_argument("--selector", default="composite",
                       choices=["momentum", "dual_momentum", "factor", "composite"],
                       help="选择选股策略")
    parser.add_argument("--top_n", type=int, default=10, help="选股数量")
    
    args = parser.parse_args()
    
    if args.mode == "pool":
        get_etf_lof_pool()
    elif args.mode == "compare":
        run_strategy_comparison()
    elif args.mode == "select":
        run_selector_example()
    elif args.mode == "select_backtest":
        run_selector_backtest()
    else:
        run_backtest_example()


if __name__ == "__main__":
    main()
