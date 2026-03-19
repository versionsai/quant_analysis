# -*- coding: utf-8 -*-
"""
Agent 运行入口
用于测试和手动运行 Agent
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from agents import init_quant_agent
from utils.logger import get_logger

logger = get_logger(__name__)


def main():
    """主函数"""
    api_key = os.environ.get("SILICONFLOW_API_KEY", "")

    if not api_key:
        print("错误: 请设置 SILICONFLOW_API_KEY 环境变量")
        print("export SILICONFLOW_API_KEY=your_api_key")
        return

    print("=" * 50)
    print("量化交易 AI Agent")
    print("=" * 50)

    agent = init_quant_agent()

    print("\n选择任务:")
    print("1. 每日市场分析")
    print("2. 交易检查")
    print("3. 自定义任务")

    choice = input("\n请输入选项 (1/2/3): ").strip()

    if choice == "1":
        print("\n正在执行每日市场分析...")
        result = agent.run_daily_analysis()
        print("\n" + "=" * 50)
        print("分析结果:")
        print("=" * 50)
        print(result)

    elif choice == "2":
        print("\n正在执行交易检查...")
        result = agent.run_trade_check()
        print("\n" + "=" * 50)
        print("检查结果:")
        print("=" * 50)
        print(result)

    elif choice == "3":
        task = input("\n请输入任务描述: ").strip()
        if task:
            print("\n正在执行任务...")
            result = agent.run(task)
            print("\n" + "=" * 50)
            print("执行结果:")
            print("=" * 50)
            print(result)
        else:
            print("任务不能为空")

    else:
        print("无效的选项")


if __name__ == "__main__":
    main()
