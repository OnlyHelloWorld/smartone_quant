import backtrader as bt
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import math


class HS300RSIStrategy(bt.Strategy):
    """
    沪深300成分股RSI轮动策略
    每月末每个行业选择RSI最低的1只股票持有
    """

    params = (
        ('rsi_period', 14),  # RSI计算周期
        ('target_value', 100000),  # 每只股票目标投资金额
        ('rebalance_day', -1),  # 每月倒数第几个交易日调仓（-1表示最后一个交易日）
        ('industry_file', '../stock_data/sector_stocks/300SW1_sectors_stocks.csv'),  # 行业分类文件
    )

    def log(self, txt, dt=None):
        """输出日志"""
        dt = dt or self.datas[0].datetime.date(0)
        print(f'{dt.isoformat()} {txt}')

    def __init__(self):
        # 加载行业分类数据
        self.industry_mapping = self.load_industry_classification()

        # 初始化RSI指标
        self.rsi_indicators = {}
        self.data_names = []

        for i, data in enumerate(self.datas):
            # 获取数据名称
            data_name = data._name if hasattr(data, '_name') else f'data_{i}'
            self.data_names.append(data_name)

            # 为每个数据源创建RSI指标
            self.rsi_indicators[data_name] = bt.indicators.RSI(
                data.close,
                period=self.params.rsi_period
            )

        # 记录持仓状态
        self.current_positions = {}
        self.last_rebalance_month = None

        # 调试信息
        self.log_enabled = True

        # 打印行业分布信息
        self.print_industry_distribution()

    def load_industry_classification(self):
        """加载行业分类数据"""
        industry_mapping = {}
        try:
            df = pd.read_csv(self.params.industry_file, header=None, names=['stock_code', 'industry'])
            for _, row in df.iterrows():
                industry_mapping[row['stock_code']] = row['industry']

            self.log(f"成功加载行业分类数据，共 {len(industry_mapping)} 只股票")
            return industry_mapping
        except Exception as e:
            self.log(f"加载行业分类文件失败: {e}")
            return {}

    def print_industry_distribution(self):
        """打印行业分布信息"""
        if not self.industry_mapping:
            return

        # 统计每个行业的股票数量
        industry_count = {}
        available_stocks = []

        for stock_code in self.data_names:
            if stock_code in self.industry_mapping:
                industry = self.industry_mapping[stock_code]
                industry_count[industry] = industry_count.get(industry, 0) + 1
                available_stocks.append(stock_code)

        self.log(f"\n=== 行业分布统计 ===")
        for industry, count in sorted(industry_count.items()):
            self.log(f"{industry}: {count}只股票")

        self.log(f"\n数据中股票总数: {len(self.data_names)}")
        self.log(f"有行业分类的股票: {len(available_stocks)}")
        self.log(f"缺少行业分类的股票: {len(self.data_names) - len(available_stocks)}")

        # 显示缺少行业分类的股票
        missing_stocks = [stock for stock in self.data_names if stock not in self.industry_mapping]
        if missing_stocks:
            self.log(f"缺少行业分类的股票: {missing_stocks[:10]}...")  # 只显示前10个

    def notify_order(self, order):
        """订单状态通知"""
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'买入执行: {order.data._name}, 价格: {order.executed.price:.2f}, '
                         f'数量: {order.executed.size}, 成本: {order.executed.value:.2f}')
            else:
                self.log(f'卖出执行: {order.data._name}, 价格: {order.executed.price:.2f}, '
                         f'数量: {order.executed.size}, 收入: {order.executed.value:.2f}')

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f'订单失败: {order.data._name}, 状态: {order.getstatusname()}')

    def notify_trade(self, trade):
        """交易通知"""
        if trade.isclosed:
            self.log(f'交易完成: {trade.data._name}, 盈亏: {trade.pnl:.2f}')

    def is_month_end(self):
        """判断是否为月末最后一个交易日"""
        current_date = self.datas[0].datetime.date(0)

        # 获取下一个交易日
        next_date = current_date + timedelta(days=1)

        # 判断是否跨月
        return current_date.month != next_date.month

    def get_industry_best_stocks(self):
        """获取每个行业中RSI最低的股票"""
        if not self.industry_mapping:
            self.log("没有行业分类数据，无法选股")
            return []

        # 按行业分组股票
        industry_stocks = {}

        for stock_code in self.data_names:
            if stock_code in self.industry_mapping:
                industry = self.industry_mapping[stock_code]
                rsi_value = self.rsi_indicators[stock_code][0]

                # 只考虑有效的RSI值
                if not np.isnan(rsi_value):
                    if industry not in industry_stocks:
                        industry_stocks[industry] = []
                    industry_stocks[industry].append((stock_code, rsi_value))

        # 每个行业选择RSI最低的股票
        selected_stocks = []

        for industry, stocks in industry_stocks.items():
            if stocks:
                # 按RSI值排序，选择最低的
                best_stock = min(stocks, key=lambda x: x[1])
                selected_stocks.append(best_stock)
                self.log(f"行业[{industry}]选中: {best_stock[0]}(RSI:{best_stock[1]:.2f})")

        return selected_stocks

    def calculate_position_size(self, data, target_value):
        """计算买入股数（A股最小100股）"""
        price = data.close[0]
        if price <= 0:
            return 0

        # 计算目标股数
        target_shares = target_value / price

        # 向下取整到100的倍数
        actual_shares = int(target_shares / 100) * 100

        return actual_shares

    def rebalance_portfolio(self):
        """重新平衡投资组合"""
        self.log("开始调仓...")

        # 获取当前资金
        current_cash = self.broker.getcash()
        current_value = self.broker.getvalue()

        self.log(f"当前资金: {current_cash:.2f}, 总资产: {current_value:.2f}")

        # 获取每个行业RSI最低的股票
        selected_stocks = self.get_industry_best_stocks()

        if not selected_stocks:
            self.log("没有可选择的股票")
            return

        self.log(f"共选中 {len(selected_stocks)} 只股票（每行业1只）")

        # 平仓所有现有持仓
        for data in self.datas:
            position = self.getposition(data)
            if position.size > 0:
                self.log(f"平仓: {data._name}, 数量: {position.size}")
                self.close(data=data)

        # 计算每只股票的目标投资金额
        available_cash = current_value  # 平仓后的可用资金
        target_value_per_stock = available_cash / len(selected_stocks)

        self.log(f"每只股票目标投资金额: {target_value_per_stock:.2f}")

        # 买入新选中的股票
        for stock_name, rsi_value in selected_stocks:
            # 找到对应的数据源
            target_data = None
            for data in self.datas:
                if data._name == stock_name:
                    target_data = data
                    break

            if target_data is None:
                self.log(f"找不到数据源: {stock_name}")
                continue

            # 计算买入数量
            position_size = self.calculate_position_size(target_data, target_value_per_stock)

            if position_size > 0:
                # 估算需要的资金
                estimated_cost = position_size * target_data.close[0]

                if estimated_cost <= current_cash:
                    industry = self.industry_mapping.get(stock_name, "未知行业")
                    self.log(f"买入: {stock_name}[{industry}], 数量: {position_size}, "
                             f"预估成本: {estimated_cost:.2f}, RSI: {rsi_value:.2f}")
                    self.buy(data=target_data, size=position_size)
                    current_cash -= estimated_cost
                else:
                    self.log(f"资金不足，跳过: {stock_name}")

        # 统计选中的行业
        selected_industries = set()
        for stock_name, _ in selected_stocks:
            industry = self.industry_mapping.get(stock_name, "未知行业")
            selected_industries.add(industry)

        self.log(f"本次调仓涉及行业: {sorted(list(selected_industries))}")

    def next(self):
        """主策略逻辑"""
        current_date = self.datas[0].datetime.date(0)
        current_month = current_date.month

        # 检查是否需要调仓
        need_rebalance = False

        if self.last_rebalance_month is None:
            # 第一次运行
            need_rebalance = True
            self.log("首次建仓")
        elif self.is_month_end() and current_month != self.last_rebalance_month:
            # 月末调仓
            need_rebalance = True
            self.log("月末调仓")

        if need_rebalance:
            self.rebalance_portfolio()
            self.last_rebalance_month = current_month


# 自定义佣金类
class A_ShareCommission(bt.CommInfoBase):
    """A股佣金设置"""
    params = (
        ('commission', 0.0001),  # 万一佣金
        ('mult', 1.0),
        ('margin', None),
        ('stocklike', True),
        ('commtype', bt.CommInfoBase.COMM_PERC),
        ('percabs', False),
    )

    def _getcommission(self, size, price, pseudoexec):
        """计算佣金"""
        commission = abs(size) * price * self.p.commission
        # A股最低佣金5元
        return max(commission, 5.0)


def run_strategy():
    """运行策略"""
    import os
    import glob

    cerebro = bt.Cerebro()

    # 添加策略
    cerebro.addstrategy(HS300RSIStrategy)

    # 设置初始资金
    cerebro.broker.setcash(1000000.0)  # 100万初始资金

    # 设置佣金
    cerebro.broker.addcommissioninfo(A_ShareCommission())

    # 设置滑点
    cerebro.broker.set_slippage_perc(0.01)  # 0.01%滑点

    # 首先读取行业分类文件，获取需要加载的股票列表
    industry_file = '../stock_data/sector_stocks/300SW1_sectors_stocks.csv'
    stock_list = []

    try:
        df_industry = pd.read_csv(industry_file, header=None, names=['stock_code', 'industry'])
        stock_list = df_industry['stock_code'].tolist()
        print(f"从行业分类文件读取到 {len(stock_list)} 只股票")

        # 显示行业分布
        industry_counts = df_industry['industry'].value_counts()
        print("\n行业分布:")
        for industry, count in industry_counts.items():
            print(f"  {industry}: {count}只")

    except Exception as e:
        print(f"读取行业分类文件失败: {e}")
        return

    # 股票数据目录
    data_folder = '../stock_data/daily'

    # 加载指定股票的CSV数据
    loaded_count = 0
    failed_stocks = []

    for stock_code in stock_list:
        csv_file = os.path.join(data_folder, f'{stock_code}.csv')

        if not os.path.exists(csv_file):
            failed_stocks.append(stock_code)
            continue

        try:
            # 读取CSV数据
            columns = ['time', 'open', 'high', 'low', 'close', 'volume', 'amount']
            df = pd.read_csv(csv_file, header=None, names=columns)

            # 检查必要的列是否存在
            required_columns = ['time', 'open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in required_columns):
                print(f"股票 {stock_code} 数据列不完整，跳过")
                failed_stocks.append(stock_code)
                continue

            # 确保时间列为datetime格式
            df['time'] = pd.to_datetime(df['time'])
            df.set_index('time', inplace=True)

            # 按时间排序
            df.sort_index(inplace=True)

            # 检查数据是否为空
            if df.empty:
                print(f"股票 {stock_code} 数据为空，跳过")
                failed_stocks.append(stock_code)
                continue

            # 创建数据源
            data = bt.feeds.PandasData(
                dataname=df,
                name=stock_code,  # 设置股票代码作为名称
                datetime=None,  # 使用index作为时间
                open='open',
                high='high',
                low='low',
                close='close',
                volume='volume',
                openinterest=-1  # 股票没有持仓量
            )

            cerebro.adddata(data)
            loaded_count += 1

        except Exception as e:
            print(f"加载股票 {stock_code} 数据失败: {e}")
            failed_stocks.append(stock_code)

    print(f"\n数据加载结果:")
    print(f"  成功加载: {loaded_count} 只股票")
    print(f"  加载失败: {len(failed_stocks)} 只股票")

    if failed_stocks and len(failed_stocks) <= 10:
        print(f"  失败股票: {failed_stocks}")
    elif len(failed_stocks) > 10:
        print(f"  失败股票（前10个）: {failed_stocks[:10]}...")

    if loaded_count == 0:
        print("没有成功加载任何股票数据，请检查数据文件路径和格式")
        return

    print(f"\n将使用 {loaded_count} 只股票进行回测")

    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name='time_return')

    print('初始资金: %.2f' % cerebro.broker.getvalue())

    # 运行策略
    results = cerebro.run()

    print('最终资金: %.2f' % cerebro.broker.getvalue())

    # 打印分析结果
    strat = results[0]

    print('\n=== 策略分析结果 ===')
    print(f'总收益率: {strat.analyzers.returns.get_analysis()["rtot"]:.2%}')
    print(f'夏普比率: {strat.analyzers.sharpe.get_analysis().get("sharperatio", 0):.2f}')
    print(f'最大回撤: {strat.analyzers.drawdown.get_analysis()["max"]["drawdown"]:.2%}')

    # 绘制结果
    cerebro.plot(style='candlestick')


if __name__ == '__main__':
    run_strategy()