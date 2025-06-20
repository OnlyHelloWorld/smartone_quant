from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import datetime  # 导入日期时间对象
import os.path  # 用于管理路径
import sys  # 用于获取脚本名称（在 argv[0] 中）

# 导入 backtrader 平台
import backtrader as bt


# 创建一个策略
class TestStrategy(bt.Strategy):
    params = (
        ('maperiod', 15),
    )

    def log(self, txt, dt=None):
        ''' 策略的日志记录函数 '''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # 保留对 data[0] 数据序列中“收盘价”行的引用
        self.dataclose = self.datas[0].close

        # 用于跟踪未完成订单及买入价格/手续费
        self.order = None
        self.buyprice = None
        self.buycomm = None

        # 添加简单移动平均指标
        self.sma = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.maperiod)

        # 用于画图展示的其他指标
        bt.indicators.ExponentialMovingAverage(self.datas[0], period=25)
        bt.indicators.WeightedMovingAverage(self.datas[0], period=25,
                                            subplot=True)
        bt.indicators.StochasticSlow(self.datas[0])
        bt.indicators.MACDHisto(self.datas[0])
        rsi = bt.indicators.RSI(self.datas[0])
        bt.indicators.SmoothedMovingAverage(rsi, period=10)
        bt.indicators.ATR(self.datas[0], plot=False)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # 买/卖单已提交/已接受 - 无需处理
            return

        # 检查订单是否已完成
        # 注意：如果资金不足，券商可能会拒绝订单
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    '买单执行, 价格: %.2f, 成本: %.2f, 手续费 %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))

                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # 卖出
                self.log('卖单执行, 价格: %.2f, 成本: %.2f, 手续费 %.2f' %
                         (order.executed.price,
                          order.executed.value,
                          order.executed.comm))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('订单已取消/保证金不足/被拒绝')

        # 记录：无未完成订单
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('操作盈亏, 毛利 %.2f, 净利 %.2f' %
                 (trade.pnl, trade.pnlcomm))

    def next(self):
        # 简单记录当前收盘价
        self.log('收盘价, %.2f' % self.dataclose[0])

        # 检查是否有未完成订单...如果有，则不能再发送第二个订单
        if self.order:
            return

        # 检查是否持仓
        if not self.position:

            # 尚未持仓...如果满足条件则可以买入
            if self.dataclose[0] > self.sma[0]:

                # 买入！（使用所有默认参数）
                self.log('发出买入指令, %.2f' % self.dataclose[0])

                # 记录已创建的订单，避免重复下单
                self.order = self.buy()

        else:

            if self.dataclose[0] < self.sma[0]:
                # 卖出！（使用所有默认参数）
                self.log('发出卖出指令, %.2f' % self.dataclose[0])

                # 记录已创建的订单，避免重复下单
                self.order = self.sell()


if __name__ == '__main__':
    # 创建 cerebro 实例
    cerebro = bt.Cerebro()

    # 添加策略
    cerebro.addstrategy(TestStrategy)

    # 数据文件在 samples 的子文件夹中。需要找到脚本所在路径
    # 因为脚本可能从任意位置调用
    modpath = os.path.dirname(os.path.abspath(sys.argv[0]))
    datapath = os.path.join(modpath, './datas/orcl-1995-2014.txt')

    # 创建数据源
    data = bt.feeds.YahooFinanceCSVData(
        dataname=datapath,
        # 不传递此日期之前的数据
        fromdate=datetime.datetime(2000, 1, 1),
        # 不传递此日期之后的数据
        todate=datetime.datetime(2000, 12, 31),
        # 不反转数据
        reverse=False)

    # 将数据源添加到 cerebro
    cerebro.adddata(data)

    # 设置初始资金
    cerebro.broker.setcash(1000.0)

    # 添加定额买入的资金管理
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)

    # 设置手续费
    cerebro.broker.setcommission(commission=0.0)

    # 输出初始资产
    print('初始资产: %.2f' % cerebro.broker.getvalue())

    # 运行回测
    cerebro.run()

    # 输出最终资产
    print('最终资产: %.2f' % cerebro.broker.getvalue())

    # 绘制回测结果
    cerebro.plot()
