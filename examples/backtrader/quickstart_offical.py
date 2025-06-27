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
        ('printlog', False),
    )

    def log(self, txt, dt=None, doprint=False):
        ''' 策略的日志记录函数 '''
        if self.params.printlog or doprint:
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

    def stop(self):
        self.log('(均线周期 %2d) 期末资产 %.2f' %
                 (self.params.maperiod, self.broker.getvalue()), doprint=True)


if __name__ == '__main__':
    # 创建 cerebro 实例
    cerebro = bt.Cerebro()

    # 添加策略
    strats = cerebro.optstrategy(
        TestStrategy,
        maperiod=range(10, 31))

    # 数据文件在 samples 的子文件夹中。需要找到脚本所在路径
    # 因为脚本可能从任意位置调用
    modpath = os.path.dirname(os.path.abspath(sys.argv[0]))
    datapath = os.path.join(modpath, './000001.SZ.csv')

    # 创建数据源
    data = bt.feeds.GenericCSVData(
        dataname=datapath,
        # 不传递此日期之前的数据
        fromdate=datetime.datetime(2022, 7, 1),
        # 不传递此日期之后的数据
        todate=datetime.datetime(2025, 6, 1),
        # 不反转数据
        dtformat=('%Y-%m-%d'),
        datetime=0,  # 日期所在列索引 (例如，第0列) [12, 14]
        high=1,  # High 价格所在列索引 (例如，第1列) [12, 14]
        low=2,  # Low 价格所在列索引 (例如，第2列) [12, 14]
        open=3,  # Open 价格所在列索引 (例如，第3列) [12, 14]
        close=4,  # Close 价格所在列索引 (例如，第4列) [12, 14]
        volume=5,  # Volume 所在列索引 (例如，第5列) [12, 14]
        openinterest=-1,  # Open Interest 所在列索引 (-1 表示不存在) [12, 14]
        headers=False,  # CSV 文件是否包含标题行 (默认 True) [2, 14, 15]
        separator=',',  # 分隔符 (默认 ",") [2, 14, 15]
        name='000001'

    )

    # 将数据源添加到 cerebro
    cerebro.adddata(data)

    # 设置初始资金
    cerebro.broker.setcash(1000.0)

    # 添加定额买入的资金管理
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)

    # 设置手续费
    cerebro.broker.setcommission(commission=0.0)

    # 运行回测
    cerebro.run(maxcpus=1)
