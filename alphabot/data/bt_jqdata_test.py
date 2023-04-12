import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import backtrader as bt
import datetime
from jqdatasdk import *


def finance_balance_sheet():
    stock_sheet = Stock('000905.XSHG', '2019-04-23', '2011-01-01')
    stock_sheet.plot_data()

    # stock_data.to_csv('egg.csv')


class Stock:
    def __init__(self, stock_id, stock_end_date, stock_start_date='2011-01-01'):
        self.stock_id = stock_id
        self.stock_start_date = stock_start_date
        self.stock_end_date = stock_end_date
        self.stock_data = self.stock_query()
        self.stock_data.fillna(0, inplace=True)
        # self.stock_data.to_csv('000905.csv')

    def stock_query(self):
        return get_price(self.stock_id, start_date=self.stock_start_date,
                         end_date=self.stock_end_date)

    def write_to_csv(self, filepath, filename, filetpye='.csv'):
        self.stock_data.to_csv(filepath + filename + filetpye)

    def plot_data(self):
        stock_data_m5 = self.stock_data['close'].rolling(window=5).mean()
        stock_data_m10 = self.stock_data['close'].rolling(window=10).mean()
        stock_data_m60 = self.stock_data['close'].rolling(window=60).mean()

        # stock_data_mean = pd.DataFrame([stock_data_m5,stock_data_m10,stock_data_m60])

        # stock_data_mean.plot()
        #
        # plt.show()

        stock_data_m60.plot(color='r')
        # stock_data_m10.plot()
        stock_data_m5.plot(color='b')

        plt.show()


# class MySignal(bt.Indicator):
#     lines = ('signal',)
#     params = (('period', 30),)
#
#     def __init__(self):
#         self.lines.signal = self.data - bt.indicators.SMA(period=self.p.period)
#

class ssa_index_ind(bt.Indicator):
    lines = ('ssa',)

    def __init__(self,ssa_window):
        self.params.ssa_window = ssa_window

        self.addminperiod(self.params.ssa_window * 2)

    def get_window_matrix(self,input_array,t,m):

        temp = []

        n = t - m + 1

        for i in range(n):
            temp.append(input_array[i:i+m])

        window_matrix = np.array(temp)

        return window_matrix

    def svd_reduce(self,window_matrix):

        # svd 分解
        u, s, v = np.linalg.svd(window_matrix)
        m1,n1 = u.shape
        m2,n2 = v.shape
        index = s.argmax()

        u1 = u[:,index]
        v1 = v[index]
        u1 = u1.reshape((m1,1))
        v1 = v1.reshape((1,n2))

        value = s.max()

        new_matrix = value * (np.dot(u1,v1))

        return new_matrix

    def recreate_array(self,new_matrix,t,m):

        ret = []
        n = t - m + 1
        for p in range(1,t+1):
            if p < m:
                alpha = p
            elif p > t - m + 1:
                alpha = t - p + 1
            else:
                alpha = m
            sigma = 0

            for j in range(1,m + 1):
                i = p - j + 1
                if i > 0 and i < n + 1:
                    sigma += new_matrix[i - 1][j - 1]

            ret.append(sigma/alpha)

        return ret

    def SSA(self,input_array,t,m):
        window_matrix = self.get_window_matrix(input_array,t,m)
        new_matrix = self.svd_reduce(window_matrix=window_matrix)
        new_array = self.recreate_array(new_matrix,t,m)

        return new_array

    def next(self):

        data_serial = self.data.get(size = self.params.ssa_window * 2)
        self.lines.ssa[0] = self.SSA(data_serial,len(data_serial),int(len(data_serial)/ 2))[-1]



class TestStrategy(bt.Strategy):
    params = (
        ('ssa_window',15),
         ('mapperiod', 15
              ),)

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close

        self.order = None
        self.buyprice = None
        self.buycomm = None


        self.ssa = ssa_index_ind(ssa_window=self.params.ssa_window,subplot = False)

        self.sma = bt.indicators.SimpleMovingAverage(
            self.datas[0], period=self.params.mapperiod
        )

    def start(self):
            print("the world call me!")

    def prenext(self):

            print("no mature")

    def notify_order(self, order):
            if order.status in [order.Submitted, order.Accepted]:
                # Buy/Sell order submitted/accepted to/by broker - Nothing to do
                return

            if order.status in [order.Completed]:
                if order.isbuy():
                    self.log("BUY EXECUTED,Price %.2f,Cost %.2f,Comm %.2f" % (order.executed.price,
                                                    order.executed.value,
                                                    order.executed.comm))
                    self.buyprice = order.executed.price
                    self.buycomm = order.executed.comm


                else:
                    self.log("SELL EXCUTED,Price %.2f,Cost %.2f,Comm %.2f" % (order.executed.price,
                                                    order.executed.value,
                                                    order.executed.comm))

                self.bar_executed = len(self)


            elif order.status in [order.Canceled, order.Margin, order.Rejected]:
                self.log("Order Canceled/Margin/Rejected")

            self.order = None
            # Check if an order has been completed
            # Attention: broker could reject order if not enougth cash

    def notify_trade(self, trade):
        if not trade.is_closed:

            return

        self.log('OPERATION PROFIT GROSS %.2f NET  %.2f' %(trade.pnl,trade.pnlcomm))

    # def next(self):
    #
    #     self.log('Close: %.2f' % self.dataclose[0])
    #
    #     if (self.dataclose[0] < self.dataclose[-1]):
    #
    #         if (self.dataclose[-1] < self.dataclose[-2]):
    #
    #
    #             self.log('BUY CREATE %.2f' % self.dataclose[0])
    #
    #             self.buy()
    def next(self):

        # Simply log the closing price of the series from the reference
        self.log('Close, %.2f' % self.dataclose[0])

        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # Check if we are in the market
        if not self.position:

            # Not yet ... we MIGHT BUY if ...
            # if self.dataclose[0] < self.dataclose[-1]:
                # current close less than previous close

                # if self.dataclose[-1] < self.dataclose[-2]:
                    # previous close less than the previous close

                    # BUY, BUY, BUY!!! (with default parameters)
            if self.dataclose[0] > self.ssa[0]:
                self.log('BUY CREATE, %.2f' % self.dataclose[0])

                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy()

        else:

            # Already in the market ... we might sell
            # if len(self) >= (self.bar_executed + 5):
                # SELL, SELL, SELL!!! (with all possible default parameters)
            if self.dataclose[0] < self.ssa[0]:
                self.log('SELL CREATE, %.2f' % self.dataclose[0])
                self.order = self.sell()
                # Keep track of the created order to avoid a 2nd order

    def stop(self):
        print("Death")




def data_stock_get(stock_id,stock_end_date,stock_start_date='2011-1-1'):

    auth('18380152997','2wsx3edc')

    stock_Adata = Stock(stock_id=stock_id,stock_end_date=stock_end_date,stock_start_date=stock_start_date)

    return stock_Adata.stock_data


if __name__ == '__main__':
    # auth('18380152997','2wsx3edc')
    # finance_balance_sheet()

    cerebro = bt.Cerebro()

    cerebro.addstrategy(TestStrategy)

    # data_000905 = pd.read_csv('000905.csv', index_col=0, parse_dates=True)
    # print(data_000905)

    dataframe = pd.read_csv('0009051.csv', index_col=0, parse_dates=True)
    dataframe['openinterest'] = 0
    data = bt.feeds.PandasData(dataname=dataframe,
                               fromdate = datetime.datetime(2011, 1, 1),
                               todate = datetime.datetime(2019, 12, 31))
    # Add the Data Feed to Cerebro
    cerebro.adddata(data)

    cerebro.broker.setcash(100000000.00)

    cerebro.broker.setcommission(commission=0.0)

    cerebro.addsizer(bt.sizers.FixedSize,stake = 10)


    #cerebro.addsizer(bt.sizers.FixedSize,staker = 10 )
    # staker 没有这个参数
    # cerebro settting

    cerebro.run()

    print('Final Protfolio Value:%2f' % cerebro.broker.getvalue())

    cerebro.plot()
