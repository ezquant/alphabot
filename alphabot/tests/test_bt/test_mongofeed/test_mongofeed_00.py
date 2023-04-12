from __future__ import (absolute_import, division, print_function, unicode_literals)

import datetime as dt

import backtrader as bt

from ialgotest.data_feeds.backtrader_mongo_feed import MongoFeed


# Create a Stratey
class TestStrategy(bt.Strategy):
    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        dt = dt or self.datas[0].datetime.datetime(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        pass

    def next(self):
        # Simply log the data of the series from the reference
        self.log('Open %.2f, High %.2f, Low %.2f, Close %.2f, Volume %.2f, Turn %.2f, TransNum %.2f' %
                 (self.datas[0].open[0], self.datas[0].high[0], self.datas[0].low[0], self.datas[0].close[0],
                  self.datas[0].volume[0], self.datas[0].turn[0], self.datas[0].transNum[0]))


if __name__ == '__main__':
    # Create a cerebro entity
    cerebro = bt.Cerebro()

    # Add a strategy
    cerebro.addstrategy(TestStrategy)

    # Create a Data Feed
    data = MongoFeed(database='emquant', code='000001.SZ', timeframe=bt.TimeFrame.Days,
                     fromdate=dt.datetime(2017, 3, 1, 0, 0, 0, 0))

    # Add the Data Feed to Cerebro
    cerebro.adddata(data)

    # Set our desired cash start
    cerebro.broker.setcash(100000.0)

    # Print out the starting conditions
    print('Starting Portfolio Value: %.2f' % cerebro.broker.getvalue())

    # Run over everything
    cerebro.run()

    # Print out the final result
    print('Final Portfolio Value: %.2f' % cerebro.broker.getvalue())
