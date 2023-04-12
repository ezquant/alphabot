from __future__ import (absolute_import, division, print_function, unicode_literals)

import time
from datetime import datetime
import backtrader as bt
import backtrader.feed as feed
from backtrader.utils import date2num
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure
from alphabot.utils import timestamp2datetime
from alphabot import logger


class MongoFeed(feed.DataBase):
    lines = (('turn'), ('transNum'),)

    params = (
        ('storage', None),
        ('code', None),
        ('time_begin', None), # Datetime
        ('time_end', None), # Datetime
        ('timeframe', bt.TimeFrame.Days), # default
        ('tz', None),
        #('delta_hours', 8), # 默认中国时区
    )

    def __init__(self, **kwargs):
        super(MongoFeed, self).__init__(**kwargs)
        # name of the table is indicated by dataname

        # iterator 4 data in the list
        self.iter = None
        self.data = None

    def start(self):
        super().start()
        if self.data is None:
            #print(f'--> self.p.time_begin: {self.p.time_begin}, type: {type(self.p.time_begin)}')
            if self.p.tz is not None and type(self.p.time_begin) == datetime:
                time_begin = self.p.time_begin.replace(tzinfo=self.p.tz)
                time_end = self.p.time_end.replace(tzinfo=self.p.tz)
            else:
                time_begin = self.p.time_begin
                time_end = self.p.time_end
            #logger.info('[MongoFeed] read %s, %s - %s' % (self.p.code, time_begin, time_end))
            _storage = self.p.storage.storage
            bars = _storage.get_bars(self.p.code, time_begin, time_end, self.p.timeframe)
            self.data = list(bars)

        # set the iterator anyway
        self.iter = iter(self.data)

    def stop(self):
        pass

    def _load(self):
        if self.iter is None:
            # if no data ... no parsing
            return False

        # try to get 1 row of data from iterator
        try:
            row = next(self.iter)
        except StopIteration:
            # end of the list
            return False

        # fill the lines (self.lines or self.l)
        #print('--> row:', row)
        #self.lines.datetime[0] = date2num(
        #        timestamp2datetime(row['time_stamp'], self.p.delta_hours))
        _datetime = timestamp2datetime(row['time_stamp'])
        #if self.p.tz is not None:
        #    #print('--> self.p.tz.utcoffset(_datetime):', self.p.tz.utcoffset(_datetime))
        #    _datetime = _datetime.replace(tzinfo=self.p.tz)
        #    #print('--> _datetime:', _datetime)
        self.lines.datetime[0] = date2num(_datetime)
        self.lines.open[0] = row['open']
        self.lines.high[0] = row['high']
        self.lines.low[0] = row['low']
        self.lines.close[0] = row['close']
        self.lines.volume[0] = row['vol']
        self.lines.openinterest[0] = -1
        #self.lines.turn[0] = row['turn'] or -1
        #self.lines.transNum[0] = row['transNum'] or -1

        # Say success
        return True
