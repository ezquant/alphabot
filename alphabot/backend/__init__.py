import datetime as dt
import backtrader as bt
from abc import ABCMeta, abstractmethod, abstractproperty, ABC
from pytz import timezone

from alphabot.data.feeds.mongo_feed import MongoFeed
from alphabot.utils import timeframe_types
from alphabot.utils.log import logger
from alphabot.storage.manager import StorageManager


class RunningModes():
    BACKTEST = 'backtest'
    TRADING = 'trading'


class RunningManager():

    def __init__(self, 
                running_mode: dict(type=RunningModes, help='set backtest or trading'),
                args):
        #logger.info('running mode: %s', running_mode)

        # Create storage manager
        storage = StorageManager(storage_type=args.cfg.settings.storage, 
                                **eval('dict(' + args.cfg.settings.storage_params + ')'))
        logger.info('Storage => %s', storage)
        
        self._run_env = {
            'storage': storage,
            'running_mode': running_mode,
            'args': args
        }
        
        # 导入参数指定的第三方执行引擎，如 rqalpha/vnpy/backtrader
        module_path = 'alphabot.backend.%s.engine' % args.cfg.settings.engine
        #print('--> module_path:', module_path)
        module_name = __import__(module_path,
                                fromlist=['Engine'])
        Engine = getattr(module_name, 'Engine')
        self.engine = Engine(run_env=self._run_env)

    def run(self):
        self.engine.run()
        self.engine.plot()


class EngineBase(ABC):
    """
    执行引擎抽象类，需要基于 backtrader/vnpy/rqalpha 等具体实现之
    """

    def __init__(self, run_env):
        #logger.info('running mode: %s', running_mode)
        self._storage = run_env['storage']
        self._running_mode = run_env['running_mode']
        self._args = run_env['args']
        self._timeframe = timeframe_types[self._args.cfg.settings.timeframe]
        dt_from, dt_to = self._parse_from_to_args()
        self.from_datetime, self.to_datetime = dt_from, dt_to
        #self._delta_hours = 8 # 默认中国时区
        #self._tz = LocalTimezone(self._delta_hours)
        self._tz = timezone('Asia/Shanghai') # 默认中国时区
        self._cash_start = 100000 #TODO: 从 args 中获取 

    @abstractmethod
    def get(self):
        pass
 
    @abstractproperty
    def pro(self):
        pass

    @abstractmethod
    def run(self):
        pass

    @abstractmethod
    def plot(self):
        pass

    def _parse_from_to_args(self):
        """解析命令行参数中的获取数据时间区间"""
        dtfmt, tmfmt = '%Y-%m-%d', 'T%H:%M:%S'
        _t = {'fromdate': None, 'todate': None}
        for a, d in ((getattr(self._args.cfg.settings, x), x) for x in ['fromdate', 'todate']):
            if a:
                strpfmt = dtfmt + tmfmt * ('T' in a) # 'e.g., 2020-06-01T09:00:00'
                _t[d] = dt.datetime.strptime(a, strpfmt)
        time_slot = tuple(_t[x] for x in ['fromdate', 'todate'])
        return time_slot

    def _get_data_historical(self, code, time_begin, time_end, timeframe):
        """
        根据过滤条件获取交易历史数据
        """
        kwargs = dict(
            timeframe=timeframe,
            compression=1, #TODO 压缩比，mongodb feed 是否支持？
            sessionstart=dt.time(9, 30),
            sessionend=dt.time(15, 0),
        )
        
        _data = None
        if self._args.cfg.settings.storage == 'cvsfile':
            # Create a CSV Data Feed（cvs feed 不接受时间和代码过滤？）
            _data = bt.feeds.BacktraderCSVData(dataname=self._args.cfg.settings.storage_params, **kwargs)
        elif self._args.cfg.settings.storage == 'mongodb':
            # Create a Mongo Data Feed
            _data = MongoFeed(storage=self._storage, dataname=code, code=code, 
                            time_begin=time_begin, time_end=time_end, 
                            tz=self._tz, **kwargs)
            _data.start() # 执行后才会读取到内存
        
        return _data

