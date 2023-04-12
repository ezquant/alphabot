import os
import importlib
import pandas as pd
import datetime as dt
import backtrader as bt
from pytz import timezone

from backtrader.utils import date2num, num2date
from backtrader_plotting import Bokeh
from backtrader_plotting.schemes import Tradimo

from alphabot import logger
from alphabot.backend import EngineBase
from alphabot.data.feeds.cvs_feed import CSVFeed
from alphabot.storage.manager import StorageManager
from alphabot.utils import timeframe_types
from alphabot.utils import LocalTimezone, timestamp2datetime


class Engine(EngineBase):
    """
    基于 backtrader 框架所实现的执行引擎。
    支持 backtrader 和聚宽这两种策略形式。
    """

    def __init__(self, run_env):
        #logger.info('running mode: %s', running_mode)
        super(Engine, self).__init__(run_env)

        self._cerebro = bt.Cerebro()
        self.__benchFeed = None

    def get(self):
        pass
 
    def pro(self):
        pass

    def _get_data_historical_jq(self, code, time_begin, time_end, unit='1d'):
        """"
        为聚宽策略获取历史数据
        """
        #print(('--> _get_data_historical ', code, time_begin, time_end, unit, self._tz))
        _data = self._get_data_historical(code, time_begin, time_end, timeframe_types[unit])
        # 注意：上述 data 为一个迭代器，主 data 会在策略时迭代读取，其他 data 需自行读取
        #if code not in self._cerebro.datasbyname:
        #    self._cerebro.adddata(_data, name=code)
        #    logger.info('add data %s of code %s' % (_data, code))
        
        ohlc = []
        for candle in _data.iter:
            #print('--> candle:', candle)
            # 由于聚宽的数据加载是在策略初始化之后调用的，_load() 不会执行到，导致 _data 中无数据。
            # 所以，为保证后续可正常访问 _data.close[0]，这里得补充执行：
            _dt_num = date2num(timestamp2datetime(candle['time_stamp']))
            _data.datetime.forward(_dt_num)
            _data.open.forward(candle['open'])
            _data.high.forward(candle['high'])
            _data.low.forward(candle['low'])
            _data.close.forward(candle['close'])
            _data.volume.forward(candle['vol'])
            _data._tz = self._tz
            # 构造 ohlc 数据
            ohlc.append([num2date(_dt_num),
                        candle['open'],
                        candle['high'],
                        candle['low'],
                        candle['close'],
                        candle['vol']])

        #print('--> len(_data.close):', len(_data.close))
        #print('--> _data.l.close[0]:', _data.l.close[0])
        #print('--> _data.close[0]:', _data.close[0])

        df = pd.DataFrame(ohlc, columns=['datetime', 'open', 'high', 'low', 'close', 'vol'])
        return df

    def _cerebro_add_data(self, code):
        """
        读取和添加指定时段历史数据 for one code
        """
        print(f'==> _cerebro_add_data() {code} {self.from_datetime} {self.to_datetime} {self._timeframe}')
        data = self._get_data_historical(code, self.from_datetime, self.to_datetime, self._timeframe)
        
        # Add the Data Feed to Cerebro
        self._cerebro.adddata(data, name=code)

    def _cerebro_set_datas(self, code_list):
        """
        读取和添加指定时段历史数据 for a code list
        """
        for code in code_list:
            self._cerebro_add_data(code)

    def _cerebro_set_benchmark(self, benchmark_code='000300.XSHG'):
        self.__benchFeed = self._get_data_historical(benchmark_code, self.from_datetime, self.to_datetime, self._timeframe)
        logger.info('set benchmark: %s, %s' % (benchmark_code, self.__benchFeed))
        self._cerebro.adddata(self.__benchFeed)
        #TODO 以下调用，若指定 data 参数，则执行时会报错，为何？
        #self._cerebro.addobserver(bt.observers.Benchmark, data=self.__benchFeed, timeframe=self._timeframe)
        #self._cerebro.addobserver(bt.observers.Benchmark, data=self.__benchFeed, timeframe=bt.TimeFrame.NoTimeFrame)
        self._cerebro.addobserver(bt.observers.Benchmark, timeframe=bt.TimeFrame.NoTimeFrame)
        #self._cerebro.addobserver(bt.observers.TimeReturn, timeframe=bt.TimeFrame.NoTimeFrame)

    def _cerebro_set_strategy(self):
        """
        Add strategy, support Backtrader or Joinquant style.
        """
        # 动态加载用户策略文件
        #print('--> strategy_file:', self._args.cfg.settings.strategy_file)
        #user_strat_dir="%s/user_strategies" % (os.getenv('XXX') if os.getenv('XXX') is not None else os.curdir())
        _strat_path = 'user_strategies.' \
            + self._args.cfg.settings.strategy_file.split('.')[0].replace('/', '.')
        #print('--> _strat_path:', _strat_path)
        _strat_module = importlib.import_module(_strat_path, package='alphabot')
        #print('--> _strat_module:', _strat_module)
        # 获取从参数传入的聚宽策略配置及模块方法等
        
        strat_info = eval('dict(' + self._args.cfg.settings.strategy_params + ')')
        
        if 'joinquant' in self._args.cfg.settings.strategy_file: # joinquant strategy
            strat_info['_strat_type'] = 'joinquant'
            strat_info['_strat_module'] = _strat_module
            
            # 聚宽策略上下文初始化
            self.strat_context = type('Class', (), {
                'engine': None, 'portfolio': None, 'subportfolios': None,
                'run_params': None})() # 聚宽策略上下文
            self.strat_context.engine = self
            self.run_params = type('Class', (), {})()
            self.run_params.type = self._args.cfg.settings.run_type
            self.strat_context.run_params = self.run_params
            strat_info['context'] = self.strat_context

            # 加载该策略所用到的全部数据（必须在本代码中加载，若策略内加载则运行时会报错）
            _code_list = getattr(_strat_module, 'all_security_pool')
            logger.info('set datas from all_security_pool: %s' % _code_list)
            self._cerebro_set_datas(_code_list)
        else:
            #strat_info['_strat_type'] = 'backtrader' # BT 策略不需要指定该参数
            pass
        
        _strategy = getattr(_strat_module, 'BTStrategyForJQ')
        print('--> _strategy:', _strategy)
        self._cerebro.addstrategy(_strategy, **strat_info)

    def run(self):
        """
        根据运行模式动态执行策略，包括运行引擎初始化，添加数据、设置策略代码、参数等
        TODO: 回测用矢量模式，交易用事件模式？
        """
        logger.info('Starting Portfolio Value: %.2f' % self._cerebro.broker.getvalue())
        
        # Broker
        self._cerebro.broker = bt.brokers.BackBroker(**eval('dict(' + self._args.cfg.backtrader.broker + ')'))
        self._cerebro.broker.setcash(self._cash_start)

        # Sizer
        self._cerebro.addsizer(bt.sizers.FixedSize, **eval('dict(' + self._args.cfg.backtrader.sizer + ')'))

        # Analyzer
        self._cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='SharpeRatio', riskfreerate=0.02, stddev_sample=True, annualize=True)
        self._cerebro.addanalyzer(bt.analyzers.Returns, _name="Returns")
        self._cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='AnnualReturn')
        self._cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DrawDown')
        self._cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='TradeAnalyzer')
        self._cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="TA")
        self._cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="TR")
        self._cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="TR_Bench", data=self.__benchFeed)
        self._cerebro.addanalyzer(bt.analyzers.SQN, _name="SQN")

        # Cerebro 其他设置
        if 'joinquant' in self._args.cfg.settings.strategy_file: # joinquant style
            # 聚宽策略所需数据已在策略设置函数中加载
            pass
        else: # backtrader style
            # Feed
            self._cerebro_add_data('000001.XSHG') #TODO: 根据策略参数加载
            # Commission
            self._cerebro.broker.setcommission(commission=0.001)
            # Slippage
            self._cerebro.broker.set_slippage_fixed(0.02)
            # Benchmark
            self._cerebro_set_benchmark('000300.XSHG')

        # Strategy
        self._cerebro_set_strategy()

        self._cerebro.run(**eval('dict(' + self._args.cfg.backtrader.cerebro + ')'))

        logger.info('Final Portfolio Value: %.2f' % self._cerebro.broker.getvalue())

    def plot(self):
        #self._cerebro.plot(numfigs=5, iplot=False)
        #b = Bokeh(style='bar', plot_mode='single', scheme=Tradimo())
        #self._cerebro.plot(b, iplot=False, **eval('dict(' + self._args.cfg.backtrader.plot + ')'))
        #TODO 自定义显示账户价值曲线、年化收益、绩效指标等
        pass


#if __name__ == '__main__':
#    pass
