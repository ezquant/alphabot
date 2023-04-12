import os
import importlib
import rqalpha
import pandas as pd
import datetime as dt
from pytz import timezone

from alphabot import logger
from alphabot.backend import EngineBase
from alphabot.data.feeds.mongo_feed import MongoFeed
from alphabot.data.feeds.cvs_feed import CSVFeed
from alphabot.storage.manager import StorageManager
from alphabot.utils import timeframe_types
from alphabot.utils import LocalTimezone, timestamp2datetime


class Engine(EngineBase):
    """
    基于 rqalpha 框架实现的执行引擎。支持 rqalpha 和聚宽这两种策略形式。
    """

    def __init__(self, run_env):
        #logger.info('running mode: %s', running_mode)
        super(Engine, self).__init__(run_env)

    def get(self):
        pass
 
    def pro(self):
        pass

    def _set_strategy(self):
        """
        Add strategy, support rqalpha or Joinquant style.
        """
        # 动态加载用户策略文件
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
            
            # 创建聚宽策略上下文
            self.strat_context = type('Class', (), {
                'engine': None, 'portfolio': None, 'subportfolios': None,
                'run_params': None})() # 聚宽策略上下文
            self.strat_context.engine = self
            run_params = type('Class', (), {})()
            run_params.type = self._args.cfg.settings.run_type
            self.strat_context.run_params = run_params
            strat_info['context'] = self.strat_context

            # 聚宽策略上下文及初始化（！！必须在引擎中执行，不能放到策略包装类中执行）
            # 设置聚宽策略上下文
            strat_set_context = getattr(strat_info['_strat_module'], 'api_set_context')
            strat_set_context(strat_info['context'])
            
            # 加载该策略所用到的全部数据（必须在本代码中加载，若策略内加载则运行时会报错）
            #_code_list = getattr(_strat_module, 'all_security_pool')
            #logger.info('set datas from all_security_pool: %s' % _code_list)
        else:
            #strat_info['_strat_type'] = 'rqalpha' # RQ 策略不需要指定该参数
            pass
        
        return strat_info

    def run(self):
        """
        根据运行模式动态执行策略，包括运行引擎初始化，添加数据、设置策略代码、参数等
        TODO: 回测用矢量模式，交易用事件模式？
        """
        #logger.info('Starting Portfolio Value: %.2f' % broker.getvalue())
        
        # 其他设置
        print('--> self._args:', self._args)
        if 'joinquant' in self._args.cfg.settings.strategy_file: # joinquant style
            # 聚宽策略所需数据已在策略设置函数中加载
            pass
        else: # rqalpha style
            #TODO 按照 rqalpha 指定方式设置数据源、费率、滑点及基准
            pass

        # Strategy
        strat_info = self._set_strategy()
        print('--> strat_info:', strat_info)
        strat = strat_info['_strat_module']

        run_type = None  # b: backtesting or p: paper trading or r: live trading
        if self._args.cfg.settings.run_type == "backtest":
            run_type = "b"
        elif self._args.cfg.settings.run_type == "sim_trade":
            run_type = "p"
        elif self._args.cfg.settings.run_type == "live_trade":
            run_type = "r"
        else:
            raise Exception("不支持的运行模式：%s" % self._args.cfg.settings.run_type)
        self._run_config = {
            "base": {
                "start_date": "20200330",  # available_data_range: 2004-12-01 - 2020-05-06
                "end_date": "20200415",
                "frequency": "1d",
                "benchmark": "000300.XSHG",
                "accounts": {
                    "STOCK": 10e6
                },
                "run_type": run_type,
            },
            "extra": {
                "log_level": "info" # verbose debug info error
            },
            "mod": {
                "sys_analyser": {
                    "enabled": True,
                    # "report_save_path": ".",
                    "plot": True,
                    "benchmark": "000300.XSHG",
                    "matching_type": "last"
                },
                "mctrader": {
                    "enabled": False,
                    "data_source": "tushare_pro",
                    "tushare_tokens": [],
                    "sid": "zzz_20210401",
                    'should_resume': True,
                    'should_run_init': True,
                    'log_file': 'mctrader.log',
                    'persist_dir': 'mctrader_persist',
                    "broker": 'thsauto://127.0.0.1:5000'
                }
            }
        }
        
        #run_func(init=init, before_trading=before_trading, handle_bar=handle_bar, config=CONFIG)
        rqalpha.run_func(init=strat.initialize, 
                before_trading=strat.before_trading_start, 
                after_trading_end=strat.after_trading_end,
                handle_bar=strat.handle_data,
                after_code_changed=strat.after_code_changed,
                config=self._run_config)

    def plot(self):
        ##TODO 自定义显示账户价值曲线、年化收益、绩效指标等
        pass


#if __name__ == '__main__':
#    pass
