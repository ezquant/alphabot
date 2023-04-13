import os
import importlib
import vnpy
import pandas as pd
import datetime as dt
#from pytz import timezone

#from backtrader.utils import date2num, num2date
from vnpy_portfoliostrategy import BacktestingEngine
from vnpy.trader.constant import Exchange, Interval, Direction, Offset
from vnpy.trader.object import BarData, TickData
from earnmi.data.import_tradeday_from_jqdata import TRAY_DAY_VT_SIMBOL
from earnmi.strategy.StockStrategyBridge import StockStrategyBridge
from earnmi.uitl.utils import utils

from alphabot import logger
from alphabot.backend import EngineBase
from alphabot.utils import CodeProcessor, LocalTimezone, timestamp2datetime, timeframe_types
from alphabot.backend.vnpy.strategy_wrap import VnpyStrategyForJQ


class Engine(EngineBase):
    """
    基于 vnpy 框架实现的执行引擎。支持 vnpy 和聚宽这两种策略形式。
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
        Add strategy, support vnpy or Joinquant style.
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
            strat_info['cash_start'] = self._args.cfg.settings.cash_start

            # 聚宽策略上下文初始化
            self.strat_context = type('Class', (), {
                'engine': None, 'portfolio': None, 'subportfolios': None,
                'run_params': None})() # 聚宽策略上下文
            self.strat_context.engine = self
            run_params = type('Class', (), {})()
            run_params.type = self._args.cfg.settings.run_type
            self.strat_context.run_params = run_params
            strat_info['context'] = self.strat_context

            # 设置聚宽策略上下文
            strat_set_context = getattr(strat_info['_strat_module'], 'api_set_context')
            strat_set_context(strat_info['context'])
            
            # 获取策略股票池
            strat_info['all_security_pool'] = getattr(strat_info['_strat_module'], 'all_security_pool')
            
            # 聚宽策略初始化（已迁移到策略封装中，这里不再执行）
            #strat_initialize = getattr(strat_info['_strat_module'], 'initialize')
            #strat_initialize(strat_set_context)
            
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

        #run_func(init=init, before_trading=before_trading, handle_bar=handle_bar, config=CONFIG)
        start = dt.datetime.strptime(self._args.cfg.settings.fromdate, '%Y-%m-%dT%H:%M:%S')  # 交易日开始时间配置
        end = dt.datetime.strptime(self._args.cfg.settings.todate, '%Y-%m-%dT%H:%M:%S')  # 交易日结束时间配置
        capital = self._args.cfg.settings.cash_start
        slippage = None  # 将在策略中设置
        commission = None  # 将在策略中设置

        vt_symbols = []
        vt_symbols.append('000001.SSE')
        rates={}  # 交易佣金
        slippages={}  # 滑点
        sizes={}  # 一手的交易单位
        priceticks={}  # 四舍五入的精度
        # earnmi StockStrategyBridge 需要下述设置
        rates[TRAY_DAY_VT_SIMBOL] = commission
        slippages[TRAY_DAY_VT_SIMBOL] = slippage
        sizes[TRAY_DAY_VT_SIMBOL] = 100
        priceticks[TRAY_DAY_VT_SIMBOL] = 0.01
        
        for i in strat_info['all_security_pool']:
            vt_symbol = CodeProcessor.convert_jq_to_plain(i) + '.SSE'
            vt_symbols.append(vt_symbol)
            rates[vt_symbol] = commission
            slippages[vt_symbol] = slippage
            sizes[vt_symbol] = 100
            priceticks[vt_symbol] = 0.01
        print(f'--> vt_symbols: {vt_symbols}')

        self.engine = BacktestingEngine()
        self.strategy = VnpyStrategyForJQ(strat_info)
        self.engine.set_parameters(
            vt_symbols=vt_symbols,  # 如果只设为 [TRAY_DAY_VT_SIMBOL] 则计算状态时无法正确隔天传递仓位等信息
            # 时间单位，设为 earnmi 默认的 DAILY，jq策略在回测时实际仍然为分钟级
            # 如果设为 MINUTE，则会由于本地缺乏分钟级 sqlite 数据，导致回测失败
            interval=Interval.DAILY,
            start=start,
            end=end,
            rates=rates,
            slippages=slippages,
            sizes=sizes,
            priceticks=priceticks,
            capital=capital,
        )
        self.engine.add_strategy(StockStrategyBridge, {"strategy": self.strategy})

        self.engine.load_data()
        self.engine.run_backtesting()

        df = self.engine.calculate_result()
        print(f'--> calculate_result df: {df}')
        # earnmi 模式下，下述状态统计数据有明显错误，可能与策略时间单位强制设为天有关，需另外计算
        #TODO 尝试解决方案1：单独计算相关指标（历史指标数据可能仍难以实现）
        #TODO 尝试解决方案2：实现 QA gateway，支持到vnpy分钟级策略
        st = self.engine.calculate_statistics()

        print('-' * 60)
        print('total hold capital:\t', f'{self.strategy.portfolio.getTotalHoldCapital():>08.2f}'.rjust(26))
        print('final_valid_capital:\t', f'{self.strategy.final_valid_capital:>08.2f}'.rjust(26))
        print('final_total_capital:\t', f'{self.strategy.portfolio.getTotalCapital():>08.2f}'.rjust(26))
        #print(f"total_commission: \t{st.total_commission:>8,.2f}")
        print(f'strategy start_trade_time: \t{self.strategy.start_trade_time}')
        print(f'strategy end_trade_time: \t{self.strategy.end_trade_time}')
        #print('--> long position:', self.strategy.portfolio.getLongPosition("601318").pos_total)
        #print('--> short position:', self.strategy.portfolio.getShortPosition("601318").pos_total)

    def _get_data_historical_jq(self, code, time_begin, time_end, unit='1d'):
        """"
        为聚宽策略获取历史数据
        """

        ## 通过 earnmi 提供的相关函数获取数据
        #code_plain = CodeProcessor.convert_jq_to_plain(code)
        ##bar = self.strategy.market.getRealTime().getKBar(code_plain)
        #bar = self.strategy.market.getHistory().getKbarFrom(code_plain, time_begin)
        ## bar 类型为 Sequence["BarData"]
        #return bar
        
        # zzz - 实际测试时发现 earnmi 通过 jqdatasdk 获取的数据很多都是错的，基本确定是聚宽提供的数据有问题
        # 这里改为使用本地 QA/tdx 更新保存的数据

        #self._tz = timezone('UTC')  # 默认UTC时区
        #print('--> _get_data_historical ', code, time_begin, time_end, unit, self._tz)
        _data = self._get_data_historical(code, time_begin, time_end, timeframe_types[unit])
        # 注意：上述 data 为一个迭代器，主 data 会在策略时迭代读取，其他 data 需自行读取
        #if code not in self._cerebro.datasbyname:
        #    self._cerebro.adddata(_data, name=code)
        #    logger.info('add data %s of code %s' % (_data, code))
        
        ohlc = []
        for candle in _data.iter:
            #print('--> candle:', candle)
            # Generate BarData object from DbBarData.
            bar = BarData(
                symbol=code,
                exchange=Exchange('SSE'),  #TODO or SZSE, or others
                datetime=timestamp2datetime(candle['time_stamp']),
                interval=Interval('d' if unit == '1d' else unit),
                volume=candle['vol'],
                open_price=candle['open'],
                high_price=candle['high'],
                open_interest=0.0,
                low_price=candle['low'],
                close_price=candle['close'],
                gateway_name="QA",
            )

            # 构造 ohlc 数据
            ohlc.append(bar)

        #print('--> len(_data.close):', len(_data.close))
        return ohlc
        #raise(Exception('！！待实现……'))

    def plot(self):
        # 自定义显示账户价值曲线、年化收益、绩效指标等
        #self.strategy.backtestContext.showChart()  # K线图
        self.engine.show_chart()  # 净值曲线、回撤等
        pass


#if __name__ == '__main__':
#    pass
