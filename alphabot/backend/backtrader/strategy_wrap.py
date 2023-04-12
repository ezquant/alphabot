import datetime as dt
import backtrader as bt

from alphabot import logger
from alphabot.utils import timestamp2datetime, CodeProcessor, Dict2Obj


class BTStrategyForJQ(bt.Strategy):
    """
    将聚宽策略封装为BT策略
    """

    #params = (
    #    ('period', None),
    #)
    params = dict(
        # 策略自定义参数
        period=21
    )

    def log(self, txt, dt=None):
        ''' Logging function fot this strategy'''
        _dt = dt or self.datas[0].datetime.date(0)
        if type(_dt) == float:
            _dt = timestamp2datetime(_dt)
        _tm = self.datas[0].datetime.time(0)
        print('%s %s, %s' % (_dt.isoformat(), _tm.isoformat(), txt))

    def __init__(self, **strat_info):
        self._strat_info = strat_info
        self._jq_strat = strat_info['_strat_module']
        self._jq_context = self._strat_info['context']
        
        print('--> self._jq_strat:', strat_info['_strat_module'])

        # 动态获取策略参数及配置
        #self._strat_config = getattr(self._jq_strat, 'config')
        
        if strat_info['_strat_type'] == 'joinquant':
            # 设置聚宽策略上下文
            self._jq_context.strat_obj = self
            self._jq_context.portfolio = type('Class', (), {})()
            self._jq_context.portfolio.positions = {} #TODO: 使用 broker 中的 positions
            self._jq_context.portfolio.long_positions = {} #TODO: 使用 broker 中的多头仓位
            self._jq_context.portfolio.starting_cash = self._jq_context.engine._cash_start
            self._jq_context.portfolio.cash = self._jq_context.engine._cerebro.broker.cash # 使用 broker 中的 cash
            self._jq_context.portfolio.locked_cash = 0.0 #TODO: 使用 broker 中的相关数据
            self._jq_context.portfolio.positions_value = 0.0 #TODO: 使用 broker 中的持仓价值数据
            self._jq_context.portfolio.portfolio_value = 0.0 #TODO: 使用 broker 中的相关数据
            self._jq_context.portfolio.total_value = 0.0 #TODO: 使用 broker 中的相关数据
            self._jq_context.subportfolios = []
            self._jq_context.subportfolios.append(self._jq_context.portfolio)

            self._strat_before_trading_start = getattr(self._jq_strat, 'before_trading_start')
            self._strat_handle_data = getattr(self._jq_strat, 'handle_data')
            self._strat_after_trading_end = getattr(self._jq_strat, 'after_trading_end')
            
            # 聚宽策略上下文及初始化（！！必须在引擎中执行，不能放到策略包装类中执行）
            # 设置聚宽策略上下文
            strat_set_context = getattr(self._jq_strat, 'api_set_context')
            strat_set_context(self._jq_context)
            # 聚宽策略初始化
            strat_initialize = getattr(self._jq_strat, 'initialize')
            strat_initialize(self._jq_context)
        
        # 设置策略内定时器
        # 开盘前
        self._add_timer(
            when=bt.timer.SESSION_START,
            offset=dt.timedelta(minutes=-15),
            repeat=dt.timedelta()
        )
        # 收盘时（策略内定时器不支持收盘后）
        self._add_timer(
            #when=dt.time(15, 30),
            when=bt.timer.SESSION_END,
            offset=dt.timedelta(minutes=0),
            repeat=dt.timedelta()
        )
         # 自定义时间
        print('--> g.__dict__:', self.g.__dict__)
        if hasattr(self.g, 'run_daily_when'):
            print('--> add timer at', self.g.run_daily_when)
            _h, _m = self.g.run_daily_when.split(':')
            self._add_timer(
                when=dt.time(int(_h), int(_m)),
                offset=dt.timedelta(),
                repeat=dt.timedelta()
            )
        
        # Keep a reference to the "close" line in the data[0] dataseries
        self.dataclose = self.datas[0].close

        # To keep track of pending orders and buy price/commission
        self.order = None
        self.buyprice = None
        self.buycomm = None

        self.bar_count = 0  # 回测时的当前 bar 数据序号

        # Indicators for the plotting show
        # 指数均线
        bt.indicators.ExponentialMovingAverage()
        # 加权均线
        bt.indicators.WeightedMovingAverage()
        # 慢速随机指数（该指标有时会爆除零的错误，先注释掉）
        #bt.indicators.StochasticSlow()
        # 异同移动平均线
        bt.indicators.MACDHisto()
        # 相对强弱指数
        rsi = bt.indicators.RSI()
        # 平均相对强弱指数
        #bt.indicators.SmoothedMovingAverage(rsi) # 当前版本代码不接受参数 rsi
        # 平均真实波动范围
        bt.indicators.ATR(self.datas[0], plot=False)
        # 增加均线，简单移动平均线（SMA）又称“算术移动平均线”，是指对特定期间的收盘价进行简单平均化
        self.sma = bt.indicators.MovingAverageSimple()
        
    def _add_timer(self, when, offset, repeat):
        # 策略定时器相关参数
        p_timer=True
        p_cheat=False
        p_weekdays=[]
        p_weekcarry=False
        p_monthdays=[]
        p_monthcarry=True
        
        if p_timer:
            self.add_timer(
                when=when,
                offset=offset,
                repeat=repeat,
                weekdays=p_weekdays,
                weekcarry=p_weekcarry,
                monthdays=p_monthdays,
                monthcarry=p_monthcarry,
                # tzdata=self.data0,
            )
        if p_cheat:
            self.add_timer(
                when=when,
                offset=offset,
                repeat=repeat,
                weekdays=p_weekdays,
                weekcarry=p_weekcarry,
                monthdays=p_monthdays,
                monthcarry=p_monthcarry,
                # tzdata=self.data0,
                cheat=True,
            )
        
    def notify_timer(self, timer, when, *args, **kwargs):
        print('strategy notify_timer with tid {}, when {} cheat {}'.
                format(timer.p.tid, when, timer.p.cheat))

        #if self.order is None and timer.params.cheat:
        #    print('-- {} Create buy order'.format(
        #        self.data.datetime.datetime()))
        #   self.order = self.buy()
        if timer.p.tid == 0:
            self._jq_context.current_dt = when
            self._strat_before_trading_start(self._jq_context)
        elif timer.p.tid == 1:
            self._strat_after_trading_end(self._jq_context)
        elif timer.p.tid == 2:
            self.g.run_daily_func(self._jq_context)
            pass
    
    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            self.log('ORDER ACCEPTED/SUBMITTED', dt=order.created.dt)
            self.order = order
            return

        if order.status in [order.Expired]:
            self.log('BUY EXPIRED')

        # Check if an order has been completed
        # Attention: broker could reject order if not enougth cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price, order.executed.value, order.executed.comm))
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:  # Sell
                self.log('SELL EXECUTED, Price: %.2f, Cost: %.2f, Comm %.2f' %
                         (order.executed.price, order.executed.value, order.executed.comm))
            self.bar_executed = len(self)
        elif order.status in [order.Canceled, order.Rejected]:
            self.log('Order Canceled/Rejected')
        elif order.status == order.Margin:
            self.log('Order Margin')

        # Write down: no pending order
        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('OPERATION PROFIT, GROSS %.2f, NET %.2f' % (trade.pnl, trade.pnlcomm))

    def start(self):
        print("the world call me!")

    def next(self):
        '''
        _, isowk, isowkday = self.datetime.date().isocalendar()
        txt = '{}, {}, Week {}, Day {}, O {}, H {}, L {}, C {}'.format(
            len(self), self.datetime.datetime(),
            isowk, isowkday,
            self.data.open[0], self.data.high[0],
            self.data.low[0], self.data.close[0])
        print('--> bar detail:', txt)
        '''
        print(f'--> current_dt:{self.datetime.datetime()}, bar_count: {self.bar_count}')
        # 奇怪！5月6号为何是从 10:04 开始的？backtrader 这种处理逻辑是不是有问题
        # --> current_dt:2021-05-06 10:04:00, bar_count: 0
        
        # 用户策略函数处理交易数据
        #_data = {'code':self.data.p.code, 'open':self.data.open[0], 'high':self.data.high[0],
        #        'low':self.data.low[0], 'close':self.data.close[0]}
        _data = {}
        for i in self.datas:
            #print('--> datas i:', i.p.code, i.data)
            _x = Dict2Obj(i.data[self.bar_count])
            _data[i.p.code] = _x
        self.bar_count += 1
        self._jq_context.current_dt = self.datetime.datetime()
        self._strat_handle_data(self._jq_context, _data)

        # Simply log the closing price of the series from the reference
        #self.log('Close, %.2f' % self.dataclose[0])

        '''
        # Check if an order is pending ... if yes, we cannot send a 2nd one
        if self.order:
            return

        # Check if we are in the market
        if not self.position:
            # Not yet ... we MIGHT BUY if ...
            if self.dataclose[0] > self.sma[0]:
                # BUY, BUY, BUY!!! (with all possible default parameters)
                self.log('BUY CREATE, %.2f' % self.dataclose[0])
                # Keep track of the created order to avoid a 2nd order
                self.order = self.buy()
        else:
            if self.dataclose[0] < self.sma[0]:
                # SELL, SELL, SELL!!! (with all possible default parameters)
                self.log('SELL CREATE, %.2f' % self.dataclose[0])
                # Keep track of the created order to avoid a 2nd order
                self.order = self.sell()
        '''
