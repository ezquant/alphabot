import datetime as dt

from vnpy.app.cta_strategy import StopOrder
from vnpy.app.portfolio_strategy import BacktestingEngine
from vnpy.trader.constant import Interval, Direction, Offset
from vnpy.trader.object import OrderData, TradeData, TickData, BarData
from earnmi.data.MarketImpl import MarketImpl
from earnmi.data.import_tradeday_from_jqdata import TRAY_DAY_VT_SIMBOL
from earnmi.strategy.StockStrategyBridge import StockStrategyBridge
from earnmi.strategy.StockStrategy import StockStrategy, Portfolio
from earnmi.uitl.utils import utils

from alphabot import logger
from alphabot.utils import timestamp2datetime, CodeProcessor, Dict2Obj


class VnpyStrategyForJQ(StockStrategy):

    market_open_count = 0
    start_trade_time = None
    end_trade_time = None
    final_valid_capital = 0
    portfolio: Portfolio = None
    daily_pre_tick: TickData = None
    bar_datas = None

    def __init__(self, strat_info):
        self._strat_info = strat_info
        self._jq_strat = strat_info['_strat_module']
        self._jq_context = self._strat_info['context']

    def on_create(self):
        """
        策略初始化.
        """
        # 聚宽策略：策略初始化。根据代码逻辑需求，在策略执行前再初始化
        self._jq_strat.initialize(self._jq_context)
        
        self.market_open_count = 0
        self.market = MarketImpl()

        # 数据访问代码注册
        self.market.addNotice("000001")
        for i in self._jq_strat.all_security_pool:
            code = CodeProcessor.convert_jq_to_plain(i)
            self.market.addNotice(code)

        if (not self.backtestContext is None):
            # 从网络上面准备数据
            self.write_log(f"on_create from backtestEngine, start={self.backtestContext.start_date},end={self.backtestContext.end_date}")
        else:
            self.write_log("on_create")

    def on_destroy(self):
        """
        决策结束.（被停止之后）
        """
        self.write_log("on_destroy")

    def on_market_prepare_open(self, portfolio: Portfolio, today: dt.datetime):
        """
        市场准备开始（比如：竞价）.
        """
        # 聚宽策略：进程初始化。聚宽每天都会重启进程，这里使用开盘前回调进行模拟
        self._jq_strat.process_initialize(self._jq_context)

        # 策略刚开始时，需要使用 portfolio 对聚宽上下文做赋值
        if self.portfolio is None:  # 策略第一天开始
            self.portfolio = portfolio

            self._jq_context.strat_obj = self

            # 设置聚宽策略上下文中的资产信息
            self._jq_context.portfolio = type('Class', (), {})()
            self._jq_context.portfolio.long_positions = {}
            #TODO 支持空头仓位（目前强制等同于多头仓位，仅适用国内股市）
            self._jq_context.portfolio.positions = self._jq_context.portfolio.long_positions
            self._jq_context.portfolio.starting_cash = self._strat_info['cash_start']  # 初始资金
            self._jq_set_portfolio(self.portfolio)
            self._jq_context.subportfolios = [self._jq_context.portfolio]

        self._jq_context.current_dt = today
        self._jq_strat.before_trading_start(self._jq_context)
        
        if(self.start_trade_time is None) :
            self.start_trade_time = today
        self.end_trade_time = today

        #self.daily_pre_tick = None

    def on_market_open(self, portfolio:Portfolio):
        """
        市场开市.
        """
        self.market_open_count = self.market_open_count + 1
        self.on_bar_per_minute_count = 0

        # 刚刚开始，没有任何持仓
        #portfolio.sell("601318", 67.15, 100)
        #portfolio.cover("601318", 67.15, 100)

        #self.daily_pre_tick = self.market.getRealTime().getTick("601318")

        #portfolio.buy("601318", 66.10, 1)
        #portfolio.sell("601318", 68.54, 500)
        #portfolio.short("601318", 68.54, 10)

        #position = portfolio.getLongPosition("601318")
        #position.getPosAvailable()

        # 清空持仓
        #longPos = portfolio.getLongPosition("601318")
        #shortPos= portfolio.getShortPosition("601318")
        #portfolio.cover("601318", 83.77, shortPos.pos_total/100)
        #portfolio.sell("601318", 81.35, longPos.pos_total/100)

    def on_market_prepare_close(self, portfolio:Portfolio):
        """
        市场准备关市.
        """
        time = self.market.getToday()
        assert time.hour == 14 and time.minute== 57

        # 收市前做点啥
        pass

    def on_market_close(self, portfolio:Portfolio):
        """
        市场关市.
        """
        time = self.market.getToday()
        assert time.hour == 15 and time.minute == 0

        assert self.on_bar_per_minute_count > 200

        self.final_valid_capital = portfolio.getValidCapital()
        portfolio.getTotalHoldCapital()

        # 更新聚宽持仓最新价格
        for i in portfolio.getLongPositions():
            pos = portfolio.getLongPosition(i)
            #print(f'--> {i} pos.pos_total: {pos.pos_total}')
            #print(f'--> {i} available position: {pos.getPosAvailable()}'), 
            #self._jq_set_position(pos.code)  # 全部更新，暂不需要
            code = CodeProcessor.convert_jq(i)
            if code in self._jq_context.portfolio.long_positions:
                pos_jq = self._jq_context.portfolio.long_positions[code]
                if pos_jq.total_amount <= 0:
                    self._jq_context.portfolio.long_positions.pop(code)
                else:
                    pos_jq.price = self.market.getRealTime().getTick(i).close_price  # 最新行情价格
                    #print(f'--> jq {code} position: {pos_jq}')

        # 聚宽策略：收市回调
        self._jq_strat.after_trading_end(self._jq_context)

    def on_bar_per_minute(self, time: dt.datetime, portfolio:Portfolio):
        """
        市场开市后的每分钟。
        """
        self.on_bar_per_minute_count = self.on_bar_per_minute_count + 1

        if(time.hour > 9 or (time.hour==9 and time.minute > 30)):
            # 开市之后的实时信息不应该为none
            bar = self.market.getRealTime().getKBar("000001")
            assert not bar is None

            tickData: BarData = self.market.getRealTime().getTick("000001")
            assert not tickData is None
            
            #preTickData: BarData = self.daily_pre_tick
            #assert not preTickData is None
            #deltaFloat = preTickData.close_price * 0.015
            #assert utils.isEqualFloat(preTickData.close_price, tickData.open_price, deltaFloat)

            self.bar_datas = {}
            for i in self._jq_strat.all_security_pool:
                code = CodeProcessor.convert_jq_to_plain(i)
                tickData = self.market.getRealTime().getTick(code)
                self.bar_datas[i] = type('Class', (), {})()
                self.bar_datas[i].open = tickData.open_price
                self.bar_datas[i].high = tickData.high_price
                self.bar_datas[i].low = tickData.low_price
                self.bar_datas[i].close = tickData.close_price
            
            # 聚宽策略：市场进行时回调
            self._jq_context.current_dt = time
            self._jq_strat.handle_data(self._jq_context, self.bar_datas)

            self._jq_set_portfolio(self.portfolio)
            #print(f'--> on_bar_per_minute() 当前资金总市值：{self._jq_context.portfolio.portfolio_value} {self._jq_context.portfolio.total_value}')
        
        #self.write_log(f"     on_bar_per_minute:{time}" )
        
        #self.daily_pre_tick = self.market.getRealTime().getTick("601318")
        #
        #if time.hour==9 and time.minute == 32:
        #    print('--> self.daily_pre_tick:', self.daily_pre_tick)
        #
        #    # 低位买进
        #    portfolio.buy("601318", self.daily_pre_tick.close_price, 20)
        #
        #    # 高位卖出
        #    #portfolio.sell("601318", 68.57, 20)
        #    
        #    # 以指定价格平仓
        #    #portfolio.cover("601318", 67.3, 10)

        #today = self.market.getToday()
        #today >= datetime(2019,3,2,0) and today <= datetime(2019,3,20,0):
        #    happen = random.random()
        #    if happen <= 0.1:
        #        self.__randomTrade(portfolio)

    #def __randomTrade(self,portfolio:Portfolio):
    #    happen = random.random()
    #    code = "601318"
    #    price = self.market.getRealTime().getTick(code).close_price
    #    trade_price = price * random.uniform(0.94, 1.06)
    #    volume = random.randint(3, 100)
    #    if happen <= 0.25:
    #        portfolio.buy(code, trade_price, volume)
    #    elif happen<=0.5:
    #        portfolio.sell(code, trade_price, volume)
    #    elif happen<=0.75:
    #        portfolio.short(code, trade_price, volume)
    #    else:
    #        portfolio.cover(code, trade_price, volume)

    def on_order(self, order: OrderData):
        #print(f"{self.market.getToday()}：onOrder: {order}")
        pass

    def on_trade(self, trade: TradeData):
        print(f"--> {self.market.getToday()} on_trade: {trade}")
        
        # 当前交易状态
        is_buy = trade.direction == Direction.LONG and trade.offset == Offset.OPEN
        is_sell = trade.direction == Direction.SHORT and trade.offset == Offset.CLOSE
        is_short = trade.direction == Direction.SHORT and trade.offset == Offset.OPEN
        is_cover = trade.direction == Direction.LONG and trade.offset == Offset.CLOSE

        if is_buy:
            pass
        
        # 聚宽策略：调仓记录回调
        #self._jq_strat.g.strategy.g.context = self._jq_context
        #self._jq_strat.g.strategy.after_adjust_end(self._jq_context, None)

        # 更新聚宽仓位信息
        self._jq_set_position(trade.symbol)

    def on_stop_order(self, stop_order: StopOrder):
        print(f"{self.market.getToday()}：on_stop_order: {stop_order}")

    def _jq_set_position(self, code):
        """
        设置或更新聚宽仓位信息
        """
        pos_jq = type('Class', (), {})()
        pos_vnpy = self.portfolio.getLongPosition(code)
        
        code_jq = CodeProcessor.convert_jq(code)
        pos_jq.security = code_jq  # 标的代码
        pos_jq.price = self.market.getRealTime().getTick(code).close_price  # 最新行情价格
        pos_jq.total_amount = pos_vnpy.pos_total # 总仓位, 但不包括挂单冻结仓位
        pos_jq.locked_amount = pos_vnpy.pos_lock  # 挂单冻结仓位
        pos_jq.closeable_amount = pos_vnpy.getPosAvailable()  # 可卖出的仓位（不包括场外基金情况）
        #TODO 开仓/加仓时：new_avg_cost = (posiont_value + trade_value + commission) / (position_amount + trade_amount)
        # 每次买入后会调整avg_cost, 卖出时avg_cost不变。这个值也会被用来计算浮动盈亏
        pos_jq.avg_cost = pos_jq.price  # 当前持仓成本，只在开仓/加仓时更新（使用最新价近似模拟）
        pos_jq.acc_avg_cost = pos_jq.price  # 累计持仓成本，在清仓/减仓时也更新（使用最新价近似模拟）
        #TODO hold_cost: 当日持仓成本，计算方法：当日无收益：hold_cost = 前收价 （清算后），
        # 加仓：hold_cost = (hold_cost * amount + trade_value)/(amount + trade_amount)，
        # 减仓：hold_cost = (hold_cost * amount - trade_value)/(amount - trade_amount)；
        # trade_value = trade_price * trade_amount
        pos_jq.hold_cost = pos_jq.price  # 当日持仓成本（使用最新价近似模拟）
        pos_jq.value = pos_vnpy.pos_total * pos_jq.hold_cost  # 标的价值
        pos_jq.init_time = self.market.getToday() # 建仓时间，格式为 datetime.datetime

        self._jq_context.portfolio.long_positions[code_jq] = pos_jq

    def _jq_set_portfolio(self, portfolio):
        """
        使用vnpy相关数据设置聚宽策略上下文中的资产信息
        """
        self._jq_context.portfolio.locked_cash = 0.0  # 锁仓资金
        self._jq_context.portfolio.cash = portfolio.getValidCapital()  # 可用资金
        self._jq_context.portfolio.positions_value = portfolio.getTotalHoldCapital()  # 持仓市值
        self._jq_context.portfolio.portfolio_value = portfolio.getTotalCapital()  # 资金总市值
        self._jq_context.portfolio.total_value = self._jq_context.portfolio.portfolio_value  #TODO 是否恰当？
        