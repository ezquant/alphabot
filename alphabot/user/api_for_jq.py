# 本模块模拟聚宽在线环境中的全局变量及函数等
# 需要且只能在聚宽策略中引用

import sys
import datetime as dt
from alphabot import logger as log
from alphabot.utils import CodeProcessor, Dict2Obj

#---------------------- 以下为对聚宽环境API的模拟 -----------------------

# 用户策略全局变量
#g = object()  # 这种写法不行
g = type('Class', (), {})()  # 这样可以
#g.log_type = log

# 用户策略上下文
context = None # 初始化时需从外面提前赋值

# 日志对象
log.set_level = lambda x, y: "set_level foobar"


class FixedSlippage:
    
    def __init__(self, slippage=0.02):
        self._slippage = slippage
    
    def set_slippage(self):
        if context.engine_based == 'backtrader':
            cerebro = context.engine._cerebro
            cerebro.broker.set_slippage_fixed(self._slippage)
            log.info('设置【固定滑点】为 %f' % self._slippage)
        elif context.engine_based == 'rqalpha':
            raise(Exception('！！待实现……'))
        elif context.engine_based == 'vnpy':
            context.strat_obj.portfolio.slippage = self._slippage
            log.info('设置【固定滑点】为 %f' % self._slippage)
            #raise(Exception('！！待实现……'))
        else:
            raise(RuntimeError('不支持的量化引擎：%s' % context.engine_based))


class PerTrade:
    
    def __init__(self, buy_cost=0.0003, sell_cost=0.0013, min_cost=5):
        self.buy_cost = buy_cost
        self.sell_cost = sell_cost
        self.min_cost = min_cost
    
    def set_commission(self):
        # 股票交易费率：买0.0003卖0.0013，近似表达为买卖费率的平均值乘以系数1.2
        cms_rate = (self.sell_cost + self.buy_cost) / 2.0 * 1.2
        cms_rate = round(cms_rate, 5)
        
        if context.engine_based == 'backtrader':
            cerebro = context.engine._cerebro
            #TODO: A股买卖费率不同，bt不好表示，这里暂时统一设置为印花税率
            cerebro.broker.setcommission(commission=cms_rate)
            log.info('[PerTrade] 设置【交易费率】为 %f' % cms_rate)
        elif context.engine_based == 'rqalpha':
            #raise(Exception('！！待实现……'))
            pass
        elif context.engine_based == 'vnpy':
            context.strat_obj.portfolio.commit_rate = cms_rate
            log.info('[PerTrade] 设置【交易费率】为 %f' % cms_rate)
            #raise(Exception('！！待实现……'))
            pass
        else:
            raise(RuntimeError('不支持的量化引擎：%s' % context.engine_based))


class OrderCost:

    def __init__(self, close_tax=0.001, open_commission=0.0003, 
                    close_commission=0.0003, min_commission=5):
        # 国内股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 
        # 每笔交易佣金最低扣5块钱
        self.close_tax = close_tax
        self.open_commission = open_commission
        self.close_commission = close_commission
        self.min_commission = min_commission
    
    def set_commission(self, type):
        if type == 'forex':
            '''
            cerebro.broker.setcommission(leverage=50,stocklike=False,commtype=bt.CommInfoBase.COMM_PERC,commission=.0000)
            # Add the new commission scheme
            #comminfo = commissions.forexSpreadCommisionScheme(spread=1.0)
            #cerebro.broker.addcommissioninfo(comminfo)
            '''
            pass
        elif type == 'futures':
            #for com in STEVENS_COMMISSIONS:
            #    cerebro.broker.setcommission(mult=com['mult'],name=com['name'],margin=com['margin'],commission=0)
            pass
        elif type == 'stock':
            # 股票交易费率：买0.0003卖0.0013，近似表达为买卖费率的平均值乘以系数1.2
            cms_rate = (self.close_tax + self.close_commission + \
                self.open_commission) / 2.0 * 1.2
            cms_rate = round(cms_rate, 5)
            
            if context.engine_based == 'backtrader':
                cerebro = context.engine._cerebro
                #cerebro.broker.setcommission(leverage=1,stocklike=True,commission=.0001,mult=1,margin=None,interest=.00,interest_long=True)
                #TODO: 实现自定义费率 https://www.backtrader.com/docu/user-defined-commissions/commission-schemes-subclassing/
                cerebro.broker.setcommission(commission=cms_rate)
                log.info('[OrderCost] 设置【交易费率】为 %f' % cms_rate)
            elif context.engine_based == 'rqalpha':
                raise(Exception('！！待实现……'))
            elif context.engine_based == 'vnpy':
                context.strat_obj.portfolio.commit_rate = cms_rate
                log.info('[OrderCost] 设置【交易费率】为 %f' % cms_rate)
                #raise(Exception('！！待实现……'))
            else:
                raise(RuntimeError('不支持的量化引擎：%s' % context.engine_based))


def enable_profile():
    pass

def set_benchmark(code):
    if context.engine_based == 'backtrader':
        # 该函数需要在 RunManager 中调用。若在策略初始化时加载将会报错，若策略内调用则无效。
        context.engine._cerebro_set_benchmark(code)
    elif context.engine_based == 'rqalpha':
        #raise(Exception('！！待实现……'))
        pass
    elif context.engine_based == 'vnpy':
        #raise(Exception('！！待实现……'))
        pass
    else:
        raise(RuntimeError('不支持的量化引擎：%s' % context.engine_based))

def set_option(name, value): #'use_real_price', True
    #TODO: xxx
    pass

def set_order_cost(obj, type='stock'):
    #聚宽设置交易手续费的新接口
    obj.set_commission(type)

def set_slippage(obj):
    obj.set_slippage()

def set_commission(obj):
    #聚宽设置交易手续费的老接口
    obj.set_commission()

def record(**args):
    print(args)

def run_daily(func_name, time: '14:53', reference_security: '000300.XSHG'):
    raise(Exception('！！待实现……'))
    g.run_daily_func = func_name
    g.run_daily_when = time

def order_target_value(code, value):
    """
    买卖 code 到指定价值 value
    """
    if context.engine_based == 'backtrader':
        #code = CodeProcessor.parse(code)[0]
        cerebro = context.engine._cerebro
        #print('--> cerebro.datasbyname:', cerebro.datasbyname)
        _data = cerebro.datasbyname[code]
        order = context.strat_obj.order_target_value(data=_data, target=value)
    elif context.engine_based == 'rqalpha':
        raise(Exception('！！待实现……'))
    elif context.engine_based == 'vnpy':
        raise(Exception('！！待实现……'))
    else:
        raise(RuntimeError('不支持的量化引擎：%s' % context.engine_based))
    
def order_target(code, amount):
    """
    买卖 code 到指定数量 amount
    """
    order = type('Class', (), {})()
    
    if context.engine_based == 'backtrader':
        raise(Exception('！！待实现……'))
    elif context.engine_based == 'rqalpha':
        raise(Exception('！！待实现……'))
    elif context.engine_based == 'vnpy':
        code_plain = CodeProcessor.convert_jq_to_plain(code)
        pos = context.strat_obj.portfolio.getLongPosition(code_plain)
        delta = amount - pos.pos_total
        price = context.strat_obj.bar_datas[code].close
        #print('--> order_target %s 目标仓位：%s, 当前仓位：%s，当前价格：%0.3f' % (code, amount, pos.pos_total, price))
        if abs(delta) < 100:
            #TODO 当前处理逻辑仅适用于中国股市，不适用于期货等
            #print('差额数量不足 100，忽略~')
            return None
        volume = abs(delta/context.strat_obj.portfolio.a_hand_size)
        
        ret = False
        if delta > 0:
            #print(f'--> zzzz 执行买入 {code_plain} {price} {volume}')
            ret = context.strat_obj.portfolio.buy(code_plain, price, volume)
            order.is_buy = True
        elif delta < 0:
            #print(f'--> zzzz 执行卖出 {code_plain} {price} {volume}')
            ret = context.strat_obj.portfolio.sell(code_plain, price, volume)
            order.is_buy = False
        else:
            return None
        
        if ret:  # vnpy earnmi 返回 True，进入锁仓，这里假定都会成交 #TODO 日后可细化改进
            order.filled = volume * context.strat_obj.portfolio.a_hand_size
            print(f'--> 订单已下，预期成交数量: {order.filled}')
        else:
            return None
        #raise(Exception('！！待实现……'))
    else:
        raise(RuntimeError('不支持的量化引擎：%s' % context.engine_based))
    
    return order

def order_value(code, value, round_100=True):
    """
    买入价值 value 的 code
    """
    # 获取该 code 当前时间点最近三条数据
    h = attribute_history(code, 3, unit='1m') # 如果 code 不在 datasbyname 中将在此加入
    # 使用 value 和当前价格计算 size，然后调用 buy
    _price = h['close'].values[0] # 该 code 当前 bar 价格
    _size = value / _price
    # 国内市场交易股数需对 100 取整
    if round_100:
        _size = (_size % 100) * 100
    
    if context.engine_based == 'backtrader':
        #code = CodeProcessor.parse(code)[0]
        cerebro = context.engine._cerebro
        # 在 backtrader 中，交易操作是根据 data 而不是 code 来做的。需要转换。
        #print('--> cerebro.datasbyname:', cerebro.datasbyname)
        _data = cerebro.datasbyname[code]
        #print('--> got data of code %s: %s' % (code, _data))
        order = context.strat_obj.buy(data=_data, size=_size, price=_price)
    elif context.engine_based == 'rqalpha':
        raise(Exception('！！待实现……'))
    elif context.engine_based == 'vnpy':
        raise(Exception('！！待实现……'))
    else:
        raise(RuntimeError('不支持的量化引擎：%s' % context.engine_based))
    
def get_security_info(code):
    obj = type('Class', (), {'display_name': None})()
    obj.display_name = code
    return obj

def _get_history_data(code, count, unit, skip_paused):
    if unit == '1d':
        # timedelta args: weeks, days, hours, minutes, seconds
        num = count + 7  # 加上最长节假日天数
        _timedelta = dt.timedelta(days=num)
        _time_begin = context.current_dt - _timedelta
        _time_end = context.current_dt.replace(hour=0).replace(minute=0).replace(second=0)
        if context.engine_based == 'backtrader':
            _time_begin = _time_begin.date()  # bt按天取数据，时间只取年月日
            _time_end = _time_end.date()
    elif unit == '1m':
        num = count + 1
        if context.current_dt.hour == 9 and context.current_dt.minute == 30:
            num = count + 7*24*60  # 加上最长节假日分钟数（包含放假前最后一天的最后一条数据）
        elif context.current_dt.hour == 13 and context.current_dt.minute == 0:
            num = count + 91  # 加上午休间隔分钟数
        _timedelta = dt.timedelta(minutes=num)
        _time_begin = context.current_dt - _timedelta
        _time_end = context.current_dt
    
    #print('--> call _get_data_historical_jq:', code, _time_begin, _time_end, unit)
    _data = context.engine._get_data_historical_jq(code, _time_begin, _time_end, unit)
    
    #TODO: field 过滤
    #print('--> _data', _data)
    
    return _data[-count:]

def attribute_history(code, count, unit='1d', fields=['close'], skip_paused=True):
    """
    根据 code 获取当前数据条之前 count 天或分钟的交易数据
    """
    # 根据 count 计算时间段
    log.debug('call attribute_history for code %s, count: %d, unit: %s' \
            % (code, count, unit))
    #print('--> context.current_dt:', context.current_dt)
    
    _x = _get_history_data(code, count, unit, skip_paused)
    
    _data = {}
    fields_lst = []
    if type(fields) == str:
        fields_lst.append(fields)  # 如 'close'
    else:
        fields_lst.extend(fields)  # 如 ('open', 'close')
    for field in fields_lst:
        #print('--> type(_x):', type(_x))
        #print('--> _x:', _x)
        if context.engine_based == 'backtrader':
            _data[field] = list(_x[field])
        elif context.engine_based == 'vnpy':
            if field == 'open':
                _data[field] = [o.open_price for o in _x]
            elif field == 'high':
                _data[field] = [o.high_price for o in _x]
            elif field == 'low':
                _data[field] = [o.low_price for o in _x]
            elif field == 'close':
                _data[field] = [o.close_price for o in _x]
            else:
                raise(Exception('不支持的 field: %s !' % field))
        else:
            raise(Exception('！！待实现……'))
    
    return _data

def history(count, unit='1d', field='close', security_list=None,
            df=True, skip_paused=False, fq='pre', pre_factor_ref_date=None):
    _data = {}
    for i in security_list:
        _l = []
        if context.engine_based == 'backtrader':
            _l = _get_history_data(i, count, unit, skip_paused)[field]
        elif context.engine_based == 'vnpy':
            _x = _get_history_data(i, count, unit, skip_paused)
            # _x: [BarData(gateway_name='DB', symbol='601398', exchange=<Exchange.SSE: 'SSE'>, datetime=datetime.datetime(2021, 1, 4, 0, 0), 
            # trading_day='', interval=<Interval.DAILY: 'd'>, interval_num=1, volume=373335881.0, open_interest=0.0, 
            # open_price=4.71, high_price=4.75, low_price=4.69, close_price=4.75)]
            for j in _x:
                if field == 'open':
                    _l.append(j.open_price)
                elif field == 'high':
                    _l.append(j.high_price)
                elif field == 'low':
                    _l.append(j.low_price)
                elif field == 'close':
                    _l.append(j.close_price)
                else:
                    raise(Exception('不支持的 field: %s !' % field))
        else:
            raise(Exception('！！待实现……'))
        _data[i] = list(_l)
    return _data

def get_security_info(security):
    obj = type('Class', (), {})()
    
    obj.code = security
    obj.display_name = security + '名称'
    obj.start_date = dt.datetime(2016, 1, 1, 0, 0, 0)
    
    return obj


#---------------------- 以下为自定义函数等内容 -----------------------

# 对 backtrader 引擎的相关适配
from alphabot.backend.backtrader.strategy_wrap import BTStrategyForJQ

BTStrategyForJQ.g = g

def api_set_context(ctx):
    """
    设置策略执行时的上下文
    """
    global context
    print('api_set_context ', ctx)
    context = ctx

    # 判断引擎类型
    context.engine_based = 'unkown'
    #print('--> str(type(context)):', str(type(context)))
    if 'backtrader' in str(type(context)):
        context.engine_based = 'backtrader'
    elif 'rqalpha' in str(type(context)):
        context.engine_based = 'rqalpha'
    elif 'vnpy' in str(type(context)):
        context.engine_based = 'vnpy'
    elif 'zipline' in str(type(context)):
        context.engine_based = 'zipline'
    print('使用量化引擎：%s' % context.engine_based)

def api_engine_init(context):
    """
    在聚宽策略初始化时，需补充执行针对本系统所做的相关适配
    """
    # 如果引擎为 backtester，则这里的 context 为本模块所中设置的全局变量
    # 若引擎为 rqalpha/zipline，则为引擎调用本函数时生成和传入的参数
    
    this = sys.modules[__name__]
    print('--> 全局 context:', this.context)  # 本模块中申明的全局变量
    print('--> this.context.run_params.type:', this.context.run_params.type)  # 本模块中申明的全局变量

    # 列举当前模块的所有全局变量
    #for key, value in globals().items():
    #    if callable(value) or value.__class__.__name__ == "module":
    #        continue
    #    print('--> 全局变量 %s: %s' % (key, globals()[key]))

    print('--> 局部 context:', context)  # 函数调用方如 rqalpha 所传入的

    context.engine_based = this.context.engine_based

    if context.engine_based == 'backtrader':
        #raise(Exception('！！待实现……'))
        pass
    elif context.engine_based == 'rqalpha':
        #run_info = context.run_info
        #print('--> run_info:', run_info)
        #print('--> run_info._run_type:', run_info._run_type)  # rqalpha config 中指定的运行模式
        
        # 合并全局变量 context 的成员到参数中传入的 context 中（包括在自定义配置中指定的运行模式）
        context.run_params = this.context.run_params
    elif context.engine_based == 'vnpy':
        #raise(Exception('！！待实现……'))
        pass
    else:
        raise(RuntimeError('不支持的量化引擎：%s' % context.engine_based))
    