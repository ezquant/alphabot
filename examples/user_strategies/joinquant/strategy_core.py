# -*- coding: utf-8 -*-
'''
# 面向对象策略框架升级版

## 程序框架说明

基于“多因子选股+多因子权重排序示例策略”模板改进而来。
主要进行了代码重构，规范了命名，增加了对新策略的支持。

注意：该框架【必须】运行在分钟或tick级！

## 重点类说明

1. GlobalVariable(object)
自定义所有规则共用的全局变量，用于规则通用数据存储。一个策略对应一个 GlobalVariable。
包含较为通用的卖股、买股、清仓函数。

2. Rule(object)
所有规则的基类。

包含聚宽策略的事件：
Initialize、handle_data、before_trading_start...等等。
即每次聚宽事件都会调用所有规则的相关事件。在 handle_data 中根据策略流程可能会中断执行，不一定所有都执行。

除此之外，新增自定义的一些事件：
on_sell_stock、on_buy_stock、before_clear_position、on_clear_position
分别为卖股、买股、清仓前、清仓后触发的事件。

3. GroupRules(Rule)
为包含一系列规则组合的规则，通该类或扩展该类，实现树形策略执行流程。

4. TimerExec(...)
指定执行时间。如果不在指定时间，该规则后面的所有规则都将不执行。

5.1 StopLossByXXX(...)
根据指定条件检查和执行止损

5.2 StopProfitByXXX(...)
根据指定条件检查和执行止盈

6. TimingByXXX(FilterStockList)
根据指定择时条件进行调仓

7. PickByXXX(FilterStockList)
根据指定条件进行选股，如 PickByGeneral 为根据聚宽选股函数进行股票、基金、概念等选股。

8. FilterByXXX(FilterStockList)
根据指定条件对股票列表进行过滤，如 FilterByIndicatorJQ 为根据聚宽财务等因子数据进行选股过滤。

9. ChoseStocksXXX(GroupRules)
选股规则组合器。此类通过一系列 ChoseStocksSimple 等子类组合选出股票池。

10. SortRules(GroupRules, FilterStockList)
多因子权重排序规则组合器。多因子权重排序算法实现类
'''

import time
#import copy
#import types
import math

#import re
import requests
import json
import smtplib
import talib
import copy
import pickle
import traceback
import datetime as dt
import numpy as np
import pandas as pd
import statsmodels.api as sm
import scipy.stats as stats
#from decimal import Decimal
from enum import Enum
from math import isnan
from prettytable import PrettyTable
from email.mime.text import MIMEText
from email.header import Header
from functools import reduce

# 对聚宽框架，优先适配在线环境，然后适配本地环境
try:
    from kuanke.user_space_api import *  # old
    from jqdata import *  # new
except:
    from jqdatasdk import *
    # 使用聚宽账号密码登录（新申请用户默认为手机号后6位）
    #_user, _passwd = '13520312255','Zdf6664518' # ok
    _user, _passwd = '17666120227', 'Qwer4321' # ok earnmi
    #_user, _passwd = '18500150123', 'YanTeng881128' # ok
    #_user, _passwd = '13695683829', 'ssk741212'
    #_user, _passwd = '18610039264', 'zg19491001'
    auth(_user, _passwd) # '聚宽账号','聚宽密码'
    #查询当日剩余可调用数据条数
    print(f'==> get JQ query count: {get_query_count()}')

    # 使用 jqdatasdk 中的相应函数
    #history  = history_engine

    # 使用自定义相关函数
    from alphabot.user.api_for_jq import *

try:
    log.info('import strategy_core...')
except:
    import logging as log
    log.basicConfig(level=log.WARNING,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    # handlers={log.FileHandler(filename='test.log', mode='a', encoding='utf-8')}
                    )
    log.info('import strategy_core...use logging')
    log.set_level = lambda x, y: "set_level foobar"

try:
    import web_api_client
except:
    log.warn("加载 web_api_client 失败")

try:
    from xq_api import loading as xq_loading
    # from trader_sync import *
except:
    log.warn("加载 xq_api 失败")


IMPORT_TAG = True  # 每次引入该模块文件时置位，可用于判断程序是否重新加载并作相应处理


# =================================基础类=======================================

class RuleLoger(object):

    def __init__(self, context, msg_header):
        try:
            self._owner_msg = msg_header
        except:
            self._owner_msg = '未知规则'
        self.context = context

    def _get_msg_with_time(self, msg):
        log_time = dt.datetime.now()
        if self.context and hasattr(self.context, 'current_dt'):
            log_time = self.context.current_dt
        _msg = '[' + log_time.strftime('%Y-%m-%d %H:%M:%S') + '][' \
            + self._owner_msg + '] ' + str(msg)
        return _msg

    def debug(self, msg, *args, **kwargs):
        log.debug(self._get_msg_with_time(msg), *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        log.info(self._get_msg_with_time(msg), *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        log.warn(self._get_msg_with_time(msg), *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        log.error(self._get_msg_with_time(msg), *args, **kwargs)

    def weixin(self, context, msg, *args, **kwargs):
        log.warn(self._get_msg_with_time(msg), *args, **kwargs)
        # 只在模拟时发微信,以免浪费发信次数限额
        if context.run_params.type == 'sim_trade':
            send_message(self._owner_msg + ': ' + msg, channel='weixin')


# --------------------------共同参数类-----------------------------------
# 1.考虑到规则的信息互通，完全分离也会增加很大的通讯量。适当的约定好的全局变量，可以增加灵活性。
# 2.因共同约定，也不影响代码重用性。
# 3.假如需要更多的共同参数。可以从全局变量类中继承一个新类并添加新的变量，并赋于所有的规则类。
#     如此达到代码重用与策略差异的解决方案。
# '''
class GlobalVariable(object):
    context = None
    _owner = None
    is_sim_trade = False  # 当前运行模式是否为模拟

    stock_pindexs = [0]  # 指示是属于股票性质的子仓列表
    op_pindexs = [0]  # 提示当前操作的股票子仓Id

    buy_stocks = []  # 选股列表
    sell_stocks = []  # 卖出的股票列表
    stocks_adjust_info = {}  # 股票调仓时相关信息，如交易时间，买卖价格等。在各具体策略中设置和更新
    
    trader_sync_is_running = False  # 是否实盘同步任务在进行中：执行开始时置 True，执行完毕后置 False
    
    def __init__(self, owner, context):
        self._owner = owner
        self.is_sim_trade = context.run_params.type == 'sim_trade'
        self._trading_status_file = 'data/trading_status_%s.pkl' \
            % dt.datetime.now().strftime('%Y%m%d-%H%M')
        log.info('交易状态缓存文件：%s' % self._trading_status_file)

    def load_trading_status(self, context=None):
        """读取交易状态持久化数据"""

        #log.warn('>>> 本程序暂时忽略交易状态持久化操作~')
        #return

        # 只在实盘或模拟盘才需读取
        #聚宽运行方式, type 为如下四个字符串之一:
        #    'simple_backtest': 回测, 通过点击'编译运行'运行
        #    'full_backtest': 回测, 通过点击'运行回测'运行
        #    'sim_trade': 模拟交易
        #    'live_trade': 实盘交易
        if context is None \
            or context.run_params.type not in ('sim_trade', 'live_trade'):
            return
        
        log.info('>>> 读取交易状态持久化数据 ... [%s]' % self._trading_status_file)

        try:
            if True:  # 聚宽平台
                trading_status = pickle.loads(
                    read_file(self._trading_status_file))  # 聚宽函数
            else:
                with open(self._trading_status_file, "rb") as fp:
                    trading_status = pickle.load(fp)
            self.buy_stocks = trading_status['buy_stocks']
            self.sell_stocks = trading_status['sell_stocks']
            self.stocks_adjust_info = trading_status['stocks_adjust_info']
            log.info('交易状态持久化 读取文件完成！trading_status: %s' % trading_status)
        except Exception as e:
            log.warn('load pkl: %s' % e)

    def dump_trading_status(self, context=None):
        """保存交易状态持久化数据"""

        # 只在实盘或模拟盘才需保存
        if context is None or \
            context.run_params.type not in ('sim_trade', 'live_trade'):
            return
        
        try:
            trading_status = {
                'buy_stocks': self.buy_stocks,
                'sell_stocks': self.sell_stocks,
                'stocks_adjust_info': self.stocks_adjust_info
            }
            if True:  # 聚宽平台
                # 数据保存到研究根目录的data子目录下（若子目录不存在，会自动创建）
                # 注意：研究环境下不会自动创建目录，需要手动先创建
                write_file(self._trading_status_file, pickle.dumps(trading_status))  # 聚宽函数
            else:
                with open(self._trading_status_file, "wb+") as fp:
                    pickle.dump(trading_status, fp, protocol=pickle.HIGHEST_PROTOCOL)
            #log.info('交易状态持久化 写入文件完成！')  # 调用频率高，故注释掉
        except Exception as e:
            log.warn('dump pkl: %s' % e)
        
    # ==============================持仓操作函数，共用================================

    # 按指定股数下单
    def order(self, sender, security, amount, pindex=0):
        cur_price = get_close_price(security, 1, '1m')
        if math.isnan(cur_price):
            return False
        position = self.context.subportfolios[pindex].long_positions[security] if self.context is not None else None
        _order = order(security, amount, pindex=pindex)
        if _order != None and _order.filled > 0:
            # 订单成功，则调用规则的买股事件 。（注：这里只适合市价，挂价单不适合这样处理）
            if amount > 0:
                self._owner.on_buy_stock(security, _order, pindex)
            elif position is not None:
                self._owner.on_sell_stock(position, _order, pindex)
            return _order
        return _order
    
    def do_order_target(self, security, amount, pindex=0):
        order = order_target(security, amount)  # 买卖标的达到指定余数，市价单
        if order != None and order.filled > 0:
            #current_value = self.context.subportfolios[pindex].long_positions[security].value
            if order.is_buy:
                self._owner.on_buy_stock(security, order, pindex)
            else:
                position = self.context.subportfolios[pindex].long_positions[security]
                self._owner.on_sell_stock(position, order, pindex)

    # 开仓买入指定价值的证券，调整至目标值
    #   报单成功并成交（包括全部成交或部分成交，此时成交量大于0），触发所有规则的 on_buy_stock 函数，返回True
    #   报单失败或者报单成功但被取消（此时成交量等于0），返回False
    # 注意：由于聚宽在一个 bar 内分不分批计算结果都一样，为便于模拟盘跟单，batch_amount 这里统一设置为大数值
    def open_position(self, context, security, value, pindex=0, batch_amount=100000):
        # 根据最新价格计算并取整，得到要交易的股票数
        cur_price = get_close_price(security, 1, '1m')
        if math.isnan(cur_price):
            return False
        value = value * 0.98  # 留出滑点及手续费（百万留1w）
        amount = int(round(value / cur_price / 100) * 100)
        if amount < 100:
            return False

        #print(("买入 %s, 近期价格：%.3f，数量：%d") % (security, cur_price, amount))
        #self.log.info(("调整 %s 的持仓目标金额至：￥%.2f（当前价格：%.3f）") % (security, value, cur_price))

        ## 调整标的至目标权重
        ##value_round = (amount + 1) * cur_price # +1为防止价格波动可能导致的购买不足
        #order = order_target_value(
        #    security, value, pindex=pindex)  # TODO 目标值按股数的100取整计算
        #if order != None and order.filled > 0:
        #    # 订单成功，则调用规则的买股事件 。（注：这里只适合市价，挂价单不适合这样处理）
        #    self._owner.on_buy_stock(security, order, pindex)
        #    return True
        #else:
        #    return False

        # 分批下单（貌似无意义，分批可在实盘跟单时进行）
        if False:
            _amount = amount
            _batch = batch_amount
            _loop = int(_amount/_batch)+1 if _amount%_batch>100 else int(_amount/_batch)
            for i in range(_loop):
                _amount_op = _batch*(i+1) if i+1!=_loop else _amount  # 当次操作后剩余股数
                #print('--> open_position(): order_target to ', _amount_op)
                self.do_order_target(security, _amount_op, pindex)
                # 开启 sleep 后，下单时延可达到 5s 以上，对实盘跟单会有影响，暂时注释掉
                #if context.run_params.type == 'sim_trade' and i < _loop - 1:
                #    time.sleep(0.5)
        else:
            # 或直接一次性下单
            self.do_order_target(security, amount, pindex)

        return True

    # 平仓，卖出指定持仓
    #   报单成功并全部成交，则触发所有规则的when_sell_stock函数，返回True
    #   报单失败或者报单成功但被取消（此时成交量等于0），或者报单非全部成交，返回False
    # 注意：由于聚宽在一个 bar 内分不分批计算结果都一样，为便于模拟盘跟单，batch_amount 这里统一设置为大数值
    def close_position(self, context, position, is_normal=True, pindex=0, batch_amount=100000):
        security = position.security
        # log.info('执行平仓 %s' % security)
        
        #order = order_target_value(security, 0, pindex=pindex)  # 可能会因停牌失败
        #if order != None:
        #    if order.filled > 0:
        #        self._owner.on_sell_stock(position, order, is_normal, pindex)
        #        if security not in self.sell_stocks:
        #            self.sell_stocks.append(security)
        #        return True
        #else:
        #    return False

        # 分批下单（貌似无意义，分批可在实盘跟单时进行）
        if False:
            _amount = position.total_amount
            _batch = batch_amount
            _loop = int(_amount/_batch)+1 if _amount%_batch>100 else int(_amount/_batch)
            for i in range(_loop):
                _amount_op = _amount-_batch*(i+1) if i+1!=_loop else 0  # 当次操作后剩余股数
                #print('--> close_position(): order_target to ', _amount_op)
                if context.portfolio.positions[security].closeable_amount > 0:
                    log.info('执行卖出 %s' % security)
                    self.do_order_target(security, _amount_op, pindex)
                    #if context.run_params.type == 'sim_trade' and i < _loop - 1:
                    #    time.sleep(0.5)
        else:
            # 或直接一次性下单
            if position.total_amount > 0:
                log.info('执行卖出 %s' % security)
                self.do_order_target(security, 0, pindex)
        
        if security not in self.sell_stocks:
            self.sell_stocks.append(security)
        
        return True

    # 清空卖出所有持仓
    # 清仓时，调用所有规则的 when_clear_position
    def clear_position(self, context, pindexs=[0]):
        pindexs = self._owner.before_clear_position(context, pindexs)
        # 对传入的子仓集合进行遍历清仓
        for pindex in pindexs:
            if context.subportfolios[pindex].long_positions:
                self.log.info(("[%d]==> 清仓，卖出所有股票") % (pindex))
                for stock in context.subportfolios[pindex].long_positions.keys():
                    position = context.subportfolios[pindex].long_positions[stock]
                    self.close_position(context, position, False, pindex)
        # 调用规则器的清仓事件
        self._owner.on_clear_position(context, pindexs)

    # 通过对象名 获取对象
    def get_obj_by_name(self, name):
        return self._owner.get_obj_by_name(name)

    # 调用外部的on_log额外扩展事件
    def on_log(sender, msg, msg_type):
        pass


# ==============================规则基类================================
class Rule(object):
    name = ''  # obj名，可以通过该名字查找到
    memo = ''  # 默认描述
    g = None  # 所属的策略全局变量
    log = None
    # 执行是否需要退出执行序列动作，用于 GroupRules 默认来判断终止执行。
    is_to_return = False

    def __init__(self, params):
        self._params = params.copy()
        pass

    # 更改参数
    def update_params(self, context, params):
        self._params = params.copy()
        pass

    def initialize(self, context):
        pass

    def handle_data(self, context, data):
        pass

    def before_trading_start(self, context):
        self.is_to_return = False

    def after_trading_end(self, context):
        self.is_to_return = False

    def process_initialize(self, context):
        pass

    def after_code_changed(self, context):
        pass

    # 卖出股票时调用的函数
    # price为当前价，amount为发生的股票数,is_normail正常规则卖出为True，止损卖出为False
    def on_sell_stock(self, position, order, is_normal, pindex=0):
        pass

    # 买入股票时调用的函数
    # price为当前价，amount为发生的股票数
    def on_buy_stock(self, stock, order, pindex=0):
        pass

    # 清仓前调用。
    def before_clear_position(self, context, pindexs=[0]):
        return pindexs

    # 清仓时调用的函数
    def on_clear_position(self, context, pindexs=[0]):
        pass

    # handle_data没有执行完 退出时。
    def on_handle_data_exit(self, context, data):
        pass

    # record副曲线
    def record(self, **kwargs):
        if self._params.get('record', False):
            record(**kwargs)

    def set_g(self, g):
        self.g = g

    # 有BUG：聚宽 run_daily 不支持回调对象成员函数
    #def run_daily_batch(self, context, params_list):
    #    """根据配置列表依次进行定时调用函数注册 - 该函数通常只在选股时使用
    #    注意：支持聚宽平台。配置示例：[[market_close_befor, '14:58', '000300.XSHG'], [...]]"""
    #    for params in params_list:
    #        func, time, refer = params[0], params[1], params[2]
    #        # time 可选值：before_open、open、every_bar、after_close或09:27之类
    #        if time not in ('before_open', 'open', 'every_bar', 'after_close') and time.find(':') < 0:
    #            raise Exception('run_daily_batch(): 不支持的时间格式 %s' % time)
    #        # func 只能接受一个参数 context，所以对其进行封装
    #        #def new_func(context):
    #        #    return func(self, context)
    #        context.self = self
    #        print('--> zzz', context, func, time, refer)
    #        #run_daily(func, time=time, reference_security=refer)
    #        run_daily(func, time=time, reference_security=refer)

    def __str__(self):
        return self.memo


# ==================================调仓条件相关规则=========================================

class TimerExec(Rule):
    """调仓时间控制器"""

    def __init__(self, params):
        Rule.__init__(self, params)
        # 配置调仓时间 times为二维数组，示例[[10,30],[14,30]] 表示 10:30和14：30分调仓
        self.times = params.get('times', [])

    def update_params(self, context, params):
        Rule.update_params(self, context, params)
        self.times = params.get('times', self.times)

    def handle_data(self, context, data):
        hour = context.current_dt.hour
        minute = context.current_dt.minute
        self.is_to_return = not [hour, minute] in self.times

    def __str__(self):
        return '调仓时间控制器: [ 调仓时间 %s ]' % (
            str(['%d:%d' % (x[0], x[1]) for x in self.times]))


class PeriodCondition(Rule):
    """调仓日计数器"""
    
    def __init__(self, params):
        Rule.__init__(self, params)
        # 调仓日计数器，单位：日
        self.period = params.get('period', 3)
        self.day_count = 0

    def update_params(self, context, params):
        Rule.update_params(self, context, params)
        self.period = params.get('period', self.period)

    def handle_data(self, context, data):
        self.is_to_return = self.day_count % self.period != 0
        
    def before_trading_start(self, context):
        self.log.info("调仓日计数 [%d]" % (self.day_count))

    def after_trading_end(self, context):
        self.day_count += 1

    def on_sell_stock(self, position, order, is_normal, pindex=0):
        if not is_normal:
            # 个股止损止盈时，即非正常卖股时，重置计数，原策略是这么写的
            self.day_count = 0

    # 清仓时调用的函数
    def on_clear_position(self, context, new_pindexs=[0]):
        self.day_count = 0

    def __str__(self):
        return '调仓日计数器: [调仓频率 %d日，调仓日计数 %d]' % (
            self.period, self.day_count)


# ========================= 选股规则相关 ===================================


# 选股过滤器基类，返回 Query
class FilterQuery(Rule):
    
    def filter(self, context, data, q):
        return None


# 股票列表过滤器基类，返回过滤后的列表
class FilterStockList(Rule):
    
    def __init__(self, params):
        Rule.__init__(self, params)
        # 当前策略中持有股票清单
        self.holding_stocks = []
    
    def filter(self, context, data, stock_list):
        return None
    
    def stock_list_info(self, stock_list):
        self.log.info('股票数量：%d，前5项：' % len(stock_list))
        for i in stock_list[:5]:
            stock_info = get_security_info(i)
            '''
            一个对象, 有如下属性:
                display_name: 中文名称
                name: 缩写简称
                start_date: 上市日期, [dt.date] 类型
                end_date: 退市日期（股票是最后一个交易日，不同于摘牌日期）， [datetime.date] 类型, 如果没有退市则为2200-01-01
                type: 股票、基金、金融期货、期货、债券基金、股票基金、QDII 基金、货币基金、混合基金、场外基金，
                    'stock'/ 'fund' / 'index_futures' / 'futures' / 'etf'/'bond_fund' / 'stock_fund' / 
                    'QDII_fund' / 'money_market_fund' / ‘mixture_fund' / 'open_fund'
                parent: 分级基金的母基金代码
            '''
            self.log.info('  %s：%s [%s]' % (i, stock_info.display_name, stock_info.type))

    def get_stock_list_and_set_adjust(self, context, data, stock_list, \
            to_buy_list, to_keep_list, buy_count):
        """根据买入和可持有列表，生成钝化后的买入清单，并更新持仓资金比例
        适合取Top排名之类的策略，可减少不必要的频繁调仓"""
        stocks_to_buy = []
        stocks_to_sell = []

        # 1. 卖出不在买入列表中的股票
        for stock in self.holding_stocks:
            if stock not in to_keep_list:
                stocks_to_sell.append(stock)
        
        # 2. 买入符合条件的待买入股票
        stocks_to_buy = list(to_buy_list)

        # 3. 生成买入清单，更新持仓和调仓信息
        if len(stocks_to_sell) or len(stocks_to_buy):
            # 只要还在持有列表中，就不减仓也不卖出
            stock_list = [i for i in self.holding_stocks if i not in stocks_to_sell]
            stock_list += [i for i in stocks_to_buy if i not in self.holding_stocks]
            stock_list = stock_list[:buy_count]

            #print('--> stocks_to_buy: %s, stocks_to_sell: %s' % (stocks_to_buy, stocks_to_sell))
            #print('--> self.holding_stocks: %s, stock_list: %s' % (self.holding_stocks, stock_list))

            # 更新内部资金比例
            # a. 首先更新卖出标的
            ratio_delta = 0.0
            for i in stocks_to_sell:
                ratio_delta += self._get_adjust_ratio_single(i)
                self._update_adjust_ratio_single(i, 0.0)
            # b. 然后更新持有标的
            if len(self.holding_stocks) == 0:
                # 初始状态，平均分配给买入标的
                self._update_adjust_ratio(context, data, stocks_to_buy, len(stocks_to_buy))
            else:
                # 更新状态，将卖出标的资金平分累加到买入标的上
                for i in stocks_to_buy:
                    ratio = ratio_delta / buy_count + self._get_adjust_ratio_single(i)
                    #print('==> 更新标的 %s 资金占比：%.3f' % (i, ratio))
                    self._update_adjust_ratio_single(i, ratio)
            
            self.holding_stocks = stock_list
            
            self._update_adjust_info(context, data, stock_list)
        
        return stock_list

    def _update_adjust_ratio(self, context, data, stock_list, buy_count):
        """按平均方式更新各股内部资金分配占比（通常在选定买入股清单后调用）"""

        for i in stock_list:
            if i not in self.g.stocks_adjust_info:
                self.g.stocks_adjust_info[i] = {}
            _fr = 1.0 / buy_count  # 按最大买入数平分
            #_fr = 1.0 / len(stock_list)  # 按当前买入数平分
            ratio = float(str(_fr).split('.')[0]+'.'+str(_fr).split('.')[1][:3])  # 不用 round 四舍五入
            self._update_adjust_ratio_single(i, ratio)
            #self.log.info('%s 组合内默认资金占比：%.2f' % (i, self.g.stocks_adjust_info[i]['inner_fund_ratio']))

    def _update_adjust_ratio_single(self, stock, ratio):
        """更新某股票的子策略内资金占比"""
        if stock not in self.g.stocks_adjust_info:
            self.g.stocks_adjust_info[stock] = {}
        self.g.stocks_adjust_info[stock]['inner_fund_ratio'] = ratio
    
    def _get_adjust_ratio_single(self, stock):
        """获取某股票的子策略内资金占比"""
        if stock not in self.g.stocks_adjust_info:
            self.g.stocks_adjust_info[stock] = {}
        if 'inner_fund_ratio' not in self.g.stocks_adjust_info[stock]:
            self.g.stocks_adjust_info[stock]['inner_fund_ratio'] = 0.0
        return self.g.stocks_adjust_info[stock]['inner_fund_ratio']

    def _update_adjust_info(self, context, data, stock_list):
        """使用当前数据更新各股的调仓信息"""
        for i in stock_list:
            self._update_adjust_info_single(context, data, i)
    
    def _update_adjust_info_single(self, context, data, stock):
        """使用当前数据更新某股票的调仓信息"""
        if stock not in self.g.stocks_adjust_info:
            self.g.stocks_adjust_info[stock] = {}
        self.g.stocks_adjust_info[stock]['adjust_price'] = data[stock].close
        self.g.stocks_adjust_info[stock]['adjust_date'] = context.current_dt.date().strftime("%Y-%m-%d")
        self.g.stocks_adjust_info[stock]['sync_that_day'] = True
    
# 选取财务数据的参数
# 使用示例 FD_param('valuation.market_cap',None,100) #先取市值小于100亿的股票
# 注：传入类型为 'valuation.market_cap'字符串而非 valuation.market_cap 是因 valuation.market_cap等存在序列化问题！！
# 具体传入field 参考  https://www.joinquant.com/data/dict/fundamentals
class FD_Factor(object):
    def __init__(self, factor, **kwargs):
        self.factor = factor
        self.min = kwargs.get('min', None)
        self.max = kwargs.get('max', None)


# 因子排序类型
class SortType(Enum):
    asc = 0  # 从小到大排序
    desc = 1  # 从大到小排序


# 价格因子排序选用的价格类型
class PriceType(Enum):
    now = 0  # 当前价
    today_open = 1  # 开盘价
    pre_day_open = 2  # 昨日开盘价
    pre_day_close = 3  # 收盘价
    ma = 4  # N日均价


# 排序基本类 共用指定参数为 weight
class SortBase(Rule):
    @property
    def weight(self):
        return self._params.get('weight', 1)

    @property
    def is_asc(self):
        return self._params.get('sort', SortType.asc) == SortType.asc

    def _sort_type_str(self):
        return '从小到大' if self.is_asc else '从大到小'

    def sort(self, context, data, stock_list):
        return stock_list


# 按N日增长率排序
# day 指定按几日增长率计算,默认为20
class Sort_growth_rate(SortBase):
    def sort(self, context, data, stock_list):
        day = self._params.get('day', 20)
        r = []
        for stock in stock_list:
            rate = get_growth_rate(stock, day)
            if rate != 0:
                r.append([stock, rate])
        r = sorted(r, key=lambda x: x[1], reverse=not self.is_asc)
        return [stock for stock, rate in r]

    def __str__(self):
        return '[权重: %s ] [排序: %s ] 按 %d 日涨幅排序' % (self.weight, self._sort_type_str(), self._params.get('day', 20))


class Sort_price(SortBase):
    def sort(self, context, data, stock_list):
        r = []
        price_type = self._params.get('price_type', PriceType.now)
        if price_type == PriceType.now:
            for stock in stock_list:
                close = data[stock].close
                r.append([stock, close])
        elif price_type == PriceType.today_open:
            curr_data = get_current_data()
            for stock in stock_list:
                r.append([stock, curr_data[stock].day_open])
        elif price_type == PriceType.pre_day_open:
            stock_data = history(count=1, unit='1d', field='open',
                                 security_list=stock_list, df=False, skip_paused=True)
            for stock in stock_data:
                r.append([stock, stock_data[stock][0]])
        elif price_type == PriceType.pre_day_close:
            stock_data = history(count=1, unit='1d', field='close', security_list=stock_list, df=False,
                                 skip_paused=True)
            for stock in stock_data:
                r.append([stock, stock_data[stock][0]])
        elif price_type == PriceType.ma:
            n = self._params.get('period', 20)
            stock_data = history(count=n, unit='1d', field='close', security_list=stock_list, df=False,
                                 skip_paused=True)
            for stock in stock_data:
                r.append([stock, stock_data[stock].mean()])

        r = sorted(r, key=lambda x: x[1], reverse=not self.is_asc)
        return [stock for stock, close in r]

    def __str__(self):
        s = '[权重: %s ] [排序: %s ] 按当 %s 价格排序' % (
            self.weight, self._sort_type_str(), str(self._params.get('price_type', PriceType.now)))
        if self._params.get('price_type', PriceType.now) == PriceType.ma:
            s += ' [%d 日均价]' % (self._params.get('period', 20))
        return s


# --- 按换手率排序 ---
class Sort_turnover_ratio(SortBase):
    def sort(self, context, data, stock_list):
        q = query(valuation.code, valuation.turnover_ratio).filter(
            valuation.code.in_(stock_list)
        )
        if self.is_asc:
            q = q.order_by(valuation.turnover_ratio.asc())
        else:
            q = q.order_by(valuation.turnover_ratio.desc())
        stock_list = list(get_fundamentals(q)['code'])
        return stock_list

    def __str__(self):
        return '[权重: %s ] [排序: %s ] 按换手率排序 ' % (self.weight, self._sort_type_str())


# --- 按财务数据排序 ---
class Sort_financial_data(SortBase):
    def sort(self, context, data, stock_list):
        factor = eval(self._params.get('factor', None))
        if factor is None:
            return stock_list
        q = query(valuation).filter(
            valuation.code.in_(stock_list)
        )
        if self.is_asc:
            q = q.order_by(factor.asc())
        else:
            q = q.order_by(factor.desc())
        stock_list = list(get_fundamentals(q)['code'])
        return stock_list

    def __str__(self):
        return '[权重: %s ] [排序: %s ] %s' % (self.weight, self._sort_type_str(), self.memo)


# ===================================调仓相关============================

# ==============================调仓规则器基类==============================
# 需要 before_adjust_start和after_adjust_end的子类可继承
class AdjustExpand(Rule):

    def before_adjust_start(self, context, data):
        pass

    def after_adjust_end(self, context, data):
        pass


class SellStocks(Rule):
    """卖出股票规则
    """
    def handle_data(self, context, data):
        self.adjust(context, data, self.g.buy_stocks)

    def adjust(self, context, data, buy_stocks):
        # 卖出不在待买股票列表中的股票
        # 对于因停牌等原因没有卖出的股票则继续持有
        for pindex in self.g.op_pindexs:
            for stock in context.subportfolios[pindex].long_positions.keys():
                if stock not in buy_stocks:
                    position = context.subportfolios[pindex].long_positions[stock]
                    self.g.close_position(context, position, True, pindex)

    def __str__(self):
        return '调仓卖出，卖出不在待购列表中的股票'

class BuyStocks(Rule):
    """
    买入股票规则
    """
    def __init__(self, params):
        Rule.__init__(self, params)
        self.buy_count = params.get('buy_count', 3)

    def update_params(self, context, params):
        Rule.update_params(self, context, params)
        self.buy_count = params.get('buy_count', self.buy_count)

    def handle_data(self, context, data):
        self.adjust(context, data, self.g.buy_stocks)

    def adjust(self, context, data, buy_stocks):
        # 买入股票
        # 根据股票数量分仓
        # 此处只根据可用金额平均分配购买，不能保证每个仓位平均分配
        for pindex in self.g.op_pindexs:
            position_count = len(context.subportfolios[pindex].long_positions)
            if self.buy_count > position_count:
                value = context.subportfolios[pindex].available_cash / \
                    (self.buy_count - position_count)
                for stock in buy_stocks:
                    # if stock in self.g.sell_stocks:
                    #    self.log.info('%s 今天已经卖出过，暂不再买入' % stock)
                    #    continue
                    if stock not in context.subportfolios[pindex].long_positions or \
                            context.subportfolios[pindex].long_positions[stock].total_amount == 0:
                        if self.g.open_position(context, stock, value, pindex):
                            if len(context.subportfolios[pindex].long_positions) == self.buy_count:
                                break

    def after_trading_end(self, context):
        self.g.sell_stocks = []

    def __str__(self):
        return '股票调仓买入规则：现金平分式买入股票达目标股票数'


# ----------------股票买卖操作记录---------------
class OpStocksRecord(AdjustExpand):

    def __init__(self, params):
        AdjustExpand.__init__(self, params)
        self.op_buy_stocks = []
        self.op_sell_stocks = []
        self.position_has_change = False

    def on_buy_stock(self, stock, order, new_pindex=0):
        #print(f'--> zzzzz: on_buy_stock {stock}')
        self.position_has_change = True
        self.op_buy_stocks.append([stock, order.filled])

    def on_sell_stock(self, position, order, is_normal, new_pindex=0):
        #print(f'--> zzzzz: on_sell_stock {stock}')
        self.position_has_change = True
        self.op_sell_stocks.append([position.security, -order.filled])

    def after_adjust_end(self, context, data):
        self.op_buy_stocks = self.merge_op_list(self.op_buy_stocks)
        self.op_sell_stocks = self.merge_op_list(self.op_sell_stocks)

    def after_trading_end(self, context):
        self.op_buy_stocks = []
        self.op_sell_stocks = []
        self.position_has_change = False

    # 对同一只股票的多次操作，进行amount合并计算。
    def merge_op_list(self, op_list):
        s_list = list(set([x[0] for x in op_list]))
        return [[s, sum([x[1] for x in op_list if x[0] == s])] for s in s_list]


# ----------------股票操作显示器---------------
class ShowPostionAdjust(OpStocksRecord):

    def after_adjust_end(self, context, data):
        # 调用父类方法
        OpStocksRecord.after_adjust_end(self, context, data)
        #print(f'--> zzzzz: len(op_sell_stocks): {len(self.op_sell_stocks)}, len(op_buy_stocks): {len(self.op_buy_stocks)}')
        # if len(self.g.buy_stocks) > 0:
        #    if len(self.g.buy_stocks) > 5:
        #        tl = self.g.buy_stocks[0:5]
        #    else:
        #        tl = self.g.buy_stocks[:]
        #    self.log.info('选股:\n' + join_list(["[%s]" % (show_stock(x)) for x in tl], ' ', 10))
        # 显示买卖日志
        if len(self.op_sell_stocks) > 0:
            self.log.info('\n%s' %
                join_list(["卖出 %s : %d" % (show_stock(x[0]), x[1]) for x in self.op_sell_stocks], '\n', 1))
        if len(self.op_buy_stocks) > 0:
            self.log.info('\n%s' %
                join_list(["买入 %s : %d" % (show_stock(x[0]), x[1]) for x in self.op_buy_stocks], '\n', 1))
        # 显示完就清除
        self.op_buy_stocks = []
        self.op_sell_stocks = []

    def __str__(self):
        return '显示调仓时买卖的股票'


# ==================================其它==============================


# -------------------------------系统参数一般性设置-------------------------------
class Set_sys_params(Rule):

    def __init__(self, params):
        Rule.__init__(self, params)
        
        try:
            # 使用真实价格交易
            self._use_real_price = self._params.get('use_real_price', True)
            set_option('use_real_price', self._use_real_price)
            
            # 设定成交量比例
            self._order_volume_ratio = float(self._params.get('order_volume_ratio', 0.2))
            set_option('order_volume_ratio', self._order_volume_ratio)
            
            # 设置回测是否开启避免未来数据模式
            #set_option("avoid_future_data", True)
        except:
            pass
        
        try:
            # 设置基准
            self._benchmark = self._params.get('benchmark', '000300.XSHG')
            set_benchmark(self._benchmark)
        except:
            pass
        
        try:
            # 过滤log
            log.set_level(*(self._params.get('level', ['order', 'error'])))
        except:
            pass
        
    def __str__(self):
        return '设置系统参数：[使用真实价格交易：%s] [设定成交量比例：%.2f] [设置基准：%s]' \
            % (self._use_real_price, self._order_volume_ratio, self._benchmark)


# ----------------设置手续费---------------
# 根据不同的时间段设置滑点与手续费并且更新指数成分股
class Set_slip_fee(Rule):
    def __init__(self, params):
        '''
        滑点设置支持三种方式：
        1、固定值： 这个价差可以是一个固定的值(比如0.02元, 交易时加减0.01元), 设定方式为：FixedSlippage(0.02)
        2、百分比： 这个价差可以是是当时价格的一个百分比(比如0.2%, 交易时加减当时价格的0.1%), 设定方式为：PriceRelatedSlippage(0.002)
        3、跳数（期货专用，双边）: 这个价差可以是合约的价格变动单位（跳数），比如2跳，设定方式为： StepRelatedSlippage(2)；滑点为小数时，向下取整，例如设置为3跳，单边1.5，向下取整为1跳。
        
        # 为全部交易品种设定固定值滑点
        set_slippage(FixedSlippage(0.02))

        # 为股票设定滑点为百分比滑点
        set_slippage(PriceRelatedSlippage(0.00246),type='stock')

        # 设置CU品种的滑点为跳数滑点2
        set_slippage(StepRelatedSlippage(2),type='futures',ref = 'CU') 

        # 为螺纹钢RB1809设定滑点为跳数滑点(注意只是这一个合约，不是所有的RB合约)
        set_slippage(StepRelatedSlippage(2),type='futures', ref="RB1809.XSGE")
        # StepRelatedSlippage(2)表示开平的单边滑点为1个价格最小单位，螺纹钢价格最小变动单位为1元/吨
        # 如果以市价单进行开多仓（或者平空仓），现价3000元，成交价3000+1*2/2=3001元
        # 如果以市价单进行开空仓（或者平多仓），现价3000元，成交价3000-1*2/2=2999元
        '''
        self.slippage = params.get('slippage', 0.02)  # 交易时各加减0.01元

    def update_params(self, context, params):
        self.slippage = params.get('slippage', self.slippage)

    def before_trading_start(self, context):
        # 根据不同的时间段设置手续费
        current_dt = context.current_dt
        if current_dt > dt.datetime(2013, 1, 1):
            set_commission(
                PerTrade(buy_cost=0.0003, sell_cost=0.0013, min_cost=5))
        elif current_dt > dt.datetime(2011, 1, 1):
            set_commission(
                PerTrade(buy_cost=0.001, sell_cost=0.002, min_cost=5))
        elif current_dt > dt.datetime(2009, 1, 1):
            set_commission(
                PerTrade(buy_cost=0.002, sell_cost=0.003, min_cost=5))
        else:
            set_commission(
                PerTrade(buy_cost=0.003, sell_cost=0.004, min_cost=5))

        # 为全部交易品种设定固定值滑点
        set_slippage(FixedSlippage(self.slippage))
        log.info('设置滑点：固定值 %.4f' % (self.slippage))

        # 为股票设定滑点为百分比滑点（！！聚宽回测时貌似无效）
        #set_slippage(PriceRelatedSlippage(self.slippage), type='stock')
        #log.info('设置滑点：相对百分比 %.2f%%' % (self.slippage * 100))

    def __str__(self):
        return '根据时间设置不同的交易费率：[设置固定滑点：%.4f]' % self.slippage


# ----------------持仓信息打印器---------------
class Show_position(Rule):
    def __init__(self, params):
        Rule.__init__(self, params)
        self.op_sell_stocks = []
        self.op_buy_stocks = []

    def after_trading_end(self, context):
        self.log.info(self.__get_portfolio_info_text(
            context, self.g.op_pindexs))
        self.op_buy_stocks = []
        self.op_buy_stocks = []

    def on_sell_stock(self, position, order, is_normal, new_pindex=0):
        self.op_sell_stocks.append([position.security, order.filled])
        pass

    def on_buy_stock(self, stock, order, new_pindex=0):
        self.op_buy_stocks.append([stock, order.filled])
        pass

    # 调仓后调用
    # def after_adjust_end(self,context,data):
    #     print self.__get_portfolio_info_text(context,self.g.op_pindexs)
    #     pass

    # 获取持仓信息，普通文本格式
    def __get_portfolio_info_text(self, context, op_sfs=[0]):
        sub_str = ''
        table = PrettyTable(["仓号", "股票数量", "持仓", "最新价", "盈亏", "持仓比", "持仓天数"])
        # table.padding_width = 1# One space between column edges and contents (default)
        for sf_id in self.g.stock_pindexs:
            cash = context.subportfolios[sf_id].cash
            p_value = context.subportfolios[sf_id].positions_value
            total_values = p_value + cash
            if sf_id in op_sfs:
                sf_id_str = str(sf_id) + ' *'
            else:
                sf_id_str = str(sf_id)
            new_stocks = [x[0] for x in self.op_buy_stocks]
            for stock in context.subportfolios[sf_id].long_positions.keys():
                position = context.subportfolios[sf_id].long_positions[stock]
                if sf_id in op_sfs and stock in new_stocks:
                    stock_str = show_stock(stock) + ' *'
                else:
                    stock_str = show_stock(stock)
                stock_raite = (position.total_amount *
                               position.price) / total_values * 100
                table.add_row([sf_id_str,
                               stock_str,
                               position.total_amount,
                               position.price,
                               "%.2f%%" % (
                                   (position.price - position.avg_cost) / position.avg_cost * 100),
                               "%.2f%%" % (stock_raite),
                               "%d" % ((context.current_dt - position.init_time).days)],
                              )
            if sf_id < len(self.g.stock_pindexs) - 1:
                table.add_row(['----', '---------------',
                               '-----', '----', '-----', '-----'])
            sub_str += '[仓号: %d] [总值:%d] [持股数:%d] [仓位:%.2f%%] \n' % (sf_id, total_values,
                            len(context.subportfolios[sf_id].long_positions), 
                            p_value * 100 / (cash + p_value))
        
        if len(context.portfolio.positions) == 0:
            return '子仓详情:\n' + sub_str
        else:
            return '子仓详情:\n' + sub_str + str(table)

    def __str__(self):
        return '持仓信息打印'


# ----------------------统计类--------------------------
class Stat(Rule):
    def __init__(self, params):
        Rule.__init__(self, params)
        # 加载统计模块
        self.trade_total_count = 0
        self.trade_success_count = 0
        self.statis = {'win': [], 'loss': []}

    def after_trading_end(self, context):
        # self.report(context)
        self.print_win_rate(context.current_dt.strftime(
            "%Y-%m-%d"), context.current_dt.strftime("%Y-%m-%d"), context)

    def on_sell_stock(self, position, order, is_normal, pindex=0):
        if order.filled > 0:
            # 只要有成交，无论全部成交还是部分成交，则统计盈亏
            self.watch(position.security, order.filled,
                       position.avg_cost, position.price)

    def reset(self):
        self.trade_total_count = 0
        self.trade_success_count = 0
        self.statis = {'win': [], 'loss': []}

    # 记录交易次数便于统计胜率
    # 卖出成功后针对卖出的量进行盈亏统计
    def watch(self, stock, sold_amount, avg_cost, cur_price):
        self.trade_total_count += 1
        current_value = sold_amount * cur_price
        cost = sold_amount * avg_cost

        percent = round((current_value - cost) / cost * 100, 2)
        if current_value > cost:
            self.trade_success_count += 1
            win = [stock, percent]
            self.statis['win'].append(win)
        else:
            loss = [stock, percent]
            self.statis['loss'].append(loss)

    def report(self, context):
        cash = context.portfolio.cash
        totol_value = context.portfolio.portfolio_value
        position = 1 - cash / totol_value
        self.log.info("收盘后持仓概况:%s" % str(list(context.portfolio.positions)))
        self.log.info("仓位概况:%.2f" % position)
        self.print_win_rate(context.current_dt.strftime(
            "%Y-%m-%d"), context.current_dt.strftime("%Y-%m-%d"), context)

    # 打印胜率
    def print_win_rate(self, current_date, print_date, context):
        if str(current_date) == str(print_date):
            win_rate = 0
            if 0 < self.trade_total_count and 0 < self.trade_success_count:
                win_rate = round(self.trade_success_count /
                                 float(self.trade_total_count), 3)

            most_win = self.statis_most_win_percent()
            most_loss = self.statis_most_loss_percent()
            starting_cash = context.portfolio.starting_cash
            total_profit = self.statis_total_profit(context)
            if len(most_win) == 0 or len(most_loss) == 0:
                return

            s = '\n----------------------------绩效报表----------------------------'
            s += '\n交易次数: {0}, 盈利次数: {1}, 胜率: {2}'.format(self.trade_total_count, self.trade_success_count,
                                                          str(win_rate * 100) + str('%'))
            s += '\n单次盈利最高: {0}, 盈利比例: {1}%'.format(
                most_win['stock'], most_win['value'])
            s += '\n单次亏损最高: {0}, 亏损比例: {1}%'.format(
                most_loss['stock'], most_loss['value'])
            s += '\n总资产: {0}, 本金: {1}, 盈利: {2}, 盈亏比率：{3}%'.format(starting_cash + total_profit, starting_cash,
                                                                  total_profit, total_profit / starting_cash * 100)
            s += '\n---------------------------------------------------------------'
            self.log.info(s)

    # 统计单次盈利最高的股票
    def statis_most_win_percent(self):
        result = {}
        for statis in self.statis['win']:
            if {} == result:
                result['stock'] = statis[0]
                result['value'] = statis[1]
            else:
                if statis[1] > result['value']:
                    result['stock'] = statis[0]
                    result['value'] = statis[1]

        return result

    # 统计单次亏损最高的股票
    def statis_most_loss_percent(self):
        result = {}
        for statis in self.statis['loss']:
            if {} == result:
                result['stock'] = statis[0]
                result['value'] = statis[1]
            else:
                if statis[1] < result['value']:
                    result['stock'] = statis[0]
                    result['value'] = statis[1]

        return result

    # 统计总盈利金额
    def statis_total_profit(self, context):
        return context.portfolio.portfolio_value - context.portfolio.starting_cash

    def __str__(self):
        return '策略绩效统计'


# ===============================其它基础函数==================================


def get_growth_rate(security, n=20):
    '''
    获取股票n日以来涨幅，根据当前价(前1分钟的close）计算
    n 默认20日
    :param security:
    :param n:
    :return: float
    '''
    lc = get_close_price(security, n)
    c = get_close_price(security, 1, '1m')

    if not isnan(lc) and not isnan(c) and lc != 0:
        return (c - lc) / lc
    else:
        log.error("数据非法, security: %s, %d日收盘价: %f, 当前价: %f" %
                  (security, n, lc, c))
        return 0


def get_close_price(security, n, unit='1d'):
    '''
    获取前n个单位时间当时的收盘价
    为防止取不到收盘价，试3遍
    :param security:
    :param n:
    :param unit: '1d'/'1m'
    :return: float
    '''
    cur_price = np.nan
    for i in range(3):
        cur_price = attribute_history(
            security, n, unit, 'close', True)['close'][0]
        if not math.isnan(cur_price):
            break
    return cur_price


# 获取一个对象的类名
def get_obj_class_name(obj):
    cn = str(obj.__class__)
    cn = cn[cn.find('.') + 1:]
    return cn[:cn.find("'")]


def show_stock(stock):
    '''
    获取股票代码的显示信息
    :param stock: 股票代码，例如: '603822.XSHG'
    :return: str，例如：'603822 嘉澳环保'
    '''
    return "%s %s" % (stock[:6], get_security_info(stock).display_name)


def join_list(pl, connector=' ', step=5):
    '''
    将list组合为str,按分隔符和步长换行显示(List的成员必须为字符型)
    例如：['1','2','3','4'],'~',2  => '1~2\n3~4'
    :param pl: List
    :param connector: 分隔符，默认空格
    :param step: 步长，默认5
    :return: str
    '''
    result = ''
    for i in range(len(pl)):
        result += pl[i]
        if (i + 1) % step == 0:
            result += '\n'
        else:
            result += connector
    return result


class Purchase_new_stocks(Rule):
    """通过实盘易申购新股"""

    def __init__(self, params):
        Rule.__init__(self, params)
        self.times = params.get('times', [[10, 00]])
        self.host = params.get('host', '')
        self.port = params.get('port', 8888)
        self.key = params.get('key', '')
        self.clients = params.get('clients', [])

    def update_params(self, context, params):
        Rule.update_params(self, context, params)
        self.times = params.get('times', [[10, 00]])
        self.host = params.get('host', '')
        self.port = params.get('port', 8888)
        self.key = params.get('key', '')
        self.clients = params.get('clients', [])

    def handle_data(self, context, data):
        hour = context.current_dt.hour
        minute = context.current_dt.minute
        if not [hour, minute] in self.times:
            return
        try:
            import shipane_sdk
        except:
            pass
        shipane = shipane_sdk.Client(g.log_type(context, self.memo), key=self.key, host=self.host, port=self.port,
                                     show_info=False)
        for client_param in self.clients:
            shipane.purchase_new_stocks(client_param)

    def __str__(self):
        return '实盘易申购新股[time: %s host: %s:%d  key: %s client:%s] ' % (
            self.times, self.host, self.port, self.key, self.clients)


# ----------------邮件通知器---------------
class Email_notice(OpStocksRecord):
    def __init__(self, params):
        OpStocksRecord.__init__(self, params)
        self.user = params.get('user', '')
        self.password = params.get('password', '')
        self.tos = params.get('tos', '')
        self.sender_name = params.get('sender', '发送者')
        self.strategy_name = params.get('strategy_name', '策略1')
        self.str_old_portfolio = ''

    def update_params(self, context, params):
        OpStocksRecord.update_params(self, context, params)
        self.user = params.get('user', '')
        self.password = params.get('password', '')
        self.tos = params.get('tos', '')
        self.sender_name = params.get('sender', '发送者')
        self.strategy_name = params.get('strategy_name', '策略1')
        self.str_old_portfolio = ''
        try:
            OpStocksRecord.update_params(self, context, params)
        except:
            pass

    def before_adjust_start(self, context, data):
        OpStocksRecord.before_trading_start(self, context)
        self.str_old_portfolio = self.__get_portfolio_info_html(context)
        pass

    def after_adjust_end(self, context, data):
        OpStocksRecord.after_adjust_end(self, context, data)
        try:
            send_time = self._params.get('send_time', [])
        except:
            send_time = []
        if self._params.get('send_with_change', True) and not self.position_has_change:
            return
        if len(send_time) == 0 or [context.current_dt.hour, context.current_dt.minute] in send_time:
            self.__send_email('%s:调仓结果' % (self.strategy_name), self.__get_mail_text_before_adjust(
                context, '', self.str_old_portfolio, self.op_sell_stocks, self.op_buy_stocks))
            self.position_has_change = False  # 发送完邮件，重置标记

    def after_trading_end(self, context):
        OpStocksRecord.after_trading_end(self, context)
        self.str_old_portfolio = ''

    def on_clear_position(self, context, new_pindexs=[0]):
        # 清仓通知
        self.op_buy_stocks = self.merge_op_list(self.op_buy_stocks)
        self.op_sell_stocks = self.merge_op_list(self.op_sell_stocks)
        if len(self.op_buy_stocks) > 0 or len(self.op_sell_stocks) > 0:
            self.__send_email('%s:清仓' % (self.strategy_name), '已触发清仓')
            self.op_buy_stocks = []
            self.op_sell_stocks = []
        pass

    # 发送邮件 subject 为邮件主题,content为邮件正文(当前默认为文本邮件)
    def __send_email(self, subject, text):
        # # 发送邮件
        username = self.user  # 你的邮箱账号
        password = self.password  # 你的邮箱授权码。一个16位字符串

        sender = '%s<%s>' % (self.sender_name, self.user)

        msg = MIMEText("<pre>" + text + "</pre>", 'html', 'utf-8')
        msg['Subject'] = Header(subject, 'utf-8')
        msg['to'] = ';'.join(self.tos)
        msg['from'] = sender  # 自己的邮件地址

        server = smtplib.SMTP_SSL('smtp.qq.com')
        try:
            # server.connect() # ssl无需这条
            server.login(username, password)  # 登陆
            server.sendmail(sender, self.tos, msg.as_string())  # 发送
            self.log.info('邮件发送成功:' + subject)
        except:
            self.log.info('邮件发送失败:' + subject)
        server.quit()  # 结束

    def __get_mail_text_before_adjust(self, context, op_info, str_old_portfolio,
                                      to_sell_stocks, to_buy_stocks):
        # 获取又买又卖的股票，实质为调仓
        mailtext = context.current_dt.strftime("%Y-%m-%d %H:%M:%S")
        if len(self.g.buy_stocks) >= 5:
            mailtext += '<br>选股前5:<br>' + \
                ''.join(['%s<br>' % (show_stock(x))
                         for x in self.g.buy_stocks[:5]])
            mailtext += '--------------------------------<br>'
        # mailtext += '<br><font color="blue">'+op_info+'</font><br>'
        if len(to_sell_stocks) + len(to_buy_stocks) == 0:
            mailtext += '<br><font size="5" color="red">* 无需调仓! *</font><br>'
            mailtext += '<br>当前持仓:<br>'
        else:
            #             mailtext += '<br>==> 调仓前持仓:<br>'+str_old_portfolio+"<br>==> 执行调仓<br>--------------------------------<br>"
            mailtext += '卖出股票:<br><font color="blue">'
            mailtext += ''.join(['%s %d<br>' % (show_stock(x[0]), x[1])
                                 for x in to_sell_stocks])
            mailtext += '</font>--------------------------------<br>'
            mailtext += '买入股票:<br><font color="red">'
            mailtext += ''.join(['%s %d<br>' % (show_stock(x[0]), x[1])
                                 for x in to_buy_stocks])
            mailtext += '</font>'
            mailtext += '<br>==> 调仓后持仓:<br>'
        mailtext += self.__get_portfolio_info_html(context)
        return mailtext

    def __get_portfolio_info_html(self, context):
        total_values = context.portfolio.positions_value + context.portfolio.cash
        position_str = "--------------------------------<br>"
        position_str += "总市值 : [ %d ]<br>持仓市值: [ %d ]<br>现金   : [ %d ]<br>" % (
            total_values,
            context.portfolio.positions_value, context.portfolio.cash
        )
        position_str += "<table border=\"1\"><tr><th>股票代码</th><th>持仓</th><th>当前价</th><th>盈亏</th><th>持仓比</th></tr>"
        for stock in context.portfolio.positions.keys():
            position = context.portfolio.positions[stock]
            if position.price - position.avg_cost > 0:
                tr_color = 'red'
            else:
                tr_color = 'green'
            stock_raite = (position.total_amount *
                           position.price) / total_values * 100
            position_str += '<tr style="color:%s"><td> %s </td><td> %d </td><td> %.2f </td><td> %.2f%% </td><td> %.2f%%</td></tr>' % (
                tr_color,
                show_stock(stock),
                position.total_amount, position.price,
                (position.price - position.avg_cost) / position.avg_cost * 100,
                stock_raite
            )

        return position_str + '</table>'

    def __str__(self):
        return '调仓结果邮件通知:[发送人:%s] [接收人:%s]' % (self.sender_name, str(self.tos))


# ================================= 以下为 zzz 修改的代码 =================================

class StopLossByIndividualPrice(Rule):
    """个股价格止损器 - 当前价格低于成本价达到阈值则平仓止损"""

    def __init__(self, params):
        self.threshold = params.get('threshold', 0.05)
        self.bypass_buy_list = params.get(
            'bypass_buy_list', False)  # 对在当前买入列表中的是否不做止损

    def update_params(self, context, params):
        self.threshold = params.get('threshold', self.threshold)
        self.bypass_buy_list = params.get(
            'bypass_buy_list', self.bypass_buy_list)

    def before_trading_start(self, context):
        pass

    # 个股止损
    def handle_data(self, context, data):
        for pindex in self.g.op_pindexs:
            for stock in context.subportfolios[pindex].long_positions.keys():
                position = context.subportfolios[pindex].long_positions[stock]
                cur_price = data[stock].close
                avg_cost = position.avg_cost
                if cur_price <= avg_cost * (1 - self.threshold):
                    if self.bypass_buy_list and stock in self.g.buy_stocks:
                        self.log.info("==> %s 到达止损线，但位于当前买入列表中，忽略止损。" % stock)
                        continue
                    msg = "==> stock: %s, cur_price: %f, avg_cost: %f, value: %f" \
                          % (stock, cur_price, avg_cost, position.value)
                    if position.init_time.date() != context.current_dt.date():
                        self.log.info(msg)
                    self.g.close_position(context, position, True, pindex)

    def __str__(self):
        return '个股价格止损: [阈值: %f，是否忽略买入列表：%s]' % (self.threshold, self.bypass_buy_list)


class StopLossByIndexPrice(Rule):
    """指数价格止损器"""
    # 指标股（一般为大盘指数）前130日内最高价超过最低价2倍，则清仓止损
    # 基于历史数据判定，因此若状态满足，则当天都不会变化
    # 增加此止损，回撤降低，收益降低
    
    def __init__(self, params):
        self.index_stock = params.get('index_stock', '000001.XSHG')
        self.day_count = params.get('day_count', 160)
        self.multiple = params.get('multiple', 2.2)
        self.is_stop_loss = False

    def update_params(self, context, params):
        self.index_stock = params.get('index_stock', self.index_stock)
        self.day_count = params.get('day_count', self.day_count)
        self.multiple = params.get('multiple', self.multiple)

    def handle_data(self, context, data):
        if not self.is_stop_loss:
            h = attribute_history(self.index_stock, self.day_count, unit='1d', fields=('close', 'high', 'low'),
                                  skip_paused=True)
            low_price_130 = h.low.min()
            high_price_130 = h.high.max()
            if high_price_130 > self.multiple * low_price_130 and h['close'][-1] < h['close'][-4] * 1 and \
                    h['close'][
                        -1] > h['close'][-100]:
                # 当日第一次输出日志
                self.log.info("==> 大盘止损，%s 前130日内最高价超过最低价2倍, 最高价: %f, 最低价: %f" % (
                    get_security_info(self.index_stock).display_name, high_price_130, low_price_130))
                self.is_stop_loss = True

        if self.is_stop_loss:
            self.g.clear_position(context, self.g.op_pindexs)
        self.is_to_return = self.is_stop_loss

    def before_trading_start(self, context):
        self.is_stop_loss = False
        pass

    def __str__(self):
        return '指数价格止损器: [%s] [参数: %s日内最高最低价: %s倍] [当前状态: %s]' % (
            self.index_stock, self.day_count, self.multiple, self.is_stop_loss)


class StopLossByIndexsIncrease(Rule):
    '''多指数N日涨幅止损'''

    def __init__(self, params):
        Rule.__init__(self, params)
        self._indexs = params.get('indexs', [])
        self._min_rate = params.get('min_rate', 0.01)
        self._n = params.get('n', 20)
        self._stop_pass_check = params.get(
            'stop_pass_check', False)  # 是否开启止损暂停交易日
        self._stop_pass_days = 0  # 止损后暂停交易天数计数
        self._stop_record_days = 90  # 止损记录天数
        self._stop_record = []  # 记录期间内产生的止损日期

    def update_params(self, context, params):
        Rule.__init__(self, params)
        self._indexs = params.get('indexs', self._indexs)
        self._min_rate = params.get('min_rate', self._min_rate)
        self._n = params.get('n', self._n)
        self._stop_pass_check = params.get(
            'stop_pass_check', self._stop_pass_check)

    def before_trading_start(self, context):
        if self._stop_pass_days > 0:
            self._stop_pass_days -= 1
        if self._stop_pass_check and self._stop_pass_days > 0:
            self.log.warn('处于止损暂停期，还剩 %d 天。' % self._stop_pass_days)

    def handle_data(self, context, data):
        hour = context.current_dt.hour
        minute = context.current_dt.minute
        if (9 == hour and 31 == minute) or (13 == hour and 31 == minute):
            pass
        else:
            return

        tmp_record = []
        cur_day = context.current_dt
        for i in self._stop_record:
            if i not in tmp_record and (cur_day - i).days <= self._stop_record_days:
                tmp_record.append(i)

        self.is_to_return = False
        r = []
        for index in self._indexs:
            gr_index = get_growth_rate(index, self._n)
            # self.log.info('%s %d日涨幅  %.2f%%' % (show_stock(index), self._n, gr_index * 100))
            r.append(gr_index > self._min_rate)
        if sum(r) == 0:
            # self.log.warn('不符合持仓条件，清仓')
            self.g.clear_position(context, self.g.op_pindexs)
            self.is_to_return = True
            if cur_day not in tmp_record:
                tmp_record.append(cur_day)
            self._stop_pass_days = len(
                tmp_record) * self._n / 2  # 最近n天止损次数 * 10 天
            # self._stop_pass_days = int((1 + math.log(len(tmp_record))) * self._n)

        if self._stop_pass_check and self._stop_pass_days > 0:
            # self.log.warn('处于止损暂停期，还剩 %d 天。' % self._stop_pass_days)
            self.is_to_return = True
            return

        self._stop_record = tmp_record
        if self._stop_pass_check:
            self.log.info('--> 最近 %d 天止损次数: %d, self._stop_pass_days: %d' %
                          (self._stop_record_days, len(self._stop_record), self._stop_pass_days))

    def after_trading_end(self, context):
        Rule.after_trading_end(self, context)
        for index in self._indexs:
            gr_index = get_growth_rate(index, self._n - 1)
            self.log.info('%s %d日涨幅  %.2f%% ' %
                          (show_stock(index), self._n - 1, gr_index * 100))

    def __str__(self):
        return '多指数20日涨幅损器[指数:%s] [涨幅:%.2f%%]' % (str(self._indexs), self._min_rate * 100)


class StopLossBy3BlackCrows(Rule):
    """三乌鸦止损"""

    def __init__(self, params):
        self.index_stock = params.get('index_stock', '000001.XSHG')
        self.dst_drop_minute_count = params.get(
            'dst_drop_minute_count', 60)  # 如果是按天执行。该值需设为0
        # 临时参数
        self.is_last_day_3_black_crows = False
        self.cur_drop_minute_count = 0

    def update_params(self, context, params):
        self.index_stock = params.get('index_stock', self.index_stock)
        self.dst_drop_minute_count = params.get(
            'dst_drop_minute_count', self.dst_drop_minute_count)

    def initialize(self, context):
        pass

    def handle_data(self, context, data):
        # 前日三黑鸦，累计当日每分钟涨幅<0的分钟计数
        # 如果分钟计数超过一定值，则开始进行三黑鸦止损
        # 避免无效三黑鸦乱止损
        if self.is_last_day_3_black_crows:
            if get_growth_rate(self.index_stock, 1) < 0:
                self.cur_drop_minute_count += 1

            if self.cur_drop_minute_count >= self.dst_drop_minute_count:
                if self.cur_drop_minute_count == self.dst_drop_minute_count:
                    msg = "==> 超过三黑鸦止损开始"
                    self.log.warn(msg)
                    self.g.clear_position(context, self.g.op_pindexs)
                self.is_to_return = True
        else:
            self.is_to_return = False

    def before_trading_start(self, context):

        def is_3_black_crows(stock):
            # talib.CDL3BLACKCROWS

            # 三只乌鸦说明来自百度百科
            # 1. 连续出现三根阴线，每天的收盘价均低于上一日的收盘
            # 2. 三根阴线前一天的市场趋势应该为上涨
            # 3. 三根阴线必须为长的黑色实体，且长度应该大致相等
            # 4. 收盘价接近每日的最低价位
            # 5. 每日的开盘价都在上根K线的实体部分之内；
            # 6. 第一根阴线的实体部分，最好低于上日的最高价位
            #
            # 算法
            # 有效三只乌鸦描述众说纷纭，这里放宽条件，只考虑1和2
            # 根据前4日数据判断
            # 3根阴线跌幅超过4.5%（此条件忽略）

            h = attribute_history(
                stock, 4, '1d', ('close', 'open'), skip_paused=True, df=False)
            h_close = list(h['close'])
            h_open = list(h['open'])

            if len(h_close) < 4 or len(h_open) < 4:
                return False

            # 一阳三阴
            if h_close[-4] > h_open[-4] \
                    and (h_close[-1] < h_open[-1] and h_close[-2] < h_open[-2] and h_close[-3] < h_open[-3]):
                # and (h_close[-1] < h_close[-2] and h_close[-2] < h_close[-3]) \
                # and h_close[-1] / h_close[-4] - 1 < -0.045:
                return True
            return False

        self.is_last_day_3_black_crows = is_3_black_crows(self.index_stock)
        if self.is_last_day_3_black_crows:
            self.log.info("==> 前4日已经构成三黑鸦形态")

    def after_trading_end(self, context):
        self.is_last_day_3_black_crows = False
        self.cur_drop_minute_count = 0

    def __str__(self):
        return '大盘三乌鸦止损器:[指数: %s] [跌计数分钟: %d] [当前状态: %s]' % (
            self.index_stock, self.dst_drop_minute_count, self.is_last_day_3_black_crows)


class StopLossByCapitalTrend(Rule):
    """资金曲线止损"""

    # 资金曲线上涨，可以忽视大盘方向；
    # 资金曲线走平 & 大盘上涨，持仓等待；
    # 资金曲线下行 & 大盘上涨，换股；
    # 资金曲线下行 & 大盘下行，清仓。

    def __init__(self, params):
        Rule.__init__(self, params)

        self._n = params.get('n', 20)  # 资金损益计算周期（天数）
        self._check_avg_capital = params.get(
            'check_avg_capital', False)  # 是否与n天平均值比较（否则为n天前）
        self._loss_rate_max = params.get('loss_rate_max', 0.01)  # 最大资金损失率
        self._stop_pass_check = params.get(
            'stop_pass_check', False)  # 是否开启止损暂停交易期功能

        self._stop_pass_days = 0  # 止损暂停交易期剩余天数
        self._stop_record_days = self._n * 2  # 止损事件最大记录天数
        self._stop_record_list = []  # 最大记录天数内的止损事件列表（日期）
        self._value_list = []  # 每天尾盘记录的总资产
        self._is_stop_loss = False  # 当天是否已激活了资金止损

    def update_params(self, context, params):
        self._n = params.get('n', self._n)
        self._check_avg_capital = params.get(
            'check_avg_capital', self._check_avg_capital)
        self._loss_rate_max = params.get('loss_rate_max', self._loss_rate_max)
        self._stop_pass_check = params.get(
            'stop_pass_check', self._stop_pass_check)

    def before_trading_start(self, context):
        if self._stop_pass_days > 0:  # 止损暂停期递减
            self._stop_pass_days -= 1
        if self._stop_pass_check and self._stop_pass_days > 0:
            self.log.warn('处于止损暂停期，还剩 %d 天。' % self._stop_pass_days)

        # 清除超过最大记录期限的止损记录
        tmp_record = []
        cur_day = context.current_dt
        for i in self._stop_record_list:
            if i not in tmp_record and (cur_day - i).days <= self._stop_record_days:
                tmp_record.append(i)
        self._stop_record_list = tmp_record

    def handle_data(self, context, data):
        # hour = context.current_dt.hour
        # minute = context.current_dt.minute
        # if (9 == hour and 31 == minute) or (13 == hour and 31 == minute):
        #    pass
        # else:
        #    return

        # 先检查资损暂停期
        if self._stop_pass_check and self._stop_pass_days > 0:  # 止损期还没完
            self.is_to_return = True
            return

        # 再进行资金曲线止损计算
        if self._equity_curve_protect(context):
            self.is_to_return = True

            cur_day = context.current_dt
            if cur_day not in self._stop_record_list:
                self._stop_record_list.append(cur_day)
            self._stop_pass_days = len(self._stop_record_list) * self._n / 2
            # self._stop_pass_days = int((1 + math.log(len(self._stop_record_list))) * self._n)

    def _equity_curve_protect(self, context):
        if not self._is_stop_loss:  # 每天只激活资金曲线止损计算1次
            cur_value = context.portfolio.total_value
            if len(self._value_list) >= self._n:
                check_value = self._value_list[-self._n]
                if self._check_avg_capital:
                    check_value = sum(self._value_list[-self._n:]) / self._n
                if cur_value < check_value * (1 - self._loss_rate_max):
                    self.g.clear_position(context, self.g.op_pindexs)
                    del self._value_list[:]  # 需要，清空后重新开始计算
                    self.log.warn("==> 启动资金曲线保护, %d日前（或平均）资产: %f, 当前资产: %f" % (
                        self._n, check_value, cur_value))
                    self._is_stop_loss = True

        return self._is_stop_loss

    def after_trading_end(self, context):
        Rule.after_trading_end(self, context)
        self._is_stop_loss = False

        # 每天尾盘记录总资产
        if self._stop_pass_days == 0:  # 止损暂停期不记录
            self._value_list.append(context.portfolio.total_value)

        if self._stop_pass_check:
            self.log.info('--> 最近 %d 天止损次数: %d, 止损暂停期还剩 %d 天' %
                          (self._stop_record_days, len(self._stop_record_list), self._stop_pass_days))

    def __str__(self):
        return '资金曲线止损 [资金损益计算周期: %d 天; 是否对比平均资产: %s; 最大损失率: %.2f%%; 是否开启止损暂停交易期: %s]' \
               % (self._n, self._check_avg_capital, self._loss_rate_max * 100, self._stop_pass_check)


class StopProfitByIndividualPrice(Rule):
    """个股价格止盈器 - 当前价格高于成本价达到阈值则平仓止盈"""

    def __init__(self, params):
        self.threshold = params.get('threshold', 0.15)

    def update_params(self, context, params):
        self.threshold = params.get('threshold', self.threshold)

    def before_trading_start(self, context):
        pass

    # 个股止盈
    def handle_data(self, context, data):
        for pindex in self.g.op_pindexs:
            for stock in context.subportfolios[pindex].long_positions.keys():
                position = context.subportfolios[pindex].long_positions[stock]
                cur_price = data[stock].close
                avg_cost = position.avg_cost
                if cur_price >= avg_cost * (1 + self.threshold):
                    msg = "==> stock: %s, cur_price: %f, avg_cost: %f, value: %f" \
                          % (stock, cur_price, avg_cost, position.value)
                    if position.init_time.date() != context.current_dt.date():
                        self.log.info(msg)
                    self.g.close_position(context, position, True, pindex)

    def __str__(self):
        return '个股价格止盈: [阈值: %f]' % (self.threshold)


class Filter_by_index(FilterStockList):
    '''选取成分股'''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.just_pick = params.get('just_pick', True)  # 直接选股不做过滤
        self.index_list = params.get('index_list', [])

    def update_params(self, context, params):
        self.just_pick = params.get('just_pick', self.just_pick)
        self.index_list = params.get('index_list', self.index_list)

    def filter(self, context, data, stock_list):
        stock_pool = []
        for index in self.index_list:
            stocks = get_index_stocks(index)
            # log.info('    got %d stocks from %s: %s' % (len(stocks), index, stocks[:3]))
            if stocks:
                stock_pool.extend(stocks)

        if self.just_pick:
            stock_list = stock_pool
        else:
            # log.info('before filter: stock_list total %d - %s' % (len(stock_list), stock_list[:3]))
            # stock_list = list(set(stock_list).intersection(set(stock_pool)))
            stock_list = [val for val in stock_list if val in stock_pool]
            # log.info('after filter: stock_list total %d' % len(stock_list))

        return stock_list

    def __str__(self):
        return '选取成分股中的股票：[%s]' % self.index_list


class Filter_by_number(FilterStockList):
    '''选取前N支股，支持最小持仓天数检查（注意！！该过滤器要放在选股的最后）'''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.holding_days_range = params.get(
            'holding_days_range', (0, 500))  # 最小和最大持仓天数范围
        self.number = params.get('number', 500)
        self.skip_head = params.get('skip_head', 0)  # 跳过开头多少股
        self.holding_records = {}

    def update_params(self, context, params):
        self.holding_days_range = params.get(
            'holding_days_range', self.holding_days_range)
        self.number = params.get('number', self.number)
        self.skip_head = params.get('skip_head', self.skip_head)

    def filter(self, context, data, stock_list):
        if self.holding_days_range[0] <= 0:
            return stock_list[self.skip_head:][:self.number]
        else:
            buy_list = []
            cur_day = context.current_dt

            # 对于已持仓股票，未满足最小持仓天数的继续保留
            for stock in context.subportfolios[0].long_positions.keys():
                position = context.subportfolios[0].long_positions[stock]
                delta_days = (cur_day - position.init_time).days
                if delta_days < self.holding_days_range[0]:
                    buy_list.append(stock)
                    if position.init_time.date() != context.current_dt.date():
                        # self.log.info('  未满足最小持仓天数，继续保留：%s, delta_days: %d' % (stock, delta_days))
                        pass

            # 对于待购买股票，若已持仓且超过最大持仓天数则强制清仓，其余加入持仓并记录开仓日期
            for stock in stock_list[self.skip_head:]:
                if len(buy_list) < self.number:
                    if stock in context.subportfolios[0].long_positions.keys():
                        position = context.subportfolios[0].long_positions[stock]
                        delta_days = (cur_day - position.init_time).days
                        if delta_days < self.holding_days_range[1]:
                            buy_list.append(stock)
                            # if position.init_time.date() != context.current_dt.date():
                            #    self.log.info('  已在买入列表中，继续保留：%s' % stock)
                        else:
                            self.log.info('  超过最大持仓天数，不再保留：%s' % stock)
                    elif stock not in buy_list:
                        buy_list.append(stock)
                        # self.log.info('  新买入：%s' % stock)

            return buy_list

    def __str__(self):
        return '【最终】选取前N支股：[%d] 最小和最大持仓天数：[%s]，跳过开头股数：[%d]' % (self.number, self.holding_days_range, self.skip_head)


class Filter_low_open_high_open(FilterStockList):
    '''高开买（低开卖）过滤'''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.price_ratio = params.get('price_ratio', 1.00)  # 限定价格比

    def update_params(self, context, params):
        self.price_ratio = params.get('price_ratio', self.price_ratio)

    def filter(self, context, data, stock_list):
        buyList = []

        for stock in stock_list:
            # 今日开盘价大于昨日收盘价则买入
            df1 = attribute_history(stock, 10, unit='1d', fields=(
                'open', 'close'), skip_paused=True)
            yesterday_close = df1['close'].values[-1:][-1]

            df2 = get_price(stock, count=5, end_date=context.current_dt, fields=[
                            'open', 'close'])
            today_open = df2['open'].values[-1:][-1]
            if today_open / yesterday_close >= self.price_ratio and stock not in buyList:
                buyList.append(stock)
                self.log.info(
                    '--> stock: %s, yesterday_close: %.2f, today_open:%.2f' % (stock, yesterday_close, today_open))

        self.log.info('过滤后的股数：%d' % len(buyList))
        return buyList

    def __str__(self):
        return '高开买低开卖过滤'


class Filter_by_increase(FilterStockList):
    '''根据涨幅过滤'''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        # self.times = params.get('times', [])  # 执行时间列表，为空表任意时刻
        self.days_count = params.get('days_count', 5)  # 计算多少天的数据
        self.enable_minute_data = params.get(
            'enable_minute_data', False)  # 是否加上当时分钟数据
        self.last_day_range = params.get(
            'last_day_range', (0.01, 0.10))  # 最后一天涨幅范围限制
        self.filter_first_ndays = params.get(
            'filter_first_ndays', True)  # 是否对前几日涨幅进行过滤
        self.first_ndays_range = params.get(
            'first_ndays_range', (0.01, 0.09))  # 前几日涨幅范围限制
        # 根据第几天的数值做排序，取值为 -days_count ~ -2
        self.sort_day_index = params.get('sort_day_index', -2)
        self.sort_type = params.get('sort_type', 'asc')  # 排序方向，asc / dsc

    def update_params(self, context, params):
        # self.times = params.get('times', self.times)
        self.days_count = params.get('days_count', self.days_count)
        self.enable_minute_data = params.get(
            'enable_minute_data', self.enable_minute_data)
        self.last_day_range = params.get('last_day_range', self.last_day_range)
        self.filter_first_ndays = params.get(
            'filter_first_ndays', self.filter_first_ndays)
        self.first_ndays_range = params.get(
            'first_ndays_range', self.first_ndays_range)
        self.sort_day_index = params.get('sort_day_index', self.sort_day_index)
        self.sort_type = params.get('sort_type', self.sort_type)

    def filter(self, context, data, stock_list):
        # if len(self.times) > 0:
        #    hour = context.current_dt.hour
        #    minute = context.current_dt.minute
        #    if not [hour, minute] in self.times:
        #        self.log.debug('不在时间序列，返回空结果')
        #        return []

        stocks_selected = FuncLib.get_rising_stocks(context, enable_minute_data=self.enable_minute_data,
                                                      stock_list=stock_list,
                                                      days_count=self.days_count,
                                                      last_day_range=self.last_day_range,
                                                      filter_first_ndays=self.filter_first_ndays,
                                                      first_ndays_range=self.first_ndays_range,
                                                      sort_day_index=self.sort_day_index,
                                                      sort_type=self.sort_type)
        tmpList = []
        for stock in stocks_selected:
            if stock in stock_list:
                tmpList.append(stock)

        # if len(tmpList) > 0:
        #    self.log.info('过滤后列表: %s' % tmpList)
        return tmpList

    def __str__(self):
        return '根据涨幅过滤 [计算 %d 天的数据，是否加上当时分钟数据: %s, 最后一天范围限制: %s, 是否对前几日涨幅进行过滤: %s, 前几日涨幅范围限制: %s]' % \
               (self.days_count, self.enable_minute_data, self.last_day_range, self.filter_first_ndays,
                self.first_ndays_range)


class Filter_by_fluctuate(FilterStockList):
    '''根据波动率过滤'''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.days = params.get('days', 60)  # 波动率计算天数
        self.sort_type = params.get('sort_type', 'asc')  # 排序方向，asc / dsc
        self.select_count = params.get('select_count', 5)  # 选取股数

    def update_params(self, context, params):
        self.days = params.get('days', self.days)
        self.sort_type = params.get('sort_type', self.sort_type)
        self.select_count = params.get('select_count', self.select_count)

    def filter(self, context, data, stock_list):
        stocks = FuncLib.get_stocks_by_fluctuate_sort(
            stock_list, days=60, sort_type=self.sort_type)
        return stocks[:self.select_count]

    def __str__(self):
        return '根据波动率过滤 [计算 %d 天的数据，排序方式: %s，选取股数: %d]' % \
               (self.days, self.sort_type, self.select_count)


class Filter_peg(FilterStockList):
    '''PEG值过滤'''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.remove_cycle_industry = params.get(
            'remove_cycle_industry', False)  # 是否剔除周期性行业
        # 取值：peg-按peg值排序；cap-按市值排序；as-按成长率逆排序
        self.result_order_by = params.get('result_order_by', 'peg')  # 结果排序方式

    def update_params(self, context, params):
        self.remove_cycle_industry = params.get(
            'remove_cycle_industry', self.remove_cycle_industry)
        self.result_order_by = params.get(
            'result_order_by', self.result_order_by)

    # 剔除周期性行业
    def fun_remove_cycle_industry(self, stock_list):
        cycle_industry = [  # 'A01', #	农业 	1993-09-17
            # 'A02', # 林业 	1996-12-06
            'A03',  # 畜牧业 	1997-06-11
            # 'A04', #	渔业 	1993-05-07
            # 'A05', #	农、林、牧、渔服务业 	1997-05-30
            'B06',  # 煤炭开采和洗选业 	1994-01-06
            'B07',  # 石油和天然气开采业 	1996-06-28
            'B08',  # 黑色金属矿采选业 	1997-07-08
            'B09',  # 有色金属矿采选业 	1996-03-20
            'B11',  # 开采辅助活动 	2002-02-05
            # 'C13', #	农副食品加工业 	1993-12-15
            # C14 	食品制造业 	1994-08-18
            # C15 	酒、饮料和精制茶制造业 	1992-10-12
            # C17 	纺织业 	1992-06-16
            # C18 	纺织服装、服饰业 	1993-12-31
            # C19 	皮革、毛皮、羽毛及其制品和制鞋业 	1994-04-04
            # C20 	木材加工及木、竹、藤、棕、草制品业 	2005-05-10
            # C21 	家具制造业 	1996-04-25
            # C22 	造纸及纸制品业 	1993-03-12
            'C23',  # 印刷和记录媒介复制业 	1994-02-24
            # C24 	文教、工美、体育和娱乐用品制造业 	2007-01-10
            'C25',  # 石油加工、炼焦及核燃料加工业 	1993-10-25
            'C26',  # 化学原料及化学制品制造业 	1990-12-19
            # C27 	医药制造业 	1993-06-29
            'C28',  # 化学纤维制造业 	1993-07-28
            'C29',  # 橡胶和塑料制品业 	1992-08-28
            'C30',  # 非金属矿物制品业 	1992-02-28
            'C31',  # 黑色金属冶炼及压延加工业 	1994-01-06
            'C32',  # 有色金属冶炼和压延加工业 	1996-02-15
            'C33',  # 金属制品业 	1993-11-30
            'C34',  # 通用设备制造业 	1992-03-27
            'C35',  # 专用设备制造业 	1992-07-01
            'C36',  # 汽车制造业 	1992-07-24
            'C37',  # 铁路、船舶、航空航天和其它运输设备制造业 	1992-03-31
            'C38',  # 电气机械及器材制造业 	1990-12-19
            # C39 	计算机、通信和其他电子设备制造业 	1990-12-19
            # C40 	仪器仪表制造业 	1993-09-17
            'C41',  # 其他制造业 	1992-08-14
            # C42 	废弃资源综合利用业 	2012-10-26
            'D44',  # 电力、热力生产和供应业 	1993-04-16
            # D45 	燃气生产和供应业 	2000-12-11
            # D46 	水的生产和供应业 	1994-02-24
            'E47',  # 房屋建筑业 	1993-04-29
            'E48',  # 土木工程建筑业 	1994-01-28
            'E50',  # 建筑装饰和其他建筑业 	1997-05-22
            # F51 	批发业 	1992-05-06
            # F52 	零售业 	1992-09-02
            'G53',  # 铁路运输业 	1998-05-11
            'G54',  # 道路运输业 	1991-01-14
            'G55',  # 水上运输业 	1993-11-19
            'G56',  # 航空运输业 	1997-11-05
            'G58',  # 装卸搬运和运输代理业 	1993-05-05
            # G59 	仓储业 	1996-06-14
            # H61 	住宿业 	1993-11-18
            # H62 	餐饮业 	1997-04-30
            # I63 	电信、广播电视和卫星传输服务 	1992-12-02
            # I64 	互联网和相关服务 	1992-05-07
            # I65 	软件和信息技术服务业 	1992-08-20
            # 'J66',  # 货币金融服务 	1991-04-03
            # 'J67',  # 资本市场服务 	1994-01-10
            # 'J68',  # 保险业 	2007-01-09
            # 'J69',  # 其他金融业 	2012-10-26
            'K70',  # 房地产业 	1992-01-13
            # L71 	租赁业 	1997-01-30
            # L72 	商务服务业 	1996-08-29
            # M73 	研究和试验发展 	2012-10-26
            'M74',  # 专业技术服务业 	2007-02-15
            # N77 	生态保护和环境治理业 	2012-10-26
            # N78 	公共设施管理业 	1992-08-07
            # P82 	教育 	2012-10-26
            # Q83 	卫生 	2007-02-05
            # R85 	新闻和出版业 	1992-12-08
            # R86 	广播、电视、电影和影视录音制作业 	1994-02-24
            # R87 	文化艺术业 	2012-10-26
            # S90 	综合 	1990-12-10
        ]

        for industry in cycle_industry:
            stocks = get_industry_stocks(industry)
            stock_list = list(set(stock_list).difference(set(stocks)))

        return stock_list

    def filter(self, context, data, stock_list):

        def fun_cal_stock_PEG(context, stock_list, stock_dict, index2='000016.XSHG', index8='399333.XSHE'):
            gr_index2 = get_growth_rate(index2)
            gr_index8 = get_growth_rate(index8)
            if not stock_list:
                PEG = {}
                avg_std = {}
                return PEG, avg_std

            q = query(valuation.code, valuation.pe_ratio
                      ).filter(valuation.code.in_(stock_list))

            df = get_fundamentals(q).fillna(value=0)

            tmpDict = df.to_dict()
            pe_dict = {}
            for i in range(len(tmpDict['code'].keys())):
                pe_dict[tmpDict['code'][i]] = tmpDict['pe_ratio'][i]
            # print(pe_dict)
            # 获取近两年有分红的票，以及他们的股息率
            df = fun_get_Divid_by_year(context, stock_list)
            if not len(df):
                PEG = {}
                avg_std = {}
                return PEG, avg_std
            # print(df)
            tmpDict = df.to_dict()

            stock_interest = {}
            for stock in tmpDict['divpercent']:
                stock_interest[stock] = tmpDict['divpercent'][stock]

            h = history(1, '1d', 'close', stock_list, df=False)
            PEG = {}
            avg_std = {}
            for stock in stock_list:
                avg_inc = stock_dict[stock]['avg_inc']
                last_inc = stock_dict[stock]['last_inc']
                inc_std = stock_dict[stock]['inc_std']

                pe = -1
                if stock in pe_dict:
                    pe = pe_dict[stock]

                interest = 0
                if stock in stock_interest:
                    interest = stock_interest[stock]

                PEG[stock] = -1
                '''
                原话大概是：
                1、增长率 > 50 的公司要小心，高增长不可持续，一旦转差就要卖掉；实现的时候，直接卖掉增长率 > 50 个股票
                2、增长平稳，不知道该怎么表达，用了 inc_std < last_inc。有思路的同学请告诉我
                '''
                if pe > 0 and last_inc <= 50 and last_inc > 0:  # and inc_std < last_inc:
                    PEG[stock] = (pe / (last_inc + interest * 100))
                    avg_std[stock] = avg_inc / inc_std

            s_list = []
            buydict = {}
            as_dict = {}
            for stock in PEG.keys():
                # and get_growth_rate(stock) > 0:
                if PEG[stock] < 0.5 and PEG[stock] > 0:
                    s_list.append(stock)
                    buydict[stock] = PEG[stock]
                    as_dict[stock] = avg_std[stock]
            g_index_growth_rate = 0.01
            if gr_index2 > g_index_growth_rate or gr_index8 > g_index_growth_rate:
                for stock in PEG.keys():
                    if stock not in s_list:
                        # and get_growth_rate(stock) > 0:
                        if PEG[stock] < 0.6 and PEG[stock] > 0:
                            s_list.append(stock)
                            buydict[stock] = PEG[stock]
                            as_dict[stock] = avg_std[stock]
                for stock in PEG.keys():
                    if stock not in s_list:
                        if PEG[stock] < 0.7 and PEG[stock] > 0:
                            s_list.append(stock)
                            buydict[stock] = PEG[stock]
                            as_dict[stock] = avg_std[stock]
                for stock in PEG.keys():
                    if stock not in s_list:
                        if PEG[stock] < 0.8 and PEG[stock] > 0:
                            s_list.append(stock)
                            buydict[stock] = PEG[stock]
                            as_dict[stock] = avg_std[stock]

            return s_list, buydict, as_dict

        def fun_get_Divid_by_year(context, stocks):
            # 按照派息日计算，计算过去 12个月的派息率(TTM)
            statsDate = context.current_dt
            start_date = statsDate - dt.timedelta(366)
            statsDate = statsDate - dt.timedelta(1)
            year = statsDate.year

            # 将当前股票池转换为国泰安的6位股票池
            stocks_symbol = []
            for s in stocks:
                stocks_symbol.append(s[0:6])

            # 按派息日，查找过去12个月的分红记录
            # 4 steps
            # 0、查找当年有分红的（受派息日约束）；
            df = gta.run_query(query(
                gta.STK_DIVIDEND.SYMBOL,  # 股票代码
                # gta.STK_DIVIDEND.PLANDIVIDENTBT,        # 股票分红预案
                # gta.STK_DIVIDEND.DECLAREDATE,
                gta.STK_DIVIDEND.DIVIDENTAT,
                gta.STK_DIVIDEND.DISTRIBUTIONBASESHARES  # 分红时的股本基数
            ).filter(
                gta.STK_DIVIDEND.DECLAREDATE < statsDate,
                gta.STK_DIVIDEND.PAYMENTDATE >= start_date,
                gta.STK_DIVIDEND.DIVDENDYEAR == year,
                gta.STK_DIVIDEND.TERMCODE != 'P2799'
            )).fillna(value=0, method=None, axis=0)

            df = df[df.SYMBOL.isin(stocks_symbol)]
            # 由于从df中将股票池的股票挑选了出来，index也被打乱，所以要重新更新index
            df = df.reset_index(drop=True)

            # 1、查找上一年有年度分红的（受派息日约束）；
            df1 = gta.run_query(query(
                gta.STK_DIVIDEND.SYMBOL,  # 股票代码
                # gta.STK_DIVIDEND.PLANDIVIDENTBT,        # 股票分红预案
                # gta.STK_DIVIDEND.DECLAREDATE,
                gta.STK_DIVIDEND.DIVIDENTAT,
                gta.STK_DIVIDEND.DISTRIBUTIONBASESHARES  # 分红时的股本基数
            ).filter(
                gta.STK_DIVIDEND.DECLAREDATE < statsDate,
                gta.STK_DIVIDEND.PAYMENTDATE >= start_date,
                gta.STK_DIVIDEND.DIVDENDYEAR == (year - 1),
                gta.STK_DIVIDEND.TERMCODE == 'P2702',  # 年度分红
                gta.STK_DIVIDEND.TERMCODE != 'P2799'
            )).fillna(value=0, method=None, axis=0)
            df1 = df1[df1.SYMBOL.isin(stocks_symbol)]
            df1 = df1.reset_index(drop=True)

            # 2、查找上一年非年度分红的（受派息日约束）；
            df2 = gta.run_query(query(
                gta.STK_DIVIDEND.SYMBOL,  # 股票代码
                # gta.STK_DIVIDEND.PLANDIVIDENTBT,        # 股票分红预案
                # gta.STK_DIVIDEND.DECLAREDATE,
                gta.STK_DIVIDEND.DIVIDENTAT,
                gta.STK_DIVIDEND.DISTRIBUTIONBASESHARES  # 分红时的股本基数
            ).filter(
                gta.STK_DIVIDEND.DECLAREDATE < statsDate,
                gta.STK_DIVIDEND.PAYMENTDATE >= start_date,
                gta.STK_DIVIDEND.DIVDENDYEAR == (year - 1),
                gta.STK_DIVIDEND.TERMCODE != 'P2702',  # 年度分红
                gta.STK_DIVIDEND.TERMCODE != 'P2799'
            )).fillna(value=0, method=None, axis=0)
            df2 = df2[df2.SYMBOL.isin(stocks_symbol)]
            df2 = df2.reset_index(drop=True)
            # print(df2)
            # 得到目前看起来，有上一年度年度分红的股票
            stocks_symbol_this_year = list(set(list(df1['SYMBOL'])))
            # 得到目前看起来，上一年度没有年度分红的股票
            stocks_symbol_past_year = list(
                set(stocks_symbol) - set(stocks_symbol_this_year))

            # 3、查找上一年度还没分红，但上上年有分红的（受派息日约束）
            df3 = gta.run_query(query(
                gta.STK_DIVIDEND.SYMBOL,  # 股票代码
                # gta.STK_DIVIDEND.PLANDIVIDENTBT,        # 股票分红预案
                gta.STK_DIVIDEND.DIVIDENTAT,
                gta.STK_DIVIDEND.DISTRIBUTIONBASESHARES  # 分红时的股本基数
            ).filter(
                gta.STK_DIVIDEND.DECLAREDATE < statsDate,
                gta.STK_DIVIDEND.PAYMENTDATE >= start_date,
                gta.STK_DIVIDEND.DIVDENDYEAR == (year - 2),
                # gta.STK_DIVIDEND.TERMCODE == 'P2702',   # 年度分红
                gta.STK_DIVIDEND.TERMCODE != 'P2799'
            )).fillna(value=0, method=None, axis=0)
            df3 = df3[df3.SYMBOL.isin(stocks_symbol_past_year)]
            df3 = df3.reset_index(drop=True)
            # 3表合并
            df = pd.concat((df, df1))
            df = pd.concat((df, df2))
            df = pd.concat((df, df3))
            # 将股票代码转为聚宽适用格式
            df['SYMBOL'] = map(normalize_code, list(df['SYMBOL']))
            df.index = list(df['SYMBOL'])

            # 获取最新股本
            q = query(valuation.code, valuation.capitalization)
            df2 = get_fundamentals(q).fillna(value=0)

            df2 = df2[df2.code.isin(df.index)]
            df2['SYMBOL'] = df2['code']

            df2 = df2.drop(['code'], axis=1)

            # 合并成一个 dataframe
            df = df.merge(df2, on='SYMBOL')
            df.index = list(df['SYMBOL'])
            df = df.drop(['SYMBOL'], axis=1)

            # 转换成 float
            df['DISTRIBUTIONBASESHARES'] = map(
                float, df['DISTRIBUTIONBASESHARES'])
            # 计算股份比值
            df['CAP_RATIO'] = df['DISTRIBUTIONBASESHARES'] / \
                (df['capitalization'] * 10000)

            df['DIVIDENTAT'] = map(float, df['DIVIDENTAT'])
            # 计算相对于目前股份而言的分红额度
            df['DIVIDENTAT'] = df['DIVIDENTAT'] * df['CAP_RATIO']

            # df = df.drop(['PLANDIVIDENTBT', 'DISTRIBUTIONBASESHARES','capitalization','CAP_RATIO'], axis=1)
            df = df.drop(['DISTRIBUTIONBASESHARES',
                          'capitalization', 'CAP_RATIO'], axis=1)
            # print(df)
            # 接下来这一步是考虑多次分红的股票，因此需要累加股票的多次分红
            df = df.groupby(df.index).sum()
            # print('df',df)
            # print(df[df.index == '002695.XSHE'])
            # 得到当前股价
            if len(list(df.index)) != 0:
                Price = history(1, unit='1d', field='close', security_list=list(df.index), df=True, skip_paused=False,
                                fq='pre')
                Price = Price.T
                df['pre_close'] = Price
                # print(df)
                # 计算股息率 = 股息/股票价格，* 10 是因为取到的是每 10 股分红
                df['divpercent'] = df['DIVIDENTAT'] / (df['pre_close'] * 10)
                df = df.drop(['pre_close', 'DIVIDENTAT'], axis=1)
                df = df[df.divpercent > 0]
                return df
            else:
                return df

        # 取得净利润增长率参数
        def fun_get_inc(context, stock_list):

            # 取最近的四个季度财报的日期
            def __get_quarter(stock_list):
                '''
                输入 stock_list
                返回最近 n 个财报的日期
                返回每个股票最近一个财报的日期
                '''
                # 取最新一季度的统计日期
                q = query(indicator.code, indicator.statDate
                          ).filter(indicator.code.in_(stock_list))
                df = get_fundamentals(q)

                stock_last_statDate = {}
                tmpDict = df.to_dict()
                for i in range(len(tmpDict['statDate'].keys())):
                    # 取得每个股票的代码，以及最新的财报发布日
                    stock_last_statDate[tmpDict['code']
                                        [i]] = tmpDict['statDate'][i]

                df = df.sort_values(by='statDate', ascending=False)
                # 取得最新的财报日期
                last_statDate = df.iloc[0, 1]

                this_year = int(str(last_statDate)[0:4])
                this_month = str(last_statDate)[5:7]

                if this_month == '12':
                    last_quarter = str(this_year) + 'q4'
                    last_two_quarter = str(this_year) + 'q3'
                    last_three_quarter = str(this_year) + 'q2'
                    last_four_quarter = str(this_year) + 'q1'
                    last_five_quarter = str(this_year - 1) + 'q4'

                elif this_month == '09':
                    last_quarter = str(this_year) + 'q3'
                    last_two_quarter = str(this_year) + 'q2'
                    last_three_quarter = str(this_year) + 'q1'
                    last_four_quarter = str(this_year - 1) + 'q4'
                    last_five_quarter = str(this_year - 1) + 'q3'

                elif this_month == '06':
                    last_quarter = str(this_year) + 'q2'
                    last_two_quarter = str(this_year) + 'q1'
                    last_three_quarter = str(this_year - 1) + 'q4'
                    last_four_quarter = str(this_year - 1) + 'q3'
                    last_five_quarter = str(this_year - 1) + 'q2'

                else:  # this_month == '03':
                    last_quarter = str(this_year) + 'q1'
                    last_two_quarter = str(this_year - 1) + 'q4'
                    last_three_quarter = str(this_year - 1) + 'q3'
                    last_four_quarter = str(this_year - 1) + 'q2'
                    last_five_quarter = str(this_year - 1) + 'q1'

                return last_quarter, last_two_quarter, last_three_quarter, last_four_quarter, last_five_quarter, stock_last_statDate

            # 查财报，返回指定值
            def __get_fundamentals_value(stock_list, myDate):
                '''
                输入 stock_list, 查询日期
                返回指定的财务数据，格式 dict
                '''
                q = query(indicator.code, indicator.inc_net_profit_year_on_year, indicator.statDate,
                          indicator.inc_net_profit_to_shareholders_year_on_year).filter(indicator.code.in_(stock_list))

                df = get_fundamentals(q, statDate=myDate).fillna(value=0)

                tmpDict = df.to_dict()
                stock_dict = {}
                for i in range(len(tmpDict['statDate'].keys())):
                    tmpList = []
                    tmpList.append(tmpDict['statDate'][i])
                    tmpList.append(
                        tmpDict['inc_net_profit_to_shareholders_year_on_year'][i])
                    # 原版使用的是下面的‘净利润同比增长’，但是修改使用‘归属母公司净利润同比增长’收益更高
                    # tmpList.append(tmpDict['inc_net_profit_year_on_year'][i])
                    stock_dict[tmpDict['code'][i]] = tmpList

                return stock_dict

            # 对净利润增长率进行处理
            def __cal_net_profit_inc(inc_list):

                inc = inc_list

                for i in range(len(inc)):  # 约束在 +- 100 之内，避免失真
                    if inc[i] > 100:
                        inc[i] = 100
                    if inc[i] < -100:
                        inc[i] = -100

                avg_inc = np.mean(inc[:4])
                last_inc = inc[0]
                inc_std = np.std(inc)

                return avg_inc, last_inc, inc_std

            # 得到最近 n 个季度的统计时间
            last_quarter, last_two_quarter, last_three_quarter, last_four_quarter, last_five_quarter, stock_last_statDate = __get_quarter(
                stock_list)

            last_quarter_dict = __get_fundamentals_value(
                stock_list, last_quarter)
            # print(last_quarter_dict)
            last_two_quarter_dict = __get_fundamentals_value(
                stock_list, last_two_quarter)
            last_three_quarter_dict = __get_fundamentals_value(
                stock_list, last_three_quarter)
            last_four_quarter_dict = __get_fundamentals_value(
                stock_list, last_four_quarter)
            last_five_quarter_dict = __get_fundamentals_value(
                stock_list, last_five_quarter)

            stock_dict = {}
            for stock in stock_list:
                inc_list = []

                if stock in stock_last_statDate:
                    if stock in last_quarter_dict:
                        if stock_last_statDate[stock] == last_quarter_dict[stock][0]:
                            inc_list.append(last_quarter_dict[stock][1])

                    if stock in last_two_quarter_dict:
                        inc_list.append(last_two_quarter_dict[stock][1])
                    else:
                        inc_list.append(0)

                    if stock in last_three_quarter_dict:
                        inc_list.append(last_three_quarter_dict[stock][1])
                    else:
                        inc_list.append(0)

                    if stock in last_four_quarter_dict:
                        inc_list.append(last_four_quarter_dict[stock][1])
                    else:
                        inc_list.append(0)

                    if stock in last_five_quarter_dict:
                        inc_list.append(last_five_quarter_dict[stock][1])
                    else:
                        inc_list.append(0)
                else:
                    inc_list = [0, 0, 0, 0]
                # print(inc_list)
                # 取得过去4个季度的平均增长，最后1个季度的增长，增长标准差
                avg_inc, last_inc, inc_std = __cal_net_profit_inc(inc_list)
                # print(stock,inc_std)
                stock_dict[stock] = {}
                stock_dict[stock]['avg_inc'] = avg_inc
                stock_dict[stock]['last_inc'] = last_inc
                stock_dict[stock]['inc_std'] = inc_std

            return stock_dict

        def fun_get_stock_market_cap(stock_list):
            q = query(valuation.code, valuation.market_cap
                      ).filter(valuation.code.in_(stock_list))
            q2 = query(valuation.code, valuation.market_cap)
            df = get_fundamentals(q).fillna(value=0)
            df2 = get_fundamentals(q2).fillna(value=0).sort_values(
                by='market_cap', ascending=True)
            df2 = df2.reset_index(drop=True)
            # 大市值排序
            sl = df.sort_values(by='market_cap', ascending=True)['code'][:10]
            for s in sl:
                print(str(df2[df2.code == s].index.tolist()) +
                      '/' + str(len(df2)))
            tmpDict = df.to_dict()
            stock_dict = {}
            for i in range(len(tmpDict['code'].keys())):
                # 取得每个股票的 market_cap
                stock_dict[tmpDict['code'][i]] = tmpDict['market_cap'][i]

            return stock_dict

        if self.remove_cycle_industry:
            stock_list = self.fun_remove_cycle_industry(stock_list)

        today = context.current_dt
        sorted_list = []
        try:
            # 过去4个季度的平均增长，最后1个季度的增长，增长标准差
            stock_dict = fun_get_inc(context, stock_list)

            old_stocks_list = []
            for stock in context.portfolio.positions.keys():
                if stock in stock_list:
                    old_stocks_list.append(stock)

            index2 = '000016.XSHG'  # 大盘指数
            index8 = '399333.XSHE'  # 小盘指数
            # index8 = '399678.XSHE'  # 深次新指数

            # 获取每只票的PEG
            selected_stock_list, peg_dict, as_dict = fun_cal_stock_PEG(
                context, stock_list, stock_dict, index2, index8)

            if self.result_order_by == 'peg':
                # 根据 PEG 值升序排序
                sorted_list = sorted(
                    peg_dict.items(), key=lambda d: d[1], reverse=False)
            elif self.result_order_by == 'cap':
                # 根据市值升序排序
                cap_dict = fun_get_stock_market_cap(selected_stock_list)
                sorted_list = sorted(
                    cap_dict.items(), key=lambda d: d[1], reverse=False)
            elif self.result_order_by == 'as':
                # 根据增长率的平均方差比降序排序
                sorted_list = sorted(
                    as_dict.items(), key=lambda d: d[1], reverse=True)
            else:
                self.log.warn('不合法的结果排序方式：%s。只能为 peg、cap 或 as。')
                sorted_list = selected_stock_list
        except Exception as e:
            formatted_lines = traceback.format_exc().splitlines()
            self.log.info(formatted_lines[-1])

        buylist = []
        # self.log.debug('--> sorted_list: %s' % sorted_list)
        for item in sorted_list:
            stock = item[0]
            buylist.append(stock)  # 候选 stocks
            # self.log.debug(stock + ", PEG = " + str(peg_dict[stock]))

        return buylist

    def __str__(self):
        return 'PEG值过滤 [是否剔除周期性行业：%s，结果排序方式：%s]' % \
               (self.remove_cycle_industry, self.result_order_by)


class FilterByRank(FilterStockList):
    """股票评分排序"""

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.rank_stock_count = params.get('rank_stock_count', 20)

    def update_params(self, context, params):
        self.rank_stock_count = params.get(
            'rank_stock_count', self.rank_stock_count)

    def filter(self, context, data, stock_list):
        if len(stock_list) > self.rank_stock_count:
            stock_list = stock_list[:self.rank_stock_count]

        dst_stocks = {}
        for stock in stock_list:
            h = attribute_history(stock, 130, unit='1d', fields=(
                'close', 'high', 'low'), skip_paused=True)
            low_price_130 = h.low.min()
            high_price_130 = h.high.max()

            avg_15 = data[stock].mavg(15, field='close')
            cur_price = data[stock].close

            score = (cur_price - low_price_130) + \
                (cur_price - high_price_130) + (cur_price - avg_15)
            dst_stocks[stock] = score

        # log.error('stock_list: %s, dst_stocks: %s' % (stock_list, dst_stocks))
        if len(dst_stocks) == 0:
            return list()
        df = pd.DataFrame(dst_stocks.values(), index=dst_stocks.keys())
        df.columns = ['score']
        df = df.sort_values(by='score', ascending=True)
        return list(df.index)

    def __str__(self):
        return '股票评分排序 [评分股数: %d ]' % (self.rank_stock_count)


class FilterByCashFlowRank(FilterStockList):
    """庄股评分排序"""

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.rank_stock_count = params.get('rank_stock_count', 600)

    def update_params(self, context, params):
        self.rank_stock_count = params.get(
            'self.rank_stock_count', self.rank_stock_count)

    def filter(self, context, data, stock_list):

        def fall_money_day_3line(security_list, n, n1=20, n2=60, n3=160):
            def fall_money_count(money, n, n1, n2, n3):
                i = 0
                count = 0
                while i < n:
                    money_MA200 = money[i:n3 - 1 + i].mean()
                    money_MA60 = money[i + n3 - n2:n3 - 1 + i].mean()
                    money_MA20 = money[i + n3 - n1:n3 - 1 + i].mean()
                    if money_MA20 <= money_MA60 and money_MA60 <= money_MA200:
                        count = count + 1
                    i = i + 1
                return count

            df = history(n + n3, unit='1d', field='money',
                         security_list=security_list, skip_paused=True)
            s = df.apply(fall_money_count, args=(n, n1, n2, n3,))
            return s

        def money_5_cross_60(security_list, n, n1=5, n2=60):
            def money_5_cross_60_count(money, n, n1, n2):
                i = 0
                count = 0
                while i < n:
                    money_MA60 = money[i + 1:n2 + i].mean()
                    money_MA60_before = money[i:n2 - 1 + i].mean()
                    money_MA5 = money[i + 1 + n2 - n1:n2 + i].mean()
                    money_MA5_before = money[i + n2 - n1:n2 - 1 + i].mean()
                    if (money_MA60_before - money_MA5_before) * (money_MA60 - money_MA5) < 0:
                        count = count + 1
                    i = i + 1
                return count

            df = history(n + n2 + 1, unit='1d', field='money',
                         security_list=security_list, skip_paused=True)
            s = df.apply(money_5_cross_60_count, args=(n, n1, n2,))
            return s

        def cow_stock_value(security_list):
            df = get_fundamentals(query(
                valuation.code, valuation.pb_ratio, valuation.circulating_market_cap
            ).filter(
                valuation.code.in_(security_list),
                valuation.circulating_market_cap <= 100
            ))
            df.set_index('code', inplace=True, drop=True)
            s_fall = fall_money_day_3line(df.index.tolist(), 120, 20, 60, 160)
            s_cross = money_5_cross_60(df.index.tolist(), 120)
            df = pd.concat([df, s_fall, s_cross], axis=1, join='inner')
            df.columns = ['pb', 'cap', 'fall', 'cross']
            df['score'] = df['fall'] * df['cross'] / \
                (df['pb'] * (df['cap'] ** 0.5))
            df.sort_values(['score'], ascending=False, inplace=True)
            return (df)

        df = cow_stock_value(stock_list[:self.rank_stock_count])
        return df.index

    def __str__(self):
        return '庄股评分排序, 评分股数: [ %d ]' % self.rank_stock_count


class Filter_by_aroon(FilterStockList):
    '''阿隆指标过滤'''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.last_days = params.get('last_days', 25)

    def update_params(self, context, params):
        self.last_days = params.get('last_days', self.last_days)

    def filter(self, context, data, stock_list):
        from numpy import array

        tmpList = []
        for stock in stock_list:
            high = attribute_history(stock, self.last_days, unit='1d', fields=['high'], skip_paused=True, df=True,
                                     fq='pre')
            low = attribute_history(stock, self.last_days, unit='1d', fields=['low'], skip_paused=True, df=True,
                                    fq='pre')

            highs = array(high['high'])
            lows = array(low['low'])

            down, up = talib.AROON(
                highs, lows, timeperiod=int(self.last_days / 2))

            if (up[-2] - down[-2] > 50) and (up[-1] - down[-1] > 50) \
                    and (up[-1] - down[-1] > up[-2] - down[-2]) and up[-1] > 70:
                tmpList.append(stock)
        return tmpList

    def __str__(self):
        return '阿隆指标过滤 [天数: %d]' % self.last_days


# 8种量价信号
class PriceVolumeSignal(Enum):
    other = 0  # 其他情况
    volume_up_price_stay = 1  # 量增价平，转阳信号
    volume_up_price_up = 2  # 量增价升，买入信号
    volume_stay_price_up = 3  # 量平价升，持续买入
    volume_down_price_up = 4  # 量减价升，继续持有
    volume_down_price_stay = 5  # 量减价平，警戒信号
    volume_down_price_down = 6  # 量减价跌，卖出信号
    volume_stay_price_down = 7  # 量平价跌，继续卖出
    volume_up_price_down = 8  # 量增价跌，弃卖观望


class Filter_by_price_vol(FilterStockList):
    """根据量价规律进行过滤

参考：http://www.cnblogs.com/carl2380/p/5949500.html

成交量是一种供需的表现，指一个时间单位内对某项交易成交的数量。当供不应求时，人潮汹涌，都要买进，成交量自然放大；反之，供过于求，市场冷清无人，买气稀少，成交 量势必萎缩。而将人潮加以数值化，便是成交量。广义的成交量包括成交股数、成交金额、换手率；狭义的也是最常用的是仅指成交股数。
成股价突破盘整，成交量大增，而股价仍继续上涨是多头市场来临的征兆，也是抄底买进的信号。多头市场结束前，成交量也会有信号暗示股价即将会下跌。

怎么去分析成交量呢？

一、成交量变化的八大规律：

　　1）量升价平，转阳信号： 股价经过持续下跌的低位区，出现成交量增加股价企稳现象，此时一般成交量的阳柱线明显多于阴柱，凸凹量差比较明显，说明底部在积聚上涨动力，有主力在进货 为中线转阳信号，可以适量买进持股待涨。有时也会在上升趋势中途也出现“量增价平”，则说明股价上行暂时受挫，只要上升趋势未破，一般整理后仍会有行情。
　　2）量升价升，买入信号：成交量持续增加，股价趋势也转为上升，这是短中线最佳的买入信号。“量增价升”是最常见的多头主动进攻模式，应积极进场买入与庄共舞。
　　3）量平价升，持续买入：成交量保持等量水平，股价持续上升，可以在期间适时适量地参与。
　　4）量跌价升，继续持有：成交量减少，股价仍在继续上升，适宜继续持股，即使如果锁筹现象较好，也只能是小资金短线参与，因为股价已经有了相当的涨幅，接近上涨末期了。有时在上涨初期也会出现“量减价升”，则可能是昙花一现，但经过补量后仍有上行空间。
　　5）量跌价平，警戒信号：成交量显著减少，股价经过长期大幅上涨之后，进行横向整理不在上升，此为警戒出货的信号。此阶段如果突发巨量天量拉出大阳大阴线，无论有无利好利空消息，均应果断派发。
　　6）量跌价跌，卖出信号：成交量继续减少，股价趋势开始转为下降，为卖出信号。此为无量阴跌，底部遥遥无期，所谓多头不死跌势不止，一直跌到多头彻底丧失信心斩仓认赔，爆出大的成交量（见阶段8），跌势才会停止，所以在操作上，只要趋势逆转，应及时止损出局。
　　7）量平价跌，继续卖出：成交量停止减少，股价急速滑落，此阶段应继续坚持及早卖出的方针，不要买入当心“飞刀断手”。
　　8）量升价跌，弃卖观望： 股价经过长期大幅下跌之后，出现成交量增加，即使股价仍在下落，也要慎重对待极度恐慌的“杀跌”，所以此阶段的操作原则是放弃卖出空仓观望。低价区的增量 说明有资金接盘，说明后期有望形成底部或反弹的产生，适宜关注。有时若在趋势逆转跌势的初期出现“量增价跌”，那么更应果断地清仓出局

二、成交量的五种形态：

　　1、缩量，缩量是指市场成交极为清淡，大部分人对市场后期走势十分认同，意见十分一致。这里面又分两种情况：一是市场人士都十分看淡后市，造成只有人卖，却没有人买，所以急剧缩量；二是，市场人士都对后市十分看好，只有人买，却没有人卖，所以又急剧缩量。
　　2、放量，放量一般发生在市场趋势发生转折的转折点处，市场各方力量对后市分歧逐渐加大，在一部分人坚决看空后市时，另一部分人却对后市坚决看好，一些人纷纷把家底甩出，另一部分人却在大手笔吸纳。
　　3、堆量， 当主力意欲拉升时，常把成交量做得非常漂亮，几日或几周以来，成交量缓慢放大，股价慢慢推高，成交量在近期的Ｋ线图上，形成了一个状似土堆的形态，堆得越 漂亮，就越可能产生大行情。相反，在高位的堆量表明主力已不想玩了，在大举出货，这种情况下我们要坚决退出，不要幻想再有巨利获取了。
　　4、市场分歧促成成交，所谓成交，当然是有买有卖才会达成，光有买或光有卖绝对达不成成交。成交必然是一部分人看空后市，另外一部分人看多后市，造成巨大的分歧，又各取所需，才会成交。
　　5、量不规则性放大缩小，这种情况一般是没有突发利好或大盘基本稳定的前提下，妖庄所为，风平浪静时突然放出历史巨量，随后又没了后音，一般是实力不强的庄家在吸引市场关注，以便出货。

三、成交量实盘操作：

　　1、实量与虚量

　　（1）第一个上升浪的成交量比较大,股价上升到一定高位后开始回落。我们称这种有量支持的上升浪为实浪。
　　（2）第二个上升浪的成交量比较小,股价上升到前头部附近时开始回落,并跌破60日平均线未见有支撑。我们称这种没有量支持的上升浪为虚浪。

    重点：
　　（1）每一轮指数行情到来之前就有一些领头羊个股正在走强,它们是本轮行情的中坚力量,它们上升空间较大,上升时间较长。
　　（2）大多数个股行情是因指数行情起哄的,因此,建仓速度快,出货时间也快,其涨幅有限。
　　（3） 当实浪上升到一定高度后,已有不少获利盘开始出逃,股价回落。当第二轮行情推高时并没有庄家出力,因此成交量较小。一旦股价到达前期头部区间时,解套盘和 获利盘双重涌出,股价回落。由于没有庄家,其跌势一路下滑,在60日均线处并不会形成支撑。一旦跌破60日均线后,则有长期下跌行情。

　　2、缩量止跌点

    当一只个股进入起飞线后即开始滑跑,在整个滑跑过程中会出现多次震仓洗盘。
    我们把5日、10日均线靠近60日均线称为【缩量止跌点】,在一个大牛股的滑跑过程中,有时会出现第一【缩量止跌点】,第二【缩量止跌点】,甚至第三第四【缩量止跌点】,这都是买入的机会。如下图：

    重点：震仓洗盘的低点一般都在60日平均线附近。一般都以60日平均线为衡量标准。
    操作要点：所以,当股价跌到60日平均线附近时,可以观察其成交量是否极其萎缩,股价能否在这里翘头向上,并发生金叉,如果是的话,这里是较好的买入点。

　　3、价越盘越高 量越盘越小

    重点：指数在60线附近就走不动了,开始小心谨慎地缩量盘整。60日平均线就是一个尺度,指数不敢过60日均线。
    操作要点：5日、10日死亡交叉以后,它跌下去不会跌二三天的,要跌就跌五天、十天,5日、10日均线死亡交叉,一般就是要跌五天、十天。这样最好了,跌到后金叉你就再买进。它什么时候出现后金叉,就什么时候买进,不就行了吗？

    "价越盘越高,量越盘越小"是强势的表现,表明该股的筹码处于供不应求状态,表明庄家对该股看好的程度,当然这其中也会混入大量的散户,所以这样的走势后面不 排除有借指数回档洗盘的可能,不管是否洗盘,我们都有办法对付它,如果三死叉洗盘的话,我们就等待三金叉买入,或者重上五线买入、过前头买入,总之这样的 股票如果在低位被发现,是不能放过的！

    """

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.days_fast = params.get('days_fast', 2)
        self.days_slow = params.get('days_slow', 5)
        self.ratio_delta_min = params.get(
            'ratio_delta_min', 0.010)  # 判断升跌的最小比例单位

    def update_params(self, context, params):
        self.days_fast = params.get('days_fast', self.days_fast)
        self.days_slow = params.get('days_slow', self.days_slow)
        self.ratio_delta_min = params.get(
            'ratio_delta_min', self.ratio_delta_min)

    # 取得对应股票代码的近期量价趋势
    def get_price_vol_trend(self, context, stock, fastperiod=2, slowperiod=4):
        half_period = (fastperiod + slowperiod) / 2
        grid = attribute_history(
            security=stock, count=slowperiod * 4, unit='1d', fields=['close', 'volume'], df=False)
        _close = grid['close']  # type: np.ndarray
        _volume = grid['volume']

        TREND_UP = 1
        TREND_STAY = 0
        TREND_DOWN = -1

        # 计算价格趋势
        _close_wma_last = _close[-1]
        # MA_Type: 0=SMA, 1=EMA, 2=WMA, 3=DEMA, 4=TEMA, 5=TRIMA, 6=KAMA, 7=MAMA, 8=T3 (Default=SMA)
        _close_wma_fast_narray = talib.MA(
            _close[-fastperiod * 3:], fastperiod, matype=2)
        _close_wma_fast = _close_wma_fast_narray[-1]
        _close_wma_slow_narray = talib.MA(
            _close[-slowperiod * 3:], slowperiod, matype=2)
        _close_wma_slow = _close_wma_slow_narray[-1]
        # self.log.debug('--> _close_wma_last: %s, _close_wma_fast: %s, _close_wma_slow: %s' % (_close_wma_last, _close_wma_fast, _close_wma_slow))
        # self.log.debug('--> _close_wma_fast / _close_wma_slow: %.4f, _close_wma_last / _close_wma_fast: %.2f' % (_close_wma_fast / _close_wma_slow, _close_wma_last / _close_wma_fast))
        if _close_wma_last / _close_wma_fast >= 1.0 and _close_wma_fast / _close_wma_slow > (
                1 + self.ratio_delta_min) ** half_period:
            close_trend = TREND_UP
        elif _close_wma_last / _close_wma_fast <= 1.0 and _close_wma_fast / _close_wma_slow < (
                1 - self.ratio_delta_min) ** half_period:
            close_trend = TREND_DOWN
        else:
            close_trend = TREND_STAY

        # 计算成交量趋势
        _volume_wma_last = _volume[-1]
        _volume_wma_fast_narray = talib.MA(
            _volume[-fastperiod * 3:], fastperiod, matype=2)
        _volume_wma_fast = _volume_wma_fast_narray[-1]
        _volume_wma_slow_narray = talib.MA(
            _volume[-slowperiod * 3:], slowperiod, matype=2)
        _volume_wma_slow = _volume_wma_slow_narray[-1]
        # self.log.debug('--> _volume_wma_last: %s, _volume_wma_fast: %s, _volume_wma_slow: %s' % (_volume_wma_last, _volume_wma_fast, _volume_wma_slow))
        # self.log.debug('--> _volume_wma_fast / _volume_wma_slow: %.4f, _volume_wma_last / _volume_wma_fast: %.2f' % (_volume_wma_fast / _volume_wma_slow, _volume_wma_last / _volume_wma_fast))
        if _volume_wma_last / _volume_wma_fast >= 1.0 and _volume_wma_fast / _volume_wma_slow > (
                1 + self.ratio_delta_min * 2) ** half_period:
            volume_trend = TREND_UP
        elif _volume_wma_last / _volume_wma_fast <= 1.0 and _volume_wma_fast / _volume_wma_slow < (
                1 - self.ratio_delta_min * 2) ** half_period:
            volume_trend = TREND_DOWN
        else:
            volume_trend = TREND_STAY

        # self.log.debug('==> volume_trend: %d, close_trend: %d' % (volume_trend, close_trend))

        # 判断量价关系
        flag = PriceVolumeSignal.other
        if volume_trend == TREND_UP and close_trend == TREND_STAY:
            if _close_wma_slow_narray[-1] / _close_wma_slow_narray[-slowperiod] < (
                    1 - self.ratio_delta_min) ** slowperiod \
                    and _close_wma_slow_narray[-slowperiod * 2] / _close_wma_slow_narray[-slowperiod] < (
                    1 - self.ratio_delta_min) ** slowperiod:  # 在持续下跌的低位区出现
                flag = PriceVolumeSignal.volume_up_price_stay
        elif volume_trend == TREND_UP and close_trend == TREND_UP:
            flag = PriceVolumeSignal.volume_up_price_up
        elif volume_trend == TREND_STAY and close_trend == TREND_UP:
            flag = PriceVolumeSignal.volume_stay_price_up
        elif volume_trend == TREND_DOWN and close_trend == TREND_UP:
            flag = PriceVolumeSignal.volume_down_price_up
        elif volume_trend == TREND_DOWN and close_trend == TREND_STAY:
            if _close_wma_slow_narray[-1] / _close_wma_slow_narray[-slowperiod] > (
                    1 + self.ratio_delta_min * 2) ** slowperiod \
                    and _close_wma_slow_narray[-slowperiod * 2] / _close_wma_slow_narray[-slowperiod] > (
                    1 + self.ratio_delta_min * 2) ** slowperiod:  # 股价经过长期大幅上涨后，成交量显著减少
                flag = PriceVolumeSignal.volume_down_price_stay
        elif volume_trend == TREND_DOWN and close_trend == TREND_DOWN:
            flag = PriceVolumeSignal.volume_down_price_down
        elif volume_trend == TREND_STAY and close_trend == TREND_DOWN:
            if _close_wma_fast_narray[-1] / _close_wma_fast_narray[-fastperiod] < (
                    1 - self.ratio_delta_min * 3) ** fastperiod:  # 股价急速滑落
                flag = PriceVolumeSignal.volume_stay_price_down
        elif volume_trend == TREND_UP and close_trend == TREND_DOWN:
            if _close_wma_slow_narray[-1] / _close_wma_slow_narray[-slowperiod] < (
                    1 - self.ratio_delta_min * 2) ** slowperiod \
                    and _close_wma_slow_narray[-slowperiod * 2] / _close_wma_slow_narray[-slowperiod] < (
                    1 - self.ratio_delta_min * 2) ** slowperiod:  # 经过长期大幅下跌之后出现
                flag = PriceVolumeSignal.volume_up_price_down

        return flag

    def filter(self, context, data, stock_list):

        hold_list = []
        for stock in context.portfolio.positions.keys():
            try:
                flag = self.get_price_vol_trend(
                    context, stock, self.days_fast, self.days_slow)
                if flag in (PriceVolumeSignal.volume_up_price_stay,
                            PriceVolumeSignal.volume_up_price_up,
                            PriceVolumeSignal.volume_stay_price_up,
                            PriceVolumeSignal.volume_down_price_up,
                            PriceVolumeSignal.volume_up_price_down
                            ) and stock not in hold_list:
                    hold_list.append(stock)
            except Exception as e:
                # self.log.info(e)
                formatted_lines = traceback.format_exc().splitlines()
                self.log.info(formatted_lines[-1])

        buy_list = []
        for stock in stock_list:
            try:
                flag = self.get_price_vol_trend(
                    context, stock, self.days_fast, self.days_slow)
                if flag in (PriceVolumeSignal.volume_up_price_stay,
                            PriceVolumeSignal.volume_up_price_up,
                            PriceVolumeSignal.volume_stay_price_up
                            ) and stock not in (hold_list + buy_list):
                    buy_list.append(stock)
            except Exception as e:
                # self.log.info(e)
                formatted_lines = traceback.format_exc().splitlines()
                self.log.info(formatted_lines[-1])

        # 返回符合趋势的股票
        self.log.info('--> 符合条件的股票：继续持有 %s，新买入 %s' % (hold_list, buy_list))
        return hold_list + buy_list

    def __str__(self):
        return '量价规律过滤 [短期天数：%d，长期天数：%d，判断升跌的最小比例单位：%.3f]' % \
               (self.days_fast, self.days_slow, self.ratio_delta_min)


class Filter_by_divident(FilterStockList):
    '''根据分红率过滤'''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.sort_type = params.get('sort_type', 'asc')  # 排序方向，asc / dsc
        self.divident_min = params.get('divident_min', 1.0)  # 最小分红率(%?)
        self.enable_weighted = params.get('enable_weighted', True)  # 使用加权方式计算

    def update_params(self, context, params):
        self.sort_type = params.get('sort_type', self.sort_type)
        self.divident_min = params.get('divident_min', self.divident_min)
        self.enable_weighted = params.get(
            'enable_weighted', self.enable_weighted)

    def filter(self, context, data, stock_list):
        # 计算指数分红率
        # 1.计算每个股票 divBal=年度分红/每股净资产
        # 2.等权分红率 = sum(divBal/pb)/n
        # 3.加权分红率 = sum(divBal/pb*流动资产)/总流动资产
        # 4.效率有些低，希望有人可以优化下
        def indexValuation(stocks, date):
            q = query(valuation.code, valuation.day, valuation.pe_ratio, valuation.pb_ratio,
                      valuation.circulating_market_cap, valuation.market_cap,
                      ).filter(valuation.code.in_(stocks))
            df = get_fundamentals(q, date)

            div_dict = {}
            divWeighted_dict = {}
            divIndex = 0  # 指数分红率
            divIndexWeighted = 0  # 加权指数分红率
            marketIndex = 0
            for index, row in df.iterrows():
                divBal = FuncLib.getDivPercent(row.code, date)[2]
                div_dict[row.code] = divBal / row.pb_ratio
                divIndex += div_dict[row.code]
                divWeighted_dict[row.code] = divBal / \
                    row.pb_ratio * row.circulating_market_cap
                divIndexWeighted += divWeighted_dict[row.code]
                marketIndex += row.circulating_market_cap

            divIndex = divIndex / len(stocks)
            divIndexWeighted = divIndexWeighted / marketIndex

            return (div_dict, divWeighted_dict, divIndex, divIndexWeighted)

        date = context.current_dt.today()
        div_dict, divWeighted_dict, divIndex, divIndexWeighted = indexValuation(
            stock_list, date)
        is_dsc = True if self.sort_type == 'dsc' else False
        if self.enable_weighted:
            sort_vol = sorted(divWeighted_dict.items(), key=(
                lambda d: d[1]), reverse=is_dsc)  # 按波动率排序
        else:
            sort_vol = sorted(div_dict.items(), key=(
                lambda d: d[1]), reverse=is_dsc)  # 按波动率排序

        sort_list = [k for k, v in sort_vol if v >= self.divident_min]

        # self.log.debug('--> sort_vol: %s' % sort_vol)
        # self.log.debug('--> sort_list: %s' % sort_list)
        return sort_list

    def __str__(self):
        return '根据分红率过滤 [排序方式: %s, 最小分红率: %.2f, 是否使用加权计算: %s]' \
               % (self.sort_type, self.divident_min, self.enable_weighted)


class Filter_by_code(FilterStockList):
    '''只选取指定代码的股票'''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.code_list = params.get('code_list', [])

    def update_params(self, context, params):
        self.code_list = params.get('code_list', self.code_list)

    def filter(self, context, data, stock_list):
        # return [stock for stock in stock_list if stock in self.code_list]
        return [stock for stock in self.code_list]

    def __str__(self):
        return '选取指定股票，列表: %s' % self.code_list


class MacdSignal(Enum):
    gold = 0
    dead = 1
    other = 2


class Filter_by_macd_ex(FilterStockList):
    '''根据MACD指标选股'''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.days_slow = params.get('days_slow', 26)
        self.days_fast = params.get('days_fast', 11)
        self.days_signal = params.get('days_signal', 5)
        self.enable_dibeili = params.get('enable_dibeili', False)  # 是否开启背离判断

    def update_params(self, context, params):
        self.days_slow = params.get('days_slow', self.days_slow)
        self.days_fast = params.get('days_fast', self.days_fast)
        self.days_signal = params.get('days_signal', self.days_signal)
        self.enable_dibeili = params.get('enable_dibeili', self.enable_dibeili)

    def filter(self, context, data, stock_list):
        buy_list = []
        for stock in context.portfolio.positions:
            if self.make_decision(context, stock) == MacdSignal.dead:  # 死叉卖出
                log.info('死叉卖出: %s' % stock)
                pass
            else:
                buy_list.append(stock)
        for stock in stock_list:
            if self.make_decision(context, stock) == MacdSignal.gold:  # 金叉买入
                buy_list.append(stock)
                log.info('金叉买入: %s' % stock)
        # self.log.info('--> stock_list total %d, buy_list total %d.' % (len(stock_list), len(buy_list)))
        if len(buy_list) > 0:
            self.log.info('选股结果：%s' % buy_list)
        return buy_list

    def make_decision(self, context, stock, fastperiod=12, slowperiod=26, signalperiod=9):
        rows = (fastperiod + slowperiod + signalperiod) * 5
        h = attribute_history(security=stock, count=rows,
                              unit='1d', fields=['close'], df=True)
        df_hData_now = get_price(
            stock, count=1, end_date=context.current_dt, frequency='1m')
        h.loc[context.current_dt, 'close'] = df_hData_now['close'].values[0]

        _close = h['close'].values  # type: np.ndarray

        # 使用 talib 方式计算 MACD，不用当天数据
        # _dif, _dea, _macd = FuncLib.taMACD(context, stock, fastperiod, slowperiod, signalperiod, enable_minute_data=True)
        # self.log.debug('--> ta # _dif[-1]: %s, _dea[-1]: %s, _macd[-1]: %s' % (_dif[-1], _dea[-1], _macd[-1]))

        # 使用自定义方式计算 MACD，用到当天数据
        _dif, _dea, _macd = FuncLib.myMACD(context, stock, fastperiod, slowperiod, signalperiod,
                                             enable_minute_data=True)
        # self.log.debug('--> my # _dif[-1]: %s, _dea[-1]: %s, _macd[-1]: %s' % (_dif[-1], _dea[-1], _macd[-1]))

        # 使用聚宽函数计算 MACD，据说用到了未来数据
        # _dif, _dea, _macd = FuncLib.jqMACD(context, stock, fastperiod, slowperiod, signalperiod)
        # self.log.debug('--> jq # _dif[-1]: %s, _dea[-1]: %s, _macd[-1]: %s' % (_dif[-1], _dea[-1], _macd[-1]))

        # 上述三种方式的输出结果示例（2批次，综合比较悬"自定义计算"较合适）：
        #
        # 不开分钟数据时，my 和 jq 的 macd 值比较接近：
        # --> ta # _dif[-1]: 0.462575735087, _dea[-1]: 0.67727046561, _macd[-1]: -0.214694730522
        # --> my # _dif[-1]: 0.350435873499, _dea[-1]: 0.514907153787, _macd[-1]: -0.328942560575
        # --> jq # _dif[-1]: 0.39366000091, _dea[-1]: 0.592248572056, _macd[-1]: -0.397177142293
        #
        # --> ta # _dif[-1]: 1.15266481542, _dea[-1]: 0.767395015341, _macd[-1]: 0.385269800079
        # --> my # _dif[-1]: 1.04582910247, _dea[-1]: 0.617720173674, _macd[-1]: 0.856217857595
        # --> jq # _dif[-1]: 1.19686206518, _dea[-1]: 0.829714535938, _macd[-1]: 0.73429505849
        #
        # 开分钟数据后，my & jq 的 macd 值更接近：
        # --> ta # _dif[-1]: 0.398133065751, _dea[-1]: 0.621442985638, _macd[-1]: -0.223309919887
        # --> my # _dif[-1]: 0.294307487223, _dea[-1]: 0.470787213214, _macd[-1]: -0.352959451982
        # --> jq # _dif[-1]: 0.39366000091, _dea[-1]: 0.592248572056, _macd[-1]: -0.397177142293
        #
        # --> ta # _dif[-1]: 1.15684324711, _dea[-1]: 0.845284661694, _macd[-1]: 0.311558585412
        # --> my # _dif[-1]: 1.057061987, _dea[-1]: 0.705588558931, _macd[-1]: 0.702946856137
        # --> jq # _dif[-1]: 1.19686206518, _dea[-1]: 0.829714535938, _macd[-1]: 0.73429505849

        # if stock == '002868.XSHE':
        #    self.log.debug('----> %s [-1]d：%.3f, %.3f, %.3f' % (
        #        stock, _dif[-1], _dea[-1], _macd[-1]))
        #    self.log.debug('----> %s [-2]d：%.3f, %.3f, %.3f' % (
        #        stock, _dif[-2], _dea[-2], _macd[-2]))

        ret_val = MacdSignal.other

        if self.enable_dibeili:

            # ----------- 底背离 ------------------------
            # 1.昨天[-1]金叉
            # 1.昨天[-1]金叉close < 上一次[-2]金叉close
            # 2.昨天[-1]金叉Dif值 > 上一次[-2]金叉Dif值
            if _macd[-1] > 0 > _macd[-2]:  # 昨天金叉
                # idx_gold: 各次金叉出现的位置
                # type: np.ndarray
                idx_gold = np.where((_macd[:-1] < 0) & (_macd[1:] > 0))[0] + 1
                if len(idx_gold) > 1:
                    if _close[idx_gold[-1]] < _close[idx_gold[-2]] and _dif[idx_gold[-1]] > _dif[idx_gold[-2]]:
                        ret_val = MacdSignal.gold

            # ----------- 顶背离 ------------------------
            # 1.昨天[-1]死叉
            # 1.昨天[-1]死叉close > 上一次[-2]死叉close
            # 2.昨天[-1]死叉Dif值 < 上一次[-2]死叉Dif值
            if _macd[-1] < 0 < _macd[-2]:  # 昨天死叉
                # idx_dead: 各次死叉出现的位置
                # type: np.ndarray
                idx_dead = np.where((_macd[:-1] > 0) & (_macd[1:] < 0))[0] + 1
                if len(idx_dead) > 1:
                    if _close[idx_dead[-1]] > _close[idx_dead[-2]] and _dif[idx_dead[-1]] < _dif[idx_dead[-2]]:
                        ret_val = MacdSignal.dead

        else:

            if _macd[-1] > 0 and _macd[-2] < 0:  # 金叉
                ret_val = MacdSignal.gold

            if _macd[-1] < 0 and _macd[-2] > 0:  # 死叉
                ret_val = MacdSignal.dead

        return ret_val

    def __str__(self):
        return '根据MACD指标选股 [是否开启背离判断: %s]' % self.enable_dibeili


class Filter_by_macd(Filter_by_macd_ex):
    pass


class Filter_by_volume_ratio(FilterStockList):
    '''量比指标过滤'''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.back_days = params.get('back_days', 5)  # 平均量计算天数
        self.value_min = params.get('value_min', 3.0)  # 量比最小值
        self.value_max = params.get('value_max', 9999.0)  # 量比最大值

    def update_params(self, context, params):
        self.last_days = params.get('back_days', self.back_days)
        self.value_min = params.get('value_min', self.value_min)
        self.value_max = params.get('value_max', self.value_max)

    def filter(self, context, data, stock_list):
        tmpList = []
        for stock in stock_list:
            value = FuncLib.get_vol_rate(
                context, stock, self.back_days)
            if (value >= self.value_min) and (value <= self.value_max):
                tmpList.append(stock)
        return tmpList

    def __str__(self):
        return '量比指标过滤 [计算天数: %d，最小值：%d，最大值：%d]' % (self.back_days,
                                                    self.value_min, self.value_max)


class Filter_by_volume_ratio_today(FilterStockList):
    '''当日量比指标过滤'''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.last_part_minutes = params.get(
            'last_part_minutes', 10)  # 后半部分时长，分钟
        self.value_min = params.get('value_min', 3.0)  # 量比最小值
        self.value_max = params.get('value_max', 9999.0)  # 量比最大值

    def update_params(self, context, params):
        self.last_part_minutes = params.get(
            'last_part_minutes', self.last_part_minutes)
        self.value_min = params.get('value_min', self.value_min)
        self.value_max = params.get('value_max', self.value_max)

    def filter(self, context, data, stock_list):
        df_value = FuncLib.get_vol_rate_today(
            context, stock_list, self.last_part_minutes)
        df_value = df_value[df_value >= self.value_min]
        df_value = df_value[df_value <= self.value_max]
        tmpList = list(df_value.index)
        self.log.debug('过滤结果: %s' % tmpList)
        return tmpList

    def __str__(self):
        return '当日量比指标过滤 [后半部分时长，分钟: %d，最小值：%.2f，最大值：%.2f]' % (self.last_part_minutes,
                                                               self.value_min, self.value_max)


class Filter_by_double_period(FilterStockList):
    '''根据翻倍期策略选股，常用于银行'''
    '''银行股评级：https://www.jisilu.cn/question/50176 '''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.number = params.get('number', 1)  # 选取多少股

    def update_params(self, context, params):
        self.number = params.get('number', self.number)

    def filter(self, context, data, stock_list):
        buy_list = []
        for stock in self.calc_double_period_01(context, data, stock_list):
            buy_list.append(stock)
            if stock not in context.portfolio.positions.keys():
                # log.info('翻倍期策略买入: %s' % stock)
                pass
        # log.info('stock_list total %d, buy_list total %d.' % (len(stock_list), len(buy_list)))
        return buy_list

    # 翻倍期1
    # 用 基本每股收益/每股净资产 得到资产收益率，用现价/每股净资产得到市净率。
    # 再用LOG(市净率两倍，资产收益率+1) 得到翻倍期1。
    def calc_double_period_01(self, context, data, stock_list):
        q = query(
            valuation.code, income.basic_eps, valuation.market_cap,
            balance.total_sheet_owner_equities, valuation.capitalization,
            valuation.pe_ratio, valuation.pb_ratio, indicator.eps
        ).filter(
            valuation.code.in_(stock_list)
        )
        df = get_fundamentals(q)
        # df['每股净资产'] = df['total_sheet_owner_equities'] / (df['capitalization'] * 10000)
        df['每股净资产'] = df['pe_ratio'] / df['pb_ratio'] * df['eps']
        df['资产收益率'] = df['basic_eps'] / df['每股净资产']
        df['市净率'] = (df['market_cap'] * 10000 /
                     df['capitalization']) / df['每股净资产']
        # df['市净率'] = df['pb_ratio']  # not good, why
        df['翻倍期1'] = np.log10(df['市净率'] * 2) / np.log10(df['资产收益率'] + 1)
        df.sort_values(by='翻倍期1', inplace=True)
        # log.info('df: %s' % df)
        return list(df['code'])[:self.number]

    def __str__(self):
        return '根据翻倍期策略选股'


class FilterByLongtouOld(FilterStockList):
    '''龙头股选股过滤 - 旧版
（一）龙头股具备的6个条件
　　1、必须从涨停板开始。不能涨停的股票不可能做龙头。
　　2、低价。只有低价股才能成为大众情人。
　　3、流通市值适中。
　　4、必须同时满足日线KDJ、周线KDJ、月线KDJ同时低价金叉。
　　5、通常在大盘下跌末端，市场恐慌时，逆市涨停，提前见底。
　　6、必须具备进取性放量特征。
　　(如果个股出现连续三日以上放量，称为进取性放量.如果只有单日放量，称为补仓性放量)
（二） 龙头股买卖条件及技巧
　　买入条件：
　　1、周KDJ在20以下金叉；
　　2、日线SAR指标（又叫傻瓜指标）第一次发出买进信号；
　　3、底部第一次放量，第一个涨停板。
　　买入技巧：
　　1、龙头股涨停开闸放水时买入。
　　2、未开板的个股，第二天该股若高开，即可在涨幅1.5～3.5％之间介入。
　　3、龙头股回到第一个涨停板的启涨点。将梅开二度，比第一个买点更稳，更准，更狠。
　　4、操作上两种选择。用分时图直接追涨停板有四种形态。
　　卖出要点和技巧：
　　1、连续涨停的龙头个股，要耐心持股，一直到不再涨停，收盘前10分钟卖出。
　　2、不连续涨停的龙头个股，用日线SAR指标判断，SAR指标是中线指标，中线持有该股，直到SAR指标第一次转为卖出信号。
    '''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.enable_price_check = params.get(
            'enable_price_check', True)  # 是否开启股价涨跌检查
        self.price_check_enable_minute_data = params.get(
            'price_check_enable_minute_data', True)  # 股价涨跌检查是否开启分钟数据
        self.price_check_days = params.get('price_check_days', 2)  # 股价涨跌检查天数
        self.price_change_range = params.get(
            'price_change_range', ((0.0, 0.10), (0.0, 0.09)))  # 每日股价许可涨跌范围
        self.enable_kdj_check = params.get(
            'enable_kdj_check', True)  # 是否开启 KDJ 检查
        self.kdj_check_week = params.get('kdj_check_week', True)  # 是否检查周线
        self.kdj_week_value_range = params.get(
            'kdj_week_value_range', (0, 30))  # KDJ 周线许可范围
        self.kdj_check_month = params.get('kdj_check_month', True)  # 是否检查月线
        self.enable_volume_check = params.get(
            'enable_volume_check', True)  # 是否开启成交量涨跌检查
        self.volume_change_3days_range = params.get('volume_change_3days_range',
                                                    ((0.0, 1000.0), (0.5, 1000.0), (0.5, 1000.0)))  # 近3日成交量许可涨跌范围

    def update_params(self, context, params):
        self.enable_price_check = params.get(
            'enable_price_check', self.enable_price_check)
        self.price_check_enable_minute_data = params.get('price_check_enable_minute_data',
                                                         self.price_check_enable_minute_data)
        self.price_check_days = params.get(
            'price_check_days', self.price_check_days)
        self.price_change_range = params.get(
            'price_change_range', self.price_change_range)
        self.enable_kdj_check = params.get(
            'enable_kdj_check', self.enable_kdj_check)
        self.kdj_check_week = params.get('kdj_check_week', self.kdj_check_week)
        self.kdj_week_value_range = params.get(
            'kdj_week_value_range', self.kdj_week_value_range)
        self.kdj_check_month = params.get(
            'kdj_check_month', self.kdj_check_month)
        self.enable_volume_check = params.get(
            'enable_volume_check', self.enable_volume_check)
        self.volume_change_3days_range = params.get(
            'volume_change_3days_range', self.volume_change_3days_range)

    def filter(self, context, data, stock_list):

        if len(stock_list) == 0:
            return stock_list

        # 0.0 输入的 stock_list 必须先满足：市值适中、PE值较低、具备进取放量特征？

        list_result = []

        # 1.1 股价涨跌检查过滤

        if self.enable_price_check:
            if self.price_check_days != len(self.price_change_range):
                self.log.warn('过滤器参数不合法：股价涨跌检查天数与涨跌范围取值数不一致！')
                return []

            df_pct_change = FuncLib.get_price_change(context,
                                                       enable_minute_data=self.price_check_enable_minute_data,
                                                       stock_list=stock_list, days_count=self.price_check_days)
            days = df_pct_change.columns

            df_result = df_pct_change
            for i in range(self.price_check_days):
                # self.log.debug('--> days[%d]: %s, self.price_change_range[%d]: %s' % (i, days[i], i, self.price_change_range[i]))
                df_result = df_result[df_result[days[i]]
                                      >= self.price_change_range[i][0]]
                df_result = df_result[df_result[days[i]]
                                      <= self.price_change_range[i][1]]

            # 采用排除算法，结果应该与上面一致
            '''
            df_excluded = df_pct_change
            for i in range(len(days)):
                df_excluded1 = df_excluded[df_excluded[days[i]] < self.price_change_range[i][0]]
                df_excluded2 = df_excluded[df_excluded[days[i]] > self.price_change_range[i][1]]
            index_selected = list(set(df_pct_change.index) ^ set(df_excluded1.index) ^ set(df_excluded2.index))
            df_result = df_pct_change.ix[index_selected]
            '''

            df_result = df_result.dropna(how='any')
            # self.log.debug('--> 涨停数据：%s' % df_result)
            list_result = list(df_result.index)
            self.log.debug('--> 近 %d 天价格满足范围的个股: %s' %
                           (self.price_check_days, list_result))

        # 1.2 满足日线KDJ、周线KDJ、月线KDJ同时低价金叉
        if self.enable_kdj_check:
            list_result_tmp = []
            for stock in list_result:
                k_1d, d_1d, j_1d = FuncLib.get_kdj_cn(context, stock, N1=9, N2=3, N3=3,
                                                        fq='1d')  # 1d：日线，5d：周线
                self.log.debug('--> %s 1d: k:%.2f, d:%.2f, j:%.2f,' %
                               (stock, k_1d[-1], d_1d[-1], j_1d[-1]))
                k_5d, d_5d, j_5d = FuncLib.get_kdj_cn(
                    context, stock, N1=9, N2=3, N3=3, fq='5d')
                self.log.debug('--> %s 5d: k:%.2f, d:%.2f, j:%.2f,' %
                               (stock, k_5d[-1], d_5d[-1], j_5d[-1]))
                k_20d, d_20d, j_20d = FuncLib.get_kdj_cn(
                    context, stock, N1=9, N2=3, N3=3, fq='20d')
                self.log.debug('--> %s 20d: k:%.2f, d:%.2f, j:%.2f,' %
                               (stock, k_20d[-1], d_20d[-1], j_20d[-1]))

                # 判断是否金叉：日线周线月线同时金叉，且周线在低位（小于20）；  # 取值可能都为0，所以第一天不做等号判断
                flag = ((k_1d[-2] < d_1d[-2] or k_1d[-3] <
                         d_1d[-3]) and k_1d[-1] > d_1d[-1])
                if self.kdj_check_week:
                    flag = flag and (
                        (k_5d[-2] <= d_5d[-2] or k_5d[-3] <= d_5d[-3]) and k_5d[-1] >= d_5d[-1])
                flag = flag and (
                    self.kdj_week_value_range[0] <= k_5d[-1] <= self.kdj_week_value_range[1])
                if self.kdj_check_month:
                    flag = flag and (
                        (k_20d[-2] <= d_20d[-2] or k_20d[-3] <= d_20d[-3]) and k_20d[-1] >= d_20d[-1])
                if flag:
                    list_result_tmp.append(stock)
            list_result = list_result_tmp
            self.log.debug('--> KDJ金叉过滤结果：%s' % list_result)

        # 1.3 具备进取放量特征

        if self.enable_volume_check:
            df_pct_change = FuncLib.get_volume_change(
                context, stock_list=list_result, days_count=3)
            if df_pct_change is None:
                self.log.debug('--> 没有获得成交量涨跌幅计算结果')
                return []
            self.log.debug('成交量涨跌幅计算结果：%s' % df_pct_change)

            # 满足最近3天连续放量（相比前日涨幅超过50%）
            days = df_pct_change.columns
            df_result = df_pct_change
            df_result = df_result[df_result[days[-1]] >=
                                  self.volume_change_3days_range[-1][0]]
            df_result = df_result[df_result[days[-1]] <=
                                  self.volume_change_3days_range[-1][1]]
            df_result = df_result[df_result[days[-2]] >=
                                  self.volume_change_3days_range[-2][0]]
            df_result = df_result[df_result[days[-2]] <=
                                  self.volume_change_3days_range[-2][1]]
            df_result = df_result[df_result[days[-3]] >=
                                  self.volume_change_3days_range[-3][0]]
            df_result = df_result[df_result[days[-3]] <=
                                  self.volume_change_3days_range[-3][1]]
            # self.log.debug('近3天连续放量：%s' % df_result)
            # 根据指定某天的涨幅做排序
            # df_result = df_result.sort_values(by=days[-1], ascending=False)
            list_result = list(df_result.index)
            self.log.debug('--> 成交量涨跌过滤结果：%s' % list_result)

        # 返回最终结果
        self.log.debug('--> 最终过滤结果: %s' % list_result)
        return list_result

    def __str__(self):
        return '龙头股选股过滤 [是否开启股价涨跌检查:%s, 股价涨跌检查是否开启分钟数据: %s, 股价涨跌检查天数: %d，' \
               '每日股价许可涨跌范围: %s, 是否开启 KDJ 检查: %s，KDJ 是否检查周线: %s, KDJ 周线许可范围: %s, ' \
               'KDJ 是否检查月线: %s, 是否开启成交量涨跌检查: %s, 近3日成交量许可涨跌范围: %s]' % \
               (self.enable_price_check, self.price_check_enable_minute_data, self.price_check_days,
                self.price_change_range, self.enable_kdj_check, self.kdj_check_week,
                self.kdj_week_value_range, self.kdj_check_month, self.enable_volume_check,
                self.volume_change_3days_range)


class BuyStocksPortion(Rule):
    """按比重股票调仓买入"""

    def __init__(self, params):
        Rule.__init__(self, params)
        self.buy_count = params.get('buy_count', 3)

    def update_params(self, context, params):
        Rule.update_params(self, context, params)
        self.buy_count = params.get('buy_count', self.buy_count)

    def adjust(self, context, data, buy_stocks):
        # 买入股票
        # 根据股票数量分仓
        # 此处只根据可用金额平均分配购买，不能保证每个仓位平均分配
        position_count = len(context.portfolio.positions)
        if self.buy_count > position_count:
            buy_num = self.buy_count - position_count
            portion_gen = generate_portion(buy_num)
            available_cash = context.portfolio.cash
            for stock in buy_stocks:
                if context.portfolio.positions[stock].total_amount == 0:
                    try:
                        buy_portion = portion_gen.next()
                        value = available_cash * buy_portion
                        if self.open_position(context, stock, value):
                            if len(context.portfolio.positions) == self.buy_count:
                                break
                    except StopIteration:
                        break
        pass

    def __str__(self):
        return '股票调仓买入规则：现金比重式买入股票达目标股票数: [ %d ]' % self.buy_count


class BuyStocksVaR(Rule):
    """使用 VaR 方法做调仓控制"""

    def __init__(self, params):
        Rule.__init__(self, params)
        self.buy_count = params.get('buy_count', 3)

    def update_params(self, context, params):
        Rule.update_params(self, context, params)
        self.buy_count = params.get('buy_count', self.buy_count)

    def handle_data(self, context, data):
        self.adjust(context, data, self.g.buy_stocks)

    def before_trading_start(self, context):
        # 设置 VaR 仓位控制参数。风险敞口: 0.05,
        # 正态分布概率表，标准差倍数以及置信率: 0.96, 95%; 2.06, 96%; 2.18, 97%; 2.34, 98%; 2.58, 99%; 5, 99.9999%
        # 赋闲资金可以买卖银华日利做现金管理: ['511880.XSHG']
        context.pc_var = PositionControlVar(context, 0.05, 2.58, [])

    def adjust(self, context, data, buy_stocks):
        # 买入股票或者进行调仓
        for pindex in self.g.op_pindexs:
            position_count = len(context.subportfolios[pindex].long_positions)
            buy_stocks = [i for i in buy_stocks if i not in self.g.sell_stocks]
            if self.buy_count > position_count:
                buy_num = self.buy_count - position_count
                context.pc_var.buy_the_stocks(context, buy_stocks[:buy_num])
            else:
                context.pc_var.func_rebalance(context)

    def after_trading_end(self, context):
        self.g.sell_stocks = []

    def __str__(self):
        return '股票调仓买入规则：使用 VaR 方式买入或者调整股票达目标股票数'


# ===================== VaR仓位控制 ===============================================

class PositionControlVar(object):
    """基于风险价值法（VaR）的仓位控制"""

    def __init__(self, context, risk_money_ratio=0.05, confidencelevel=2.58, moneyfund=[]):
        """ 相关参数说明：
            1. 设置风险敞口
            risk_money_ratio = 0.05

            2. 正态分布概率表，标准差倍数以及置信率
                1.96, 95%; 2.06, 96%; 2.18, 97%; 2.34, 98%; 2.58, 99%; 5, 99.9999%
            confidencelevel = 2.58

            3. 使用赋闲资金做现金管理的基金(银华日利)
            moneyfund = ['511880.XSHG']
        """
        self.risk_money = context.portfolio.portfolio_value * risk_money_ratio
        self.confidencelevel = confidencelevel
        self.moneyfund = self.__delete_new_moneyfund(context, moneyfund, 60)

    def __str__(self):
        return 'VaR仓位控制'

    # 剔除上市时间较短的基金产品
    def __delete_new_moneyfund(self, context, equity, deltaday):
        deltaDate = context.current_dt.date() - dt.timedelta(deltaday)

        tmpList = []
        for stock in equity:
            if get_security_info(stock).start_date < deltaDate:
                tmpList.append(stock)

        return tmpList

    # 卖出股票
    def sell_the_stocks(self, context, stocks):
        for stock in stocks:
            if stock in context.portfolio.positions.keys():
                # 这段代码不甚严谨，多产品轮动会有问题，本案例 OK
                if stock not in self.moneyfund:
                    equity_ratio = {}
                    equity_ratio[stock] = 0
                    trade_ratio = self.func_getequity_value(
                        context, equity_ratio)
                    self.func_trade(context, trade_ratio)

    # 买入股票
    def buy_the_stocks(self, context, stocks):
        equity_ratio = {}
        if len(stocks) == 0:
            return
        ratio = 1.0 / len(stocks)  # TODO 需要比照原始代码确认逻辑是否正确
        for stock in stocks:
            equity_ratio[stock] = ratio
        trade_ratio = self.func_getequity_value(context, equity_ratio)
        self.func_trade(context, trade_ratio)

    # 股票调仓
    def func_rebalance(self, context):
        myholdlist = list(context.portfolio.positions.keys())
        if myholdlist:
            for stock in myholdlist:
                if stock not in self.moneyfund:
                    equity_ratio = {stock: 1.0}
                    trade_ratio = self.func_getequity_value(
                        context, equity_ratio)
                    self.func_trade(context, trade_ratio)

    # 根据预设的 risk_money 和 confidencelevel 来计算，可以买入该多少权益类资产
    def func_getequity_value(self, context, equity_ratio):
        def __func_getdailyreturn(stock, freq, lag):
            hStocks = history(lag, freq, 'close', stock, df=True)
            dailyReturns = hStocks.resample('D', how='last').pct_change().fillna(
                value=0, method=None, axis=0).values
            return dailyReturns

        def __func_getStd(stock, freq, lag):
            dailyReturns = __func_getdailyreturn(stock, freq, lag)
            std = np.std(dailyReturns)
            return std

        def __func_getEquity_value(__equity_ratio, __risk_money, __confidence_ratio):
            __equity_list = list(__equity_ratio.keys())
            hStocks = history(1, '1d', 'close', __equity_list, df=False)

            __curVaR = 0
            __portfolio_VaR = 0

            for stock in __equity_list:
                # 每股的 VaR，VaR = 上一日的价格 * 置信度换算得来的标准倍数 * 日收益率的标准差
                __curVaR = hStocks[stock] * __confidence_ratio * \
                    __func_getStd(stock, '1d', 120)
                # 一元会分配买多少股
                # 1单位资金，分配时，该股票可以买多少股
                __curAmount = 1 * __equity_ratio[stock] / hStocks[stock]
                __portfolio_VaR += __curAmount * __curVaR  # 1单位资金时，该股票上的实际风险敞口

            if __portfolio_VaR:
                __equity_value = __risk_money / __portfolio_VaR
            else:
                __equity_value = 0

            if isnan(__equity_value):
                __equity_value = 0

            return __equity_value

        risk_money = self.risk_money
        equity_value, bonds_value = 0, 0

        equity_value = __func_getEquity_value(
            equity_ratio, risk_money, self.confidencelevel)
        portfolio_value = context.portfolio.portfolio_value
        if equity_value > portfolio_value:
            portfolio_value = equity_value
            bonds_value = 0
        else:
            bonds_value = portfolio_value - equity_value

        trade_ratio = {}
        equity_list = list(equity_ratio.keys())
        for stock in equity_list:
            if stock in trade_ratio:
                trade_ratio[stock] += round((equity_value *
                                             equity_ratio[stock] / portfolio_value), 3)
            else:
                trade_ratio[stock] = round(
                    (equity_value * equity_ratio[stock] / portfolio_value), 3)

        # 没有对 bonds 做配仓，因为只有一个
        if self.moneyfund:
            stock = self.moneyfund[0]
            if stock in trade_ratio:
                trade_ratio[stock] += round((bonds_value *
                                             1.0 / portfolio_value), 3)
            else:
                trade_ratio[stock] = round(
                    (bonds_value * 1.0 / portfolio_value), 3)
        log.info('trade_ratio: %s' % trade_ratio)
        return trade_ratio

    # 交易函数
    def func_trade(self, context, trade_ratio):
        def __func_trade(context, stock, value):
            log.info(stock + " 调仓到 " + str(round(value, 2)) + "\n")
            order_target_value(stock, value)

        def __func_tradeBond(context, stock, Value):
            hStocks = history(1, '1d', 'close', stock, df=False)
            curPrice = hStocks[stock]
            curValue = float(
                context.portfolio.positions[stock].total_amount * curPrice)
            deltaValue = abs(Value - curValue)
            if deltaValue > (curPrice * 100):
                if Value > curValue:
                    cash = context.portfolio.cash
                    if cash > (curPrice * 100):
                        __func_trade(context, stock, Value)
                else:
                    # 如果是银华日利，多卖 100 股，避免个股买少了
                    if stock == self.moneyfund[0]:
                        Value -= curPrice * 100
                    __func_trade(context, stock, Value)

        def __func_tradeStock(context, stock, ratio):
            total_value = context.portfolio.portfolio_value
            if stock in self.moneyfund:
                __func_tradeBond(context, stock, total_value * ratio)
            else:
                curPrice = history(1, '1d', 'close', stock,
                                   df=False)[stock][-1]
                curValue = context.portfolio.positions[stock].total_amount * curPrice
                Quota = total_value * ratio
                if Quota:
                    if abs(Quota - curValue) / Quota >= 0.25:
                        if Quota > curValue:
                            cash = context.portfolio.cash
                            if cash >= Quota * 0.25:
                                __func_trade(context, stock, Quota)
                        else:
                            __func_trade(context, stock, Quota)
                else:
                    __func_trade(context, stock, Quota)

        trade_list = list(trade_ratio.keys())

        hStocks = history(1, '1d', 'close', trade_list, df=False)

        myholdstock = list(context.portfolio.positions.keys())
        total_value = context.portfolio.portfolio_value

        # 已有仓位
        holdDict = {}
        hholdstocks = history(1, '1d', 'close', myholdstock, df=False)
        for stock in myholdstock:
            tmpW = round(
                (context.portfolio.positions[stock].total_amount * hholdstocks[stock]) / total_value, 2)
            holdDict[stock] = float(tmpW)

        # 对已有仓位做排序
        tmpDict = {}
        for stock in holdDict:
            if stock in trade_ratio:
                tmpDict[stock] = round(
                    (trade_ratio[stock] - holdDict[stock]), 2)
        tradeOrder = sorted(tmpDict.items(), key=lambda d: d[1], reverse=False)

        _tmplist = []
        for idx in tradeOrder:
            stock = idx[0]
            __func_tradeStock(context, stock, trade_ratio[stock])
            _tmplist.append(stock)

        # 交易其他股票
        for i in range(len(trade_list)):
            stock = trade_list[i]
            if len(_tmplist) != 0:
                if stock not in _tmplist:
                    __func_tradeStock(context, stock, trade_ratio[stock])
            else:
                __func_tradeStock(context, stock, trade_ratio[stock])


# ===================== 实盘类 =====================================
def trader_sync_all_day(context):
    global g
    log.info('【开始执行全天实盘同步】%s' % g.trader_instance)
    g.trader_instance._sync_execute(context)
    
    
class TraderBase(OpStocksRecord):
    """实盘跟单基类"""
    
    def __init__(self, params):
        OpStocksRecord.__init__(self, params)
        # 是否只在模拟盘时运行（除非在编译或回测时想调试实盘跟单，否则应设为 True）
        self.only_for_sim_trade = params.get('only_for_sim_trade', True)
        # 是否在发生调仓后执行同步
        self.sync_with_change = params.get('sync_with_change', True)
        # 是否执行本地全天同步（从交易前开始）
        # 貌似聚宽的每个用户程序执行都是单线程的，无论放在 handle_bar 或 run_daily 
        # 都不是并发执行，都可能导致延时。聚宽上不建议开启，建议使用远程仓位同步
        self.sync_all_day = params.get('sync_all_day', False)
        # 是否执行定时同步
        # 二维数组，如 [[10,30],[14,30]] 表示分别在10:30和14:30执行同步
        self.sync_timers = params.get('sync_timers', [])
        # 每次同步开平仓循环次数（每次开平仓耗时约20秒至几分钟）
        self.sync_loop_times = params.get('sync_loop_times', 1)
        # 是否使用限价单
        self.use_limit_order = params.get('use_limit_order', False)
        # 限价同步时若未找到调仓价格，是否使用当前价格
        self.use_curr_price = params.get('use_curr_price', True)
        # 分批执行时每次操作股票数（取值为100的整数），若为0则不分批
        self.batch_amount = params.get('batch_amount', 2500)
        # 同步操作工作时间段。二维数组，如 [[9,30,11,30],[13,0,15,0]] 表示两个有效的同步时间段
        self.sync_terms = params.get('sync_terms', [[9,30,11,30],[13,0,15,0]])
        # 是否在远程交易代理主机上做仓位同步（这将取代本地同步过程）
        self.sync_on_remote = params.get('sync_on_remote', True)

        self.s = None  # session
        self.last_adjust_time = None  # 当天最后调仓时间

    def update_params(self, context, params):
        self.only_for_sim_trade = params.get(
            'only_for_sim_trade', self.only_for_sim_trade)
        self.sync_with_change = params.get(
            'sync_with_change', self.sync_with_change)
        self.sync_all_day = params.get(
            'sync_all_day', self.sync_all_day)
        self.sync_timers = params.get(
            'sync_timers', self.sync_timers)
        self.sync_loop_times = params.get(
            'sync_loop_times', self.sync_loop_times)
        self.use_limit_order = params.get(
            'use_limit_order', self.use_limit_order)
        self.use_curr_price = params.get(
            'use_curr_price', self.use_curr_price)
        self.batch_amount = params.get(
            'batch_amount', self.batch_amount)
        self.sync_terms = params.get(
            'sync_terms', self.sync_terms)
        self.sync_on_remote = params.get(
            'sync_on_remote', self.sync_on_remote)

    def initialize(self, context):
        OpStocksRecord.initialize(self, context)

        if self.sync_all_day:
            global g
            g.trader_instance = self
            run_daily(trader_sync_all_day, time='9:15', reference_security='000300.XSHG')  # 聚宽函数

    def process_initialize(self, context):
        # 是否跳过远程真实跟单操作
        self._bypass_operate = self.only_for_sim_trade and context.run_params.type != 'sim_trade'

    def handle_data(self, context, data):
        # 貌似聚宽的 handle_data 是顺序执行的，一个执行完了才到下一个，所以这里尽量不要做过于耗时的操作
        for [hour, minute] in self.sync_timers:  # 定时同步
            if context.current_dt.hour == hour and context.current_dt.minute == minute:
                self._sync_execute(context)

    def after_adjust_end(self, context, data):
        self.last_adjust_time = time.time()
        # self.log.info('~~~ after_adjust_end: self.position_has_change: %s' % self.position_has_change)
        # 只在仓位变动后执行跟仓
        if self.sync_with_change:
            if self.position_has_change:
                self._sync_execute(context)
        #else:
        #    self._sync_execute(context)
        self.position_has_change = False

    def on_clear_position(self, context, pindex=[0]):
        self.last_adjust_time = time.time()
        if self.sync_with_change:
            if self.position_has_change:
                self._sync_execute(context)
        #else:
        #    self._sync_execute(context)
        self.position_has_change = False

    def before_trading_start(self, context):
        self.last_adjust_time = None

    def after_trading_end(self, context):
        self.last_adjust_time = None
        # 清除无法序列化的交易接口对象
        self.s = None
    
    def _sync_execute(self, context):
        if self.g.is_sim_trade and not self.get_session():
            return
        
        # 直接提交远程仓位同步任务
        if self.sync_on_remote:
            p_data = {}
            p_data['positions'] = self._positions_to_dict_jq(context)
            p_data['stocks_adjust_info'] = self.g.stocks_adjust_info
            p_data['use_limit_order'] = self.use_limit_order
            p_data['use_curr_price'] = self.use_curr_price
            p_data['batch_amount'] = self.batch_amount
            # 若为模拟盘，则做真实同步
            if self.g.is_sim_trade:
                self.log.info('>> 提交远程仓位同步信息：%s' % p_data)
                for i in range(3):
                    ret = self.s.sync_positions(p_data)
                    if ret:
                        self.log.info('>> 提交远程仓位同步任务成功！%s' % ret)
                        break
                    else:
                        time.sleep(2)
                else:
                    self.log.info('>> 提交远程仓位同步任务失败！！服务器或网络是否正常？')
            else:
                self.log.info('非模拟盘，假装提交远程仓位同步任务成功。')
            return
        
        # 本地调用远程接口做同步

        if self.g.trader_sync_is_running:
            self.log.warn('==> 有其他同步任务在进行中，取消本次同步。')
            return
        else:
            self.g.trader_sync_is_running = True

        # 如果启动后，没有加载过交易持久数据，则加载之
        global IMPORT_TAG
        if IMPORT_TAG:
            self.g.load_trading_status(context)
            IMPORT_TAG = False
        
        self.context = context
        diff_sell = True
        diff_buy = True
        pindex = 0
        loop_count = 0

        try:

            while (diff_sell or diff_buy):

                # 若为模拟盘，则做真实同步
                if self.g.is_sim_trade:

                    # 如果设置了有效同步时间段，则检查之
                    if len(self.sync_terms) > 0:
                        _now = dt.datetime.now()
                        _m = _now.hour * 60 + _now.minute
                        for [begin_h, begin_m, end_h, end_m] in self.sync_terms:
                            if begin_h * 60 + begin_m <= _m <= end_h * 60 + end_m:
                                break  # 在有效时间段内，退出检查
                        else:
                            # 不在有效同步时间段内
                            if _m > self.sync_terms[-1][-2] * 60 + self.sync_terms[-1][-1]:
                                self.log.warn('==> 已超出最大同步时间范围，退出同步。')
                                self.g.trader_sync_is_running = False
                                return
                            # 每5秒检查一次，每5分钟提示一次
                            if _m % 300 == 1:
                                self.log.info('不在有效同步时间段内，稍后再查……')
                            time.sleep(5)
                            continue
                        
                    self.log.warn(
                        '==> [%s] 开始实盘同步！[第%d次循环，%s] 若遭遇市场黑天鹅，请暂停策略并手工清仓！' \
                            % (dt.datetime.now(), loop_count + 1, 
                            '不限次数' if self.sync_all_day else '最多%d次' % self.sync_loop_times))
            
                    # 无论市价单还是限价单，跟盘都执行先卖后买
                    #TODO 如果是做T，则有时需先买后卖
                    if self.use_limit_order:  # 限价单跟盘
                        self.log.info('采用限价单跟盘，执行先卖后买操作！')
                    else:
                        self.log.info('采用市价单跟盘，执行先卖后买操作！')

                    # 首先撤销当前所有未完成买卖单
                    self.revoke(context)

                    diff_sell = self.__diff(
                        self.get_broker_positions(), context.subportfolios[pindex].long_positions)
                    if diff_sell:
                        self.log.info('执行卖出操作。操作前资金状况: ' +
                                    json.dumps(self.s.balance, ensure_ascii=False))
                        # self.log.info('卖出 ' + ' '.join(diff_sell) + ': ' + str(diff_sell))
                        self.close_list(context, diff_sell)
                        self.log.info(
                            '卖出操作结束。资金状况: ' + json.dumps(self.s.balance, ensure_ascii=False))

                    diff_buy = self.__diff(
                        context.subportfolios[pindex].long_positions, self.get_broker_positions())
                    if diff_buy:
                        self.log.info('执行买入操作。操作前资金状况: ' +
                                    json.dumps(self.s.balance, ensure_ascii=False))
                        # self.log.info('买入 ' + ' '.join(diff_buy) + ': ' + str(diff_buy))
                        self.open_list(context, diff_buy)
                        self.log.info(
                            '买入操作结束。资金状况: ' + json.dumps(self.s.balance, ensure_ascii=False))

                # 否则，做假同步
                else:
                    self.log.info('非模拟盘，假装同步x秒……')
                    #time.sleep(5)

                loop_count += 1
                if not self.sync_all_day and (diff_sell or diff_buy):
                    if loop_count >= self.sync_loop_times:
                        self.log.warn(
                            '==> [%s] 实盘同步结束！差额调仓已执行最多 %d 次' % (
                                dt.datetime.now(), self.sync_loop_times))
                        break
                    if self.g.is_sim_trade:
                        time.sleep(3)
                # self.log.info('同步后持仓: ' + json.dumps(self.get_broker_positions()))
        
        except Exception as e:
            # self.log.info(e)
            formatted_lines = traceback.format_exc().splitlines()
            self.log.info(formatted_lines)

        self.g.trader_sync_is_running = False

    def __diff(self, from_positions, to_positions):
        if not from_positions:
            from_positions = {}
        if not to_positions:
            to_positions = {}

        de = {}

        # from_positions 是否为券商
        flag = True if len(from_positions) > 0 and \
            list(from_positions.keys())[0] == list(from_positions.keys())[0][:6] \
            else False

        new_from = {}
        for x in from_positions:
            new_from[x[:6]] = from_positions[x]
        new_to = {}
        for x in to_positions:
            new_to[x[:6]] = to_positions[x]

        if flag:  # from 为券商，key格式：'002696'
            for stock in new_from:
                if new_from[stock]['Amount'] > 0:
                    if stock in new_to:
                        if new_from[stock]['Amount'] > new_to[stock].total_amount:
                            self.log.info('__diff(): sell %s, amount from ' % stock + str(
                                new_from[stock]['Amount']) + ', to ' + str(new_to[stock].total_amount))
                            new_from[stock]['Amount'] -= new_to[stock].total_amount
                            de[stock] = new_from[stock]
                    else:
                        self.log.info('__diff(): sell %s, amount ' %
                                      stock + str(new_from[stock]['Amount']))
                        de[stock] = new_from[stock]
        else:  # from 为 joinquant，key格式：'002696.XSHE'
            for stock in new_from:
                # self.log.info('---> help(new_from[%s]): %s' % (stock, help(new_from[stock])))
                if new_from[stock].total_amount > 0:
                    one = {}
                    one['Code'] = stock[:6]
                    one['Price'] = new_from[stock].hold_cost
                    one['NowPrice'] = new_from[stock].price
                    one['TotalAmount'] = new_from[stock].total_amount
                    one['Amount'] = new_from[stock].total_amount
                    if stock in new_to:
                        delta = one['Amount'] - new_to[stock]['Amount']
                        if delta >= 0:  # 相等也加入，便于后续调仓
                            self.log.info(
                                '_diff(): buy %s, amount from ' % stock + str(new_to[stock]['Amount']) + ', to ' + str(
                                    one['Amount']))
                            one['Amount'] = delta
                            de[stock] = one
                    else:
                        self.log.info('_diff(): buy %s, amount ' %
                                      stock + str(one['Amount']))
                        de[stock] = one

        return de if len(de) > 0 else False

    def _positions_to_dict_jq(self, context, pindex=0):
        positions_dict = {}
        for position in list(context.subportfolios[pindex].long_positions.values()):
            #print('--> position:', position)
            # UserPosition({
            #   'security': '512280.XSHG', 'price': 1.607, 'total_amount': 15600, 
            #   'closeable_amount': 0, 'avg_cost': 1.607, 'acc_avg_cost': 1.607})
            positions_dict[position.security] = {}
            positions_dict[position.security]['security'] = position.security  # 标的代码
            positions_dict[position.security]['price'] = position.price  # 最新行情价格
            positions_dict[position.security]['total_amount'] = position.total_amount  # 总仓位, 但不包括挂单冻结仓位
            positions_dict[position.security]['locked_amount'] = position.locked_amount  # 挂单冻结仓位
            positions_dict[position.security]['closeable_amount'] = position.closeable_amount  # 可卖出的仓位
            positions_dict[position.security]['avg_cost'] = position.avg_cost  # 当前持仓成本，只在开仓/加仓时更新
            positions_dict[position.security]['acc_avg_cost'] = position.acc_avg_cost  # 累计持仓成本，在清仓/减仓时也更新
            positions_dict[position.security]['hold_cost'] = position.hold_cost  # 当日持仓成本
            positions_dict[position.security]['value'] = position.value  # 标的价值
        return positions_dict

    def sync_positions(self, context, p_data):
        pass

    def close_list(self, context, stocks):
        pass

    def revoke(self, context):
        pass

    def open_list(self, context, stocks):
        pass

    def get_broker_positions(self):
        pass

    def get_session(self):
        pass

    def __str__(self):
        return '实盘跟单基类'


class TraderShipane(TraderBase):
    """实盘易跟单（兼容官网版及自开发版服务端）
    所有买卖操作，如果没有强制使用限价单，则只有最后一次用限价单，之前用市价单；
    另外，最后一次操作执行后不撤销，之前的操作在再次循环前都会撤销掉。

    # 实盘跟单目前尚未解决的问题:
    1、?
       * 解决方案:
       A、。
    2、是否哪里计算还有问题?
       * 解决方案：
       A、加调试代码,仔细分析：待续。
    """

    #TODO close_list 和 open_list 支持三种订单类型，可做策略参数配置：
    # MARKET：市价单，LIMIT：限价单，HYPER：第一次循环为市价单，后续为限价单

    #TODO 支持劵软多账号

    def __init__(self, params):
        super(TraderShipane, self).__init__(params)
        self.server_type = params.get('server_type', 'mybroker')
        self.op_buy_ratio = params.get('op_buy_ratio', 1.0)  # 实际买入比例

        self.s = None
        self.broker_portfolio = 0
        self.positions = {}

        # 同花顺模拟交易持仓行格式: 证券代码 证券名称 股票余额 可用余额 冻结数量 盈亏 成本价 盈亏比例(%)
        # 市价 市值 交易市场 股东帐户 实际数量 可申赎数量
        # self._broker_position_title = \
        #    {'code': u'证券代码', 'name': u'证券名称', 'cost_price': u'成本价',
        #    'current_price': u'市价', 'holdings': u'实际数量', 'market_value': u'市值'}
        # 1、老版同花顺真实交易持仓行格式: 证券代码 证券名称 持仓数量 可用数量 冻结数量 浮动盈亏 参考成本价 盈亏比例(%)
        # 当前价 最新市值 买入价值 市场代码 交易市场 股东帐户 当前股数 可申赎数量 资讯 未知 未知
        # 2、新版同花顺真实交易持仓行格式: 证券代码 证券名称 持仓数量 可用数量 冻结数量 参考成本价 当前价 浮动盈亏
        # 盈亏比例(%) 最新市值 交易市场
        self._broker_position_title = \
            {'code': u'证券代码', 'name': u'证券名称', 'cost_price': u'参考成本价',
             'current_price': u'当前价', 'holdings': u'持仓数量', 'market_value': u'最新市值'}

    def update_params(self, context, params):
        super(TraderShipane, self).update_params(params)
        
        self.server_type = params.get('server_type', self.server_type)
        self.op_buy_ratio = params.get('op_buy_ratio', self.op_buy_ratio)

    def __make_session(self):
        for i in range(1, 5):
            try:
                # 可选参数包括：host, port, client 等，请将下面的 IP 替换为实际 IP
                # !!! 实盘易账号密码当前为明文，尽量避免使用
                self.s = web_api_client.JoinQuantExecutor(
                    host='127.0.0.1',  # localhost
                    port=8888 if self.server_type == 'shipane' else 8889,
                    key='xxxxyyyyzzzz',
                    client='title:monijiaoyi',
                    crypto=False if self.server_type == 'shipane' else True
                )
                break
            except Exception as e:
                message = 'Create session FAILED for %d times, do retry. Fail detail: %s' % (
                    i, str(e))
                self.log.warn(message)
                formatted_lines = traceback.format_exc().splitlines()
                self.log.warn(formatted_lines)
            time.sleep(1)
        else:
            self.log.error('Create session FAILED!! Please check it.')

    def process_initialize(self, context):
        super(TraderShipane, self).process_initialize(context)
        self.context = context
        self.log.info('操作所占总仓位的比例：%2f %%' % (self.op_buy_ratio * 100))
        self.get_session()

    def execute_order(self, order_):
        '''嵌入到策略代码中，在执行完买卖操作后调用'''

        if not order_:
            return
        if not self.get_session():
            return
        if not self._bypass_operate:
            self.s.execute(order_)

    def cancel_order(self, order_):
        '''嵌入到策略代码中，在执行完取消订单操作后调用'''

        if not order_:
            return
        if not self.get_session():
            return
        if not self._bypass_operate:
            self.s.cancel(order_)

    def sync_positions(self, context, p_data):
        if not self.get_session():
            return False
        return self.s.sync_positions(p_data)

    def _close_once(self, context, stock, price, amount):
        """单次卖出操作"""

        #self.log.debug('close_list(): stock: %s, price: %f, amount: %d' % (stock, price, amount))
        if not self._bypass_operate:
            r = self.s.sell(
                stock, price=price, amount=amount)
    
    def close_list(self, context, stocks):
        if not stocks:
            return
        if not self.get_session():
            return

        for stock in stocks:
            if self.context.run_params.type in ('simple_backtest', 'full_backtest'):
                del self.positions[stock[:6]]
            else:
                price = 0  # 默认市价卖出
                if self.use_limit_order:
                    # stock 不带市场后缀，判断时需特殊处理。stock 示例：161903，
                    # g.stocks_adjust_info 示例：{'160421.XSHE': 1.72, '161219.XSHE': 2.932...}
                    for _x in self.g.stocks_adjust_info:
                        if stock == _x.split('.')[0]:
                            # 使用第一次同步价格
                            price = self.g.stocks_adjust_info[_x]['adjust_price']
                            break
                    else:
                        self.log.warn('%s: 未找到调仓时价格！' % stock)
                        if self.use_curr_price:
                            # 取当前价格作为同步价格
                            price = round(stocks[stock]['NowPrice'], 4)
                            self.log.warn('%s: 使用当前价格卖出: %.4f' % (stock, price))
                        else:
                            self.log.warn('取消卖出')
                            continue
                
                # self.log.info('close_list() adjust_weight: stock: %s, close position' % stock)
                # if not self._bypass_operate:
                #     r = self.s.adjust_weight(stock, 0)  # 全部清仓，这里不合适

                op_times = 1  # 分多少批卖出
                if self.batch_amount > 100:
                    op_times = int(
                        stocks[stock]['Amount'] / self.batch_amount) + 1
                # self.log.info('close_list(): broker/jq, op_times: %2d' % op_times)

                amount = int(self.batch_amount / 100) * 100  # 购股数量为100的整数倍
                for i in range(op_times - 1):
                    if amount > 0:
                        self._close_once(context, stock, price, amount)
                else:
                    # 最后一次为分批后的余数
                    amount = int(
                        (stocks[stock]['Amount'] - self.batch_amount * (op_times - 1)) / 100) * 100
                    if amount > 0:
                        self._close_once(context, stock, price, amount)

                if price > 0:  # 若为限价单，则等待一会，增加卖出成功率
                    time.sleep(15)
        
    def _open_once(self, context, stock, price, amount):
        """单次买入操作"""

        #self.log.debug('open_list(): stock: %s, price: %f, amount: %f' % (stock, price, amount))
        if not self._bypass_operate:
            r = self.s.buy(
                stock, price=price, amount=amount)

    def open_list(self, context, stocks):
        if not stocks:
            return
        if not self.get_session():
            return

        for stock in stocks:
            buy_amount_min = 100
            if stocks[stock]['Amount'] * self.op_buy_ratio < buy_amount_min:
                self.log.info('open_list() delta amount of %s less than %d, pass.' % (
                    stock, buy_amount_min))
                continue
            if self.context.run_params.type in ('simple_backtest', 'full_backtest'):
                self.positions[stock[:6]] = stocks[stock]
            else:
                price = 0  # 默认市价买入
                if self.use_limit_order:
                    for _x in self.g.stocks_adjust_info:
                        if stock == _x.split('.')[0]:
                            # 使用第一次同步价格
                            price = self.g.stocks_adjust_info[_x]['adjust_price']
                            break
                    else:
                        self.log.warn('%s: 未找到调仓时价格！' % stock)
                        if self.use_curr_price:
                            # 取当前价格作为同步价格
                            price = round(stocks[stock]['NowPrice'], 4)
                            self.log.warn('%s: 使用当前价格买入: %.4f' % (stock, price))
                        else:
                            self.log.warn('取消买入')
                            continue

                op_times = 1  # 分多少批买入
                if self.batch_amount > 100:
                    op_times = int(
                        stocks[stock]['Amount'] * self.op_buy_ratio / self.batch_amount) + 1
                # log.info('open_list(): broker/jq ratio: %2f, op_times: %2d' % (self.op_buy_ratio, op_times))

                amount = int(self.batch_amount / 100) * 100  # 购股数量为100的整数倍
                for i in range(op_times - 1):
                    if amount > 0:
                        self._open_once(context, stock, price, amount)
                else:
                    # 最后一次为分批后的余数
                    amount = int((stocks[stock]['Amount'] * self.op_buy_ratio - \
                        self.batch_amount * (op_times - 1)) / 100) * 100
                    if amount > 0:
                        self._open_once(context, stock, price, amount)
                
                if price > 0:  # 若为限价单，则等待一会，增加买入成功率
                    time.sleep(15)
    
    def revoke(self, context, way=''):  # 撤买、撤卖或全撤
        if self.context.run_params.type in ('simple_backtest', 'full_backtest'):
            return
        if not self.get_session():
            return

        for x in range(1):  # 循环检查撤单x次
            for i in self.s.entrust:
                #if True:
                if i['entrust_status'] in ('pending'):  # 除了 pending 还有其他失败状态？
                    if way == 'buy' and i['entrust_way'] != 'buy':
                        continue
                    elif way == 'sell' and i['entrust_way'] != 'sell':
                        continue
                    if not self._bypass_operate:
                        r = self.s.cancel_entrust(i['entrust_no'])
                        self.log.info('revoke() entrust: %s' % str(i))
                        break #TODO 目前THS实盘跟单接口只支持全撤，所以调用一次即可
                    time.sleep(1)
            time.sleep(3)

    def get_broker_positions(self):
        if self.context.run_params.type in ('simple_backtest', 'full_backtest'):
            return self.positions
        if not self.get_session():
            return
        broker_positions = {}
        bp = self.s.get_positions()
        # self.log.info('==> bp["positions"].columns: ' + str(bp['positions'].columns))
        for i, row in bp['positions'].iterrows():
            # row: 证券代码  证券名称  股票余额 可用余额 冻结数量 盈亏 成本价 盈亏比例(%)
            # 市价 市值 交易市场 股东帐户 实际数量 可申赎数量
            # self.log.info('==> row: ' + str(row))
            one = {}
            # row[u'证券代码'] format: '002696'
            # 为便于__diff()处理，需要转为兼容格式(002696)
            one['Code'] = row[self._broker_position_title['code']][-6:]
            one['Price'] = (lambda x: float(x) if len(x) > 0 else 0.0)(
                row[self._broker_position_title['cost_price']])
            one['NowPrice'] = (lambda x: float(x) if len(x) > 0 else 0.0)(
                row[self._broker_position_title['current_price']])
            one['Amount'] = (lambda x: int(float(x)) if len(x) > 0 else 0.0)(
                row[self._broker_position_title['holdings']])
            broker_positions[one['Code']] = one
        return broker_positions

    def get_session(self):

        def make_session(self, i):
            try:
                if self.s is None:
                    self.__make_session()
                if self.s is not None:
                    if self.s.balance:
                        return True
            except Exception as e:
                message = '[#%d] call self.s.balance FAILED! Error info: %s' % (
                    i, str(e))
                log.error(message)
                send_message(message, channel='weixin')
                return False

        if self.context.run_params.type in ('simple_backtest', 'full_backtest'):
            return True

        for i in range(2):
            if make_session(self, i):
                return True
            else:
                time.sleep(2)

        return False

    def before_trading_start(self, context):
        TraderBase.before_trading_start(self, context)
        self.follow_adjust_count = 0
        if not self.g.is_sim_trade:
            return
        if not self.get_session():
            return

        msg = '当日交易前实盘持仓情况: \n%s' % str(self.__get_position_in_table())
        log.info(msg)
        send_message(msg, channel='weixin')

    def after_trading_end(self, context):
        TraderBase.after_trading_end(self, context)
        self.follow_adjust_count = 0
        if not self.g.is_sim_trade:
            return
        if not self.get_session():
            return

        # 清除无法序列化的交易接口对象
        self.s = None

        # 撤销当天所有未完成买卖单
        self.revoke(context)

        try:
            log.info('-' * 60)
            log.info(self.__str__() + ' - 今日交易统计信息')
            log.info('    ' + '-' * 30)
            balance = self.s.balance
            record(enable_balance=balance[0]['enable_balance'])
            record(market_balance=balance[0]['market_balance'])
            record(asset_balance=balance[0]['asset_balance'])
            log.info('    资金状况: ', json.dumps(balance, ensure_ascii=False))
            log.info('    可用金额: ', json.dumps(
                balance[0]['enable_balance'], ensure_ascii=False))
            log.info('    ' + '-' * 30)
            log.info('    获取持仓: ')
            table = self.__get_position_in_table()
            self.log.info('\n' + str(table))
            if context.run_params.type == 'sim_trade':
                msg = '    今日委托单: %s' % json.dumps(
                    self.s.entrust, ensure_ascii=False)
                log.info(msg)
                send_message(str(table) + msg, channel='weixin')
        except Exception as e:
            message = '！！！调用实盘易 API 获取当日信息失败！详情：%s' % (str(e))
            log.error(message)
            formatted_lines = traceback.format_exc().splitlines()
            self.log.info(formatted_lines)
        
    def __get_position_in_table(self):
        table = PrettyTable(
            ["股票代码", "股票名称", "购入均价", "当天收盘价", "当前盈亏比", "持仓数量", "持仓占比"])
        table.align["代码"] = "1"  # 以代码字段左对齐
        table.padding_width = 1  # 填充宽度
        bp = self.s.get_positions()
        for idx, i in bp['positions'].iterrows():

            row = []
            row.append(i[self._broker_position_title['code']])
            row.append(i[self._broker_position_title['name']])
            cost_price = i[self._broker_position_title['cost_price']]
            row.append(cost_price)
            current_price = i[self._broker_position_title['current_price']]
            row.append(current_price)

            ratio = 0.0
            if len(current_price) > 0 and len(cost_price) > 0:
                if float(cost_price) > 0:  # 同花顺有时会保留为0的持仓信息
                    ratio = (float(current_price) -
                             float(cost_price)) / float(cost_price)
            row.append(str(round(ratio * 100, 2)) + '%')

            current_holdings = i[self._broker_position_title['holdings']]
            row.append(current_holdings)

            market_value = i[self._broker_position_title['market_value']]
            if len(market_value) > 0:
                ratio = float(market_value) / \
                    float(self.s.balance[0]['asset_balance'])
            row.append(str(round(ratio * 100, 2)) + '%')

            table.add_row(row)
        return table

    def __str__(self):
        return '策略实盘跟单(实盘易)：[服务端类型：%s，是否在发生调仓后执行同步：%s，是否全天执行同步：%s，' \
                '定时同步时间：%s，每次同步开平仓循环次数（非全天同步时）：%d 次，是否使用限价单：%s，' \
                '资金投入比例：%0.2f，每次分批操作股数：%d]' % \
               (self.server_type, self.sync_with_change, self.sync_all_day, 
                self.sync_timers, self.sync_loop_times, self.use_limit_order,
                self.op_buy_ratio, self.batch_amount)


class TraderXzsec(TraderBase):
    """东方财富证券实盘跟单"""

    def __init__(self, params):
        super(TraderXzsec, self).__init__(params)
        self.trader_client = params.get('client', 'xzsec')
        self.trader_baseurl = params.get('url', 'http://sohunjug.com:8001/')
        self.username = params.get('username', '')
        self.password = params.get('password', '')
        self.excute_times_max = 10  # 开平仓操作次数，实际使用时需大于最大分批次数
        self.s = None
        self.count = 0
        self.positions = {}

    def process_initialize(self, context):
        self.get_session()

    def close_list(self, context, stocks):
        if not stocks:
            return
        if not self.get_session():
            return
        count = 0
        need_loop = True
        while need_loop:
            need_loop = False
            for stock in stocks:
                url = self.trader_baseurl + 'trade'
                p = dict(
                    code=stock,
                    price=stocks[stock]['NowPrice'] - 0.02,
                    amount=stocks[stock]['Amount'],
                    type='s'
                )
                if self.context.run_params.type in ('simple_backtest', 'full_backtest'):
                    del self.positions[stock[:6]]
                else:
                    r = self.s.post(url, data=json.dumps(p))
            positions = self.get_broker_positions()
            for stock in stocks:
                if stock in positions:
                    if positions[stock]['Amount'] > 0:
                        need_loop = True
                        count += 1
                        break
            time.sleep(1)
            self.revoke(context)
            if count > self.excute_times_max:
                self.log.error('close_list() 内执行下单操作已经达到 %d 次，退出执行。该轮操作可能不完整，'
                               '请分析处理。' % self.excute_times_max)
                break

    def revoke(self, context):
        if self.context.run_params.type in ('simple_backtest', 'full_backtest'):
            return
        if not self.get_session():
            return
        url = self.trader_baseurl + 'revokelist'
        positions = json.loads(str(self.s.get(url).text))
        for stock in positions:
            url = self.trader_baseurl + 'revokeorder'
            p = dict(
                order_id=stock['RevokeID']
            )
            r = self.s.post(url, data=json.dumps(p))

    def open_list(self, context, stocks):
        if not stocks:
            return
        if not self.get_session():
            return
        count = 0
        need_loop = True
        while need_loop:
            need_loop = False
            for stock in stocks:
                url = self.trader_baseurl + 'trade'
                p = dict(
                    code=stock,
                    price=stocks[stock].price + count * 0.02,
                    amount=stocks[stock].total_amount,
                    type='b'
                )
                if self.context.run_params.type in ('simple_backtest', 'full_backtest'):
                    one = {}
                    one['Code'] = stock[:6]
                    one['Price'] = stocks[stock].price
                    one['NowPrice'] = stocks[stock].price
                    one['Amount'] = stocks[stock].total_amount
                    self.positions[stock[:6]] = one
                else:
                    r = self.s.post(url, data=json.dumps(p))
            positions = self.get_broker_positions()
            for stock in stocks:
                if stock[:6] not in positions:
                    need_loop = True
                    count += 1
                    break
            time.sleep(1)
            self.revoke(context)
            if count > self.excute_times_max:
                self.log.error('open_list() 内执行下单操作已经达到 %d 次，退出执行。该轮操作可能不完整，'
                               '请分析处理。' % self.excute_times_max)
                break

    def get_broker_positions(self):
        if self.context.run_params.type in ('simple_backtest', 'full_backtest'):
            return self.positions
        if not self.get_session():
            return
        url = self.trader_baseurl + 'list'
        return json.loads(str(self.s.get(url).text.encode('utf-8')))

    def get_session(self):
        if self.context.run_params.type in ('simple_backtest', 'full_backtest'):
            return True
        if self.count > 10:
            return False
        flag = True
        if self.s is not None:
            url = self.trader_baseurl + 'account'
            r = self.s.get(url)
            if 'Crash' in r.text:
                flag = False
        if flag:
            self.s = requests.session()
            p = dict(
                username=self.username,
                password=self.password
            )
            url = self.trader_baseurl + 'login'
            r = self.s.post(url, data=json.dumps(p))
            if 'token' not in r.text:
                self.count += 1
                return False
            self.s.headers['Authorization'] = json.loads(r.text)['token']
        return True

    def after_trading_end(self, context):
        # 清除无法序列化的交易接口对象
        self.s = None
        # 撤销当天所有未完成买卖单
        self.revoke(context)

    def __str__(self):
        return '策略实盘跟单(东方财富证券)'


class TraderXueqiu(TraderBase):
    """雪球跟单
    TODO !!! 雪球跟单目前存在的未解决问题:
    1、下单大量买小股后,返回的成交信息与提交的申请不一致,数量都变成了100,并且价格变得很高。是否是在雪球自己的私有池里操作?
       * 解决方案:
       A、查证是否属实：改为分批操作后，收到的单价降低，基本确认雪球模拟炒股不太真实，尤其在小市值情况；
       B、换实盘易方案对比：基本实现。
    2、经常出现某些股买入过量,某些股无足够资金购买的情况。是否哪里计算还有问题?
       * 解决方案：
       A、加调试代码,仔细分析：待续。
    """

    def __init__(self, params):
        super(TraderXueqiu, self).__init__(params)
        # TODO 如果开平仓操作次数 > 1，实盘时如果券商不能及时完成买卖、或者持仓状态刷新不及时，
        # 则可能会导致多次调仓操作。在未解决该问题前，建议取该值为1。若操作失败则告警并手工补全。
        self.excute_times_max = 1  # 开平仓操作次数
        self.broker_portfolio = 0
        self.batch_amount = 500  # 每次操作的股票数量
        self.s = None
        self.positions = {}

    def process_initialize(self, context):
        self.context = context
        self.get_session()

    def update_params(self, context, params):
        pass

    def close_list(self, context, stocks):
        if not stocks:
            return
        if not self.get_session():
            return
        count = 0
        flag = True
        while flag:
            flag = False
            for stock in stocks:
                if self.context.run_params.type in ('simple_backtest', 'full_backtest'):
                    del self.positions[stock[:6]]
                else:
                    # price = round(stocks[stock]['NowPrice'] - 0.02, 2)
                    # amount = int(stocks[stock]['Amount'])
                    # log.info('===> self.s.sell() stock: %s, price: %f, amount: %f' % (stock, price, amount))
                    # r = self.s.sell(stock, price=price, amount=amount)
                    log.info(
                        '===> self.s.adjust_weight() stock: %s, close position' % stock)
                    r = self.s.adjust_weight(stock, 0)
                    time.sleep(3)
            positions = self.get_broker_positions()
            for stock in stocks:
                if stock in positions:
                    if positions[stock]['Amount'] > 0:
                        flag = True
                        count += 1
                        break
            self.revoke(context)
            if self.excute_times_max > 1 and count > self.excute_times_max:
                self.log.error('close_list() 内执行下单操作已经达到 %d 次，退出执行。该轮操作可能不完整，'
                               '请分析处理。' % self.excute_times_max)
                break

    def revoke(self, context):
        if self.context.run_params.type in ('simple_backtest', 'full_backtest'):
            return
        if not self.get_session():
            return
        for i in self.s.entrust:
            if i['entrust_status'] in ('pending'):
                r = self.s.cancel_entrust(i['entrust_no'])

    def open_list(self, context, stocks):
        if not stocks:
            return
        if not self.get_session():
            return
        self.broker_portfolio = self.s.balance[0]['asset_balance']
        count = 1
        flag = True
        while flag:
            flag = False
            for stock in stocks:
                if self.context.run_params.type in ('simple_backtest', 'full_backtest'):
                    one = {}
                    one['Code'] = stock[:6]
                    one['Price'] = stocks[stock].price
                    one['NowPrice'] = stocks[stock].price
                    one['Amount'] = (1.0 / len(stocks)) * 100
                    self.positions[stock[:6]] = one
                else:
                    price = round(stocks[stock].price + count * 0.02, 2)
                    # ratio = self.broker_portfolio / self.context.portfolio.total_value
                    ratio = 1  # 上述计算法不合适，建议采用动态调整模拟盘资金的方式
                    op_times = 1  # 默认一次性买入，不分批
                    if self.batch_amount > 100:
                        op_times = int(
                            stocks[stock].total_amount / self.batch_amount) + 1  # 分批买入
                    log.info(
                        'open_list(): broker/jq ratio: %2f, op_times: %2d' % (ratio, op_times))
                    # 雪球的 amount 为改变的百分比*100后的整数
                    amount = int(ratio * (self.batch_amount *
                                          price / self.broker_portfolio * 100))
                    for i in range(op_times - 1):
                        if amount > 0:
                            log.info('open_list(): stock: %s, price: %f, amount: %f' % (
                                stock, price, amount))
                            r = self.s.buy(stock, price=price, amount=amount)
                        time.sleep(3)
                    else:
                        amount = int(ratio * ((stocks[stock].total_amount - self.batch_amount * (op_times - 1))
                                              * price / self.broker_portfolio * 100))
                        if amount > 0:
                            log.info('open_list(): stock: %s, price: %f, amount: %f' % (
                                stock, price, amount))
                            r = self.s.buy(stock, price=price, amount=amount)
                    # or self.s.adjust_weight(stock, amount)
                    time.sleep(3)
            positions = self.get_broker_positions()
            for stock in stocks:
                if stock[:6] not in positions:
                    flag = True
                    count += 1
                    break
            self.revoke(context)
            if count > self.excute_times_max:
                self.log.warn('open_list() 内执行下单操作已经达到 %d 次，退出执行。该轮操作可能不完整，'
                              '请分析处理。' % self.excute_times_max)
                break

    def get_broker_positions(self):
        if self.context.run_params.type in ('simple_backtest', 'full_backtest'):
            return self.positions
        if not self.get_session():
            return
        broker_positions = {}
        for i in self.s.position:
            one = {}
            one['Code'] = i['stock_code'][-6:]
            one['Price'] = i['last_price']
            one['NowPrice'] = i['last_price']
            one['Amount'] = i['enable_amount']
            broker_positions[one['Code']] = one
        return broker_positions

    def get_session(self):
        if self.context.run_params.type in ('simple_backtest', 'full_backtest'):
            return True
        if self.s is None:
            self.s = xq_loading(self.context)
        if self.s is not None:
            if self.s.balance:
                return True
        return False

    def after_trading_end(self, context):
        if context.run_params.type in ('simple_backtest', 'full_backtest'):
            return
        if not self.get_session():
            return
        try:
            log.info('-' * 60)
            log.info(self.__str__() + ' - 今日交易统计信息')
            log.info('    ' + '-' * 30)
            log.info('    资金状况: ', json.dumps(
                self.s.balance, ensure_ascii=False))
            log.info('    可用金额: ',
                     json.dumps(self.s.balance[0]['enable_balance'], ensure_ascii=False))
            log.info('    ' + '-' * 30)
            log.info('    获取持仓: ', json.dumps(
                self.s.position, ensure_ascii=False))
            table = PrettyTable(
                ["股票代码", "股票名称", "购入均价", "当天收盘价", "当前盈亏比", "持仓数量", "持仓占比"])
            table.align["代码"] = "1"  # 以代码字段左对齐
            table.padding_width = 1  # 填充宽度
            for cs in self.s.position:
                row = []
                row.append(cs['stock_code'])
                row.append(cs['stock_name'])
                row.append(cs['cost_price'])
                row.append(cs['last_price'])
                ratio = (float(cs['last_price']) -
                         float(cs['cost_price'])) / float(cs['cost_price'])
                row.append(str(round(ratio * 100, 2)) + '%')
                row.append(cs['current_amount'])
                ratio = float(cs['market_value']) / \
                    float(self.s.balance[0]['asset_balance'])
                row.append(str(round(ratio * 100, 2)) + '%')
                table.add_row(row)
            self.log.info(table)
            if context.run_params.type == 'sim_trade':
                log.info('    今日委托单: ', json.dumps(
                    self.s.entrust, ensure_ascii=False))
        except Exception as e:
            message = '！！！调用雪球 API 获取当日信息失败！详情：%s' % (str(e))
            log.error(message)
            formatted_lines = traceback.format_exc().splitlines()
            self.log.info(formatted_lines)
        # 清除无法序列化的交易接口对象
        self.s = None

    def __str__(self):
        return '策略实盘跟单(雪球)'


####################################################################
# 通用函数库
####################################################################

class FuncLib(object):
    """本策略框架所用的各种自定义通用函数库"""

    '''
    MACD 指标属于大势趋势类指标，它由长期均线MACD，短期均线DIF，红色能量柱（多头），绿色能量柱(空头)，0轴（多空分界线）五部分组成。
    它是利用短期均线DIF与长期均线MACD交叉作为信号。MACD指标所产生的交叉信号较迟钝，而作为制定相应的交易策略使用效果较好。
    '''

    @staticmethod
    def myMACD(context, stock, fastperiod=12, slowperiod=26, signalperiod=9, enable_minute_data=False):
        """计算MACD指标（支持当天分钟级，无未来函数）
        """
        current_dt = context.current_dt
        df = attribute_history(security=stock, count=slowperiod * 4, unit='1d',
                               fields=['close'], skip_paused=True, df=True, fq='pre')
        if enable_minute_data:
            # last_prices = history(1, unit='1m', field='close', security_list=[stock])
            # df.loc[current_dt, 'close'] = last_prices[stock][0]
            df_hData_now = get_price(
                stock, count=1, end_date=current_dt, frequency='1m')
            df.loc[current_dt, 'close'] = df_hData_now['close'].values[0]

        ewma12 = pd.DataFrame.ewm(df['close'], span=fastperiod).mean()
        ewma60 = pd.DataFrame.ewm(df['close'], span=slowperiod).mean()

        dif = ewma12 - ewma60
        dea = pd.DataFrame.ewm(dif, span=signalperiod).mean()
        # 有些地方的bar = (dif-dea)*2，但是talib中MACD的计算是bar = (dif-dea)*1
        macd = (dif - dea) * 2

        return dif, dea, macd

    @staticmethod
    def taMACD(context, stock, fastperiod=12, slowperiod=26, signalperiod=9, enable_minute_data=False):
        """使用 talib 方式计算MACD指标（与聚宽计算值接近）
        """
        current_dt = context.current_dt
        df = attribute_history(security=stock, count=slowperiod * 4, unit='1d',
                               fields=['close'], skip_paused=True, df=True, fq='pre')
        if enable_minute_data:
            # last_prices = history(1, unit='1m', field='close', security_list=[stock])
            # df.loc[current_dt, 'close'] = last_prices[stock][0]
            df_hData_now = get_price(
                stock, count=1, end_date=current_dt, frequency='1m')
            df.loc[current_dt, 'close'] = df_hData_now['close'].values[0]

        # 使用 talib 方式计算 MACD，不用当天数据
        _close = df['close'].as_matrix()
        dif, dea, macd = talib.MACD(
            _close, fastperiod, slowperiod, signalperiod)

        return dif, dea, macd

    # 
    @staticmethod
    def jqMACD(context, stock, fastperiod=12, slowperiod=26, signalperiod=9):
        """使用聚宽函数计算MACD指标（据说用到了未来数据，但结果与使用 talib 计算值很接近）
        """
        from jqlib.technical_analysis import MACD
        current_dt = context.current_dt
        check_date = current_dt.strftime('%Y-%m-%d')
        previous_date = (current_dt - dt.timedelta(days=1)
                         ).strftime('%Y-%m-%d')

        dif = []
        dea = []
        macd = []

        _dif, _dea, _macd = MACD(stock, check_date=previous_date,
                                 SHORT=fastperiod, LONG=slowperiod, MID=signalperiod)
        dif.append(_dif[stock])
        dea.append(_dea[stock])
        macd.append(_macd[stock])
        _dif, _dea, _macd = MACD(
            stock, check_date=check_date, SHORT=fastperiod, LONG=slowperiod, MID=signalperiod)
        dif.append(_dif[stock])
        dea.append(_dea[stock])
        macd.append(_macd[stock])

        return dif, dea, macd

    @staticmethod
    def market_safety_macd(context, index_stock, fastperiod=12, slowperiod=26, enable_minute_data=False):
        """根据MACD及WMA均线判断大盘安全度
            usage: if not market_safety_macd('000300.XSHG'): return
        """
        current_dt = context.current_dt
        rows = slowperiod * 3
        grid = attribute_history(
            security=index_stock, count=rows, unit='1d', fields=['close'])
        if enable_minute_data:
            # last_prices = history(1, unit='1m', field='close', security_list=[index])
            # df.loc[current_dt, 'close'] = last_prices[stock][0]
            price_now = get_price(
                index_stock, count=1, end_date=current_dt, frequency='1m')
            grid.loc[current_dt, 'close'] = price_now['close'].values[0]

        _close = grid['close']  # type: np.ndarray
        signalperiod = (fastperiod + slowperiod) / 7
        _dif, _dea, _macd = talib.MACD(
            _close, fastperiod, slowperiod, signalperiod)

        n = int((fastperiod + slowperiod + signalperiod) / 2)
        # MA_Type: 0=SMA, 1=EMA, 2=WMA, 3=DEMA, 4=TEMA, 5=TRIMA, 6=KAMA, 7=MAMA, 8=T3 (Default=SMA)
        wma20 = talib.MA(_close, n, matype=2)

        # 是否 MACD > 0 且收盘价站在 WMA 均线之上
        flag = _macd[-1] > 0 and _close[-1] > wma20[-1]
        # 最新 MACD 值
        value = _macd[-1]

        log.debug('[%s 趋势] 是否 macd %.2f>0 且收盘价 %.2f 站在 %d 日WMA均线 %.2f 之上：%s'
                  % (index_stock, _macd[-1], _close[-1], n, wma20[-1], flag))

        return flag, value

    @staticmethod
    def get_market_temperature(context, stock_list=list(), up_rate_min=0.025, down_rate_min=0.025):
        """获取当前市场温度指数
        """
        # 算法：根据涨跌比例以及涨跌停比例两个因子进行分析
        # 返回值范围：-60000 ~ 60000，对应全涨停和全跌停；所有涨跌幅低于3%则为0；
        #       如涨跌为5%，涨跌停为1%，则为1000；如涨跌为-2%，涨跌停为-0.04%，则为-400；
        #       总而言之，指数在：
        #           <-500：走势很差，短线风险大，建议空仓；
        #           -500~-100：走势较差，空仓或轻仓为宜；
        #           -100~100：震荡市场，轻仓为宜；
        #           100~500：小幅上涨，轻仓为宜，适量重仓；
        #           500~1000：市场活跃，可以重仓；
        #           >1000：重仓为宜。
        # 参考妖股助手：共3409只，上涨225只，下跌42只，涨停8只，跌停1只 = 温度646
        
        q = query(valuation.code)
        if len(stock_list) > 0:
            q = q.filter(
                valuation.code.in_(stock_list)
            )
        stocks = list(get_fundamentals(q)['code'])

        current_dt = context.current_dt
        
        # 获取近n天价格数据
        #yesterday = current_dt - dt.timedelta(days=1)  # 该写法有bug
        yesterday = context.previous_date
        df_hData = get_price(
            stocks, count=5, end_date=yesterday.replace(hour=23), frequency='1d')

        # 加上当天数据
        # 注意！！对于分钟级策略，frequency 不能设置为 1d，将会导致回测时用到未来数据。
        df_hData_now = get_price(
            stocks, count=1, end_date=current_dt, frequency='1m')
        # df_hData_now.dropna(how='all', inplace=True)
        df_hData = pd.concat([df_hData, df_hData_now], axis=1)
        # log.info('--> df_hData[close]: %s' % df_hData['close'])

        # 计算涨跌幅
        # 用shift方法错位
        # df_hData['pct_change'] = ((df_hData['close'] - df_hData['close'].shift(1)) / df_hData['close'])
        # 或用pct_Change函数
        df_hData['pct_change'] = df_hData['close'].pct_change()

        d_today = current_dt.strftime("%Y-%m-%d %H:%M:%S")
        df_pct_change = df_hData['pct_change'].stack(level=0)[d_today]

        cnt_total = len(df_pct_change)
        cnt_up = len(df_pct_change[df_pct_change >= up_rate_min])
        cnt_down = len(df_pct_change[df_pct_change <= -down_rate_min])
        cnt_up_top = len(df_pct_change[df_pct_change >= 0.099])
        cnt_down_top = len(df_pct_change[df_pct_change <= -0.099])

        # 计算涨跌因子
        pr_1 = (cnt_up - cnt_down) / float(cnt_total) if cnt_total > 0 else 0

        # 计算涨跌停因子
        pr_2_1 = cnt_up_top / float(cnt_total) if cnt_up > 0 else 0
        pr_2_2 = cnt_down_top / float(cnt_total) if cnt_down > 0 else 0
        pr_2 = (pr_2_1 - pr_2_2) * 5.0  # 5.0为涨跌停权重因子

        val = (pr_1 + pr_2) * 10000  # 市场温度指数

        log.info('*** 市场温度指数：%d（涨跌因子：%f, 涨跌停因子：%f）；股票总量：%d, 上涨：%d, 涨停：%d, 下跌：%d, 跌停：%d' %
                 (val, pr_1, pr_2, cnt_total, cnt_up, cnt_up_top, cnt_down, cnt_down_top))

        return int(val)

    @staticmethod
    def get_vol_rate(context, stock, backDays=5):
        """计算相对成交量指标
        """
        # 函数名称：量比计算（https://baike.baidu.com/item/%E9%87%8F%E6%AF%94/1847747?fr=aladdin）
        # 函数解释：计算相对成交量的指标。它是指股市开市后平均每分钟的成交量与过去5个交易日平均每分钟成交量之比。
        # 计算公式：量比 =（现成交总手数 / 现累计开市时间(分) ）/ 过去5日平均每分钟成交量。
        
        current_dt = context.current_dt
        cdt = current_dt

        tStart = dt.timedelta(hours=9, minutes=30)
        tNow = dt.timedelta(hours=cdt.hour, minutes=cdt.minute)

        duration = tNow - tStart
        durationMinutes = duration.seconds / 60

        if cdt.hour > 11:
            durationMinutes -= 90

        avgVolToday = 0
        if durationMinutes > 0:
            hData = attribute_history(stock, durationMinutes, unit='1m', fields=(
                'volume'), skip_paused=True, df=False)
            volumeToday = np.array(hData['volume'], dtype='f8')
            avgVolToday = sum(volumeToday) / durationMinutes

        hData = attribute_history(stock, backDays, unit='1d', fields=(
            'volume'), skip_paused=True, df=False)
        volumeHistory = np.array(hData['volume'], dtype='f8')
        avgVolHistory = sum(volumeHistory) / (backDays * 4 * 60)

        volRate = avgVolToday / avgVolHistory

        return volRate

    @staticmethod
    def get_vol_rate_today(context, stocks, last_part_minutes=10):
        """当天量比计算（自定义函数）
        """
        # 函数名称：当天量比计算（自定义函数）
        # 函数解释：分钟级调用，计算到当日当时为止，后面与前面两部分成交量之比
        
        current_dt = context.current_dt
        cdt = current_dt

        tStart = dt.timedelta(hours=9, minutes=30)
        tNow = dt.timedelta(hours=cdt.hour, minutes=cdt.minute)

        duration = tNow - tStart
        durationMinutes = duration.seconds / 60

        if cdt.hour > 11:
            durationMinutes -= 90

        volRate = None
        if durationMinutes >= last_part_minutes * 2:  # 保证前半部时间大于后半部
            hData = history(count=durationMinutes, unit='1m',
                            security_list=stocks, field='volume', skip_paused=True, df=True)
            len_first_part = durationMinutes - last_part_minutes
            # log.debug('--> len_first_part: %d' % len_first_part)
            volumeFirstPart = hData[0:len_first_part]
            volumeLastPart = hData[len_first_part:]
            # volumeFirstPart.dropna(how='any', inplace=True)
            # volumeLastPart.dropna(how='any', inplace=True)
            # log.debug('--> volumeFirstPart: %s' % volumeFirstPart[-5:])
            # log.debug('--> volumeLastPart: %s' % volumeLastPart[-5:])
            volRate = volumeLastPart.sum() / volumeFirstPart.sum()
        else:
            return None

        return volRate

    @staticmethod
    def get_kdj_cn(context, stock, N1=9, N2=3, N3=3, fq='1d'):
        """计算 KDJ 指标
        当K值由较小逐渐大于D值，在图形上显示K线从下方上穿D线，所以在图形上K线向上突破D线时，
        俗称金叉，即为买进的讯号。
        """
        '''
        计算 KDJ 指标（来自聚宽社区，已验证与同花顺一致。https://www.joinquant.com/post/1903?tag=new）
        用法：
        urrent_dt = dt.strptime('2017-11-28 00:00:00', "%Y-%m-%d %H:%M:%S")
        k,d,j = get_kdj_cn(context, '000001.XSHE', N1=9, N2=3, N3=3, fq='5d')  # 1d：日线，5d：周线
        print('k:%.2f, d:%.2f, j:%.2f,' % (k[-1],d[-1],j[-1]))
        输出：k:79.8942, d:75.2031, j:89.2763

        :param context: joinquant context
        :param stock:
        :param N1:
        :param N2:
        :param N3:
        :param fq:
        :return:
        '''

        current_dt = context.current_dt

        # 同花顺和通达信等软件中的SMA
        def SMA_CN(close, timeperiod):
            close = np.nan_to_num(close)
            return reduce(lambda x, y: ((timeperiod - 1) * x + y) / timeperiod, close)

        # 同花顺和通达信等软件中的KDJ
        def KDJ_CN(high, low, close, fastk_period, slowk_period, fastd_period):

            try:
                kValue, dValue = talib.STOCHF(
                    high, low, close, fastk_period, fastd_period=1, fastd_matype=0)
            except Exception as e:
                # formatted_lines = traceback.format_exc().splitlines()
                # log.warn(formatted_lines[-1])
                return [100.0, 100.0, 100.0], [100.0, 100.0, 100.0], [100.0, 100.0, 100.0]

            kValue = np.array(map(lambda x: SMA_CN(
                kValue[:x], slowk_period), range(1, len(kValue) + 1)))
            dValue = np.array(map(lambda x: SMA_CN(
                kValue[:x], fastd_period), range(1, len(kValue) + 1)))
            jValue = 3 * kValue - 2 * dValue

            def func(arr): return np.array(
                [0 if x < 0 else (100 if x > 100 else x) for x in arr])

            kValue = func(kValue)
            dValue = func(dValue)
            jValue = func(jValue)
            return kValue, dValue, jValue

        hData = attribute_history(stock, 30, unit=fq, fields=(
            'close', 'volume', 'open', 'high', 'low'), skip_paused=True, df=False)
        volume = hData['volume']
        volume = np.array(volume, dtype='f8')
        close = hData['close']
        open = hData['open']
        high = hData['high']
        low = hData['low']

        kValue, dValue, jValue = KDJ_CN(high, low, close, 9, 3, 3)

        return kValue, dValue, jValue

    @staticmethod
    def get_kdj(context, stock, N1=9, N2=3, N3=3):
        """计算 KDJ 指标（来自聚宽社区，据说比 KDJ_CN 更正确）
        是否可靠待验证。https://www.joinquant.com/post/7348?tag=algorithm）
        """

        current_dt = context.current_dt

        def SMA_bookaa(d, N):
            v = pd.Series(index=d.index)
            last = np.nan
            for key in d.index:
                x = d[key]
                if last == last:
                    x1 = (x + (N - 1) * last) / N
                else:
                    x1 = x
                last = x1
                v[key] = x1
                if x1 != x1:
                    last = x
            return v

        def zhibiao_kdj(data, N1=9, N2=3, N3=3):
            low1 = pd.rolling_min(data.low, N1)
            high1 = pd.rolling_max(data.high, N1)
            rsv = (data.close - low1) / (high1 - low1) * 100
            k = SMA_bookaa(rsv, N2)
            d = SMA_bookaa(k, N3)
            j = k * 3 - d * 2
            return k, d, j

        #yesterday = current_dt - dt.timedelta(days=1)
        yesterday = context.previous_date
        data = get_price(stock, count=135, end_date=yesterday.replace(
            hour=23), frequency='1d')

        k, d, j = zhibiao_kdj(data, 9, 3, 3)
        data['k'] = k
        data['d'] = d
        data['j'] = j
        # data[['k', 'd', 'j']]

        return data[['k', 'd', 'j']]

    @staticmethod
    def get_sar(context, stock, count=20):
        """计算 SAR 指标（来自聚宽社区，是否可靠待验证）
        注：count 取值默认为20天，count 不同时计算出的 SAR 值也会略有差异
        
        当股票股价从SAR曲线下方开始向上突破SAR曲线时，为买入信号
        当股票股价向上突破SAR曲线后继续向上运动而SAR曲线也同时向上运动时，持股待涨
        当股票股价从SAR曲线上方开始向下突破SAR曲线时，为卖出信号
        """

        '''
        来自聚宽社区 https://www.joinquant.com/post/4106?tag=algorithm）
        
        :param current_dt:
        :param stock:
        :param count:
        :return:
        '''
        current_dt = context.current_dt
        #yesterday = current_dt - dt.timedelta(days=1)
        yesterday = context.previous_date
        df = get_price(stock, end_date=yesterday.replace(hour=23), frequency='daily',
                       fields=('open', 'high', 'low', 'close'), count=count)
        sar = talib.SAR(df.high.values, df.low.values,
                        acceleration=0.02, maximum=0.2)
        # print(sar[-8:])
        return sar[-8:]

    @staticmethod
    def get_rsi(context, stock, days_slow, days_fast):
        """计算指定股票的RSI风险指标（包括当天当时分钟数据）
        """

        current_dt = context.current_dt

        # 获取近n天价格数据
        #yesterday = current_dt - dt.timedelta(days=1)
        yesterday = context.previous_date
        df_hData = get_price(stock, count=days_slow + days_fast + 5, end_date=yesterday.replace(hour=23),
                             frequency='1d')

        # 加上当天数据
        # 注意！！对于分钟级策略，frequency 不能设置为 1d，将会导致回测时用到未来数据。
        # 这里的 current_dt 精确到分钟，所以取到的是当时的分钟数据
        df_hData_now = get_price(
            stock, count=1, end_date=current_dt, frequency='1m')
        # df_hData_now.dropna(how='all', inplace=True)
        # df_hData_now = df_hData_now.iloc[-1]
        df_hData.loc[current_dt, 'close'] = df_hData_now['close'].values[0]
        # log.info('--> df_hData[close]: %s' % df_hData['close'])

        closep = df_hData['close'].values

        RSI_F = talib.RSI(closep, timeperiod=days_fast)
        RSI_S = talib.RSI(closep, timeperiod=days_slow)
        isFgtS = RSI_F > RSI_S
        isFstS = RSI_F < RSI_S

        rsiS = RSI_S[-1]
        rsiF = RSI_F[-1]

        '''
        RSI指标运行范围在0-100之内，实际上RSI很少会运行到90以上，即便当前市场最强势的股票也
        不会运行到90到100之间。RSI指标也很少运行到20以下，更不要说运行到0了。一般股票运行在20到80之间。
        当处于熊市时，从20运行到50左右，即开始下跌；处于牛市当中，RSI下降到40-50之间即开始新的一波上涨，
        RSI运行到80附近，开始一波新的调整。

        牛市下：
        慢速RSI 在55以上时，单边上涨市场，快速RSI上穿慢速RSI即可建仓
        慢速RSI 在55以下时，调整震荡市场，谨慎入市，取连续N天快速RSI大于慢速RSI建仓
        慢速RSI 在60以上时，牛市，无需减仓操作持仓即可
        '''
        bsFlag = None

        if rsiS >= 70:
            bsFlag = "high"  # 高位
        elif rsiS >= 35:
            if isFgtS[-1] and isFgtS[-3]:
                bsFlag = "up"  # 上行
            elif isFstS[-1] and isFstS[-3]:
                bsFlag = "down"  # 下行
            else:
                bsFlag = "sideway"  # 盘整
        elif rsiS >= 25:
            bsFlag = "low"  # 低位
        else:
            if isFgtS[-1] and isFgtS[-3]:
                bsFlag = "verylow"  # 超低位 且上扬
            else:
                bsFlag = "superlow"  # 超低位 且可能下跌

        # log.info('--> get_rsi() bsFlag: %s, rsiS: %d' % (bsFlag, rsiS))
        return bsFlag, rsiS

    @staticmethod
    def get_ar(stock, count):
        """计算指定股票的AR活跃度指标
        """

        df_ar = attribute_history(
            stock, count - 1, '1d', fields=('open', 'high', 'low'), skip_paused=True)
        df_ar_today = get_price(stock, start_date=g.d_today, end_date=g.d_today, frequency='daily',
                                fields=['open', 'high', 'low'])

        df_ar = df_ar.append(df_ar_today)

        ar = sum(df_ar['high'] - df_ar['open']) / \
            sum(df_ar['open'] - df_ar['low']) * 100

        '''
        AR指标 在180以上时，股市极高活跃 
        AR指标 在120 - 180时，股市高活跃
        AR指标 在70 - 120时，股市盘整
        AR指标 在60 - 70以上时，股市走低
        AR指标 在60以下时，股市极弱
        '''
        brFlag = 1

        if ar > 180:
            brFlag = 5
        elif ar > 120 and ar <= 180:
            brFlag = 4
        elif ar > 70 and ar <= 120:
            brFlag = 3
        elif ar > 60 and ar <= 70:
            brFlag = 2
        else:
            brFlag = 1

        g.AR_T = ar

        return brFlag

    @staticmethod
    def get_finance_report_8q(nowDate, stocklist):
        """获取过去8个季度财报
        """

        q = query(income.code, indicator.statDate
                  ).filter(income.code.in_(stocklist))
        df_tmp = get_fundamentals(q, nowDate)
        statDate = df_tmp['statDate'].max()
        year = statDate[:4]
        month = statDate[5:7]
        quarter = int(int(month) / 3)
        quarters = []
        # 当年的季度
        for i in range(quarter, 0, -1):
            quarters.append(year + 'q' + str(i))
        # 去年的季度
        for i in range(4, 0, -1):
            quarters.append(str(int(year) - 1) + 'q' + str(i))
        # 前年的季度
        for i in range(4, quarter, -1):
            quarters.append(str(int(year) - 2) + 'q' + str(i))
        return quarters

    @staticmethod
    def get_price_change(context, enable_minute_data=False, stock_list=list(), days_count=15):
        """计算最近时期股价涨跌幅
        """

        current_dt = context.current_dt

        if len(stock_list) == 0:
            return None

        # 获取全部股票近5个交易日价格数据
        q = query(valuation.code)
        q = q.filter(
            valuation.code.in_(stock_list)
        )
        stocks = list(get_fundamentals(q)['code'])
        df_hData = get_price(stocks, count=days_count + 1,
                             end_date=(current_dt - timedelta(days=1)).replace(hour=23), frequency='1d')

        if enable_minute_data:
            # 加上当天当时数据
            df_hData_now = get_price(
                stocks, count=1, end_date=current_dt, frequency='1m')
            # df_hData_now.dropna(how='all', inplace=True)
            df_hData = pd.concat([df_hData, df_hData_now], axis=1)
            # log.info('--> df_hData[close]: %s' % df_hData['close'])

        # 计算涨跌幅
        # 用shift方法错位
        # df_hData['pct_change'] = ((df_hData['close'] - df_hData['close'].shift(1)) / df_hData['close'])
        # 或用pct_Change函数
        df_hData['pct_change'] = df_hData['close'].pct_change()
        # print(df_hData['pct_change'])
        # 取不包括第一天的涨幅数据并转置
        df_pct_change = df_hData['pct_change'].iloc[1:].T
        # log.debug(df_pct_change)

        return df_pct_change

    @staticmethod
    def get_rising_stocks(context, enable_minute_data=False, stock_list=list(), days_count=15,
                          last_day_range=(0.01, 0.10),
                          filter_first_ndays=False, first_ndays_range=(0.01, 0.09),
                          sort_day_index=-2, sort_type='asc'):
        """根据近几天数据获取涨停的股票列表
        """
        # 参数1；context t- 聚宽上下文
        # 参数2；stock_list
        # 参数3；days_count - 计算多少天的数据；
        # 参数4；last_day_min - 计算多少天的数据；
        # 参数5；filter_first_ndays - 是否对前几天涨幅进行过滤，默认为 False
        # 参数6：first_ndays_range - 前几天涨幅范围，默认为 (0.01, 0.09)
        # 参数7：sort_day_index - 根据某日数据对结果进行排序，取值为 -days_count ~ -2
        # 参数8：sort_type - 排序方向，acs 或者 dsc
        
        current_dt = context.current_dt
        df_pct_change = FuncLib.get_price_change(
            current_dt, enable_minute_data, stock_list, days_count)

        if df_pct_change is None:
            return []

        # 根据最后一天条件过滤股票
        days = df_pct_change.columns
        df_result = df_pct_change[df_pct_change[days[-1]] >= last_day_range[0]]
        df_result = df_result[df_result[days[-1]] <= last_day_range[1]]
        # print(df_result)

        if filter_first_ndays:
            # 根据前几天条件过滤股票
            for i in days[:len(days) - 1]:
                df_result = df_result[df_result[i] >= first_ndays_range[0]]
                df_result = df_result[df_result[i] <= first_ndays_range[1]]

        # 根据指定某天的涨幅做排序
        is_asc = True if sort_type == 'asc' else False
        df_result = df_result.sort_values(
            by=days[sort_day_index], ascending=is_asc)
        list_result = list(df_result.index)

        # log.debug('满足条件的股票数据：\n%s' % df_result)
        # log.debug('满足条件的股票列表：%s' % list_result)

        return list_result

    @staticmethod
    def get_volume_change(context, stock_list=list(), days_count=15):
        """计算最近时期成交量涨跌幅
        """

        current_dt = context.current_dt

        if len(stock_list) == 0:
            return None

        # 获取全部股票最近交易日数据
        q = query(valuation.code).filter(
            valuation.code.in_(stock_list)
        )
        stocks = list(get_fundamentals(q)['code'])
        df_hData = get_price(stocks, count=days_count + 1,
                             end_date=(current_dt - timedelta(days=1)).replace(hour=23), frequency='1d')

        # 计算涨跌幅
        df_hData['pct_change'] = df_hData['volume'].pct_change()
        df_pct_change = df_hData['pct_change'].iloc[1:].T
        # log.debug(df_pct_change)

        return df_pct_change

    @staticmethod
    def get_stocks_by_fluctuate_sort(stock_list, days=60, sort_type='asc'):
        """按波动率排序股票
        """

        from math import sqrt
        # 计算n天波动率
        dic_vol = dict.fromkeys(stock_list, 0)
        for stock in stock_list:
            price = attribute_history(
                stock, days, '1d', ('close'))  # 取60日收盘价并计算波动率
            vol = sqrt(price.var()) / price.mean()
            dic_vol[stock] = vol

        is_dsc = True if sort_type == 'dsc' else False
        sort_vol = sorted(dic_vol.items(), key=(
            lambda d: d[1]['close']), reverse=is_dsc)  # 按波动率排序
        # log.debug('--> 波动率排序结果：%s' % sort_vol)

        stockpool = []
        for (m, n) in sort_vol:
            stockpool.append(m)

        return stockpool

    @staticmethod
    def get_stocks_by_finance_data(stock_list):
        """根据基本面选股

        为防止亏损公司或财务指标异常公司的影响。
        剔除资产报酬率、毛利率、权益报酬率等异常的公司。
        即要求 ROA>0.5%、 adj-ROE>2%、利润总额>0、毛利率>0。
        选取投资收益率 > 0；
        根据PB升序排列，取前30%；
        根据三费占比升序排列，取前30%；
        根据收入现金率降序排列，取前30%；
        """

        ## 获取财务数据
        df = get_fundamentals(query(
            valuation.code,  # 股票代码
            valuation.pb_ratio,  # 市净率
            income.total_profit,  # 利润总额
            income.total_operating_revenue,  # 营业总收入
            income.total_operating_cost,  # 营业总成本
            income.sale_expense,  # 销售费用
            income.administration_expense,  # 管理费用
            income.financial_expense,  # 财务费用
            income.operating_revenue,  # 主营业务收入
            balance.total_assets,  # 资产总计
            balance.total_liability,  # 负债合计
            balance.account_receivable,  # 应收账款
            balance.accounts_payable,  # 应付账款
            income.investment_income,  # 投资收益
            cash_flow.goods_sale_and_service_render_cash,  # 销售商品、提供劳务收到的现金(
        ).filter(income.code.in_(stock_list)
                 ).filter(
            # ROA>0.5% (资产报酬率=利润总额/总资产)
            income.total_profit / balance.total_assets > 0.005,
            # adj-ROE>2% (调整的净资产报酬率=利润总额/净资产)
            income.total_profit / \
                (balance.total_assets - balance.total_liability) > 0.02,
            # 利润总额>0
            income.total_profit > 0,
            # 毛利率>0 (（主营业务收入-主营业务成本） / 主营业务收入)
            (income.total_operating_revenue - income.total_operating_cost) / \
            income.total_operating_revenue > 0,
        )).dropna()  # 去除NaN值

        ## 指标计算
        # 三费占比=（销售费用＋管理费用＋财务费用） /主营业务收入
        df['3'] = (df.sale_expense + df.administration_expense +
                   df.financial_expense) / df.operating_revenue
        # 投资收益率 = 投资收益/总资产
        df['tzsy'] = df.investment_income / df.total_assets
        # 收入现金率 = 销售商品提供劳务收到的现金/主营业务收入
        df['srxj'] = df.goods_sale_and_service_render_cash / df.operating_revenue
        #     # 应收账款占比=应收账款/主营业务收入
        # df['ys'] = df.account_receivable / df.operating_revenue
        #     # 应付账款占比=应付账款/主营业务收入
        # df['yf'] = df.accounts_payable / df.operating_revenue

        ## 股票筛选
        # 投资收益率 > 0
        df = df[df.tzsy > 0]
        # 根据PB升序排列，取前30%
        df = df.sort_index(by=['pb_ratio'])
        code1 = list(df.code.head(int(len(df) * 0.3)))
        # 根据三费占比升序排列，取前30%
        df2 = df[df.code.isin(code1)]
        df2 = df2.sort_index(by=['3'])
        code2 = list(df2.code.head(int(len(df2) * 0.3)))
        print(len(code2))
        # 根据收入现金率降序排列，取前30%
        df3 = df2[df2.code.isin(code2)]
        df3 = df3.sort_index(by=['srxj'], ascending=False)
        code3 = list(df3.code.head(int(len(df3) * 0.3)))
        print(len(code3))

        return code3

    @staticmethod
    def queryDivPercent(stockCode, year, date, use_gta_data=False):
        """查询分红比例
        """

        gtaStockCode = stockCode[0:6]
        divDf = None

        if use_gta_data:
            # 国泰安数据将下架，使用聚源数据替代：https://www.joinquant.com/post/12996?tag=algorithm
            from jqdata import gta  # 国泰安数据

            # 国泰君安数据
            divQ = query(
                gta.STK_DIVIDEND.DIVDENDYEAR,  # 分红年份
                gta.STK_DIVIDEND.SYMBOL,  # 股票代码
                gta.STK_DIVIDEND.DIVIDENTBT,  # 股票分红
                gta.STK_DIVIDEND.DECLAREDATE,  # 分红消息的时间
                gta.STK_DIVIDEND.DISTRIBUTIONBASESHARES,  # 分红时的股本基数
                gta.STK_DIVIDEND.TERMCODE
            ).filter(
                gta.STK_DIVIDEND.DECLAREDATE < date,
                gta.STK_DIVIDEND.DIVDENDYEAR == year,
                gta.STK_DIVIDEND.SYMBOL == gtaStockCode)
            divDf = gta.run_query(divQ)

        else:
            # 聚源数据
            """
            证券类别(SecuCategory)与(CT_SystemConst)表中的DM字段关联，令LB = 1177，得到证券类别的具体描述：1-A股，2-B股，3-H股，4-大盘，5-国债回购，
            6-国债现货，7-金融债券，8-开放式基金，9-可转换债券，10-其他，11-企业债券，12-企业债券回购，13-投资基金，14-央行票据，15-深市代理沪市股票，
            16-沪市代理深市股票，17-资产支持证券，18-资产证券化产品，19-买断式回购，20-衍生权证，21-股本权证，22-股指期货，23-商业银行定期存款，
            24-其他股票，25-牛熊证，26-收益增长线，27-新质押式回购，28-地方政府债，29-可交换公司债，30-拆借，31-信用风险缓释凭证，
            32-浮息债计息基准利率，33-定期存款凭证，34-个股期权，35-大额存款凭证，36-债券借贷，51-港股，52-合订证券，53-红筹股，55-优先股，
            60-基金，61-信托基金，62-ETF基金，63-参与证书，64-杠杆及反向产品，65-债务证券，66-基金票据，69-美国证券(交易试验计划)，71-普通预托证券，
            72-优先预托证券，73-股票，74-普通股，75-美国存托股票（ADS），76-国债期货。
            """
            secu_category = [1]  # 证券类别:1-A股，SecuMain表中的字段
            secu_list = []
            secu_list.append(gtaStockCode)

            from jqdata import jy  # 聚源数据

            incode = jy.run_query(query(
                jy.SecuMain.SecuCode,
                jy.SecuMain.InnerCode,
            ).filter(
                jy.SecuMain.SecuCode.in_(secu_list),
                jy.SecuMain.SecuCategory.in_(secu_category),
            ))

            q = query(
                jy.LC_Dividend.InnerCode,  # 证券内部编码
                # jy.LC_Dividend.EPS, # 每股收益(元)
                jy.LC_Dividend.CashDiviRMB,  # 实派(税后/人民币元)
                jy.LC_Dividend.AdvanceDate,
                # jy.LC_Dividend.BonusShareRatio, # 送股比例(10送X)
                # jy.LC_Dividend.TranAddShareRaio, # 转增股比例(10转增X)
                jy.LC_Dividend.DiviBase,  # 分红股本基数(股)
            ).filter(
                jy.LC_Dividend.AdvanceDate >= '{}-01-01'.format(year),  # 预案公布日
                jy.LC_Dividend.AdvanceDate <= '{}-12-31'.format(year),
                jy.LC_Dividend.DividendImplementDate < date,  # 分红实施公告日
                jy.LC_Dividend.IfDividend == '1',
                jy.LC_Dividend.InnerCode.in_(incode['InnerCode'])  # 股票代码
            ).order_by(jy.LC_Dividend.ExDiviDate.desc())  # 除权除息日

            df = jy.run_query(q).fillna(value=0, method=None, axis=0)
            divDf = pd.merge(incode, df, on='InnerCode')

        return divDf

    @staticmethod
    def getDivPercent(stockCode, date, use_gta_data=False):
        """得到股票年度分红相对资产比例
        """

        year = date.year - 1
        divDf = FuncLib.queryDivPercent(stockCode, year, date)

        if use_gta_data:
            hasYeayEndDiv = False
            for termCode in divDf.TERMCODE:
                if termCode == 'P2702':
                    hasYeayEndDiv = True
            if hasYeayEndDiv <= 0:
                year = year - 1
                divDf = FuncLib.queryDivPercent(stockCode, year, date)

            result = 0
            for index, row in divDf.iterrows():
                if row.DIVIDENTBT != row.DIVIDENTBT:
                    continue
                metailQ = query(valuation.code, balance.equities_parent_company_owners, valuation.capitalization
                                ).filter(valuation.code == stockCode)
                metailDf = get_fundamentals(metailQ, row.DECLAREDATE)
                if len(metailDf) > 0:
                    shareB = metailDf.equities_parent_company_owners[0] / \
                        metailDf.capitalization[0] / 10000
                    divP = float(row.DIVIDENTBT) / shareB / 10
                    result += divP

        else:
            result = 0
            for index, row in divDf.iterrows():
                if row.CashDiviRMB != row.CashDiviRMB:
                    continue
                metailQ = query(valuation.code, balance.equities_parent_company_owners, valuation.capitalization
                                ).filter(valuation.code == stockCode)
                metailDf = get_fundamentals(metailQ, row.AdvanceDate)
                if len(metailDf) > 0:
                    shareB = metailDf.equities_parent_company_owners[0] / \
                        metailDf.capitalization[0] / 10000
                    divP = float(row.CashDiviRMB) / shareB / 10
                    result += divP

        return stockCode, year, result

    @staticmethod
    def get_stock_pairs(stock_list, start_date="2016-03-01", end_date="2017-09-01"):
        """
        计算股票的协整关系

        在 Python 的 Statsmodels 包中，有直接用于协整关系检验的函数 coint，该函数包含于 statsmodels.tsa.stattools 中。
        首先构造一个读取股票价格，判断协整关系的函数。该函数返回的两个值分别为协整性检验的 p 值矩阵以及所有传入的参数中协整性较强的股票对。
        不需要在意 p 值具体是什么，可以这么理解它：p 值越低，协整关系就越强；p 值低于 0.05 时，协整关系便非常强。

        使用示例：
            # 使用上面所得的 A* 级基金分析协整度
            #stock_list = ['169102.XSHE', '150330.XSHE', '169101.XSHE', '150057.XSHE', '164906.XSHE', '159916.XSHE']
            # ETF
            stock_list = ['159928.XSHE', '159916.XSHE', '512210.XSHG', '512600.XSHG', '510630.XSHG', '159920.XSHE', '159905.XSHE', '159946.XSHE', '513100.XSHG', '159941.XSHE', '510360.XSHG', '513030.XSHG']
            # 电器
            #stock_list = ['002508.XSHE', '000333.XSHE', '000651.XSHE', '000418.XSHE', '002032.XSHE', '002705.XSHE']
            pairs = get_stock_pairs(stock_list, start_date="2016-03-01", end_date="2017-09-01")
            print(pairs)
        """
        import statsmodels.api as sm
        import seaborn as sns

        # 输入是一DataFrame，每一列是一支股票在每一日的价格
        def find_cointegrated_pairs(dataframe):
            # 得到DataFrame长度
            n = dataframe.shape[1]
            # 初始化p值矩阵
            pvalue_matrix = np.ones((n, n))
            # 抽取列的名称
            keys = dataframe.keys()
            # 初始化强协整组
            pairs = []
            # 对于每一个i
            for i in range(n):
                # 对于大于i的j
                for j in range(i + 1, n):
                    # 获取相应的两只股票的价格Series
                    stock1 = dataframe[keys[i]]
                    stock2 = dataframe[keys[j]]
                    # 分析它们的协整关系
                    result = sm.tsa.stattools.coint(stock1, stock2)
                    # 取出并记录p值
                    pvalue = result[1]
                    pvalue_matrix[i, j] = pvalue
                    # 如果p值小于0.05
                    if pvalue < 0.05:
                        # 记录股票对和相应的p值
                        pairs.append((keys[i], keys[j], pvalue))
            # 返回结果
            return pvalue_matrix, pairs

        prices_df = get_price(stock_list, start_date,
                              end_date, frequency="daily", fields=["close"])
        pvalues, pairs = find_cointegrated_pairs(prices_df["close"])
        sns.heatmap(1 - pvalues, xticklabels=stock_list,
                    yticklabels=stock_list, cmap='RdYlGn_r', mask=(pvalues == 1))
        # print(pairs)

        return pairs

    @staticmethod
    def filter_special(context, stock_list, filter_high_low_limit=False):
        """
        过滤器，过滤停牌、ST、科创、新股等
        """
        curr_data = get_current_data()

        stock_list = [stock for stock in stock_list if stock[0:3] != '688']  # 过滤科创板'688'
        stock_list = [stock for stock in stock_list if not curr_data[stock].paused]
        stock_list = [stock for stock in stock_list if not curr_data[stock].is_st]
        stock_list = [stock for stock in stock_list if not curr_data[stock].name.startswith('ST')]
        stock_list = [stock for stock in stock_list if not curr_data[stock].name.startswith('*ST')]
        stock_list = [stock for stock in stock_list if '退' not in curr_data[stock].name]
        #stock_list = [stock for stock in stock_list if curr_data[stock].day_open > 1]
        #stock_list = [stock for stock in stock_list if (context.current_dt.date() - get_security_info(stock).start_date).days > 150]
        
        if filter_high_low_limit:
            stock_list = [stock for stock in stock_list if curr_data[stock].day_open != curr_data[stock].low_limit]  # 过滤跌停
            stock_list = [stock for stock in stock_list if curr_data[stock].day_open != curr_data[stock].high_limit]  # 过滤涨停
        
        return stock_list

#######################################################################
#   以下为 2020-12-13 后为新策略开发所添加或修改的代码
#######################################################################

### 框架类 ###
# 系统框架所需各种功能类

class GroupRules(Rule):
    """规则组合器 - 通过此类或此类的子类，来规整集合其它规则。可嵌套，实现规则树，实现多策略组合。"""
    
    rules = []
    # 规则配置list下标描述变量。提高可读性与未来添加更多规则配置。
    cs_enabled, cs_name, cs_memo, cs_class_type, cs_param = range(5)

    def __init__(self, params):

        Rule.__init__(self, params)
        self.config = params.get('config', [])
        pass

    def update_params(self, context, params):
        Rule.update_params(self, context, params)
        self.config = params.get('config', self.config)

    def initialize(self, context):
        # 创建规则
        self.rules = self.create_rules(context, self.config)
        #print('--> [%s] self.rules: %s, in initialize' % (self, self.rules))
        for rule in self.rules:
            rule.initialize(context)

    def handle_data(self, context, data):
        for rule in self.rules:
            ret = rule.handle_data(context, data)
            if rule.is_to_return:
                #print('--> check rule 1: %s, self.is_to_return: %s' % (rule, rule.is_to_return))
                self.is_to_return = True
                return ret
        self.is_to_return = False

    def before_trading_start(self, context):
        Rule.before_trading_start(self, context)
        for rule in self.rules:
            rule.before_trading_start(context)

    def after_trading_end(self, context):
        Rule.after_trading_end(self, context)
        for rule in self.rules:
            rule.after_trading_end(context)

    def process_initialize(self, context):
        Rule.process_initialize(self, context)
        for rule in self.rules:
            rule.process_initialize(context)

    def after_code_changed(self, context):
        Rule.after_code_changed(context)
        # 重整所有规则
        # print(self.config)
        self.rules = self.check_chang(context, self.rules, self.config)
        #print('--> [%s] self.rules: %s, in after_code_changed' % (self, self.rules))
        # for rule in self.rules:
        #     rule.after_code_changed(context)
    
    # 检测新旧规则配置之间的变化。
    def check_chang(self, context, rules, config):
        nl = []
        for c in config:
            # 按顺序循环处理新规则
            if not c[self.cs_enabled]:  # 不使用则跳过
                continue
            # print c[self.cs_memo]
            # 查找旧规则是否存在
            find_old = None
            for old_r in rules:
                if old_r.__class__ == c[self.cs_class_type] and old_r.name == c[self.cs_name]:
                    find_old = old_r
                    break
            if find_old is not None:
                # 旧规则存在则添加到新列表中,并调用规则的更新函数，更新参数。
                nl.append(find_old)
                find_old.memo = c[self.cs_memo]
                find_old.log = g.log_type(context, c[self.cs_memo])
                find_old.update_params(context, c[self.cs_param])
                find_old.after_code_changed(context)
            else:
                # 旧规则不存在，则创建并添加
                new_r = self.create_rule(context, 
                    c[self.cs_class_type], c[self.cs_param], c[self.cs_name], c[self.cs_memo])
                nl.append(new_r)
                # 调用初始化时该执行的函数
                new_r.initialize(context)
        return nl

    def on_sell_stock(self, position, order, is_normal, new_pindex=0):
        for rule in self.rules:
            rule.on_sell_stock(position, order, is_normal, new_pindex)

    # 清仓前调用。
    def before_clear_position(self, context, pindexs=[0]):
        for rule in self.rules:
            pindexs = rule.before_clear_position(context, pindexs)
        return pindexs

    def on_buy_stock(self, stock, order, pindex=0):
        for rule in self.rules:
            rule.on_buy_stock(stock, order, pindex)

    def on_clear_position(self, context, pindexs=[0]):
        for rule in self.rules:
            rule.on_clear_position(context, pindexs)

    def before_adjust_start(self, context, data):
        for rule in self.rules:
            rule.before_adjust_start(context, data)

    def after_adjust_end(self, context, data):
        for rule in self.rules:
            rule.after_adjust_end(context, data)

    # 创建一个规则执行器，并初始化一些通用事件
    def create_rule(self, context, class_type, params, name, memo):
        obj = class_type(params)
        # obj.g = self.g
        obj.set_g(self.g)
        obj.name = name
        obj.memo = memo
        obj.log = g.log_type(context, obj.memo)
        # print(g.log_type, obj.memo)
        return obj

    # 根据规则配置创建规则执行器
    def create_rules(self, context, config):
        # config里 0.是否启用，1.描述，2.规则实现类名，3.规则传递参数(dict)]
        return [self.create_rule(context, c[self.cs_class_type], c[self.cs_param], 
                c[self.cs_name], c[self.cs_memo]) for c in config if c[self.cs_enabled]]

    # 显示规则组合，嵌套规则组合递归显示
    def show_strategy(self, level_str=''):
        s = '\n' + level_str + str(self)
        level_str = '    ' + level_str
        for i, r in enumerate(self.rules):
            if isinstance(r, GroupRules):
                s += r.show_strategy('%s%d.' % (level_str, i + 1))
            else:
                s += '\n' + '%s%d. %s' % (level_str, i + 1, str(r))
        return s

    # 通过name查找obj实现
    def get_obj_by_name(self, name):
        if name == self.name:
            return self

        f = None
        for rule in self.rules:
            if isinstance(rule, GroupRules):
                f = rule.get_obj_by_name(name)
                if f != None:
                    return f
            elif rule.name == name:
                return rule
        return f

    def __str__(self):
        return self.memo  # 返回默认的描述

class BuyStocksEqually(Rule):
    """根据指定总待持仓股票只数（buy_count），平分资金购买。
    使用该方式，若实际只数较少，则资金利用率会不足（但对于协整策略，收益反而更好）。
    如果 buy_count 值设为0，则根据实际待持仓只数平分资金买入。对于非协整策略，若买入数量经常不足，可据此提高资金利用率。"""

    def __init__(self, params):
        Rule.__init__(self, params)
        self.buy_count = params.get('buy_count', 0)  # 购股数量
        self.deal_rate = params.get('deal_rate', 1.0)  # 资金使用率
        self.bypass_sold = params.get('bypass_sold', False)  # 不买入当天已卖出过的

    def update_params(self, context, params):
        Rule.update_params(self, context, params)
        self.buy_count = params.get('buy_count', self.buy_count)
        self.deal_rate = params.get('deal_rate', self.deal_rate)
        self.bypass_sold = params.get('bypass_sold', self.bypass_sold)

    def handle_data(self, context, data):
        self.adjust(context, data, self.g.buy_stocks)

    def adjust(self, context, data, buy_stocks):
        # 买入股票，根据股票数量平均分仓
        _total_to_holds = self.buy_count
        if self.buy_count == 0:  # 根据持股数量而不是指定的 buy_count 数平分
            _total_to_holds = len(
                set(context.subportfolios[0].long_positions.keys()) | set(buy_stocks))
            if _total_to_holds == 0:
                return
        #log.info('待持股只数：%d' % _total_to_holds)
        for pindex in self.g.op_pindexs:
            position_count = len(context.subportfolios[pindex].long_positions)
            # 从回测情况看，买入比例并不均衡。以下代码有 BUG？
            #if _total_to_holds > position_count:
            #    value = context.subportfolios[pindex].available_cash / (_total_to_holds - position_count)
            #    value = value * self.deal_rate
            #    for stock in buy_stocks:
            #        if self.bypass_sold and stock in self.g.sell_stocks:
            #            self.log.info('%s 今天已经卖出过，暂不再买入' % stock)
            #            continue
            #        if stock not in context.subportfolios[pindex].long_positions \
            #                or context.subportfolios[pindex].long_positions[stock].total_amount == 0:
            #            if self.g.open_position(context, stock, value, pindex):
            #                if len(context.subportfolios[pindex].long_positions) == _total_to_holds:
            #                    break
            # 改为如下代码：
            target_value = (context.subportfolios[pindex].total_value +
                            context.subportfolios[pindex].locked_cash) / _total_to_holds
            target_value = target_value * self.deal_rate
            for stock in buy_stocks:
                if self.bypass_sold and stock in self.g.sell_stocks:
                    self.log.info('%s 今天已经卖出过，暂不再买入' % stock)
                    continue
                # 检查当前仓位与目标仓位的差距
                if stock in context.subportfolios[pindex].long_positions:
                    current_value = context.subportfolios[pindex].long_positions[stock].value
                    delta = target_value - current_value
                    delta_n = int(delta / data[stock].close)
                    cash_n = int(context.portfolio.cash / data[stock].close)
                    # 由于国内市场限制当日卖出，减仓调整时有时会出现"订单委托失败：……当前可平仓证券数量不足"，可忽略
                    if delta_n < 300 or cash_n < 300:  # 差额或可买股数不足300则不开仓，避免日内价格波动导致的频繁调仓
                        continue
                    else:
                        delta_r = math.fabs(delta / target_value)  # 取绝对值会略提升收益率
                        self.log.debug('[%s]> 目标额￥%2.f，当前额￥%.2f，还差%.0f%% %d股'
                              % (stock, target_value, current_value, delta_r*100, delta_n))
                self.g.open_position(context, stock, target_value, pindex)

    def after_trading_end(self, context):
        self.g.sell_stocks = []

    def __str__(self):
        return '调仓买入，现金平分式买入股票 [ 购股数量: %s，资金使用率: %.0f %% ]' \
               % (str(self.buy_count) if self.buy_count != 0 else '不指定', self.deal_rate * 100)

class AdjustPositionByFundRatio(Rule):
    """根据各股指定资金占比调整仓位。
       依据全局变量 self.g.stocks_adjust_info[i] 中的 group_fund_ratio 和 inner_fund_ratio
    """

    def __init__(self, params):
        Rule.__init__(self, params)

    def update_params(self, context, params):
        Rule.update_params(self, context, params)

    def handle_data(self, context, data):
        self.adjust(context, data, self.g.buy_stocks)

    def adjust(self, context, data, buy_stocks):
        # 卖出不在待买股票列表中的股票
        for pindex in self.g.op_pindexs:
            for stock in context.subportfolios[pindex].long_positions.keys():
                if stock not in buy_stocks:
                    position = context.subportfolios[pindex].long_positions[stock]
                    self.g.close_position(context, position, True, pindex)
        
        # 对于要持有的股票，或根据资金占比买入或调整仓位
        for stock in buy_stocks:
            self.adjust_position(context, stock, pindex, data)

    def adjust_position(self, context, stock, pindex, data):
        """调整股票仓位到指定的资金比率
        TODO 整合动态仓位管理？
        """

        # 计算目标仓位
        target_ratio = self.g.stocks_adjust_info[stock]['group_fund_ratio'] \
            * self.g.stocks_adjust_info[stock]['inner_fund_ratio']
        target_value = (context.subportfolios[pindex].total_value +
            context.subportfolios[pindex].locked_cash) * target_ratio
        
        # 获取当前仓位
        current_value = 0.0
        if stock in context.subportfolios[pindex].long_positions:
            current_value = context.subportfolios[pindex].long_positions[stock].value
        
        try:
            # 检查可买资金
            if target_value > current_value and \
                int(context.portfolio.cash / data[stock].close) < 120:
                return
            
            # 检查仓位差值，不足一手或差值在5%以内，不交易
            delta = target_value - current_value  # 资金差额
            delta_n = int(delta / data[stock].close)  # 股数差额
            if abs(delta_n) < 120 or abs(delta / target_value) < 0.05:
                return
            
            # 如果可平仓数量小于待卖出数量，不交易
            if delta < 0 and context.portfolio.positions[stock].closeable_amount < abs(delta_n):
                return
            
            # 非当日调仓股票且差值在10%以内，也不交易，避免价格波动导致的不必要进出
            the_day = context.current_dt.date().strftime("%Y-%m-%d")
            #print('--> the_day: %s' % the_day)
            #print('--> stocks_adjust_info[%s]: %s' % (stock, self.g.stocks_adjust_info[stock]))
            if self.g.stocks_adjust_info[stock]['adjust_date'] != the_day \
                and abs(delta / target_value) < 0.1:
                #self.log.info('非当日调仓股票，且差值在10%以内，不调仓')
                return
            
            if delta != target_value:  # 过滤掉某些完全无法买入的记录
                self.log.info('[%s]> 目标额￥%2.f，当前额￥%.2f，相差%.0f%% %d股'
                    % (stock, target_value, current_value, delta/target_value*100, delta_n))
            
        except Exception as e:
            self.log.warn('Exception raised: %s. %s, %.2f %.2f' % \
                (e, stock, context.portfolio.cash, data[stock].close))
        
        # 调整仓位到目标金额
        # 由于国内市场限制当日卖出，减仓调整时有时会出现"订单委托失败：……当前可平仓证券数量不足"，可忽略
        self.g.open_position(context, stock, target_value, pindex)  # 执行买或卖操作
        
    def after_trading_end(self, context):
        self.g.sell_stocks = []

    def __str__(self):
        return '根据资金占比调整仓位'

class ChoseStocksSimple(GroupRules):
    """简单选股组合器 - 只根据规则组合返回选股结果，不更新建议持股列表"""

    def __init__(self, params):
        GroupRules.__init__(self, params)
        self.group_fund_ratio = params.get('group_fund_ratio', 1.0)  # 选股组合资金总占比（0~1.0）
        # 多策略选股时若开启“独占选股”，则本选股器有非空结果时，取消后续其他策略的处理
        self.exclusive = params.get('exclusive', False)  # 是否独占选股

        self.stock_list = [] # 最近选中的股票列表
        self.has_run = False

    def update_params(self, context, params):
        GroupRules.update_params(self, context, params)
        self.group_fund_ratio = params.get('group_fund_ratio', self.group_fund_ratio)
        self.exclusive = params.get('exclusive', self.exclusive)

    def handle_data(self, context, data):
        try:
            to_run_one = self._params.get('day_only_run_one', False)
        except:
            to_run_one = False
        if to_run_one and self.has_run:
            #self.log.debug('设置一天只选一次，跳过选股。')
            return self.stock_list
        
        ret = GroupRules.handle_data(self, context, data)
        #print('--> GroupRules.is_to_return:', GroupRules.is_to_return) # False when some rule is_to_return
        #print('--> self.is_to_return: %s' % self.is_to_return) # True when some rule is_to_return
        if self.is_to_return:
            # 若为定时器，其返回值为None，则在此返回之前选股结果
            # 若为择时规则，其返回值为True/False，则在此返回空列表
            if ret is not None:
                self.stock_list = []
            return self.stock_list

        # 如果有查询器，则首先根据财务数据选股（若有多个查询器，只有最后一个生效）
        q = None
        for rule in self.rules:
            if isinstance(rule, FilterQuery):
                q = rule.filter(context, data, q)
        if q:
            self.stock_list = list(get_fundamentals(q)['code']) if q != None else []
        
        # 如果有过滤器，则对股票列表进行过滤
        for rule in self.rules:
            if isinstance(rule, FilterStockList):
                #print('--> 待过滤股票列表 by %s：%s' % (rule, self.stock_list))
                self.stock_list = rule.filter(context, data, self.stock_list)
                if rule.is_to_return:  # 择时清仓
                    return self.stock_list

        self.stock_list = list(self.stock_list)
        #self.stock_list.sort() # 初选结果可能有顺序，故不必排序

        self.has_run = True

        return self.stock_list

    def before_trading_start(self, context):
        #print('--> [%s] self.rules: %s' % (self, self.rules))
        GroupRules.before_trading_start(self, context)
        self.has_run = False

    def __str__(self):
        return '%s [ 组合资金占比: %.3f 是否为独占模式: %s ]' % \
            (self.memo, self.group_fund_ratio, self.exclusive)

class ChoseStocksMulti(GroupRules):
    """多策略选股组合器"""

    def __init__(self, params):
        GroupRules.__init__(self, params)

    def handle_data(self, context, data):
        stock_list = []

        # 依次简单策略选股，合并结果
        for rule in self.rules:
            _ret = rule.handle_data(context, data)
            _group_fund_ratio_sum = 0.0
            if isinstance(rule, ChoseStocksSimple):
                #print('--> _ret:', _ret)
                stock_list.extend(_ret)
                #self.log.info('%s 组合总资金占比：%.3f' % (self.memo, rule.group_fund_ratio))
                _fund_ratio_sum = 0.0
                for i in _ret:  # 更新各股资金占比（组合占比及内部占比，跟单时需相乘）
                    if i not in self.g.stocks_adjust_info:
                        self.g.stocks_adjust_info[i] = {}
                    self.g.stocks_adjust_info[i]['group_fund_ratio'] = rule.group_fund_ratio
                    #print('--> %s stocks_adjust_info: %s' % (i, self.g.stocks_adjust_info[i]))
                    _fund_ratio_sum += self.g.stocks_adjust_info[i]['inner_fund_ratio']
                if _fund_ratio_sum > 1.1:
                    self.log.warn('!!! 组合内资金占比总和 %.3f > 1.1，请检查策略代码是否有误！' % _fund_ratio_sum)
                _group_fund_ratio_sum += rule.group_fund_ratio
            if _group_fund_ratio_sum > 1.1:
                self.log.warn('!!! 多策略组合资金占比总和 %.3f > 1.1，请检查多策略配置是否有误！' % _group_fund_ratio_sum)
            # 如果进取策略指定了排他参数，则不再继续处理其他策略
            if len(stock_list) > 0 and hasattr(rule, 'exclusive') and rule.exclusive:
                break

        # 如果有过滤器，则对股票列表进行过滤（一般为根据选股数量进行截取）
        for rule in self.rules:
            if isinstance(rule, FilterStockList):
                stock_list = rule.filter(context, data, stock_list)
        
        if self.g.buy_stocks != stock_list:
            ## 更新调仓价格，一般在策略中进行，这里查漏补缺
            for i in (self.g.buy_stocks + stock_list):
                if i not in self.g.stocks_adjust_info:
                    self._update_adjust_info_single(context, data, i)

            # 更新建议持股列表
            #self.g.buy_stocks = list(set(self.g.buy_stocks) | set(stock_list))
            self.g.buy_stocks = stock_list
            self.log.info('建议持股列表出现变化，已改为：\n[%s]' % join_list(
                ["(%s %s %s)" % (x, show_stock(x), self.g.stocks_adjust_info[x]) \
                    for x in self.g.buy_stocks], ' ', 10))
            
            self.g.dump_trading_status(context)
        #else:
        #    self.log.info('建议持股列表无变化。')

    def before_trading_start(self, context):
        #print('--> [%s] self.rules: %s' % (self, self.rules))
        for rule in self.rules:
            if isinstance(rule, ChoseStocksSimple):
                rule.before_trading_start(context)

    def __str__(self):
        return self.memo

class AdjustPosition(GroupRules):
    """调仓规则组合器"""

    # 重载，主要是判断和调用 before_adjust_start / after_adjust_end 方法
    def handle_data(self, context, data):
        for rule in self.rules:
            if isinstance(rule, AdjustExpand):
                rule.before_adjust_start(context, data)

        GroupRules.handle_data(self, context, data)
        if self.is_to_return:
            return
        
        for rule in self.rules:
            if isinstance(rule, AdjustExpand):
                rule.after_adjust_end(context, data)

class StrategyRulesCombiner(GroupRules):
    """策略规则组合器"""

    def initialize(self, context):
        self.g = self._params.get('g_class', GlobalVariable)(self, context)
        self.memo = self._params.get('memo', self.memo)
        self.name = self._params.get('name', self.name)
        global g
        self.log = g.log_type(context, self.memo)
        self.g.context = context
        GroupRules.initialize(self, context)

    def handle_data(self, context, data):
        # 如果启动后，没有加载过交易持久数据，则加载之
        global IMPORT_TAG
        if IMPORT_TAG:
            self.g.load_trading_status(context)
            IMPORT_TAG = False
        
        for rule in self.rules:
            rule.handle_data(context, data)
            # 这里新增控制，假如是其它策略规则组合器要求退出的话，不退出。
            if rule.is_to_return and not isinstance(rule, StrategyRulesCombiner):
                self.is_to_return = True
                return
        self.is_to_return = False
        pass

    # 重载 set_g 函数，self.g 不再被外部修改
    def set_g(self, g):
        if self.g is None:
            self.g = g

class SortRules(GroupRules, FilterStockList):
    """多因子排序：每个规则产生一个排名，并根据排名和权重进行因子计算"""

    def filter(self, context, data, stock_list):
        # self.log.info(join_list([show_stock(stock) for stock in stock_list[:10]], ' ', 10))
        sorted_stocks = []
        total_weight = 0  # 总权重。
        for rule in self.rules:
            if isinstance(rule, SortBase):
                total_weight += rule.weight
        for rule in self.rules:
            if not isinstance(rule, SortBase):
                continue
            if rule.weight == 0:
                continue  # 过滤权重为0的排序规则，为以后批量自动调整权重作意外准备
            stocks = stock_list[:]  # 为防排序规则搞乱list，每次都重新复制一份
            # 获取规则排序
            tmp_stocks = rule.sort(context, data, stocks)
            # rule.log.info(join_list([show_stock(stock) for stock in tmp_stocks[:10]], ' ', 10))

            for stock in stock_list:
                # 如果被评分器删除，则不增加到总评分里
                if stock not in tmp_stocks:
                    stock_list.remove(stock)

            sd = {}
            rule_weight = rule.weight * 1.0 / total_weight
            for i, stock in enumerate(tmp_stocks):
                sd[stock] = (i + 1) * rule_weight
            sorted_stocks.append(sd)
        result = []

        for stock in stock_list:
            total_score = 0
            for sd in sorted_stocks:
                score = sd.get(stock, 0)
                if score == 0:  # 如果评分为0 则直接不再统计其它的
                    total_score = 0
                    break
                else:
                    total_score += score
            if total_score != 0:
                result.append([stock, total_score])
        result = sorted(result, key=lambda x: x[1])
        # 仅返回股票列表
        return [stock for stock, score in result]

    def __str__(self):
        return '多因子权重排序器'


### 指标类 ###
# IndicatorXXX(...) 可以被下述各类根据需要调用

class IndicatorRSJ():
    """技术指标-RSJ"""
    pass

class IndicatorNorthBond():
    """技术指标-北上资金"""

    def __init__(self):
        pass

    def _get_finance_data(self, context, end_date, day_desc=False, limit=200, calc_by_quota=False):
        # 默认截止到前一天（北上资金ETF参考策略使用的是当天，疑似用到了未来数据）
        if not end_date:
            #end_date = context.current_dt # 回测时会读取到未来数据？
            #end_date = (context.current_dt - dt.timedelta(1)).strftime('%Y-%m-%d') # 有bug，前一日可能非交易日
            end_date = context.previous_date
        # print(end_date.date(),'@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')

        table = finance.STK_ML_QUOTA
        q = query(
            table.day, table.quota_daily, table.quota_daily_balance,
            table.buy_amount, table.sell_amount
        ).filter(
            table.link_id.in_(['310001', '310002']), table.day <= end_date
        ).order_by(table.day.desc()).limit(limit*2)  # 每天两行数据

        #print('--> q:', q)
        df_money = finance.run_query(q)
        #print('--> df_money:', df_money)

        if calc_by_quota:
            # 按每日额度计
            df_money['net_amount'] = df_money['quota_daily'] - df_money['quota_daily_balance']
        else:
            # 按成交金额计（北上资金ETF策略所用方式）
            df_money['net_amount'] = df_money['buy_amount'] - df_money['sell_amount']
            #print('--> df_money with net_amount:', df_money)
            
        # 分组求和计算每日净流入
        df_money = df_money.groupby('day')[['net_amount']].sum().sort_values(
            'day', ascending=not day_desc)

        # 模拟盘再加上当日当时数据
        if context.run_params.type == 'sim_trade':
        #if True: # 仅用于代码调试
            amount_curr = 0  # 当日北上总额

            # 聚宽API只能获取到今天前的北向资金数据，模拟交易时可通过URL抓取当天到当时为止的数据：
            # 历史数据：'http://push2his.eastmoney.com/api/qt/kamt.kline/get?fields1=f1,f3,f5&fields2=f51,f52&klt=101&lmt=300'
            # 实时数据：'http://push2.eastmoney.com/api/qt/kamt.rtmin/get?fields1=f1,f3&fields2=f51,f52,f54,f56'
            # 页面参考：http://quote.eastmoney.com/center/hsgt.html
            ''' 12-24日测试的返回数据示例：
            {
                "rc": 0,
                "rt": 14,
                "svr": 182482208,
                "lt": 1,
                "full": 0,
                "data": {
                    "s2n": [
                    "9:30,4087.20,-27341.40,-23254.20",
                    "9:31,2131.88,-13765.20,-11633.32",
                    "9:32,-7217.75,-23696.57,-30914.32",
                    "9:33,-7297.45,-12021.08,-19318.53",
                    "9:34,-14131.58,-17681.84,-31813.42",
                    ......
                    "14:46,293245.93,225331.13,518577.06",
                    "14:47,289465.08,221477.24,510942.32",
                    "14:48,285725.88,218590.37,504316.25"
                    ],
                    "s2nDate": "12-23" # 貌似获取到的也是前一天的……
                }
            }
            '''
            try:
                import requests
                import json
                _url = 'http://push2.eastmoney.com/api/qt/kamt.rtmin/get?fields1=f1,f3&fields2=f51,f52,f54,f56'
                r = requests.get(_url)
                _s2n = json.loads(r.text).get('data').get('s2n')
                _s2n_date = json.loads(r.text).get('data').get('s2nDate')
                amount_curr = 0
                for x in _s2n:
                    amount_curr += float(x.split(',')[-1])
                amount_curr = amount_curr / 10**8  # 亿元 #TODO：数据单位待确定
                log.info('从东财获取到 %s 的北向资金总额：%f 亿' % (_s2n_date, amount_curr))
            except Exception as e:
                log.warn('从东财获取北向资金失败！' + str(e))

            # 或采用如下更精致的方法（参考 github 上相关项目实现）：
            # code from: https://github.com/jarekqin/north_bound/blob/master/northern_cash_flow_min.py
            """
            def define_basic_infom():
                cookies = {
                    'qgqp_b_id': '297950ac21997d0770ff1c36c30dc9f7',
                    '_qddaz': 'QD.vsea88.n8e9al.ke89kfpo',
                    'pgv_pvi': '5407696896',
                    'cowCookie': 'true',
                    'st_si': '28335634614582',
                    'cowminicookie': 'true',
                    'st_asi': 'delete',
                    'intellpositionL': '483px',
                    'st_pvi': '43322278274401',
                    'st_sp': '2020-08-20%2014%3A30%3A53',
                    'st_inirUrl': 'http%3A%2F%2Fdata.eastmoney.com%2Fhsgtcg%2FStockStatistics.aspx',
                    'st_sn': '10',
                    'st_psi': '20200909094128813-113300303606-1318002988',
                    'intellpositionT': '973.545px',
                }
                headers = {
                    'Connection': 'keep-alive',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36',
                    'Accept': '*/*',
                    'Referer': 'http://data.eastmoney.com/hsgt/index.html',
                    'Accept-Language': 'zh-CN,zh;q=0.9',
                }
                params = (
                    ('fields1', 'f1,f2,f3,f4'),
                    ('fields2', 'f51,f52,f53,f54,f55,f56'),
                    ('ut', 'b2884a393a59ad64002292a3e90d46a5'),
                    ('cb', 'jQuery183021065377894684523_1599615610093'),
                    ('_', '1599616390274'),
                )
                return cookies, headers, params

            def data_graping_solving():
                cookies, headers, params = define_basic_infom()
                try:
                    response = requests.get('http://push2.eastmoney.com/api/qt/kamt.rtmin/get', headers=headers, params=params,
                                            cookies=cookies, verify=False)
                except requests.exceptions.ConnectionError:
                    while True:
                        response = requests.get('http://push2.eastmoney.com/api/qt/kamt.rtmin/get', headers=headers, params=params,
                                                cookies=cookies, verify=False)
                        if response.status_code == 200:
                            break
                # 获取分钟数据
                pattern = re.compile(r'"data":[{](.*?)[}]', re.S)
                data = pattern.findall(response.text)[0]
                data = data.split(']')[0]  # 获取分割后的第一组北向资金数据
                data = [x.replace('"', '').replace('[', '') for x in data.split('"s2n":')[-1].split('",')]
                dataframe = pd.DataFrame([x.split(',')[1:] for x in data],
                                        index=[dt.datetime.today().strftime('%Y-%m-%d') + ' ' + x.split(',')[0] for x in data],
                                        columns=['HK2SH', 'HK2SH_Remained_CF', 'HK2SZ', 'HK2SZ_Remained_CF', 'WHOLE_NORTHERN_CF'])
                dataframe.index = pd.to_datetime(dataframe.index)
                return dataframe.replace('-', np.nan).dropna()

            try:
                df_min = data_graping_solving()
                # 12-24日晚上18点测试获取到的数据示例：
                ''' WHOLE_NORTHERN_CF
                2020-12-24 09:30:00         -23254.20
                2020-12-24 09:31:00         -11633.32
                2020-12-24 09:32:00         -30914.32
                2020-12-24 09:33:00         -19318.53
                2020-12-24 09:34:00         -31813.42
                2020-12-24 09:35:00          -4007.97
                ...                               ...
                2020-12-24 14:57:00         642550.06
                2020-12-24 14:58:00         649156.59
                2020-12-24 14:59:00         723635.87
                2020-12-24 15:00:00         652325.73
                [241 rows x 5 columns]
                '''
                for x in df_min['WHOLE_NORTHERN_CF']:
                    amount_curr += float(x)
                amount_curr = amount_curr / 10**8 # 亿元
                log.info('从东财获取到当天北向资金总额：%f 亿' % amount_curr)
            except Exception as e:
                log.warn('从东财获取北向资金失败！' + str(e))
            """

            # 插入当前日期数据 amount_curr 到 df_money
            #amount_curr = amount_curr * 1.5 # 当日数据适当增加权重，原因：1、当日未结束，2、数据更实时
            df_new = pd.DataFrame([[context.current_dt.strftime('%Y-%m-%d'), amount_curr]],
                                  columns=['day', 'net_amount'])
            df_new.set_index('day', inplace=True)
            #print('--> df_new:', df_new)
            #print('--> df_money:', df_money)
            df_new = df_new.append(df_money, ignore_index=False, sort=False)
            df_money = df_new

        print('近 %d 日北上资金流入：\n%s...' % (limit, df_money[:10]))
        return df_money

    def get_total_net_in(self, context, end_date=None, total_days=3):
        """获取近x天的北向资金总额。在日内交易时段调用。"""

        total_net_in = 0.0  # 近x天北上资金总额
        net_in_list = []

        #table = finance.STK_ML_QUOTA
        #n_sh = finance.run_query(query(
        #    table).filter(table.day <= end_date.date(),
        #                                table.link_id == 310001).order_by(
        #    table.day.desc()).limit(10))
        #n_sz = finance.run_query(query(
        #    table).filter(table <= end_date.date(),
        #                                table.link_id == 310002).order_by(
        #    table.day.desc()).limit(10))
        #
        #for i in range(0, total_days):
        #    sh_in = n_sh['buy_amount'][i] - n_sh['sell_amount'][i]
        #    sz_in = n_sz['buy_amount'][i] - n_sz['sell_amount'][i]
        #    net_in_list.append(sh_in + sz_in)
        #    # 这里获取到的数据与东财网基本一致

        df_money = self._get_finance_data(
            context, end_date, day_desc=True, limit=10)
        sum_days = total_days + 1 if context.run_params.type == 'sim_trade' else total_days
        for i in range(0, sum_days):
            net_in_list.append(df_money['net_amount'][i])

        total_net_in = sum(net_in_list[-total_days:])
        #log.info('近 %d 日北上资金清单：%s，总额：%.02f' % (total_days, net_in_list, total_net_in))
        return total_net_in

    def calc_boll(self, df_in, stdev_n=1.5):
        #print('--> df_in len: %d' % len(df_in))
        mid = df_in.mean()
        stdev = df_in.std()
        upper = mid + stdev_n * stdev
        lower = mid - stdev_n * stdev
        mf = df_in.iloc[-1]
        return mf, upper, lower

    def get_boll(self, context, end_date=None, windows=[150], stdev_n=1.5):
        """获取北向资金布林带。"""

        limit = max(windows)
        df_money = self._get_finance_data(context, end_date, day_desc=False, limit=limit)
        
        boll_ret = {}
        for i in windows:
            df_in = df_money['net_amount'].iloc[-i:]
            mf, upper, lower = self.calc_boll(df_in, stdev_n)
            boll_ret[i] = (mf, upper, lower)

        return boll_ret

class IndicatorMarketRisk():
    """技术指标-大盘风险控制
    # 克隆自聚宽文章：https://www.joinquant.com/post/29173
    # 标题：价值投资策略-大盘择时
    # 作者：叶松
    自行添加了来自于“中小板龙头——RSIX择时”的RSIX指标计算
    """

    class RiskStatus(Enum):
        WARNING = 1
        NORMAL = 2

    def __init__(self, index_stock='000001.XSHG'):
        self.index_stock = index_stock  # 所用指数股 000001.XSHG 或 000300.XSHG
        self.status = self.RiskStatus.NORMAL

    def risk_is_low(self, context, data, ctype='ma_rsi'):
        """根据指定的计算方法，判断市场风险是否可以交易
        ctype 计算方，可选值：ma_rsi, ma_rsi_v1, rsix"""

        could_trade = False
        if ctype == 'ma_rsi_v1':
            could_trade = self._check_for_ma_rsi_v1(context)
        elif ctype == 'ma_rsi':
            could_trade = self._check_for_ma_rsi(context)
        elif ctype == 'rsix':
            could_trade = self._check_for_rsix(context)
        
        #print('--> could_trade: %s' % could_trade)
        return could_trade

    def _check_for_ma_rsi_v1(self, context):
        ma_rate = self._compute_ma_rate(10000, True)
        
        could_trade = False
        if (0.75 < ma_rate < 1.50):
            could_trade = self._check_for_rsi(90, 35, 99, False)
        else:
            could_trade = self._check_for_rsi(15, 50, 70, False)

        return could_trade

    def _check_for_ma_rsi(self, context):
        could_trade = False

        ma_rate = self._compute_ma_rate(1000, False)
        if (ma_rate <= 0.0):
            return could_trade

        if (self.status == self.RiskStatus.NORMAL):
            if ((ma_rate > 2.5) or (ma_rate < 0.30)):
                self.status = self.RiskStatus.WARNING
        elif (self.status == self.RiskStatus.WARNING):
            if (0.35 <= ma_rate <= 0.7):
                self.status = self.RiskStatus.NORMAL

        if (self.status == self.RiskStatus.WARNING):
            #if (self.status == RiskStatus.WARNING) or not(self._check_for_usa_intrest_rate(context)):
            could_trade = self._check_for_rsi(15, 55, 90, False) and self._check_for_rsi(90, 50, 90, False)
            # could_trade = self._check_for_rsi(60, 47, 99, False)
            #record(status=2.5)
        elif (self.status == self.RiskStatus.NORMAL):
            could_trade = self._check_for_rsi(60, 50, 99, False)
            # could_trade = True
            #record(status=0.7)

        return could_trade

    def _compute_ma_rate(self, period, show_ma_rate):
        hst = get_bars(self.index_stock, period, '1d', ['close'])
        close_list = hst['close']
        if (len(close_list) == 0):
            return -1.0

        if (math.isnan(close_list[0]) or math.isnan(close_list[-1])):
            return -1.0

        period = min(period, len(close_list))
        if (period < 2):
            return -1.0

        #ma = close_list.sum() / len(close_list)
        ma = talib.MA(close_list, timeperiod=period)[-1]
        ma_rate = hst['close'][-1] / ma
        if (show_ma_rate):
            record(mar=ma_rate)

        return ma_rate

    def _check_for_rsi(self, period, rsi_min, rsi_max, show_rsi):
        hst = attribute_history(self.index_stock, period + 1, '1d', ['close'])
        close = [float(x) for x in hst['close']]
        if (math.isnan(close[0]) or math.isnan(close[-1])):
            return False

        rsi = talib.RSI(np.array(close), timeperiod=period)[-1]
        if (show_rsi):
            record(RSI=max(0, (rsi - 50)))

        return (rsi_min < rsi < rsi_max)

    def _check_for_usa_intrest_rate(self, context):
        could_trade = True
        '''
                            时间        利率    
        -------------------------------------------------
        美联储利率决议    2017/11/02    01.25
        美联储利率决议    2017/12/14    01.50---+
        美联储利率决议    2018/02/01    01.50   |
        美联储利率决议    2018/03/22    01.75   |
        美联储利率决议    2018/05/03    01.75   |
        美联储利率决议    2018/06/14    02.00   |
        美联储利率决议    2018/08/02    02.00   |
        美联储利率决议    2018/09/27    02.25<--+
        美联储利率决议    2018/11/09    02.25
        '''
        # 美联储利率大于1.5%并继续加息则为大利空
        if (string_to_datetime('2017-12-14 00:00:00') <= context.current_dt <= string_to_datetime('2018-09-27 00:00:00')):
            could_trade = False
        
        return could_trade

    def _check_for_rsix(self, context, g_n=5):
        """计算RSIX大盘指标，判断市场风险

        RSIX>0 是大盘特别好的时候，游资才敢大胆砸钱。缺点就是 RSIX>0 的时间段不是很长，一般仅适用于龙头追涨。
        使用建议：多策略组合时，RSIX>0 用龙头股策略，RSIX=0 用其他策略。"""
        #end_dt = context.current_dt  # 有未来？
        end_dt = context.previous_date  # zzz 修正
        price = get_bars(self.index_stock, 100, unit='1d', fields=['close','high','low'],
                        include_now=True, end_dt=end_dt)
        price1 = []
        price2 = []
        price3 = []
        for i in range(len(price)-1):
            price2.append(abs(price['close'][i+1] - price[i]['close']))
            if price['close'][i+1] > price['close'][i]:
                price1.append(price['close'][i+1] - price['close'][i])
                price3.append(2.0*(price['close'][i+1] - price['close'][i]))
            else:
                price1.append(0)
                price3.append(0)
        #price3 = price1*2
        
        def SMA(vals, n, m) :
            # 算法1 
            return reduce(lambda x, y: ((n - m) * x + y * m) / n, vals)
            
        #sma_data = tb.SMA(price['close'],5)
        sma_data1 = SMA(price1, g_n, 1)
        sma_data2 = SMA(price2, g_n, 1)
        sma_data3 = SMA(price3, g_n, 1)
        
        diff = max(sma_data3 - sma_data2, 0)
        mtr = 0
        j = -1
        while j >= -g_n:
            mtr = mtr + max(max(price['high'][j] - price['low'][j], \
                                abs(price['close'][j-1] - price['high'][j])), \
                            abs(price['close'][j-1] - price['low'][j]))
            j = j - 1
        atr = 1.0 * mtr / g_n
        rsix = 0  
        if diff > atr * 0.15:
            rsix = diff
        
        could_trade = False
        could_trade = rsix > 0
        return could_trade

### 止损类 ###
# StopLossByXXX(...)

### 止盈类 ###
# StopProfitByXXX(...)

### 选股类 ###
# PickByXXX(FilterQuery or FilterStockList)

class PickByXXX(FilterStockList):
    """选取股票类模板"""

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.arg01 = params.get('arg01', True)  # arg01

    def update_params(self, context, params):
        self.arg01 = params.get('arg01', self.arg01)

    def before_trading_start(self, context):
        pass
    
    def filter(self, context, data, stock_list):
        buy_stocks = []

        #选股计算逻辑……

        return buy_stocks

    def after_trading_end(self, context):
        pass
    
    def __str__(self):
        return '选取股票类模板 [参数01：%s]' % self.arg01

class PickByGeneral(FilterStockList):
    """通用选股 - 支持多种分类，如行业、概念、类型等"""
    
    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.category = params.get('category', 'all')  # 'all', 'index', 'industry', 'concept'
        self.obj_list = params.get('obj_list', [''])

    def update_params(self, context, params):
        self.category = params.get('category', self.category)
        self.obj_list = params.get('obj_list', self.obj_list)

    def filter(self, context, data, stock_list):
        stock_list = self._get_stocks(self.category, self.obj_list)
        self.stock_list_info(stock_list)
        return stock_list
    
    def _get_stocks(self, category='all', obj_list=[]):
        '''
            参数 category：获取成分股的方式。可选取值为 'all', 'concept', 'industry', 'index'
            参数 obj_list：可选取值。根据上述参数取值会各有不同，具体参见代码注释。
        '''
        
        stocks = []
        
        for obj in obj_list:

            # 获取标的成份股
            # category 为 all 时，可选取值为：'stock', 'fund', 'index', 'futures', 'options', 
            # 'etf', 'lof', 'fja', 'fjb', 'open_fund', 'bond_fund', 'stock_fund', 'QDII_fund', 
            # 'money_market_fund', 'mixture_fund'。为空时返回所有股票, 不包括基金、指数和期货。
            #   stock   - 获取所有股票列表
            #   fund    - 获取所有基金列表
            #   index   - 获取所有指数列表
            #   futures - 获取所有期货列表
            #   options - 获取所有期权列表
            #   etf     - 获取etf基金列表（一般为被动式指数基金）
            #   lof     - 获取lof基金列表（支持场外销售，可被动指数也可主动管理）
            #   fja     - 获取分级A基金列表（偏固定收益）
            #   fjb     - 获取分级B基金列表（偏进取型收益）
            if category == 'all':
                if obj == '':
                    stocks += list(get_all_securities().index)
                else:
                    stocks += list(get_all_securities(obj).index)

            # 获取概念成份股
            # category 为 concept 时，可选取值为概念股名称，如：
            #   GN127	食品安全	2013-12-31
            #   GN909	消费金融	2017-10-30
            #   GN179	医药电商	2014-12-09
            # 参考：https://www.joinquant.com/data/dict/plateData  # 标的以 GN 开头
            if category == 'concept':
                stocks += get_concept_stocks(obj)

            # 获取行业成份股
            # category 为 industry 时，可选取值为行业股名称，如：
            #   801120	食品饮料I	2003-10-15
            #   801150	医药生物I	2003-10-15
            #   HY005	日常消费	1999-12-30
            #   HY006	医疗保健	1999-12-30
            # 参考：https://www.joinquant.com/data/dict/plateData  # 标的以非 GN 开头
            # 聚宽行业以 HY 开头，申万为数字
            if category == 'industry':
                stocks += get_industry_stocks(obj)

            # 获取指数成份股
            # category 为 index 时，可选取值为指数股名称，如：
            #   000912.XSHG	300消费	2007-07-02
            #   399912.XSHE	沪深300主要消费指数	2007-07-02
            #   399932.XSHE	中证主要消费指数	2009-07-03
            #   000913.XSHG	300医药	2007-07-02
            #   399913.XSHE	沪深300医药卫生指数	2007-07-02
            #   399933.XSHE	中证医药卫生指数	2009-07-03
            # 参考：https://www.joinquant.com/data/dict/indexData
            if category == 'index':
                stocks += get_index_stocks(obj)

        return stocks

    def _get_high_grade_fund(self):
        """选取自定义A级基金 - 根据历史表现分级筛选"""

        stocks = self._get_stocks('all', ['fund'])
        df_price = get_price(stocks, start_date='2015-05-01', end_date='2016-03-20', frequency='daily', fields='close',
                             skip_paused=False, fq='pre')

        df_tmp = df_price.close.describe().T[['max', 'min']]
        df_tmp['down'] = (df_tmp['max'] - df_tmp['min']) / df_tmp['max']
        df_tmp = df_tmp.dropna(axis=0, how='any')

        df_stock = df_tmp.sort_values(by='down')

        for s in df_stock.index:
            df_stock.ix[s, 'name'] = get_security_info(s).display_name
            # price1 = get_price(s, end_date='2008-05-08', frequency='daily', fields='close',count=1, fq='pre').values[0]
            # price2 = get_price(s, end_date='2014-07-25', frequency='daily', fields='close',count=1, fq='pre').values[0]
            # price3 = get_price(s, end_date='2016-03-04', frequency='daily', fields='close',count=1, fq='pre').values[0]
            # price4 = get_price(s, end_date='2017-10-23', frequency='daily', fields='close',count=1, fq='pre').values[0]
            price1 = get_price(s, end_date='2014-01-01', frequency='daily',
                               fields='close', count=1, fq='pre').values[0]
            price2 = get_price(s, end_date='2016-03-01', frequency='daily',
                               fields='close', count=1, fq='pre').values[0]
            price3 = get_price(s, end_date='2016-03-01', frequency='daily',
                               fields='close', count=1, fq='pre').values[0]
            price4 = get_price(s, end_date='2017-08-30', frequency='daily',
                               fields='close', count=1, fq='pre').values[0]
            df_stock.ix[s, 'profit_1'] = (price2 - price1) / price1
            df_stock.ix[s, 'profit_2'] = (price4 - price3) / price3
            # print(df_stock.ix[s,'profit_2'])
            if df_stock.ix[s, 'profit_2'] > 0.5 and df_stock.ix[s, 'down'] < 0.3:
                df_stock.ix[s, 'grade'] = 'A+'
            elif df_stock.ix[s, 'profit_2'] > 0.5 and df_stock.ix[s, 'down'] >= 0.3 and df_stock.ix[s, 'down'] < 0.5:
                df_stock.ix[s, 'grade'] = 'A'
            elif df_stock.ix[s, 'profit_2'] > 0.3 and df_stock.ix[s, 'down'] < 0.3:
                df_stock.ix[s, 'grade'] = 'A-'
            elif df_stock.ix[s, 'profit_2'] < 0.3 and df_stock.ix[s, 'down'] > 0.3 \
                    or df_stock.ix[s, 'profit_2'] < 0.2:
                df_stock.ix[s, 'grade'] = 'C'
            else:
                df_stock.ix[s, 'grade'] = 'B'

        df_stock = df_stock.sort_values(by='profit_2', ascending=False)

        # write_file('test.csv', stock.to_csv(encoding="utf_8_sig"), append=False)
        funds_A1 = df_stock[df_stock['grade'] == 'A+']
        print(funds_A1)
        print([i for i in funds_A1.index])

        funds_A2 = df_stock[df_stock['grade'] == 'A']
        print(funds_A2)

        funds_A3 = df_stock[df_stock['grade'] == 'A-']
        print(funds_A3)

        funds_B = df_stock[df_stock['grade'] == 'B']
        print(funds_B)

        funds_list = []
        funds_list += [i for i in funds_A1.index]
        funds_list += [i for i in funds_A2.index]
        funds_list += [i for i in funds_A3.index]
        # funds_list += [i for i in funds_B.index]
        # print(funds_list)

        return funds_list

    def __str__(self):
        return '通用选股 [类别: %s，目标列表：%s ]' % (self.category, self.obj_list)

class PickByEvalJQ(FilterStockList):
    """根据聚宽语句选取股票"""

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.just_pick = params.get('just_pick', True)  # 直接选股不做过滤
        self.eval_input = params.get(
            'eval_input', "get_concept_stocks('GN189')")  # 选股语句

    def update_params(self, context, params):
        self.just_pick = params.get('just_pick', self.just_pick)
        self.eval_input = params.get('eval_input', self.eval_input)

    def filter(self, context, data, stock_list):
        buy_stocks = []

        stock_pool = eval(self.eval_input)

        if self.just_pick:
            buy_stocks = stock_pool
        elif len(stock_list) > 0:
            buy_stocks = list(set(stock_list) & set(stock_pool))

        msg = '--> 选股股票：%s' % str(buy_stocks)[:120]
        msg += '。total: %d' % len(buy_stocks)
        self.log.debug(msg)

        #self.log.info('选定买入股：%s' % buy_stocks)
        return buy_stocks

    def __str__(self):
        return '根据聚宽语句选取股票 [语句：%s]' % self.eval_input

class PickBySmallCapitalCZJZ(FilterStockList):
    """中小板成长股相对价值选股
    （源自：01-中小板成长股相对价值评估-Clone）"""

    # 克隆自聚宽文章：https://www.joinquant.com/post/32779
    # 标题：企业价值评估模型优化-13年至今年化62%，回撤32%
    # 作者：春天飞雪

    '''
    企业价值评估模型优化-13年至今年化62%，回撤32%

    原策略见https://www.joinquant.com/view/community/detail/9d9870655682a35b91001e2c0d9a2dc6
    企业价值评估模型-05年至今371倍，年化45%

    根据财务管理原则的有效市场理论，资本市场上的价格能够真实的反映其价值。
    股票估价的股利模型，假设其他条件不变，影响每股股价的唯一因素是每股现金
    股利的多少（P＝D/Ks）。那么如果股利支付率不变的情况下（相对价值法隐含的
    假设前提），影响每股现金股利的因素就是每股收益的多少（每股收益增长率到
    底能有多高）。这就将每股价格与每股收益联系到了一起，即市盈率，直观地反
    映投入与产出的关系。关键驱动因素：增长率。

    原理：可比企业平均市盈率/可比企业平均增长率＝目标企业市盈率/目标企业增长率
    修正的平均市盈率＝可比企业平均市盈率÷（可比企业平均增长率×100）
    目标企业评估流通价值＝修正的平均市盈率×增长率×目标企业每股收益 x 流通股本
    选股思路：min(当前流通值- 企业评估流通价值)
    模型：上述值最小值选500股，500股中选流通值最小的前10股买入。
    05至今371倍，年化收益率45%
    模型适用任何资金规模。

    原策略回撤为70%，优化后回撤有较大改进
    优化思路：当市场整体市盈率过高时，股市存在泡沫，持股风险较大，当市场整体
    调整市盈率> 9 时清仓，当市场整体调整市盈< 5时入场，根据回测情况，当市场
    整体调整市盈< 3 时，市场为振荡市，策略收益能超越指数涨幅。
    一般情况下，市场整体调整市盈率不会大于GDP增长率，考虑过往中国的GDP增长，
    选择了9为清仓条件。

    zzz - alpha一般，21年效果较好
    '''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.time_to_sell = params.get('time_to_sell', [10, 30])
        self.time_to_buy = params.get('time_to_buy', [14, 45])
        self.buy_count = params.get('buy_count', 5)

        # 股票池
        self.security_universe_index = "399101.XSHE"  # 中小板
        self.buy_count = 5
        self.adj_pe = 0

    def update_params(self, context, params):
        self.time_to_sell = params.get('time_to_sell', self.time_to_sell)
        self.time_to_buy = params.get('time_to_buy', self.time_to_buy)
        self.buy_count = params.get('buy_count', self.buy_count)

    def before_trading_start(self, context):
        pass
        
    def filter(self, context, data, stock_list):
        #选股计算逻辑……

        stocks_to_buy = []
        stocks_to_sell = []

        hour = context.current_dt.hour
        minute = context.current_dt.minute
        
        if [hour, minute] == self.time_to_sell:
            # 处理卖出
            curr_data = get_current_data()
            #如果持仓中有停牌股票，如果买入涨停板会有2支股票
            if self.adj_pe > 9:
                for stock in self.holding_stocks:
                    if not curr_data[stock].paused \
                    and context.portfolio.positions[stock].closeable_amount > 0:
                        stocks_to_sell.append(stock)
                        self.log.info('交易清淡早盘清仓 %s' % stock)
        
        elif [hour, minute] == self.time_to_buy:
            # 处理买入
            # 选取中小板中市值最小的若干只
            curr_data = get_current_data()
            check_out_lists = self._stock_choice(context)
            q = query(valuation.code).filter(
                valuation.code.in_(check_out_lists)
            ).order_by(
                valuation.circulating_market_cap.asc()
            ).limit(
                self.buy_count * 3
            )
            check_out_lists = list(get_fundamentals(q).code)
            # 过滤: 三停（停牌、涨停、跌停）及st,*st,退市
            check_out_lists = self._filter_limitup_stock(context, check_out_lists)
            check_out_lists = self._filter_limitdown_stock(context, check_out_lists)
            check_out_lists = FuncLib.filter_special(context, check_out_lists)
            # 取需要的只数
            check_out_lists = check_out_lists[:self.buy_count]
            
            if self.adj_pe <= 5:
                # 卖出
                for stock in self.holding_stocks:
                    if stock not in check_out_lists:
                        self.log.info("卖出 %s" % (stock))
                        stocks_to_sell.append(stock)
                    else:
                        self.log.info("%s 已经在持仓列表中" % (stock))
                # 买入
                for stock in check_out_lists:
                    stocks_to_buy.append(stock)
                    self.log.info("买入 %s" % (stock))

        if len(stocks_to_sell) or len(stocks_to_buy):
            #stock_list = list(set(stock_list) - stocks_to_sell | stocks_to_buy)[:self.buy_count]
            # 上面的写法可能会改变顺序，所以改为如下写法：
            stock_list = [i for i in stock_list if i not in stocks_to_sell]
            stock_list += [i for i in stocks_to_buy if i not in stock_list]
            stock_list = stock_list[:self.buy_count]

            self.holding_stocks = stock_list

            self._update_adjust_ratio(context, data, stock_list, len(stock_list))
            self._update_adjust_info(context, data, stock_list)

        #if len(stock_list) > 0: self.log.info('选定买入股：%s' % stock_list)
        return stock_list

    def after_trading_end(self, context):
        pass

    def _stock_choice(self, context):
        # 选取中小板中市值最小的若干只
        end_date = context.previous_date
        #curr_data = get_current_data()
        q = query(
            valuation.code, valuation.circulating_market_cap, valuation.pe_ratio,
            indicator.inc_revenue_year_on_year,indicator.eps,valuation.circulating_cap
        ).filter(
            #valuation.circulating_market_cap < 2e10,
            valuation.pe_ratio > 1,
            indicator.inc_revenue_year_on_year > 1,
            indicator.inc_revenue_year_on_year < 100
        ).order_by(
            # 按市值降序排列
            valuation.circulating_market_cap.desc()
        )
        adj_lists = get_fundamentals(q, date = end_date)
        
        #pe平均值
        pe_avg = adj_lists['pe_ratio'].mean()
        self.log.info('平均市盈率 %f' % (pe_avg))
        #收入增长平均值inc_revenue_year_on_year
        income_avg = adj_lists['inc_revenue_year_on_year'].mean()
        self.log.info('收入平均增长率 %f' % (income_avg))
        #调整后pe
        adj_pe = pe_avg / income_avg
        self.log.info('调整市盈率 %f' % (adj_pe))
        self.adj_pe = adj_pe
        #record(adj_pe=adj_pe)

        adj_lists['adj_pe'] = adj_lists['inc_revenue_year_on_year'] * adj_pe
        adj_lists['adj_cap'] = adj_lists['adj_pe'] * adj_lists['eps'] * adj_lists['circulating_cap'] / 10000
        adj_lists['choice_market'] = adj_lists['circulating_market_cap'] - adj_lists['adj_cap']
        factor = adj_lists[adj_lists.choice_market < 0]
        #print(factor)
        factor = factor.sort_values('choice_market', ascending=True)
        check_out_lists = factor['code'].tolist()

        return check_out_lists
    
    # 过滤涨停的股票
    def _filter_limitup_stock(self, context, stock_list):
        last_prices = history(1, unit='1m', field='close', security_list=stock_list)
        current_data = get_current_data()
        
        # 已存在于持仓的股票即使涨停也不过滤，避免此股票再次可买，但因被过滤而导致选择别的股票
        return [stock for stock in stock_list if stock in self.holding_stocks
                or last_prices[stock][-1] < current_data[stock].high_limit]

    # 过滤跌停的股票
    def _filter_limitdown_stock(self, context, stock_list):
        last_prices = history(1, unit='1m', field='close', security_list=stock_list)
        current_data = get_current_data()
        
        return [stock for stock in stock_list if stock in self.holding_stocks
                or last_prices[stock][-1] > current_data[stock].low_limit]
    
    def __str__(self):
        return '中小板成长价值选股 [买入数量：%s 卖出时间点：%s 买入时间点：%s]' \
            % (self.buy_count, self.time_to_sell, self.time_to_buy)
    
class PickByHotETF(FilterStockList):
    """选股热门ETF - 来自于【01-北向资金ETF-Clone】。
    
    原理：根据大盘指数成交量选取对应基金（纯宽基指数）。在多个种类的“指数-ETF”对中，排序持仓前几个。
    持仓原则：
        1、对沪深指数的成交量进行统计，如果连续6（lag）天成交量小于7（lag0)天成交量的，空仓处理（购买货币基金511880、银华日利或国债511010）
        2、13个交易日内（lag1）涨幅大于1的，并且“均线差值”大于0的才进行考虑。
        3、对符合考虑条件的ETF的涨幅进行排序，买涨幅最高的三个。
    
    ** 貌似聚宽支持的指数和基金比较有限
    ** 做指数化投资，只要ETF基金，不要ETF连接基金。另外需要一个费率低廉，没有最低五块钱的证券账户，如安信、银河等
    可参考“东财行业主题指数基金”列表：http://fund.eastmoney.com/ZS_jzzzl.html#os_0;isall_0;ft_054|;pt_5
    或：https://zhuanlan.zhihu.com/p/121799411?utm_source=wechat_session
    证监会官方网站基金投资知识：http://www.csrc.gov.cn/pub/shenzhen/xxfw/tzzsyd/jjtz/
    """
    
    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.buy_count = params.get('buy_count', 5)  # 最大选取数量

        self.keep_count = self.buy_count * 2 if self.buy_count > 2 else 4

        self._signal = 'BUY'  # 交易信号初始化
        self._lag1 = 13  # 比价均线周期
        self._lag2 = 13  # 价格涨幅计算周期

        self._candidate = {}
        self._buy = []  # 购买股票列表

        # 本策略所选用的指数、基金对, 会自动过滤掉交易时没有上市的
        # 聚宽可选指数清单（推荐中证系列）：https://www.joinquant.com/data/dict/indexData
        # 在中证指数官网搜索某指数，在“相关产品”页中即可见关联ETF：http://www.csindex.com.cn/zh-CN/indices/index
        self._stock_pool = {  # 原始策略股票池
            '399001.XSHE' :'150019.XSHE',#银华锐进
            '399905.XSHE' :'159902.XSHE',#中小板指
            #'399975.XSHE':'512880.XSHG',#券商B
            '399632.XSHE' :'159901.XSHE',#深100etf
            #'399001.XSHG':'162605.XSHE',#景顺鼎益
            '000016.XSHG' :'510050.XSHG',#上证50
            #'000010.XSHG':'510180.XSHG',#上证180
            #'000015.XSHG' :'510880.XSHG',#红利ETF
            '399324.XSHE' :'159905.XSHE',#深红利
            '399006.XSHE' :'159915.XSHE',#创业板
            #'399006.XSHE':'150153.XSHE',#创业板B
            '000300.XSHG' :'510300.XSHG',#沪深300
            '000905.XSHG' :'510500.XSHG',#中证500
            '399673.XSHE' :'159949.XSHE',#创业板50
            #'399976.XSHE':'515700.XSHG',#新能车etf
            #'399967.XSHE':'512660.XSHG',#军工etf 
            #'399933.XSHE' :'512010.XSHG',#医药etf 
            #'399441.XSHE' :'512290.XSHG',#生物医药etf 
            #'AU1903.XSGE' :'518800.XSHG',#黄金基金 
            #'399997.XSHE' :'512690.XSHG',#白酒
            #'000932.XSHG' :'159928.XSHE',#消费
        }
        #'''
        self._stock_pool = {  # zzz 修正股票池（效果待更多验证）
            '399001.XSHE' :'150019.XSHE',  #银华锐进（有杠杆？）
            '399905.XSHE' :'159902.XSHE',  #中小板指
            #'399975.XSHE':'512880.XSHG',  #券商B
            '399632.XSHE' :'159901.XSHE',  #深100etf
            '399001.XSHG':'162605.XSHE',   #景顺鼎益
            '000016.XSHG' :'510050.XSHG',  #上证50
            #'000010.XSHG':'510180.XSHG',  #上证180
            #'000015.XSHG' :'510880.XSHG', #红利ETF
            '399324.XSHE' :'159905.XSHE',  #深红利
            '399006.XSHE' :'159915.XSHE',  #创业板
            #'399006.XSHE':'150153.XSHE',  #创业板B
            '000300.XSHG' :'510300.XSHG',  #沪深300
            '000905.XSHG' :'510500.XSHG',  #中证500
            '399673.XSHE' :'159949.XSHE',  #创业板50
            '399976.XSHE':'515700.XSHG',   #新能车etf
            '399967.XSHE':'512660.XSHG',   #军工etf 
            '399933.XSHE' :'512010.XSHG',  #医药etf 
            #'399441.XSHE' :'512290.XSHG', #生物医药etf 
            'AU1903.XSGE' :'518800.XSHG',  #黄金基金 
            '399997.XSHE' :'512690.XSHG',  #白酒
            '000932.XSHG' :'159928.XSHE',  #中证消费
            #'930719.XXXX':'501005.XSHG',  #中证精准医疗主题指数（聚宽不支持以9开头的指数？）
        }
        #'''
        stocks_info = "\n股票池:\n"
        for security in self._stock_pool.values():
            s_info = get_security_info(security)
            if s_info:
                stocks_info += "【%s】%s 上市日期：%s\n" % (
                    s_info.code, s_info.display_name, s_info.start_date)
            else:
                stocks_info += security + '\n'
        print(stocks_info)

    def update_params(self, context, params):
        self.buy_count = params.get('buy_count', self.buy_count)
        
    def before_trading_start(self, context):
        self._before_market_open(context)

    def _before_market_open(self, context):
        # 确保交易标的已经上市 lag1 个交易日以上
        #log.info('--> zzzzzzz 函数运行时间 _before_market_open'+ str(context.current_dt.time()))
        yesterday = context.previous_date
        list_date = self._get_before_after_trade_days(
            yesterday, self._lag1)  # 今天的前lag1个交易日的日期
        self._candidate = {}  # 可交易对象
        all_funds = get_all_securities(
            types='fund', date=yesterday)  # 上个交易日之前上市的所有基金
        all_idxes = get_all_securities(
            types='index', date=yesterday)  # 上个交易日之前就已经存在的指数
        #log.info('--> self._stock_pool:', self._stock_pool)
        for idx in self._stock_pool:
            if idx in all_idxes.index:
                if all_idxes.loc[idx].start_date <= list_date:  # 指数已经在要求的日期前上市
                    symbol = self._stock_pool[idx]
                    if symbol in all_funds.index:
                        if all_funds.loc[symbol].start_date <= list_date:  # 对应的基金也已经在要求的日期前上市
                            self._candidate[idx] = symbol  # 则列入可交易对象中
        return

    def after_trading_end(self, context):
        pass

    def filter(self, context, data, stock_list):
        '''
        try:
            self._get_signal(context)
            if self._signal == 'CLEAR':
                stock_list = []
            elif self._signal == 'BUY':
                stock_list = self._buy
        except Exception as e:
            #log.error(e)
            formatted_lines = traceback.format_exc().splitlines()
            self.log.error(formatted_lines[-7:])

        # 平均分配更新买入持仓股内部资金占比
        self._update_adjust_ratio(context, data, stock_list, len(stock_list))
        self._update_adjust_info(context, data, stock_list)
        '''

        stocks_to_buy = []
        stocks_to_sell = []

        #hour = context.current_dt.hour
        #minute = context.current_dt.minute
        #if not (hour == 9 and minute == 31):
        #    return stock_list

        # 根据原策略逻辑，指定买入列表1只，可持列表4只

        # 1. 重新选择股票
        try:
            self._get_signal(context)
            if self._signal == 'CLEAR':  # 清仓
                to_buy_list = []
                to_keep_list = []
            elif self._signal == 'BUY':  # 换仓
                to_buy_list = self._buy[0:self.buy_count]
                to_keep_list = self._buy[0:self.keep_count]
            else:  # 不变
                to_buy_list = stock_list
                to_keep_list = stock_list
        except Exception as e:
            #log.error(e)
            formatted_lines = traceback.format_exc().splitlines()
            self.log.error(formatted_lines[-7:])
        
        stock_list = self.get_stock_list_and_set_adjust(context, data, stock_list,
                            to_buy_list, to_keep_list, self.buy_count)
        
        if len(stock_list) > 0: self.log.info('选定买入股：%s' % stock_list)
        return stock_list

    def _get_before_after_trade_days(self, date, count, is_before=True):
        """
        来自： https://www.joinquant.com/view/community/detail/c9827c6126003147912f1b47967052d9?type=1
        date :查询日期
        count : 前后追朔的数量
        is_before : True , 前count个交易日  ; False ,后count个交易日
        返回 : 基于date的日期, 向前或者向后count个交易日的日期 ,一个dt.date 对象
        """
        all_date = pd.Series(get_all_trade_days())
        if isinstance(date, str):
            date = dt.datetime.strptime(date, '%Y-%m-%d').date()
        if isinstance(date, dt.datetime):
            date = date.date()

        if is_before:
            return all_date[all_date <= date].tail(count).values[0]
        else:
            return all_date[all_date >= date].head(count).values[-1]

    # 获取信号
    def _get_signal(self, context):
        # 创建保持计算结果的 DataFrame
        df_etf = pd.DataFrame(columns=['基金代码', '对应指数', '周期涨幅', '均线差值'])
        current_data = get_current_data()
        #log.info('--> self._candidate:', self._candidate)
        for mkt_idx in self._candidate:
            security = self._candidate[mkt_idx] # 指数对应的基金
            # 获取最近lag1天股票收盘价
            close_data = attribute_history(
                security, self._lag1, '1d', ['close'], df=False)
            # 获取股票现价
            current_price = current_data[security].last_price
            # 获取股票的阶段收盘价涨幅（当前_lag2设置等于_lag1，相当于取昨日收盘价）
            cp_increase = (
                current_price / close_data['close'][self._lag2 - self._lag1] - 1) * 100
            # 取得平均价格
            ma_n1 = close_data['close'].mean()
            # 计算前一收盘价与均值差值
            pre_price = (current_price / ma_n1 - 1) * 100
            df_etf = df_etf.append({'基金代码': security, '对应指数': mkt_idx, 
                                    '周期涨幅': cp_increase, '均线差值': pre_price},
                                    ignore_index=True)

        if len(df_etf) == 0:
            self.log.info("交易信号 - 没有可以交易的品种，清仓")
            self._signal = 'OTHER'
            return

        # 按照涨幅降序排列
        df_etf.sort_values(by='周期涨幅', ascending=False, inplace=True)
        
        # 取 lag1 个交易日内涨幅大于1%（但原代码实际为0.1%，如下），且“均线差值”大于0的标的
        if df_etf['周期涨幅'].iloc[0] < 0.1 or df_etf['均线差值'].iloc[0] < 0:
            self.log.info('交易信号 - 所有品种均不符合要求，空仓')
            self._signal = 'CLEAR'
            return

        # 获取到符合要求的基金品种，买入
        #print("--> df_etf['基金代码']:", df_etf['基金代码'])
        #self._buy = [df_etf['基金代码'].iloc[0],]
        self._buy = list(df_etf['基金代码'].iloc[0:])
        self.log.info("交易信号 - 建议持有 %s" % self._buy)
        self._signal = 'BUY'
        return

    def __str__(self):
        return '结合大盘成交量选取热门ETF [ 最多选取: %d ]' % self.buy_count

class PickByLongtou(FilterStockList):
    """追涨龙头选股 - 来自于聚宽【01-追涨龙头V1-Clone】"""
    
    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.g_security = []
        self.g_security_del = []
        #g.sell_list = []
        self.g_small_cyc = 3
        self.g_small_cyc_before = 3
        self.g_big_cyc = 3
        self.g_big_cyc_before = 3
        self.g_big_cyc_before_1 = 3
        self.stocks_bought_today = []  # 存储当天买入的股票列表
        #g.limit_stocks = []
        self.start_times =  ' 09:15:00'
        #mid_time = ' 09:20:00'
        self.end_times =  ' 09:25:00'
    
    def update_params(self, context, params):
        pass

    def initialize(self, context):
        # 注册定时执行函数
        # 其中每个成员为一个三元组tupple，其最后一个参数为运行时参考标的
        # 传入的参考标的只做种类区分，因此传入'000300.XSHG'或其他股票是一样的
        params_list = []
        # 开盘前运行（已在后面另外调用，这里先注释掉）
        #params_list.append((self.run_daily_call_auction, '09:27', '000300.XSHG'))
        # 收盘后运行
        #params_list.append((self.run_daily_after_market_close, 'after_close', '000300.XSHG'))
        #self.run_daily_batch(context, params_list)  # 聚宽 run_daily 不支持回调对象成员函数

    def filter(self, context, data, stock_list):
        hour = context.current_dt.hour
        minute = context.current_dt.minute
        
        # 执行开盘时要运行的代码
        if hour == 9 and minute == 30:
            log.info('开盘时代码运行时间 ' + str(context.current_dt.time()))

            # TODO 待改进：本来应该在盘前 '09:27' 调用，但当前聚宽的 run_daily 
            # 不支持回调函数为类成员函数，故暂且后移到这里调用
            self.run_daily_call_auction(context)
            
            # 以下代码来自于 market_open(context)
            
            stocks_to_sell = []
            date = context.current_dt.strftime("%Y-%m-%d")
            before_days = get_trade_days(end_date=context.current_dt, count=6)  #获取交易日期
            #for stock in list(context.portfolio.positions.keys()):
            log.info('--> stock_list at 09:27: %s' % stock_list)
            for stock in stock_list:
                log.info('向前2天涨停：' + str(before_days[-2]) + ' ' + str(stock))
                # 获取股票的收盘价
                close_data = get_bars(stock, count=3, unit='1d', fields=['open','high','low','close'])
                if close_data['close'][-1] / close_data['close'][-2] < 1.098:
                    df = self.get_call_auction_data(stock, date)
                    if len(df) > 0:
                        a1_p_max = df['a1_p'].max()
                        a1_p_min = df['a1_p'].min()
                        a1_p0 = df['a1_p'][-1]
                        a1_p1 = df['a1_p'][-2]
                        a1_p2 = df['a1_p'][-3]
                        print(stock + ' ' + str(a1_p_max) + ' ' + str(a1_p_min) \
                            + ' ' + str(a1_p1) + ' ' + str(a1_p0))
                        if a1_p0 < a1_p1 or a1_p1 < a1_p2 or a1_p0 / close_data['close'][-1] < 0.98:
                            stocks_to_sell.append(stock)
                            print('前一天没涨停，开盘卖出 ' + stock)
            stock_list = [i for i in stock_list if i not in stocks_to_sell]

            #cash = context.portfolio.available_cash
            #total_value = context.portfolio.total_value
            #if len(self.g_security) == 0:
            #    return []
            #buy_cash = cash / (len(self.g_security))
            for stock in self.g_security:
                #if stock in list(context.portfolio.positions.keys()):
                if stock in stock_list:
                    continue
                log.info("开盘买入 %s" % (stock))
                # 用所有 cash 买入股票
                #order_value(stock, buy_cash)
                stock_list.append(stock)
                self.stocks_bought_today.append(stock)
        
        # 执行收盘前要运行的代码
        #log.info('--> hour: %d, minute: %d' % (hour, minute))
        if hour == 14 and minute == 57:
            log.info('收盘前代码运行时间 ' + str(context.current_dt.time()))

            # 以下代码来自于 before_market_close(context)

            stocks_to_sell = []
            g.last_df = history(1, '1d', 'close', stock_list)
            log.info('--> stock_list at 14:57:', stock_list)
            for stock in stock_list:
                # 获取股票的收盘价
                close_data = get_bars(stock, count=1, unit='1m', fields=['open','high','low','close'])
                close_price = close_data['close'][-1]
                log.info('close_price = %.2f' % close_price)
                if close_price / g.last_df[stock][0] < 1.098:
                    stocks_to_sell.append(stock)
                elif close_price / g.last_df[stock][0] > 1.11 and close_price / g.last_df[stock][0] < 1.195:
                    stocks_to_sell.append(stock)
                else:
                    log.info('------- 涨停不卖出: %s' % stock)
            # 将今天买入的从 stocks_to_sell 中排除
            #log.info('--> 1 stocks_bought_today: %s' % self.stocks_bought_today)
            stocks_to_sell_new = [i for i in stocks_to_sell if i not in self.stocks_bought_today]
            stock_list = [i for i in stock_list if i not in stocks_to_sell_new]
            #stock_list = list(set(stock_list) & set(stocks_to_sell_new))  # 另一种写法

        # 全交易时段需执行的代码
        # 以下代码来自于 market_open_sell(context)

        # 卖出跌幅超过 5% 的股票
        stocks_to_sell = []
        #for stock in list(context.portfolio.positions.keys()):
        for stock in stock_list:
            close_data_1d = get_bars(stock, count=3, unit='1d', fields=['open','high','low','close'])
            close_data_1m = get_bars(stock, count=3, unit='1m', fields=['open','high','low','close'], include_now=True)

            stop_line = 0.95
            if close_data_1d['close'][-1] / close_data_1d['close'][-2] > 1.12:
                stop_line = 0.85
            
            if close_data_1m['close'][-1] / close_data_1d['close'][-1] < stop_line:
                order_target(stock, 0)
                stocks_to_sell.append(stock)
                print('跌幅超过 5%，盘中卖出 ' + str(stock))
        # 将今天买入的从 stocks_to_sell 中排除
        #log.info('--> 2 stocks_bought_today: %s' % self.stocks_bought_today)
        stocks_to_sell_new = [i for i in stocks_to_sell if i not in self.stocks_bought_today]
        stock_list = [i for i in stock_list if i not in stocks_to_sell_new]

        # 只在 09:34 之前换仓
        if context.current_dt.strftime("%H:%M:%S") >= '09:34:00':
            return stock_list
        
        cash = context.portfolio.available_cash
        #total_value = context.portfolio.total_value
        #if len(self.g_security_del) == 0:
        #    return []
        #buy_cash = cash / len(self.g_security_del)
        end_time = context.current_dt.strftime("%Y-%m-%d %H:%M:%S")
        start_time = context.current_dt.strftime("%Y-%m-%d 09:15:00")
        mid20 = context.current_dt.strftime("%Y-%m-%d 09:15:00")
        mid25 = context.current_dt.strftime("%Y-%m-%d 09:25:00")
        #log.info('--> self.g_security_del:', self.g_security_del)
        for stock in self.g_security_del:
            #if stock in list(context.portfolio.positions.keys()):
            if stock in stock_list:
                continue
            # 获取股票的收盘价
            t = get_ticks(stock, end_time, start_time, fields=['time','current','a1_p','a1_v'], skip=False)
            df = pd.DataFrame(t)
            df.time = pd.to_datetime(df.time.astype(int).astype(str))
            df.set_index('time', inplace=True)
            df.current[df.current==0] = df.a1_p[df.current==0]
            
            # 第二阶段 9:20 到 9:25
            df25 = df[df.index > mid20]
            df25 = df25[df25.index < mid25]
            df25_max = df25['a1_p'].max()
            df25_min = df25['a1_p'].min()
            
            #print(df)
            print(end_time + ' ' + str(stock) + ' ' + str(df['current'][-1]))
            
            if df['current'][-1] / df25_min > 1:
                log.info("价格高于均价 1%%, 买入 %s" % (stock))
                # 用所有 cash 买入股票
                #order_value(stock, buy_cash)
                stock_list.append(stock)
                self.stocks_bought_today.append(stock)
        
        self.stock_list_info(stock_list)
        if len(stock_list) > 0: self.log.info('选定买入股：%s' % stock_list)
        return stock_list

    # 计算上市天数
    def filter_by_listed_days(self, stock_list, end_date, n=100):
        # type: (list, Union[str, dt.date, dt.datetime], int) -> list
        """
        过滤掉 stock_list 中尚未上市的，或者上市天数不足 n 天的股票
        """
        # 1. 取得 n 个交易日之前的交易日期 trade_date
        trade_date = get_trade_days(end_date=end_date, count=n)[0]
        # 2. 获取 trade_date 日就已经上市的所有股票
        all_stocks = get_all_securities(date=trade_date)
        # 3. 过滤 stock_list，剔除掉不在 all_stocks 中的
        valid_stk_list = list(all_stocks[all_stocks.index.isin(stock_list)].index)
        return valid_stk_list

    def st_check(self, stock_list):  # 过滤st股
        current_data = get_current_data()
        stock_list = [stock for stock in stock_list if not current_data[stock].is_st]
        return stock_list

    # 统计连续涨停股票 v1
    def statistical_limit_up_old(self, stock_list, this_date):
        limit_up_dic = {}  # 存放连续涨停的股票
        date_list = []
        code_list = []
        high_list = []
        name_list = []
        limit_stocks = []
        for stock in stock_list:
            # 定义涨停次数(至少一次涨停，否则被过滤)
            n = 0
            df = get_price(stock,end_date=this_date,frequency='1d',fields=['pre_close','low','close','high_limit','paused'] ,count=20)
            #过滤当天停牌的股票
            if df['paused'][-1] != 0:
                continue
            for i in range(18):
                if df['close'][-1-i] >= df['high_limit'][-1-i] and df['paused'][-1-i] == 0 and  df['close'][-1-i] / df['pre_close'][-1-i] > 1.09:
                    if df['close'][-2-i] >= df['high_limit'][-2-i] and df['paused'][-2-i] == 0:
                        n += 1
                    elif df['close'][-1-i] >= df['low'][-1-i] + 0.05:
                        n += 1
                    else:
                        if df['close'][-i] >= df['low'][-i] + 0.05:
                            limit_up_dic[stock] = n
                            code_list.append(stock)
                            high_list.append(n)
                            name_list.append(str(get_security_info(stock).display_name))
                        break
                else:
                    if n > 0:
                        if df['close'][-i] >= df['low'][-i] + 0.05:
                            limit_up_dic[stock] = n
                            code_list.append(stock)
                            high_list.append(n)
                            name_list.append(str(get_security_info(stock).display_name))
                        elif df['close'][-i+1] >= df['low'][-i+1] + 0.05:
                            n = n -1
                            limit_up_dic[stock] = n
                            code_list.append(stock)
                            high_list.append(n)
                            name_list.append(str(get_security_info(stock).display_name))
                        break

        df_data = pd.DataFrame(columns = ["股票代码","股票名称","涨停高度"]) #创建一个空的dataframa
        df_data['股票代码'] = list(code_list)
        df_data.stock_name = name_list  # 股票名称
        df_data.up_high = list(high_list)  # 涨停高度
        self.g_small_cyc = df_data['涨停高度'].max()
        df_data['涨停高度'] = np.where(df_data['涨停高度'] < -1, self.g_small_cyc, df_data['涨停高度'])
        df_data_max = df_data[df_data['涨停高度'] == self.g_small_cyc]
        
        log.info('----- 新高股票 ----- ' + str(this_date))
        log.info(df_data_max)
    
        if self.g_small_cyc <= self.g_small_cyc_before:
            self.g_big_cyc = self.g_small_cyc_before
            
        elif self.g_small_cyc > self.g_big_cyc:
            self.g_big_cyc = self.g_small_cyc
            
        print('self.g_small_cyc1='+str(self.g_small_cyc)+', self.g_big_cyc1 = '+str(self.g_big_cyc)+', self.g_big_cyc_before_1 = '+str(self.g_big_cyc_before))      
        if (self.g_big_cyc <= 4 and self.g_small_cyc <= 4 and len(df_data_max) < 4):
            for stock in df_data_max['股票代码']:
                df = get_price(stock,end_date=this_date,frequency='1d',fields=['volume','open','close','high_limit','paused'] ,count=3)
                if df['open'][-1] != df['high_limit'][-1] or df['open'][-2] != df['high_limit'][-2]:
                    limit_stocks.append(stock)
                else:
                    print('一字板，删除'+str(stock))
        else:
            limit_stocks = {}
        
        self.g_security = limit_stocks
        self.g_small_cyc_before = self.g_small_cyc
        self.g_big_cyc_before_1 = self.g_big_cyc_before
        self.g_big_cyc_before = self.g_big_cyc
        print('self.g_small_cyc='+str(self.g_small_cyc)+', self.g_big_cyc = '+str(self.g_big_cyc)+', self.g_big_cyc_before = '+str(self.g_big_cyc_before))
        return limit_up_dic
    
    # 统计连续涨停股票 v2
    # ref: https://www.joinquant.com/view/community/detail/0f2211b00ab29d18d914eefba43301f0?type=1
    def statistical_limit_up(self, stock_list, this_date):
        # 定义存放连续涨停的字典
        limit_up_dic = {}
        date_list = []
        code_list = []
        high_list = []
        name_list = []
        limit_stocks = []
        for stock in stock_list:
            try:
                # 定义涨停次数(至少一次涨停，否则被过滤)
                n = 0
                df = get_price(stock,end_date=this_date,frequency='1d',fields=['pre_close','low','close','high_limit','paused'] ,count=15)
                #过滤当天停牌的股票
                if df['paused'][-1] != 0 or df['close'][-1] / df['pre_close'][-1] <  1.09:
                    continue
                flag = False
                for i in range(15):
                    if df['close'][-1-i] >= df['high_limit'][-1-i] and df['paused'][-1-i] == 0:
                        if df['close'][-2-i] >= df['high_limit'][-2-i] and df['paused'][-2-i] == 0:
                            n += 1
                        elif df['close'][-1-i] >= df['low'][-1-i] + 0.05:
                            n += 1
                        else:
                            if df['close'][-i] >= df['low'][-i] + 0.05:
                                flag = True
                            else:
                                break
                    else:
                        if n > 0:
                            if df['close'][-i] >= df['low'][-i] + 0.05:
                                flag = True
                            elif df['close'][-i+1] >= df['low'][-i+1] + 0.05:
                                n = n -1
                                flag = True
                    if flag == True:
                        limit_up_dic[stock] = n
                        code_list.append(stock)
                        high_list.append(n)
                        name_list.append(str(get_security_info(stock).display_name))
                        break
            except:
                pass

        df_data = pd.DataFrame(columns = ["股票代码","股票名称","涨停高度"]) #创建一个空的dataframa
        df_data['股票代码'] = list(code_list)
        df_data.stock_name = name_list
        df_data.up_high = list(high_list)
        self.g_small_cyc = df_data['涨停高度'].max()
        df_data['涨停高度'] = np.where(df_data['涨停高度'] <  -1, self.g_small_cyc, df_data['涨停高度'])
        df_data_max = df_data[df_data['涨停高度'] == self.g_small_cyc]
        
        log.info('----- 新高股票 ----- ' + str(this_date))
        log.info(df_data_max)
    
        if self.g_small_cyc <= self.g_small_cyc_before:
            self.g_big_cyc = self.g_small_cyc_before
            
        elif self.g_small_cyc > self.g_big_cyc:
            self.g_big_cyc = self.g_small_cyc
            
        print('self.g_small_cyc1='+str(self.g_small_cyc)+', self.g_big_cyc1 = '+str(self.g_big_cyc)+', self.g_big_cyc_before_1 = '+str(self.g_big_cyc_before))      
        if (self.g_big_cyc <= 4 and self.g_small_cyc <= 4 and len(df_data_max) < 4):
            for stock in df_data_max['股票代码']:
                df = get_price(stock,end_date=this_date,frequency='1d',fields=['volume','open','close','high_limit','paused'] ,count=3)
                if df['open'][-1] != df['high_limit'][-1] or df['open'][-2] != df['high_limit'][-2]:
                    limit_stocks.append(stock)
                else:
                    print('一字板，删除'+str(stock))
        else:
            limit_stocks = {}
        
        self.g_security = limit_stocks
        self.g_small_cyc_before = self.g_small_cyc
        self.g_big_cyc_before_1 = self.g_big_cyc_before
        self.g_big_cyc_before = self.g_big_cyc
        print('self.g_small_cyc='+str(self.g_small_cyc)+', self.g_big_cyc = '+str(self.g_big_cyc)+', self.g_big_cyc_before = '+str(self.g_big_cyc_before))
        return limit_up_dic

    # 获得候选股票
    def pick_stocks(self, this_date):
        stocks = get_all_securities(types=['stock'], date=this_date).index  # 获得所有股票列表
        stocks = self.st_check(stocks)  # 过滤st股
        stocks = self.filter_by_listed_days(stocks, this_date, 100)  # 过滤上市天数不足的股票
        
        df = get_price(stocks, end_date=this_date, frequency='1d', fields=['open','close','high','high_limit','paused'], count=1).iloc[:,0]
        df = df[df['paused']==0]  # 过滤暂停的
        df = df[df['close']>2.2]  # 过滤收盘价过低的
        stocks = df[df['close']==df['high_limit']].index.tolist()  # 取收盘价为涨停价的
        return stocks
        
    def get_call_auction_data(self, jq_code, date):
        start = date + self.start_times
        end = date + self.end_times
        t = get_ticks(jq_code,end,start,None,['time','current','a1_p','a1_v'],skip=False)
        df = pd.DataFrame(t)
        df.time = pd.to_datetime(df.time.astype(int).astype(str))
        df.set_index('time', inplace=True)
        df.current[df.current==0] = df.a1_p[df.current==0]
        return df
    
    def run_daily_call_auction(self, context):
        log.info('函数运行时间(Call_auction)：' + str(context.current_dt.time()))
        
        self.g_security_del = []
        if len(self.g_security) == 0:
            return
        
        date = context.current_dt.strftime("%Y-%m-%d")
        buy_list_temp = []
        for jq_code in self.g_security:
            df = self.get_call_auction_data(jq_code, date)
            if len(df) > 0:
                a1_p_max = df['a1_p'].max()
                a1_p_min = df['a1_p'].min()
                a1_p0 = df['a1_p'][-1]
                a1_p1 = df['a1_p'][-2]
                a1_p2 = df['a1_p'][-3]
                print(jq_code+' '+str(a1_p_max)+' '+str(a1_p_min)+' '+str(a1_p1)+' '+str(a1_p0))
                    # 获取股票的收盘价
                close_data_1d = get_bars(jq_code, count=3, unit='1d', fields=['open','high','low','close'])
                close_1 = close_data_1d['close'][-1]
                close_3 = close_data_1d['close'][-3]
                print('jq_code = ' + str(jq_code) + '  close_3 = '+str(close_3))
                if a1_p0 / close_3 < 1.5 and a1_p0 > 3.3 and a1_p0 / close_1 > 0.95:
                    if a1_p0 < a1_p1 or a1_p1 < a1_p2:
                        self.g_security_del.append(jq_code)
                    else:
                        buy_list_temp.append(jq_code)
        self.g_security = buy_list_temp
        
    def before_trading_start(self, context):
        #log.info('函数运行时间 before_trading_start：' + str(context.current_dt.time()))
        #log.info('--> clear stocks_bought_today')
        self.stocks_bought_today = []

    def after_trading_end(self, context):
        #log.info('函数运行时间 after_trading_end: ' + str(context.current_dt.time()))
        # 得到当天所有成交记录
        #trades = get_trades()
        #for _trade in trades.values():
        #    log.info('成交记录：'+str(_trade))
        # 获得候选股票
        stock_list = self.pick_stocks(context.current_dt.strftime("%Y-%m-%d"))
        # 统计今天涨停股票在过去9天内的连扳情况
        bellwether = self.statistical_limit_up(stock_list, context.current_dt)
        # send_message('美好的一天~')

    def __str__(self):
        return self.memo

class PickByValueInvestment (FilterStockList):
    """价值投资大盘风控选股
    （源自：01-价值投资-大盘择时-Clone）"""

    # 克隆自聚宽文章：https://www.joinquant.com/post/29173
    # 标题：价值投资策略-大盘择时
    # 作者：叶松

    '''
    # 标题：收益狂飙，年化收益100%，11年1700倍，绝无未来函数
    # 作者：jqz1226

    之前版本的价值策略在2020年表现不尽如人意，所以，针对最近两年做了一点优化。
    选股删除了市值因子，增加了大盘择时。
    
    选股周期：一个月（每月第一个交易日）。换仓周期：一周（每周一）。
    止盈止损：每周三。
    止损：5日线回撤10%
    止盈：30% (orig 35%)

    zzz - alpha中等，回撤较低，收益较稳
    '''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.buy_count = params.get('buy_count', 5)

        self.STOP_PROFIT = 0.30  # 止盈点
        self.STOP_LOSS = -0.10  # 止损点

        self.month_now = None  # 当前月份
        self.week_now = None  # 当前第几周
        self.week_day = None  # 当前周第几天

        self.stock_list = []  # 选定买入股票
        self.idct_market_risk = IndicatorMarketRisk('000300.XSHG')

    def update_params(self, context, params):
        self.buy_count = params.get('buy_count', self.buy_count)

    def filter(self, context, data, stock_list):
        stocks_to_buy = []
        stocks_to_sell = []

        # 每月第一个交易日重选股票
        if context.current_dt.month != self.month_now:
            self.month_now = context.current_dt.month  # 更新当前月标记
            # 获取满足条件的股票列表
            self.stock_list = self.get_stock_list(context)
            # 过滤: 三停（停牌、涨停、跌停）及st,*st,退市
            check_out_lists = self.stock_list
            check_out_lists = self.filter_st_stock(check_out_lists)
            check_out_lists = self.filter_limitup_stock(context, check_out_lists)
            check_out_lists = self.filter_paused_stock(check_out_lists)
            # 排序选取
            self.stock_list = self.get_check_stocks_sort(context, check_out_lists)
            self.log.info('选股列表：%s' % self.stock_list)

        hour = context.current_dt.hour
        minute = context.current_dt.minute

        if hour == 9 and minute == 31:
            _y, _w, _d = context.current_dt.isocalendar()  # 某年、第几周、该周第几天
            #print('--> %d年第%d周 本周第%d个交易日' % (_y, _w, _d))
            if self.week_now != _w:  # 新的一周开始
                self.week_now = _w
                self.week_day = 1
            else:
                self.week_day += 1
        
        # 每周第三个交易日开盘时止盈止损
        if self.week_day == 3 and hour == 9 and minute == 35:
            current_data = get_current_data()
            for stock in self.holding_stocks:
                closeable_amount= context.portfolio.positions[stock].closeable_amount
                if closeable_amount:
                    close_data = attribute_history(stock, 5, '1d', ['close'])
                    e_5 = (close_data['close'][-1] - close_data['close'][0]) / close_data['close'][0]
                    earn = (current_data[stock].last_price - context.portfolio.positions[stock].avg_cost) / context.portfolio.positions[stock].avg_cost
                    if earn > self.STOP_PROFIT:  # 止盈
                        stocks_to_sell.append(stock)
                        log.info('止赢：%s %s %.2f' % (current_data[stock].name, stock, earn))
                    if e_5 < self.STOP_LOSS:  # 止损
                        stocks_to_sell.append(stock)
                        log.info('止损：%s %s %.2f' % (current_data[stock].name, stock, earn))
                    
        # 每周第一个交易日换仓（14:40 生成买入列表）
        if self.week_day == 1 and hour == 14 and minute == 40:
            # 先卖出不在买入列表中的股票
            for stock in self.holding_stocks:
                if stock not in self.stock_list:
                    stocks_to_sell.append(stock)
            # 再买入当前情况符合条件的待买入股票
            if self.idct_market_risk.risk_is_low(context, data):
                for stock in self.stock_list:
                    close_data = attribute_history(stock, 5, '1d', ['close'])
                    e_5 = (close_data['close'][-1] - close_data['close'][0]) / close_data['close'][0]
                    if not self.short_ma_under_long_ma(stock) and not e_5 < -0.1:
                        # 短期均线不低于长期均线，且近5日跌幅不超过10%
                        stocks_to_buy.append(stock)
            else:
                self.log.info('大盘风控未达标，取消买入')

        if len(stocks_to_sell) or len(stocks_to_buy):
            #print('--> self.holding_stocks: %s, stock_list: %s, self.stock_list:%s' % (self.holding_stocks, stock_list, self.stock_list))
            #print('--> stocks_to_sell: %s, stocks_to_buy: %s' % (stocks_to_sell, stocks_to_buy))
            #stock_list = list(set(stock_list) - stocks_to_sell | stocks_to_buy)[:self.buy_count]
            # 上面的写法可能会改变顺序，所以改为如下写法：
            stock_list = [i for i in stock_list if i not in stocks_to_sell]
            stock_list += [i for i in stocks_to_buy if i not in stock_list]
            stock_list = stock_list[:self.buy_count]

            self.holding_stocks = stock_list

            self._update_adjust_ratio(context, data, stock_list, len(stock_list))
            self._update_adjust_info(context, data, stock_list)
        
        if len(stock_list) > 0: self.log.info('选定买入股：%s' % stock_list)
        return stock_list

    def before_trading_start(self, context):
        pass
        
    def after_trading_end(self, context):
        pass

    def get_stock_list(self, context):
        temp_list = list(get_all_securities(types=['stock']).index)    
        # 剔除停牌股
        current_data = get_current_data()
        temp_list = [stock for stock in temp_list if not current_data[stock].paused]
        # 获取多期财务数据
        panel = self.get_data(context, temp_list, 4)
        
        # 1.总市值≧市场平均值*1.0。
        df_mkt = panel.loc[['circulating_market_cap'],3,:]
        df_mkt = df_mkt[df_mkt['circulating_market_cap']>df_mkt['circulating_market_cap'].mean()*1.1]
        l1 = set(df_mkt.index)
        
        # 2.最近一季流动比率≧市场平均值（流动资产合计/流动负债合计）。
        df_cr = panel.loc[['total_current_assets','total_current_liability'],3,:]
        # 替换零的数值
        df_cr = df_cr[df_cr['total_current_liability']!=0]
        df_cr['cr'] = df_cr['total_current_assets']/df_cr['total_current_liability']
        df_cr_temp = df_cr[df_cr['cr']>df_cr['cr'].mean()]
        l2 = set(df_cr_temp.index)

        # 3.近四季股东权益报酬率（roe）≧市场平均值。
        l3 = {}
        for i in range(4):
            roe_mean = panel.loc['roe',i,:].mean()
            df_3 = panel.iloc[:,i,:]
            df_temp_3 = df_3[df_3['roe']>roe_mean]
            if i == 0:    
                l3 = set(df_temp_3.index)
            else:
                l_temp = df_temp_3.index
                l3 = l3 & set(l_temp)
        l3 = set(l3)

        # 4.近3年自由现金流量均为正值。（cash_flow.net_operate_cash_flow - cash_flow.net_invest_cash_flow）
        y = context.current_dt.year
        l4 = {}
        for i in range(1,4):
            log.info('year', str(y-i))
            df = get_fundamentals(query(cash_flow.code, cash_flow.statDate, cash_flow.net_operate_cash_flow, \
                                        cash_flow.net_invest_cash_flow), statDate=str(y-i))
            if len(df) != 0:
                df['FCF'] = df['net_operate_cash_flow']-df['net_invest_cash_flow']
                df = df[df['FCF']>1000000]
                l_temp = df['code'].values
                if len(l4) != 0:
                    l4 = set(l4) & set(l_temp)
                l4 = l_temp
            else:
                continue
        l4 = set(l4)
        
        # 5.近四季营收成长率介于6%至30%（）。 'IRYOY':indicator.inc_revenue_year_on_year, # 营业收入同比增长率(%)
        l5 = {}
        for i in range(4):
            df_5 = panel.iloc[:,i,:]
            df_temp_5 = df_5[(df_5['inc_revenue_year_on_year']>15) & (df_5['inc_revenue_year_on_year']<50)]
            if i == 0:    
                l5 = set(df_temp_5.index)
            else:
                l_temp = df_temp_5.index
                l5 = l5 & set(l_temp)
        l5 = set(l5)
        
        # 6.近四季盈余成长率介于8%至50%。(eps比值)
        l6 = {}
        for i in range(4):
            df_6 = panel.iloc[:,i,:]
            df_temp = df_6[(df_6['eps']>0.08) & (df_6['eps']<0.5)]
            if i == 0:    
                l6 = set(df_temp.index)
            else:
                l_temp = df_temp.index
                l6 = l6 & set(l_temp)
        l6 = set(l6)
        
        return list(l2 & l3 & l4 & l5 & l6)
        
    # 过滤停牌股票
    def filter_paused_stock(self, stock_list):
        current_data = get_current_data()
        return [stock for stock in stock_list if not current_data[stock].paused]

    # 过滤ST及其他具有退市标签的股票
    def filter_st_stock(self, stock_list):
        current_data = get_current_data()
        return [
            stock for stock in stock_list if not current_data[stock].is_st and 'ST'
            not in current_data[stock].name and '*' not in current_data[stock].name
            and '退' not in current_data[stock].name
        ]

    # 过滤涨停/跌停的股票
    def filter_limitup_stock(self, context, stock_list):
        last_prices = history(1,
                        unit='1m',
                        field='close',
                        security_list=stock_list)
        current_data = get_current_data()

        # 已存在于持仓的股票即使涨停也不过滤，避免此股票再次可买，但因被过滤而导致选择别的股票
        return [
            stock for stock in stock_list
            if stock in list(context.portfolio.positions.keys())
            or last_prices[stock][-1] <= current_data[stock].high_limit
            or last_prices[stock][-1] >= current_data[stock].low_limit
        ]

    def get_check_stocks_sort(self, context,check_out_lists):
        df = get_fundamentals(query(valuation.circulating_cap, valuation.pe_ratio, valuation.code
                            ).filter(valuation.code.in_(check_out_lists)), date=context.previous_date)
        # 从大到小排序
        df = df.sort_values('circulating_cap', ascending=False)
        out_lists = list(df['code'].values)
        return out_lists
    
    #去极值（分位数法）
    def winsorize(self, se):
        q = se.quantile([0.025, 0.975])
        if isinstance(q, pd.Series) and len(q) == 2:
            se[se < q.iloc[0]] = q.iloc[0]
            se[se > q.iloc[1]] = q.iloc[1]
        return se

    #获取多期财务数据内容
    def get_data(self, context, pool, periods):
        q = query(valuation.code, income.statDate, income.pubDate).filter(valuation.code.in_(pool))
        df = get_fundamentals(q)
        df.index = df.code
        stat_dates = set(df.statDate)
        stat_date_stocks = {sd:[stock for stock in df.index if df['statDate'][stock]==sd] \
                for sd in stat_dates}

        def quarter_push(quarter):
            if quarter[-1]!='1':
                return quarter[:-1]+str(int(quarter[-1])-1)
            else:
                return str(int(quarter[:4])-1)+'q4'

        q = query(valuation.code, valuation.code, valuation.circulating_market_cap,
                    balance.total_current_assets, balance.total_current_liability, \
                    indicator.roe, cash_flow.net_operate_cash_flow, cash_flow.net_invest_cash_flow, \
                    indicator.inc_revenue_year_on_year, indicator.eps)

        stat_date_panels = {sd:None for sd in stat_dates}

        for sd in stat_dates:
            quarters = [sd[:4]+'q'+str(int(int(sd[5:7]) / 3))]
            for i in range(periods-1):
                quarters.append(quarter_push(quarters[-1]))
            nq = q.filter(valuation.code.in_(stat_date_stocks[sd]))
            
            quarters.reverse()
            pre_panel = { quarter:get_fundamentals(nq, statDate=quarter) for quarter in quarters }
            for thing in list(pre_panel.values()):
                thing.index = thing.code.values
            panel = pd.Panel(pre_panel)
            panel.items = list(range(len(quarters)))
            stat_date_panels[sd] = panel.transpose(2,0,1)

        final = pd.concat(list(stat_date_panels.values()), axis=2)
        final = final.dropna(axis=2)
        return final
        
    def short_ma_under_long_ma(self, security):
        """短期均线是否在长期均线之下"""
        close_data = attribute_history(security, 5, '1d', ['close'])
        MA5 = close_data['close'].mean()
        close_data = attribute_history(security, 10, '1d', ['close'])
        MA10 = close_data['close'].mean()
        close_data = attribute_history(security, 15, '1d', ['close'])
        MA20 = close_data['close'].mean()
        close_data = attribute_history(security, 25, '1d', ['close'])
        MA30 = close_data['close'].mean()
        if MA5 < MA20 and MA10 < MA30:  #and MA20>MA30 :
            return True
        return False

    def __str__(self):
        return '价值投资大盘风控选股 [买入数量：%s]' % self.buy_count
    
class PickByROE2(FilterStockList):
    """ROE优化选股
    （源自：01-ROE去杠杆优化-沪深300-Clone）"""

    '''
    # 克隆自聚宽文章：https://www.joinquant.com/post/33185
    # 标题：优化的ROE选股
    # 作者：KaiXuan
    
    优化的ROE选股
    ROE: 净资产收益率作为重要的估值手段但是经常由于行业和个股特性的原因导
    致其判断具有很大的误差.

    本文使用资金杠杆对 ROE 进行优化使:
    *ROE_ = ROE / 资金杠杆
    资金杠杆 = 总资产/(总资产-总负债)

    现在根据 ROE_ 的大小每日选10只股票 发现选股效果异常优异。
    '''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.buy_count = params.get('buy_count', 10)

        self.stocks = []  # 选定买入股票

        # 止损后60天内不再买入
        self.IgnoreStocks = pd.Series()
        self.ignoreDays = 60
        self.lossRate = 0.8

    def update_params(self, context, params):
        self.buy_count = params.get('buy_count', self.buy_count)

    def filter(self, context, data, stock_list):
        stocks_to_buy = []
        stocks_to_sell = []

        #hour = context.current_dt.hour
        #minute = context.current_dt.minute

        # 1. 选择股票池
        factors = self.get_factor(context)
        # 负债比率小于50%、毛利率大于10%、pcf_ratio>0  
        factors = factors[
                        (factors['liability']<0.5) &  # 加上该过滤 收益降低
                        (factors['gross_profit_margin']>0) & 
                        (factors['pe_ratio']>0) & 
                        (factors['eps']>0)]
        print(factors.head(3))
        # selectFactor = ['liability','roe']
        factors = factors['roe'] * (1-factors['liability'])
        score = factors.sort_values(ascending = False)
        self.stocks = list(score.index[3:self.buy_count*2])  # zzz - 排除前三后收益确实更好
        
        # 2. 判断持仓止损
        positions_dict = {}
        for i in self.holding_stocks:
            if i in context.portfolio.positions:
                positions_dict[i] = context.portfolio.positions[i]
        for position in list(positions_dict.values()):
            s = position.security
            if position.price / position.avg_cost < self.lossRate:
                stocks_to_sell.append(s)
                self.IgnoreStocks[s] = 0
        
        # 更新止损黑名单日期计数
        self.IgnoreStocks = self.IgnoreStocks[self.IgnoreStocks<self.ignoreDays]
        self.IgnoreStocks += 1
        
        # 3. 卖出不在买入列表中的股票
        for stock in self.holding_stocks:
            if stock not in self.stocks:
                stocks_to_sell.append(stock)
        
        # 4. 买入符合条件的待买入股票
        for stock in self.stocks:
            if stock not in self.IgnoreStocks.index:
                stocks_to_buy.append(stock)

        # 5. 生成买入清单，更新持仓和调仓信息
        if len(stocks_to_sell) or len(stocks_to_buy):
            #print('--> self.holding_stocks: %s, stock_list: %s, self.stocks:%s' % (self.holding_stocks, stock_list, self.stocks))
            #print('--> stocks_to_sell: %s, stocks_to_buy: %s' % (stocks_to_sell, stocks_to_buy))
            #stock_list = list(set(stock_list) - stocks_to_sell | stocks_to_buy)[:self.buy_count]
            # 上面的写法可能会改变顺序，所以改为如下写法：
            stock_list = [i for i in stock_list if i not in stocks_to_sell]
            stock_list += [i for i in stocks_to_buy if i not in stock_list]
            stock_list = stock_list[:self.buy_count]

            self.holding_stocks = stock_list

            self._update_adjust_ratio(context, data, stock_list, len(stock_list))
            self._update_adjust_info(context, data, stock_list)
        
        if len(stock_list) > 0: self.log.info('选定买入股：%s' % stock_list)
        return stock_list

    def before_trading_start(self, context):
        pass
        
    def after_trading_end(self, context):
        pass

    def get_factor(self, context):
        d = context.previous_date
        # 获取当天的筛选后的成份股
        stocklist = self.get_stocks_filtered(d, "000300.XSHG")  # 沪深300 good
        #stocklist = get_stocks_filtered(d, "000852.XSHG")  # 中证1000 bad
        #stocklist = get_stocks_filtered(d, "000905.XSHG")  # 中证500 ok
        
        # 查询语句
        q = query(valuation.code,
                balance.total_liability/balance.total_assets,#负债率
                indicator.gross_profit_margin, # 毛利率
                valuation.turnover_ratio, 
                valuation.pe_ratio, 
                indicator.eps, 
                indicator.roe, 
                indicator.inc_net_profit_year_on_year   

        ).filter(valuation.code.in_(stocklist))
        # 获得当天因子数据
        factors = get_fundamentals(q,d).set_index('code')
        factors.columns = ['liability','gross_profit_margin','turnover_ratio','pe_ratio',
                            'eps','roe','inc_net_profit_year_on_year']
        return factors

    def get_stocks_filtered(self, tradedate, indexID='000300.XSHG'):
        """
        获取某一天筛选后的指数成份股
        :param tradedate: 指定某一天
        :param indexID:
        :return:
        """
        # 获取当天指数成份股列表
        stocklist = get_index_stocks(indexID, date=tradedate)
        # 判断当天是否是st,返回的是df
        is_st = get_extras('is_st', stocklist, end_date=tradedate, count=1).T
        # 判断当天是否全天停牌,返回的是df
        is_susp = get_price(stocklist,end_date=tradedate, count=1, fields='paused', panel=False).set_index('code')[['paused']]
        is_susp = is_susp == 1
        # 判断上市日期大于90天,大于返回False
        # 拼接前两个df,再新建一列is_short
        con_df = pd.concat([is_st, is_susp], axis=1)
        # 判断每行只要有True的就返回True,切取3个都是False的行(股票)
        stock_filtered = con_df[~con_df.any(axis=1)].index.tolist()
        return stock_filtered

    def __str__(self):
        return 'ROE优化选股 [买入数量：%s]' % self.buy_count

class PickByROX(FilterStockList):
    '''ROE/ROA/ROIC价值选股'''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.rox = params.get('rox', 'roe_opt')  # 使用因子 roe, roe_opt, roa
        self.cmcap_min = params.get('cmcap_min', 10)  # 最小流通市值，默认值：10 亿
        self.buy_count = params.get('buy_count', 10)  # 选股数量，默认值：10 只

        self.keep_count = self.buy_count * 2 if self.buy_count > 2 else 4
        self.daily_calc_done = False

    def update_params(self, context, params):
        self.cmcap_min = params.get('cmcap_min', self.cmcap_min)
        self.buy_count = params.get('buy_count', self.buy_count)

    def filter(self, context, data, stock_list):
        # 打分工具
        def f_sum(x):
            return sum(x)

        if not self.daily_calc_done:
            # 获取股票池
            q = query(valuation.code, 
                valuation.pb_ratio,
                valuation.circulating_market_cap,
                indicator.roa,
                indicator.roe,
                balance.total_liability/balance.total_assets, #负债率
            ).filter(indicator.code.in_(stock_list))
            df = get_fundamentals(q)
            df.index = df.code
            df = df.drop('code', axis=1)
            df.columns = ['pb_ratio', 'cmcap', 'roa', 'roe', 'liability']
            df['roe_opt'] = df['roe'] * (1 - df['liability'])
            
            # 根据pb、rox和市值做基本筛选
            df = df[(df[self.rox] > 0) & (df['pb_ratio'] > 0) & (
                df['cmcap'] > self.cmcap_min)].sort_values('pb_ratio')
            # 取rox倒数，获取综合得分
            df['1/rox'] = 1 / df[self.rox]
            df['point'] = df[['pb_ratio', '1/rox']].rank().T.apply(f_sum)
            # 按得分进行排序，取指定数量的股票
            df = df.sort_values('point')[:self.keep_count]

            to_keep_list = list(df.index)
            to_buy_list = list(df[:self.buy_count].index)
            self.log.info('总共选出 %d 只股票' % len(to_buy_list))

            self.daily_calc_done = True

        #stock_list = to_buy_list
        #self._update_adjust_ratio(context, data, to_buy_list, len(to_buy_list))
        #self._update_adjust_info(context, data, to_buy_list)
        # 改为与“外资重仓”选股一样的调仓方式，减少不必要的交易波动
        stock_list = self.get_stock_list_and_set_adjust(context, data, stock_list,
                            to_buy_list, to_keep_list, self.buy_count)
        
        if len(stock_list) > 0: self.log.info('选定买入股：%s' % stock_list)
        return stock_list

    def before_trading_start(self, context):
        self.daily_calc_done = False

    def after_trading_end(self, context):
        self.daily_calc_done = False

    def __str__(self):
        return 'ROX选股 [因子：%s 最小流通市值：%s 买入数量：%s]' % \
            (self.rox, self.cmcap_min, self.buy_count)

class PickByCointPairs(FilterStockList):
    """价格协整选股-多组配对"""

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.buy_count = params.get('buy_count', 6)
        self.inter = params.get('inter', 0.010)
        self.stocks_groups = params.get(
            'stocks_groups', {'inter': 0.010, 'stock_list': []})
        self.stock_list_all = []
        self.df_last = None
        self.stocks_bought_today = []  # 存储当天买入的股票列表

    def update_params(self, context, params):
        self.buy_count = params.get('buy_count', self.buy_count)
        self.inter = params.get('inter', self.inter)
        self.stocks_groups = params.get('stocks_groups', self.stocks_groups)

    def filter(self, context, data, stock_list):
        self.stock_list_all = []
        #if stock_list:
        #    self.stock_list_all += stock_list
        for i in self.stocks_groups:
            self.stock_list_all += i['stock_list']
        #log.debug('--> 传入的 stock_list：%s，最终 stock_list_all: %s' % 
        #        (stock_list, self.stock_list_all))

        # 配合市场温度检测，每天 09:33 之后才开始处理
        hour = context.current_dt.hour
        minute = context.current_dt.minute
        if hour == 9:
            #if minute == 31:
            #    print(f'--> data: {[data[i].close for i in data]}')
            if minute < 33:
                return stock_list  # 未到时间点，直接返回以前的选股
            elif minute == 33:  # 每天 09:33 更新获取昨日价格数据
                self._update_df_last()

        if self.df_last is None:  # 若盘中手动重启，则可能为 None
            self._update_df_last()
            
        buy_list = []

        ## 选择策略外参数中的候选股
        #if stock_list and len(stock_list) > 0:
        #    buy_stock = self.__select_stock_by_ratio(context, data, stock_list, self.inter)
        #    if buy_stock:
        #        buy_list.append(buy_stock)
        # 选择策略内参数中的候选股
        for i in self.stocks_groups:
            buy_stock = self.__select_stock_by_ratio(context, data, i['stock_list'], i['inter'])
            if buy_stock:
                buy_list.append(buy_stock)

        buy_list = buy_list[:self.buy_count]

        # 平均分配更新买入持仓股内部资金占比
        # 这里只需更新资金占比，调仓价格等信息已在选股时予以更新
        self._update_adjust_ratio(context, data, buy_list, self.buy_count)

        #if len(buy_list) > 0: self.log.info('选定买入股：%s' % buy_list)
        return buy_list

    def before_trading_start(self, context):
        self.stocks_bought_today = []
    
    def after_trading_end(self, context):
        self.stocks_bought_today = []
        for stock in self.stock_list_all:
            stock_amount = 0
            if stock in context.subportfolios[0].long_positions.keys():
                position = context.subportfolios[0].long_positions[stock]
                stock_amount = position.total_amount
            eval('record(X%s=%d)' % (stock[:6], stock_amount))

    def __select_stock_by_ratio(self, context, data, stock_pair, inter):
        # 如果该组股票当日已经有过买入，直接返回已买入股票
        for stock in stock_pair:
            if stock in self.stocks_bought_today:
                # self.log.info('该组股票中的 %s 已于今日买入过，故此忽略其他~' % stock)
                return stock

        buy_stock = None
        ratio = []

        # 计算候选股当前价格波动率
        for code in stock_pair:
            #print(f'--> {code} 当前股价：{data[code].close}，昨日收盘：{self.df_last[code][-1]}')
            try:
                ratio.append(data[code].close / self.df_last[code][-1])
            except Exception as e:
                #formatted_lines = traceback.format_exc().splitlines()
                #self.log.warn(formatted_lines)
                self.log.info('%s 数据未就绪，操作取消：%s' % (code, data[code]))
                return buy_stock
        #log.debug('--> ratio: %s' % ratio)

        if len(ratio) == 0:
            return buy_stock

        #log.debug('positions.keys: %s' % context.portfolio.positions.keys())

        need_buy = False

        # 如果当前有持仓，且存在其他配对股价格偏低超过阈值，则换仓
        # 如果当前没有持仓，且存在配对股价格波动差大于阈值，则开仓
        # 其他情况不动
        if len(set(context.portfolio.positions.keys()) & set(stock_pair)) > 0:
            for code in context.portfolio.positions.keys():
                if code in stock_pair:
                    index = stock_pair.index(code)
                    if ratio[index] - min(ratio) > inter:
                        self.log.info('出现换仓机会，卖出当前持仓 %s' % code)
                        need_buy = True
                    else:
                        # TODO 如果当前持仓，且价格偏低超过阈值，则平仓？为止损目的，会降低收益
                        # if 1.0 - ratio[index] > 0.050:
                        if False:
                            log.info(
                                '-- Filter_rotation: close. need sell %s.' % code)
                        else:
                            # 继续保留当前持有
                            buy_stock = code
        else:
            if max(ratio) - min(ratio) > inter:
                need_buy = True

        # 买入价格偏低超过阈值的股票
        if need_buy:
            # print('--> stock_pair: %s' % stock_pair)
            # print('--> ratio: %s' % ratio)
            _code_min = stock_pair[ratio.index(min(ratio))]
            _code_max = stock_pair[ratio.index(max(ratio))]
            if _code_min not in self.g.buy_stocks:  # 减少不必要的日志输出
                self.log.info('选定价格波动跌过阈值（%.3f）的股票 %s，最新价：%.3f' \
                    % (inter, _code_min, data[_code_min].close))
            buy_stock = _code_min

            # 记录调仓价格、调仓时间、是否只在当天同步
            self._update_adjust_info_single(context, data, _code_min)
            self._update_adjust_info_single(context, data, _code_max)
            
            self.g.dump_trading_status(context)

        #log.debug('--> buy_stock: %s' % buy_stock)
        self.stocks_bought_today.append(buy_stock)
        self.log.info('选定买入股：%s' % buy_stock)
        return buy_stock

    def _update_df_last(self):
        """获取所有候选股昨日收盘价"""
        
        self.df_last = history(1, unit='1d', field='close',
                                security_list=self.stock_list_all, df=False, skip_paused=True, fq='pre')
        self.log.info('昨日收盘价: %s' % self.df_last)
        
    def __str__(self):
        return '价格协整选股-多组配对: [选股数量: %d 波动阈值: %.3f，策略内配对股参数: %s]' \
            % (self.buy_count, self.inter, self.stocks_groups)

class PickByForeignCapital(FilterStockList):
    """外资重仓选股
    （源自：01-外资策略V0.1-Clone）"""

    '''
    简单的外资策略，年化41%

    策略思路：买入前50日外资净买入额最高的1只股票，如果之后一直在前4名，
    则一直持有，直到跌破前4，换入其他股票。策略收益和起始时间有关，首次
    买入的股票不同，之后的持仓会有差别。

    zzz - alpha较高，收益稳定，值得研究。17年后效果较强。回撤有点大，可加上rsrs择时再看看
    '''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.buy_count = params.get('buy_count', 1)

        # buy_count,keep_count 收益例：1,2=444%, 1,3=480%, 1,4=550%, 1,5=496%, 1,10=496%
        self.keep_count = self.buy_count * 2 if self.buy_count > 2 else 4  # 每次重选时，取Top的数量
        self.period_day = 50 # 取前N个交易日的外资交易数据
        self.period_day_short = 15  # 最近N个交易日数据
        self.all_trade_days = get_all_trade_days() # JQ函数，取所有交易日（从 2005-01-04 到现在）
        
    def update_params(self, context, params):
        self.buy_count = params.get('buy_count', self.buy_count)

    def filter(self, context, data, stock_list):
        stocks_to_buy = []
        stocks_to_sell = []

        #hour = context.current_dt.hour
        #minute = context.current_dt.minute
        #if not (hour == 9 and minute == 31):
        #    return stock_list

        # 根据原策略逻辑，指定买入列表1只，可持列表4只

        # 重新选择股票
        to_buy_list, to_keep_list = self.get_foreign_investment(context)
        
        stock_list = self.get_stock_list_and_set_adjust(context, data, stock_list,
                            to_buy_list, to_keep_list, self.buy_count)
        
        if len(stock_list) > 0: self.log.info('选定买入股：%s' % stock_list)
        return stock_list

    def before_trading_start(self, context):
        pass
        
    def after_trading_end(self, context):
        pass

    def get_foreign_investment(self, context):
        current_date = context.current_dt.date()
        
        self.to_buy = []
        cnt = -1
        for i in range(len(self.all_trade_days)-1, 0, -1):
            if self.all_trade_days[i] <= current_date:
                cnt += 1
            if cnt >= self.period_day:
                break
        
        period_date = self.all_trade_days[i]
        period_date_short = self.all_trade_days[i+self.period_day-self.period_day_short]
        #print("period_date={0}, i={1}".format(period_date, i))
        
        # 获取最近N个交易日买入最大的股票
        data_ori = finance.run_query(query(finance.STK_EL_TOP_ACTIVATE).filter( \
                    finance.STK_EL_TOP_ACTIVATE.link_id.in_(['310001', '310002']), \
                    finance.STK_EL_TOP_ACTIVATE.day >= period_date, \
                    finance.STK_EL_TOP_ACTIVATE.day < current_date).limit(1000))
        #print(data_ori)
        data_ori['absolute_buy'] = data_ori['buy'] - data_ori['sell']
        data_ori_statistics = data_ori.groupby(['code']).sum()
        #data_ori_statistics.sort_values(by=['absolute_buy'],ascending=False,inplace=True)
        data_ori_statistics.sort_values(['absolute_buy'], ascending=False, inplace=True)
        data_ori_statistics = data_ori_statistics.loc[data_ori_statistics["absolute_buy"] > 10].head()
        #log.info(data_ori_statistics)
        
        # 获取短期买入最大的股票
        data_short = finance.run_query(query(finance.STK_EL_TOP_ACTIVATE).filter( \
                    finance.STK_EL_TOP_ACTIVATE.link_id.in_(['310001', '310002']), \
                    finance.STK_EL_TOP_ACTIVATE.day >= period_date_short, \
                    finance.STK_EL_TOP_ACTIVATE.day < current_date).limit(1000))
        
        #print(data_ori)
        data_short['absolute_buy'] = data_short['buy'] - data_short['sell']
        data_short_statistics = data_short.groupby(['code']).sum()
        data_short_statistics.sort_values(['absolute_buy'], ascending=False, inplace=True)
        # log.info(data_short_statistics)
        
        data_short_statistics = data_short_statistics.loc[data_short_statistics["absolute_buy"] > -1000000000].head()
        # log.info("TOP-N: %s" % data_ori_statistics)
        
        # 方案1: 350%
        to_buy_list = list(data_ori_statistics.head(self.buy_count).index)
        #  还在前几名的不卖, 调整仓位？
        to_keep_list = list(data_ori_statistics.head(self.keep_count).index)
        
        '''
        # 方案2: 288% 不佳
        # 取近期和远期都有的股票
        long_code_list = list(data_ori_statistics.head(10).index)
        #print(long_code_list)
        short_code_list = list(data_short_statistics.head(100).index)
        #print(short_code_list)
        code = set(long_code_list) & set(short_code_list)
        #print(code)
        new_df = data_ori_statistics[data_ori_statistics.index.isin(code)]
        #print(new_df)
        to_buy_list = list(new_df.head(self.buy_count).index)
        # 还在前几名的不卖
        #to_keep_list = list(data_ori_statistics.head(self.keep_count).index)
        to_keep_list = self.to_buy
        '''

        return to_buy_list, to_keep_list

    def __str__(self):
        return '外资重仓选股 [买入数量：%s]' % self.buy_count

class PickByIkunPool(FilterStockList):
    """基金一哥复刻选股（易方达基金经理，量化选股思路复刻）
    （源自：01-量化张坤选股思路-大市值-Clone）"""

    '''
    # 克隆自聚宽文章：https://www.joinquant.com/post/33386
    # 标题：量化张坤选股思路
    # 作者：一燃
    
    近三年来，市场上最耀眼的公募基金经理，非张坤莫属。他也是第一个千亿公
    募基金经理。正好笔者有幸看到一篇讲述张坤投资经验的访谈报告。
    以下为原文地址，部分金句以及笔者简易的策略复现。

    1、原文地址见于雪球，强烈建议原文阅读：https://xueqiu.com/4585619621/177357673 
    2、金句赏析
    3、复现张坤量化思路
    （1）5年roic均值>0.1
    （2）自由现金流强劲、有息负债率低（不喜欢大量负债、重资产）
    （3）排除行业地位不高，上下游议价能力差，运营资本高------ #用毛利率
        高借代 行业地位高（可能不准确？？）， 净运营资本低
    持股8只，每月1号调仓，等分仓位，不择时。选股有大市值偏好（持仓稳定性）

    可能存在的风险：
    1、作为过去三年市场里最耀眼的星，按照张坤思路选出来的股票自然不会差，但
    这难免有“倒车镜”的未来函数嫌疑，不过五年roic硬规定以及对于重资产和自
    由现金流的深度理解又是其智慧体现。未来怎么走需要拭目以待。
    2、策略中为了稳定持仓，有大市值倾向，虽然不用担心容量问题，但是大市值风
    格持久性存疑。
    '''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.buy_count = params.get('buy_count', 8)
        self.index_list = params.get('index_list', ['000300.XSHG', '000905.XSHG'])  # '399006.XSHE'

        # buy_count,keep_count 收益例：1,2=444%, 1,3=480%, 1,4=550%, 1,5=496%, 1,10=496%
        self.keep_count = self.buy_count * 2 if self.buy_count > 2 else 4  # 每次重选时，取Top的数量
        
    def update_params(self, context, params):
        self.buy_count = params.get('buy_count', self.buy_count)
        self.index_list = params.get('index_list', self.index_list)

    def filter(self, context, data, stock_list):
        stocks_to_buy = []
        stocks_to_sell = []

        #hour = context.current_dt.hour
        #minute = context.current_dt.minute
        #if not (hour == 9 and minute == 31):
        #    return stock_list

        # 重新选择股票
        stocks = []
        for i in self.index_list:
            stocks += get_index_stocks(i)
        stocks = list(set(stocks))
        
        # 排除价格过高的股票
        stk_exclude = []
        value_avg = context.portfolio.total_value / self.buy_count
        for i in stocks:
            close_data = attribute_history(i, 3, '1d', ['close'])
            current_price = close_data['close'][-1]
            if value_avg / current_price < 300:
                stk_exclude.append(i)
        stocks = list(set(stocks) - set(stk_exclude))

        pool = self.ikun_pool(context, stocks)
        if pool is None:
            return stock_list
        to_buy_list = pool[:self.buy_count]
        to_keep_list = pool[:self.keep_count]
        
        # 平分资金调仓
        stock_list = to_buy_list
        self._update_adjust_ratio(context, data, to_buy_list, len(to_buy_list))
        self._update_adjust_info(context, data, to_buy_list)
        # 改为与“外资重仓”选股一样的调仓方式，减少不必要的交易波动（貌似不佳）
        #stock_list = self.get_stock_list_and_set_adjust(context, data, stock_list,
        #                    to_buy_list, to_keep_list, self.buy_count)
        
        if len(stock_list) > 0: self.log.info('选定买入股：%s' % stock_list)
        return stock_list

    def before_trading_start(self, context):
        pass
        
    def after_trading_end(self, context):
        pass

    def ikun_pool(self, context, stocks):
        from sklearn import preprocessing
        from jqfactor import get_factor_values

        #start_date = dt.datetime.today().date()-dt.timedelta(days=1)
        start_date = context.previous_date  # zzz 上述写法有未来，已修正
        stk2 = list(get_fundamentals(query(
                valuation.code,
            ).filter(
                valuation.code.in_(stocks),
                balance.equities_parent_company_owners > 0,
                balance.retained_profit > 0,
                valuation.pe_ratio > 0,
            ), date=start_date).code.values)
        
        def delete_st(stocks, begin_date):
            st_data = get_extras('is_st',stocks, count = 1,end_date=begin_date)
            stockList = [stock for stock in stocks if not st_data[stock][0]]
            return stockList
        
        def delete_stop(stocks, beginDate, n=365*3):
            stockList=[]
            
            beginDate = dt.datetime.strptime(beginDate, "%Y-%m-%d")
            for stock in stocks:
                start_date = get_security_info(stock).start_date
                if start_date<(beginDate-dt.timedelta(days=n)).date():
                    stockList.append(stock)
            return stockList
        
        def get_jq_factor(date, feasible_stocks, jqfactors_list):
            factor_data = get_factor_values(securities=feasible_stocks, 
                                factors=jqfactors_list, count=1, end_date=date)
            df_jq_factor = pd.DataFrame(index=feasible_stocks)
            
            for i in factor_data.keys():
                df_jq_factor[i] = factor_data[i].iloc[0,:]
            
            return df_jq_factor
        
        stk = delete_st(stk2, start_date)
        #TODO 删除价格过高的股票
        
        one_year_ago = start_date - dt.timedelta(days=365)
        two_year_ago = start_date - dt.timedelta(days=365*2)
        three_year_ago = start_date - dt.timedelta(days=365*3)
        four_year_ago = start_date - dt.timedelta(days=365*4)
        
        # 1、5年roic均值>0.1
        ikun_1 = pd.DataFrame(index=stk)
        ikun_1['roic1'] = get_jq_factor(start_date,stk,'roic_ttm').dropna()
        ikun_1['roic2'] = get_jq_factor(one_year_ago,stk,'roic_ttm').dropna()
        ikun_1['roic3'] = get_jq_factor(two_year_ago,stk,'roic_ttm').dropna()
        ikun_1['roic4'] = get_jq_factor(three_year_ago,stk,'roic_ttm').dropna()
        ikun_1['roic5'] = get_jq_factor(four_year_ago,stk,'roic_ttm').dropna()
        ikun_1 = ikun_1.dropna()
        ikun_1['roic_mean'] = ikun_1.mean(axis=1)
        ikun_11 = list(ikun_1[ikun_1.roic_mean>=0.1].index)
        
        #print('--> len(ikun_11): %d' % len(ikun_11))
        if len(ikun_11) == 0:
            return None
            
        #TODO 2、自由现金流强劲、有息负债率低（不喜欢大量负债、重资产）
        # free cash flow/total__assets高  非流动资产/总资产低，有息负债率低
        
        # 3、排除行业地位不高，上下游议价能力差，运营资本高
        #用毛利率高借代 行业地位高（可能不准确？？）， 净运营资本低高
        ikun_f = get_history_fundamentals(
            ikun_11,
            [income.operating_revenue,
             income.operating_cost,
             balance.total_current_assets,
             balance.total_current_liability,
             cash_flow.net_operate_cash_flow,
             cash_flow.fix_intan_other_asset_acqui_cash,
             balance.total_assets,
             balance.total_non_current_assets,
            ],
            watch_date=start_date, count=4).dropna()  # 连续的6个季度
        ikun_f['fcf'] = ikun_f['net_operate_cash_flow']-ikun_f['fix_intan_other_asset_acqui_cash']
        
        ikun_total_current_assets = ikun_f.groupby('code')['total_current_assets'].mean()
        ikun_total_current_liability = ikun_f.groupby('code')['total_current_liability'].mean()
        
        ikun_operating_revenue = ikun_f.groupby('code')['operating_revenue'].sum()
        ikun_operating_cost = ikun_f.groupby('code')['operating_cost'].sum()
        
        ikun_total_assets = ikun_f.groupby('code')['total_assets'].mean()
        ikun_total_non_current_assets = ikun_f.groupby('code')['total_non_current_assets'].mean()
        
        ikun_fcf = ikun_f.groupby('code')['fcf'].sum()
        
        ikun_2 = pd.DataFrame(index=ikun_11)
        ikun_2['fcfsum'] = ikun_fcf / ikun_total_assets
        ikun_2['f_l'] = get_jq_factor(start_date, ikun_11, 'financial_liability').dropna()
        ikun_2['a'] = ikun_total_assets
        ikun_2['d_a'] = -ikun_2['f_l'] / ikun_2['a']
        del ikun_2['a']
        del ikun_2['f_l']
        ikun_2['fix_asset'] = -ikun_total_non_current_assets/ikun_total_assets
        
        ikun_3 = pd.DataFrame(index=ikun_11)
        ikun_3['gpm'] = (ikun_operating_revenue-ikun_operating_cost) / ikun_operating_revenue
        ikun_3['net working capital'] = (ikun_total_current_liability - \
            ikun_total_current_assets) / ikun_total_assets
        
        ikun_2 = ikun_2.dropna()
        ikun_2 = ikun_2.rank(ascending=True)
        ikun_2_v = np.sum(preprocessing.scale(ikun_2), axis=1)
        ikun_2_v1 = preprocessing.scale(ikun_2_v )
        ikun_2_i = ikun_2.index.tolist()
        ikun_end2 = pd.DataFrame({'code':ikun_2_i, 'ikun2':ikun_2_v1}).set_index('code')

        ikun_3 = ikun_3.dropna()
        ikun_3 = ikun_3.rank(ascending=True)
        ikun_3_v = np.sum(preprocessing.scale(ikun_3), axis=1)
        ikun_3_v1 = preprocessing.scale(ikun_3_v )
        ikun_3_i = ikun_3.index.tolist()
        ikun_end3 = pd.DataFrame({'code':ikun_3_i, 'ikun3':ikun_3_v1}).set_index('code')

        ikun_score = pd.merge(ikun_end3, ikun_end2, on=['code'])    
        ikun_score_v = np.sum(ikun_score.dropna(), axis=1)
        ikun_score_v1 = preprocessing.scale(ikun_score_v)
        ikun_score_i = ikun_score.dropna().index.tolist()
        ikun_score1 = pd.DataFrame({'code':ikun_score_i, 'score':ikun_score_v1}).set_index('code')    
        ikun_score1 = ikun_score1.sort_values(by='score', ascending=False).head(3*self.buy_count) 
        
        def get_pb(x):
            q = query( 
                valuation.pb_ratio,
            ).filter(
                valuation.code == x
            )
            return get_fundamentals(q, start_date)['pb_ratio'].item()
        
        def get_marketcap(x):
            q = query( 
                valuation.market_cap,
            ).filter(
                valuation.code == x
            )
            return get_fundamentals(q, start_date)['market_cap'].item()
        
        ikun_score1['value'] = ikun_score1.index.map(get_marketcap)
        result = list(ikun_score1.sort_values(by='value', ascending=False).index) 

        return result 
        
    def __str__(self):
        return '基金一哥复刻选股 [买入数量：%s 所用指数股列表：%s]' % (self.buy_count, self.index_list)

class PickByLongtouSmallCap(FilterStockList):
    """中小板龙头股选股+RSIX择时
    （源自：01-中小板龙头股-RSIX择时-Clone-运行较慢）
    """

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.index_stock = params.get('index_stock', '000001.XSHG')
        self.buy_count = params.get('buy_count', 3)
        #self.keep_count = self.buy_count * 2 if self.buy_count > 2 else 4  # 每次重选时，取Top的数量
        
        self.month_now = None  # 当前月份
        self.buy_list_pool_yesterday = []  # 前一日买入股票池
        self.max_zt_days_yesterday = 1  # 前一日最大涨停天数
        self.total_is_run = 0  # 涨停数量或者最大涨停天数是否超过前一日
        self.stock_pool = None  # 每月重选的股票池
        
        self.idct_market_risk = IndicatorMarketRisk(self.index_stock)

    def update_params(self, context, params):
        self.index_stock = params.get('index_stock', self.index_stock)
        self.buy_count = params.get('buy_count', self.buy_count)

    def before_trading_start(self, context):
        # 每月第一个交易日重选股票
        if context.current_dt.month != self.month_now:
            self.month_now = context.current_dt.month
            self._get_stock_pool(context)
        
        self.today_bought_stocks = set()
        
        # 取20日涨停价，收盘价，最低价
        count = 20
        g_high_limit = history(count, '1d', 'high_limit', self.stock_pool)
        g_close = history(count, '1d', 'close', self.stock_pool)
        g_low = history(count, '1d', 'low', self.stock_pool)
        #g_x1 = history(1, '1d', 'high_limit', self.stock_pool)  # 用来存今日涨停价，后面会重新赋值
        #end_date = context.current_dt.date()  # 有未来?
        end_date = context.previous_date  # zzz 修正
        for i in self.stock_pool:
            stock_data = get_price(i, end_date=end_date, frequency='daily', \
                fields=['high_limit'], skip_paused=False, fq='pre', count=1)
            #g_x1[i][0] = stock_data['high_limit'][0]
        
        # 统计涨停天数
        stocks_zt = {}
        for security in self.stock_pool:
            #print('--> %s, g_close:%s, g_high_limit:%s' % (security, g_close[security], g_high_limit[security]))
            if(g_close[security][-1] == g_high_limit[security][-1] \
            and g_low[security][-1] < g_high_limit[security][-1]):
                stocks_zt[security] = 1  # 昨日非一字板涨停
        for security in stocks_zt:
            for i in range(0, count-1):
                if(g_close[security][count-2-i] == g_high_limit[security][count-2-i] \
                and g_low[security][count-2-i] < g_high_limit[security][count-2-i]):
                    stocks_zt[security] += 1  # 非一字连板天数
                else:
                    break
        self.max_zt_days = 0  # 涨停最多天数
        for key in stocks_zt:
            if stocks_zt[key] > self.max_zt_days:
                self.max_zt_days = stocks_zt[key]
        
        self.buy_list_pool = []
        for key in stocks_zt:
            if(stocks_zt[key] == self.max_zt_days):
                self.buy_list_pool.append(key)
        if len(self.buy_list_pool):
            #log.info('___' * 10)
            self.log.info('最高板：%d天 股票池：%d只 %s' \
                % (self.max_zt_days, len(self.buy_list_pool), self.buy_list_pool))
        if len(self.buy_list_pool) >= len(self.buy_list_pool_yesterday) \
        or self.max_zt_days >= self.max_zt_days_yesterday:
            self.total_is_run = 1
        
        self.buy_list_pool_yesterday = self.buy_list_pool
        self.max_zt_days_yesterday = self.max_zt_days
        
    def filter(self, context, data, stock_list):
        stocks_to_buy = []
        stocks_to_sell = []

        hour = context.current_dt.hour
        minute = context.current_dt.minute

        # 若涨停数量或者最大涨停天数未超过前一日，则清空持仓
        if (hour == 9 and minute == 30) and self.total_is_run == 0:
            for stock in self.holding_stocks:
                closeable_amount = context.portfolio.positions[stock].closeable_amount
                if closeable_amount > 0:
                    stocks_to_sell.append(stock)
                    self.log.info('卖出 %s' % stock)

        # 卖出尾盘未涨停的股票
        if (hour == 14 and minute > 50):
            current = get_current_data()
            for stock in self.holding_stocks:
                closeable_amount = context.portfolio.positions[stock].closeable_amount
                if data[stock].close < current[stock].high_limit and closeable_amount > 0:
                    stocks_to_sell.append(stock)
                    self.log.info('卖出 %s' % stock)
        
        # 如果当天涨停未达标、或已达最大买入数量、或涨停板数量不超过3，则不再买入
        if self.total_is_run == 0 \
        or len(self.today_bought_stocks) >= self.buy_count \
        or not (self.fit_linear(context, 3) > 0 and self.max_zt_days > 3):
            pass
        
        else:
            # 买入龙头股
            current = get_current_data()
            if (hour < 11 and len(self.today_bought_stocks) < self.buy_count) \
            and self.DIF(context, '000001.XSHG')[-1] > self.DIF(context, '000001.XSHG')[-2] \
            and self.idct_market_risk.risk_is_low(context, data, ctype="rsix"):  # 冲天炮龙头
                for security in self.buy_list_pool:
                    #if((security in context.portfolio.positions)==0):  # 排除重复买 有bug？
                    if security not in self.holding_stocks:  # 排除重复买
                        stocks_to_buy.append(security)
                        self.log.info('买入 %s' % security)

        # 生成买入清单，更新持仓和调仓信息
        if len(stocks_to_sell) or len(stocks_to_buy):
            #print('--> self.holding_stocks: %s, stock_list: %s, self.stocks:%s' % (self.holding_stocks, stock_list, self.stocks))
            #print('--> stocks_to_sell: %s, stocks_to_buy: %s' % (stocks_to_sell, stocks_to_buy))
            #stock_list = list(set(stock_list) - stocks_to_sell | stocks_to_buy)[:self.buy_count]
            # 上面的写法可能会改变顺序，所以改为如下写法：
            stock_list = [i for i in stock_list if i not in stocks_to_sell]
            stock_list += [i for i in stocks_to_buy if i not in stock_list]
            stock_list = stock_list[:self.buy_count]

            self.holding_stocks = stock_list

            # 平均调仓
            self._update_adjust_ratio(context, data, stock_list, len(stock_list))
            self._update_adjust_info(context, data, stock_list)
        
        #if len(stock_list) > 0: self.log.info('选定买入股：%s' % stock_list)
        return stock_list

    def after_trading_end(self, context):
        pass

    def fit_linear(self, context, count):
        """直线拟合
        count:拟合天数
        """
        from sklearn.linear_model import LinearRegression
        security = self.index_stock
        df = history(count=count, unit='1d', field='close', security_list=security, \
            df=True, skip_paused=False, fq='pre')
        model = LinearRegression()
        x_train = np.arange(0, len(df[security])).reshape(-1, 1)
        y_train = df[security].values.reshape(-1, 1)
        # print(x_train,y_train)
        
        model.fit(x_train, y_train)
        
        # 计算出拟合的最小二乘法方程
        # y = mx + c 
        c = model.intercept_
        m = model.coef_
        c1 = round(float(c), 2)
        m1 = round(float(m), 2)
        # print("最小二乘法方程 : y = {} + {}x".format(c1,m1))
        return m1

    def _get_stock_pool(self, context):
        self.stock_pool = get_industry_stocks('HY001') + get_industry_stocks('HY002') \
            + get_industry_stocks('HY003') + get_industry_stocks('HY004') \
            + get_industry_stocks('HY005') + get_industry_stocks('HY006') \
            + get_industry_stocks('HY007') + get_industry_stocks('HY008') \
            + get_industry_stocks('HY009') + get_industry_stocks('HY010') \
            + get_industry_stocks('HY011')
        self.stock_pool = list(set(self.stock_pool))
        # 当天上市的所有股票，过滤ST等
        self.stock_pool = FuncLib.filter_special(context, self.stock_pool)
        
    def DIF(self, context, stock):
        # 原策略所用方法，效果也不错
        h = get_price(stock, count=100, end_date=context.previous_date, frequency='daily', fields=['close'], skip_paused=False, fq='pre')
        close = np.array(h['close'])
        macd_tmp = talib.MACD(close, fastperiod=7, slowperiod=28, signalperiod=5) 
        DIF = macd_tmp[0]

        # 使用自定义函数替代，效果不如上面
        #_dif, _dea, _macd = FuncLib.myMACD(context, stock, fastperiod=7, slowperiod=28, 
        #                                    signalperiod=5, enable_minute_data=True)
        #DIF = _dif
        
        return DIF

    def __str__(self):
        return '外资重仓选股 [买入数量：%s]' % self.buy_count
    

### 滤股类 ###
# FilterByXXX(FilterStockList)

class FilterByCommon(FilterStockList):
    """过滤ST、涨停、跌停或停牌的股票，可配置"""
    
    def __init__(self, params):
        FilterStockList.__init__(self, params)

        # 过滤类型，可选值：'st', 'high_limit', 'low_limit', 'pause', 'kechuang', 'chuangye', 'zhongxiao'
        self.filters = params.get(
            'filters', ['st', 'high_limit', 'low_limit', 'pause', 'kechuang', 'chuangye'])

    def update_params(self, context, params):
        self.filters = params.get('filters', self.filters)

    def filter(self, context, data, stock_list):
        #TODO 聚宽上有加速优化版
        current_data = get_current_data()
        if 'st' in self.filters:
            stock_list = [stock for stock in stock_list if
                          not current_data[stock].is_st
                          and 'ST' not in current_data[stock].name
                          and '*' not in current_data[stock].name
                          and '退' not in current_data[stock].name]
        if 'high_limit' in self.filters:  # 涨停
            stock_list = [stock for stock in stock_list if stock in context.portfolio.positions.keys()
                          or data[stock].close < data[stock].high_limit]
        if 'low_limit' in self.filters:   # 跌停
            stock_list = [stock for stock in stock_list if stock in context.portfolio.positions.keys()
                          or data[stock].close > data[stock].low_limit]
        if 'pause' in self.filters:  # 停牌
            stock_list = [
                stock for stock in stock_list if not current_data[stock].paused
                          and "退" not in current_data[stock].name
                          and "N" not in current_data[stock].name
                          and "C" not in current_data[stock].name]
            # 以及剔除过去180天停牌时间超过1/3的股票
            def fun_del_pauses(stock_list):
                __stock_list = []
                for stock in stock_list:
                    pau = attribute_history(stock, 180, unit='1d', fields=['paused'], skip_paused=False, df=True,
                                            fq='pre')
                    pau_days = len(pau[pau.paused == 1])
                    if pau_days / len(pau) < 0.3:
                        __stock_list.append(stock)
                return __stock_list
            stock_list = fun_del_pauses(stock_list)
        if 'kechuang' in self.filters:   # 科创板
            stock_list = [stock for stock in stock_list if not stock.startswith('688')]
        if 'chuangye' in self.filters:   # 创业板
            stock_list = [stock for stock in stock_list if not stock.startswith('300')]
        if 'zhongxiao' in self.filters:   # 中小板
            stock_list = [stock for stock in stock_list if not stock.startswith('002')]

        self.stock_list_info(stock_list)
        return stock_list

    def __str__(self):
        return '一般性股票过滤：%s' % (self.filters)

class FilterByDays(FilterStockList):
    """过滤上市天数"""

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.public_days_min = params.get('public_days_min', 63)
        self.public_days_max = params.get('public_days_max', 36500)

    def update_params(self, context, params):
        self.public_days_min = params.get(
            'public_days_min', self.public_days_min)
        self.public_days_max = params.get(
            'public_days_max', self.public_days_max)

    def filter(self, context, data, stock_list):
        tmpList = []
        for stock in stock_list:
            days_public = (context.current_dt.date() -
                           get_security_info(stock).start_date).days
            # 上市未超过1年
            if days_public >= self.public_days_min and days_public <= self.public_days_max:
                tmpList.append(stock)
        return tmpList

    def __str__(self):
        return '过滤上市天数 [最少: %d，最大: %d]' % (self.public_days_min, self.public_days_max)

class FilterByExcludeSZ(FilterStockList):
    '''排除深证'''

    def filter(self, context, data, stock_list):
        return [stock for stock in stock_list if stock[0:1] != '0']

    def __str__(self):
        return '排除深证股票'

class FilterByExcludeSH(FilterStockList):
    '''排除上证'''

    def filter(self, context, data, stock_list):
        return [stock for stock in stock_list if stock[0:1] != '6']

    def __str__(self):
        return '排除上证股票'

class FilterByBuyCount(FilterStockList):
    """根据待购数量截取股票列表"""

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.buy_count = params.get('buy_count', 3)

    def update_params(self, context, params):
        self.buy_count = params.get('buy_count', self.buy_count)

    def filter(self, context, data, stock_list):
        if len(stock_list) > self.buy_count:
            stock_list = stock_list[:self.buy_count]
        
        # 这里就不要调用 _update_adjust_ratio 和 _update_adjust_info_all 更新调仓信息了
        return stock_list

    def __str__(self):
        return '%s [ 选取只数: %d ]' % (self.memo, self.buy_count)

class FilterByIndicatorJQSingle(FilterStockList):
    """根据单个聚宽因子对股票列表进行过滤"""

    def filter(self, context, data, stock_list):
        q = query(valuation).filter(
            valuation.code.in_(stock_list)
        )
        
        factor = eval(self._params.get('factor', None))
        min = self._params.get('min', None)
        max = self._params.get('max', None)

        if factor is not None:
            return stock_list
        if min is None and max is None:
            return stock_list
        if min is not None:
            q = q.filter(
                factor > min
            )
        if max is not None:
            q = q.filter(
                factor < max
            )
        
        stock_list = list(get_fundamentals(q)['code'])
        
        self._update_adjust_ratio(context, data, stock_list, len(stock_list))
        self._update_adjust_info(context, data, stock_list)
            
        return stock_list

    def __str__(self):
        factor = self._params.get('factor', None)
        min = self._params.get('min', None)
        max = self._params.get('max', None)
        s = self.memo + ':'
        if min is not None and max is not None:
            s += ' [ %s < %s < %s ]' % (min, factor, max)
        elif min is not None:
            s += ' [ %s < %s ]' % (min, factor)
        elif max is not None:
            s += ' [ %s > %s ]' % (factor, max)
        else:
            s += '参数错误'
        return s

class FilterByIndicatorJQ(FilterStockList):
    """根据多个聚宽因子对股票列表进行过滤"""

    def filter(self, context, data, stock_list):
        q = query(valuation, balance, cash_flow, income, indicator).filter(
            valuation.code.in_(stock_list)
        )

        for fd_param in self._params.get('factors', []):
            if not isinstance(fd_param, FD_Factor):
                continue
            if fd_param.min is None and fd_param.max is None:
                continue
            factor = eval(fd_param.factor)
            if fd_param.min is not None:
                q = q.filter(
                    factor > fd_param.min
                )
            if fd_param.max is not None:
                q = q.filter(
                    factor < fd_param.max
                )
        
        order_by = eval(self._params.get('order_by', None))
        sort_type = self._params.get('sort', SortType.asc)
        if order_by is not None:
            if sort_type == SortType.asc:
                q = q.order_by(order_by.asc())
            else:
                q = q.order_by(order_by.desc())

        limit = self._params.get('limit', None)
        if limit is not None:
            q = q.limit(limit)

        stock_list = list(get_fundamentals(q)['code'])
        
        self._update_adjust_ratio(context, data, stock_list, len(stock_list))
        self._update_adjust_info(context, data, stock_list)
        
        return stock_list

    def __str__(self):
        s = self.memo + ':'
        for fd_param in self._params.get('factors', []):
            if not isinstance(fd_param, FD_Factor):
                continue
            if fd_param.min is None and fd_param.max is None:
                continue
            s += '\n\t\t\t\t---'
            if fd_param.min is not None and fd_param.max is not None:
                s += '[ %s < %s < %s ]' % (fd_param.min,
                                           fd_param.factor, fd_param.max)
            elif fd_param.min is not None:
                s += '[ %s < %s ]' % (fd_param.min, fd_param.factor)
            elif fd_param.max is not None:
                s += '[ %s < %s ]' % (fd_param.factor, fd_param.max)

        order_by = self._params.get('order_by', None)
        sort_type = self._params.get('sort', SortType.asc)
        if order_by is not None:
            s += '\n\t\t\t\t---'
            sort_type = '从小到大' if sort_type == SortType.asc else '从大到小'
            s += '[排序:%s %s]' % (order_by, sort_type)
        limit = self._params.get('limit', None)
        if limit is not None:
            s += '\n\t\t\t\t---'
            s += '[限制选股数:%s]' % (limit)
        return '多因子选股:' + s

class FilterBySortOHLCV(FilterStockList):
    """按交易数据排序过滤"""

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        # 字段名，如 day_open or 'open', 'close', 'low', 'high', 'volume', 'money', 
        # 'factor', 'high_limit','low_limit', 'avg', 'pre_close'
        self.field = params.get('field', 'day_open')
        self.price_range = params.get('price_range', [0, 1000])  # 价格范围
        self.asc = params.get('asc', False)  # 是否正序排序
        self.count = params.get('count', 10)  # 选取数量

    def update_params(self, context, params):
        self.field = params.get('field', self.field)
        self.asc = params.get('asc', self.asc)
        self.count = params.get('count', self.count)
        self.price_range = params.get('price_range', self.price_range)

    def filter(self, context, data, stock_list):
        _ll = [] # 待排除的股票
        def get_current_price(ele):
            price = None
            if self.field == 'day_open':
                current_data = get_current_data()
                price = current_data[ele].day_open
            else:
                df = get_price(ele, end_date=context.previous_date, count=1,
                                frequency='daily', fields=[self.field], fq=None) # or fq='pre'
                price = df[self.field][0]
            if price < self.price_range[0] or price > self.price_range[1]:
                _ll.append(ele)
            return price
        
        stock_list.sort(key=get_current_price, reverse=not self.asc)
        _new = [i for i in stock_list if i not in _ll]
        stock_list = _new[0:self.count]
        
        self.stock_list_info(stock_list)
        return stock_list
    
    def __str__(self):
        return '按交易数据排序过滤 [字段名: %s，正序：%s，选取数量：%d ]' \
            % (self.field, self.asc, self.count)

class FilterByNorthMoney(FilterStockList):
    """北向资金持股过滤 - 根据北上资金占比或增持率排序过滤
    来源于【01-北上资金仓位调整-Clone】（北上詹姆斯）以及【01-北向资金选股轮换-Clone】
    """

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.method = params.get('method', 'share_ratio') # share_ratio / share_delta_limit / share_ratio_delta
        self.buy_count = params.get('buy_count', 10)  # 最多选取多少只股
        self.df_saved = None  # 北向资金前一数据df

    def update_params(self, context, params):
        self.method = params.get('method', self.method)
        self.buy_count = params.get('buy_count', self.buy_count)

    def before_trading_start(self, context):
        pass

    def after_trading_end(self, context):
        pass

    def filter(self, context, data, stock_list):
        ## 每天一次计算出当日要买入的股票，其他时候直接返回选取的股票列表
        table = finance.STK_HK_HOLD_INFO
        q = query(table.day, table.name, table.code, table.share_ratio)\
            .filter(table.link_id.in_(['310001', '310002']),
                    table.day.in_([context.previous_date]))
        df = finance.run_query(q)

        def get_df_merged(df_new, df_old):
            #df_merged = pd.merge(df_new, df_old, on='code', how='outer')
            ##df_merged.fillna(0, inplace=True)
            #df_merged.dropna(axis=0, how='any')
            df_merged = pd.merge(df_new, df_old, on=['code'], how='inner')
            df_merged['share_ratio_delta'] = df_merged['share_ratio_x'] - df_merged['share_ratio_y']
            df_merged['share_ratio_delta_ratio'] = df_merged['share_ratio_delta'] / df_merged['share_ratio_y']
            df_merged[['share_ratio_delta', 'share_ratio_delta_ratio']] \
                = df_merged[['share_ratio_delta', 'share_ratio_delta_ratio']].astype('float')
            #print('--> df_merged:', df_merged)
            return df_merged

        _picked = []
        if self.method == 'share_ratio': # 验证结果：一般
            # 按北向持股比率逆排序（原文方式）
            _sorted = list(df.sort_values(by='share_ratio', ascending=False)['code'].values)
            #TODO 能否性能优化？
            for i in _sorted:
                if i in stock_list:
                    _picked.append(i)
        elif self.method == 'share_ratio_delta':  # 验证结果：好
            # 按增减持超过阈值选取（原文方式）
            # 原文根据增减仓情况做跟随动态调整，本框架目前只支持全买入或全卖出，故暂不实现
            #bx_dict = df_new.set_index('code')['share_ratio'].to_dict()
            #for security in bx_dict:
            #    if (security in self.df_saved) and (security in stock_list):
            #        change = bx_dict[security] - self.df_saved[security]
            #        if abs(change) < delta_threshold:
            #            continue
            #        print('股票=%s 北向增减持=%.2f' % (security, change))
            #        if change < -delta_threshold and (security in context.portfolio.positions):
            #            order_value(security, -port_total(context)*0.2)
            #        if change > delta_threshold and (security not in context.portfolio.positions):
            #            order_value(security, port_total(context)*0.2)
            share_threshold = 3  # 北向资金占比（持股比率）最小阈值，建议 3%
            delta_threshold = 0.3  # 北向资金占比增持最小阈值 0.3%?
            df_new = df[df['share_ratio'] > share_threshold]
            #df_new.fillna(0, inplace=True)  # 防止后续share_ratio取差值后去空时，其他为nan的字段干扰
        
            if self.df_saved is not None: # 第一天为 None，跳过
                df_merged = get_df_merged(df_new, self.df_saved)
                df_merged = df_merged.sort_values(by='share_ratio_delta', ascending=False)
                df_merged = df_merged[df_merged['share_ratio_delta'] > delta_threshold]
                for i in list(df_merged['code']):
                    #print('--> i:', i)
                    if i in stock_list:
                        _picked.append(i)
                self.log.info('满足条件（北向占比大于: %.2f%%, 占比增值大于: %.2f%% 按增值逆排序）的股票只数：%d' \
                    % (share_threshold, delta_threshold, len(_picked)))
                
            self.df_saved = df_new
        elif self.method == 'share_ratio_delta_ratio':  # 验证结果：不佳
            # 按北向增持比率逆排序
            share_threshold = 3  # 北向资金占比（持股比率）最小阈值，建议 3%
            df_new = df[df['share_ratio'] > share_threshold]
        
            if self.df_saved is not None: # 第一天为 None，跳过
                df_merged = get_df_merged(df_new, self.df_saved)
                df_merged = df_merged.sort_values(by='share_ratio_delta_ratio', ascending=False)
                for i in list(df_merged['code']):
                    if i in stock_list:
                        _picked.append(i)
                self.log.info('满足条件（北向占比增值比率逆排序）的股票只数：%d' \
                    % (len(_picked)))
                
            self.df_saved = df_new
        else:
            # 或选出高股价的股票（不佳）
            def _get_current_price(code):
                current_data = get_current_data()
                return current_data[code].day_open
            _picked = list(df['code'].values)
            _picked.sort(key=_get_current_price, reverse=True)
        
        stock_list = list(_picked)
        stock_list = stock_list[:self.buy_count]

        if len(stock_list) > 0: self.log.info('选定买入股：%s' % stock_list)
        return stock_list

    def __str__(self):
        return '北向资金持股过滤 [ 方式: %s, 选取股数: %d ]' \
            % (self.method, self.buy_count)

class FilterByRsiGeneral(FilterStockList):
    '''RSI通用趋势过滤，可使用价格、成交量等不同变量'''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.index_stock = params.get(
            'index_stock', None)  # 若设置了指标股，则只对指标股分析趋势
        self.days_slow = params.get('days_slow', 14)
        self.days_fast = params.get('days_fast', 5)
        # 取值在下列表中 self.calc_types
        self.calc_type = params.get('calc_type', 'close')
        self.calc_types = params.get(
            'calc_types', ['close', 'high', 'low', 'volume'])
        self.allow_range = params.get('allow_range', [(0, 100)])
        self.allow_trend = params.get('allow_trend', ['up', 'sideway', 'down'])
        self.enable_minute_data = params.get(
            'enable_minute_data', False)  # 是否补充当日当时分钟数据

    def update_params(self, context, params):
        self.index_stock = params.get('index_stock', self.index_stock)
        self.days_slow = params.get('days_slow', self.days_slow)
        self.days_fast = params.get('days_fast', self.days_fast)
        self.calc_type = params.get('calc_type', self.calc_type)
        self.calc_types = params.get('calc_types', self.calc_types)
        self.allow_range = params.get('allow_range', self.allow_range)
        self.allow_trend = params.get('allow_trend', self.allow_trend)
        self.enable_minute_data = params.get(
            'enable_minute_data', self.enable_minute_data)

    # 取得对应股票代码的RSI指标计算值
    def get_rsi(self, context, stock):

        # 初始化风险参数
        con_FAST_RSI = self.days_fast
        con_SLOW_RSI = self.days_slow
        d_today = (context.current_dt).strftime("%Y-%m-%d %H:%M:%S")

        # 取得历史收盘数据
        df_hData = attribute_history(
            stock, self.days_slow * 4, unit='1d', skip_paused=True)
        if self.enable_minute_data:
            # 注意！！对于分钟级策略，frequency 不能设置为 1d，将会导致回测时用到未来数据。
            # last_prices = history(1, unit='1m', security_list=[stock])
            # df_hData.loc[context.current_dt.today(), 'close'] = last_prices[stock][0]
            df_hData_now = get_price(
                stock, count=1, end_date=context.current_dt, frequency='1m')
            df_hData.loc[context.current_dt.today(
            ), 'close'] = df_hData_now['close'].values[0]

        calc_p = df_hData[self.calc_type].values
        RSI_F = talib.RSI(calc_p, timeperiod=con_FAST_RSI)
        RSI_S = talib.RSI(calc_p, timeperiod=con_SLOW_RSI)
        isFgtS = RSI_F > RSI_S
        isFstS = RSI_F < RSI_S

        rsiS = RSI_S[-1]
        rsiF = RSI_F[-1]

        # 根据长短期趋势判断走势
        bsFlag = None
        if isFgtS[-1] and isFgtS[-3]:
            bsFlag = "up"  # 上行
        elif isFstS[-1] and isFstS[-3]:
            bsFlag = "down"  # 下行
        else:
            bsFlag = "sideway"  # 盘整

        return bsFlag, rsiS  # 走势，RSI计算值

    def filter(self, context, data, stock_list):

        if self.calc_type not in self.calc_types:
            self.log.error('unknown calc_type: %s' % self.calc_type)
            return []

        if self.index_stock:
            try:
                bsFlag, rsiS = self.get_rsi(context, self.index_stock)
                # self.log.debug('got rsi of %s: %s, %s' % (self.index_stock, bsFlag, rsiS))
                if bsFlag in self.allow_trend:
                    for rg in self.allow_range:
                        if rsiS >= rg[0] and rsiS <= rg[1]:
                            return stock_list
                else:
                    return []
            except Exception as e:
                # self.log.info(e)
                formatted_lines = traceback.format_exc().splitlines()
                self.log.info(formatted_lines[-1])
                return []

        else:
            tmp = {}
            for stock in stock_list:
                try:
                    bsFlag, rsiS = self.get_rsi(context, stock)
                    if bsFlag in self.allow_trend:
                        for rg in self.allow_range:
                            if rsiS >= rg[0] and rsiS <= rg[1]:
                                # self.log.debug('got rsi of %s: %s, %s' % (stock, bsFlag, rsiS))
                                tmp[stock] = rsiS
                except Exception as e:
                    # self.log.info(e)
                    formatted_lines = traceback.format_exc().splitlines()
                    self.log.info(formatted_lines[-1])

            # 返回符合趋势的股票
            # self.log.info('--> 获取到符合趋势的股票 %d 只。' % len(tmp))
            return [k for k, v in sorted(tmp.items(), key=lambda d: d[1], reverse=False)]
            # return list(tmp)

    def __str__(self):
        return 'RSI通用过滤 [指标股：%s，长期天数：%d，短期天数：%d，计算类型：%s，许可取值范围：%s，许可走势：%s，%s]' % \
               (self.index_stock, self.days_slow, self.days_fast, self.calc_type, self.allow_range, self.allow_trend,
                'RSI 补充当天当时分钟级数据（非价格类不建议开启）' if self.enable_minute_data else '未开启当天数据')

class FilterByTrend(FilterStockList):
    '''指标股趋势过滤，适合协整轮动等策略'''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.tech_type = params.get('tech_type', 'RSI')  # RSI or safety
        tech_types = ('RSI', 'safety')
        if self.tech_type not in tech_types:
            raise Exception('不支持的技术指标：%s。可选值：%s' \
                % (self.tech_type, tech_types))
        self.rsi_allow = params.get('rsi_allow', ['up', 'sideway', 'low'])
        self.index_stock = params.get(
            'index_stock', None)  # 若设置了指标股，则只对指标股分析趋势
        self.days_fast = params.get('days_fast', 5)
        self.days_slow = params.get('days_slow', 12)

    def update_params(self, context, params):
        self.tech_type = params.get('tech_type', self.tech_type)
        self.rsi_allow = params.get('rsi_allow', self.rsi_allow)
        self.index_stock = params.get('index_stock', self.index_stock)
        self.days_fast = params.get('days_fast', self.days_fast)
        self.days_slow = params.get('days_slow', self.days_slow)

    def filter(self, context, data, stock_list):
        flag = False

        if self.index_stock:
            if self.tech_type == 'RSI':
                bsFlag = ''
                try:
                    bsFlag, rsiS = FuncLib.get_rsi(context, self.index_stock, self.days_slow,
                                                     self.days_fast)
                except Exception as e:
                    # self.log.info(e)
                    formatted_lines = traceback.format_exc().splitlines()
                    self.log.info(formatted_lines[-1])
                flag = bsFlag in self.rsi_allow
            elif self.tech_type == 'safety':
                flag, value = FuncLib.market_safety_macd(context, self.index_stock, self.days_fast,
                                                      self.days_slow, False)
            else:
                return []
            if flag:
                return stock_list
            else:
                return []
        else:
            tmp = {}
            for stock in stock_list:
                if self.tech_type == 'RSI':
                    bsFlag = ''
                    rsiS = 0.0
                    try:
                        bsFlag, rsiS = FuncLib.get_rsi(
                            context, stock, self.days_slow, self.days_fast)
                    except Exception as e:
                        # self.log.info(e)
                        formatted_lines = traceback.format_exc().splitlines()
                        self.log.info(formatted_lines[-1])
                    flag = bsFlag in self.rsi_allow
                    if flag:
                        tmp[stock] = rsiS
                else:
                    flag, value = FuncLib.market_safety_macd(context, self.index_stock, self.days_fast,
                                                          self.days_slow, False)
                    if flag:
                        tmp[stock] = value
            # 返回符合趋势的股票
            # self.log.info('--> 获取到符合趋势的股票 %d 只。' % len(tmp))
            return [k for k, v in sorted(tmp.items(), key=lambda d: d[1], reverse=False)]
            # return list(tmp)

    def __str__(self):
        return '指标股趋势过滤 [指标股：%s，技术指标：%s，允许的趋势: %s，短期天数：%d，长期天数：%d]' % \
               (self.index_stock, self.tech_type,
                self.rsi_allow, self.days_fast, self.days_slow)

class FilterByMarketTemperature(FilterStockList):
    '''根据市场温度过滤股票'''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.stock_list_only = params.get(
            'stock_list_only', False)  # 只基于待过滤股票判断市场温度
        self.temperature_range = params.get(
            'temperature_range', (100, 10000))  # 允许温度范围 -60000~60000
        self.calc_start_time = params.get(
            'calc_start_time', (9, 31))  # 开始计算时间点
        self.calc_every_bar = params.get(
            'calc_every_bar', True)  # 是否每个时间周期都重新计算
        self.up_rate_min = params.get('up_rate_min', 0.025)  # 判断为上涨的最小幅度
        self.down_rate_min = params.get('down_rate_min', 0.025)  # 判断为下跌的最小幅度
        self.temperature = 0

    def update_params(self, context, params):
        self.stock_list_only = params.get(
            'stock_list_only', self.stock_list_only)
        self.temperature_range = params.get(
            'temperature_range', self.temperature_range)
        self.calc_start_time = params.get(
            'calc_start_time', self.calc_start_time)
        self.calc_every_bar = params.get('calc_every_bar', self.calc_every_bar)
        self.up_rate_min = params.get('up_rate_min', self.up_rate_min)
        self.down_rate_min = params.get('down_rate_min', self.down_rate_min)

    def filter(self, context, data, stock_list):
        hour = context.current_dt.hour
        minute = context.current_dt.minute

        if hour < self.calc_start_time[0] or (hour == self.calc_start_time[0] and minute < self.calc_start_time[1]):
            # 未到开始计算时间点
            return []
        else:
            if (hour == self.calc_start_time[0] and minute == self.calc_start_time[1]) or self.calc_every_bar:
                try:
                    if self.stock_list_only:
                        if len(stock_list) > 0:
                            self.temperature = FuncLib.get_market_temperature(context, stock_list,
                                                                                up_rate_min=self.up_rate_min,
                                                                                down_rate_min=self.down_rate_min)
                    else:
                        self.temperature = FuncLib.get_market_temperature(context, [],
                                                                            up_rate_min=self.up_rate_min,
                                                                            down_rate_min=self.down_rate_min)
                    if self.temperature_range[0] <= self.temperature <= self.temperature_range[1]:
                        pass
                    else:
                        self.log.info('市场温度指标 %d 不在允许范围内，股票清空' %
                                      self.temperature)
                        return []
                except Exception as e:
                    # self.log.info(e)
                    formatted_lines = traceback.format_exc().splitlines()
                    self.log.info(formatted_lines)
                    self.log.info('计算市场温度指标失败，股票清空')
                    return []
            if self.temperature_range[0] <= self.temperature <= self.temperature_range[1]:
                return stock_list
            else:
                # self.log.info('市场温度指标 %d 不在允许范围内，股票清空' % self.temperature)
                return []

    def __str__(self):
        return '根据市场温度过滤股票 [只基于待过滤股票: %s, 温度允许范围: %s, 开始计算时间: %s, 是否每个时间周期都重新计算: %s, ' \
               '判断为上涨的最小幅度: %.3f, 判断为下跌的最小幅度: %.3f]' % \
               (self.stock_list_only, self.temperature_range, self.calc_start_time, self.calc_every_bar,
                self.up_rate_min, self.down_rate_min)

class FilterByLongtouFast(FilterStockList):
    """龙头追涨极速版过滤 - 来自于聚宽【01-上涨趋势策略-2-改-Clone】"""

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.buy_count = params.get('buy_count', 3)  # 选取数量

    def update_params(self, context, params):
        self.buy_count = params.get('buy_count', self.buy_count)

    def filter(self, context, data, stock_list):
        return self.check_stocks(context, stock_list)
    
    def check_stocks(self, context, stock_list):
        """股票筛选"""
        from scipy.stats import linregress

        current_data = get_current_data()

        # 未停牌、未涨跌停、非科创板
        stock_list = [stock for stock in stock_list if
                        (not current_data[stock].paused) and
                        (current_data[stock].low_limit < current_data[stock].day_open < current_data[
                            stock].high_limit) and
                        (not stock.startswith('688'))
                        ]

        # 昨收盘价不高于500元/股
        s_close_1 = history(1, '1d', 'close', stock_list).iloc[-1]
        stock_list = list(s_close_1[s_close_1 <= 500].index)

        # 近30个交易日的最高价 / 昨收盘价 <=1.1, 即HHV(HIGH,30)/C[-1] <= 1.1
        high_max_30 = history(30, '1d', 'high', stock_list).max()
        s_fall = high_max_30 / s_close_1
        stock_list = list(s_fall[s_fall <= 1.1].index)

        # 近7个交易日的交易量均值 与 近180给交易日的成交量均值 相比，放大不超过1.5倍  MA(VOL,7)/MA(VOL,180) <=1.5
        df_vol = history(180, '1d', 'volume', stock_list)
        s_vol_ratio = df_vol.iloc[-7:].mean() / df_vol.mean()
        stock_list = list(s_vol_ratio[s_vol_ratio <= 1.5].index)

        # 对近120个交易日的股价进行线性回归：入选条件 slope / intercept > 0.005 and r_value**2 > 0.8
        target_dict = {}
        x = np.arange(120)
        for stock in stock_list:
            y = attribute_history(stock, 120, '1d', 'close', df=False)['close']
            slope, intercept, r_value, p_value, std_err = linregress(x, y)
            if slope / intercept > 0.005 and r_value > 0.9:  #  
                target_dict[stock] = slope # r_value ** 2

        # 入选股票按照R Square 降序排序, 取前N名
        target_list = []
        if target_dict:
            df_score = pd.DataFrame.from_dict(
                target_dict, orient='index', columns=['score', ]
            ).sort_values(
                by='score', ascending=False
            )
            target_list = list(df_score.index[:self.buy_count])
        
        return target_list

    def __str__(self):
        return '龙头追涨极速版过滤 [选取数量：%d ]' \
            % (self.buy_count)
    

### 择时类 ###
# TimingByXXX(Rule)

class TimingDecide(Enum):
    LONG = 0
    SHORT = 1
    OTHER = 2

class TimingByNorthMoney(Rule):
    """北上资金择时"""

    def __init__(self, params):
        self.method = params.get('method', 'total_net_in') # total_net_in or boll
        method_allowd = ('total_net_in', 'boll')
        if self.method not in method_allowd:
            raise Exception('不支持的计算方式：%s。可选项：%s' % (self.method, method_allowd))
        
        self._idct_north_bond = IndicatorNorthBond()

    def update_params(self, context, params):
        self.method = params.get('method', self.method)

    def handle_data(self, context, data):
        _td = self._trade_direction(context)
        if _td == TimingDecide.SHORT: # 清空
            self.is_to_return = True
        else:
            self.is_to_return = False
        
        return self.is_to_return
    
    def _trade_direction(self, context):
        """判断交易方向
        返回值：
            TimingDecide.LONG - 建议做多（换仓或加仓）
            TimingDecide.SHORT - 建议做空（清仓或买入低风险标的）
            TimingDecide.OTHER - 无建议，待命
        """
        if self.method == 'boll':
            boll_ret = self._idct_north_bond.get_boll(context, windows=[252,150,80,20])
            
            #根据资金流入布林带择时：任意3根上穿上界买入，任意2根下穿下界清仓。
            buy_num = 0
            sell_num = 0
            for i in boll_ret:
                _mf, _upper, _lower = boll_ret[i]
                log.info('%3d 日北上资金，最新：%.2f 上界：%.2f 下界：%.2f' % (i, _mf, _upper, _lower))
                if _mf >= _upper:
                    buy_num += 1
                elif _mf <= _lower:
                    sell_num += 1
            
            if sell_num >= 2:    # 原文为 2
                log.info("建议清仓。北向资金下穿布林带下界超2根，跌穿下界数：%s\n" % (sell_num))
                return TimingDecide.SHORT
            elif buy_num >=3:   # 原文为 3
                log.info("建议开仓。北向资金上穿布林带上界超3根，上穿上界数：%s\n" % (buy_num))
                return TimingDecide.LONG
            else:
                log.info("建议仓位不变")
                return TimingDecide.OTHER

        elif self.method == 'total_net_in':
            _total_net_in = self._idct_north_bond.get_total_net_in(context)
            log.info('近3日北上资金流入：%.2f' % _total_net_in)
            if _total_net_in > 0:
                return TimingDecide.LONG
            elif _total_net_in < 0:
                return TimingDecide.SHORT
            
        return TimingDecide.OTHER

    def before_trading_start(self, context):
        self.is_to_return = False

    def after_trading_end(self, context):
        self.is_to_return = False

    def __str__(self):
        return '北上资金择时 [ 方式: %s ]' % self.method

class TimingByMarketEmotion(Rule):
    """大盘择时-市场情绪"""

    def __init__(self, params):
        self.subject = params.get('subject', '000300.XSHG') # 判断标的
        self.lag = params.get('lag', 6)  # 大盘成交量连续跌破均线的天数，发出空仓信号
        self.lag0 = params.get('lag0', 7) # 大盘行情监控周期
        self.field = params.get('field', 'volume') # 判断字段 close / volume
        self.only_long = params.get('only_long', False) # 只允许做多行情（不推荐，会减少回撤以及收益）
        self._emotion_rate = 0  # 最近涨幅
        
    def update_params(self, context, params):
        self.subject = params.get('subject', self.subject)
        self.lag = params.get('lag', self.lag)
        self.lag0 = params.get('lag0', self.lag0)
        self.field = params.get('field', self.field)
        self.only_long = params.get('only_long',  self.only_long)

    def handle_data(self, context, data):
        ret_val = self._emotion_monitor()
        if self.only_long and ret_val != TimingDecide.LONG:
            self.log.info("大盘行情（%s）非连续高于均线，空仓" % self.field)
            self.is_to_return = True
        elif ret_val == TimingDecide.SHORT:
            self.log.info("大盘行情（%s）持续低于均线，空仓" % self.field)
            self.is_to_return = True
        else:
            self.is_to_return = False
        
        return self.is_to_return

    def _emotion_monitor(self):
        """基于动量或价格的市场行情判断"""
        _cnt = self.lag0 + max(self.lag, 3)
        
        _df = attribute_history(
            security=self.subject, count=_cnt, unit='1d', fields=self.field)[self.field]
        v_ma_lag0 = talib.MA(_df, self.lag0)  # 前 self.lag - 1 个会变成nan
        
        self._emotion_rate = round(
            (_df[-1] / v_ma_lag0[-1] - 1) * 100, 2)  # 最后一天相对于均值的增幅百分比
        
        _diff = (_df - v_ma_lag0)
        if _diff[-1] >= 0:
            # 最近3天都站在均线之上
            ret_val = TimingDecide.LONG if (_diff[-3:] >= 0).all() \
                    else TimingDecide.OTHER
        else:
            # 最近lag天都在均线之下
            ret_val = TimingDecide.SHORT if (_diff[-self.lag:] < 0).all() \
                    else TimingDecide.OTHER
        
        self.log.info("%s 指数，大盘行情：%s，相比%d日均线的涨幅：%0.2f%%" %
                 (self.subject, ret_val, self.lag, self._emotion_rate))
        
        return ret_val

    def before_trading_start(self, context):
        self.is_to_return = False

    def after_trading_end(self, context):
        self.is_to_return = False

    def __str__(self):
        return '大盘择时-市场情绪 [ 判断标的: %s，空仓信号计算日：%d，行情监控周期：%d，判断字段：%s，只允许做多行情：%s ]' \
            % (self.subject, self.lag, self.lag0, self.field, self.only_long)

class TimingByMarketRSRS(Rule):
    """大盘择时-RSRS：成交量加权右偏标准分
    参考：https://zhuanlan.zhihu.com/p/33501881 & https://www.joinquant.com/post/27399

    基于光大证券研报《基于阻力支撑相对强度(RSRS)的市场择时》，给出了RSRS斜率指标择时，以及在斜率基础上的标准化指标择时策略。

    一、 阻力支撑相关概念

    阻力位是指指标价格上涨时可能遇到的压力，即交易者认为卖方力量开始反超买方，从而价格难以继续上涨或从此回调下跌的价位；
    支撑位则是交易者认为买方力量开始反超卖方，从而止跌或反弹上涨的价位。

    常见的确定阻力支撑位的方法有，布林带上下轨突破策略（突破上轨建仓买入，突破下轨卖出平仓）和均线策略（如超过20日均线建仓买入，
    低于20日均线卖出平仓）。然而，布林带突破策略在震荡期间出现了持续亏损，均线策略交易成本巨大，且在震荡期间的回撤很大。

    二、阻力支撑相对强度（RSRS）

    阻力支撑相对强度(Resistance Support Relative Strength, RSRS)是另一种阻力位与支撑位的运用方式，它不再把阻力位与
    支撑位当做一个定值，而是看做一个变量，反应了交易者对目前市场状态顶底的一种预期判断。

    我们按照不同市场状态分类来说明支撑阻力相对强度的应用逻辑：

    1.市场在上涨牛市中：
    如果支撑明显强于阻力，牛市持续，价格加速上涨
    如果阻力明显强于支撑，牛市可能即将结束，价格见顶
    2. 市场在震荡中：
    如果支撑明显强于阻力，牛市可能即将启动
    如果阻力明显强于支撑，熊市可能即将启动
    3.市场在下跌熊市中：
    如果支撑明显强于阻力，熊市可能即将结束，价格见底
    如果阻力明显强于支撑，熊市持续，价格加速下跌

    每日最高价和最低价是一种阻力位与支撑位，它是当日全体市场参与者的交易行为所认可的阻力与支撑。
    一个很自然的想法是建立最高价和最低价的线性回归，并计算出斜率。即：

    当斜率值很大时，支撑强度大于阻力强度。
    阻力渐小，上方上涨空间大
    支撑渐强，下跌势头欲止

    当斜率值很小时，阻力强度大于支撑强度。
    阻力渐强，上涨势头渐止
    支撑渐送，下方下跌空间渐大

    三、阻力支撑相对强度（RSRS）指标择时策略

    第一种方法是直接将斜率作为指标值。当日RSRS斜率指标择时策略如下：
    1、取前N日最高价与最低价序列。（N = 18）
    2、将两个序列进行OLS线性回归。
    3、将拟合后的β值作为当日RSRS斜率指标值。
    4、当RSRS斜率大于S(buy)时，全仓买入，小于S(sell)时，卖出平仓。（S(buy)=1,S(sell)=0.8）

    由于市场处于不同时期时，斜率的均值有比较大的波动。因此，直接采用斜率均值作为择时指标并不太合适。
    我们尝试下面的方法。

    第二种方法是在斜率基础上进行标准化，取标准分作为指标值。RSRS斜率标准分指标择时策略如下：
    1、取前M日的RSRS斜率时间序列。（M = 600）
    2、计算当日RSRS斜率的标准分RSRS(std)：其中μM为前M日的斜率均值，σM为前M日的标准差
    3、若RSRS(std)大于S(buy)，则全仓买入；若RSRS(std)小于S(sell)，则卖出平仓。（S(buy)=0.7,S(sell)=−0.7）

    注：benchmark和标的股票均为沪深300指数，尝试N取自10-30，M取自400-800，发现N=18，M=600时收益率最高。

    zzz：经验证，2021-02-01 ~ 2021-03-07 之间大盘下跌7%，该择时基本无效（用了两种来自JQ的不同计算方法，均如此）
    """

    def __init__(self, params):
        # 设置 RSRS 指标中N, M的值
        self.N = params.get('N', 18)        # 指标计算的统计周期 18
        self.M = params.get('M', 600)       # 指标计算的统计样本长度 600
        self.buy = params.get('buy', 0.7)   # 买入斜率阈值 0.7
        self.sell = params.get('sell', -0.7)    # 卖出斜率阈值 -0.7
        # 计算指标股
        self.index_stock = params.get('index_stock', '000300.XSHG')

        self.daily_calc_done = False        # 当天是否已完成计算
        self.signal_tag = -1                # 0:行情看差，1:行情看好
        self.init_calc = True               # 首次运行判断

        # 中间变量
        self.beta_weight = []
        self.r2_weight = []
        self.volatility = []
        self.pos = False
        self.days = 0
        self.rsrs = 0

    def update_params(self, context, params):
        self.N = params.get('N', self.N)
        self.M = params.get('M', self.M)
        self.buy = params.get('buy', self.buy)
        self.sell = params.get('sell', self.sell)
        self.index_stock = params.get('index_stock', self.index_stock)

    def handle_data(self, context, data):
        # 每天只在指定时间点执行一次
        #hour = context.current_dt.hour
        #minute = context.current_dt.minute
        #if not (self.run_time[0] == hour and self.run_time[1] == minute):
        #    return

        if not self.daily_calc_done:
            signal = self._callsignal(context,'000001.XSHG')
            self.daily_calc_done = True
            if signal == 0:
                self.is_to_return = True
                self.log.info('行情看跌，建议轻仓！')
                self.signal_tag = signal
            elif signal == 1:
                self.is_to_return = False
                self.log.info('行情看涨，建议重仓！')
                self.signal_tag = signal
            else:  # signal == -1
                if self.signal_tag == 0:  # 继续轻仓
                    self.is_to_return = True
                    self.log.info('其他行情，保持轻仓！')
                elif self.signal_tag == 1:  # 继续重仓
                    self.log.info('其他行情，保持重仓！')
        
        return self.is_to_return

    def _callsignal(self, context, stock):
        if self.init_calc:
            # 计算2005年1月5日至回测开始日期的RSRS斜率指标
            prices = get_price(self.index_stock, '2005-05-01', context.previous_date, '1d', ['high', 'low', 'volume'])
            all_highs = prices.high.values
            all_lows = prices.low.values
            all_volumes = prices.volume.values
            #
            for i in range(0, len(all_highs) - self.N + 1):  # range(a,b)=[a,b)
                highs = all_highs[i:i + self.N]  # dataframe 不包括[a:b]中的b，相当于[a:b)
                lows = all_lows[i:i + self.N]
                volumes = all_volumes[i:i + self.N]
                #
                beta, r2 = self.calc_beta_r2(highs, lows, volumes)
                #
                self.beta_weight.append(beta)
                self.r2_weight.append(r2)

            # 计算2005-01-01至开盘前一天的指数日收益率的波动率
            daily_return = get_price(self.index_stock, '2005-05-01', context.previous_date, '1d',
                                    'close')['close'].pct_change()[1:].values
            for i in range(0, len(daily_return) - self.N + 1):
                std = np.std(daily_return[i:i + self.N])
                self.volatility.append(std)

            self.init_calc = False
        
        else:
            prices = attribute_history(self.index_stock, self.N, '1d', ['high', 'low', 'volume'], df=False)
            highs = prices['high']
            lows = prices['low']
            volumes = prices['volume']
            #
            beta, r2 = self.calc_beta_r2(highs, lows, volumes)
            #
            self.beta_weight.append(beta)
            self.r2_weight.append(r2)

            # 计算N日收益率的标准差、M日收益率标准差的分位数（钝化RSRS所需）
            # 用过去self.N天的数据,计算波动率,并获取它在过去self.M所处的百分位
            daily_return = attribute_history(self.index_stock, self.N + 1, '1d', ['close'])['close'].pct_change()[1:].values
            # 计算N日收益率的标准差
            std = np.std(daily_return)
            self.volatility.append(std)

            # ---------------------------
            # 计算rsrs
            # ---------------------------
            # 标准化/归一化
            section = self.beta_weight[-self.M:]
            mu = np.mean(section)
            sigma = np.std(section)
            z_score = (section[-1] - mu) / sigma
            # 右偏标准分
            vol_rsrs_right = z_score * beta * r2  # 成交量加权-右偏标准分
            # 其它的rsrs
            vol_rsrs = z_score * r2  # 成交量加权-无偏标准分
            std_percent = stats.percentileofscore(self.volatility[-self.M:], std) / 100  # 计算M日收益率标准差的分位数
            vol_rsrs_right_dun = z_score * beta * (r2 ** (2 * std_percent))  # 钝化-成交量加权-右偏标准分
            vol_rsrs_dun = z_score * (r2 ** (2 * std_percent))  # 钝化-成交量加权-无偏标准分
            
            log.info('z_score= %.3f, beta= %.3f, r2= %.3f, std= %.3f, std_percent= %.3f' % (
                z_score, beta, r2, std, std_percent))
            log.info('成交量加权-右偏rsrs     = %.3f，成交量加权-无偏rsrs     = %.3f' % (vol_rsrs_right, vol_rsrs))
            log.info('钝化-成交量加权-右偏rsrs= %.3f，钝化-成交量加权-无偏rsrs= %.3f' % (vol_rsrs_right_dun, vol_rsrs_dun))
            
            # 成交量加权-右偏标准分        vol_rsrs_right      = z_score*beta*r2
            # 成交量加权-钝化-右偏标准分   vol_rsrs_right_dun  = z_score*beta*(r2**(2*std_percent))
            # 成交量加权-无偏标准分        vol_rsrs            = z_score*r2
            # 成交量加权-钝化-无偏标准分   vol_rsrs_dun        = z_score*(r2**(2*std_percent))
            self.rsrs = vol_rsrs_right
            log.info('self.rsrs=%.3f' % self.rsrs)

        # 如果上一时间点的 RSRS 斜率大于买入阈值, 则全仓买入
        if self.rsrs > self.buy:
            return 1
        # 如果上一时间点的 RSRS 斜率小于卖出阈值, 则空仓卖出
        elif self.rsrs < self.sell:
            return 0
        
        return -1  # 其他
    
    def calc_beta_r2(self, highs, lows, volumes):
        # type: (np.ndarray, np.ndarray, np.ndarray) -> (float, float)
        # 成交量加权的权重列表
        weights = volumes / volumes.sum()

        # 加权最小二乘法  #sm.WLS(y, xx, weights)
        xx = sm.add_constant(lows)  # 为模型增加常数项，即回归线在y轴上的截距(括号里面写上自变量)
        model = sm.WLS(highs, xx, weights).fit()  # 加权最小二乘法
        beta = model.params[1]  # model.params.low
        r2 = model.rsquared
        return beta, r2

    def before_trading_start(self, context):
        self.is_to_return = False
        self.daily_calc_done = False

    def after_trading_end(self, context):
        self.is_to_return = False
        self.daily_calc_done = False

    def __str__(self):
        return '大盘择时-RSRS [指标股：%s，统计周期：%s，统计样本长度: %s，买入斜率阈值：%.2f，卖出率阈值：%.2f]' % \
               (self.index_stock, self.N, self.M, self.buy, self.sell)

class TimingByMarketSafety(Rule):
    """大盘择时-安全度"""

    def __init__(self, params):
        # 计算安全度所用指数股
        self.index_stock = params.get('index_stock', '000300.XSHG')
        # 开始计算的时间点
        self.calc_start_time = params.get('calc_start_time', (9, 33))
        # 是否每个时间周期都重新计算
        self.calc_every_bar = params.get('calc_every_bar', True)
        # 是否补充当日当时分钟数据
        self.enable_minute_data = params.get('enable_minute_data', True)
        
        self.macd_days_fast = params.get('macd_days_fast', 10)
        self.macd_days_slow = params.get('macd_days_slow', 25)
        self.safety = False

    def update_params(self, context, params):
        self.index_stock = params.get('index_stock', self.index_stock)
        self.calc_start_time = params.get('calc_start_time', self.calc_start_time)
        self.calc_every_bar = params.get('calc_every_bar', self.calc_every_bar)
        self.enable_minute_data = params.get('enable_minute_data', self.enable_minute_data)

        self.macd_days_fast = params.get('macd_days_fast', self.macd_days_fast)
        self.macd_days_slow = params.get('macd_days_slow', self.macd_days_slow)

    def handle_data(self, context, data):
        hour = context.current_dt.hour
        minute = context.current_dt.minute

        if hour < self.calc_start_time[0] \
            or (hour == self.calc_start_time[0] and minute < self.calc_start_time[1]):
            # 未到开始计算时间点
            self.is_to_return = True
            return
        else:
            if (hour == self.calc_start_time[0] and minute == self.calc_start_time[1]) \
                or self.calc_every_bar:
                try:
                    self.safety, value = FuncLib.market_safety_macd(
                        context, self.index_stock,
                        self.macd_days_fast, self.macd_days_slow,
                        enable_minute_data=self.enable_minute_data)
                    if not self.safety:
                        self.log.info('市场安全度未达标')
                        self.is_to_return = True
                    else:
                        self.is_to_return = False
                except Exception as e:
                    # self.log.info(e)
                    formatted_lines = traceback.format_exc().splitlines()
                    self.log.info(formatted_lines)
                    self.log.info('市场安全度指标计算错误')
                    self.is_to_return = True
            else:
                if not self.safety:
                    self.is_to_return = True
            
        return self.is_to_return

    def before_trading_start(self, context):
        self.is_to_return = False

    def after_trading_end(self, context):
        self.is_to_return = False

    def __str__(self):
        return '大盘择时-安全度 [指标股: %s, 开始计算时间: %s, 是否每个时间周期都重新计算: %s, ' \
               '是否补充当日当时分钟数据: %s, MACD 短期天数: %d, MACD 长期天数: %d]' % \
               (self.index_stock, self.calc_start_time, self.calc_every_bar,
                self.enable_minute_data, self.macd_days_fast, self.macd_days_slow)

class TimingByMagicKLine(Rule):
    """魔力K线择时
        # 源自聚宽文章：https://www.joinquant.com/post/32112
        # 标题：20行代码8年胜率100%躲过了牛年第一场大跌
        # 作者：zireego
    zzz - 回撤很低。主要问题：开仓率过低，仅适合中长期策略
    """

    def __init__(self, params):
        # 计算所用指数股
        self.index_stock = params.get('index_stock', '000001.XSHG')
        
        self.daily_calc_done = False  # 当天是否已完成计算
        self.signal_tag = -1  # 0:行情看差，1:行情看好

    def update_params(self, context, params):
        self.index_stock = params.get('index_stock', self.index_stock)

    def handle_data(self, context, data):
        if not self.daily_calc_done:
            signal = self._callsignal(context,'000001.XSHG')
            self.daily_calc_done = True
            if signal == 0:
                self.is_to_return = True
                self.log.info('行情看跌，建议轻仓！')
                self.signal_tag = signal
            elif signal == 1:
                self.is_to_return = False
                self.log.info('行情看涨，建议重仓！')
                self.signal_tag = signal
            else:  # signal == -1
                if self.signal_tag == 0:  # 继续轻仓
                    self.is_to_return = True
                    self.log.info('其他行情，保持轻仓！')
                elif self.signal_tag == 1:  # 继续重仓
                    self.log.info('其他行情，保持重仓！')
        
        return self.is_to_return

    def _callsignal(self, context, stock):
        c = attribute_history(stock, count=32, unit='1d', fields=['close'], skip_paused=True, df=True, fq='pre')['close']
        
        if c.values.argmax() > 22 and c.values.argmin() == 0 and c[-1] < c[-30:].mean() and c[-2] < c[-32:-1].mean():
            return 0  # 行情看差
        elif c[2] == max(c[2:]) and c[-20:].mean() > c[-30:].mean() and c[-10:].mean() > c[-20:].mean():
            return 1  # 行情看好
            
        return -1  # 其他

    def before_trading_start(self, context):
        self.is_to_return = False
        self.daily_calc_done = False

    def after_trading_end(self, context):
        self.is_to_return = False
        self.daily_calc_done = False

    def __str__(self):
        return '魔力K线择时 [指标股: %s]' % \
               (self.index_stock)

class TimingByMarketRiskControl(Rule):
    """大盘择时-风险控制"""

    def __init__(self, params):
        # 计算所用指数股
        self.index_stock = params.get('index_stock', '000300.XSHG')
        # 开始计算的时间点
        self.calc_start_time = params.get('calc_start_time', (9, 33))
        # 是否每个时间周期都重新计算
        self.calc_every_bar = params.get('calc_every_bar', False)
        
        self.safety = False
        self.idct_market_risk = IndicatorMarketRisk(self.index_stock)

    def update_params(self, context, params):
        if self.index_stock != params.get('index_stock', self.index_stock):
            self.index_stock = params.get('index_stock', self.index_stock)
            self.idct_market_risk = IndicatorMarketRisk(self.index_stock)
        self.calc_start_time = params.get('calc_start_time', self.calc_start_time)
        self.calc_every_bar = params.get('calc_every_bar', self.calc_every_bar)

    def handle_data(self, context, data):
        hour = context.current_dt.hour
        minute = context.current_dt.minute

        if hour < self.calc_start_time[0] \
            or (hour == self.calc_start_time[0] and minute < self.calc_start_time[1]):
            # 未到开始计算时间点
            self.is_to_return = True
            return
        else:
            if (hour == self.calc_start_time[0] and minute == self.calc_start_time[1]) \
                or self.calc_every_bar:
                try:
                    self.safety = self.idct_market_risk.risk_is_low(context, data)
                    if not self.safety:
                        self.log.info('市场风险度未达标')
                        self.is_to_return = True
                    else:
                        self.log.info('市场风险度正常，可以交易')
                        self.is_to_return = False
                except Exception as e:
                    # self.log.info(e)
                    formatted_lines = traceback.format_exc().splitlines()
                    self.log.info(formatted_lines)
                    self.log.info('市场风险度指标计算错误')
                    self.is_to_return = True
            else:
                if not self.safety:
                    self.is_to_return = True
        
        return self.is_to_return

    def before_trading_start(self, context):
        self.is_to_return = False

    def after_trading_end(self, context):
        self.is_to_return = False

    def __str__(self):
        return '大盘择时-风险控制 [指标股: %s, 开始计算时间: %s, 是否每个时间周期都重新计算: %s]' % \
               (self.index_stock, self.calc_start_time, self.calc_every_bar)


class AddStocksByCode(FilterStockList):
    '''添加指定代码股票到股票列表'''

    def __init__(self, params):
        FilterStockList.__init__(self, params)

        self.code_list = params.get('code_list', [])

    def update_params(self, context, params):
        self.code_list = params.get('code_list', self.code_list)

    def filter(self, context, data, stock_list):
        stocks = list(set(stock_list) | set(self.code_list))
        return stocks

    def __str__(self):
        return '添加指定股票，列表: %s' % self.code_list


#TODO ！！为了与聚宽线上正在运行的代码保持兼容，特此指定下述别名，后续要逐渐取消

Filter_buy_count = FilterByBuyCount
Filter_Cointegration = PickByCointPairs


#######################################################################
#                                                                     #
#                          以下仅为测试代码                              #
#                                                                     #
#######################################################################
'''
一个完整的组合策略，一般依次包括如下规则：

  1. 基础规则（必选）：系统参数、交易费率、统计信息等
  2. 止损规则：
    - 大盘止损
    - 个股止损
  3. 止盈规则：
    - 全仓止盈（较少用）
    - 个股止盈
  4. 调度规则：
    - 日内调度：指定日内调度时间
    - 隔日调度：实现隔日周期调度等
  5. 择时规则：
    - 大盘择时：确定标的、决定仓位
      基于北向资金、市场情绪、板块热度等（如熊市仅使用防守标的，或半仓等）
    - 个股择时：根据个股或板块的技术形态、买卖信号确定进出场时机
      日内均线、简单技术指标等
    - 仓位管理：根据凯利公式、ATR等方法动态调整仓位
  6. 选股规则（必选）：选取新股票组合（包括一组进攻标的和一组防守标的）
    - 进攻标的：高收益、中风险
      如龙头股、打板股、高价股等
    - 防守标的：中低收益、中低风险
      如优质基金、日利基金、债券基金等
  7. 调仓规则（必选）：
    - 买卖规则：指定成交比率等
    - 调仓通知：模拟盘调仓邮件、APP通知等
    - 跟仓规则：模拟盘实盘跟仓

这些规则将组合成一个总的策略，每分钟在 handle_data 按顺序依次执行。
注：有些策略如协整，其选股与择时一般是在一起的。

#TODO：支持非 handle_data 内处理的其他规则，如日内定时函数、盘前后回调函数等

以下为示例策略：组合策略-攻守全时
逻辑：攻守兼备策略组合，北上或高价蓝筹+基金协整
特点：收益稳定，回撤小。适合各种上行。
注意：基金协整部分需每半年定期调整组合。
'''

if __name__ == '__main__':

    # 为本地测试，模拟上下文及全局变量
    from types import SimpleNamespace as sns
    context = sns(run_params=sns(type=None), aaa=1)
    g = sns()

    def config_strategy_rules(context):
        """ 策略配置规则示例 """

        g.strategy_memo = '策略示例'
        # 定义 log 输出的类类型，必须指定。如需自定义log，可继承修改之。
        g.log_type = RuleLoger
        index2 = '000016.XSHG'  # 大盘指数
        index8 = '399333.XSHE'  # 小盘指数
        #index8 = '399678.XSHE'  # 深次新指数
        buy_count = 8

        ### 1. 基础规则（必选）：系统参数、交易费率、统计信息等

        rules_basic = [
            [True, '_rules_basic_', '基础规则', GroupRules, {
                'config': [
                    [True, '', '设置系统参数', Set_sys_params, {
                        'benchmark': '000300.XSHG'  # 指定基准为次新股指
                    }],
                    [True, '', '手续费设置器', Set_slip_fee, {}],
                    [True, '', '持仓信息打印器', Show_position, {}],
                    [True, '', '统计执行器', Stat, {}],
                ]
            }]
        ]

        ### 2. 止损规则：

        rules_stop_loss = [
            [True, '_rules_stop_loss_', '止损规则', GroupRules, {
                'config': [
                    [False, '_SL_by_index_price_', '指数价格止损器', StopLossByIndexPrice, {
                        'index_stock': index8,  # 指标股 默认 '000001.XSHG'
                        'day_count': 160,  # 可选 取day_count天内的最高价，最低价。默认160
                        'multiple': 2.2  # 可选 最高价为最低价的multiple倍时，触发清仓
                    }],
                    [False, '_SL_by_3_black_crows_', '指数三乌鸦止损', StopLossBy3BlackCrows, {  # no effact. have bug ??!!
                        'index_stock': '000001.XSHG',  # 指标股 默认 '000001.XSHG'
                        'dst_drop_minute_count': 90,  # 默认60分钟，三乌鸦触发情况下，一天之内有多少分钟涨幅<0,则触发止损
                    }],
                    [False, '_SL_by_Mul_index_', '多指数20日涨幅止损器', StopLossByIndexsIncrease, {
                        'indexs': [index2, index8],
                        'min_rate': 0.005
                    }],
                    [False, '_SL_by_capital_', '资金曲线止损', StopLossByCapitalTrend, {
                        'loss_rate_max': 0.05,  # 资金损失率阈值
                        'n': 20,  # 资金损益计算周期（天数）
                        'check_avg_capital': False,  # 是否与n天平均值比较（否则为n天前）
                        'stop_pass_check': False  # 是否开启止损暂停交易期功能
                    }]
                ]
            }]
        ]

        ### 3. 止赢规则：

        rules_stop_profit = [
            [False, '_rules_stop_profit_', '止赢规则', GroupRules, {
                'config': [
                ]
            }]
        ]

        ### 4. 调度规则：

        rules_schedule = [
            [False, '_rules_schedule_', '调度规则', GroupRules, {
                'config': [
                    [False, '_rs_time_c_', '调仓时间', TimerExec, {
                        'times': [[14, 45]],  # 调仓时间列表，二维数组，可指定多个时间点
                    }],
                    [False, '', '调仓日计数器', PeriodCondition, {
                        'period': 3,  # 调仓频率,日
                    }],
                ]
            }]
        ]

        ### 5. 选股规则（必选）：选取新股票组合（包括一组进攻标的和一组防守标的）

        rules_pick = [
            [True, '_rules_pick_', '多策略选股', ChoseStocksMulti, {
                'config': [
                    # 攻守全时选股策略 - 组合进取策略、防守策略和择时规则，获取最终选股结果
                    # 5.1 进取型选股（可选）：
                    [False, '_rp_aggress_', '进取型选股', ChoseStocksSimple, {
                        'config': [
                            [True, '_rpa_te_', '执行时间', TimerExec, {
                                'times': [[14, 38]],  # 执行时间列表，二维数组，可指定多个时间点
                            }],
                            # -------- 选股前择时，降低无用计算 ----
                            [False, '_rpa_fbnb_', '北上资金择时', TimingByNorthMoney, {
                                'method': 'boll', # total_net_in or boll # 本策略中boll验证稍好
                            }], # 开启北上过滤时，无需再开北上择时，效果等同
                            # zzz - 回测分析发现开启大盘行情或开启“只允许做多”均不佳
                            [False, '_rpa_tbme_v_', '大盘择时-市场情绪-V', TimingByMarketEmotion, {
                                'subject': '000300.XSHG', # 判断标的 000926.XSHG不佳
                                'lag': 7,  # 大盘成交量监控周期
                                'field': 'volume',  # 判断字段 close / volume
                                'only_long': True  # 只允许做多行情
                            }],
                            # zzz - 回测发现动量因素有效，价格因素基本无效，故此注释掉
                            #[False, '_rpa_tbme_c_', '大盘择时-市场情绪-C', TimingByMarketEmotion, {
                            #    'subject': '000300.XSHG', # 判断标的
                            #    'lag': 10,  # 大盘成交量监控周期
                            #    'field': 'close',  # 判断字段 close / volume
                            #    'only_long': True  # 只允许做多行情
                            #}],
                            [True, '_rpa_fbnb_', '大盘择时-RSRS', TimingByMarketRSRS, {
                                'index_stock': '000300.XSHG'  # '000300.XSHG'
                            }],  # 尚可
                            [False, '_rpa_fbnb_', '大盘择时-市场安全度', TimingByMarketSafety, {
                                'index_stock': '000300.XSHG'  # '000300.XSHG'
                            }],  # 不佳
                            # -------- 特殊选股 --------
                            [True, '_rpa_pbhe_', '选取热门ETF', PickByHotETF, {
                                'buy_count': 5,  # 最多选取只数
                            }],  # 回测验证较好！RSRS优于北上择时
                            # -------- 通用选股 --------
                            [False, '_rpa_pbg_', '通用选股', PickByGeneral, {
                                'category': 'index',  # 'all', 'index', 'industry', 'concept'
                                 # index 指数成分股推荐值（20-10-8 ~ 21-1-8 不择时 alpha）：
                                 # * 000300.XSHG 沪深300:3.07，000905.XSHG 中证500:3.44
                                 # 399312.XSHE 国证300:1.94，399007.XSHE 深证300:0.29
                                 # 000852.XSHG 中证1000:-0.33，399311.XSHE 国证1000:1.61，399011.XSHE 深证1000:1.14
                                 # 399907.XSHE 中证中小盘700:2.47
                                 # 
                                 # 399006.XSHE 创业板指:0.88，399018.XSHE 创业板创新，399691.XSHE 创业板专利领先:0.09
                                 # 399016.XSHE 深证创新:0.09，399015.XSHE 深证中小创新
                                 # 000688.XSHG 科创50:0
                                 # 
                                 # 000932.XSHG 中证消费:-0.31，000990.XSHG 全指消费
                                 # 000997.XSHG 大消费，399385.XSHE 1000消费
                                 # 399912.XSHE 沪深300主要消费:-0.22，399931.XSHE 中证可选消费
                                 # 
                                 # 399394.XSHE 国证医药，399386.XSHE 1000医药
                                 # 000933.XSHG 中证医药:0.12，000991.XSHG 全指医药
                                 #
                                 # 000832.XSHG 中证转债:0，399307.XSHE 深证转债，399298.XSHE 深证中高信用债
                                 # 
                                 # * 000926.XSHG 中证央企:4.34，000042.XSHG 上证央企:-0.06
                                 # 000825.XSHG 央企红利:0.52，000927.XSHG 央企100:0.88
                                 # 399335.XSHE 深证央企:0.39，399926.XSHE 中证央企综合:3.53
                                 # 000955.XSHG 中证国企:0.76，000960.XSHG 中证龙头:-0.33
                                 # 000980.XSHG 中证超大:0.48，399803.XSHE 中证工业4.0:-0.12，399808.XSHE 中证新能源
                                 # 399811.XSHE 中证申万电子行业:-0.68，399812.XSHE 中证养老产业，
                                 # 399813.XSHE 中证国防安全:0.33，399939.XSHE 中证民营企业200:-0.34
                                 # 399961.XSHE 中证上游资源产业:0.68，399962.XSHE 中证中游制造产业:0.33，399963.XSHE 中证下游消费服务产业:1.69
                                'obj_list': ['000926.XSHG']
                            }],
                            [True, '_rpa_fbcm_', '一般性过滤', FilterByCommon, {
                                'filters': ['st', 'high_limit', 'low_limit', 'pause', 'kechuang', 'chuangye']
                            }],
                            # -------- 选股过滤 --------
                            [False, '_rpa_fbso_', '按交易数据排序过滤', FilterBySortOHLCV, {
                                # 字段名，如 day_open or 'open', 'close', 'low', 'high', 'volume',
                                # 'money', 'factor', 'high_limit','low_limit', 'avg', 'pre_close'
                                'field': 'day_open',
                                'price_range': [0, 10000],  # 价格范围
                                'asc': False,  # 是否正序排序
                                'count': 10  # 选取数量
                            }],
                            [False, '_rpa_fbnm_', '北向资金持股过滤', FilterByNorthMoney, {
                                'method': 'share_ratio_delta',  # share_ratio / share_ratio_delta / share_ratio_delta_ratio
                                'buy_count': 5,  # 最多选取只数
                            }],
                            [False, '_rpa_fbrg_', 'RSI通用趋势过滤', FilterByRsiGeneral, {
                                'index_stock': None,  # 若设置，则只按指标股分析趋势
                                'days_slow': 14,
                                'days_fast': 5,
                                'calc_type': 'close',
                                'calc_types': ['close', 'high', 'low', 'volume'],
                                'allow_range': [(0, 100)],
                                'allow_trend': ['up', 'sideway', 'down'],
                                'enable_minute_data': False  # 是否补充当时分钟数据
                            }],
                            [False, '_rpa_fbnm_', '根据指标股趋势过滤', FilterByTrend, {
                                'tech_type': 'RSI',  # RSI or safety
                                'rsi_allow': ['up', 'sideway', 'low'],
                                'index_stock': None,  # 若设置，则只按指标股分析趋势
                                'days_fast': 5,
                                'days_slow': 12
                            }],
                            [False, '_rpa_fbmt_', '根据市场温度过滤', FilterByMarketTemperature, {
                                'stock_list_only': False,  # 只基于待过滤股票判断市场温度
                                'temperature_range': (-100, 60000),  # 最小允许温度
                                'calc_start_time': (9, 33),  # 什么时间点后开始计算生效
                                'calc_every_bar': False,  # 是否每个时间周期都重新计算
                                'up_rate_min': 0.025,  # 判断为上涨的最小幅度
                                'down_rate_min': 0.025,  # 判断为下跌的最小幅度
                            }],
                            #[False, '_rpa_fbr_', '股票评分过滤', FilterByRank, {
                            #    'rank_stock_count': 20  # 评分股数
                            #}],
                            #[False, '_rpa_fbcfr_', '庄股评分选取', FilterByCashFlowRank, {
                            #    'rank_stock_count': 600
                            #}],
                            # -------- 截取数量 --------
                            [True, '_rpa_fbc', '选股数量', FilterByBuyCount, {
                                'buy_count': 3
                            }],
                        ],
                        'day_only_run_one': True,
                        'exclusive': True  # 是否开启独占模式（买入时也应不指定 buy_count）
                    }],
                    # 5.2 防守型选股（必选）：
                    [True, '_rp_conserve_', '防守型选股', ChoseStocksSimple, {
                        'config': [
                            [True, '_rpc_fr_', '配对轮动选股', PickByCointPairs, {
                                #'buy_count': 6,  # 选股数量b
                                'inter': 0.015,  # 价格波动阈值，突破则交易
                                'stocks_groups': [
                                    # 2021-05-14 最新验证有效的组合：
                                    {'inter': 0.012, 'stock_list': ['501038.XSHG', '161610.XSHE']},  # 银华明择, 融通领先  a=0.52 cool（银华明择难买入）
                                    {'inter': 0.012, 'stock_list': ['166023.XSHE', '160421.XSHE']},  # 中欧瑞丰, 华安智增  a=0.21 good
                                    {'inter': 0.012, 'stock_list': ['501028.XSHG', '163409.XSHE']},  # 财通福瑞, 兴全绿色  a=0.19 good
                                    {'inter': 0.008, 'stock_list': ['169101.XSHE', '169102.XSHE']},  # 东证睿丰, 东证睿阳  a=0.18 good
                                    {'inter': 0.008, 'stock_list': ['501046.XSHG', '159941.XSHE']},  # 财通福鑫, 纳指ETF  a=0.15 good
                                    {'inter': 0.008, 'stock_list': ['168102.XSHE', '168103.XSHE']},  # 九泰锐富, 九泰锐益  a=0.14 good
                                    {'inter': 0.008, 'stock_list': ['161810.XSHE', '164403.XSHE']},  # 银华内需, 农业精选  a=0.11 good
                                    {'inter': 0.008, 'stock_list': ['160518.XSHE', '161834.XSHE']},  # 博时睿远, 银华鑫锐  a=0.09 ok
                                    {'inter': 0.008, 'stock_list': ['501016.XSHG', '161706.XSHE']},  # 券商基金, 招商成长  a=0.05 ok
                                    ##{'inter': 0.008, 'stock_list': ['163409.XSHE', '502010.XSHG']},  # 兴全绿色, 证券LOF  a=0.12 good
                                ]
                            }],
                            #[True, '_rpc_fr', '股票评分', Filter_rank, {  # 与上述策略互斥
                            #    'rank_stock_count': 20  # 评分股数
                            #}],
                            #[False, '_rpc_fcfr', '庄股评分', Filter_cash_flow_rank, {'rank_stock_count': 600}],
                            #[True, '_rpc_fbc_', '选股数量', FilterByBuyCount, {
                            #    'buy_count': 5
                            #}],
                        ],
                        'group_fund_ratio': 1.0,  # 该选股组合占总资金比（0~1.0）
                        'day_only_run_one': False,  # 每天只选一次
                        'exclusive': False  # 是否独占选股
                    }],
                    [True, '_rp_fbc_', '最终选股数量', FilterByBuyCount, {
                        'buy_count': buy_count
                    }],
                ]
            }]
        ]

        ### 6. 择时规则：
        # 择时可以在选股后，也可以在选股时，根据需要调整

        rules_market_timing = [
            [False, '_rules_market_timing_', '择时规则', GroupRules, {
                'config': [
                ]
            }]
        ]

        ### 7. 调仓规则（必选）

        rules_adjust = [
            [True, '_rules_adjust_', '调仓规则', AdjustPosition, {
                'config': [
                    [False, '', '卖出股票', SellStocks, {}],
                    [False, '_apc_bse_', '买入股票', BuyStocksEqually , {
                        # 若不指定 buy_count 则根据实际只数平分买入。
                        # 不指定通常仅适用于实际只数经常不足，且为非协整的策略。
                        #'buy_count': buy_count,  # 购股数量（本例中为协整组合数量）
                        'deal_rate': 1.0,  # 成交比率（设置为 1.0 收益最好）
                    }],
                    [True, '_apc_bse_', '资金占比调整仓位', AdjustPositionByFundRatio, {
                    }],
                    #[False, '按比重买入股票', BuyStocksPortion, {'buy_count': buy_count}],
                    #[False, 'VaR 方式买入股票或调仓', BuyStocksVaR, {'buy_count': buy_count}],
                    [True, '_show_postion_adjust_',
                        '显示买卖的股票', ShowPostionAdjust, {}],
                    # 模拟盘调仓邮件通知，暂时只试过QQ邮箱，其它邮箱不知道是否支持
                    #[True, '_apc_en_', '调仓邮件通知执行器', Email_notice, {
                    #    'user': '1234567@qq.com', # QQmail
                    #    'password': '1234567', # QQmail密码
                    #    'tos': ["boss<crystalphi@gmail.com>"], # 接收人Email地址，可多个
                    #    'sender': '聚宽模拟盘', # 发送人名称
                    #    'strategy_name': g.strategy_memo, # 策略名称
                    #    'send_with_change': False, # 持仓有变化时才发送
                    #}],
                    #[True, '_apc__', '实盘同步', TraderShipane, {
                    #    'server_type': 'mybroker',
                    #    # 本策略必须为限价单，每天开盘后即开始同步，持续到交易结束
                    #    #TODO 验证远程同步小步市价单效果
                    #    'sync_with_change': True,  # 是否在发生调仓后执行同步（适合市价单）
                    #    #'sync_all_day': False,  # 是否全天执行同步（适合限价单，从交易前开始，聚宽等线性平台不适用）
                    #    #'sync_timers': [[14,50]],  # 定时同步时间（如 [[14,31],[14,50]]，影响时延，尽量不用或晚用）
                    #    #'sync_loop_times': 3,  # 每次同步开平仓循环次数（非全天同步时有效）
                    #    'use_limit_order': True,  # 是否使用限价单
                    #    'use_curr_price': True,  # 限价同步时若未找到调仓价格，是否使用当前价格
                    #    #'op_buy_ratio': 1.0,  # 资金投入比例 100%
                    #    'batch_amount': 5000,  # 每次分批操作股数（市价单一般应小于2800）
                    #    'sync_on_remote': True  # 是否在远程交易代理主机上做仓位同步（这将取代本地同步过程）
                    #}],
                ]
            }]
        ]

        ### 组合成一个总的策略

        g.strategy_config = (rules_basic
                             + rules_stop_loss
                             + rules_stop_profit
                             + rules_schedule
                             + rules_pick
                             + rules_market_timing
                             + rules_adjust)

    # ======== 执行策略配置测试 ========
    # 配置策略规则
    config_strategy_rules(context)
    # 组合策略规则
    g.strategy = StrategyRulesCombiner({'config': g.strategy_config,
                                        'g_class': GlobalVariable,
                                        'memo': g.strategy_memo,
                                        'name': '_main_'})
    g.strategy.initialize(context)

    # 打印策略参数
    #g.strategy.log.info(g.strategy.show_strategy())
    print(g.strategy.show_strategy())  # 命令行执行上面无输出，故这里改为 print
