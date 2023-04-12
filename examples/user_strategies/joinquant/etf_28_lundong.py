# 克隆自聚宽文章：https://www.joinquant.com/post/19490
# 标题：用指数战胜指数，ETF二八轮动对冲模型
# 作者：东南有大树

# 导入函数库
import numpy as np
import pandas as pd
import datetime
#from jqdata import *
from alphabot.user.api_for_jq import *

# ！！必须指定本策略所有可能交易的标的 - 基于 backtrader 框架的引擎需要该信息
all_security_pool = [
    '000300.XSHG', '000905.XSHG', '510300.XSHG', '510500.XSHG',
    '150051.XSHE', '511010.XSHG']

'''
================================================================================
总体回测前调用
================================================================================
'''
# 初始化函数，设定基准等
def initialize(context):
    print('--> initialize context:', context)
    # 设定沪深 300 作为基准
    set_benchmark('000300.XSHG') # default 000300.XSHG
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 过滤掉 order 系列 API 产生的比 error 级别低的 log
    log.set_level('order', 'error')
    # 股票类每笔交易时的手续费是：买入时佣金万分之三，卖出时佣金万分之三加千分之一印花税, 每笔交易佣金最低扣 5 块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.0003, close_commission=0.0003, min_commission=5), type='stock')
    
    # 设置模型参数
    set_parms()
    
    # 运行
    run_daily(market_trade, time='14:53', reference_security='000300.XSHG')
    log.info('----------------- 策略执行开始 -------------------')
    
# 设置参数
def set_parms():
    g.cycle = 20  # 设置计算动量的周期
    g.index_list = ['000300.XSHG', '000905.XSHG'] # []  # 指数列表
    g.etf_list = ['510300.XSHG', '510500.XSHG']  # ETF列表
    g.bond = '150051.XSHE'  # 负相关品种
    # g.bond = '511010.XSHG'  # 国债ETF
    g.ratio = 0.00  # 动量正值最小幅度
    
'''
================================================================================
每天开盘前调用
================================================================================
'''
def before_trading_start(context):
    #print('--> call before_trading_start..')
    # 将滑点设置为 0
    set_slippage(FixedSlippage(0))
    # 根据不同的时间段设置手续费
    #print('--> context.current_dt:', context.current_dt)
    dt = context.current_dt
    if dt > datetime.datetime(2013,1, 1):
        set_commission(PerTrade(buy_cost=0.0003, sell_cost=0.0013, min_cost=5)) 
    elif dt > datetime.datetime(2011,1, 1):
        set_commission(PerTrade(buy_cost=0.001, sell_cost=0.002, min_cost=5))
    elif dt > datetime.datetime(2009,1, 1):
        set_commission(PerTrade(buy_cost=0.002, sell_cost=0.003, min_cost=5))
    else:
        set_commission(PerTrade(buy_cost=0.003, sell_cost=0.004, min_cost=5))

'''
================================================================================
每个市场交易周期完成后调用（每天或者每分钟等）
================================================================================
'''
#每个交易数据周期需要运行的函数
def handle_data(context, data):
    #print('--> handle_data# %s, %s' % (context.current_dt, data))
    return

'''
================================================================================
每天收盘后调用
================================================================================
'''
# 本策略中不需要
def after_trading_end(context):
    log.info('*'*50)

'''
================================================================================
策略所需自定义函数
================================================================================
'''
# 每天定时运行（建议在收盘前）
def market_trade(context):
    signal = get_signal(context)
    log.info('信号 %s' % signal)
    if signal == 'clear' and len(context.portfolio.positions) > 0:
        for etf in context.portfolio.positions.keys():
            order_target_value(etf, 0) 
            log.info('卖出 ' + get_security_info(etf).display_name)
    elif (signal in g.etf_list):
        if g.bond in context.portfolio.positions.keys():
            order_target_value(g.bond, 0) 
            log.info('卖出 ' + get_security_info(g.bond).display_name)
        order_value(signal, context.portfolio.cash)
        log.info('买入 ' + get_security_info(signal).display_name)
    elif signal == g.bond:
        for etf in context.portfolio.positions.keys():
            order_target_value(etf, 0)
            log.info('卖出 ' + get_security_info(etf).display_name)
        order_value(g.bond, context.portfolio.cash)
        log.info('买入 ' + get_security_info(g.bond).display_name)
    
# 获取交易信号
def get_signal(context):
    hold = context.portfolio.positions.keys()
    # 计算全部标的的动量值
    power_df = pd.DataFrame({e: {'power': get_power(i, g.cycle)}\
        for i, e in zip(g.index_list, g.etf_list)}).T
    # 判断是否需要买入国债
    if g.bond not in hold and False not in [True if power_df.loc[i, 'power'] <= 0 else False for i in power_df.index]:
        return g.bond
    # 判断持仓标的是否出现负动量
    elif [i for i in power_df.index if i in hold and power_df.loc[i, 'power'] <= 0] and g.bond not in hold:
        return 'clear'
    # 判断哪个标的的动量值大于0，选择最大的
    elif power_df.sort_values(by=['power'])['power'][-1] > g.ratio and len(set(g.etf_list) & set(hold)) <= 0:
        return power_df.sort_values(by=['power']).index[-1]

# 计算动量值
def get_power(stock, interval=20):
    h = attribute_history(stock, interval, unit='1d', fields=('close'), skip_paused=True)
    #print('--> h:', h)
    return (h['close'].values[-1] - h['close'].values[0]) / h['close'].values[0]
