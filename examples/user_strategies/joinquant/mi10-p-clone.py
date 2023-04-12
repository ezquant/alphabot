# 克隆自聚宽文章：https://www.joinquant.com/post/29406
# 标题：20年最优策略，推荐给大家，公寓变三房
# 作者：水煮花生

#enable_profile()
# 导入函数库
#from jqdata import *
from alphabot.user.api_for_jq import *


# 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 输出内容到日志 log.info()
    log.info('初始函数开始运行且全局只运行一次')
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')

    ### 股票相关设定 ###
    # 股票类每笔交易时的手续费是：买入时佣金万分之2.5，卖出时佣金万分之2.5加千分之一印花税, 每笔交易佣金最低扣5块钱
    set_order_cost(OrderCost(close_tax=0.001, open_commission=0.00025, close_commission=0.00025, min_commission=5), type='stock')
    
    g.stocksnum = 5
    run_daily(period,time='9:00')
    
def period(context):
    stockcode_list = get_index_stocks('000001.XSHG')+get_index_stocks('399106.XSHE')
    all_data = get_current_data()
    stockcode_list = [stockcode for stockcode in stockcode_list if not all_data[stockcode].paused]
    stockcode_list = [stockcode for stockcode in stockcode_list if not all_data[stockcode].is_st]
    stockcode_list = [stockcode for stockcode in stockcode_list if'退' not in all_data[stockcode].name]    
    q=query(valuation.code).filter(valuation.code.in_(stockcode_list),valuation.pe_ratio >0).order_by(valuation.market_cap.asc()).limit(g.stocksnum)
    #q=query(valuation.code).filter(valuation.code.in_(stockcode_list)).order_by(valuation.circulating_market_cap.asc()).limit(g.stocksnum)
    df = get_fundamentals(q)
    
    buylist = list(df['code'])
    
    #先检查仓位，止盈止损；仓位空了后加米5
    for stk in context.portfolio.positions:
        keynum = context.portfolio.positions[stk].security
        cost = context.portfolio.positions[stk].avg_cost
        price = context.portfolio.positions[stk].price
        value = context.portfolio.positions[stk].value
        ret = price/cost - 1
        print('股票(%s)共有:%s,成本价:%s,现价:%s,收益率:%s' % (keynum,value,cost,price,ret))
        
        #五天均线策略
        close_data = attribute_history(stk,5,'1d',['close'])
        MA5 = close_data['close'].mean()
        current_price = close_data['close'][-1]
        if current_price > 1.1*MA5:
            if order(stk,200) != None:
                print('股票%s突破5日均线，加手' % keynum)

        #止盈止损清仓    
        if ret > 1:
            if order_target(stk,0) != None:
                print('触发+100%的止盈线,清仓股票'+ keynum)
        elif ret < -0.1:
            if order_target(stk,0) != None:
                print('触发-10%的止损线,清仓股票'+ keynum)
            
            
    cash_ratio = context.portfolio.available_cash/context.portfolio.total_value        
    if cash_ratio >0.5:
        for stk in buylist:
            order(stk,200)
            
    print('当前账户总资产:%s,持仓:%s' %(context.portfolio.total_value,context.portfolio.positions_value))

"""    
    #先秀仓，然后清仓，最后按市值各买1手
    print('当前时间:%s,总资产:%s,持仓价值%s' % (context.current_dt,context.portfolio.total_value,context.portfolio.positions_value))
    for stk in context.portfolio.positions:
        cost = context.portfolio.positions[stk].avg_cost
        price = context.portfolio.positions[stk].price
        value = context.portfolio.positions[stk].value
        ret = price/cost - 1
        print('股票代码%s持仓%s,成本:%s,现价:%s,收益率:%s' % (stk,value,cost,price,ret))
        
    for stk in context.portfolio.positions:
        order_target(stk,0)
    
    postion_per_stk = context.portfolio.cash/g.stocksnum
    for stk in buylist:
        order_value(stk,postion_per_stk)
"""




"""
    ## 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'000300.XSHG'或'510300.XSHG'是一样的）
      # 开盘前运行
    run_daily(before_market_open, time='before_open', reference_security='000300.XSHG')
      # 开盘时运行
    run_daily(market_open, time='open', reference_security='000300.XSHG')
      # 收盘后运行
    run_daily(after_market_close, time='after_close', reference_security='000300.XSHG')

## 开盘前运行函数
def before_market_open(context):
    # 输出运行时间
    log.info('函数运行时间(before_market_open)：'+str(context.current_dt.time()))

    # 给微信发送消息（添加模拟交易，并绑定微信生效）
    # send_message('美好的一天~')

    # 要操作的股票：平安银行（g.为全局变量）
    g.security = '000001.XSHE'

## 开盘时运行函数
def market_open(context):
    log.info('函数运行时间(market_open):'+str(context.current_dt.time()))
    security = g.security
    # 获取股票的收盘价
    close_data = get_bars(security, count=5, unit='1d', fields=['close'])
    # 取得过去五天的平均价格
    MA5 = close_data['close'].mean()
    # 取得上一时间点价格
    current_price = close_data['close'][-1]
    # 取得当前的现金
    cash = context.portfolio.available_cash

    # 如果上一时间点价格高出五天平均价1%, 则全仓买入
    if (current_price > 1.01*MA5) and (cash > 0):
        # 记录这次买入
        log.info("价格高于均价 1%%, 买入 %s" % (security))
        # 用所有 cash 买入股票
        order_value(security, cash)
    # 如果上一时间点价格低于五天平均价, 则空仓卖出
    elif current_price < MA5 and context.portfolio.positions[security].closeable_amount > 0:
        # 记录这次卖出
        log.info("价格低于均价, 卖出 %s" % (security))
        # 卖出所有股票,使这只股票的最终持有量为0
        order_target(security, 0)

## 收盘后运行函数
def after_market_close(context):
    log.info(str('函数运行时间(after_market_close):'+str(context.current_dt.time())))
    #得到当天所有成交记录
    trades = get_trades()
    for _trade in trades.values():
        log.info('成交记录：'+str(_trade))
    log.info('一天结束')
    log.info('##############################################################')
"""
