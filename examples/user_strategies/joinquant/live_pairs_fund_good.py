"""
逻辑：全时策略的子策略。基于新策略框架，实现基金协整。
特点：收益稳定，回撤小，适合全行情。alpha较高，但实盘只能限价单，跟单可能不易。
分析：1、基金协整部分需每半年定期调整组合（当前为2020-12-12）。
      2、经实盘对比分析验证，基金协整策略只适合限价单！（虽然会有成交不足问题）
         - 沪市市价单只支持最优五档，流动性不足，存在被对方调价套利的情况。
         - 深市市价单支持5种委托模式，流动性较好，但回测验证几无套利空间。

TODO: 协整防守策略，每隔6月重新分析和替换新的股票对，包括相关参数。
TODO: 协整防守策略，股票调整替换能否全自动化？
"""

import talib as tl
# 使用本地系统提供的API封装，替换聚宽在线API
from alphabot.user.api_for_jq import *
from .strategy_core import *

# 聚宽策略移植到本地系统后，必须补充设置全部股票池才能正常加载数据
all_security_pool = [
    #'000001.XSHG', '000002.XSHG', # test
    '601398.XSHG', '601288.XSHG','601939.XSHG','601988.XSHG',
    # 以下均为基金，貌似 QUANTAIX 中引入没有相关数据
    #'160518.XSHE', '161610.XSHE', '166023.XSHE', '160421.XSHE',
    #'501028.XSHG', '163409.XSHE', '169101.XSHE', '169102.XSHE',
    #'501046.XSHG', '159941.XSHE', '168102.XSHE', '168103.XSHE',
    #'161810.XSHE', '164403.XSHE', '501016.XSHG', '161706.XSHE'
]


def config_strategy_rules(context):
        """ 策略配置规则示例 """

        g.strategy_memo = '策略示例'
        # 定义 log 输出的类类型，必须指定。如需自定义log，可继承修改之。
        g.log_type = RuleLoger
        # 判断是运行模式，回测还是模拟
        g.is_sim_trade = context.run_params.type == 'sim_trade'
        index2 = '000016.XSHG'  # 大盘指数
        index8 = '399333.XSHE'  # 小盘指数
        #index8 = '399678.XSHE'  # 深次新指数
        buy_count = 1

        ### 1. 基础规则（必选）：系统参数、交易费率、统计信息等

        rules_basic = [
            [True, '_rules_basic_', '基础规则', GroupRules, {
                'config': [
                    [True, '', '设置系统参数', Set_sys_params, {
                        'benchmark': '000300.XSHG'  # 指定基准为次新股指
                    }],
                    [True, '', '手续费设置器', Set_slip_fee, {
                        # 固定滑点, 默认为0.02 市价交易时各加减x/2元
                        #'slippage': 0.02  # 市价单必须设置，沪市建议0.050，深市建议0.015
                        'slippage': 0.002  # 限价单可设为0
                    }],
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
                        'dst_drop_minute_count': 90,  # 可选，在三乌鸦触发情况下，一天之内有多少分钟涨幅<0,则触发止损，默认60分钟
                    }],
                    [False, '_SL_by_indexs_increase', '多指数N日涨幅止损器', StopLossByIndexsIncrease, {
                        'indexs': [index2, index8],
                        'n': 20,
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

        rules_chose_stocks = [
            [True, '_rules_chose_stocks_', '多策略选股', ChoseStocksMulti, {
                'config': [
                    # 攻守全时选股策略 - 组合进取策略、防守策略和择时规则，获取最终选股结果
                    # 5.1 进取型选股（可选）：
                    [False, '_rp_aggress_', '进取型选股', ChoseStocksSimple, {
                        'config': [
                        ],
                        'day_only_run_one': True,
                        'exclusive': True  # 是否开启独占模式（买入时也应不指定 buy_count）
                    }],
                    # 5.2 防守型选股（必选）：
                    [True, '_rp_conserve_', '防守型选股', ChoseStocksSimple, {
                        'config': [
                            # -------- 选股前择时，降低无用计算 ----
                            [False, '_rpa_fbnb_', '魔力K线择时', TimingByMagicKLine, {
                                'index_stock': '000001.XSHG'
                            }], # 较好
                            [False, '_rpa_fbnb_', '北上资金择时', TimingByNorthMoney, {
                                'method': 'boll', # total_net_in or boll # 本策略中boll验证稍好
                            }], # 几无影响
                            [False, '_rpa_tbme_v_', '大盘择时-市场情绪-V', TimingByMarketEmotion, {
                                'subject': '000300.XSHG', # 判断标的 000926.XSHG不佳
                                'lag': 7,  # 大盘成交量监控周期 7较好
                                'field': 'volume',  # 判断字段 close 或 volume
                                'only_long': False  # 只允许做多行情
                            }],  # 略好
                            [False, '_rpa_tbmrs_', '指数RSRS择时', TimingByMarketRSRS, {
                                'index_stock': '000300.XSHG',  # '000300.XSHG'
                                'N': 18,  # 统计周期
                                'M': 200,  # 统计样本长度
                                'buy': 0.85,  # 买入斜率
                                'sell': -0.85  # 卖出斜率
                            }],  # 几无影响
                            [False, '_rpa_tbms_', '市场安全度择时', TimingByMarketSafety, {
                                'index_stock': '000300.XSHG'  # '000300.XSHG'
                            }],  # 不佳
                            # -------- 选股 --------
                            [True, '_rpc_pbcp_', '银行配对协整', PickByCointPairs, {
                                'buy_count': 1,  # 选股数量
                                'stocks_groups': [
                                    # 四大行协整
                                    #{'inter': 0.010, 'stock_list': ['000001.XSHG', '000002.XSHG']},  # test
                                    {'inter': 0.010, 'stock_list': ['601398.XSHG', '601288.XSHG','601939.XSHG','601988.XSHG']},  # 工农建中
                                ]
                            }],
                            [False, '_rpc_pbcp_', '基金配对协整', PickByCointPairs, {
                                #'buy_count': 6,  # 选股数量
                                'inter': 0.015,  # 价格波动阈值，突破则交易
                                'stocks_groups': [
                                    # 费率分析：券商佣金万1.5，买卖来回手续费合计万3，加上万10的印花税，
                                    # 这样，T+0的来回成本是万13=0.13%，如5元的票大概不到1分差价
                                    # 基金一般都在3元以内，成本+滑点比率约为：0.0013+0.002/3 ~= 0.003
                                    # （华泰证券基金费率只有0.03%，这样成本滑点比率约为0.002）
                                    
                                    # inter值应大于成本滑点比，且尽可能每天交易。该值越大，盈利价差越大，但交易频率也越低。
                                    # 一般设在0.015~0.025之间为宜，建议不低于0.008（每天全换仓收益0.005，按实盘1/5计，年化28%）。
                                    
                                    # 选择组合时，p值并非越低越好，需经回测验证。p值在 0.04 左右时收益貌似更好
                                    # 从回测结果看，市场套利机会日益降低。沪深市场间略高于同市场。
                                    
                                    # 2021-05-14 最新验证有效的组合：
                                    {'inter': 0.015, 'stock_list': ['160518.XSHE', '161610.XSHE']},  # 博时睿远, 融通领先  a=1.65 cool
                                    {'inter': 0.015, 'stock_list': ['166023.XSHE', '160421.XSHE']},  # 中欧瑞丰, 华安智增  a=0.21 good
                                    {'inter': 0.015, 'stock_list': ['501028.XSHG', '163409.XSHE']},  # 财通福瑞, 兴全绿色  a=0.19 good
                                    {'inter': 0.012, 'stock_list': ['169101.XSHE', '169102.XSHE']},  # 东证睿丰, 东证睿阳  a=0.18 good
                                    {'inter': 0.012, 'stock_list': ['501046.XSHG', '159941.XSHE']},  # 财通福鑫, 纳指ETF  a=0.15 good
                                    {'inter': 0.012, 'stock_list': ['168102.XSHE', '168103.XSHE']},  # 九泰锐富, 九泰锐益  a=0.14 good
                                    {'inter': 0.012, 'stock_list': ['161810.XSHE', '164403.XSHE']},  # 银华内需, 农业精选  a=0.11 good
                                    {'inter': 0.012, 'stock_list': ['501016.XSHG', '161706.XSHE']},  # 券商基金, 招商成长  a=0.05 ok
                                    {'inter': 0.008, 'stock_list': ['163409.XSHE', '502010.XSHG']},  # 兴全绿色, 证券LOF  a=0.12 good
                                    ##{'inter': 0.012, 'stock_list': ['501038.XSHG', '161610.XSHE']},  # 银华明择, 融通领先  a=0.52 cool（银华明择难买入）
                                    ##{'inter': 0.010, 'stock_list': ['160518.XSHE', '161834.XSHE']},  # 博时睿远, 银华鑫锐  a=0.09 ok（银华鑫锐难买入）
                                    
                                    # 发现一个现象：直接使用分析出来的协整较好的组合做回测，收益往往不佳；
                                    # 手动换成有第三方交集的股票对，反而效果较好。比如：
                                    #{'inter': 0.012, 'stock_list': ['163409.XSHE', '161706.XSHE']},  # p=0.0x 兴全绿色, 招商成长  a=0.01 ok
                                    #{'inter': 0.008, 'stock_list': ['163409.XSHE', '161706.XSHE']},  # p=0.0x 兴全绿色, 招商成长  a=0.07 good
                                    #{'inter': 0.008, 'stock_list': ['159928.XSHE', '512600.XSHG']},  # p=0.0x 消费ETF, 主要消费  a=0.03 ok
                                    
                                    # 以下为 2021-05-13 重新分析出的组合（0.01<=p<=0.05，日均成交>200w）
                                    # 回测期间 21-04-12 ~ 21-05-12
                                    
                                    # 详情：非ETF
                                    #{'inter': 0.008, 'stock_list': ['501016.XSHG', '501046.XSHG']},  # p=0.01 券商基金, 财通福鑫
                                    #{'inter': 0.008, 'stock_list': ['161706.XSHE', '163412.XSHE']},  # p=0.01 招商成长, 兴全轻资
                                    #{'inter': 0.008, 'stock_list': ['163415.XSHE', '502050.XSHG']},  # p=0.01 兴全模式, 上证50B
                                    #{'inter': 0.008, 'stock_list': ['163412.XSHE', '502050.XSHG']},  # p=0.02 兴全轻资, 上证50B
                                    #{'inter': 0.008, 'stock_list': ['162605.XSHE', '163415.XSHE']},  # p=0.02 景顺鼎益, 兴全模式  a=-0.01 -
                                    #{'inter': 0.008, 'stock_list': ['161903.XSHE', '163415.XSHE']},  # p=0.03 万家优选, 兴全模式  a=-0.07 bad
                                    #{'inter': 0.008, 'stock_list': ['161903.XSHE', '163417.XSHE']},  # p=0.04 万家优选, 兴全合宜  a=-0.01 bad
                                    #{'inter': 0.008, 'stock_list': ['161706.XSHE', '163402.XSHE']},  # p=0.04 招商成长, 兴全趋势
                                    #{'inter': 0.008, 'stock_list': ['161810.XSHE', '164403.XSHE']},  # p=0.04 银华内需, 农业精选  a=0.11 good
                                    #{'inter': 0.008, 'stock_list': ['161903.XSHE', '168102.XSHE']},  # p=0.04 万家优选, 九泰锐富
                                    #{'inter': 0.008, 'stock_list': ['161903.XSHE', '163412.XSHE']},  # p=0.04 万家优选, 兴全轻资  a=-0.08 bad
                                    #{'inter': 0.008, 'stock_list': ['160133.XSHE', '502050.XSHG']},  # p=0.04 南方天元, 上证50B （无成交）
                                    #{'inter': 0.008, 'stock_list': ['501046.XSHG', '502050.XSHG']},  # p=0.04 财通福鑫, 上证50B
                                    #{'inter': 0.008, 'stock_list': ['162605.XSHE', '502050.XSHG']},  # p=0.04 景顺鼎益, 上证50B
                                    #{'inter': 0.008, 'stock_list': ['161903.XSHE', '162605.XSHE']},  # p=0.05 万家优选, 景顺鼎益  a=-0.04 bad
                                    #{'inter': 0.008, 'stock_list': ['501016.XSHG', '161706.XSHE']},  # p=0.05 券商基金, 招商成长  a=0.05 good
                                    #{'inter': 0.008, 'stock_list': ['161706.XSHE', '161130.XSHE']},  # p=0.05 招商成长, 纳指LOF
                                    #{'inter': 0.008, 'stock_list': ['161903.XSHE', '163409.XSHE']},  # p=0.05 万家优选, 兴全绿色
                                    #{'inter': 0.008, 'stock_list': ['163409.XSHE', '502010.XSHG']},  # p=0.05 兴全绿色, 证券LOF  a=0.12 good
                                    #{'inter': 0.008, 'stock_list': ['161128.XSHE', '163409.XSHE']},  # p=0.05 标普科技, 兴全绿色
                                    #{'inter': 0.008, 'stock_list': ['161903.XSHE', '161130.XSHE']},  # p=0.06 万家优选, 纳指LOF  a=-0.12 bad
                                    #{'inter': 0.008, 'stock_list': ['161128.XSHE', '161130.XSHE']},  # p=0.06 标普科技, 纳指LOF
                                    #{'inter': 0.008, 'stock_list': ['161903.XSHE', '168105.XSHE']},  # p=0.06 万家优选, 九泰泰富

                                    # 详情：全部基金
                                    #{'inter': 0.008, 'stock_list': ['510800.XSHG', '163415.XSHE']},  # p=0.01 上证50, 兴全模式  a=-0.04 bad
                                    #{'inter': 0.012, 'stock_list': ['166001.XSHE', '512090.XSHG']},  # p=0.01 中欧趋势, MSCI易基
                                    #{'inter': 0.012, 'stock_list': ['512700.XSHG', '512090.XSHG']},  # p=0.01 银行基金, MSCI易基
                                    #{'inter': 0.008, 'stock_list': ['512700.XSHG', '512550.XSHG']},  # p=0.01 银行基金, 富时A50  a=0.02 ok
                                    #{'inter': 0.012, 'stock_list': ['510050.XSHG', '163409.XSHE']},  # p=0.01 50ETF, 兴全绿色
                                    #{'inter': 0.012, 'stock_list': ['512700.XSHG', '512160.XSHG']},  # p=0.01 银行基金, MSCI基金
                                    #{'inter': 0.012, 'stock_list': ['510050.XSHG', '166001.XSHE']},  # p=0.01 50ETF, 中欧趋势
                                    #{'inter': 0.008, 'stock_list': ['512520.XSHG', '161706.XSHE']},  # p=0.01 MSCIETF, 招商成长  a=-0.03 bad
                                    #{'inter': 0.012, 'stock_list': ['510800.XSHG', '163412.XSHE']},  # p=0.01 上证50, 兴全轻资
                                    #{'inter': 0.012, 'stock_list': ['161706.XSHE', '163412.XSHE']},  # p=0.01 招商成长, 兴全轻资
                                    #{'inter': 0.012, 'stock_list': ['512600.XSHG', '166001.XSHE']},  # p=0.01 主要消费, 中欧趋势
                                    #{'inter': 0.008, 'stock_list': ['510630.XSHG', '512600.XSHG']},  # p=0.01 消费行业, 主要消费
                                    #{'inter': 0.008, 'stock_list': ['510050.XSHG', '162605.XSHE']},  # p=0.01 50ETF, 景顺鼎益  a=-0.09 bad
                                    #{'inter': 0.012, 'stock_list': ['512700.XSHG', '512520.XSHG']},  # p=0.02 银行基金, MSCIETF
                                    #{'inter': 0.012, 'stock_list': ['510180.XSHG', '163412.XSHE']},  # p=0.02 180ETF, 兴全轻资
                                    #{'inter': 0.008, 'stock_list': ['161903.XSHE', '159928.XSHE']},  # p=0.02 万家优选, 消费ETF  a=-0.03 bad
                                    #{'inter': 0.008, 'stock_list': ['510800.XSHG', '163417.XSHE']},  # p=0.02 上证50, 兴全合宜  a=-0.09 bad
                                    #{'inter': 0.012, 'stock_list': ['163412.XSHE', '510710.XSHG']},  # p=0.02 兴全轻资, 上50ETF
                                    #{'inter': 0.008, 'stock_list': ['161903.XSHE', '512600.XSHG']},  # p=0.02 万家优选, 主要消费  a=0.01 ok
                                    #{'inter': 0.012, 'stock_list': ['510710.XSHG', '163415.XSHE']},  # p=0.02 上50ETF, 兴全模式
                                    #{'inter': 0.008, 'stock_list': ['166001.XSHE', '510180.XSHG']},  # p=0.02 中欧趋势, 180ETF  a=-0.04 bad
                                    #{'inter': 0.008, 'stock_list': ['512700.XSHG', '510710.XSHG']},  # p=0.02 银行基金, 上50ETF  a=-0.02 bad
                                    #{'inter': 0.008, 'stock_list': ['163409.XSHE', '512570.XSHG']},  # p=0.02 兴全绿色, 中证证券  a=0.09 ok
                                    #{'inter': 0.008, 'stock_list': ['512900.XSHG', '163409.XSHE']},  # p=0.02 证券基金, 兴全绿色  a=0.04 ok
                                    #{'inter': 0.008, 'stock_list': ['510050.XSHG', '163412.XSHE']},  # p=0.02 50ETF, 兴全轻资  a=-0.01 bad
                                    #{'inter': 0.008, 'stock_list': ['501046.XSHG', '159941.XSHE']},  # p=0.02 财通福鑫, 纳指ETF  a=0.15 cool
                                    #{'inter': 0.012, 'stock_list': ['163409.XSHE', '512070.XSHG']},  # p=0.02 兴全绿色, 证券保险  a=0.00 -
                                    #{'inter': 0.012, 'stock_list': ['161706.XSHE', '512070.XSHG']},  # p=0.02 招商成长, 证券保险  a=-0.07 bad
                                    #{'inter': 0.008, 'stock_list': ['162605.XSHE', '163415.XSHE']},  # p=0.02 景顺鼎益, 兴全模式  a=-0.01 bad
                                    #{'inter': 0.012, 'stock_list': ['510630.XSHG', '510710.XSHG']},  # p=0.02 消费行业, 上50ETF
                                    #{'inter': 0.008, 'stock_list': ['510050.XSHG', '163415.XSHE']},  # p=0.03 50ETF, 兴全模式  a=-0.02 bad
                                    #{'inter': 0.012, 'stock_list': ['163409.XSHE', '510710.XSHG']},  # p=0.03 兴全绿色, 上50ETF
                                    #{'inter': 0.012, 'stock_list': ['512600.XSHG', '510710.XSHG']},  # p=0.03 主要消费, 上50ETF
                                    #{'inter': 0.012, 'stock_list': ['510800.XSHG', '512600.XSHG']},  # p=0.03 上证50, 主要消费
                                    #{'inter': 0.012, 'stock_list': ['512810.XSHG', '510180.XSHG']},  # p=0.03 军工行业, 180ETF
                                    #{'inter': 0.008, 'stock_list': ['510630.XSHG', '510050.XSHG']},  # p=0.03 消费行业, 50ETF  a=-0.04 bad
                                    #{'inter': 0.008, 'stock_list': ['512160.XSHG', '163415.XSHE']},  # p=0.03 MSCI基金, 兴全模式  a=0.01 -
                                    #{'inter': 0.012, 'stock_list': ['512550.XSHG', '168103.XSHE']},  # p=0.03 富时A50, 九泰锐益
                                    #{'inter': 0.012, 'stock_list': ['510800.XSHG', '163402.XSHE']},  # p=0.03 上证50, 兴全趋势
                                    #{'inter': 0.012, 'stock_list': ['161130.XSHE', '510180.XSHG']},  # p=0.03 纳指LOF, 180ETF
                                    #{'inter': 0.012, 'stock_list': ['512880.XSHG', '163409.XSHE']},  # p=0.03 证券ETF, 兴全绿色
                                    #{'inter': 0.012, 'stock_list': ['510180.XSHG', '163415.XSHE']},  # p=0.03 180ETF, 兴全模式
                                    #{'inter': 0.008, 'stock_list': ['512700.XSHG', '510050.XSHG']},  # p=0.03 银行基金, 50ETF  a=-0.02 bad
                                    #{'inter': 0.012, 'stock_list': ['166001.XSHE', '159928.XSHE']},  # p=0.03 中欧趋势, 消费ETF
                                    #{'inter': 0.008, 'stock_list': ['161903.XSHE', '163415.XSHE']},  # p=0.03 万家优选, 兴全模式  a=-0.07 bad
                                    #{'inter': 0.012, 'stock_list': ['160133.XSHE', '510050.XSHG']},  # p=0.03 南方天元, 50ETF
                                    #{'inter': 0.012, 'stock_list': ['510800.XSHG', '159928.XSHE']},  # p=0.03 上证50, 消费ETF
                                    #{'inter': 0.012, 'stock_list': ['161903.XSHE', '166001.XSHE']},  # p=0.03 万家优选, 中欧趋势
                                    #{'inter': 0.012, 'stock_list': ['510800.XSHG', '162605.XSHE']},  # p=0.03 上证50, 景顺鼎益  a=-0.09 bad
                                    #{'inter': 0.012, 'stock_list': ['512550.XSHG', '512090.XSHG']},  # p=0.03 富时A50, MSCI易基
                                    #{'inter': 0.012, 'stock_list': ['512700.XSHG', '510180.XSHG']},  # p=0.03 银行基金, 180ETF
                                    #{'inter': 0.012, 'stock_list': ['161903.XSHE', '163417.XSHE']},  # p=0.04 万家优选, 兴全合宜  a=-0.06 bad
                                    #{'inter': 0.012, 'stock_list': ['510180.XSHG', '163402.XSHE']},  # p=0.04 180ETF, 兴全趋势  a=-0.06 bad
                                    #{'inter': 0.012, 'stock_list': ['163409.XSHE', '159941.XSHE']},  # p=0.04 兴全绿色, 纳指ETF
                                    #{'inter': 0.012, 'stock_list': ['510050.XSHG', '512810.XSHG']},  # p=0.04 50ETF, 军工行业
                                    #{'inter': 0.008, 'stock_list': ['161706.XSHE', '163402.XSHE']},  # p=0.04 招商成长, 兴全趋势  a=-0.03 bad
                                    #{'inter': 0.012, 'stock_list': ['163409.XSHE', '512000.XSHG']},  # p=0.04 兴全绿色, 券商ETF
                                    #{'inter': 0.012, 'stock_list': ['501046.XSHG', '510050.XSHG']},  # p=0.04 财通福鑫, 50ETF
                                    #{'inter': 0.012, 'stock_list': ['510050.XSHG', '161130.XSHE']},  # p=0.04 50ETF, 纳指LOF
                                    #{'inter': 0.012, 'stock_list': ['163417.XSHE', '512550.XSHG']},  # p=0.04 兴全合宜, 富时A50
                                    #{'inter': 0.012, 'stock_list': ['161130.XSHE', '510710.XSHG']},  # p=0.04 纳指LOF, 上50ETF
                                    #{'inter': 0.012, 'stock_list': ['512680.XSHG', '510180.XSHG']},  # p=0.04 军工基金, 180ETF
                                    #{'inter': 0.012, 'stock_list': ['510800.XSHG', '510710.XSHG']},  # p=0.04 上证50, 上50ETF
                                    #{'inter': 0.012, 'stock_list': ['161903.XSHE', '168102.XSHE']},  # p=0.04 万家优选, 九泰锐富
                                    #{'inter': 0.012, 'stock_list': ['159941.XSHE', '161130.XSHE']},  # p=0.04 纳指ETF, 纳指LOF
                                    #{'inter': 0.012, 'stock_list': ['161903.XSHE', '163412.XSHE']},  # p=0.04 万家优选, 兴全轻资
                                    #{'inter': 0.012, 'stock_list': ['501046.XSHG', '510180.XSHG']},  # p=0.04 财通福鑫, 180ETF
                                    #{'inter': 0.008, 'stock_list': ['510050.XSHG', '510710.XSHG']},  # p=0.04 50ETF, 上50ETF （无成交）
                                    #{'inter': 0.012, 'stock_list': ['512810.XSHG', '510710.XSHG']},  # p=0.04 军工行业, 上50ETF
                                    #{'inter': 0.012, 'stock_list': ['163417.XSHE', '510180.XSHG']},  # p=0.04 兴全合宜, 180ETF
                                    #{'inter': 0.012, 'stock_list': ['510800.XSHG', '512810.XSHG']},  # p=0.04 上证50, 军工行业
                                    #{'inter': 0.012, 'stock_list': ['512070.XSHG', '163412.XSHE']},  # p=0.04 证券保险, 兴全轻资
                                    #{'inter': 0.012, 'stock_list': ['510630.XSHG', '159928.XSHE']},  # p=0.05 消费行业, 消费ETF
                                    #{'inter': 0.012, 'stock_list': ['161903.XSHE', '162605.XSHE']},  # p=0.05 万家优选, 景顺鼎益
                                    #{'inter': 0.012, 'stock_list': ['510800.XSHG', '510050.XSHG']},  # p=0.05 上证50, 50ETF
                                    #{'inter': 0.012, 'stock_list': ['161706.XSHE', '161130.XSHE']},  # p=0.05 招商成长, 纳指LOF
                                    #{'inter': 0.012, 'stock_list': ['510050.XSHG', '168103.XSHE']},  # p=0.05 50ETF, 九泰锐益
                                    #{'inter': 0.012, 'stock_list': ['512090.XSHG', '163415.XSHE']},  # p=0.05 MSCI易基, 兴全模式
                                    #{'inter': 0.012, 'stock_list': ['512680.XSHG', '161706.XSHE']},  # p=0.05 军工基金, 招商成长  a=-0.09 bad
                                    #{'inter': 0.012, 'stock_list': ['513100.XSHG', '512160.XSHG']},  # p=0.05 纳指ETF, MSCI基金
                                    #{'inter': 0.012, 'stock_list': ['162605.XSHE', '512550.XSHG']},  # p=0.05 景顺鼎益, 富时A50
                                    #{'inter': 0.012, 'stock_list': ['510800.XSHG', '163409.XSHE']},  # p=0.05 上证50, 兴全绿色
                                    #{'inter': 0.008, 'stock_list': ['161903.XSHE', '510630.XSHG']},  # p=0.05 万家优选, 消费行业  a=-0.76 bad（数据有问题吧）
                                    #{'inter': 0.012, 'stock_list': ['161903.XSHE', '163409.XSHE']},  # p=0.05 万家优选, 兴全绿色
                                    #{'inter': 0.012, 'stock_list': ['159928.XSHE', '510710.XSHG']},  # p=0.05 消费ETF, 上50ETF
                                    #{'inter': 0.012, 'stock_list': ['512550.XSHG', '163415.XSHE']},  # p=0.05 富时A50, 兴全模式
                                    #{'inter': 0.012, 'stock_list': ['166001.XSHE', '512280.XSHG']},  # p=0.05 中欧趋势, 景顺MSCI
                                    #{'inter': 0.012, 'stock_list': ['512680.XSHG', '510050.XSHG']},  # p=0.05 军工基金, 50ETF
                                    #{'inter': 0.012, 'stock_list': ['161128.XSHE', '163409.XSHE']},  # p=0.05 标普科技, 兴全绿色
                                    #{'inter': 0.008, 'stock_list': ['161706.XSHE', '512090.XSHG']},  # p=0.06 招商成长, MSCI易基  a=-0.05 bad
                                    #{'inter': 0.012, 'stock_list': ['166001.XSHE', '163412.XSHE']},  # p=0.06 中欧趋势, 兴全轻资
                                    #{'inter': 0.012, 'stock_list': ['510800.XSHG', '510630.XSHG']},  # p=0.06 上证50, 消费行业
                                    #{'inter': 0.012, 'stock_list': ['161903.XSHE', '161130.XSHE']},  # p=0.06 万家优选, 纳指LOF
                                    #{'inter': 0.012, 'stock_list': ['159928.XSHE', '510180.XSHG']},  # p=0.06 消费ETF, 180ETF
                                    #{'inter': 0.012, 'stock_list': ['161128.XSHE', '161130.XSHE']},  # p=0.06 标普科技, 纳指LOF
                                    #{'inter': 0.012, 'stock_list': ['501046.XSHG', '510710.XSHG']},  # p=0.06 财通福鑫, 上50ETF
                                    #{'inter': 0.012, 'stock_list': ['510050.XSHG', '159928.XSHE']},  # p=0.06 50ETF, 消费ETF
                                    #{'inter': 0.008, 'stock_list': ['161706.XSHE', '512810.XSHG']},  # p=0.06 招商成长, 军工行业  a=-0.08 bad

                                    #详情：2021-04-29 纯深市组合（尚未回测验证完，基本上套利空间都很小）': '''
                                    #{'inter': 0.008, 'stock_list': ['161706.XSHE', '163412.XSHE']},  # p=0.01 招商成长, 兴全轻资
                                    #{'inter': 0.008, 'stock_list': ['161903.XSHE', '163415.XSHE']},  # p=0.03 万家优选, 兴全模式
                                    #{'inter': 0.008, 'stock_list': ['161903.XSHE', '166001.XSHE']},  # p=0.03 万家优选, 中欧趋势  a=-0.09 bad
                                    #{'inter': 0.008, 'stock_list': ['163415.XSHE', '159905.XSHE']},  # p=0.03 兴全模式, 深红利
                                    #{'inter': 0.008, 'stock_list': ['161903.XSHE', '163417.XSHE']},  # p=0.04 万家优选, 兴全合宜  a=-0.09 bad
                                    #{'inter': 0.008, 'stock_list': ['163409.XSHE', '159941.XSHE']},  # p=0.04 兴全绿色, 纳指ETF
                                    #{'inter': 0.008, 'stock_list': ['161706.XSHE', '163402.XSHE']},  # p=0.04 招商成长, 兴全趋势
                                    #{'inter': 0.008, 'stock_list': ['161903.XSHE', '168102.XSHE']},  # p=0.04 万家优选, 九泰锐富
                                    #{'inter': 0.008, 'stock_list': ['159941.XSHE', '161130.XSHE']},  # p=0.04 纳指ETF, 纳指LOF (bad 机会极少)
                                    #{'inter': 0.008, 'stock_list': ['161903.XSHE', '163412.XSHE']},  # p=0.04 万家优选, 兴全轻资
                                    #{'inter': 0.008, 'stock_list': ['163402.XSHE', '166001.XSHE']},  # p=0.05 兴全趋势, 中欧趋势
                                    #{'inter': 0.008, 'stock_list': ['163402.XSHE', '163409.XSHE']},  # p=0.05 兴全趋势, 兴全绿色 (good)
                                    #{'inter': 0.008, 'stock_list': ['161706.XSHE', '161130.XSHE']},  # p=0.05 招商成长, 纳指LOF
                                    
                                    ## 详情：2020-12-07 挑选的组合（21-05-12重新验证）
                                    # 回测期间 21-04-01 ~ 21-05-12
                                    #{'inter': 0.012, 'stock_list': ['501038.XSHG', '161610.XSHE']},  # 银华明择 融通领先 a=0.40 cool（银华明择流动性不足？）
                                    #{'inter': 0.012, 'stock_list': ['166023.XSHE', '160421.XSHE']},  # 中欧瑞丰 华安智增 a=0.13 good
                                    #{'inter': 0.012, 'stock_list': ['501028.XSHG', '163409.XSHE']},  # 财通福瑞 兴全绿色 a=0.08 good
                                    #{'inter': 0.012, 'stock_list': ['168102.XSHE', '168103.XSHE']},  # 九泰锐富 九泰锐益 a=0.07 ok
                                    #{'inter': 0.012, 'stock_list': ['160518.XSHE', '161834.XSHE']},  # 博时睿远 银华鑫锐 a=0.07 ok
                                    #{'inter': 0.012, 'stock_list': ['169101.XSHE', '169102.XSHE']},  # 东证睿X a=0.06 ok
                                    #{'inter': 0.012, 'stock_list': ['159905.XSHE', '163407.XSHE']},  # 深红利 兴全300 a=0.02 -
                                    #{'inter': 0.012, 'stock_list': ['510030.XSHG', '160919.XSHE']},  # 价值ETF 大成产业 a=0.02 -
                                    #{'inter': 0.012, 'stock_list': ['512180.XSHG', '159933.XSHE']},  # 建信MSCI 金地ETF a=-0.03 bad
                                    #{'inter': 0.012, 'stock_list': ['512280.XSHG', '501043.XSHG']},  # 景顺MSCI 沪深300A a=-0.01 bad
                                    #{'inter': 0.012, 'stock_list': ['512550.XSHG', '163415.XSHE']},  # 富时A50 兴全模式 a=-0.04 bad
                                ]
                            }],
                            #[True, '_rpc_fr', '股票评分', Filter_rank, {  # 与上述策略互斥
                            #    'rank_stock_count': 20  # 评分股数
                            #}],
                            #[False, '_rpc_fcfr', '庄股评分', Filter_cash_flow_rank, {'rank_stock_count': 600}],
                            #[True, '_rpc_fbc_', '选股数量', Filter_buy_count, {
                            #    'buy_count': 7
                            #}],
                        ],
                        'group_fund_ratio': 1.0,  # 该选股组合占总资金比（0~1.0）
                        'day_only_run_one': False,  # 每天只选一次
                        'exclusive': False  # 是否独占选股
                    }],
                    [True, '_rp_fbc_', '最终选股数量', Filter_buy_count, {
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
                    [False, '_apc_bse_', '平分买入', BuyStocksEqually, {
                        # 若不指定 buy_count 则根据实际只数平分买入。
                        # 不指定仅适用于实际只数经常不足，且为非协整的策略。
                        'buy_count': buy_count,  # 总持股只数
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
                    #[g.is_sim_trade, '_apc__', '实盘同步', TraderShipane, {
                    #    'server_type': 'mybroker',
                    #    'sync_with_change': True, # 在发生调仓后执行同步（同步操作可有多次跟仓动作）
                    #    'times': [[13, 5]],  # 同步时间列表（如 [[13, 5]] 表示在 13:05 执行同步），可指定多个时间点，如果不指定则仅在出现调仓后调用
                    #    'op_buy_ratio': 1.0,  # 资金投入比例比例
                    #    'batch_amount': 20000,  # 每次分批操作股数
                    #    'use_limit_order': True,  # 是否使用限价单
                    #    'follow_adjust_interval': 5,  # 多少分钟后再跟仓一次
                    #    'follow_adjust_times': 66,  # 总共再跟仓多少次
                    #}],
                ]
            }]
        ]

        ### 组合成一个总的策略

        g.strategy_config = (rules_basic
                             + rules_stop_loss
                             + rules_stop_profit
                             + rules_schedule
                             + rules_chose_stocks
                             + rules_market_timing
                             + rules_adjust)


# ===================================聚宽调用==============================================

def initialize(context):
    
    enable_profile()
    
    set_option("avoid_future_data", True)  # 检查是否用到未来数据
    set_option('use_real_price', True)  # 用真实价格交易
    
    # 设置日志级别为info
    log.set_level('order', 'info')

    # 设定成交量比例
    set_option('order_volume_ratio', 0.3)
    
    # 配置策略规则
    config_strategy_rules(context)
    # 组合策略规则
    g.strategy = StrategyRulesCombiner({'config': g.strategy_config,
                            'g_class': GlobalVariable,
                            'memo': g.strategy_memo,
                            'name': '_main_'})
    g.strategy.initialize(context)

    # 打印规则参数
    g.strategy.log.info(g.strategy.show_strategy())


# 按分钟回测
def handle_data(context, data):
    # 保存context到全局变量量，主要是为了方便规则器在一些没有context的参数的函数里使用。
    g.strategy.g.context = context
    # 执行策略
    g.strategy.handle_data(context, data)


# 开盘
def before_trading_start(context):
    log.info("==============================================================")
    g.strategy.g.context = context
    g.strategy.before_trading_start(context)

# 收盘
def after_trading_end(context):
    g.strategy.g.context = context
    g.strategy.after_trading_end(context)
    g.strategy.g.context = None


# 进程启动(一天一次)
def process_initialize(context):
    try:
        g.strategy.g.context = context
        g.strategy.process_initialize(context)
    except:
        pass


# 这里示例进行模拟更改回测时，如何调整策略,基本通用代码。
def after_code_changed(context):
    try:
        g.strategy
    except:
        print('更新代码->原先不是OO策略，重新调用initialize(context)。')
        initialize(context)
        return

    try:
        print('=> 更新代码')
        config_strategy_rules(context)
        g.strategy.g.context = context
        g.strategy.update_params(context, {'config': g.strategy_config})
        g.strategy.after_code_changed(context)
        g.strategy.log.info(g.strategy.show_strategy())
    except Exception as e:
        # log.error('更新代码失败:' + str(e) + '\n重新创建策略')
        # initialize(context)
        pass


# 显示策略组成
def log_param():
    def get_rules_str(rules):
        return '\n'.join(['   %d.%s ' % (i + 1, str(r)) for i, r in enumerate(rules)]) + '\n'

    s = '\n---------------------策略一览：规则组合与参数----------------------------\n'
    s += '一、持仓股票的处理规则:\n' + get_rules_str(g.position_stock_rules)
    s += '二、调仓条件判断规则:\n' + get_rules_str(g.adjust_condition_rules)
    s += '三、Query选股规则:\n' + get_rules_str(g.pick_stock_by_query_rules)
    s += '四、股票池过滤规则:\n' + get_rules_str(g.filter_stock_list_rules)
    s += '五、调仓规则:\n' + get_rules_str(g.adjust_position_rules)
    s += '六、其它规则:\n' + get_rules_str(g.other_rules)
    s += '--------------------------------------------------------------------------'
    log.info(s)
