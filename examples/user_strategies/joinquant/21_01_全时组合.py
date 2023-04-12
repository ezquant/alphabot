"""
逻辑：基于新策略框架，实现牛市进取、熊市防守的全行情双组合策略。
特点：收益稳定，回撤小。适合全行情。
注意：1、基金协整部分需每半年定期调整组合（当前为2020-12-12）。
      2、经实盘对比分析验证，基金协整只适合限价单！（虽然会有成交不足问题）

TODO: 远程同步加日志
TODO: 在子策略中指定下单类型
TODO: * 资金比例调仓支持动态仓位管理
TODO: * 子策略失效检测？
TODO: * 研究一下低价股的做T套利方式
      检测到连续买入，则拉高出货；检测到连续卖出，则压低收货。
      选择流动性有限的标的，便于超控？确定具体套利类型，研究资料及可行性
TODO: 研究：指数增强、T0算法交易（两条主线），对冲、进取
      打板、热门板块
      券商研究报告策略挖掘
    
** “多驱战车”多策略组合，共选股~40只
      60w只开启前4个策略，120w后开启7个策略。
      启用策略的资金配比之和应等于90~100%。
      
      #开发状态 #资金配比（60w/200w） #策略名称 #持有数量
      
      ok 20%/10% 外资策略 1（价格较高，资金太少不交易）
      ok 20%/15% 中小板成长价值 5（或最小流通RSRS）
      ok 50%/30% ROE优化蓝筹-北向择时 10
      ok 10%/15% 中小板龙头股-RSIX择时 3
      
      ok 0/15% 基金一哥大市值 8
      ok 0/5% 四大行配对 1
      ok 0/10% 基金协整 5
      
      ? A/H折价 3
      ? 追涨-龙回头极致改进版-V9 1
      ? 追涨大师-首次涨停 1
      ? ROX选股-RSRS择时 5
      ? 市场追逐-板块轮动
      ? 双因子-ROA-500指数增强
      
      - 热门ETF-北向择时（21年不佳，可弃） 3
      - 中小板最小流通市值（a不及中小板成长价值，暂弃） 0
      - 价值投资-大盘择时（选股池太小过拟合，弃用） 0
      - FScore蓝筹-北向择时（a不及ROE蓝筹选股，暂弃） 0
      - 大交易量基金轮动（21年不佳，可弃） 3
      - 韶华顺势（21年不佳，暂弃） 2
"""

import talib as tl
# 使用本地系统提供的API封装，替换聚宽在线API
from alphabot.user.api_for_jq import *
from .strategy_core import *


# 聚宽策略移植到本地系统后，必须补充设置全部股票池才能正常加载数据
all_security_pool = []


def config_strategy_rules(context):
        """ 策略配置规则示例 """

        g.strategy_memo = '我的策略'
        # 定义 log 输出的类类型，必须指定。如需自定义log，可继承修改之。
        g.log_type = RuleLoger
        # 判断是运行模式，回测还是模拟
        #g.is_sim_trade = context.run_params.type == 'sim_trade'
        index2 = '000016.XSHG'  # 大盘指数
        index8 = '399333.XSHE'  # 小盘指数
        #index8 = '399678.XSHE'  # 深次新指数
        buy_count = 20

        ### 1. 基础规则（必选）：系统参数、交易费率、统计信息等

        rules_basic = [
            [True, '_rules_basic_', '基础规则', GroupRules, {
                'config': [
                    [True, '', '设置系统参数', Set_sys_params, {
                        'benchmark': '000300.XSHG',  # 基准股指
                        'order_volume_ratio': '0.2'  # 成交量比例
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
                        'index_stock': '000001.XSHG',  # 使用的指数,默认 '000001.XSHG'
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
            [True, '_rcs_csm_', '多策略选股', ChoseStocksMulti, {
                'config': [
                    # 全时多策略组合
                    #（！下述配置的标识和类型对必须唯一，否则代码更新时会错误替换）
                    # 5.1 进取型选股（5%，1只，可选，实盘一般不开）：
                    [False, '_csm_css_1_', '进取型选股', ChoseStocksSimple, {
                        'config': [
                            [False, '_rpa_te_', '执行时间', TimerExec, {
                                'times': [[9, 30]],  # 执行时间列表，二维数组，可指定多个时间点
                            }],
                            # -------- 选股前择时，降低无用计算 ----
                            [False, '_rpa_fbnb_', '魔力K线择时', TimingByMagicKLine, {
                                'index_stock': '000001.XSHG'
                            }], # 
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
                            # -------- 特殊选股 --------
                            [False, '_rpa_pbhe_', '选取热门ETF', PickByHotETF, {
                                'max_stock_count': 5,  # 最多选取只数
                            }],  # 回测验证较好！RSRS优于北上择时
                            [True, '_rpa_pblt_', '追涨龙头选股', PickByLongtou, {
                            }],
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
                            [False, '_rpa_fbcm_', '一般性过滤', FilterByCommon, {
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
                                'max_stock_count': 5,  # 最多选取只数
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
                            [True, '_rpa_fbc', '选股数量', Filter_buy_count, {
                                'buy_count': 1
                            }],
                        ],
                        'group_fund_ratio': 0.05,  # 该选股组合占总资金比（0~1.0）
                        'day_only_run_one': False,  # 一天只选一次
                        'exclusive': False  # 是否开启独占模式
                    }],
                    
                    # 5.2 外资策略（5%，1只，必选）：
                    [True, '_csm_css_2_', '外资重仓策略', ChoseStocksSimple, {
                        'config': [
                            [True, '_rpa_te_', '执行时间', TimerExec, {
                                'times': [[10, 15]],  # 执行时间列表，二维数组，可指定多个时间点
                            }],
                            # -------- 选股前择时，降低无用计算 ----
                            # -------- 特殊选股 --------
                            [True, '_rpa_pbfc_', '外资重仓选股', PickByForeignCapital, {
                                'buy_count': 1  # 只买1只最佳
                            }],
                            # -------- 通用选股 --------
                            # -------- 选股过滤 --------
                            # -------- 截取数量 --------
                        ],
                        'group_fund_ratio': 0.20,  # 该选股组合占总资金比（0~1.0）
                        'day_only_run_one': True,
                        'exclusive': False  # 是否开启独占模式
                    }],
                    # 5.3 中小板成长价值策略（15%，5只，必选）：
                    [True, '_csm_css_3_', '中小板成长价值策略', ChoseStocksSimple, {
                        'config': [
                            # -------- 选股前择时，降低无用计算 ----
                            # -------- 特殊选股 --------
                            [True, '_rpa_pbltqf_', '中小板成长价值选股', PickBySmallCapitalCZJZ, {
                                'time_to_sell': [10, 30],  # 默认 [10, 30]
                                'time_to_buy': [14, 5],  # 默认 [14, 45]
                                'buy_count': 5,  # 最多选取只数
                            }],
                            # -------- 通用选股 --------
                            # -------- 选股过滤 --------
                            # -------- 截取数量 --------
                        ],
                        'group_fund_ratio': 0.20,  # 该选股组合占总资金比（0~1.0）
                        'day_only_run_one': False,
                        'exclusive': False  # 是否开启独占模式
                    }],
                    # 5.4 ROE优化蓝筹-北向择时策略（30%，10只，必选）：
                    [True, '_csm_css_4_', 'ROE优化蓝筹策略', ChoseStocksSimple, {
                        'config': [
                            [True, '_rpa_te_', '执行时间', TimerExec, {
                                'times': [[14, 10]],  # 执行时间列表，二维数组，可指定多个时间点
                            }],
                            # -------- 选股前择时，降低无用计算 ----
                            [True, '_rpa_fbnb_', '北上资金择时', TimingByNorthMoney, {
                                'method': 'boll', # total_net_in, boll # 本策略中 xxx 稍好
                            }], # 开启北上过滤时，无需再开北上择时，效果等同
                            # -------- 特殊选股 --------
                            [True, '_rpa_pbvi_', '优化ROE蓝筹选股', PickByROE2, {
                                'buy_count': 10,  # 最多选取只数
                            }],
                            # -------- 通用选股 --------
                            # -------- 选股过滤 --------
                            # -------- 截取数量 --------
                        ],
                        'group_fund_ratio': 0.50,  # 该选股组合占总资金比（0~1.0）
                        'day_only_run_one': True,
                        'exclusive': False  # 是否开启独占模式
                    }],
                    # 5.5 中小板龙头股-RSIX择时策略（15%，3只，必选）：
                    [True, '_csm_css_5_', '中小板龙头股RSIX择时策略', ChoseStocksSimple, {
                        'config': [
                            # -------- 选股前择时，降低无用计算 ----
                            # -------- 特殊选股 --------
                            [True, '_rpa_pbltsc_', '中小板龙头股-RSIX择时', PickByLongtouSmallCap, {
                                'buy_count': 3,
                                'index_stock': '000001.XSHG'
                            }],
                            # -------- 通用选股 --------
                            # -------- 选股过滤 --------
                            # -------- 截取数量 --------
                        ],
                        'group_fund_ratio': 0.10,  # 该选股组合占总资金比（0~1.0）
                        'day_only_run_one': False,
                        'exclusive': False  # 是否开启独占模式
                    }],
                    
                    # ！！总资金大于120w后再考虑开启下述策略
                    
                    # 5.6 基金一哥大市值策略（15%，8只，必选）：
                    [False, '_csm_css_6_', '基金一哥大市值策略', ChoseStocksSimple, {
                        'config': [
                            [True, '_rpa_te_', '执行时间', TimerExec, {
                                'times': [[10, 15]],  # 执行时间列表，二维数组，可指定多个时间点
                            }],
                            # -------- 选股前择时，降低无用计算 ----
                            # -------- 特殊选股 --------
                            [True, '_rpa_pbikp_', '基金一哥复刻选股', PickByIkunPool, {
                                'buy_count': 8,
                                'index_list': ['000300.XSHG', '000905.XSHG']
                            }],
                            # -------- 通用选股 --------
                            # -------- 选股过滤 --------
                            # -------- 截取数量 --------
                        ],
                        'group_fund_ratio': 0.15,  # 该选股组合占总资金比（0~1.0）
                        'day_only_run_one': True,
                        'exclusive': False  # 是否开启独占模式
                    }],
                    # 5.7 四大行配对策略（5%，1只，必选）：
                    [False, '_csm_css_7_', '四大行配对策略', ChoseStocksSimple, {
                        'config': [
                            # -------- 选股前择时，降低无用计算 ----
                            # -------- 选股 --------
                            [True, '_rpc_pbcp_', '银行配对协整', PickByCointPairs, {
                                'buy_count': 1,  # 选股数量
                                'stocks_groups': [
                                    # 四大行协整
                                    {'inter': 0.010, 'stock_list': ['601398.XSHG', '601288.XSHG','601939.XSHG','601988.XSHG']},  # 工农建中
                                ]
                            }],
                        ],
                        'group_fund_ratio': 0.05,  # 该选股组合占总资金比（0~1.0）
                        'day_only_run_one': False,  # 每天只选一次
                        'exclusive': False  # 是否独占选股
                    }],
                    # 5.8 基金协整策略（10%，5只，必选）：
                    [False, '_csm_css_8_', '基金协整策略', ChoseStocksSimple, {
                        'config': [
                            # -------- 选股前择时，降低无用计算 ----
                            # -------- 选股 --------
                            [True, '_rpc_pbcp_', '基金配对协整', PickByCointPairs, {
                                'buy_count': 5,  # 选股数量
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
                                    {'inter': 0.014, 'stock_list': ['169101.XSHE', '169102.XSHE']},  # 东证睿丰, 东证睿阳  a=0.18 good
                                    {'inter': 0.014, 'stock_list': ['501046.XSHG', '159941.XSHE']},  # 财通福鑫, 纳指ETF  a=0.15 good
                                    {'inter': 0.014, 'stock_list': ['168102.XSHE', '168103.XSHE']},  # 九泰锐富, 九泰锐益  a=0.14 good
                                    {'inter': 0.014, 'stock_list': ['161810.XSHE', '164403.XSHE']},  # 银华内需, 农业精选  a=0.11 good
                                    {'inter': 0.014, 'stock_list': ['501016.XSHG', '161706.XSHE']},  # 券商基金, 招商成长  a=0.05 ok
                                    #{'inter': 0.008, 'stock_list': ['163409.XSHE', '502010.XSHG']},  # 兴全绿色, 证券LOF  a=0.12 good
                                    ##{'inter': 0.012, 'stock_list': ['501038.XSHG', '161610.XSHE']},  # 银华明择, 融通领先  a=0.52 cool（银华明择难买入）
                                    ##{'inter': 0.010, 'stock_list': ['160518.XSHE', '161834.XSHE']},  # 博时睿远, 银华鑫锐  a=0.09 ok（银华鑫锐难买入）
                                ]
                            }],
                        ],
                        'group_fund_ratio': 0.10,  # 该选股组合占总资金比（0~1.0）
                        'day_only_run_one': False,  # 每天只选一次
                        'exclusive': False  # 是否独占选股
                    }],
                    
                    [False, '_csm_fbc_', '最终选股数量', Filter_buy_count, {
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
                    [False, '_apc__', '实盘同步', TraderShipane, {
                        'server_type': 'mybroker',
                        # 本策略必须为限价单，每天开盘后即开始同步，持续到交易结束
                        #TODO 验证远程同步小步市价单效果
                        'sync_with_change': True,  # 是否在发生调仓后执行同步（适合市价单）
                        #'sync_all_day': False,  # 是否全天执行同步（适合限价单，从交易前开始，聚宽等线性平台不适用）
                        'sync_timers': [[11, 25], [14, 15]],  # 定时同步时间（如 [[14,31],[14,50]]，影响时延，尽量不用或晚用）
                        #'sync_loop_times': 3,  # 每次同步开平仓循环次数（非全天同步时有效）
                        'use_limit_order': True,  # 是否使用限价单
                        'use_curr_price': True,  # 限价同步时若未找到调仓价格，是否使用当前价格
                        #'op_buy_ratio': 1.0,  # 资金投入比例 100%
                        'batch_amount': 5000,  # 每次分批操作股数（考虑手续费及价格冲击，市价单建议2.5k，限价单建议1w）
                        'sync_on_remote': True  # 是否在远程交易代理主机上做仓位同步（这将取代本地同步过程）
                    }],
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
