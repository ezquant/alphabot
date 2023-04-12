#!/usr/local/bin/python

#coding :utf-8
#
""" 从网络下载或更新相关数据，写入本地 mongodb 数据库

2020-09-20 目前可获取数据类型：
 'QA_SU_crawl_eastmoney', # 东方财富网 股票资金流向
 'QA_SU_save_bond_day', # 债券
 'QA_SU_save_bond_list',
 'QA_SU_save_bond_min',
 'QA_SU_save_etf_day', # 交易所交易基金，开放式基金
 'QA_SU_save_etf_list',
 'QA_SU_save_etf_min',
 'QA_SU_save_financialfiles',
 'QA_SU_save_financialfiles_fromtdx',
 'QA_SU_save_future_day', # 期货
 'QA_SU_save_future_day_all',
 'QA_SU_save_future_list',
 'QA_SU_save_future_min',
 'QA_SU_save_future_min_all',
 'QA_SU_save_index_day', # 指数
 'QA_SU_save_index_list',
 'QA_SU_save_index_min',
 'QA_SU_save_index_transaction',
 'QA_SU_save_option_300etf_day', # 期权
 'QA_SU_save_option_300etf_min',
 'QA_SU_save_option_50etf_day',
 'QA_SU_save_option_50etf_min',
 'QA_SU_save_option_commodity_day',
 'QA_SU_save_option_commodity_min',
 'QA_SU_save_option_contract_list',
 'QA_SU_save_option_day_all',
 'QA_SU_save_option_min_all',
 'QA_SU_save_report_calendar_day',
 'QA_SU_save_report_calendar_his',
 'QA_SU_save_single_bond_day',
 'QA_SU_save_single_bond_min',
 'QA_SU_save_single_etf_day',
 'QA_SU_save_single_etf_min',
 'QA_SU_save_single_future_day',
 'QA_SU_save_single_future_min',
 'QA_SU_save_single_index_day',
 'QA_SU_save_single_index_min',
 'QA_SU_save_single_stock_day',
 'QA_SU_save_single_stock_min',
 'QA_SU_save_stock_block',
 'QA_SU_save_stock_day',
 'QA_SU_save_stock_divyield_day', #分红
 'QA_SU_save_stock_divyield_his',
 'QA_SU_save_stock_info',
 'QA_SU_save_stock_info_tushare',
 'QA_SU_save_stock_list',
 'QA_SU_save_stock_min',
 'QA_SU_save_stock_min_5',
 'QA_SU_save_stock_transaction',
 'QA_SU_save_stock_xdxr',
 'QA_fetch_stock_list'
"""

from QUANTAXIS.QASU.main import (QA_SU_save_etf_list,
                                 QA_SU_save_etf_day, 
                                 QA_SU_save_etf_min,
                                 QA_SU_save_future_list,
                                 QA_SU_save_future_day,
                                 QA_SU_save_future_min,
                                 QA_SU_save_financialfiles,
                                 QA_SU_save_index_list,
                                 QA_SU_save_index_day, 
                                 QA_SU_save_index_min,
                                 QA_SU_save_index_transaction,
                                 QA_SU_save_stock_block, 
                                 QA_SU_save_stock_day,
                                 QA_SU_save_stock_info,
                                 QA_SU_save_stock_info_tushare,
                                 QA_SU_save_stock_list, 
                                 QA_SU_save_stock_min,
                                 QA_SU_save_stock_xdxr,
                                 QA_SU_save_report_calendar_day,
                                 QA_SU_save_report_calendar_his,
                                 QA_SU_save_stock_divyield_day,
                                 QA_SU_save_stock_divyield_his)

from QUANTAXIS.QASU.save_binance import (QA_SU_save_binance,
                                         QA_SU_save_binance_1day,
                                         QA_SU_save_binance_1hour,
                                         QA_SU_save_binance_1min,
                                         QA_SU_save_binance_symbol)


def fetch_quantaxis_data():

    # 获取股票数据
    if True:
        data_engine = 'tdx' # tdx:通达信, ts:tushare, gm:掘金, jq:聚宽

        QA_SU_save_stock_list(data_engine) # done, 不支持 jq
        QA_SU_save_stock_day(data_engine) # 
        QA_SU_save_stock_min(data_engine) # done
        QA_SU_save_stock_xdxr(data_engine) # done
        QA_SU_save_stock_block(data_engine) # done
        #QA_SU_save_stock_info(data_engine) # error, 不支持tdx
        QA_SU_save_financialfiles() # done
        #QA_SU_save_stock_divyield_day() # error
        #QA_SU_save_report_calendar_day() # error

    # 获取指数、基金和期货数据
    if False:
        data_engine = 'tdx'  # 只支持通达信

        QA_SU_save_index_list(data_engine) # done
        QA_SU_save_index_day(data_engine) # done
        QA_SU_save_index_min(data_engine) # done（指数，分钟）
        #QA_SU_save_index_transaction(data_engine) # 容量巨大，需要上百 GB 空间，暂时取消
        
        QA_SU_save_etf_list(data_engine) # done
        QA_SU_save_etf_day(data_engine) # err
        QA_SU_save_etf_min(data_engine) # 
        
        #QA_SU_save_future_list(data_engine) # err
        #QA_SU_save_future_day(data_engine) # err
        #QA_SU_save_future_min(data_engine) # err


if __name__ == '__main__':
    fetch_quantaxis_data()
    