import pymongo
import pandas as pd
import datetime
import tushare as ts
import QUANTAXIS as QA
from dateutil.relativedelta import relativedelta
from jqdatasdk import auth, get_query_count
from jqdatasdk import query, valuation, indicator, get_fundamentals, finance, get_bars

ts.set_token('d79c15feb3718d16953d1524f8076a076a33f55efb9eafa0d5484310')

pro = ts.pro_api()

# pd全局设置
pd.set_option('display.max_rows', 5000)
pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 300)

# 使用聚宽账号密码登录（新申请用户默认为手机号后6位）
_user, _passwd = '13520312255','Zdf6664518' # ok
#_user, _passwd = '18611823120', '823120' # ok
#_user, _passwd = '15640316927', '316927'
#_user, _passwd = '18500150123', 'YanTeng881128' # ok
#_user, _passwd = '18620668927', 'minpeng123'
#_user, _passwd = '13695683829', 'ssk741212'
#_user, _passwd = '18610039264', 'zg19491001'
auth(_user, _passwd) # '聚宽账号','聚宽密码'

#查询当日剩余可调用数据条数
print('get JQ query count:', get_query_count())

myClient = pymongo.MongoClient('mongodb://localhost:27017/')


def _save2mongodb(myCollection, data, isDf=True):
    if isDf:
        myCollection.insert_many(QA.QA_util_to_json_from_pandas(data))
    else:
        myCollection.insert_many(data)


def mt_save_financial_from_JQData(stk, start_date, end_date):
    '''
    从jqdata中获取财务数据指标indicator
    可以用一个dict来保存已增加通用性
    :param start_date:开始日期
    :param end_date:结束日期
    :return:获取的值
    '''

    queryDict = {'indicator': indicator, # 财务指标数据
                 'finance.STK_FIN_FORCAST': finance.STK_FIN_FORCAST, # 业绩预告
                 'finance.STK_INCOME_STATEMENT': finance.STK_INCOME_STATEMENT, # 合并利润表
                 'finance.STK_INCOME_STATEMENT_PARENT': finance.STK_INCOME_STATEMENT_PARENT, # 母公司利润表
                 'finance.STK_CASHFLOW_STATEMENT': finance.STK_CASHFLOW_STATEMENT, # 合并现金流表
                 'finance.STK_CASHFLOW_STATEMENT_PARENT': finance.STK_CASHFLOW_STATEMENT_PARENT, # 母公司现金流表
                 'finance.STK_BALANCE_SHEET': finance.STK_BALANCE_SHEET, # 合并资产表
                 'finance.STK_BALANCE_SHEET_PARENT': finance.STK_BALANCE_SHEET_PARENT, # 母公司资产表
                 }

    if stk not in queryDict.keys():
        return

    mydb = myClient['stockFinanceDbJQData']
    myCollection = mydb[stk]

    try:
        # 获取已有数据的datetimes
        ref_ = myCollection.distinct('datetime')
    except:
        ref_ = []

    q = query(queryDict[stk])
    df = pd.DataFrame()

    start_dt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.datetime.strptime(end_date, '%Y-%m-%d')

    delta = relativedelta(months=3)
    while start_dt < end_dt:
        quarter_para = "{0}q{1}".format(start_dt.year, start_dt.month // 3 + 1)

        if quarter_para in ref_:
            pass
        else:
            
            df = get_fundamentals(q, statDate=quarter_para)
            df["datetime"] = quarter_para
            df.rename(columns={"statDate.1" : "statDate1"}, inplace=True)
            df["code"] = df["code"].apply(lambda x: x.replace('.XSHE', '.SZ'))
            df["code"] = df["code"].apply(lambda x: x.replace('.XSHG', '.SH'))

        if df.empty is not True:
            _save2mongodb(myCollection, df)
            print("更新日期：{0}",quarter_para)

        start_dt += delta

def TestGetMinDataFromJQdata():
    df = get_bars('000651.XSHE', 1000000, unit='5m', fields=['date', 'open', 'high', 'low', 'close', 'volume', 'factor'])
    df.to_csv("格力5min")

if __name__ == '__main__':
    # q = query(indicator).filter(indicator.code == '000001.XSHE')
    # df = pd.DataFrame()
    # df = get_fundamentals(q, statDate='2015')
    # print(df)

    # mt_save_financial_from_JQData('finance.STK_INCOME_STATEMENT',"2005-01-01", "2020-04-16")

    TestGetMinDataFromJQdata()
