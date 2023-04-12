import jqdatasdk as jq
import jqdatasdk.utils as jq_utils
import jqdatasdk.api as jq_api
from Common import DatetimeUtils, StringUtils
import Config as cfg


class JQData_GW(object):

    __user_name = "18611823120"
    __pw = "823120"

    def __init__(self):
        jq.auth(self.__user_name, self.__pw)

    def get_1min_bars(self, stock_id: str, count: int, end_date: str):
        return jq.get_bars(jq.normalize_code(stock_id), count, unit='1m',
                         fields=['date', 'open', 'close', 'high', 'low', 'volume', 'money'], include_now=False,
                         end_dt=end_date)

    def get_industries(self, industry_code: cfg.IndustryCode):
        return jq.get_industries(name=industry_code.name)

    def get_industry_stocks(self, industry_id: str):
        return jq.get_industry_stocks(industry_id)

    def get_all_conecpts(self):
        return jq.get_concepts()

    def get_concept_stocks(self, concept_code: str, date = None):
        if date is None:
            date = DatetimeUtils.date_to_str(DatetimeUtils.get_n_days_before_date(100))
        return jq.get_concept_stocks(concept_code, date)

    def get_all_trade_days(self):
        return jq.get_all_trade_days()

    def get_trade_days(self, start_date: str, end_date: str = None):
        return jq.get_trade_days(start_date=start_date, end_date = end_date)

    # "000001.XSHE" -> "000001"
    def normalize_stock_id(self, stock_id: str):
        return jq_utils.normal_security_code(stock_id)

    # code could be index or stock id
    # ['000001', 'SZ000001', '000001SZ', '000001.sz', '000001.XSHE'] -> "000001.XSHE"
    def normalize_code(self, code: str):
        return jq_api.normalize_code(code)


gw = JQData_GW()
# gw.test()

print(jq.get_query_count())


# stocks = jq.get_index_stocks('000300.XSHG')
# print(stocks)

# print(jq.get_industry("600519.XSHG",date="2018-06-01"))

# print(gw.get_all_conecpts())
# print(gw.get_industries(cfg.IndustryCode.zjw))

# print(gw.get_industry_stocks("L72"))
# print(gw.get_all_trade_days())
# print(jq.get_trade_days("2019-10-01"))

# print(cfg.IndustryCode.zjw.name)

# print(jq.normalize_code("000001"))

