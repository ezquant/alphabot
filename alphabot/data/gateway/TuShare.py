import tushare as ts
from StockConfig import StockDataType

class TuShare_GW(object):
    __token = "2fe3395cd410fb8ef0c467b5f5c5e871a69050f7d1ba434299d7ceab"
    __stock_id_suffix = ".sz"

    __freq_map = {
        StockDataType.DAILY: 'D',
        StockDataType.ONE_HOUR: '60min',
        # StockDataType.THIRTY_MINS: '30min',
        # StockDataType.TEN_MINS: '10min',
        StockDataType.FIVE_MINS: '5min',
        StockDataType.ONE_MIN: '1min'
    }

    def __init__(self):
        ts.set_token(self.__token)

    def get_stock_price(self, stock_data_type: StockDataType, stock_id: str, start_date: str, end_date: str):
        stock_id = stock_id + self.__stock_id_suffix
        return ts.pro_bar(ts_code=stock_id, start_date=start_date, end_date=end_date, freq=self.__freq_map[stock_data_type])
