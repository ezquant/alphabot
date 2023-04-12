from pytdx.reader import lc_min_bar_reader, TdxDailyBarReader, block_reader
from pytdx.reader.block_reader import BlockReader_TYPE_GROUP
from pytdx.hq import TdxHq_API, TDXParams
import pandas as pd
import Common.CommonUtils as com_utils
from Common.Exception.UnimplementedException import UnimplementedException
import Config as cfg
from StockConfig import StockDataType

class TDX_GW(object):

    __lc_min_bar_reader = None
    __daily_bar_reader = None
    __tdx_api = None
    __connected_ip = '119.147.212.81'
    __connected_port = 7709

    def __init__(self):
        self.__lc_min_bar_reader = lc_min_bar_reader.TdxLCMinBarReader()
        self.__daily_bar_reader = TdxDailyBarReader()
        self.__block_reader = block_reader.BlockReader()
        self.__tdx_api = TdxHq_API()

    def get_local_stock_bars(self, file_path: str, stock_date_type: StockDataType):
        if stock_date_type == StockDataType.ONE_MIN or \
            stock_date_type == StockDataType.FIVE_MINS:
            # start = time.time()
            # df = self.__lc_min_bar_reader.get_df(file_path)
            data = self.__lc_min_bar_reader.parse_data_by_file(file_path)
            df = pd.DataFrame(data=data)
            # df = df['date', 'open', 'high', 'low', 'close', 'amount', 'volume']
            # print(f"TDX get 1min bar time spent: {(time.time() - start) * 1000} ms")

            return df[['date', 'open', 'high', 'low', 'close', 'amount', 'volume']]
        elif stock_date_type == StockDataType.DAILY:
            data = self.__daily_bar_reader.get_df(file_path).reset_index()
            data['date'] = data['date'].dt.strftime('%Y-%m-%d')
            return data[['date', 'open', 'high', 'low', 'close', 'amount', 'volume']]
        else:
            raise UnimplementedException

    def get_local_stock_bars_raw_data(self, file_path: str, stock_date_type: StockDataType):
        if stock_date_type == StockDataType.ONE_MIN or \
            stock_date_type == StockDataType.FIVE_MINS:
            return self.__lc_min_bar_reader.parse_data_by_file(file_path)
        elif stock_date_type == StockDataType.DAILY:
            return self.__daily_bar_reader.parse_data_by_file(file_path)
        else:
            raise UnimplementedException

    def get_realtime_stock_1min_bars(self, market: str, stock_id: str):
        with self.__tdx_api.connect(self.__connected_ip, self.__connected_port):
            market_code = self.__get_market_code(market)
            df = self.__tdx_api.to_df(
                self.__tdx_api.get_security_bars(8, market_code, stock_id, 0, 10))  # 返回DataFrame
            return df

    def get_realtime_stocks_quotes(self, stock_ids: []):
        stock_list = []
        for id in stock_ids:
            stock_list.append((com_utils.get_stock_market(id), id))
        with self.__tdx_api.connect(self.__connected_ip, self.__connected_port):
            return self.__tdx_api.get_security_quotes(stock_list)

    def get_history_minute_time_data(self, market: str, stock_id: str, date: int):
        with self.__tdx_api.connect(self.__connected_ip, self.__connected_port):
            market_code = self.__get_market_code(market)
            df = self.__tdx_api.to_df(
                self.__tdx_api.get_history_minute_time_data(market_code, stock_id, date))
            return df

    def get_xdxr_info(self, market: str, stock_id: str):
        with self.__tdx_api.connect(self.__connected_ip, self.__connected_port):
            market_code = self.__get_market_code(market)
            df = self.__tdx_api.to_df(
                self.__tdx_api.get_xdxr_info(market_code, stock_id))
            return df

    def test(self):
        with self.__tdx_api.connect(self.__connected_ip, self.__connected_port):
            return self.__tdx_api.get_history_minute_time_data(0, '000001', '2020-10-09')

    def __get_market_code(self, market: str):
        if market == cfg.MARKET.SHANGHAI:
            return 1
        elif market == cfg.MARKET.SHENZHEN:
            return 0
        else:
            raise UnimplementedException

if __name__ == '__main__':
    tdx = TDX_GW()
    print(tdx.test())


