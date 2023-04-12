
import backtrader as bt
from backtrader.feeds import BacktraderCSVData
from alphabot.utils.log import logger
from alphabot.storage.mongodb import MongoDB


# TODO: Transform return result into a common class.
class StorageManager():
    storage_types = {
        'csvfile': BacktraderCSVData,
        'mongodb': MongoDB,
        #'mysql': None
    }

    def __init__(self, storage_type, **kargs):
        storage_cls = self.storage_types[storage_type]
        self.storage = storage_cls(**kargs)

    def __repr__(self):
        return '<Storage: %s >' % (self.storage)

    # TODO: Reconsider params.
    def update_order(self, _id, fills, status):
        self.storage.update_order(_id, fills, status)

    def create_candle(self, time, low, high, open, close, volume, product_id, interval):
        self.storage.create_candle(time, low, high, open, close, volume, product_id, interval)

    def get_max_candle_by_time(self, product_id):
        return self.storage.get_max_candle_by_time(product_id)

    def get_bars(self, code: str, time_begin: str, time_end: str):
        return self.storage.get_bars(code, time_begin, time_end)
    
