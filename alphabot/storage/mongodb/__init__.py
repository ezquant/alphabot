import time
from datetime import datetime
from mongoengine import connect
import backtrader as bt
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure

from alphabot.storage import BaseStorage
from alphabot.storage.mongodb.models import Order, Candle
from alphabot.config import config
from alphabot.utils import CodeProcessor
from alphabot import logger


class MongoDB(BaseStorage):

    def __init__(self):
        #connect('alphabot')
        self.host = config['db']['mongodb']['host']
        self.port = config['db']['mongodb']['port']
        self.database = config['db']['mongodb']['database']
        client = MongoClient(self.host, self.port)
        try:
            # The ismaster command is cheap and does not require auth.
            client.admin.command('ismaster')
        except ConnectionFailure as err:
            print('Failed to establish connection to MongoDB: %s' % err)
        self.db = client.get_database(self.database)

    def __repr__(self):
        return 'MongoDB <Host: %s , Port: %s, Database: %s>' % (self.host, self.port, self.database)

    def create_order(self, trade_order):
        order = Order(
            order_id=trade_order.order_id,  # TODO: Coinbase specific. Make this more generic.
            price=trade_order.price,
            side=trade_order.side,
            size=trade_order.size,
            product_id=trade_order.product_id,
            created_at=trade_order.created_at,
            done_at=trade_order.done_at,
            status=trade_order.status,
            type=trade_order.type,
        )
        order.save()

    def get_orders(self, statuses: list):
        expr = [{'status': status} for status in statuses]
        orders = self.db.get_collection('orders')
        return orders.find({'$or': expr})

    def update_order(self, order_id, fills, status):
        orders = self.db.get_collection('orders')
        orders.update(
            {'order_id': order_id},
            {'$push': {'fills': {'$each': fills}}, '$set': {'status': status}}
        )

    def create_candle(self, time, low, high, open, close, volume, product_id, interval):
        candle = Candle(
            time=time,
            low=low,
            high=high,
            open=open,
            close=close,
            volume=volume,
            product_id=product_id,
            interval=interval
        )
        candle.save()

    def get_max_candle_by_time(self, product_id):
        candles = self.db.get_collection('candles')
        cursor = candles.find().sort([('time', -1)])
        try:
            return cursor.next()
        except StopIteration:
            return

    def get_bars(self, code: 'e.g., 000001.XSHG', 
                time_begin: 'Datetime 格式', time_end: 'Datetime 格式', 
                timeframe: '过滤时间字段') \
            -> dict(type=list, help='the result data of queried'):
        """
        读取通过QUANTAXIS下载的证券交易数据 - zzz 开发
        """
        
        # 根据标的代码前缀判断数据类别
        _code_pre, _code_post = CodeProcessor.parse(code)
        _db_pre = ''
        # 如果是普通股，则读取 stock 数据集，如 600123
        if self._code_in_db(_code_pre, 'stock_list'):
            _db_pre = 'stock'
        # 如果是指数股，则读取 index 数据集，如 000300
        elif self._code_in_db(_code_pre, 'index_list'):
            _db_pre = 'index'
        # 如果是 ETF 股，则读取 etf 数据集，如 510300
        elif self._code_in_db(_code_pre, 'etf_list'):
            _db_pre = 'index' # QA的数据库就是这么存的
        else:
            raise(Exception('cannot found code %s in any db!' % _code_pre))
        
        # 根据代码后缀、范围及时间片单位访问不同的数据库
        if timeframe == bt.TimeFrame.Days:
            db_name = _db_pre + '_day' # 要读取的数据集
            time_name, time_seq = 'date_stamp', ("$gte", "$lt")  # 取到前一bar
        elif timeframe == bt.TimeFrame.Minutes:
            db_name = _db_pre + '_min'
            time_name, time_seq = 'time_stamp', ("$gte", "$lt")  # 取到前一bar
        else:
            raise(Exception('invalid timeframe: %s' % timeframe))
        
        # 生成查询语句
        _query = {
            "code": _code_pre,
            time_name: {
                time_seq[0]: float(time.mktime(time_begin.timetuple())),
                time_seq[1]: float(time.mktime(time_end.timetuple()))
            }
        }
        #print('--> time_begin:', time_begin, 'mktime:', float(time.mktime(time_begin.timetuple())))
        #print('--> time_end:', time_end, 'mktime:', float(time.mktime(time_end.timetuple())))
        if db_name[-3:] == 'min':
            _query['type'] = '1min' # 分钟数据类型还有 5min,15min,30min,60min
        #print(f'--> 查询 {db_name}: {_query}, {time_begin}~{time_end}')
        
        # 执行查询
        col = self.db.get_collection(db_name)
        if col.count_documents(_query) == 0:
            raise Exception('%s - no data found! Queried at %s: %s!' % (code, db_name, _query))
        #_bars = col.find(_query).sort([(time_name, ASCENDING)])
        # 采用 aggregate 取代 find 方法，将结果中的 date_stamp 列名改为 time_stamp
        _project_dict = {"open":1, "close":1, "high":1, "low":1, 
                        "vol":1, "amount":1, "code":1, "time_stamp":1}
        if time_name == 'date_stamp':
            _project_dict["time_stamp"] = "$" + time_name
        _bars = col.aggregate([
                    {"$match": _query},
                    {"$sort": {time_name:ASCENDING}},
                    {"$project": _project_dict}
                ])
        
        return _bars

    def _code_in_db(self, code, db_name):
        col = self.db.get_collection(db_name)
        _query = {
            "code": code
        }
        if col.count_documents(_query) > 0:
            return True
        else:
            return False
        