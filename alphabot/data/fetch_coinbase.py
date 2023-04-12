from datetime import datetime, timedelta
from alphabot.exchange.coinbase.adapter import CoinbaseAdapter
from alphabot.storage.manager import StorageManager
from alphabot import logger


def import_rates():
    storage_manager = StorageManager('mongodb')
    granularity = 3600
    product_id = 'ETH-USD'
    offset = 3

    max_candle = storage_manager.get_max_candle_by_time('ETH-USD')
    if max_candle:
        start = max_candle['time']
        retrieve_forward(datetime.fromtimestamp(start), product_id, granularity, offset)
    else:
        end = datetime.utcnow() - timedelta(days=15)
        retrieve_backward(end, product_id, granularity, offset)


def retrieve_forward(start, product_id, granularity, offset):
    adapter = CoinbaseAdapter()
    storage_manager = StorageManager('mongodb')

    while True:
        yesterday = datetime.utcnow() - timedelta(days=1)
        if start + timedelta(days=offset) > yesterday:
            end = yesterday
        else:
            end = start + timedelta(days=offset)
        logger.info('Importing from: {start} to end: {end}'.format(start=start, end=end))
        response = adapter.get_historic_rates(product_id,
                                              start.replace(microsecond=0).isoformat(),
                                              end.replace(microsecond=0).isoformat(),
                                              granularity)

        for candle in response:
            storage_manager.create_candle(candle[0],
                                          candle[1],
                                          candle[2],
                                          candle[3],
                                          candle[4],
                                          candle[5],
                                          product_id,
                                          granularity)

        max_time = response[0][0]
        start = datetime.fromtimestamp(max_time)
        if start.date() == yesterday.date():
            break


def retrieve_backward(end, product_id, granularity, offset):
    adapter = CoinbaseAdapter()
    storage_manager = StorageManager('mongodb')

    while True:
        start = end - timedelta(days=offset)
        logger.info('Importing from: {start} to end: {end}'.format(start=start, end=end))
        response = adapter.get_historic_rates(product_id,
                                              start.replace(microsecond=0).isoformat(),
                                              end.replace(microsecond=0).isoformat(),
                                              granularity)
        if not response:
            break

        for candle in response:
            storage_manager.create_candle(candle[0],
                                          candle[1],
                                          candle[2],
                                          candle[3],
                                          candle[4],
                                          candle[5],
                                          product_id,
                                          granularity)

        min_time = response[-1][0]
        end = datetime.fromtimestamp(min_time)


if __name__ == '__main__':
    import_rates()
