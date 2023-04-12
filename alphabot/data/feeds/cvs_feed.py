import backtrader as bt


class CSVFeed(bt.feeds.GenericCSVData):
    """交易信息数据格式。
    除此之外还有扩展版的 OLHCVA 格式等。"""

    params = (
        ('dtformat', '%Y-%m-%d %H:%M:%S'),
        ('datetime', 0),
        ('time', -1),
        ('open', 1),
        ('high', 2),
        ('low', 3),
        ('close', 4),
        ('volume', 5),
        ('openinterest', -1),
        ('timeframe', bt.TimeFrame.Minutes),
        ('compression', 1), #60 表示一小时
    )
