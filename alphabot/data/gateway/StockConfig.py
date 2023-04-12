import enum


class Constant:
    MINUTES_IN_DAY = 240


class StockDataType(enum.Enum):
    UNDEFINED = 0
    DAILY = 1
    ONE_HOUR = 2
    THIRTY_MINS = 3
    TEN_MINS = 4
    FIVE_MINS = 5
    ONE_MIN = 6


class StockDataSource(enum.Enum):
    UNKNOWN = 0
    MIXED = 1
    TUSHARE = 2
    JQDATA = 3
    TDX = 4
    LOCAL = 5


class StockCode:
    Test = ["000001", "000002", "000003"]
