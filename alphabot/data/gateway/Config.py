import enum
from StockConfig import StockDataSource

class IndustryCode(enum.Enum):
    sw_l1 = 0 # 申万一级行业
    sw_l2 = 1 # 申万二级行业
    sw_l3 = 2 # 申万三级行业
    jq_l1 = 3 # 聚宽一级行业
    jq_l2 = 4 # 聚宽二级行业
    zjw = 5  # 证监会行业


class Constant:
    DATE_INDEX = {
        StockDataSource.TUSHARE: "trade_time",
        StockDataSource.JQDATA: "date",
        StockDataSource.TDX: "date"
    }

    IDX_SUFFIX = '.IDX'

    TDX_BLOCK_CODE_LIST = 'code_list'
    TDX_BLOCK_NAME = 'blockname'

class TDX_BLOCK_NAME:
    HUSHENG_300 = '沪深300'
    ZHONGZHENG_100 = '中证100'
    ZHONGZHENG_200 = '中证200'
    CHUANGYEBANZHI = '创业板指'
    ZHONGZHENGHONGLI = '中证红利'


'''
交易市场	            代码后缀	示例代码	    证券简称
上海证券交易所	        .XSHG	    '600519.XSHG'	贵州茅台
深圳证券交易所	        .XSHE	    '000001.XSHE'	平安银行
中金所	                .CCFX	    'IC9999.CCFX'	中证500主力合约
大商所	                .XDCE	    'A9999.XDCE'	豆一主力合约
上期所	                .XSGE	    'AU9999.XSGE'	黄金主力合约
郑商所	                .XZCE	    'CY8888.XZCE'	棉纱期货指数
上海国际能源期货交易所	.XINE	    'SC9999.XINE'	原油主力合约
'''
class JQDATA_SUFFIX:
    SH = 'XSHG'
    SZ = 'XSHE'


class MARKET:
    SHANGHAI = 'sh'
    SHENZHEN = 'sz'

'''
both exist: 000001
both exist: 000002
both exist: 000009
both exist: 000010
both exist: 000016
both exist: 000017
both exist: 000019
both exist: 000043
both exist: 000903
both exist: 000905
'''
class DUPLICATED_INDEX_CODE_IN_MARKETS:
    code_map = {
        '000001': MARKET.SHANGHAI,
        '000002': MARKET.SHANGHAI,
        '000009': MARKET.SHANGHAI,
        '000010': MARKET.SHANGHAI,
        '000016': MARKET.SHANGHAI,
        '000017': MARKET.SHANGHAI,
        '000019': MARKET.SHANGHAI,
        '000043': MARKET.SHANGHAI,
        '000903': MARKET.SHANGHAI,
        '000905': MARKET.SHANGHAI,
    }
