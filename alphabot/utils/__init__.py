import re
import datetime
from datetime import timedelta, tzinfo


class UTC(tzinfo):
    """实现了格林威治的tzinfo类"""

    def utcoffset(self, dt):
        return timedelta(0)

    def dst(self, dt):
        return timedelta(0)


class LocalTimezone(tzinfo):
    """实现本地时间的类"""

    def __init__(self, delta_hours=8):
        super(LocalTimezone, self).__init__()
        self._delta_hours = delta_hours

    def utcoffset(self, dt):
        return timedelta(hours=self._delta_hours) # 本地时区偏差

    def dst(self, dt):
        return timedelta(0)

    def tzname(self, dt):    #tzname需要返回时区名
        return '%c%02d:00' % ('+' if self._delta_hours >=0 else '-', self._delta_hours)
    
    
#print(datetime.datetime.now(UTC())) # UTC时间  差八个小时
#print(datetime.datetime.now(LocalTimezone())) # 根据本地时区生成offset-aware类的datetime对象
#print(type(datetime.datetime.now(LocalTimezone())))
#print(datetime.datetime.now()) # 北京时间，一旦生成了一个offset-naive类型的datetime对象
#print(datetime.datetime.now().replace(tzinfo=UTC())) # 调用replace(tzinfo=UTC())即可转换成offset-aware类型
#print(datetime.datetime.now().replace(tzinfo=LocalTimezone()).astimezone(UTC())) # 时区转换，


# 聚宽时间片单位，与 BT 时间片单位对应关系
import backtrader as bt
timeframe_types = dict(
    (
        ('1m', bt.TimeFrame.Minutes),
        ('1d', bt.TimeFrame.Days)
    )
)

# TimeStamp 转化为 Datetime
def timestamp2datetime(timestamp, convert_to_local=True, delta_hours=8):
    ''' Converts UNIX timestamp to a datetime object. '''
    if isinstance(timestamp, (int, int, float)):
        dt = datetime.datetime.utcfromtimestamp(timestamp)
        if convert_to_local: # 是否转化为本地时间
            dt = dt + datetime.timedelta(hours=delta_hours) # 中国默认时区
        return dt
    return timestamp



# 标的代码处理器
class CodeProcessor():
    """
    对标的代码进行解析、格式转换、分类等相关处理。
    本项目默认使用聚宽格式。
    """

    '''
    JQData证券代码标准格式（后缀）ref: https://www.joinquant.com/help/api/help
    
    由于同一代码可能代表不同的交易品种，JQData给每个交易品种后面都添加了该市场特定的代码后缀，
    用户在调用API时，需要将参数security传入带有该市场后缀的证券代码，
    如security='600519.XSHG'，以便于区分实际调用的交易品种。
    
    以下列出了每个交易市场的代码后缀和示例代码。
    交易市场            代码后缀  示例代码	     证券简称
    上海证券交易所      .XSHG	 '600519.XSHG'  贵州茅台
    深圳证券交易所	    .XSHE	 '000001.XSHE'  平安银行
    中金所             .CCFX	'IC9999.CCFX'   中证500主力合约
    大商所             .XDCE	'A9999.XDCE'    豆一主力合约
    上期所             .XSGE	'AU9999.XSGE'   黄金主力合约
    郑商所             .XZCE	'CY8888.XZCE'   棉纱期货指数
    上海国际能源期货    .XINE	 'SC9999.XINE'   原油主力合约
    '''

    def __init__(self):
        pass
        
    @classmethod
    def parse(self, code):
        # 解析证券代码
        _c = code.split('.')
        if len(_c) > 1:
            return _c[0], _c[1]
        else:
            return _c[0], None

    def get_code_type(self, code):
        pass

    def little8code(self, x):
        assert len(x) == 5
        if x == '.XSHG':
            x = 'SH'
        elif x == '.XSHE':
            x = 'SZ'
        elif x == '.INDX':
            x = 'IX'
        return x

    def convert_prefix(self, code):
        """
        将 600000.XSHG 格式的股票代码转化为 SH600000
        :param code:
        :return:
        """
        stock_format = [r'^[SI][ZHX]\d{6}$', r'^\d{6}\.[A-Z]{4}$']
        if re.match(stock_format[1], code):  # 600001.XSHG
            code = self.little8code(code[6:]) + code[:6]
        elif re.match(stock_format[0], code):  # SH600001
            pass
        else:
            print("股票格式错误 ~")
        return code

    def convert_postfix(self, code):
        """
        将 SH600000 格式的股票代码转化为 600000.XSHG
        :param code: 股票代码
        :return:
        """
        EXCHANGE_DICT = {
            "XSHG": "SH",
            "XSHE": "SZ",
            "XSGE": "SF",
            "XDCE": "DF",
            "XZCE": "ZF",
            "CCFX": "CF",
            "XINE": "IF",
        }
        CON_EXCHANGE_DICT = {value: key for key, value in EXCHANGE_DICT.items()}

        market = code[:2]
        code = code[2:]
        exchange = CON_EXCHANGE_DICT.get(market)
        return '.'.join((code, exchange))

    @classmethod
    def convert_jq_sdk(self, code):
        """
        使用聚宽函数将其他形式的股票代码转换为 jqdatasdk 函数可用的股票代码形式。
        仅适用于A股市场股票代码、期货以及基金代码。支持传入单只股票或一个股票 list。示例：
        #输入
        normalize_code(['000001', 'SZ000001', '000001SZ', '000001.sz', '000001.XSHE'])
        #输出
        ['000001.XSHE', '000001.XSHE', '000001.XSHE', '000001.XSHE', '000001.XSHE']
        """
        from jqdatasdk import normalize_code
        return normalize_code(code)
    
    @classmethod
    def convert_jq(self, code):
        """
        聚宽股票代码转换
        """
        # 提取字符串中的数字并返回
        def ston(string):
            s1 = ''
            for s in string:
                if s.isdecimal():
                    s1 = s1 + s
            return s1

        code = code.strip()
        s = ston(code)
        if (len(s) < 6 and len(s) > 0):
            s = s.zfill(6) + '.XSHE'
        if len(s) == 6:
            if s[0:1] == '6' or s[0:1] == '9' or s[0:1]== '5':
                s = s + '.XSHG'
            else:
                s = s + '.XSHE'
        return s

    @classmethod
    def convert_jq_to_plain(self, code):
        """
        将聚宽股票代码转换为纯数字股票代码形式。
        仅适用于A股市场股票代码、期货以及基金代码。支持传入单只股票。示例：
        #输入 '000001.XSHE'
        #输出 '000001'
        """
        return self.parse(code)[0]


class Dict2Obj(dict):
    """字典转为对象"""

    def __init__(self, *args, **kwargs):
        super(Dict2Obj, self).__init__(*args, **kwargs)

    def __getattr__(self, key):
        value = self[key]
        if isinstance(value, dict):
            value = Dict2Obj(value)
        return value
