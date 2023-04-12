# real_trader

zzz: 尚未开始整合。

本地化的tick实盘交易解决方案 [历史行情数据 + 实时tick数据 + 实盘下单] 后续增加历史tick接口

目前支持的券商 中泰证券

支持聚宽策略代码直接使用

使用期间遇到问题 或 需要低佣金开户 欢迎加我微信沟通

<img src="https://i.loli.net/2020/04/29/STGwYt9OV2vBLk5.jpg" width="30%" height="30%" />

### 安装 tesseract-ocr 

#### 1. 下载 tesseract-ocr 并配置环境变量

首先需要下载 tesseract，它为 tesserocr 提供了支持。
进入下载页面，选择下载 5.0 版本。或者访问这里：百度网盘 <a href="https://pan.baidu.com/s/1ZQTx3t9ICVMaXyzqRRLwcA" >tesseract-ocr</a>
下载完成后双击运行，安装程序。需要注意的是，需要句选 Additional language data(download）选项来安装 OCR 识别支持的语言包，这样 OCR 便可以识别多国语言 。

给tesseract配置环境变量：
（1）将tesseract安装路径添加到path环境变量中
（2）将tesseract的语言包添加到环境变量中：在环境变量中新建一个系统变量，变量名称为TESSDATA_PREFIX，取值为安装目录下的tessdata路径
     如：C:\Program Files\Tesseract-OCR\tessdata

接下来 ， 再安装 tesserocr 即可：
conda install -c simonflueckiger tesserocr

#### 2. 下载券商客户端
http://download.95538.cn/download/software/hx/ths_order.exe

#### 3. 安装python依赖

```
## 安装依赖
cd real_trader && pip install -r requirements.txt
```

期间可能出现 Microsoft Visual C++ 14.0 is required. Get it with "Microsoft Visual C++ Build Tools

可以下载 <a href="https://pan.baidu.com/s/1VpQOyy3riFXmQobugLNcyQ" >visualcppbuildtools_full</a> 解决此问题

### 云端部署
建议使用 TightVNC 微软自带的远程桌面有些技术问题

###  编写一个简单的例子 订阅贵州茅台的实时tick并下单

新建一个code.py

首先引入依赖

```
## 引入所需的python类库
from jqdatasdk import *
from trade_bundle.live_trade import *
from trade_order.order_api import *    
```

然后编写整体结构 

```
## 初始化时调用
def initialize(context):

	## 订阅贵州茅台的tick
	## 通常是在开盘订阅 这里为了测试放在了初始化函数里订阅
	subscribe('600519.XSHG', 'tick')
	
## 开盘前调用 09:00
def before_trading_start(context):
	pass
	
## 盘中tick触发调用
def handle_tick(context, tick):
	
	## 这里打印出订阅的股票代码和当前价格
	print('股票代码 => {} 当前价格 => {}'.format(tick.code, tick.current))
	
	## 查询实盘账号有多少可用资金
	cash = context.portfolio.available_cash
	print('当前可用资金 => {}'.format(cash))
	
	## 交易函数慎重调用 因为直接对接实盘
	## 满仓市价买入贵州茅台
	## order_value(tick.code, cash)
	
	## 市价买入100股贵州茅台
	## order(tick.code, 100)
	
## 收盘后半小时调用 15:30
def after_trading_end(context):
	
	## 收盘后取消所有标的订阅
	unsubscribe_all()
```

紧跟着在代码最后添加配置信息

```
## 初始化jqdatasdk 
## 方便获取历史行情和财务数据 暂时从聚宽获取
## 这里需要申请一下 https://www.joinquant.com/default/index/sdk
auth('聚宽账号','聚宽密码')

## 初始化实盘下单模块 
## 这里填写实盘资金账号和密码 还有 券商客户端安装路径 
## 客户端默认安装路径是 D:\中泰证券独立下单\xiadan.exe
init_trader(g, context, '资金账号', '资金密码', r'D:\中泰证券独立下单\xiadan.exe')

## 初始化实时tick行情
init_current_bundle(initialize, before_trading_start, after_trading_end, handle_tick)
```

最后完整的代码应该是这样 使用python index.py 执行就可以了

```
## 引入所需的python类库
from jqdatasdk import *
from trade_bundle.live_trade import *
from trade_order.order_api import *   

## 初始化时调用
def initialize(context):
	
	## 订阅贵州茅台的tick
	## 通常是在开盘订阅 这里为了测试放在了初始化函数里订阅
	subscribe('600519.XSHG', 'tick')
	
## 开盘前调用 09:00
def before_trading_start(context):
	pass
	
## 盘中tick触发调用
def handle_tick(context, tick):
	
	## 这里打印出订阅的股票代码和当前价格
	print('股票代码 => {} 当前价格 => {}'.format(tick.code, tick.current))
	
	## 查询实盘账号有多少可用资金
	cash = context.portfolio.available_cash
	print('当前可用资金 => {}'.format(cash))
	
	## 交易函数慎重调用 因为直接对接实盘 需要时解开注释
	## 满仓市价买入贵州茅台
	## order_value(tick.code, cash)
	
	## 市价买入100股贵州茅台
	## order(tick.code, 100)
	
## 收盘后半小时调用 15:30
def after_trading_end(context):
	
	## 收盘后取消所有标的订阅
	unsubscribe_all()
	
## 初始化jqdatasdk 
## 方便获取历史行情和财务数据 暂时从聚宽获取
## 这里需要申请一下 https://www.joinquant.com/default/index/sdk
auth('聚宽账号','聚宽密码')

## 初始化实盘下单模块 
## 这里填写实盘资金账号和密码 还有 券商客户端安装路径 
## 客户端默认安装路径是 D:\中泰证券独立下单\xiadan.exe
init_trader(g, context, '资金账号', '资金密码', r'D:\中泰证券独立下单\xiadan.exe')

## 初始化实时tick行情
init_current_bundle(initialize, before_trading_start, after_trading_end, handle_tick)
```
