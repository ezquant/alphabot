from alphabot.config import config, load_config
from earnmi.data.FetcherDailyBar import test as test_fdb
from earnmi.data.FetcherMintuesBar import test as test_fmb

load_config('config_dev.yaml')
#test_fdb()
test_fmb()

