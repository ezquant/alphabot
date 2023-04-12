:: python -m alphabot.cli --worker data-fetch --config-file config_dev.yaml
:: python -m alphabot.cli --worker run-backtest --config-file config_dev.yaml

::set strat_file=sample.py
::set strat_file=simple.py
::set strat_file=joinquant/etf_28_lundong.py
set strat_file=joinquant/live_pairs_fund_good.py

::set timeframe=1d
::set fromdate=2020-06-01T00:00:00
::set todate=2020-08-01T00:00:00

set timeframe=1m
set fromdate=2020-09-01T00:00:00
set todate=2020-09-08T23:59:59

python.exe -m alphabot.cli --worker run-backtest --config-file config_dev.yaml ^
  --fromdate %fromdate% ^
  --todate %todate% ^
  --strategy-file %strat_file% ^
  --timeframe %timeframe%
  ::--cerebro runonce=False
