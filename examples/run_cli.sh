#strat_file=sample.py
#strat_file=simple.py
#strat_file=joinquant/etf_28_lundong.py
#strat_file=joinquant/live_pairs_fund_good.py

#python -m alphabot.cli --config-file config_dev.yaml --worker data-fetch
python -m alphabot.cli --config-file config_dev.yaml --worker run-backtest
#python -m alphabot.cli --config-file config_dev.yaml --worker run-trading
