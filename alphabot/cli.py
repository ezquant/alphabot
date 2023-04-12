from __future__ import (absolute_import, division, print_function,
                                unicode_literals)

import time
import argparse

from alphabot.config import config, load_config
from alphabot.data.fetch_quantaxis import fetch_quantaxis_data
from alphabot.utils import Dict2Obj, timeframe_types
from alphabot.utils.log import logger
from alphabot.utils.log import setup_logging
from alphabot.storage.manager import StorageManager
from alphabot.backend import RunningManager
from alphabot.backend import RunningModes


def parse_args(pargs=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=(
            'Alphabot CLI Entry.'
        )
    )
    
    # set config file
    parser.add_argument('--config-file',
                        type=str.lower,
                        help='Config file (YAML)')
    
    # set log
    parser.add_argument('--log-file', help='Log file name')
    parser.add_argument('--logging-level',
                        default='info',
                        choices=['debug', 'info', 'warning', 'error'],
                        type=str.lower,
                        help='Print logs')
    
    # worker mode
    parser.add_argument('--worker',
                        choices=['data-fetch', 'run-backtest', 'run-trading'],
                        type=str.lower,
                        help='Workers')
    
    return parser.parse_args(pargs)


def entry():
    args = parse_args()

    # Logging setup
    setup_logging(args.logging_level, filename=args.log_file)

    # Load yaml config file （必须执行，否则 config 将为空）
    args.cfg = Dict2Obj(load_config(args.config_file))
    print('--> yaml config:', config)
    logger.info('Exchange => %s', args.cfg.settings.exchange)
    logger.info('Strategy file => %s', args.cfg.settings.strategy_file)

    if args.worker == 'data-fetch':
        logger.info('Starting data fetch')
        while True:
            # 更新 QUANTAXIS
            fetch_quantaxis_data()
            logger.info('Sleeping for 4 hours...')
            break  # 若需持续定时更新数据，则注销次行
            #time.sleep(3600*4)
        logger.info('Exiting..')
        return
    
    if args.worker == 'run-backtest':
        logger.info('Starting backtest')
        rm = RunningManager(RunningModes.BACKTEST, args)
        rm.run()

    if args.worker == 'run-trading':
        logger.info('Starting live trading')
        rm = RunningManager(RunningModes.TRADING, args)
        rm.run()


if __name__ == '__main__':
    
    entry()
