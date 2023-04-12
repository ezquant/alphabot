import logging
import logging.handlers

from alphabot import logger


#FORMAT = "[%(asctime)s] Thread(%(threadName)s) %(levelname)s %(name)s:%(funcName)s:%(lineno)s - %(message)s"
FORMAT = "[%(threadName)s] %(levelname)s %(name)s:%(funcName)s:%(lineno)s - %(message)s"


def setup_logging(level, filename=None):
    level = getattr(logging, level.upper())
    h = logging.StreamHandler()
    h.setLevel(level)
    h.setFormatter(logging.Formatter(FORMAT))
    logger.setLevel(level)
    logger.addHandler(h)

    if filename:
        trfh = logging.handlers.TimedRotatingFileHandler(filename, 'h', 1, 100)
        trfh.setLevel(level)
        trfh.setFormatter(logging.Formatter(FORMAT))
        logger.addHandler(trfh)
