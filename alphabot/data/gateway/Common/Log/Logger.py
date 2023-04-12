import logging
import sys
from logging.handlers import TimedRotatingFileHandler
import os
from Common import FileUtils

FORMATTER = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
LOG_FILE_NAME = "quant_log.log"

CONSOLE_LEVEL = logging.DEBUG
FILE_LEVEL = logging.WARNING

class Logger:

    __logger = None

    @classmethod
    def __get_console_handler(cls):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(FORMATTER)
        console_handler.setLevel(CONSOLE_LEVEL)
        return console_handler

    @classmethod
    def __get_file_handler(cls):
        current_file = os.path.abspath(os.path.dirname(__file__))
        log_path = os.path.join(
            current_file, FileUtils.convert_file_path_based_on_system('..\\..\\Logs\\'))
        if not os.path.exists(log_path):
            os.makedirs(log_path)

        log_file_path = log_path + LOG_FILE_NAME

        file_handler = TimedRotatingFileHandler(log_file_path, when='midnight')
        file_handler.setFormatter(FORMATTER)
        file_handler.setLevel(FILE_LEVEL)

        return file_handler

    @classmethod
    def get_logger(cls, logger_name):
        if Logger.__logger is None:
            # logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.DEBUG)  # better to have too much log than not enough
            logger.addHandler(cls.__get_console_handler())
            logger.addHandler(cls.__get_file_handler())
            # with this pattern, it's rarely necessary to propagate the error up to parent
            logger.propagate = False
            Logger.__logger = logger
        return Logger.__logger
