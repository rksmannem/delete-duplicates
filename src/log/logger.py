import logging
import logging.config
import time

from logging.handlers import TimedRotatingFileHandler
from pythonjsonlogger import jsonlogger


class LoggerType:
    def __init__(self, log_file_name):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)

        json_handler = logging.StreamHandler()

        format_str = '%(asctime)s,%(levelname)-8s [%(filename)s:%(lineno)d] %(message)s'
        formatter = jsonlogger.JsonFormatter(format_str)
        json_handler.setFormatter(formatter)

        logger.addHandler(json_handler)

        filename = log_file_name + "_" + time.strftime("%m%d%Y_%H%M%S") + '.log'
        file_handler = TimedRotatingFileHandler(filename, when='midnight', interval=1, backupCount=0, encoding=None)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        self.Log = logger

    def get_logger(self):
        return self.Log
