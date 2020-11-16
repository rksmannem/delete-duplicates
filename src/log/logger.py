import logging
import logging.config
import time

from logging.handlers import TimedRotatingFileHandler
from pythonjsonlogger import jsonlogger


class LoggerType:
    def __init__(self, log_file_name):
        logger = logging.getLogger(__name__)

        format_str = '%(asctime)s,%(levelname)-8s [%(filename)s:%(lineno)d] %(message)s'
        formatter = jsonlogger.JsonFormatter(format_str)
        logger.setLevel(logging.DEBUG)

        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        stream_handler.setFormatter(formatter)

        filename = log_file_name + "_" + time.strftime("%m%d%Y_%H%M%S") + '.log'

        file_handler = TimedRotatingFileHandler(filename, when='midnight', interval=1, backupCount=0, encoding=None)
        file_handler.setFormatter(formatter)

        logger.addHandler(stream_handler)
        logger.addHandler(file_handler)

        self.Log = logger

    def get_logger(self):
        new_logger = self.Log
        return new_logger
