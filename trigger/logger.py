import logging, sys

from .common import log

class LogFormatter(logging.Formatter):
    def __init__(self, format='%(levelname)s: %(message)s'):
        super().__init__(format)

    def format(self, record):
        original_format = self._style._fmt
        if record.levelno == logging.INFO:
            self._style._fmt = '%(message)s'
        result = super().format(record)
        self._style._fmt = original_format
        return result


class LogFile:
    def __init__(self, file, level):
        self.file = file
        self.level = level

    def toHandler(self, formatter):
        try:
            file_handler = logging.FileHandler(self.file)
        except:
            raise RuntimeError('Could not open log file ' + self.file)
        else:
            file_handler.setFormatter(formatter)
            file_handler.setLevel(logging.getLevelName(self.level))
            return file_handler


def configure_logger(logger=log, console_level='INFO', log_files=None):
    formatter = LogFormatter()
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.getLevelName(console_level))
    logger.addHandler(console_handler)
    if log_files:
        for log_file in log_files:
            file_handler = LogFile(*log_file).toHandler(formatter)
            if file_handler:
                logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)
