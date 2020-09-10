import logging
import sys
from os import PathLike
from typing import Iterable, Tuple

log: logging.Logger = logging.getLogger('drs_trigger')


class LogFormatter(logging.Formatter):
    def __init__(self, fmt='%(levelname)s: %(message)s'):
        super().__init__(fmt)

    def format(self, record):
        original_format = self._style._fmt
        if record.levelno == logging.INFO:
            self._style._fmt = '%(message)s'
        result = super().format(record)
        self._style._fmt = original_format
        return result


class LogFile:
    def __init__(self, file: PathLike, level: str):
        self.file = file
        self.level = level

    def to_handler(self, formatter: LogFormatter):
        try:
            file_handler = logging.FileHandler(self.file)
        except OSError:
            raise RuntimeError('Could not open log file ' + str(self.file))
        else:
            file_handler.setFormatter(formatter)
            file_handler.setLevel(logging.getLevelName(self.level))
            return file_handler


def configure_logger(logger=log, console_level='INFO', log_files: Iterable[Tuple[PathLike, str]] = None):
    formatter = LogFormatter()
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.getLevelName(console_level))
    logger.addHandler(console_handler)
    if log_files:
        for log_file in log_files:
            file_handler = LogFile(*log_file).to_handler(formatter)
            if file_handler:
                logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)
