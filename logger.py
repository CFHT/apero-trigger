import logging, sys
import subprocess

logger = logging.getLogger('drs_trigger')

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

def configure(console_level='INFO', file=None, file_level='INFO'):
    formatter = LogFormatter()
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.getLevelName(console_level))
    logger.addHandler(console_handler)
    if file is not None:
        try:
            file_handler = logging.FileHandler(file)
        except:
            logger.error('Could not open log file %s', file)
        else:
            file_handler.setFormatter(formatter)
            file_handler.setLevel(logging.getLevelName(file_level))
            logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)

def director_message(message, level=None):
    if level:
        message = level + ': ' + message
    command = '@say_ ' + message + '\n'
    subprocess.run(['nc', '-q', '0', 'spirou-session', '20140'], input=command, encoding='ascii')
