from enum import Enum
from pathlib import Path

from apero.core import constants

config = constants.load('SPIROU')

class RootDataDirectories:
    input: Path = Path(config['DRS_DATA_RAW'])
    tmp: Path = Path(config['DRS_DATA_WORKING'])
    reduced: Path = Path(config['DRS_DATA_REDUC'])


class Fiber(Enum):
    AB = 'AB'
    A = 'A'
    B = 'B'
    C = 'C'
