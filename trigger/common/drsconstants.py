from enum import Enum
from pathlib import Path
from typing import List

from apero.core import constants
from apero.science import telluric

config = constants.load('SPIROU')

DRS_VERSION: str = config['DRS_VERSION']
TELLURIC_STANDARDS: List[str] = list(telluric.get_whitelist(config)[0])


class RootDataDirectories:
    input: Path = Path(config['DRS_DATA_RAW'])
    tmp: Path = Path(config['DRS_DATA_WORKING'])
    reduced: Path = Path(config['DRS_DATA_REDUC'])


class Fiber(Enum):
    AB = 'AB'
    A = 'A'
    B = 'B'
    C = 'C'


class CcfParams:
    mask: str = config['CCF_DEFAULT_MASK']
    width: float = config['CCF_DEFAULT_WIDTH']
    step: float = config['CCF_DEFAULT_STEP']
