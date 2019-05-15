import logging
from collections import namedtuple

log = logging.getLogger('drs_trigger')

CcfParams = namedtuple('CcfParams', ('mask', 'v0', 'range', 'step'))

FIBER_LIST = ('AB', 'A', 'B', 'C')


# Exception representing any failure for a DRS recipe
class RecipeFailure(Exception):
    def __init__(self, reason, command_string):
        self.reason = reason
        self.command_string = command_string

    def __str__(self):
        return 'DRS command failed (' + self.reason + '): ' + self.command_string
