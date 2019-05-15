from collections import namedtuple

from trigger import DrsSteps


class CfhtDrsSteps(namedtuple('CfhtDrsSteps', DrsSteps._fields + ('distribute', 'database'))):
    @classmethod
    def all(cls):
        drs_steps = DrsSteps.all()
        return cls(*drs_steps, True, True)

    @classmethod
    def from_keys(cls, keys):
        drs_steps = DrsSteps.from_keys(keys)
        distribute = 'distribute' in keys
        database = 'database' in keys
        return cls(*drs_steps, distribute, database)
