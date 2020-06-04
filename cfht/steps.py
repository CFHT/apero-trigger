from enum import auto
from typing import Iterable

from trigger.baseinterface.steps import Step, StepsFactory
from trigger.common import DrsSteps


class CfhtStep(Step):
    DISTRIBUTE = auto()
    DISTRAW = auto()
    DATABASE = auto()


class CfhtDrsSteps(DrsSteps):
    cfht_steps_factory = StepsFactory(CfhtStep)

    @classmethod
    def _steps_factories(cls) -> Iterable[StepsFactory]:
        return (*super()._steps_factories(), cls.cfht_steps_factory)
