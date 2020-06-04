from enum import auto
from typing import Iterable

from ..baseinterface.steps import Step, StepsFactory, Steps


class PreprocessStep(Step):
    PPCAL = auto()
    PPOBJ = auto()


class CalibrationStep(Step):
    BADPIX = auto()
    LOC = auto()
    SHAPE = auto()
    FLAT = auto()
    THERMAL = auto()
    WAVE = auto()


class ObjectStep(Step):
    EXTRACT = auto()
    LEAK = auto()
    FITTELLU = auto()
    CCF = auto()
    POL = auto()
    PRODUCTS = auto()


class DrsSteps(Steps):
    preprocess_steps_factory = StepsFactory(PreprocessStep, all_key='preprocess')
    calibration_steps_factory = StepsFactory(CalibrationStep, all_key='calibrations')
    object_steps_factory = StepsFactory(ObjectStep, all_key='objects')

    @classmethod
    def _steps_factories(cls) -> Iterable[StepsFactory]:
        return (cls.preprocess_steps_factory,
                cls.calibration_steps_factory,
                cls.object_steps_factory)
