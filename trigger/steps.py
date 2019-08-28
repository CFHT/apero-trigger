from collections import namedtuple
from enum import Enum, auto


class PreprocessStep(Enum):
    PPCAL = auto()
    PPOBJ = auto()


class CalibrationStep(Enum):
    DARK = auto()
    BADPIX = auto()
    LOC = auto()
    SLIT = auto()
    SHAPE = auto()
    FF = auto()
    WAVE = auto()


class ObjectStep(Enum):
    EXTRACT = auto()
    POL = auto()
    FITTELLU = auto()
    CCF = auto()
    PRODUCTS = auto()
    MKTELLU = auto()


class StepsFactory:
    def __init__(self, enum):
        self.enum = enum

    def all(self):
        return set(self.enum)

    def from_keys(self, keys, all_key=None):
        if all_key and all_key in keys:
            return self.all()
        steps = set()
        for key in keys:
            try:
                steps.add(self.enum[key.upper()])
            except:
                pass
        return steps


class DrsSteps(namedtuple('DrsSteps', ('preprocess', 'calibrations', 'objects'))):
    preprocess_steps_factory = StepsFactory(PreprocessStep)
    calibration_steps_factory = StepsFactory(CalibrationStep)
    object_steps_factory = StepsFactory(ObjectStep)

    @classmethod
    def all(cls):
        return cls(cls.preprocess_steps_factory.all(),
                   cls.calibration_steps_factory.all(),
                   cls.object_steps_factory.all())

    @classmethod
    def from_keys(cls, keys):
        preprocess_steps = cls.preprocess_steps_factory.from_keys(keys, all_key='preprocess')
        calibration_steps = cls.calibration_steps_factory.from_keys(keys, all_key='calibrations')
        object_steps = cls.object_steps_factory.from_keys(keys, all_key='objects')
        return cls(preprocess_steps, calibration_steps, object_steps)
