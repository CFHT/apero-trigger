from __future__ import annotations

from enum import Enum, auto, EnumMeta
from typing import Tuple, Union

from logger import log
from .drsconstants import TELLURIC_STANDARDS
from ..baseinterface.headerchecker import HeaderChecker


class FiberType(Enum):
    DARK = 'DARK'
    FLAT = 'FLAT'
    FP = 'FP'
    HCONE = 'HCONE'
    HCTWO = 'HCTWO'
    OBJ = 'OBJ'


class ExposureType(EnumMeta):
    @property
    def science_fiber(self) -> FiberType:
        return self.value[0]

    @property
    def reference_fiber(self) -> FiberType:
        return self.value[1]

    def __dpr_type(self) -> str:
        return self.science_fiber.value + '_' + self.reference_fiber.value

    def to_dpr_type(self) -> str:
        if len(self.value > 2):
            return '_'.join((self.__dpr_type(), *self.value[2:]))
        return self.__dpr_type()

    @staticmethod
    def from_dpr_type(dpr_type: str) -> Union[Tuple[FiberType, FiberType], Tuple[FiberType, FiberType, str]]:
        sections = dpr_type.split('_')
        if len(sections) > 2:
            return (FiberType(sections[0]), FiberType(sections[1]), *sections[2:])
        return FiberType(sections[0]), FiberType(sections[1])


class ObjectType(Enum, metaclass=ExposureType):
    OBJ_DARK = (FiberType.OBJ, FiberType.DARK)
    OBJ_FP = (FiberType.OBJ, FiberType.FP)
    UNKNOWN = auto()

    @staticmethod
    def from_dpr_type(dpr_type: str) -> ObjectType:
        try:
            return ObjectType(ExposureType.from_dpr_type(dpr_type))
        except ValueError:
            log.warning('Unknown object type %s', dpr_type)
            return ObjectType.UNKNOWN


class CalibrationType(Enum, metaclass=ExposureType):
    DARK_DARK_INT = (FiberType.DARK, FiberType.DARK, 'INT')
    DARK_DARK_TEL = (FiberType.DARK, FiberType.DARK, 'TEL')
    DARK_FLAT = (FiberType.DARK, FiberType.FLAT)
    FLAT_DARK = (FiberType.FLAT, FiberType.DARK)
    FLAT_FLAT = (FiberType.FLAT, FiberType.FLAT)
    FP_FP = (FiberType.FP, FiberType.FP)
    HCONE_HCONE = (FiberType.HCONE, FiberType.HCONE)
    FP_HCONE = (FiberType.FP, FiberType.HCONE)
    HCONE_FP = (FiberType.HCONE, FiberType.FP)
    UNKNOWN = auto()

    @staticmethod
    def from_dpr_type(dpr_type: str) -> CalibrationType:
        try:
            return CalibrationType(ExposureType.from_dpr_type(dpr_type))
        except ValueError:
            log.warning('Unknown calibration type %s', dpr_type)
            return CalibrationType.UNKNOWN


class InstrumentMode(Enum):
    SPECTROSCOPY = ('P16', 'P16')
    POLAR1 = ('P14', 'P16')
    POLAR2 = ('P2', 'P16')
    POLAR3 = ('P2', 'P4')
    POLAR4 = ('P14', 'P4')
    UNKNOWN = auto()

    @staticmethod
    def from_rhombs(rhombs: Tuple[str, str]) -> InstrumentMode:
        try:
            return InstrumentMode(rhombs)
        except ValueError:
            log.warning('Unknown rhomb config %s %s', rhombs[0], rhombs[1])
            return InstrumentMode.UNKNOWN

    def is_polarimetry(self) -> bool:
        return self in (InstrumentMode.POLAR1,
                        InstrumentMode.POLAR2,
                        InstrumentMode.POLAR3,
                        InstrumentMode.POLAR4)


class TargetType(Enum):
    STAR = auto()
    SKY = auto()
    TELLURIC_STANDARD = auto()


class ObjectConfig:
    def __init__(self, instrument_mode: InstrumentMode, target: TargetType, object_type: ObjectType = None):
        self.instrument_mode = instrument_mode
        self.target = target
        self.object_type = object_type

    def __eq__(self, other: ObjectConfig):
        return (self.instrument_mode.is_polarimetry() == other.instrument_mode.is_polarimetry()
                and self.target == other.target
                and self.object_type == other.object_type)


class ExposureConfig:
    def __init__(self, calibration: CalibrationType = None, obj: ObjectConfig = None, is_aborted=False):
        if calibration is None and obj is None:
            raise ValueError('Exposure must be calibration or object')
        if calibration is not None and obj is not None:
            raise ValueError('Exposure cannot be both calibration and object')
        self.calibration = calibration
        self.object = obj
        self.is_aborted = is_aborted

    def __eq__(self, other: ExposureConfig):
        return (self.calibration and other.calibration and self.calibration == other.calibration
                or self.object and other.object and self.object == other.object)

    @classmethod
    def from_header_checker(cls, header_checker: HeaderChecker) -> ExposureConfig:
        is_aborted = header_checker.is_aborted()
        if header_checker.is_object():
            instrument_mode = InstrumentMode.from_rhombs(header_checker.get_rhomb_positions())
            if header_checker.is_sky():
                target = TargetType.SKY
            elif header_checker.get_object_name() in TELLURIC_STANDARDS:
                target = TargetType.TELLURIC_STANDARD
            else:
                target = TargetType.STAR
            try:
                dpr_type = header_checker.get_dpr_type()
                object_type = ObjectType.from_dpr_type(dpr_type)
                obj = ObjectConfig(instrument_mode, target, object_type)
            except (ValueError, RuntimeError):
                obj = ObjectConfig(instrument_mode, target)
            return cls(obj=obj, is_aborted=is_aborted)
        else:
            try:
                dpr_type = header_checker.get_dpr_type()
                calibration = CalibrationType.from_dpr_type(dpr_type)
            except (ValueError, RuntimeError):
                calibration = True
            return cls(calibration=calibration, is_aborted=is_aborted)
