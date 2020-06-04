from abc import ABC, abstractmethod
from typing import Dict, Iterable, Sequence

from .exposure import IExposure
from .processor import IErrorHandler


class ICustomHandler(IErrorHandler):
    @abstractmethod
    def exposure_pre_process(self, exposure: IExposure):
        pass

    @abstractmethod
    def exposure_preprocess_done(self, exposure: IExposure):
        pass

    @abstractmethod
    def exposure_post_process(self, exposure: IExposure, result):
        pass

    @abstractmethod
    def sequence_post_process(self, sequence: Iterable[IExposure], result):
        pass


class ICalibrationState(ABC):
    pass


class IDrsTrigger(ABC):
    @abstractmethod
    def reduce(self, exposures_in_order: Iterable[IExposure]):
        pass

    @abstractmethod
    def preprocess(self, exposure: IExposure) -> bool:
        pass

    @abstractmethod
    def process_file(self, exposure: IExposure) -> Dict:
        pass

    @abstractmethod
    def process_sequence(self, exposures: Iterable[IExposure]) -> Dict:
        pass

    @staticmethod
    @abstractmethod
    def find_sequences(exposures: Iterable[IExposure], **kwargs) -> Iterable[Sequence[IExposure]]:
        pass

    @property
    @abstractmethod
    def calibration_state(self) -> ICalibrationState:
        pass

    @calibration_state.setter
    @abstractmethod
    def calibration_state(self, state: ICalibrationState):
        pass

    @staticmethod
    @abstractmethod
    def Exposure(night: str, file: str) -> IExposure:
        pass
