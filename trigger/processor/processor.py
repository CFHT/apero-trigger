from typing import Collection, Dict, Sequence

from .calibrationprocessor import CalibrationProcessor
from .drswrapper import DRS
from .objectprocessor import ObjectProcessor
from ..baseinterface.processor import IErrorHandler
from ..baseinterface.steps import Step
from ..common import CalibrationStep, Exposure, ExposureConfig, ObjectStep, PreprocessStep


class Processor:
    def __init__(self, steps: Collection[Step], trace: bool, error_handler: IErrorHandler):
        self.steps = steps
        self.drs = DRS(trace, error_handler=error_handler)
        self.calibration_processor = CalibrationProcessor(steps, self.drs)
        self.object_processor = ObjectProcessor(steps, self.drs)

    def preprocess_exposure(self, config: ExposureConfig, exposure: Exposure) -> bool:
        if (config.object and PreprocessStep.PPOBJ in self.steps
                or config.calibration and PreprocessStep.PPCAL in self.steps):
            if config.is_aborted:
                return False
            return self.drs.cal_preprocess(exposure)
        else:
            return exposure.preprocessed.exists()

    def process_exposure(self, config: ExposureConfig, exposure: Exposure) -> Dict:
        if config.object:
            return self.object_processor.process_object_exposure(config.object, exposure)

    def process_sequence(self, config: ExposureConfig, exposures: Sequence[Exposure]) -> Dict:
        if config.calibration:
            self.calibration_processor.add_sequence_to_queue(exposures, config.calibration)
            return self.calibration_processor.attempt_processing_queue()
        elif config.object:
            return self.object_processor.process_object_sequence(config.object, exposures)

    def reset_state(self):
        self.calibration_processor.reset_state(partial=False)

    @staticmethod
    def is_exposure_config_used_for_step(config: ExposureConfig, step: Step) -> bool:
        if config.calibration and step == PreprocessStep.PPCAL or config.object and step == PreprocessStep.PPOBJ:
            return True
        if config.calibration and isinstance(step, CalibrationStep):
            return CalibrationProcessor.is_calibration_type_used_for_step(config.calibration, step)
        elif config.object and isinstance(step, ObjectStep):
            return ObjectProcessor.is_object_config_used_for_step(config.object, step)
