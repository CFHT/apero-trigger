from typing import Collection, Dict, Iterable, Sequence

from logger import log
from .baseinterface.drstrigger import ICalibrationState, ICustomHandler, IDrsTrigger
from .baseinterface.steps import Step
from .common.pathhandler import Exposure
from .exposureconfig import SpirouExposureConfig
from .headerchecker import SpirouHeaderChecker
from .processor import Processor


class BaseDrsTrigger(IDrsTrigger):
    def __init__(self, steps: Collection[Step], trace=False, custom_handler=None):
        self.steps = steps
        self.custom_handler: ICustomHandler = custom_handler
        self.processor = Processor(self.steps, trace, self.custom_handler)

    def reduce(self, exposures_in_order: Iterable[Exposure]):
        self.processor.reset_state()
        for exposure in exposures_in_order:
            if not self.preprocess(exposure):
                continue
            try:
                self.process_file(exposure)
            except:
                log.error('Critical failure processing %s, skipping', exposure.preprocessed, exc_info=True)
        sequences = self.find_sequences(exposures_in_order)
        for sequence in sequences:
            try:
                result = self.process_sequence(sequence)
                if result and result.get('calibrations_complete'):
                    self.processor.reset_state(partial=True)
            except:
                log.error('Critical failure processing %s, skipping', sequence, exc_info=True)

    def preprocess(self, exposure: Exposure) -> bool:
        exposure_config = SpirouExposureConfig.from_file(exposure.raw)
        if self.custom_handler:
            self.custom_handler.exposure_pre_process(exposure)
        result = self.processor.preprocess_exposure(exposure_config, exposure)
        if self.custom_handler:
            self.custom_handler.exposure_preprocess_done(exposure)
        return result

    def process_file(self, exposure: Exposure) -> Dict:
        try:
            exposure_config = SpirouExposureConfig.from_file(exposure.preprocessed)
        except FileNotFoundError as err:
            log.error('File %s not found, skipping processing', err.filename)
        else:
            result = self.processor.process_exposure(exposure_config, exposure)
            if self.custom_handler:
                self.custom_handler.exposure_post_process(exposure, result)
            return result

    def process_sequence(self, exposures: Iterable[Exposure]) -> Dict:
        exposures_matching_config = []
        sequence_config = None
        for exposure in exposures:
            try:
                if sequence_config is None:
                    sequence_config = SpirouExposureConfig.from_file(exposure.preprocessed)
                else:
                    exposure_config = SpirouExposureConfig.from_file(exposure.preprocessed)
                    assert exposure_config == sequence_config, 'Exposure type changed mid-sequence'
                exposures_matching_config.append(exposure)
            except FileNotFoundError:
                log.error('Failed to open file %s while processing sequence, skipping file', exposure.preprocessed)
            except AssertionError as err:
                log.error(err.args)
        if exposures_matching_config:
            result = self.processor.process_sequence(sequence_config, exposures_matching_config)
            if self.custom_handler:
                self.custom_handler.sequence_post_process(exposures_matching_config, result)
            return result
        else:
            log.error('No files found in sequence, skipping')

    @staticmethod
    def find_sequences(exposures: Iterable[Exposure], **kwargs) -> Iterable[Sequence[Exposure]]:
        finished_sequences = []
        # TODO: should use OCTOKEN and OCEXPNUM if available
        """
        - OCTOKEN should stay the same across exposures
        - CMPLTEXP/NEXP can determine position in sequence
        - OCEXPNUM will be the same for a repeated exposure, but different if the sequence is repeated
            (e.g. increases when set up through the queue vs stays the same when RO does the repeat)
        - In most cases probably want to take highest SNR exposure for a given exposure for a polar sequence
            but have to consider the case with gaps between observations
        """
        fallback_exposures = exposures
        if fallback_exposures:
            exposure_order = {file: i for i, file in enumerate(exposures)}
            finished_sequences.extend(BaseDrsTrigger.find_sequences_fallback(fallback_exposures, **kwargs))
            finished_sequences.sort(key=lambda sequence: exposure_order[sequence[0]])
        return finished_sequences

    @staticmethod
    def find_sequences_fallback(exposures: Iterable[Exposure], **kwargs) -> Iterable[Sequence[Exposure]]:
        ignore_incomplete = kwargs.get('ignore_incomplete')
        ignore_incomplete_last = ignore_incomplete or kwargs.get('ignore_incomplete_last')
        finished_sequences = []
        current_sequence = []
        last_index = 0
        for exposure in exposures:
            header = SpirouHeaderChecker(exposure.raw)
            exp_index, exp_total = header.get_exposure_index_and_total()
            if exp_index < last_index + 1:
                if exp_index == 1:
                    log.warning('Exposure number reset mid-sequence, ending previous sequence early: %s',
                                current_sequence)
                    if not ignore_incomplete:
                        finished_sequences.append(current_sequence.copy())
                    current_sequence.clear()
                else:
                    log.error('Exposures appear to be out of order: %s', current_sequence)
            elif exp_index > last_index + 1:
                log.error('Exposure appears to be missing from sequence: %s', current_sequence)
            last_index = exp_index
            current_sequence.append(exposure)
            if exp_index == exp_total:
                finished_sequences.append(current_sequence.copy())
                current_sequence.clear()
                last_index = 0
        if not ignore_incomplete_last and current_sequence:
            finished_sequences.append(current_sequence.copy())
        return finished_sequences

    @property
    def calibration_state(self) -> ICalibrationState:
        return self.processor.calibration_processor.state

    @calibration_state.setter
    def calibration_state(self, state: ICalibrationState):
        self.processor.calibration_processor.state = state

    @staticmethod
    def Exposure(night: str, file: str) -> Exposure:
        return Exposure(night, file)
