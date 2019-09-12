from abc import ABC, abstractmethod

from .common import log
from .exposureconfig import ExposureConfig
from .headerchecker import HeaderChecker
from .pathhandler import Exposure
from .processor import Processor


class AbstractCustomHandler(ABC):
    @abstractmethod
    def handle_recipe_failure(self, error):
        pass

    @abstractmethod
    def exposure_pre_process(self, exposure):
        pass

    @abstractmethod
    def exposure_post_process(self, exposure, result):
        pass

    @abstractmethod
    def sequence_post_process(self, sequence, result):
        pass


class BaseDrsTrigger:
    def __init__(self, steps, ccf_params, trace=False, custom_handler=None):
        self.steps = steps
        self.custom_handler = custom_handler
        self.processor = Processor(self.steps, ccf_params, trace, self.custom_handler)

    def reduce(self, night, files_in_order):
        current_sequence = []
        self.processor.reset_state()
        for file in files_in_order:
            if not self.preprocess(night, file):
                continue
            try:
                self.process_file(night, file)
                completed_sequences = self.sequence_checker(night, current_sequence, file)
                for completed_sequence in completed_sequences:
                    if completed_sequence:
                        self.process_sequence(night, completed_sequence)
            except:
                log.error('Critical failure processing %s, skipping', file, exc_info=True)

    def preprocess(self, night, file):
        exposure = Exposure(night, file)
        exposure_config = ExposureConfig.from_file(exposure.raw)
        if self.custom_handler:
            self.custom_handler.exposure_pre_process(exposure)
        return self.processor.preprocess_exposure(exposure_config, exposure)

    def process_file(self, night, file):
        exposure = Exposure(night, file)
        exposure_config = ExposureConfig.from_file(exposure.preprocessed)
        result = self.processor.process_exposure(exposure_config, exposure)
        if self.custom_handler:
            self.custom_handler.exposure_post_process(exposure, result)

    def process_sequence(self, night, files):
        exposures = []
        sequence_config = None
        for file in files:
            exposure = Exposure(night, file)
            try:
                if sequence_config is None:
                    sequence_config = ExposureConfig.from_file(exposure.preprocessed)
                else:
                    exposure_config = ExposureConfig.from_file(exposure.preprocessed)
                    assert exposure_config.is_matching_type(sequence_config), 'Exposure type changed mid-sequence'
                exposures.append(exposure)
            except FileNotFoundError:
                log.error('Failed to open file %s while processing sequence, skipping file', exposure.preprocessed)
            except AssertionError as err:
                log.error(err.args)
        if exposures:
            result = self.processor.process_sequence(sequence_config, exposures)
            if self.custom_handler:
                self.custom_handler.sequence_post_process(exposures, result)
        else:
            log.error('No files found in sequence, skipping')

    # Appends file to current_sequence, and if sequence is now complete, returns it and clears current_sequence.
    @staticmethod
    def sequence_checker(night, current_sequence, file):
        exposure = Exposure(night, file)
        finished_sequences = []
        header = HeaderChecker(exposure.raw)
        exp_index, exp_total = header.get_exposure_index_and_total()
        if len(current_sequence) > 0 and exp_index == 1:
            log.warning('Exposure number reset mid-sequence, ending previous sequence early: %s', current_sequence)
            finished_sequences.append(current_sequence.copy())
            current_sequence.clear()
        current_sequence.append(exposure.raw.name)
        if exp_index == exp_total:
            finished_sequences.append(current_sequence.copy())
            current_sequence.clear()
        return finished_sequences
