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
        self.processor.reset_state()
        for file in files_in_order:
            if not self.preprocess(night, file):
                continue
            try:
                self.process_file(night, file)
            except:
                log.error('Critical failure processing %s, skipping', file, exc_info=True)
        sequences = self.find_sequences(night, files_in_order)
        for sequence in sequences:
            try:
                self.process_sequence(night, sequence)
            except:
                log.error('Critical failure processing %s, skipping', sequence, exc_info=True)

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
        return result

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
            return result
        else:
            log.error('No files found in sequence, skipping')

    @staticmethod
    def find_sequences(night, files, **kwargs):
        finished_sequences = []
        # TODO: should use OCTOKEN and OCEXPNUM if available
        '''
        - OCTOKEN should stay the same across exposures
        - CMPLTEXP/NEXP can determine position in sequence
        - OCEXPNUM will be the same for a repeated exposure, but different if the sequence is repeated
            (e.g. increases when set up through the queue vs stays the same when RO does the repeat)
        - In most cases probably want to take highest SNR exposure for a given exposure for a polar sequence
            but have to consider the case with gaps between observations
        '''
        fallback_files = files
        if fallback_files:
            exposure_order = {file: i for i, file in enumerate(files)}
            finished_sequences.extend(BaseDrsTrigger.find_sequences_fallback(night, fallback_files, **kwargs))
            finished_sequences.sort(key=lambda sequence: exposure_order[sequence[0]])
        return finished_sequences

    @staticmethod
    def find_sequences_fallback(night, files, **kwargs):
        ignore_incomplete = kwargs.get('ignore_incomplete')
        ignore_incomplete_last = ignore_incomplete or kwargs.get('ignore_incomplete_last')
        finished_sequences = []
        current_sequence = []
        last_index = 0
        for file in files:
            exposure = Exposure(night, file)
            header = HeaderChecker(exposure.raw)
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
            current_sequence.append(file)
            if exp_index == exp_total:
                finished_sequences.append(current_sequence.copy())
                current_sequence.clear()
                last_index = 0
        if not ignore_incomplete_last and current_sequence:
            finished_sequences.append(current_sequence.copy())
        return finished_sequences
