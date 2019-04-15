import os, glob
from collections import defaultdict
from astropy.io import fits

from logger import logger
from commandmap import CommandMap
from pathhandler import PathHandler
from drswrapper import DRS
from fileselector import sort_and_filter_files, HeaderChecker

TRIGGER_VERSION = '014'

class DrsTrigger:
    def __init__(self, steps, realtime=False, trace=False, ccf_params=None):
        self.do_realtime = realtime
        self.ccf_params = ccf_params
        self.steps = steps
        self.command_map = CommandMap(self.steps, trace, realtime)

    def __find_files(self, night, runid=None):
        path_pattern = PathHandler(night, '*.fits').raw.fullpath
        all_files = [file for file in glob.glob(path_pattern) if os.path.exists(file)]  # filter out broken symlinks
        files = sort_and_filter_files(all_files, self.steps, runid)  # Filter out unused input files ahead of time
        return files

    def __get_subrange(self, files, start_file, end_file):
        start_index, end_index = None, None
        for i, file in enumerate(files):
            filename = os.path.basename(file)
            if filename == start_file:
                start_index = i
            if filename == end_file:
                end_index = i + 1
        if start_index is not None and end_index is not None:
            return files[start_index:end_index]
        if start_index is None:
            logger.error('Did not find range start file %s', start_file)
        if end_index is None:
            logger.error('Did not find range end file %s', end_file)

    def reduce_night(self, night, runid=None):
        if self.do_realtime:
            raise RuntimeError('Realtime mode not meant for reducing entire night!')
        files = self.__find_files(night, runid)
        self.reduce(night, files)

    def reduce_range(self, night, start_file, end_file):
        if self.do_realtime:
            raise RuntimeError('Realtime mode not meant for reducing entire fileset!')
        files = self.__find_files(night)
        subrange = self.__get_subrange(files, start_file, end_file)
        if subrange:
            self.reduce(night, subrange)

    def reduce(self, night, files_in_order):
        if self.do_realtime:
            raise RuntimeError('Realtime mode not meant for reducing entire fileset!')
        current_sequence = []
        for file in files_in_order:
            if not self.preprocess(night, file):
                continue
            try:
                self.process_file(night, file)
                completed_sequence = self.sequence_checker(night, current_sequence, file)
                if completed_sequence:
                    self.process_sequence(night, completed_sequence)
            except:
                logger.error('Critical failure processing %s, skipping', file, exc_info=True)

    def preprocess(self, night, file):
        path = PathHandler(night, file)
        try:
            return self.command_map.preprocess_exposure(path)
        except Exception as e:
            raise RuntimeError('Error running pre-processing on', path.raw.fullpath, e)

    def process_file(self, night, file):
        path = PathHandler(night, file)
        exposure_config = self.__exposure_config_from_file(path)
        try:
            result = self.command_map.process_exposure(exposure_config, path, self.ccf_params)
            return result
        except Exception as e:
            raise RuntimeError('Error extracting', path.preprocessed.fullpath, e)

    def process_sequence(self, night, files):
        paths = [PathHandler(night, file) for file in files]
        sequence_config = self.__exposure_config_from_file(paths[0])
        for path in paths:
            exposure_config = self.__exposure_config_from_file(path)
            assert exposure_config == sequence_config, 'Exposure type changed mid-sequence'
        try:
            result = self.command_map.process_sequence(sequence_config, paths)
            return result
        except Exception as e:
            raise RuntimeError('Error processing sequence', files, e)

    # Appends file to current_sequence, and if sequence is now complete, returns it and clears current_sequence.
    @staticmethod
    def sequence_checker(night, current_sequence, file):
        path = PathHandler(night, file)
        finished_sequence = None
        header = HeaderChecker(path.raw.fullpath)
        exp_index, exp_total = header.get_exposure_index_and_total()
        if len(current_sequence) > 0 and exp_index == 1:
            logger.warning('Exposure number reset mid-sequence, ending previous sequence early: %s', current_sequence)
            finished_sequence = current_sequence.copy()
            current_sequence.clear()
        current_sequence.append(path.raw.filename)
        if exp_index == exp_total:
            if finished_sequence:
                logger.error('Unable to return early ended sequence: %s', current_sequence)
            finished_sequence = current_sequence.copy()
            current_sequence.clear()
        return finished_sequence

    @staticmethod
    def __exposure_config_from_file(path):
        filename = path.preprocessed.fullpath
        header = HeaderChecker(filename)
        dpr_type = header.get_dpr_type()
        if header.is_object():
            mode = 'SPEC' if header.is_spectroscopy() else 'POL'
            tell = 'TELL' if header.is_telluric_standard() else ''
            obj_type = mode + tell
            if dpr_type.startswith('OBJ_'):
                return dpr_type.replace('OBJ', obj_type, 1)
            else:
                logger.warning('Object exposure %s has DPRTYPE %s instead of OBJ', filename, dpr_type)
        return dpr_type

    @staticmethod
    def drs_version():
        return DRS.version()

    @staticmethod
    def trigger_version():
        return TRIGGER_VERSION
