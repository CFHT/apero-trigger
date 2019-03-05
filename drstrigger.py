import os, glob
from collections import defaultdict
from astropy.io import fits

from logger import logger
from commandmap import CommandMap
from pathhandler import PathHandler
from drswrapper import DRS
from fileselector import sort_and_filter_files, is_telluric_standard, is_spectroscopy

TRIGGER_VERSION = '011'

class DrsTrigger:
    def __init__(self, realtime=False, trace=False, ccf_mask=None, **types):
        self.do_realtime = realtime
        self.ccf_mask = ccf_mask
        self.types = defaultdict(lambda: True, types)
        self.command_map = CommandMap(self.types, trace, realtime)

    def __find_files(self, night, runid=None):
        path_pattern = PathHandler(night, '*.fits').raw.fullpath
        all_files = [file for file in glob.glob(path_pattern) if os.path.exists(file)]  # filter out broken symlinks
        files = sort_and_filter_files(all_files, self.types, runid)  # Filter out unused input files ahead of time
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
            self.process_file(night, file)
            completed_sequence = self.sequence_checker(night, current_sequence, file)
            if completed_sequence:
                self.process_sequence(night, completed_sequence)

    def preprocess(self, night, file):
        path = PathHandler(night, file)
        try:
            return self.command_map.preprocess_exposure(path)
        except Exception as e:
            raise RuntimeError('Error running pre-processing on', path.raw.fullpath, e)

    def process_file(self, night, file):
        path = PathHandler(night, file)
        exposure_config = self.__exposure_config_from_file(path.preprocessed.fullpath)
        try:
            result = self.command_map.process_exposure(exposure_config, path, self.ccf_mask)
            return result
        except Exception as e:
            raise RuntimeError('Error extracting', path.preprocessed.fullpath, e)

    def process_sequence(self, night, files):
        paths = [PathHandler(night, file) for file in files]
        sequence_config = self.__exposure_config_from_file(paths[0].preprocessed.fullpath)
        for path in paths:
            exposure_config = self.__exposure_config_from_file(path.preprocessed.fullpath)
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
        filename = path.raw.filename
        finished_sequence = None
        header = fits.open(path.raw.fullpath)[0].header
        if 'CMPLTEXP' not in header or 'NEXP' not in header:
            logger.warning('%s missing CMPLTEXP/NEXP in header, treating sequence as single exposure', filename)
            exp_index = 1
            exp_total = 1
        else:
            exp_index = header['CMPLTEXP']
            exp_total = header['NEXP']
        if len(current_sequence) > 0 and exp_index == 1:
            logger.warning('Exposure number reset mid-sequence, ending previous sequence early: %s', current_sequence)
            finished_sequence = current_sequence.copy()
            current_sequence.clear()
        current_sequence.append(filename)
        if exp_index == exp_total:
            if finished_sequence:
                logger.error('Unable to return early ended sequence: %s', current_sequence)
            finished_sequence = current_sequence.copy()
            current_sequence.clear()
        return finished_sequence

    @staticmethod
    def __exposure_config_from_file(filename):
        try:
            header = fits.open(filename)[0].header
        except:
            raise RuntimeError('Failed to open', filename)
        if 'OBSTYPE' in header and header['OBSTYPE'] == 'OBJECT':
            prefix = 'SPEC' if is_spectroscopy(header) else 'POL'
            suffix = 'TELL' if is_telluric_standard(header) else 'OBJ'
            return prefix + '_' + suffix
        elif 'DPRTYPE' in header and header['DPRTYPE'] != 'None':
            return header['DPRTYPE']
        else:
            raise RuntimeError('Non-object file missing DPRTYPE keyword', filename)

    @staticmethod
    def drs_version():
        return DRS.version()

    @staticmethod
    def trigger_version():
        return TRIGGER_VERSION
