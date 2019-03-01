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

    def reduce_night(self, night, runid=None):
        if self.do_realtime:
            raise RuntimeError('Realtime mode not meant for reducing entire night!')
        path_pattern = PathHandler(night, '*.fits').raw.fullpath
        all_files = [file for file in glob.glob(path_pattern) if os.path.exists(file)]  # filter out broken symlinks
        files = sort_and_filter_files(all_files, self.types, runid)  # Filter out unused input files ahead of time
        self.reduce(night, files)

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
        finished_sequence = None
        header = fits.open(file)[0].header
        if 'CMPLTEXP' not in header or 'NEXP' not in header:
            logger.warning('%s missing CMPLTEXP/NEXP in header, treating sequence as single exposure', file)
            exp_index = 1
            exp_total = 1
        else:
            exp_index = header['CMPLTEXP']
            exp_total = header['NEXP']
        if len(current_sequence) > 0 and exp_index == 1:
            logger.warning('Exposure number reset mid-sequence, ending previous sequence early: %s', current_sequence)
            finished_sequence = current_sequence.copy()
            current_sequence.clear()
        current_sequence.append(file)
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
