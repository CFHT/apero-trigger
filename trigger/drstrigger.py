from multiprocessing import Pool

from .basedrstrigger import BaseDrsTrigger
from .common import log
from .drswrapper import DRS_VERSION
from .fileselector import sort_and_filter_files
from .pathhandler import Night, RootDirectories


class DrsTrigger(BaseDrsTrigger):
    @staticmethod
    def drs_version():
        return DRS_VERSION

    def reduce_all_nights(self, num_processes=None, runid=None):
        nights = self.__find_nights('*')
        self.reduce_nights(nights, num_processes, runid)

    def reduce_qrun(self, qrunid, num_processes=None, runid=None):
        nights = self.__find_nights(qrunid + '-*')
        self.reduce_nights(nights, num_processes, runid)

    def reduce_nights(self, nights, num_processes=None, runid=None):
        if num_processes:
            pool = Pool(num_processes)
            combined = [(item, runid) for item in nights]
            pool.starmap(self.reduce_night, combined)
        else:
            for night in nights:
                self.reduce_night(night, runid)

    def reduce_night(self, night, runid=None):
        log.info('Processing night %s', night)
        files = self.__find_files(night, runid)
        self.reduce(night, files)

    def reduce_range(self, night, start_file, end_file):
        files = self.__find_files(night)
        subrange = self.__get_subrange(files, start_file, end_file)
        if subrange:
            self.reduce(night, subrange)

    def __find_nights(self, night_pattern):
        night_root = RootDirectories.input
        nights = [night for night in night_root.glob(night_pattern) if night.is_dir()]
        return sorted(nights)

    def __find_files(self, night, runid=None):
        night_directory = Night(night).input_directory
        all_files = [file for file in night_directory.glob('*.fits') if file.exists()]  # filter out broken symlinks
        files = sort_and_filter_files(all_files, self.steps, runid)  # Filter out unused input files ahead of time
        return files

    def __get_subrange(self, files, start_file, end_file):
        start_index, end_index = None, None
        for i, file in enumerate(files):
            if file.name == start_file:
                start_index = i
            if file.name == end_file:
                end_index = i + 1
        if start_index is not None and end_index is not None:
            return files[start_index:end_index]
        if start_index is None:
            log.error('Did not find range start file %s', start_file)
        if end_index is None:
            log.error('Did not find range end file %s', end_file)
