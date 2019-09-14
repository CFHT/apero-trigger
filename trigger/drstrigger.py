from multiprocessing import Pool

from .basedrstrigger import BaseDrsTrigger
from .common import log
from .drswrapper import DRS_VERSION
from .fileselector import FileSelector
from .pathhandler import Night, RootDirectories
from .steps import ObjectStep


class DrsTrigger(BaseDrsTrigger):
    def __init__(self, steps, ccf_params, trace=False, custom_handler=None):
        super().__init__(steps, ccf_params, trace, custom_handler)

    @staticmethod
    def drs_version():
        return DRS_VERSION

    def reduce_all_nights(self, filters, num_processes=None):
        nights = self.__find_nights('*')
        self.reduce_nights(nights, filters, num_processes)

    def reduce_qrun(self, qrunid, filters, num_processes=None):
        nights = self.__find_nights(qrunid + '-*')
        self.reduce_nights(nights, filters, num_processes)

    def reduce_nights(self, nights, filters, num_processes=None):
        if num_processes:
            pool = Pool(num_processes)
            combined = [(item, filters) for item in nights]
            pool.starmap(self.reduce_night, combined)
        else:
            for night in nights:
                self.reduce_night(night, filters)

    def reduce_night(self, night, filters):
        log.info('Processing night %s', night)
        files = self.__find_files(night, filters)
        self.reduce(night, files)

    def reduce_range(self, night, start_file, end_file, filters):
        files = self.__find_files(night, filters)
        subrange = self.__get_subrange(files, start_file, end_file)
        if subrange:
            self.reduce(night, subrange)

    def mk_tellu(self):
        if self.steps.object_steps and ObjectStep.MKTELLU in self.steps.object_steps:
            self.processor.drs.obj_mk_tellu()

    def __find_nights(self, night_pattern):
        night_root = RootDirectories.input
        nights = [night for night in night_root.glob(night_pattern) if night.is_dir()]
        return sorted(nights)

    def __find_files(self, night, filters):
        night_directory = Night(night).input_directory
        all_files = [file for file in night_directory.glob('*.fits') if file.exists()]  # Filter out broken symlinks
        file_selector = self.get_file_selector()
        files = file_selector.sort_and_filter_files(all_files, self.steps, filters)  # Filter out unused input files
        return files

    def get_file_selector(self):
        return FileSelector()

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
