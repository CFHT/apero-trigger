from multiprocessing import Pool
from typing import Collection, Iterable, Sequence, Tuple

from logger import log
from .basedrstrigger import BaseDrsTrigger
from .baseinterface.steps import Step
from .common import CcfParams, Exposure, Night
from .common.drsconstants import DRS_VERSION, RootDataDirectories
from .fileselector import FileSelectionFilters, FileSelector


class DrsTrigger(BaseDrsTrigger):
    def __init__(self, steps: Collection[Step], ccf_params: CcfParams, trace=False, custom_handler=None):
        super().__init__(steps, ccf_params, trace, custom_handler)

    @staticmethod
    def drs_version() -> str:
        return DRS_VERSION

    def reduce_all_nights(self, filters: FileSelectionFilters, num_processes: int = None):
        nights = self.__find_nights('*')
        self.reduce_nights(nights, filters, num_processes)

    def reduce_qrun(self, qrunid: str, filters: FileSelectionFilters, num_processes: int = None):
        nights = self.__find_nights(qrunid + '-*')
        self.reduce_nights(nights, filters, num_processes)

    def reduce_nights(self, nights: Iterable[str], filters: FileSelectionFilters, num_processes: int = None):
        if num_processes:
            pool = Pool(num_processes)
            combined = [(item, filters) for item in nights]
            pool.starmap(self.reduce_night, combined)
        else:
            for night in nights:
                self.reduce_night(night, filters)

    def reduce_night(self, night: str, filters: FileSelectionFilters):
        log.info('Processing night %s', night)
        calibrations, objects = self.__files_split(night, filters)
        self.reduce(calibrations)
        self.reduce(objects)

    def reduce_range(self, night: str, start_file: str, end_file: str, filters: FileSelectionFilters):
        files = self.__files_combined(night, filters)
        subrange = self.__get_subrange(files, Exposure(night, start_file), Exposure(night, end_file))
        if subrange:
            self.reduce(subrange)

    @staticmethod
    def __find_nights(night_pattern: str) -> Sequence[str]:
        night_root = RootDataDirectories.input
        nights = [str(night.relative_to(night_root)) for night in night_root.glob(night_pattern) if night.is_dir()]
        return sorted(nights)

    @staticmethod
    def __find_exposures(night: str) -> Sequence[Exposure]:
        night_directory = Night(night).input_directory
        log.info('Looking for fits files in %s', night_directory)
        # Filter out broken symlinks
        return [Exposure(night, file.name) for file in night_directory.glob('*.fits') if file.exists()]

    def __files_split(self, night: str, filters: FileSelectionFilters) -> Tuple[Sequence[Exposure], Sequence[Exposure]]:
        all_exposures = self.__find_exposures(night)
        calibrations, objects = self.get_file_selector().sort_and_filter_files_split(all_exposures, self.steps, filters)
        log.info('%s calibration files used:', len(calibrations))
        log.info('%s', '\n'.join((exp.raw.name for exp in calibrations)))
        log.info('%s object files used:', len(objects))
        log.info('%s', '\n'.join((exp.raw.name for exp in objects)))
        return calibrations, objects

    def __files_combined(self, night: str, filters: FileSelectionFilters) -> Sequence[Exposure]:
        all_exposures = self.__find_exposures(night)
        exposures = self.get_file_selector().sort_and_filter_files_combined(all_exposures, self.steps, filters)
        log.info('%s files used:', len(exposures))
        log.info('%s', '\n'.join((exp.raw.name for exp in exposures)))
        return exposures

    def get_file_selector(self) -> FileSelector:
        return FileSelector()

    @staticmethod
    def __get_subrange(files: Iterable[Exposure], start_file: Exposure, end_file: Exposure) -> Iterable[Exposure]:
        start_index, end_index = None, None
        for i, file in enumerate(files):
            if file == start_file:
                start_index = i
            if file == end_file:
                end_index = i + 1
        if start_index is not None and end_index is not None:
            return files[start_index:end_index]
        if start_index is None:
            log.error('Did not find range start file %s', start_file)
        if end_index is None:
            log.error('Did not find range end file %s', end_file)
