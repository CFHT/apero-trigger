from __future__ import annotations

from collections import defaultdict
from enum import Enum, auto
from pathlib import Path
from typing import Collection, Dict, Iterable, Optional, Sequence, Tuple, Union

from logger import log
from .baseinterface.headerchecker import DateTime
from .baseinterface.steps import Step
from .common import CalibrationStep, Exposure, ExposureConfig, ObjectStep, PreprocessStep, TELLURIC_STANDARDS
from .headerchecker import HeaderChecker, SpirouHeaderChecker
from .processor import Processor


class FileType(Enum):
    CALIBRATION = auto()
    OBJECT = auto()
    ALL = auto()


class FileSelector:
    def single_file_selector(self, exposure: Exposure, steps: Collection[Step]) -> SingleFileSelector:
        return SingleFileSelector(exposure, steps)

    def sort_and_filter_files_split(self, exposures: Iterable[Exposure], steps: Collection[Step],
                                    filters: FileSelectionFilters) -> Tuple[Sequence[Exposure], Sequence[Exposure]]:
        date_by_file_by_type = self.__filter_files(exposures, steps, filters, True)
        return (sorted(date_by_file_by_type[FileType.CALIBRATION], key=date_by_file_by_type[FileType.CALIBRATION].get),
                sorted(date_by_file_by_type[FileType.OBJECT], key=date_by_file_by_type[FileType.OBJECT].get))

    def sort_and_filter_files_combined(self, exposures: Iterable[Exposure], steps: Collection[Step],
                                       filters: FileSelectionFilters) -> Sequence[Exposure]:
        date_by_file_by_type = self.__filter_files(exposures, steps, filters, False)
        return sorted(date_by_file_by_type[FileType.ALL], key=date_by_file_by_type[FileType.ALL].get)

    def __filter_files(self, exposures: Iterable[Exposure], steps: Collection[Step], filters: FileSelectionFilters,
                       split_by_type: bool) -> Dict[FileType, Dict[Exposure, DateTime]]:
        date_by_file_by_type = defaultdict(dict)
        for exposure in exposures:
            selector = self.single_file_selector(exposure, steps)
            desired_file_check = selector.is_desired_file(filters)
            if not desired_file_check:
                continue
            desired_file_type, checker = desired_file_check
            if not split_by_type:
                desired_file_type = FileType.ALL
            sort_key = checker.get_obs_date()
            if not sort_key:
                log.warning('File %s missing observation date info, skipping.', checker.file)
            else:
                date_by_file_by_type[desired_file_type][exposure] = sort_key
        return date_by_file_by_type


class SingleFileSelector:
    def __init__(self, exposure: Exposure, steps: Collection[Step]):
        self.exposure = exposure
        self.steps = steps

    def is_desired_file(self, filters: FileSelectionFilters) -> Union[FileType, None]:
        result = self.a_step_uses_file(self.exposure, self.steps)
        if result and filters.matches_all_filters(result[1]):
            return result

    @classmethod
    def a_step_uses_file(cls, exposure: Exposure, steps: Collection[Step]) -> Optional[Tuple[FileType, HeaderChecker]]:
        file = exposure.raw
        if cls.has_calibration_extension(exposure.raw):
            if not any(cls.is_calibration_step(step) for step in steps):
                return
            file_type = FileType.CALIBRATION
            if PreprocessStep.PPCAL not in steps:
                if exposure.preprocessed.exists():
                    file = exposure.preprocessed
        elif cls.has_object_extension(exposure.raw):
            if not any(cls.is_object_step(step) for step in steps):
                return
            file_type = FileType.OBJECT
            if PreprocessStep.PPOBJ not in steps:
                if exposure.preprocessed.exists():
                    file = exposure.preprocessed
        else:
            return
        checker = SpirouHeaderChecker(file)
        exposure_config = ExposureConfig.from_header_checker(checker)
        if any(cls.is_exposure_config_used_for_step(exposure_config, step) for step in steps):
            return file_type, checker

    @staticmethod
    def is_exposure_config_used_for_step(exposure_config: ExposureConfig, step: Step):
        return Processor.is_exposure_config_used_for_step(exposure_config, step)

    @staticmethod
    def is_calibration_step(step: Step) -> bool:
        return step == PreprocessStep.PPCAL or isinstance(step, CalibrationStep)

    @staticmethod
    def is_object_step(step: Step) -> bool:
        return step == PreprocessStep.PPOBJ or isinstance(step, ObjectStep)

    @staticmethod
    def has_calibration_extension(file: Path) -> bool:
        return file.name.endswith(('a.fits', 'c.fits', 'd.fits', 'f.fits'))

    @staticmethod
    def has_object_extension(file: Path) -> bool:
        return file.name.endswith('o.fits')


class FileSelectionFilters:
    def __init__(self, runids=None, targets=None, unique_targets=False):
        self.runids = runids
        self.targets = targets
        self.unique_targets = set() if unique_targets else None

    def matches_all_filters(self, checker: HeaderChecker) -> bool:
        return self.is_desired_runid(checker) and self.is_desired_target(checker)

    def is_desired_runid(self, checker: HeaderChecker) -> bool:
        if self.runids is None:
            return True
        run_id = checker.get_runid()
        if not run_id:
            log.warning('File %s missing RUNID keyword, skipping.', checker.file)
            return False
        return run_id in self.runids

    def is_desired_target(self, checker: HeaderChecker) -> bool:
        if self.targets is None:
            return True
        try:
            target = checker.get_object_name()
            return target in self.targets
        except RuntimeError:
            log.warning('File %s missing OBJECT keyword, skipping.', checker.file)
            return False

    def is_unique_target(self, checker: HeaderChecker) -> bool:
        if self.unique_targets is None:
            return True
        target = checker.get_object_name()
        if target not in self.unique_targets:
            self.unique_targets.add(target)
            return True
        return False

    @classmethod
    def telluric_standards(cls) -> FileSelectionFilters:
        return cls(targets=TELLURIC_STANDARDS)
