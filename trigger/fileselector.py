from .common import log
from .drswrapper import TELLURIC_STANDARDS
from .exposureconfig import ExposureConfig, TargetType
from .headerchecker import HeaderChecker
from .steps import PreprocessStep, ObjectStep


class FileSelector:
    def __init__(self, file_selector_class=None):
        self.SingleFileSelector = file_selector_class if file_selector_class else SingleFileSelector

    def sort_and_filter_files(self, files, steps, filters):
        checkers = [HeaderChecker(file) for file in files]
        filtered = filter(lambda checker: self.SingleFileSelector(checker, steps).is_desired_file(filters), checkers)
        sorted = FileSelector.sort_files_by_observation_date(filtered)
        return sorted

    @staticmethod
    def sort_files_by_observation_date(checkers):
        file_times = {}
        for checker in checkers:
            obs_date = checker.get_obs_date()
            if not obs_date:
                log.warning('File %s missing observation date info, skipping.', checker.file)
            else:
                file_times[checker.file] = obs_date
        return sorted(file_times, key=file_times.get)


class SingleFileSelector:
    def __init__(self, checker, steps):
        self.checker = checker
        self.steps = steps

    def is_desired_file(self, filters):
        return self.is_desired_etype(self.checker, self.steps) and filters.matches_all_filters(self.checker)

    @classmethod
    def is_desired_etype(cls, checker, steps):
        return (cls.step_uses_all_calibrations(steps)and cls.has_calibration_extension(checker.file) or
                cls.step_uses_all_objects(steps) and cls.has_object_extension(checker.file) or
                steps.objects and cls.has_object_extension(checker.file) and cls.is_desired_object(checker, steps))

    @staticmethod
    def step_uses_all_calibrations(steps):
        return steps.preprocess and PreprocessStep.PPCAL in steps.preprocess or steps.calibrations

    @staticmethod
    def step_uses_all_objects(steps):
        return steps.preprocess and PreprocessStep.PPOBJ in steps.preprocess

    @staticmethod
    def step_uses_some_objects(steps):
        return steps.objects

    @staticmethod
    def has_calibration_extension(file):
        return file.name.endswith(('a.fits', 'c.fits', 'd.fits', 'f.fits'))

    @staticmethod
    def has_object_extension(file):
        return file.name.endswith('o.fits')

    @staticmethod
    def is_desired_object(checker, steps):
        object_config = ExposureConfig.from_header_checker(checker).object
        return (ObjectStep.EXTRACT in steps.objects or
                ObjectStep.POL in steps.objects and object_config.instrument_mode.is_polarimetry() or
                ObjectStep.FITTELLU in steps.objects and object_config.target == TargetType.STAR or
                ObjectStep.MKTEMPLATE in steps.objects and object_config.target == TargetType.STAR or
                ObjectStep.CCF in steps.objects and object_config.target == TargetType.STAR or
                ObjectStep.PRODUCTS in steps.objects)


class FileSelectionFilters:
    def __init__(self, runids=None, targets=None, unique_targets=False):
        self.runids = runids
        self.targets = targets
        self.unique_targets = set() if unique_targets else None

    def matches_all_filters(self, checker):
        return self.is_desired_runid(checker) and self.is_desired_target(checker)

    def is_desired_runid(self, checker):
        if self.runids is None:
            return True
        run_id = checker.get_runid()
        if not run_id:
            log.warning('File %s missing RUNID keyword, skipping.', checker.file)
            return False
        return run_id in self.runids

    def is_desired_target(self, checker):
        if self.targets is None:
            return True
        try:
            target = checker.get_object_name()
            return target in self.targets
        except RuntimeError:
            log.warning('File %s missing OBJECT keyword, skipping.', checker.file)
            return False

    def is_unique_target(self, checker):
        if self.unique_targets is None:
            return True
        target = checker.get_object_name()
        if target not in self.unique_targets:
            self.unique_targets.add(target)
            return True
        return False

    @classmethod
    def telluric_standards(cls):
        return cls(targets=TELLURIC_STANDARDS)
