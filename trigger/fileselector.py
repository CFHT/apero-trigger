from .common import log
from .exposureconfig import ExposureConfig, TargetType
from .headerchecker import HeaderChecker
from .steps import PreprocessStep, ObjectStep


class FileSelector:
    def __init__(self, file_selector_class=None):
        self.SingleFileSelector = file_selector_class if file_selector_class else SingleFileSelector

    def sort_and_filter_files(self, files, steps, runid=None):
        checkers = [HeaderChecker(file) for file in files]
        filtered = filter(lambda checker: self.SingleFileSelector(checker, steps).is_desired_file(runid), checkers)
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

    def is_desired_file(self, runid=None):
        return self.is_desired_etype(self.checker, self.steps) and self.is_desired_runid(self.checker, runid)

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
                ObjectStep.MKTELLU in steps.objects and object_config.target == TargetType.TELLURIC_STANDARD or
                ObjectStep.FITTELLU in steps.objects and object_config.target == TargetType.STAR or
                ObjectStep.CCF in steps.objects and object_config.target == TargetType.STAR or
                ObjectStep.PRODUCTS in steps.objects)

    @staticmethod
    def is_desired_runid(checker, runid_filter=None):
        run_id = checker.get_runid()
        if runid_filter and not run_id:
            log.warning('File %s missing RUNID keyword, skipping.', checker.file)
            return False
        elif runid_filter and run_id != runid_filter:
            return False
        return True
