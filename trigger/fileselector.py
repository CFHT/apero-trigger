from .common import log
from .exposureconfig import ExposureConfig, TargetType
from .headerchecker import HeaderChecker
from .steps import PreprocessStep, ObjectStep


def sort_and_filter_files(files, steps, runid=None):
    checkers = [HeaderChecker(file) for file in files]
    filtered = filter(lambda checker: is_desired_file(checker, steps)
                                      and not checker.is_aborted()
                                      and is_desired_runid(checker, runid),
                      checkers)
    return sort_files_by_observation_date(filtered)


def is_desired_file(checker, steps):
    return (steps.preprocess and PreprocessStep.PPCAL in steps.preprocess and has_calibration_extension(checker.file) or
            steps.preprocess and PreprocessStep.PPOBJ in steps.preprocess and has_object_extension(checker.file) or
            steps.calibrations and has_calibration_extension(checker.file) or
            steps.objects and has_object_extension(checker.file) and is_desired_object(checker, steps))


def has_object_extension(file):
    return file.name.endswith('o.fits')


def has_calibration_extension(file):
    return file.name.endswith(('a.fits', 'c.fits', 'd.fits', 'f.fits'))


def is_desired_object(checker, steps):
    object_config = ExposureConfig.from_header_checker(checker).object
    return (ObjectStep.EXTRACT in steps.objects or
            ObjectStep.POL in steps.objects and object_config.instrument_mode.is_polarimetry() or
            ObjectStep.MKTELLU in steps.objects and object_config.target == TargetType.TELLURIC_STANDARD or
            ObjectStep.FITTELLU in steps.objects and object_config.target == TargetType.STAR or
            ObjectStep.CCF in steps.objects and object_config.target == TargetType.STAR or
            ObjectStep.PRODUCTS in steps.objects or
            steps.distribute or
            steps.database)


def is_desired_runid(checker, runid_filter=None):
    run_id = checker.get_runid()
    if runid_filter and not run_id:
        log.warning('File %s missing RUNID keyword, skipping.', checker.file)
        return False
    elif runid_filter and run_id != runid_filter:
        return False
    return True


def sort_files_by_observation_date(checkers):
    file_times = {}
    for checker in checkers:
        obs_date = checker.get_obs_date()
        if not obs_date:
            log.warning('File %s missing observation date info, skipping.', checker.file)
        else:
            file_times[checker.file] = obs_date
    return sorted(file_times, key=file_times.get)
