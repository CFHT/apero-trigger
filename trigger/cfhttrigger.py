from __future__ import annotations

from pathlib import Path

from apero.core.instruments.default.default_config import DRS_VERSION
from astropy.io import fits

from logger import log
from .common import Fiber
from .common.exposureconfig import ExposureConfig
from .common.pathhandler import Exposure
from .dbinterface import DatabaseHeaderConverter, QsoDatabase
from .director import director_message
from .distribution import distribute_raw_file
from .drswrapper.drs import DRS
from .drswrapper.errorhandler import IErrorHandler
from .drswrapper.recipefailure import RecipeFailure
from .sessionlink import setup_symlink

TRIGGER_VERSION = '0.7.001'


class CfhtHandler(IErrorHandler):
    def __init__(self):
        self.director_warnings = False

    def handle_recipe_failure(self, error: RecipeFailure):
        if self.director_warnings:
            director_message(str(error), level='warning')


class CfhtTrigger:
    def __init__(self, trace=False):
        self.custom_handler: CfhtHandler = CfhtHandler()
        self.drs = DRS(trace, error_handler=self.custom_handler)
        self.database = QsoDatabase()

    def process_calibrations(self, night: str):
        runfile = 'calib_run.ini'
        self.drs.apero_processing(runfile, obs_dir=night)

    def preprocess(self, exposure: Exposure) -> bool:
        distribute_raw_file(exposure.raw)
        result = self.drs.preprocess(exposure)
        if result:
            self.__update_db_with_headers(exposure.odometer, exposure.preprocessed, preprocessed_only=True)
        return result

    def process_file(self, exposure: Exposure):
        try:
            exposure_config = ExposureConfig.from_file(exposure.preprocessed)
        except FileNotFoundError as err:
            log.error('File %s not found, skipping processing', err.filename)
        else:
            if exposure_config.object and not exposure_config.is_aborted:
                if self.drs.extract(exposure, fiber=Fiber.AB.value, quicklook=True):
                    extracted_path = exposure.q2ds(Fiber.AB)
                    self.__update_db_with_headers(exposure.odometer, extracted_path)

    def exposure(self, night: str, file: str) -> Exposure:
        return Exposure(night, file)

    def exposure_from_path(self, file_path: Path) -> Exposure:
        # return Exposure.from_path(file_path)
        return setup_symlink(file_path)

    def __update_db_with_headers(self, odometer: int, path: Path, preprocessed_only=False):
        db_headers = {'obsid': str(odometer)}
        try:
            if path:
                with fits.open(path) as hdu_list:
                    if preprocessed_only:
                        db_headers.update(DatabaseHeaderConverter.preprocessed_header_to_db(hdu_list[0].header))
                    else:
                        db_headers.update(DatabaseHeaderConverter.extracted_header_to_db(hdu_list[0].header))
        except FileNotFoundError as err:
            log.warning('File not found during database update: %s', err.filename)
        self.database.send_pipeline_headers(db_headers, in_progress=preprocessed_only)

    @staticmethod
    def drs_version() -> str:
        return DRS_VERSION.value

    @staticmethod
    def trigger_version() -> str:
        return TRIGGER_VERSION
