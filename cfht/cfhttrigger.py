from pathlib import Path
from typing import Collection, Dict, Iterable, Sequence

from astropy.io import fits

from logger import log
from trigger.baseinterface.drstrigger import ICustomHandler
from trigger.baseinterface.processor import RecipeFailure
from trigger.baseinterface.steps import Step
from trigger.common import Exposure
from trigger.drstrigger import DrsTrigger
from trigger.exposureconfig import SpirouExposureConfig
from trigger.fileselector import FileSelector
from .dbinterface import DatabaseHeaderConverter, QsoDatabase
from .director import director_message
from .distribution import ProductDistributorFactory, distribute_raw_file
from .fileselector import CfhtFileSelector
from .sessionlink import setup_symlink
from .steps import CfhtStep

TRIGGER_VERSION = '0.6.002'


class CfhtHandler(ICustomHandler):
    def __init__(self, realtime: bool, trace: bool, steps: Collection[Step]):
        self.director_warnings = realtime
        self.updating_database = CfhtStep.DATABASE in steps and not trace
        self.distributing_raw = CfhtStep.DISTRAW in steps and not trace
        distributing_products = CfhtStep.DISTRIBUTE in steps and not trace
        self.distributor_factory = ProductDistributorFactory(realtime, distributing_products)
        self.database = QsoDatabase() if distributing_products or self.updating_database else None
        self.realtime = realtime

    def handle_recipe_failure(self, error: RecipeFailure):
        # TODO: update to handle new error passing format and recipe names
        ignore_modules = ('cal_CCF_E2DS_spirou',
                          'cal_CCF_E2DS_FP_spirou',
                          'obj_mk_tellu',
                          'obj_fit_tellu',
                          'pol_spirou')
        ignore_error = error.command_string.startswith(ignore_modules)
        ignore_error = True
        if self.director_warnings and not ignore_error:
            director_message(str(error), level='warning')

    def exposure_pre_process(self, exposure: Exposure):
        if self.distributing_raw:
            distribute_raw_file(exposure.raw)

    def exposure_preprocess_done(self, exposure: Exposure):
        if self.updating_database:
            self.__update_db_with_headers(exposure.odometer, exposure.preprocessed, preprocessed_only=True)

    def exposure_post_process(self, exposure: Exposure, result: Dict):
        config = SpirouExposureConfig.from_file(exposure.preprocessed)
        if config.object:
            if self.updating_database:
                self.__update_db_with_headers(exposure.odometer, result.get('extracted_path'), result.get('ccf_path'))
            if self.database:
                exposure_status = self.database.get_exposure(exposure.odometer)
            else:
                exposure_status = None
            distributor = self.distributor_factory.get_exposure_distributor(exposure_status)
            distributor.distribute_product(exposure, 'e')
            distributor.distribute_product(exposure, 's')
            if result.get('is_telluric_corrected'):
                distributor.distribute_product(exposure, 't')
            if result.get('is_ccf_calculated'):
                distributor.distribute_product(exposure, 'v')

    def sequence_post_process(self, sequence: Sequence[Exposure], result: Dict):
        config = SpirouExposureConfig.from_file(sequence[0].preprocessed)
        if config.object and config.object.instrument_mode.is_polarimetry():
            # TODO: this needs to use a list of obsids rather than range, order and gapless-ness not guaranteed
            if result.get('is_polar_done'):
                if self.database:
                    exposures_status = self.database.get_exposure_range(sequence[0].odometer, sequence[-1].odometer)
                else:
                    exposures_status = None
                distributor = self.distributor_factory.get_sequence_distributor(exposures_status)
                distributor.distribute_product(sequence[0], 'p')
        elif config.calibration:
            if self.database:
                for sequence in result.get('processed_sequences'):
                    for exposure in sequence:
                        pass  # TODO: update database to mark exposure as processed
            if self.realtime:
                calibrations_complete = result.get('calibrations_complete')
                # TODO: fire off calibrations done processing notices

    def __update_db_with_headers(self, odometer: int, path: Path, ccf_path: Path = None, preprocessed_only=False):
        if not self.updating_database:
            return
        db_headers = {'obsid': str(odometer)}
        try:
            if path:
                with fits.open(path) as hdu_list:
                    if preprocessed_only:
                        db_headers.update(DatabaseHeaderConverter.preprocessed_header_to_db(hdu_list[0].header))
                    else:
                        db_headers.update(DatabaseHeaderConverter.extracted_header_to_db(hdu_list[0].header))
            if ccf_path:
                with fits.open(ccf_path) as hdu_list:
                    db_headers.update(DatabaseHeaderConverter.ccf_header_to_db(hdu_list[1].header))
        except FileNotFoundError as err:
            log.warning('File not found during database update: %s', err.filename)
        self.database.send_pipeline_headers(db_headers, in_progress=preprocessed_only)


class CfhtDrsTrigger(DrsTrigger):
    @staticmethod
    def trigger_version() -> str:
        return TRIGGER_VERSION

    def get_file_selector(self) -> FileSelector:
        return CfhtFileSelector()

    def __init__(self, steps: Collection[Step], realtime=False, trace=False):
        handler = CfhtHandler(realtime, trace, steps)
        super().__init__(steps, trace, handler)

    def exposure_from_path(self, path: Path) -> Exposure:
        return setup_symlink(path)


class CfhtRealtimeTrigger(CfhtDrsTrigger):
    def __init__(self, steps: Collection[Step], trace=False):
        super().__init__(steps, realtime=True, trace=trace)

    def find_sequences(self, exposures: Iterable[Exposure]) -> Iterable[Sequence[Exposure]]:
        return super().find_sequences(exposures, ignore_incomplete_last=True)
