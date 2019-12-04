from astropy.io import fits

from trigger import AbstractCustomHandler, DrsTrigger, ExposureConfig, DrsSteps, log
from .dbinterface import QsoDatabase, DatabaseHeaderConverter
from .director import director_message
from .distribution import ProductDistributorFactory, distribute_raw_file
from .fileselector import CfhtFileSelector
from .steps import CfhtDrsSteps

TRIGGER_VERSION = '025'


class CfhtHandler(AbstractCustomHandler):
    def __init__(self, realtime, trace, distributing_raw, distributing_products, updating_database):
        self.director_warnings = realtime
        self.updating_database = updating_database and not trace
        self.distributing_raw = distributing_raw
        self.distributor_factory = ProductDistributorFactory(trace, realtime, distributing_products)
        self.database = QsoDatabase() if distributing_products or updating_database else None
        self.realtime = realtime

    def handle_recipe_failure(self, error):
        ignore_modules = ('cal_CCF_E2DS_spirou',
                          'cal_CCF_E2DS_FP_spirou',
                          'obj_mk_tellu',
                          'obj_fit_tellu',
                          'pol_spirou')
        if self.director_warnings and not error.command_string.startswith(ignore_modules):
            director_message(str(error), level='warning')

    def exposure_pre_process(self, exposure):
        if self.distributing_raw:
            distribute_raw_file(exposure.raw)

    def exposure_post_process(self, exposure, result):
        config = ExposureConfig.from_file(exposure.preprocessed)
        if config.object:
            if self.updating_database:
                self.__update_db_with_headers(exposure, result.get('extracted_path'), result.get('ccf_path'))
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

    def sequence_post_process(self, sequence, result):
        config = ExposureConfig.from_file(sequence[0].preprocessed)
        if config.object and config.object.instrument_mode.is_polarimetry():
            # TODO: this needs to use a list of obsids rather than range, order and gapless-ness not guaranteed
            if result.get('is_polar_done'):
                if self.database:
                    exposures_status = self.database.get_exposure_range(sequence[0].odometer, sequence[-1].odometer)
                else:
                    exposures_status = None
                distributor = self.distributor_factory.get_seqeunce_distributor(exposures_status)
                distributor.distribute_product(sequence[0], 'p')
        elif config.calibration:
            if self.database:
                for sequence in result.get('processed_sequences'):
                    for exposure in sequence:
                        pass # TODO: update database to mark exposure as processed
            if self.realtime:
                calibrations_complete = result.get('calibrations_complete')
                # TODO: fire off calibrations done processing notices

    def __update_db_with_headers(self, exposure, exposure_path=None, ccf_path=None):
        if not self.updating_database:
            return
        db_headers = {'obsid': str(exposure.odometer)}
        try:
            if exposure_path:
                with fits.open(exposure_path) as hdu_list:
                    db_headers.update(DatabaseHeaderConverter.extracted_header_to_db(hdu_list[0].header))
            if ccf_path:
                with fits.open(ccf_path) as hdu_list:
                    db_headers.update(DatabaseHeaderConverter.ccf_header_to_db(hdu_list[0].header))
        except FileNotFoundError as err:
            log.warning('File not found during database update: %s', err.filename)
        self.database.send_pipeline_headers(db_headers)


class CfhtDrsTrigger(DrsTrigger):
    @staticmethod
    def trigger_version():
        return TRIGGER_VERSION

    def get_file_selector(self):
        return CfhtFileSelector()

    def __init__(self, steps, ccf_params, realtime=False, trace=False):
        handler = CfhtHandler(realtime, trace, steps.distraw, steps.distribute, steps.database)
        super().__init__(steps, ccf_params, trace, handler)


class CfhtRealtimeTrigger(CfhtDrsTrigger):
    def __init__(self, ccf_params):
        super().__init__(CfhtDrsSteps.all(), ccf_params, realtime=True)

    def find_sequences(self, night, files):
        return super().find_sequences(night, files, ignore_incomplete_last=True)


class CfhtRealtimeTester(CfhtDrsTrigger):
    def __init__(self, ccf_params):
        steps = CfhtDrsSteps(*DrsSteps.all(), distraw=False, distribute=False, database=False)
        super().__init__(steps, ccf_params, realtime=True)
