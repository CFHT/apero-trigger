from astropy.io import fits

from trigger import AbstractCustomHandler, DrsTrigger, ExposureConfig, Steps
from .dbinterface import QsoDatabase, DatabaseHeaderConverter
from .director import director_message
from .distribution import ProductDistributorFactory


class CfhtHandler(AbstractCustomHandler):
    def __init__(self, realtime, trace, distributing, updating_database):
        self.director_warnings = realtime
        self.updating_database = updating_database
        self.distributor_factory = ProductDistributorFactory(trace, realtime, distributing)
        self.database = QsoDatabase() if distributing or updating_database else None

    def handle_recipe_failure(self, exposure_or_sequence, error):
        ignore_modules = ('cal_CCF_E2DS_spirou',
                          'cal_CCF_E2DS_FP_spirou',
                          'obj_mk_tellu',
                          'obj_fit_tellu',
                          'pol_spirou')
        if self.director_warnings and not error.startswith(ignore_modules):
            director_message(str(error), level='warning')

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

    def __update_db_with_headers(self, exposure, exposure_path=None, ccf_path=None):
        if not self.updating_database:
            return
        db_headers = {'obsid': str(exposure.odometer)}
        if exposure_path:
            with fits.open(exposure_path) as hdu_list:
                db_headers.update(DatabaseHeaderConverter.extracted_header_to_db(hdu_list[0].header))
        if ccf_path:
            with fits.open(ccf_path) as hdu_list:
                db_headers.update(DatabaseHeaderConverter.ccf_header_to_db(hdu_list[0].header))
        self.database.send_pipeline_headers(db_headers)


class CfhtDrsTrigger(DrsTrigger):
    def __init__(self, steps, realtime=False, trace=False, ccf_params=None):
        super().__init__(steps, realtime, trace, ccf_params)
        handler = CfhtHandler(realtime, trace, self.steps.objects.distribute, self.steps.objects.database)
        self.set_custom_handler(handler)


class CfhtRealtimeTrigger(CfhtDrsTrigger):
    def __init__(self):
        super().__init__(Steps.all(), realtime=True)
