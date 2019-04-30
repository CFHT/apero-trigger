import pickle
from collections import defaultdict, namedtuple

from astropy.io import fits

from dbinterface import QsoDatabase, DatabaseHeaderConverter
from drswrapper import DRS, FIBER_LIST, CcfParams
from logger import logger
from pathhandler import Exposure
from productbundler import ProductBundler

CACHE_FILE = '.drstrigger.cache'

LAST_DARK_KEY = 'LAST_DARK'
FP_QUEUE_KEY = 'FP_QUEUE'
DARK_FLAT_QUEUE_KEY = 'DARK_FLAT_QUEUE'
FLAT_DARK_QUEUE_KEY = 'FLAT_DARK_QUEUE'
FLAT_QUEUE_KEY = 'FLAT_QUEUE'


class Steps(namedtuple('Steps', ('preprocess', 'calibrations', 'objects'))):
    @classmethod
    def all(cls):
        return cls(True, True, ObjectSteps.all())

    @classmethod
    def from_keys(cls, keys):
        return cls('preprocess' in keys, 'calibrations' in keys, ObjectSteps.from_keys(keys))


class ObjectSteps(namedtuple('ObjectSteps', ('extract', 'pol', 'mktellu', 'fittellu', 'ccf',
                                             'products', 'distribute', 'database'))):
    @classmethod
    def all(cls):
        return cls(True, True, True, True, True, True, True, True)

    @classmethod
    def from_keys(cls, keys):
        temp_dict = {field: field in keys for field in cls._fields}
        return cls(**temp_dict)


class CommandMap:
    def __init__(self, steps, trace, realtime):
        self.steps = steps
        self.trace = trace
        self.realtime = realtime

    def preprocess_exposure(self, path):
        command_map = PreprocessCommandMap(self.steps, self.trace, self.realtime)
        command = command_map.get_command(None)
        return command(path)

    def process_exposure(self, config, path, ccf_params=None):
        command_map = ExposureCommandMap(self.steps, self.trace, self.realtime, ccf_params)
        command = command_map.get_command(config)
        return command(path)

    def process_sequence(self, config, paths):
        command_map = SequenceCommandMap(self.steps, self.trace, self.realtime)
        command = command_map.get_command(config)
        return command(paths)


class BaseCommandMap(object):
    def __init__(self, commands, steps, trace, realtime):
        self.__commands = commands
        self.__load_cache()
        self.steps = steps
        self.trace = trace
        self.realtime = realtime
        self.drs = DRS(trace, realtime)
        self.bundler = ProductBundler(trace, realtime, steps.objects.products, steps.objects.distribute)
        self.database = QsoDatabase() if (self.steps.objects.products or self.steps.objects.database) else None

    def __save_cache(self):
        try:
            pickle.dump(self.__trigger_cache, open(CACHE_FILE, 'wb'))
        except (OSError, IOError) as e:
            logger.error('Failed to serialize trigger cache, this will probably cause errors later on')

    def __load_cache(self):
        try:
            self.__trigger_cache = pickle.load(open(CACHE_FILE, 'rb'))
        except (OSError, IOError):
            self.__trigger_cache = {}

    def set_cached_exposure(self, exposure, key):
        self.__trigger_cache[key] = exposure.raw.name
        self.__save_cache()

    def get_cached_exposure(self, night, key):
        return Exposure(night, self.__trigger_cache[key])

    def set_cached_sequence(self, exposures, key):
        self.__trigger_cache[key] = [exposure.raw.name for exposure in exposures]
        self.__save_cache()

    def get_cached_sequence(self, night, key):
        return [Exposure(night, filename) for filename in self.__trigger_cache[key]]

    def get_command(self, config):
        return self.__commands[config]


class PreprocessCommandMap(BaseCommandMap):
    def __init__(self, steps, trace, realtime=False):
        commands = defaultdict(lambda: self.__preprocess, {})
        super().__init__(commands, steps, trace, realtime)

    def __preprocess(self, exposure):
        if self.steps.preprocess:
            return self.drs.cal_preprocess(exposure)
        else:
            return exposure.preprocessed.exists()


class ExposureCommandMap(BaseCommandMap):
    def __init__(self, steps, trace, realtime=False, ccf_params=None):
        commands = defaultdict(lambda: self.__do_nothing, {
            'SPEC_DARK': self.__extract_normal_obj_dark,
            'POL_DARK': self.__extract_normal_obj_dark,
            'SPEC_FP': self.__extract_normal_obj_fp,
            'POL_FP': self.__extract_normal_obj_fp,
            'SPECTELL_DARK': self.__extract_telluric_standard,
            'POLTELL_DARK': self.__extract_telluric_standard,
            'SPECTELL_FP': self.__extract_telluric_standard,
            'POLTELL_FP': self.__extract_telluric_standard,
            'SPECSKY_DARK': self.__extract_sky,
            'POLSKY_DARK': self.__extract_sky,
            'SPECSKY_FP': self.__extract_sky,
            'POLSKY_FP': self.__extract_sky,
        })
        super().__init__(commands, steps, trace, realtime)
        self.ccf_params = ccf_params
        if self.ccf_params is None:
            self.ccf_params = CcfParams('masque_sept18_andres_trans50.mas', 0, 200, 1)

    def __unknown(self, exposure):
        logger.warning('Unrecognized DPRTYPE, skipping exposure %s', exposure.preprocessed.name)
        return None

    def __do_nothing(self, exposure):
        return None

    def __extract_object(self, exposure):
        if self.steps.objects.extract:
            self.drs.cal_extract_RAW(exposure)
        if self.steps.objects.products:
            self.bundler.set_exposure_status(self.database.get_exposure(exposure.odometer))
        self.bundler.create_spec_product(exposure)

    def __telluric_correction(self, exposure):
        if self.steps.objects.fittellu:
            try:
                result = self.drs.obj_fit_tellu(exposure)
                telluric_corrected = result is not None
            except:
                telluric_corrected = False
        else:
            expected_telluric_path = exposure.e2ds('AB', telluric_corrected=True, flat_fielded=True)
            telluric_corrected = expected_telluric_path.exists()
        self.bundler.create_tell_product(exposure)
        return telluric_corrected

    def __ccf(self, exposure, telluric_corrected, fp):
        if self.steps.objects.ccf:
            self.drs.cal_CCF_E2DS(exposure, self.ccf_params, telluric_corrected=telluric_corrected, fp=fp)
        self.bundler.create_ccf_product(exposure, self.ccf_params.mask, telluric_corrected=telluric_corrected, fp=fp)

    def __extract_normal_obj(self, exposure, fp):
        self.__extract_object(exposure)
        telluric_corrected = self.__telluric_correction(exposure)
        self.__ccf(exposure, telluric_corrected, fp=fp)
        if self.realtime or self.steps.objects.database:
            ccf_path = exposure.ccf('AB', self.ccf_params.mask, telluric_corrected=telluric_corrected, fp=fp)
            self.__update_db_with_headers(exposure, ccf_path)

    def __extract_normal_obj_dark(self, exposure):
        self.__extract_normal_obj(exposure, fp=False)

    def __extract_normal_obj_fp(self, exposure):
        self.__extract_normal_obj(exposure, fp=True)

    def __extract_telluric_standard(self, exposure):
        self.__extract_object(exposure)
        if self.steps.objects.mktellu:
            pass # need to update this to work with new creation model
            # self.drs.obj_mk_tellu(exposure)
        if self.realtime or self.steps.objects.database:
            self.__update_db_with_headers(exposure)

    def __extract_sky(self, exposure):
        self.__extract_object(exposure)
        if self.realtime or self.steps.objects.database:
            self.__update_db_with_headers(exposure)

    def __update_db_with_headers(self, exposure, ccf_path=None):
        db_headers = {'obsid': str(exposure.odometer)}
        with fits.open(exposure.e2ds('AB')) as hdu_list:
            db_headers.update(DatabaseHeaderConverter.extracted_header_to_db(hdu_list[0].header))
        if ccf_path:
            with fits.open(ccf_path) as hdu_list:
                db_headers.update(DatabaseHeaderConverter.ccf_header_to_db(hdu_list[0].header))
        self.database.send_pipeline_headers(db_headers)


class SequenceCommandMap(BaseCommandMap):
    def __init__(self, steps, trace, realtime=False):
        commands = defaultdict(lambda: self.__unknown, {
            'DARK_DARK': self.__dark,
            'DARK_FLAT': self.__dark_flat,
            'FLAT_DARK': self.__flat_dark,
            'FLAT_FLAT': self.__flat,
            'FP_FP': self.__fabry_perot,
            'HCONE_HCONE': self.__hc_one,
            'SPEC_DARK': self.__do_nothing,
            'SPEC_FP': self.__do_nothing,
            'SPECTELL_DARK': self.__do_nothing,
            'SPECTELL_FP': self.__do_nothing,
            'SPECSKY_DARK': self.__do_nothing,
            'SPECSKY_FP': self.__do_nothing,
            'POL_DARK': self.__polar,
            'POL_FP': self.__polar,
            'POLTELL_DARK': self.__polar,
            'POLTELL_FP': self.__polar,
            'POLSKY_DARK': self.__polar,
            'POLSKY_FP': self.__polar,
        })
        super().__init__(commands, steps, trace, realtime)

    def __unknown(self, exposures):
        logger.warning('Unrecognized DPRTYPE, skipping sequence %s',
                       ' '.join([exposure.preprocessed.name for exposure in exposures]))
        return None

    def __do_nothing(self, path):
        return None

    def __dark(self, exposures):
        if self.steps.calibrations:
            result = self.drs.cal_DARK(exposures)
            last_dark = exposures[-1]
            self.set_cached_exposure(last_dark, LAST_DARK_KEY)
            return result

    def __dark_flat(self, exposures):
        if self.steps.calibrations:
            self.set_cached_sequence(exposures, DARK_FLAT_QUEUE_KEY)

    def __flat_dark(self, exposures):
        if self.steps.calibrations:
            self.set_cached_sequence(exposures, FLAT_DARK_QUEUE_KEY)

    def __flat(self, exposures):
        if self.steps.calibrations:
            self.set_cached_sequence(exposures, FLAT_QUEUE_KEY)
            # Generate bad pixel mask using last flat and last dark
            last_flat = exposures[-1]
            night = last_flat.night
            last_dark = self.get_cached_exposure(night, LAST_DARK_KEY)
            assert last_dark is not None, 'Need a known DARK file for cal_BADPIX'
            self.drs.cal_BADPIX(last_flat, last_dark)
            # Process remaining loc queues
            self.__process_cached_loc_queue(night, DARK_FLAT_QUEUE_KEY)
            self.__process_cached_loc_queue(night, FLAT_DARK_QUEUE_KEY)

    def __process_cached_loc_queue(self, night, cache_key):
        exposures = self.get_cached_sequence(night, cache_key)
        if exposures:
            self.drs.cal_loc_RAW(exposures)
        self.set_cached_sequence([], cache_key)
        return exposures

    def __process_cached_flat_queue(self, night):
        if self.steps.calibrations:
            flat_exposures = self.get_cached_sequence(night, FLAT_QUEUE_KEY)
            if flat_exposures:
                self.drs.cal_FF_RAW(flat_exposures)
                self.set_cached_sequence([], FLAT_QUEUE_KEY)
                return flat_exposures

    def __fabry_perot(self, exposures):
        if self.steps.calibrations:
            self.set_cached_sequence(exposures, FP_QUEUE_KEY)

    def __process_cached_fp_queue(self, hc_exposure):
        if self.steps.calibrations:
            fp_exposures = self.get_cached_sequence(hc_exposure.night, FP_QUEUE_KEY)
            if fp_exposures:
                self.drs.cal_SLIT(fp_exposures)
                self.drs.cal_SHAPE(hc_exposure, fp_exposures)
                self.__process_cached_flat_queue(fp_exposures[0].night)  # Can finally flat field once we have the tilt
                # for fp_exposure in fp_exposures:
                #     self.drs.cal_extract_RAW(fp_exposure)
                self.drs.cal_extract_RAW(fp_exposures[-1])
                self.set_cached_sequence([], FP_QUEUE_KEY)
            return fp_exposures

    def __hc_one(self, exposures):
        if self.steps.calibrations:
            last_hc = exposures[-1]
            fp_exposures = self.__process_cached_fp_queue(last_hc)
            if fp_exposures:
                last_fp = fp_exposures[-1]
                self.drs.cal_extract_RAW(last_hc)
                self.__wave(last_hc, last_fp)

    def __wave(self, hc_exposure, fp_exposure):
        if self.steps.calibrations:
            assert fp_exposure is not None, 'Need an extracted FP file for cal_WAVE'
            for fiber in FIBER_LIST:
                self.drs.cal_HC_E2DS(hc_exposure, fiber)
                self.drs.cal_WAVE_E2DS(fp_exposure, hc_exposure, fiber)

    def __polar(self, exposures):
        if len(exposures) < 2:
            return
        if self.steps.objects.pol:
            self.drs.pol(exposures)
        if self.steps.objects.products:
            exposures_status = self.database.get_exposure_range(exposures[0].odometer, exposures[-1].odometer)
            self.bundler.set_sequence_status(exposures_status)
        self.bundler.create_pol_product(exposures[0])
