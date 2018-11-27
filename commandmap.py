import pickle
from collections import defaultdict, OrderedDict

from astropy.io import fits

from logger import logger
from drswrapper import DRS
from pathhandler import PathHandler
from dbinterface import send_headers_to_db

CACHE_FILE = '.drstrigger.cache'
FIBER_LIST = ('AB', 'A', 'B', 'C')

LAST_DARK_KEY = 'LAST_DARK'
FP_QUEUE_KEY = 'FP_QUEUE'
DARK_FLAT_QUEUE_KEY = 'DARK_FLAT_QUEUE'
FLAT_DARK_QUEUE_KEY = 'FLAT_DARK_QUEUE'
FLAT_QUEUE_KEY = 'FLAT_QUEUE'

class CommandMap:
    def __init__(self, steps, trace, realtime):
        self.steps = steps
        self.trace = trace
        self.realtime = realtime

    def preprocess_exposure(self, path):
        if self.steps['preprocess']:
            command = DRS(self.trace, self.realtime).cal_preprocess
            return command(path)

    def process_exposure(self, config, path, ccf_mask=None):
        command_map = ExposureCommandMap(self.steps, self.trace, self.realtime, ccf_mask)
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

    def set_cached_file(self, path, key):
        self.__trigger_cache[key] = path.raw_filename()
        self.__save_cache()

    def get_cached_file(self, night, key):
        return PathHandler(night, self.__trigger_cache[key])

    def set_cached_sequence(self, paths, key):
        self.__trigger_cache[key] = [path.raw_filename() for path in paths]
        self.__save_cache()

    def get_cached_sequence(self, night, key):
        return [PathHandler(night, path) for path in self.__trigger_cache[key]]

    def get_command(self, config):
        return self.__commands[config]


class ExposureCommandMap(BaseCommandMap):
    def __init__(self, steps, trace, realtime=False, ccf_mask=None):
        commands = defaultdict(lambda: self.__unknown, {
            'DARK_DARK': self.__do_nothing,
            'DARK_FLAT': self.__do_nothing,
            'FLAT_DARK': self.__do_nothing,
            'FLAT_FLAT': self.__do_nothing,
            'FP_FP': self.__do_nothing,
            'HCONE_HCONE': self.__do_nothing,
            'DARK_HCONE': self.__do_nothing,
            'HCONE_DARK': self.__do_nothing,
            'SPEC_OBJ': self.__extract_and_apply_telluric_correction,
            'POL_OBJ': self.__extract_and_apply_telluric_correction,
            'SPEC_TELL': self.__extract_telluric_standard,
            'POL_TELL': self.__extract_telluric_standard,
            'OBJ_FP': self.__extract_object,
        })
        super().__init__(commands, steps, trace, realtime)
        self.ccf_mask = ccf_mask
        if self.ccf_mask is None:
            self.ccf_mask = 'gl581_Sep18_cleaned.mas'

    def __unknown(self, path):
        logger.warning('Unrecognized DPRTYPE, skipping exposure %s', path.preprocessed_filename())
        return None

    def __do_nothing(self, path):
        return None

    def __extract_object(self, path):
        result = self.drs.cal_extract_RAW(path)
        if not self.trace:
            create_final_product(path)
        return result

    def __extract_and_apply_telluric_correction(self, path):
        if self.steps['objects']:
            self.__extract_object(path)
            # TODO - skip telluric correction on sky exposures
            telluric_corrected = False
            try:
                if self.steps['fittellu']:
                    self.drs.obj_fit_tellu(path)
                    telluric_corrected = True
            finally:
                if self.steps['ccf']:
                    self.drs.cal_CCF_E2DS(path, self.ccf_mask, telluric_corrected=telluric_corrected)
            if self.realtime:
                db_headers = {'obsid': path.raw_filename().replace('o.fits', '')}
                db_headers.update(get_product_headers(path.e2ds_path('AB')))
                db_headers.update(get_ccf_headers(path.ccf_path('AB', self.ccf_mask, telluric_corrected)))
                send_headers_to_db(db_headers)

    def __extract_telluric_standard(self, path):
        if self.steps['objects']:
            self.__extract_object(path)
            if self.steps['mktellu']:
                self.drs.obj_mk_tellu(path)


class SequenceCommandMap(BaseCommandMap):
    def __init__(self, steps, trace, realtime=False):
        commands = defaultdict(lambda: self.__unknown, {
            'DARK_DARK': self.__dark,
            'DARK_FLAT': self.__dark_flat,
            'FLAT_DARK': self.__flat_dark,
            'FLAT_FLAT': self.__flat,
            'FP_FP': self.__fabry_perot,
            'HCONE_HCONE': self.__hc_one,
            'POL_OBJ': self.__polar,
            'POL_TELL': self.__polar,
        })
        super().__init__(commands, steps, trace, realtime)

    def __unknown(self, paths):
        logger.warning('Unrecognized DPRTYPE, skipping sequence %s',
                       ' '.join([path.preprocessed_filename() for path in paths]))
        return None

    def __dark(self, paths):
        if self.steps['calibrations']:
            result = self.drs.cal_DARK(paths)
            last_dark = paths[-1]
            self.set_cached_file(last_dark, LAST_DARK_KEY)
            return result

    def __dark_flat(self, paths):
        if self.steps['calibrations']:
            self.set_cached_sequence(paths, DARK_FLAT_QUEUE_KEY)

    def __flat_dark(self, paths):
        if self.steps['calibrations']:
            self.set_cached_sequence(paths, FLAT_DARK_QUEUE_KEY)

    def __flat(self, paths):
        if self.steps['calibrations']:
            self.set_cached_sequence(paths, FLAT_QUEUE_KEY)
            # Generate bad pixel mask using last flat and last dark
            last_flat = paths[-1]
            night = last_flat.night()
            last_dark = self.get_cached_file(night, LAST_DARK_KEY)
            assert last_dark is not None, 'Need a known DARK file for cal_BADPIX'
            self.drs.cal_BADPIX(last_flat, last_dark)
            # Process remaining loc queues
            self.__process_cached_loc_queue(night, DARK_FLAT_QUEUE_KEY)
            self.__process_cached_loc_queue(night, FLAT_DARK_QUEUE_KEY)

    def __process_cached_loc_queue(self, night, cache_key):
        paths = self.get_cached_sequence(night, cache_key)
        if paths:
            self.drs.cal_loc_RAW(paths)
        self.set_cached_sequence([], cache_key)
        return paths

    def __process_cached_flat_queue(self, night):
        if self.steps['calibrations']:
            flat_paths = self.get_cached_sequence(night, FLAT_QUEUE_KEY)
            if flat_paths:
                self.drs.cal_FF_RAW(flat_paths)
                self.set_cached_sequence([], FLAT_QUEUE_KEY)
                return flat_paths

    def __fabry_perot(self, paths):
        if self.steps['calibrations']:
            self.set_cached_sequence(paths, FP_QUEUE_KEY)

    def __process_cached_fp_queue(self, hc_path):
        if self.steps['calibrations']:
            fp_paths = self.get_cached_sequence(hc_path.night(), FP_QUEUE_KEY)
            self.drs.cal_SLIT(fp_paths)
            self.drs.cal_SHAPE(hc_path, fp_paths)
            self.__process_cached_flat_queue(fp_paths[0].night())  # Can finally flat field once we have the tilt
            last_fp = fp_paths[-1]
            self.drs.cal_extract_RAW(last_fp)  # TODO should all fp files be extracted together as a single image?
            self.set_cached_sequence([], FP_QUEUE_KEY)
            return fp_paths

    def __hc_one(self, paths):
        if self.steps['calibrations']:
            assert len(paths) == 1, 'Too many HCONE_HCONE files'
            hc_path = paths[0]
            fp_paths = self.__process_cached_fp_queue(hc_path)
            last_fp = fp_paths[-1]
            self.drs.cal_extract_RAW(hc_path)
            self.__wave(hc_path, last_fp)

    def __wave(self, hc_path, fp_path):
        if self.steps['calibrations']:
            assert fp_path is not None, 'Need an extracted FP file for cal_WAVE'
            for fiber in FIBER_LIST:
                pass
                # Wavelength recipes currently not working all that well
                #self.drs.cal_HC_E2DS(hc_path, fiber)
                #self.drs.cal_WAVE_E2DS(fp_path, hc_path, fiber)

    def __polar(self, paths):
        if self.steps['pol']:
            return self.drs.pol(paths)


def create_final_product(path):
    filepath = path.raw_path()
    s1d_files = OrderedDict((fiber, path.s1d_path(fiber)) for fiber in FIBER_LIST)
    e2ds_files = OrderedDict((fiber, path.e2ds_path(fiber)) for fiber in FIBER_LIST)
    combined_file_1d = path.final_product_path('s')
    combined_file_2d = path.final_product_path('e')
    create_mef(filepath, s1d_files, combined_file_1d)
    create_mef(filepath, e2ds_files, combined_file_2d)


def create_mef(primary_header_file, extension_files, output_file):
    logger.info('Creating MEF %s', output_file)
    primary = fits.open(primary_header_file)[0]
    mef = fits.HDUList(primary)
    for ext_name, ext_file in extension_files.items():
        ext = fits.open(ext_file)[0]
        ext.header.insert(0, ('EXTNAME', ext_name))
        mef.append(ext)
    mef.writeto(output_file, overwrite=True)


def get_product_headers(input_file):
    header = fits.open(input_file)[0].header
    return {
        'dprtype': header['DPRTYPE'],
        'snr10': header['SNR10'],
        'snr34': header['SNR34'],
        'snr44': header['SNR44']
    }


def get_ccf_headers(input_file):
    header = fits.open(input_file)[0].header
    return {
        'ccfmask': header['CCFMASK'],
        'ccfmacpp': header['CCFMACPP'],
        'ccfcontr': header['CCFCONTR'],
        'ccfrv': header['CCFRV'],
        'ccfrvc': header['CCFRVC'],
        'ccffwhm': header['CCFFWHM']
    }
