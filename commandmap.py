import pickle
from collections import defaultdict, OrderedDict

from astropy.io import fits

from drswrapper import DRS
from pathhandler import PathHandler
from dbinterface import send_headers_to_db

CACHE_FILE = '.drstrigger.cache'
FIBER_LIST = ('AB', 'A', 'B', 'C')

LAST_DARK_KEY = 'LAST_DARK'
LAST_FP_KEY = 'LAST_FP'
DARK_FLAT_QUEUE_KEY = 'DARK_FLAT_QUEUE'
FLAT_DARK_QUEUE_KEY = 'FLAT_DARK_QUEUE'
FLAT_QUEUE_KEY = 'FLAT_QUEUE'

class CommandMap:
    @staticmethod
    def preprocess_exposure(path):
        command = DRS.cal_preprocess
        return command(path)

    @staticmethod
    def process_exposure(config, path, realtime=False):
        command_map = ExposureCommandMap(realtime)
        command = command_map.get_command(config)
        return command(path)

    @staticmethod
    def process_sequence(config, paths, realtime=False):
        command_map = SequenceCommandMap(realtime)
        command = command_map.get_command(config)
        return command(paths)


class BaseCommandMap(object):
    def __init__(self, commands):
        self.__commands = commands
        self.__load_cache()

    def __save_cache(self):
        try:
            pickle.dump(self.__trigger_cache, open(CACHE_FILE, 'wb'))
        except (OSError, IOError) as e:
            print('Failed to serialize trigger cache, this will probably cause errors later on')

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

    def set_flat_queue(self, flats):
        self.__trigger_cache['FLAT_QUEUE'] = [flat.raw_filename() for flat in flats]
        self.__save_cache()

    def get_flat_queue(self, night):
        return [PathHandler(night, flat) for flat in self.__trigger_cache['FLAT_QUEUE']]

    def get_command(self, config):
        return self.__commands[config]


class ExposureCommandMap(BaseCommandMap):
    def __init__(self, realtime=False):
        commands = defaultdict(lambda: self.__unknown, {
            'DARK_DARK': self.__do_nothing,
            'DARK_FLAT': self.__do_nothing,
            'FLAT_DARK': self.__do_nothing,
            'FLAT_FLAT': self.__do_nothing,
            'FP_FP': self.__do_nothing,  # Need to wait to extract until we have slit
            'HCONE_HCONE': DRS.cal_extract_RAW,
            'DARK_HCONE': self.__do_nothing,
            'HCONE_DARK': self.__do_nothing,
            'SPEC_OBJ': self.__extract_and_apply_telluric_correction,
            'POL_OBJ': self.__extract_and_apply_telluric_correction,
            'SPEC_TELL': self.__extract_telluric_standard,
            'POL_TELL': self.__extract_telluric_standard,
            'OBJ_FP': self.__extract_object,
        })
        super().__init__(commands)
        self.realtime = realtime

    def __unknown(self, path):
        print("Unrecognized DPRTYPE, skipping", path.preprocessed_filename())
        return None

    def __do_nothing(self, path):
        return None

    def __extract_object(self, path):
        result = DRS.cal_extract_RAW(path)
        filepath = path.raw_path()
        s1d_files = OrderedDict((fiber, path.s1d_path(fiber)) for fiber in FIBER_LIST)
        e2ds_files = OrderedDict((fiber, path.e2ds_path(fiber)) for fiber in FIBER_LIST)
        combined_file_1d = path.final_product_path('s')
        combined_file_2d = path.final_product_path('e')
        create_mef(filepath, s1d_files, combined_file_1d)
        create_mef(filepath, e2ds_files, combined_file_2d)
        return result

    def __extract_and_apply_telluric_correction(self, path):
        self.__extract_object(path)
        (dpr_type, snr10, snr34, snr44) = get_product_headers(path.e2ds_path('AB'))
        obsid = path.raw_filename().replace('o.fits', '')
        if self.realtime:
            send_headers_to_db(obsid, dpr_type, snr10, snr34, snr44)
        # TODO - skip telluric correction on sky exposures
        try:
            DRS.obj_fit_tellu(path)
        except:
            DRS.cal_CCF_E2DS(path, telluric_corrected=False)
        else:
            DRS.cal_CCF_E2DS(path, telluric_corrected=True)

    def __extract_telluric_standard(self, path):
        self.__extract_object(path)
        DRS.obj_mk_tellu(path)


class SequenceCommandMap(BaseCommandMap):
    def __init__(self, realtime=False):
        commands = defaultdict(lambda: self.__unknown, {
            'DARK_DARK': self.__dark,
            'DARK_FLAT': self.__dark_flat,
            'FLAT_DARK': self.__flat_dark,
            'FLAT_FLAT': self.__flat,
            'FP_FP': self.__slit,
            'HCONE_HCONE': self.__wave,
            'POL_OBJ': DRS.pol,
            'POL_TELL': DRS.pol,
        })
        super().__init__(commands)

    def __unknown(self, paths):
        print("Unrecognized DPRTYPE, skipping", *[path.preprocessed_filename() for path in paths])
        return None

    def __dark(self, paths):
        result = DRS.cal_DARK(paths)
        last_dark = paths[-1]
        self.set_cached_file(last_dark, LAST_DARK_KEY)
        return result

    def __dark_flat(self, paths):
        self.set_cached_sequence(paths, DARK_FLAT_QUEUE_KEY)

    def __flat_dark(self, paths):
        self.set_cached_sequence(paths, FLAT_DARK_QUEUE_KEY)

    def __flat(self, paths):
        self.set_cached_sequence(paths, FLAT_QUEUE_KEY)
        # Generate bad pixel mask using last flat and last dark
        last_flat = paths[-1]
        night = last_flat.night()
        last_dark = self.get_cached_file(night, LAST_DARK_KEY)
        assert last_dark is not None, 'Need a known DARK file for cal_BADPIX'
        DRS.cal_BADPIX(last_flat, last_dark)
        # Process remaining loc queues
        dark_flats = self.get_cached_sequence(night, DARK_FLAT_QUEUE_KEY)
        if len(dark_flats) > 0:
            DRS.cal_loc_RAW(dark_flats)
        self.set_cached_sequence([], DARK_FLAT_QUEUE_KEY)
        flat_darks = self.get_cached_sequence(night, FLAT_DARK_QUEUE_KEY)
        if len(flat_darks) > 0:
            DRS.cal_loc_RAW(flat_darks)
        self.set_cached_sequence([], FLAT_DARK_QUEUE_KEY)

    def __process_cached_flat_queue(self, night):
        flat_paths = self.get_cached_sequence(night, FLAT_QUEUE_KEY)
        result = DRS.cal_FF_RAW(flat_paths)
        self.set_cached_sequence([], FLAT_QUEUE_KEY)
        return result

    def __slit(self, paths):
        result = DRS.cal_SLIT(paths)
        self.__process_cached_flat_queue(paths[0].night())  # Can finally flat field once we have the tilt
        last_fp = paths[-1]
        DRS.cal_extract_RAW(last_fp)  # TODO determine if all fp files should be extracted together as a single image
        self.set_cached_file(last_fp, LAST_FP_KEY)
        return result

    def __wave(self, paths):
        assert len(paths) == 1, 'Too many HCONE_HCONE files'
        hc_path = paths[0]
        fp_path = self.get_cached_file(hc_path.night(), LAST_FP_KEY)
        assert fp_path is not None, 'Need an extracted FP file for cal_WAVE'
        for fiber in FIBER_LIST:
            DRS.cal_WAVE_E2DS(fp_path, hc_path, fiber)


def create_mef(primary_header_file, extension_files, output_file):
    print('Creating MEF', output_file)
    primary = fits.open(primary_header_file)[0]
    mef = fits.HDUList(primary)
    for ext_name, ext_file in extension_files.items():
        ext = fits.open(ext_file)[0]
        ext.header.insert(0, ('EXTNAME', ext_name))
        mef.append(ext)
    mef.writeto(output_file, overwrite=True)

def get_product_headers(input_file):
    header = fits.open(input_file)[0].header
    return (header['DPRTYPE'], header['SNR10'], header['SNR34'], header['SNR44'])
