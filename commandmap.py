import os
import pathlib
import pickle
import shutil
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
        self.bundler = ProductBundler(trace, realtime, steps['products'], steps['distribute'])

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
        self.__trigger_cache[key] = path.raw.filename
        self.__save_cache()

    def get_cached_file(self, night, key):
        return PathHandler(night, self.__trigger_cache[key])

    def set_cached_sequence(self, paths, key):
        self.__trigger_cache[key] = [path.raw.filename for path in paths]
        self.__save_cache()

    def get_cached_sequence(self, night, key):
        return [PathHandler(night, path) for path in self.__trigger_cache[key]]

    def get_command(self, config):
        return self.__commands[config]


class ExposureCommandMap(BaseCommandMap):
    def __init__(self, steps, trace, realtime=False, ccf_mask=None):
        commands = defaultdict(lambda: self.__do_nothing, {
            'SPEC_OBJ': self.__extract_and_apply_telluric_correction,
            'POL_OBJ': self.__extract_and_apply_telluric_correction,
            'SPEC_TELL': self.__extract_telluric_standard,
            'POL_TELL': self.__extract_telluric_standard,
        })
        super().__init__(commands, steps, trace, realtime)
        self.ccf_mask = ccf_mask
        if self.ccf_mask is None:
            self.ccf_mask = 'gl581_Sep18_cleaned.mas'

    def __unknown(self, path):
        logger.warning('Unrecognized DPRTYPE, skipping exposure %s', path.preprocessed.filename)
        return None

    def __do_nothing(self, path):
        return None

    def __extract_object(self, path):
        if self.steps['objects']:
            self.drs.cal_extract_RAW(path)
        self.bundler.create_spec_product(path)

    def __extract_and_apply_telluric_correction(self, path):
        self.__extract_object(path)
        # TODO - skip telluric correction on sky exposures
        telluric_corrected = False
        try:
            if self.steps['fittellu']:
                temp = self.drs.obj_fit_tellu(path)
                if temp is not None:
                    telluric_corrected = True
            else:
                telluric_corrected = True
            self.bundler.create_tell_product(path)
        finally:
            if self.steps['ccf']:
                self.drs.cal_CCF_E2DS(path, self.ccf_mask, telluric_corrected=telluric_corrected)
            self.bundler.create_ccf_product(path, self.ccf_mask, telluric_corrected=telluric_corrected)
        if self.realtime:
            ccf_path = path.ccf('AB', self.ccf_mask, telluric_corrected=telluric_corrected).fullpath
            self.__update_db_with_headers(path, ccf_path)

    def __extract_telluric_standard(self, path):
        self.__extract_object(path)
        if self.steps['mktellu']:
            self.drs.obj_mk_tellu(path)
        if self.realtime:
            self.__update_db_with_headers(path)

    def __update_db_with_headers(self, path, ccf_fullpath=None):
        db_headers = {'obsid': path.raw.filename.replace('o.fits', '')}
        db_headers.update(get_product_headers(path.e2ds('AB').fullpath))
        if ccf_fullpath:
            db_headers.update(get_ccf_headers(ccf_fullpath))
        send_headers_to_db(db_headers)

class SequenceCommandMap(BaseCommandMap):
    def __init__(self, steps, trace, realtime=False):
        commands = defaultdict(lambda: self.__unknown, {
            'DARK_DARK': self.__dark,
            'DARK_FLAT': self.__dark_flat,
            'FLAT_DARK': self.__flat_dark,
            'FLAT_FLAT': self.__flat,
            'FP_FP': self.__fabry_perot,
            'HCONE_HCONE': self.__hc_one,
            'SPEC_OBJ': self.__do_nothing,
            'SPEC_TELL': self.__do_nothing,
            'POL_OBJ': self.__polar,
            'POL_TELL': self.__polar,
        })
        super().__init__(commands, steps, trace, realtime)

    def __unknown(self, paths):
        logger.warning('Unrecognized DPRTYPE, skipping sequence %s',
                       ' '.join([path.preprocessed.filename for path in paths]))
        return None

    def __do_nothing(self, path):
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
            night = last_flat.night
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
            fp_paths = self.get_cached_sequence(hc_path.night, FP_QUEUE_KEY)
            if fp_paths:
                self.drs.cal_SLIT(fp_paths)
                self.drs.cal_SHAPE(hc_path, fp_paths)
                self.__process_cached_flat_queue(fp_paths[0].night)  # Can finally flat field once we have the tilt
                for fp_path in fp_paths:
                    self.drs.cal_extract_RAW(fp_path)
                self.set_cached_sequence([], FP_QUEUE_KEY)
            return fp_paths

    def __hc_one(self, paths):
        if self.steps['calibrations']:
            hc_path = paths[0]
            fp_paths = self.__process_cached_fp_queue(hc_path)
            if fp_paths:
                last_fp = fp_paths[-1]
                self.drs.cal_extract_RAW(hc_path)
                self.__wave(hc_path, last_fp)

    def __wave(self, hc_path, fp_path):
        if self.steps['calibrations']:
            assert fp_path is not None, 'Need an extracted FP file for cal_WAVE'
            for fiber in FIBER_LIST:
                self.drs.cal_HC_E2DS(hc_path, fiber)
                self.drs.cal_WAVE_E2DS(fp_path, hc_path, fiber)

    def __polar(self, paths):
        if self.steps['pol']:
            self.drs.pol(paths)
        self.bundler.create_pol_product(paths[0])


class ProductBundler:
    def __init__(self, trace, realtime, create, distribute):
        self.trace = trace
        self.realtime = realtime
        self.create = create or realtime
        self.distribute = distribute or realtime

    def create_spec_product(self, path):
        filepath = path.raw.fullpath
        s1d_files = OrderedDict((fiber, path.s1d(fiber).fullpath) for fiber in FIBER_LIST)
        e2ds_files = OrderedDict((fiber, path.e2ds(fiber, flat_fielded=True).fullpath) for fiber in FIBER_LIST)
        product_1d = path.final_product('s').fullpath
        product_2d = path.final_product('e').fullpath
        self.create_mef(filepath, s1d_files, product_1d)
        self.create_mef(filepath, e2ds_files, product_2d)
        self.distribute_file(product_1d)
        self.distribute_file(product_2d)

    def create_pol_product(self, path):
        filepath = path.raw.fullpath
        input_files = OrderedDict((
            ('StokesI', path.reduced('e2ds_AB_StokesI').fullpath),
            ('pol', path.reduced('e2ds_pol').fullpath),
            ('null1', path.reduced('e2ds_null1_pol').fullpath),
            ('null2', path.reduced('e2ds_null2_pol').fullpath)
        ))
        product_pol = path.final_product('p').fullpath
        self.create_mef(filepath, input_files, product_pol)
        self.distribute_file(product_pol)

    def create_tell_product(self, path):
        tell_path = path.e2ds('AB', telluric_corrected=True, flat_fielded=True).fullpath
        product_tell = path.final_product('t').fullpath
        self.copy_fits(tell_path, product_tell)
        self.distribute_file(product_tell)

    def create_ccf_product(self, path, ccf_mask, telluric_corrected):
        ccf_path = path.ccf('AB', ccf_mask, telluric_corrected=telluric_corrected).fullpath
        product_ccf = path.final_product('v').fullpath
        self.copy_fits(ccf_path, product_ccf)
        self.distribute_file(product_ccf)

    def copy_fits(self, input_file, output_file):
        if not self.create:
            return
        logger.info('Creating FITS %s', output_file)
        if self.trace:
            return
        temp = fits.open(input_file)
        temp.writeto(output_file, overwrite=True)

    def create_mef(self, primary_header_file, extension_files, output_file):
        if not self.create:
            return
        logger.info('Creating MEF %s', output_file)
        if self.trace:
            return
        primary = fits.open(primary_header_file)[0]
        mef = fits.HDUList(primary)
        for ext_name, ext_file in extension_files.items():
            ext = fits.open(ext_file)[0]
            ext.header.insert(0, ('EXTNAME', ext_name))
            mef.append(ext)
        mef.writeto(output_file, overwrite=True)

    def distribute_file(self, file):
        if self.trace or not self.distribute:
            return
        hdu = fits.open(file)[0]
        run_id = hdu.header['RUNID']
        dist_root = '/data/distribution/spirou/'
        subdir = 'quicklook' if self.realtime else 'reduced'
        dist_dir = os.path.join(dist_root, run_id.lower(), subdir)
        pathlib.Path(dist_dir).mkdir(parents=True, exist_ok=True)
        new_file = shutil.copy2(file, dist_dir)
        logger.info('Distributing %s', new_file)


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
        'ccfmask': header['CCFMASK1'],
        'ccfmacpp': header['CCFMACP1'],
        'ccfcontr': header['CCFCONT1'],
        'ccfrv': header['CCFRV1'],
        'ccfrvc': header['CCFRVC'],
        'ccffwhm': header['CCFFWHM1']
    }
