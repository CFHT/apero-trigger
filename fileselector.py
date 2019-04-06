import datetime
from os import path

from astropy.io import fits
from logger import logger
from envconfig import drs_root


def read_telluric_whitelist():
    whitelist_file = path.join(drs_root, 'SpirouDRS/data/constants/tellu_whitelist.txt')
    with open(whitelist_file) as f:
        return set(line for line in f.read().splitlines() if not (line.startswith('#') or line == ''))


TELLURIC_STANDARDS = read_telluric_whitelist()


def sort_and_filter_files(files, types, runid=None):
    return sort_files_by_date_header(filter_files_by_type(files, types), runid)


def filter_files_by_type(files, types):
    return [file for file in files if is_desired_type(file, types)]


def is_desired_type(file, types):
    header = HeaderChecker(file)
    return (types.preprocess and (has_calibration_extension(file) or has_object_extension(file)) or
            types.calibrations and has_calibration_extension(file) or
            types.objects and has_object_extension(file) and is_desired_object(types.objects, header))


def is_desired_object(object_types, header):
    return (object_types.extract or
            object_types.pol or
            object_types.mktellu and header.is_telluric_standard() or
            object_types.fittellu and not header.is_telluric_standard() or
            # TODO: handle skipping sky exposures when command map is updated to do so
            object_types.ccf and not header.is_telluric_standard() or
            object_types.products or
            object_types.distribute or
            object_types.database)


def has_object_extension(file):
    return file.endswith('o.fits')


def has_calibration_extension(file):
    return file.endswith(('a.fits', 'c.fits', 'd.fits', 'f.fits'))


class HeaderChecker:
    def __init__(self, file):
        self.file = file
        self.header = None

    def __lazy_loading(self):
        if not self.header:
            try:
                hdulist = fits.open(self.file)
            except:
                raise RuntimeError('Failed to open', self.file)
            self.header = hdulist[0].header

    def is_object(self):
        self.__lazy_loading()
        return 'OBSTYPE' in self.header and self.header['OBSTYPE'] == 'OBJECT'

    def is_spectroscopy(self):
        self.__lazy_loading()
        if 'SBRHB1_P' not in self.header:
            raise RuntimeError('Object file missing SBRHB1_P keyword', self.file)
        if 'SBRHB2_P' not in self.header:
            raise RuntimeError('Object file missing SBRHB2_P keyword', self.file)
        return self.header['SBRHB1_P'] == 'P16' and self.header['SBRHB2_P'] == 'P16'

    def is_telluric_standard(self):
        self.__lazy_loading()
        name_keyword = 'OBJECT'
        if name_keyword not in self.header:
            name_keyword = 'OBJNAME'
            if name_keyword not in self.header:
                raise RuntimeError('Object file missing OBJECT and OBJNAME keywords', self.file)
        return self.header[name_keyword] in TELLURIC_STANDARDS

    def get_dpr_type(self):
        self.__lazy_loading()
        if 'DPRTYPE' not in self.header or self.header['DPRTYPE'] == 'None':
            raise RuntimeError('File missing DPRTYPE keyword', self.file)
        return self.header['DPRTYPE']

    def get_exposure_index_and_total(self):
        self.__lazy_loading()
        if 'CMPLTEXP' not in self.header or 'NEXP' not in self.header:
            logger.warning('%s missing CMPLTEXP/NEXP in header, treating sequence as single exposure', self.file)
            return 1, 1
        else:
            return self.header['CMPLTEXP'], self.header['NEXP']


def sort_files_by_date_header(files, runid=None):
    file_times = {}
    for file in files:
        header = fits.open(file)[0].header
        if runid and 'RUNID' not in header:
            logger.warning('File %s missing RUNID keyword, skipping.', file)
        elif runid and header['RUNID'] != runid:
            pass
        elif 'DATE' not in header:
            logger.warning('File %s missing DATE keyword, skipping.', file)
        else:
            date_string = header['DATE']
            timestamp = datetime.datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S')
            file_times[file] = timestamp
    return sorted(file_times, key=file_times.get)
