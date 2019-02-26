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
    return (types['preprocess'] and (has_calibration_extension(file) or has_object_extension(file)) or
            types['calibrations'] and has_calibration_extension(file) or
            types['objects'] and has_object_extension(file) or
            types['pol'] and has_object_extension(file) or
            types['mktellu'] and has_object_extension(file) and is_telluric_standard(fits.open(file)[0].header) or
            types['fittellu'] and has_object_extension(file) and not is_telluric_standard(fits.open(file)[0].header) or
            # TODO: handle skipping sky exposures when command map is updated to do so
            types['ccf'] and has_object_extension(file) and not is_telluric_standard(fits.open(file)[0].header) or
            types['products'] and has_object_extension(file) or
            types['distribute'] and has_object_extension(file) or
            types['database'] and has_object_extension(file))


def has_object_extension(file):
    return file.endswith('o.fits')


def has_calibration_extension(file):
    return file.endswith(('a.fits', 'c.fits', 'd.fits', 'f.fits'))


def is_spectroscopy(header):
    if 'SBRHB1_P' not in header:
        raise RuntimeError('Object file missing SBRHB1_P keyword')
    if 'SBRHB2_P' not in header:
        raise RuntimeError('Object file missing SBRHB2_P keyword')
    return header['SBRHB1_P'] == 'P16' and header['SBRHB2_P'] == 'P16'


def is_telluric_standard(header):
    name_keyword = 'OBJECT'
    if name_keyword not in header:
        name_keyword = 'OBJNAME'
        if name_keyword not in header:
            raise RuntimeError('Object file missing OBJECT and OBJNAME keywords')
    return header[name_keyword] in TELLURIC_STANDARDS


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
