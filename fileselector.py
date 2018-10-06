import datetime
from astropy.io import fits
from logger import logger

TELLURIC_STANDARD_PROGRAMS = ['18AE96', '18BE93']


def sort_and_filter_files(files, types):
    return sort_files_by_date_header(filter_files_by_type(files, types))


def filter_files_by_type(files, types):
    return [file for file in files if is_desired_type(file, types)]


def is_desired_type(file, types):
    return (types['preprocess'] and (has_calibration_extension(file) or has_object_extension(file)) or
            types['calibrations'] and has_calibration_extension(file) or
            types['objects'] and has_object_extension(file) or
            types['mktellu'] and has_object_extension(file) and is_telluric_standard(fits.open(file)[0].header) or
            types['fittellu'] and has_object_extension(file) and not is_telluric_standard(fits.open(file)[0].header) or
            # TODO: handle skipping sky exposures when command map is updated to do so
            types['ccf'] and has_object_extension(file) and not is_telluric_standard(fits.open(file)[0].header))


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
    if 'RUNID' not in header:
        raise RuntimeError('Object file missing RUNID keyword')
    return header['RUNID'] in TELLURIC_STANDARD_PROGRAMS


def sort_files_by_date_header(files):
    file_times = {}
    for file in files:
        header = fits.open(file)[0].header
        if 'DATE' not in header:
            logger.warning('File %s missing DATE keyword, skipping.', file)
        else:
            date_string = header['DATE']
            timestamp = datetime.datetime.strptime(date_string, '%Y-%m-%dT%H:%M:%S')
            file_times[file] = timestamp
    return sorted(file_times, key=file_times.get)
