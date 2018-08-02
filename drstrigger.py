import sys
from astropy.io import fits

from commandmap import CommandMap
from pathhandler import PathHandler
from drswrapper import DRS

TRIGGER_VERSION = '002'
TELLURIC_STANDARD_PROGRAMS = ['18AE96', '18BE93']


def sequence_runner(current_sequence, file, night):
    if not file.endswith(('g.fits', 'r.fits', 'RW.fits', 'pp.fits')):
        hdu = fits.open(file)[0].header
        exp_index = hdu['CMPLTEXP']
        exp_total = hdu['NEXP']
        drstrigger(night, file=file)
        current_sequence.append(file)
        if exp_index == exp_total:
            drstrigger(night, sequence=current_sequence)
            current_sequence = []
    return current_sequence


def drstrigger(night, file=None, sequence=None):
    try:
        sys.argv = [sys.argv[0]]  # Wipe out argv so DRS doesn't rely on CLI arguments instead of what is passed in
        if file is not None:
            process_exposure(night, file)
        if sequence is not None:
            process_sequence(night, sequence)
    except Exception as e:
        print('Error:', e)
    except SystemExit:
        print("DRS recipe failed")


def process_exposure(night, file):
    path = PathHandler(night, file)
    try:
        CommandMap.preprocess_exposure(path)
    except Exception as e:
        raise RuntimeError('Error running pre-processing on', path.raw_path(), e)
    exposure_config = exposure_config_from_file(path.preprocessed_path())
    try:
        result = CommandMap.process_exposure(exposure_config, path)
        return result
    except Exception as e:
        raise RuntimeError('Error extracting', path.preprocessed_path(), e)


def process_sequence(night, files):
    paths = [PathHandler(night, file) for file in files]
    sequence_config = exposure_config_from_file(paths[0].preprocessed_path())
    for path in paths:
        exposure_config = exposure_config_from_file(path.preprocessed_path())
        assert exposure_config == sequence_config, 'Exposure type changed mid-sequence'
    try:
        result = CommandMap.process_sequence(sequence_config, paths)
        return result
    except Exception as e:
        raise RuntimeError('Error processing sequence', files, e)


def exposure_config_from_file(filename):
    try:
        header = fits.open(filename)[0].header
    except:
        raise RuntimeError('Failed to open', filename)
    if 'OBSTYPE' in header and header['OBSTYPE'] == 'OBJECT':
        if 'RUNID' not in header:
            raise RuntimeError('Object file missing RUNID keyword', filename)
        if 'SBRHB1_P' not in header:
            raise RuntimeError('Object file missing SBRHB1_P keyword', filename)
        if 'SBRHB2_P' not in header:
            raise RuntimeError('Object file missing SBRHB2_P keyword', filename)
        prefix = 'SPEC' if header['SBRHB1_P'] == 'P16' and header['SBRHB2_P'] == 'P16' else 'POL'
        suffix = 'TELL' if header['RUNID'] in TELLURIC_STANDARD_PROGRAMS else 'OBJ'
        return prefix + '_' + suffix
    elif 'DPRTYPE' in header and header['DPRTYPE'] != "None":
        return header['DPRTYPE']
    else:
        raise RuntimeError('Non-object file missing DPRTYPE keyword', filename)


def drs_version():
    return DRS.version()


def trigger_version():
    return TRIGGER_VERSION
