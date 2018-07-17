#!/data/spirou/venv/bin/python3

import sys, os
PYTHONPATHS = ['/data/spirou/spirou-drs/INTROOT', '/data/spirou/spirou-drs/INTROOT/bin']
sys.path.extend(PYTHONPATHS)

import argparse
from drstriggerlite import ActualExposureConfig, CommandMap, MissingKeysError, OpeningFITSError, UnknownConfigError
from drstriggerlite.drsCommands import cal_preprocess_spirou

from astropy.io import fits

def main(args):
    night = args.night
    filenames = args.filenames
    sys.argv = [sys.argv[0]] # Wipe out argv so DRS doesn't rely on CLI arguments instead of what is passed in
    if len(filenames) == 1:
        filename = filenames[0]
        ppfile = preprocess_file(night, filename)
        if ppfile is not None:
            assert os.path.exists(ppfile)
            hdu = fits.open(filename)[0].header
            exp_index = hdu['CMPLTEXP']
            exp_total = hdu['NEXP']
            if exp_total == 1:
                reduce_sequence(night, [ppfile])
            else:
                print('Waiting for full sequence to reduce - exposure', exp_index, 'of', exp_total)
    elif len(filenames) > 1:
        for index, filename in enumerate(filenames):
            ppfile = preprocessed_filename(filename)
            assert os.path.exists(ppfile)
            hdu = fits.open(filename)[0].header
            exp_index = hdu['CMPLTEXP']
            exp_total = hdu['NEXP']
            assert exp_index == index + 1
            assert exp_total == len(filenames)
        reduce_sequence(night, [preprocessed_filename(filename) for filename in filenames])

def preprocess_file(night, filename):
    try:
        cal_preprocess_spirou(night, os.path.basename(filename))
        print('Finished pre-processing on', filename)
        return preprocessed_filename(filename)
    except Exception as e:
        print('Error running preprocessing on', filename)
        print(e)

def preprocessed_filename(filename):
    return os.path.splitext(filename)[0] + '_pp.fits'

def reduce_sequence(night, filenames):
    try:
        sequence_config = ActualExposureConfig.from_file(filenames[0])
        for filename in filenames:
            exposure_config = ActualExposureConfig.from_file(filename)
            assert exposure_config == sequence_config
        drs_command = CommandMap().get(sequence_config)
        basenames = [os.path.basename(filename) for filename in filenames]
        result = drs_command(night, files=basenames)
        print('Finished running recipe on', filenames)
        return result
    except OpeningFITSError as e:
        print('Failed to open FITS header of', e.filename)
    except MissingKeysError as e:
        print('Header of', e.filename, 'missing keyword(s):', ','.join(e.keys))
    except UnknownConfigError as e:
        print('Failed to find matching DRS command to with DPRTYPE =', e.config)
    except Exception as e:
        print('Error occurred while running DRS trigger')
        print(e)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('night')
    parser.add_argument('filenames', nargs='+')
    args = parser.parse_args()
    main(args)
