#!/data/spirou/venv/bin/python3

import sys, os
import argparse
import pickle
from collections import defaultdict
from astropy.io import fits

PYTHONPATHS = ['/data/spirou/spirou-drs/INTROOT', '/data/spirou/spirou-drs/INTROOT/bin']
sys.path.extend(PYTHONPATHS)
import commands as drsCommands

class CommandMap(object):
    def __init__(self):
        self.__process_exposure = defaultdict(lambda: self.__unknown, {
            'DARK_DARK': self.__do_nothing,
            'DARK_FLAT': self.__do_nothing,
            'FLAT_DARK': self.__do_nothing,
            'FLAT_FLAT': self.__do_nothing,
            'FP_FP': self.__fp,
            'HCONE_HCONE': drsCommands.cal_extract_RAW_spirou,
            'DARK_HCONE': self.__do_nothing,
            'HCONE_DARK': self.__do_nothing,
            'OBJECT': drsCommands.cal_extract_RAW_spirou,
        })
        self.__process_sequence = defaultdict(lambda: self.__unknown, {
            'DARK_DARK': drsCommands.cal_DARK_spirou,
            'DARK_FLAT': drsCommands.cal_loc_RAW_spirou,
            'FLAT_DARK': drsCommands.cal_loc_RAW_spirou,
            'FLAT_FLAT': drsCommands.cal_FF_RAW_spirou,
            'FP_FP': drsCommands.cal_SLIT_spirou,
            'HCONE_HCONE': self.__wave,
        })
        try:
            self.last_fp = pickle.load(open('last_fp.cache', 'rb'))
        except (OSError, IOError) as e:
            pass

    def preprocess_exposure(self, night, file):
        basename = os.path.basename(file)
        drsCommands.cal_preprocess_spirou(night, basename)

    def process_exposure(self, config, night, file):
        basename = os.path.basename(file)
        command = self.__process_exposure[config]
        return command(night, basename)

    def process_sequence(self, config, night, files):
        basenames = [os.path.basename(file) for file in files]
        command = self.__process_sequence[config]
        return command(night, basenames)

    def __do_nothing(self, night, file):
        return None

    def __unknown(self, night, file):
        print("Unrecognized DPRTYPE, skipping", file)
        return None

    def __fp(self, night, file):
        self.last_fp = night + '_' + file # To match format output by the DRS
        try:
            pickle.dump(self.last_fp, open('last_fp.cache', 'wb'))
        except (OSError, IOError) as e:
            print('Failed to serialize last_fp, this will probably cause an error on cal_WAVE')
        return drsCommands.cal_extract_RAW_spirou(night, file)

    def __wave(self, night, files):
        assert len(files) == 1, 'Too many HCONE_HCONE files'
        assert self.last_fp is not None, 'Need an extracted FP file for cal_WAVE'
        for fiber in ('AB', 'A', 'B', 'C'):
            hcone = fiber_filename(night + '_' + files[0], fiber) # To match format output by the DRS
            fp = fiber_filename(self.last_fp, fiber)
            drsCommands.cal_WAVE_E2DS_spirou(night, fp, hcone)


class ActualExposureConfig:
    @classmethod
    def from_file(cls, filename):
        try:
            header = fits.open(filename)[0].header
        except:
            raise RuntimeError('Failed to open', filename)
        if 'DPRTYPE' in header:
            return header['DPRTYPE']
        elif 'OBSTYPE' in header and header['OBSTYPE'] == 'OBJECT':
            return 'OBJECT'
        else:
            raise RuntimeError('Non-object file missing DPRTYPE keyword', filename)

command_map = CommandMap()

def main(args):
    night = args.night
    filename = args.file
    sequence = args.sequence
    sys.argv = [sys.argv[0]] # Wipe out argv so DRS doesn't rely on CLI arguments instead of what is passed in
    if filename is not None:
        process_exposure(night, filename)
    if sequence is not None:
        process_seqeunce(night, [preprocessed_filename(filename) for filename in sequence])

def preprocessed_filename(filename):
    return os.path.splitext(filename)[0] + '_pp.fits'

def fiber_filename(filename, fiber):
    return os.path.splitext(filename)[0] + '_e2ds_' + fiber + '.fits'

def process_exposure(night, file):
    try:
        command_map.preprocess_exposure(night, file)
        print('Finished pre-processing on', file)
    except Exception as e:
        raise RuntimeError('Error running pre-processing on', file, e)
    preprocessed = preprocessed_filename(file)
    exposure_config = ActualExposureConfig.from_file(preprocessed)
    try:
        result = command_map.process_exposure(exposure_config, night, preprocessed)
        return result
    except Exception as e:
        raise RuntimeError('Error extracting', preprocessed, e)

def process_seqeunce(night, files):
    sequence_config = ActualExposureConfig.from_file(files[0])
    for filename in files:
        exposure_config = ActualExposureConfig.from_file(filename)
        assert exposure_config == sequence_config, 'Exposure type changed mid-sequence'
    try:
        result = command_map.process_sequence(sequence_config, night, files=files)
        print('Finished running recipe on', files)
        return result
    except Exception as e:
        raise RuntimeError('Error processing sequence', files, e)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('night')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--file')
    group.add_argument('--sequence', nargs='+')
    args = parser.parse_args()
    try:
        main(args)
    except Exception as e:
        print(e)
