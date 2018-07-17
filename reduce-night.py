#!/data/spirou/venv/bin/python3

import glob, os
import argparse
from fileproccesser import blocking_subprocess
from astropy.io import fits

input_directory = '/data/spirou2/raw/'
bin_dir = '/data/spirou/realtime/'

env = os.environ.copy()

def reduce_night(night):
    file_pattern = os.path.join(input_directory, night, '*.fits')
    unsorted_files = [file for file in glob.glob(file_pattern) if os.path.exists(file)] # filter out broken symlinks
    all_files = sorted(unsorted_files, key=os.path.getmtime)
    current_sequence = []
    for file in all_files:
        if not file.endswith(('g.fits', 'r.fits', 'RW.fits', 'pp.fits')):
            hdu = fits.open(file)[0].header
            exp_index = hdu['CMPLTEXP']
            exp_total = hdu['NEXP']
            process_file(night, file)
            if exp_total > 1:
                current_sequence.append(file)
                if exp_index == exp_total:
                    process_seqeunce(night, current_sequence)
                    current_sequence = []
        else:
            print("Spirou DRS skipping file:", file)

def process_file(night, file):
    print("Spirou DRS processing file:", file)
    blocking_subprocess(os.path.join(bin_dir, 'drstrigger-new.py'), [night, file], env=env)

def process_seqeunce(night, files):
    print("Spirou DRS processing files:", files)
    blocking_subprocess(os.path.join(bin_dir, 'drstrigger-new.py'), [night] + files, env=env)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('night')
    args = parser.parse_args()
    reduce_night(args.night)
