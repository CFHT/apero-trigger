#!/data/spirou/venv/bin/python3

import glob, os
import argparse
from fileproccesser import blocking_subprocess

input_directory = '/data/spirou2/raw/'
bin_dir = '/data/spirou/realtime/'

env = os.environ.copy()

def reduce_night(night):
    file_pattern = os.path.join(input_directory, night, '*.fits')
    unsorted_files = [file for file in glob.glob(file_pattern) if os.path.exists(file)] # filter out broken symlinks
    all_files = sorted(unsorted_files, key=os.path.getmtime)
    for file in all_files:
        if not file.endswith(('g.fits', 'r.fits', 'RW.fits', 'pp.fits')):
            print("Spirou DRS processing file:", file)
            blocking_subprocess(os.path.join(bin_dir, 'preprocess-wrapper.py'), [night, file], env=env)
            pp_pattern = os.path.splitext(file)[0] + '*_pp.fits'
            ppfile = glob.glob(pp_pattern)
            assert len(ppfile) == 1
            ppfile = ppfile[0]
            blocking_subprocess(os.path.join(bin_dir, 'drstrigger.py'), [night, ppfile], env=env)
        else:
            print("Spirou DRS skipping file:", file)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('night')
    args = parser.parse_args()
    reduce_night(args.night)
