#!/data/spirou/venv/bin/python3

import os, argparse, glob
from shared import sequence_runner, input_directory

def main(args):
    night = args.night
    file_pattern = os.path.join(input_directory, night, '*.fits')
    unsorted_files = [file for file in glob.glob(file_pattern) if os.path.exists(file)] # filter out broken symlinks
    all_files = sorted(unsorted_files, key=os.path.getmtime)
    current_sequence = []
    for file in all_files:
        current_sequence = sequence_runner(current_sequence, file, night)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('night')
    args = parser.parse_args()
    main(args)
