#!/data/spirou/venv/bin/python3

import os, argparse, pickle
from shared import sequence_runner, input_directory

def main(args):
    rawpath = args.rawpath
    night, file = setup_symlink(rawpath)
    current_sequence = load_sequence_cache()
    current_sequence = sequence_runner(current_sequence, file, night)
    save_sequence_cache(current_sequence)

def setup_symlink(rawpath):
    rawdir, filename = os.path.split(rawpath)
    night = os.path.basename(rawdir)
    linkdir = os.path.join(input_directory, night)
    if not os.path.exists(linkdir):
        os.mkdir(linkdir)
    linkpath = os.path.join(linkdir, filename)
    try:
        os.symlink(rawpath, linkpath)
    except FileExistsError as e:
        pass
    return night, linkpath

def load_sequence_cache():
    try:
        return pickle.load(open('sequence.cache', 'rb'))
    except (OSError, IOError) as e:
        print("No sequence cache found. This should not appear after the first time this script is run.")

def save_sequence_cache(current_sequence):
    try:
        pickle.dump(current_sequence, open('sequence.cache', 'wb'))
    except (OSError, IOError) as e:
        print('Failed to save sequence cache')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('rawpath')
    args = parser.parse_args()
    main(args)
