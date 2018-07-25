#!/data/spirou/venv/bin/python3

import os, argparse, pickle
from shared import sequence_runner, input_directory, set_drs_config_subdir

SEQUENCE_CACHE_FILE = '.sequence.cache'

set_drs_config_subdir('config/realtime/')
sessiondir = '/data/sessions/spirou/'

def main(rawpath):
    night, file = setup_symlink(rawpath)
    current_sequence = load_sequence_cache()
    current_sequence = sequence_runner(current_sequence, file, night)
    save_sequence_cache(current_sequence)

def setup_symlink(rawpath):
    rawdir, filename = os.path.split(rawpath)
    if rawdir.startswith(sessiondir):
        night = rawdir[len(sessiondir):]
    else:
        raise RuntimeError('Night directory should start with ' + sessiondir)
    linkdir = os.path.join(input_directory, night)
    if not os.path.exists(linkdir):
        os.makedirs(linkdir)
    linkpath = os.path.join(linkdir, filename)
    try:
        os.symlink(rawpath, linkpath)
    except FileExistsError as e:
        pass
    return night, linkpath

def load_sequence_cache():
    try:
        return pickle.load(open(SEQUENCE_CACHE_FILE, 'rb'))
    except (OSError, IOError) as e:
        print("No sequence cache found. This should not appear after the first time this script is run.")
        return []

def save_sequence_cache(current_sequence):
    try:
        pickle.dump(current_sequence, open(SEQUENCE_CACHE_FILE, 'wb'))
    except (OSError, IOError) as e:
        print('Failed to save sequence cache')

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('rawpath')
    args = parser.parse_args()
    main(args.rawpath)

# echo "@say_ status: test spirou realtime status" | nc -q 0 spirou-session 20140