import os, pickle

from envconfig import sessiondir, set_drs_config_subdir
set_drs_config_subdir('config/realtime/')

from pathhandler import PathHandler
from drstrigger import drstrigger, sequence_checker

SEQUENCE_CACHE_FILE = '.sequence.cache'


def realtime(rawpath):
    night, file = setup_symlink(rawpath)
    drstrigger(night, file=file, realtime=True)
    current_sequence = load_sequence_cache()
    completed_sequence = sequence_checker(current_sequence, file)
    save_sequence_cache(current_sequence)
    drstrigger(night, sequence=completed_sequence)


def setup_symlink(rawpath):
    rawdir, filename = os.path.split(rawpath)
    if rawdir.startswith(sessiondir):
        night = rawdir[len(sessiondir):]
    else:
        raise RuntimeError('Night directory should start with ' + sessiondir)
    path = PathHandler(night, filename)
    linkdir = path.input_directory()
    if not os.path.exists(linkdir):
        os.makedirs(linkdir)
    linkpath = path.raw_path()
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

# echo "@say_ status: test spirou realtime status" | nc -q 0 spirou-session 20140
