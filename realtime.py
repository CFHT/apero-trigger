import pickle
from pathlib import Path

from distribution import distribute_file
from envconfig import sessiondir, set_drs_config_subdir
set_drs_config_subdir('realtime')

from logger import logger
from pathhandler import Exposure
from drstrigger import DrsTrigger
from commandmap import Steps

SEQUENCE_CACHE_FILE = '.sequence.cache'


def realtime(raw_path):
    try:
        night, file = setup_symlink(raw_path)
        distribute_file(file, 'raw')
        trigger = DrsTrigger(Steps.all(), realtime=True)
        if not trigger.preprocess(night, file):
            return
        trigger.process_file(night, file)
        current_sequence = load_sequence_cache()
        completed_sequence = trigger.sequence_checker(night, current_sequence, file)
        save_sequence_cache(current_sequence)
        if completed_sequence:
            trigger.process_sequence(night, completed_sequence)
    except Exception as e:
        logger.error('Error during realtime processing', exc_info=True)


def setup_symlink(raw_path):
    try:
        relative_path = Path(raw_path).relative_to(sessiondir)
    except ValueError:
        raise RuntimeError('Night directory should start with ' + sessiondir)
    night = relative_path.parent
    filename = relative_path.name

    link_path = Exposure(night, filename).raw
    link_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        link_path.symlink_to(raw_path)
    except FileExistsError as e:
        pass
    return night, link_path


def load_sequence_cache():
    try:
        return pickle.load(open(SEQUENCE_CACHE_FILE, 'rb'))
    except (OSError, IOError) as e:
        logger.warning('No sequence cache found. This should not appear after the first time this script is run.')
        return []


def save_sequence_cache(current_sequence):
    try:
        pickle.dump(current_sequence, open(SEQUENCE_CACHE_FILE, 'wb'))
    except (OSError, IOError) as e:
        logger.error('Failed to save sequence cache')
