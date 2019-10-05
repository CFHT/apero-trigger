import pickle
from pathlib import Path

from drsloader import DrsLoader
from logger import log

class RealtimeCache():
    CACHE_FILE = '.drstrigger.cache'

    @classmethod
    def save_cache(cls, current_sequence, calibration_processor):
        try:
            cache = {
                'current_sequence': current_sequence,
                **calibration_processor.get_state()
            }
            pickle.dump(cache, open(cls.CACHE_FILE, 'wb'))
        except (OSError, IOError) as e:
            log.error('Failed to serialize realtime cache', exc_info=True)

    @classmethod
    def load_cache(self, current_sequence, calibration_processor):
        try:
            cache = pickle.load(open(self.CACHE_FILE, 'rb'))
            sequence = cache.get('current_sequence')
            if sequence:
                current_sequence[:] = sequence
            calibration_processor.set_state(**cache)
        except (OSError, IOError):
            log.warning('No realtime cache found. This should not appear after the first time this script is run.')


def realtime(raw_path):
    try:
        DrsLoader.set_drs_config_subdir('realtime')
        loader = DrsLoader()
        cfht = loader.get_loaded_trigger_module()
        night, file = setup_symlink(raw_path, cfht)
        ccf_params = cfht.CcfParams('masque_sept18_andres_trans50.mas', 0, 200, 1)
        trigger = cfht.CfhtRealtimeTrigger(ccf_params)
        if not trigger.preprocess(night, file):
            return
        trigger.process_file(night, file)
        current_sequence = []
        RealtimeCache.load_cache(current_sequence, trigger.processor.calibration_processor)
        completed_sequences = trigger.sequence_checker(night, current_sequence, file)
        for completed_sequence in completed_sequences:
            if completed_sequence:
                trigger.process_sequence(night, completed_sequence)
        RealtimeCache.save_cache(current_sequence, trigger.processor.calibration_processor)
    except Exception as e:
        log.error('Error during realtime processing', exc_info=True)


def setup_symlink(raw_path, cfht):
    try:
        relative_path = Path(raw_path).relative_to(DrsLoader.SESSION_DIR)
    except ValueError:
        raise RuntimeError('Night directory should start with ' + DrsLoader.SESSION_DIR)
    night = relative_path.parent
    filename = relative_path.name

    link_path = cfht.Exposure(night, filename).raw
    link_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        link_path.symlink_to(raw_path)
    except FileExistsError as e:
        pass
    return night, link_path
