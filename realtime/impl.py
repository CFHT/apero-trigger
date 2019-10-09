from multiprocessing import Event

from drsloader import DrsLoader
from .localdb import CalibrationStateCache, RealtimeStateCache
from .process import RealtimeProcessor
from .starter import start_realtime as start_realtime_general


def start_realtime(num_processes, file_queue):
    trigger = load_realtime_trigger()
    remote_api = ApiBridge(file_queue)
    realtime_cache = RealtimeStateCache()
    start_realtime_general(trigger.find_sequences, remote_api, realtime_cache, process_from_queues, num_processes,
                           fetch_interval=10, tick_interval=1, exit_event=Event())


def load_realtime_trigger():
    DrsLoader.set_drs_config_subdir('realtime')
    loader = DrsLoader()
    cfht = loader.get_loaded_trigger_module()
    ccf_params = cfht.CcfParams('masque_sept18_andres_trans50.mas', 0, 200, 1)
    trigger = cfht.CfhtRealtimeTrigger(ccf_params)
    return trigger


def process_from_queues(exposure_queue, sequence_queue, exposures_done, sequences_done):
    trigger = load_realtime_trigger()
    calibration_cache = CalibrationStateCache()
    processor = RealtimeProcessor(trigger, DrsLoader.SESSION_DIR, calibration_cache, tick_interval=1)
    processor.process_from_queues(exposure_queue, sequence_queue, exposures_done, sequences_done)


class ApiBridge:
    def __init__(self, file_queue):
        self.queue = file_queue

    def get_new_exposures(self, cursor):
        files = []
        while not self.queue.empty():
            files.append(self.queue.get(block=False))
