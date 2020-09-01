from __future__ import annotations

from multiprocessing import Queue
from pathlib import Path

from drsloader import DrsLoader
from trigger.baseinterface.drstrigger import IDrsTrigger
from .apibridge import ApiBridge
from .localdb import CalibrationStateCache, RealtimeStateCache
from .manager import start_realtime
from .process import RealtimeProcessor


def load_and_start_realtime(num_processes: int, file_queue: Queue[Path]):
    trigger = __load_realtime_trigger()
    remote_api = ApiBridge(file_queue, DrsLoader.SESSION_DIR, trigger)
    realtime_cache = RealtimeStateCache()
    start_realtime(trigger.find_sequences, remote_api, realtime_cache, __process_from_queues, num_processes,
                   fetch_interval=10, tick_interval=1)


def __load_realtime_trigger() -> IDrsTrigger:
    loader = DrsLoader('realtime_config')
    cfht = loader.get_loaded_trigger_module()
    trigger = cfht.CfhtRealtimeTrigger()
    return trigger


def __process_from_queues(*args):
    trigger = __load_realtime_trigger()
    calibration_cache = CalibrationStateCache()
    processor = RealtimeProcessor(trigger, calibration_cache, tick_interval=1)
    processor.process_from_queues(*args)
