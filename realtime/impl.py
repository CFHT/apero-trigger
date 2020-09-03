from __future__ import annotations

from functools import partial
from multiprocessing import Queue
from pathlib import Path
from typing import Iterable, Optional

from drsloader import DrsLoader
from trigger.baseinterface.drstrigger import IDrsTrigger
from .apibridge import ApiBridge
from .localdb import CalibrationStateCache, RealtimeStateCache
from .manager import start_realtime
from .process import RealtimeProcessor


def load_and_start_realtime(num_processes: int, file_queue: Queue[Path],
                            config_subdir: Optional[str], steps: Optional[Iterable[str]], trace: Optional[bool]):
    trigger = __load_realtime_trigger(config_subdir, steps, trace)
    remote_api = ApiBridge(file_queue, trigger)
    realtime_cache = RealtimeStateCache()
    process_from_queues = partial(__process_from_queues, config_subdir, steps, trace)
    start_realtime(trigger.find_sequences, remote_api, realtime_cache, process_from_queues, num_processes,
                   fetch_interval=10, tick_interval=1)


def __load_realtime_trigger(config_subdir: Optional[str], steps: Optional[Iterable[str]],
                            trace: Optional[bool]) -> IDrsTrigger:
    loader = DrsLoader(config_subdir)
    cfht = loader.get_loaded_trigger_module()
    steps = cfht.CfhtDrsSteps.all() if steps is None else cfht.CfhtDrsSteps.from_keys(steps)
    trigger = cfht.CfhtRealtimeTrigger(steps, trace)
    return trigger


def __process_from_queues(config_subdir: Optional[str], steps: Optional[Iterable[str]], trace: Optional[bool], *args):
    trigger = __load_realtime_trigger(config_subdir, steps, trace)
    calibration_cache = CalibrationStateCache()
    processor = RealtimeProcessor(trigger, calibration_cache, tick_interval=1)
    processor.process_from_queues(*args)
