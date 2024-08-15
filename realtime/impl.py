from __future__ import annotations

from functools import partial
from multiprocessing import Queue
from pathlib import Path
from typing import Optional

from trigger import CfhtTrigger
from .apibridge import ApiBridge
from .localdb import DataCache
from .manager import RealtimeStateCache, start_realtime
from .process import RealtimeProcessor, init_realtime_process, process_from_queues


def load_and_start_realtime(num_processes: int, file_queue: Queue[Path],
                            trace: Optional[bool]):
    trigger = CfhtTrigger(trace)
    remote_api = ApiBridge(file_queue, trigger)
    realtime_cache: RealtimeStateCache = DataCache(Path('.drstrigger-realtime.cache'))
    process_from_queues_part = partial(__process_from_queues, trace)
    start_realtime(remote_api, realtime_cache, init_realtime_process, process_from_queues_part,
                   num_processes, 10, 1, 1)


def __process_from_queues(trace: Optional[bool]):
    trigger = CfhtTrigger(trace)
    processor = RealtimeProcessor(trigger)
    return process_from_queues(processor)
