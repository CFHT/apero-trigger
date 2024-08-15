from __future__ import annotations

import queue
import time
from abc import ABC, abstractmethod
from multiprocessing import Event, Pool, Queue, Value
from typing import Callable, Iterable, Sequence

from logger import log
from trigger.common.pathhandler import Exposure
from .localdb import DataCache
from .typing import InitArgs, InitProcess, ProcessFromQueues

SequenceFinder = Callable[[Iterable[Exposure]], Iterable[Sequence[Exposure]]]
# Need a forward reference...
RealtimeStateCache = DataCache['Realtime']


class IExposureApi(ABC):
    @abstractmethod
    def get_new_exposures(self, cursor) -> Iterable[Exposure]:
        pass


def start_realtime(remote_api: IExposureApi, realtime_cache: RealtimeStateCache,
                   init_queues: InitProcess, process_from_queues: ProcessFromQueues, num_processes: int,
                   fetch_interval: float, tick_interval: float, subprocess_tick_interval: float,
                   started_running: Event = None, finished_running: Value = None, stop_running: Event = None):
    if started_running is None:
        started_running = Event()
    if finished_running is None:
        finished_running = Value('i', 0)
    if stop_running is None:
        stop_running = Event()
    realtime = Realtime(remote_api, realtime_cache, subprocess_tick_interval)
    try:
        realtime = realtime_cache.load()
        realtime.inject(remote_api, realtime_cache, subprocess_tick_interval)
    except (OSError, IOError):
        log.warning('Realtime state file %s not found. This should only appear the first time realtime is run.',
                    realtime_cache.cache_file)
    realtime.main(num_processes, init_queues, process_from_queues, fetch_interval, tick_interval,
                  started_running, finished_running, stop_running)


class Realtime:
    def __init__(self, remote_api: IExposureApi, local_db: RealtimeStateCache,
                 subprocess_tick_interval: float):
        # Shared construction method
        self.__construct()
        # Injectable resources
        self.remote_api = remote_api
        self.local_db = local_db
        self.subprocess_tick_interval = subprocess_tick_interval
        # Set initial state
        self.exposures_to_process = []
        self.cursor = None

    def __construct(self):
        self.exposure_in_queue = Queue()
        self.exposure_out_queue = Queue()

    # Need to call this after __setstate__ is called, e.g. after loading from pickle.
    def inject(self, remote_api: IExposureApi, local_db: RealtimeStateCache,
               subprocess_tick_interval: float):
        self.remote_api = remote_api
        self.local_db = local_db
        self.subprocess_tick_interval = subprocess_tick_interval

    def __getstate__(self):
        return {key: self.__dict__[key] for key in ('exposures_to_process',
                                                    'cursor')}

    def __setstate__(self, state):
        self.__construct()
        self.__dict__.update(state)
        for exposure in self.exposures_to_process:
            self.exposure_in_queue.put(exposure)

    def main(self, num_processes: int, init_operation: InitProcess, process_operation: ProcessFromQueues,
             fetch_interval: float, tick_interval: float,
             started_running: Event, finished_running: Value, stop_running: Event):

        stop_signal = Event()
        init_args: InitArgs = (self.subprocess_tick_interval, stop_signal,
                               self.exposure_in_queue,
                               self.exposure_out_queue)
        with Pool(num_processes, init_operation, init_args, maxtasksperchild=1) as pool:
            async_results = []
            for i in range(num_processes):
                async_results.append(pool.apply_async(process_operation))
            started_running.set()

            while not stop_running.is_set():
                self.__fetch_and_handle_new_exposures()
                fetch_time = time.time() + fetch_interval
                while time.time() < fetch_time:
                    self.__queue_tick(finished_running)
                    self.__replace_finished_processes(pool, async_results, process_operation)
                    time.sleep(tick_interval)

            stop_signal.set()
            pool.close()
            pool.join()

    @staticmethod
    def __replace_finished_processes(pool, async_results, operation):
        for i, result in enumerate(async_results):
            if result and result.ready():
                try:
                    value = result.get()
                    if operation is not None:
                        if not value:
                            log.error('Process %i did not return success: %s', i, value)
                        async_results[i] = pool.apply_async(operation)
                    else:
                        async_results[i] = None
                except Exception as e:
                    log.error('Error occurred in process %i', i, exc_info=e)

    def __fetch_and_handle_new_exposures(self):
        new_exposures = self.remote_api.get_new_exposures(self.cursor)
        if new_exposures:
            # self.cursor = new_exposures[-1].get_timestamp()  # THIS IS NOT A REAL METHOD
            for exposure in new_exposures:
                self.exposure_in_queue.put(exposure)
                self.exposures_to_process.append(exposure)
            self.local_db.save(self)

    def __queue_tick(self, finished_running):
        updated = 0
        while not self.exposure_out_queue.empty():
            try:
                exposure = self.exposure_out_queue.get(block=False)
                self.exposures_to_process.remove(exposure)
                updated += 1
            except queue.Empty:
                pass
        if updated:
            self.local_db.save(self)
            with finished_running.get_lock():
                finished_running.value += updated
