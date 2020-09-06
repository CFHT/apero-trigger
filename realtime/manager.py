from __future__ import annotations

import queue
import time
from abc import ABC, abstractmethod
from multiprocessing import Event, Pool, Queue, Value
from typing import Callable, Iterable, Sequence

from logger import log
from trigger.baseinterface.exposure import IExposure
from .localdb import DataCache
from .sequencestatetracker import SequenceStateTracker
from .typing import InitArgs, InitProcess, ProcessFromQueues

SequenceFinder = Callable[[Iterable[IExposure]], Iterable[Sequence[IExposure]]]
# Need a forward reference...
RealtimeStateCache = DataCache['Realtime']


class IExposureApi(ABC):
    @abstractmethod
    def get_new_exposures(self, cursor) -> Iterable[IExposure]:
        pass


def start_realtime(find_sequences: SequenceFinder, remote_api: IExposureApi, realtime_cache: RealtimeStateCache,
                   init_queues: InitProcess, process_from_queues: ProcessFromQueues, num_processes: int,
                   fetch_interval: float, tick_interval: float, subprocess_tick_interval: float,
                   started_running: Event = None, finished_running: Value = None, stop_running: Event = None):
    if started_running is None:
        started_running = Event()
    if finished_running is None:
        finished_running = Value('i', 0)
    if stop_running is None:
        stop_running = Event()
    realtime = Realtime(find_sequences, remote_api, realtime_cache, subprocess_tick_interval)
    try:
        realtime = realtime_cache.load()
        realtime.inject(find_sequences, remote_api, realtime_cache, subprocess_tick_interval)
    except (OSError, IOError):
        log.warning('Realtime state file %s not found. This should only appear the first time realtime is run.',
                    realtime_cache.cache_file)
    realtime.main(num_processes, init_queues, process_from_queues, fetch_interval, tick_interval,
                  started_running, finished_running, stop_running)


class Realtime:
    def __init__(self, sequence_finder: SequenceFinder, remote_api: IExposureApi, local_db: RealtimeStateCache,
                 subprocess_tick_interval: float):
        # Shared construction method
        self.__construct()
        # Injectable resources
        self.sequence_finder = sequence_finder
        self.remote_api = remote_api
        self.local_db = local_db
        self.subprocess_tick_interval = subprocess_tick_interval
        # Set initial state
        self.sequence_mapper = SequenceStateTracker()
        self.exposures_to_process = []
        self.sequences_to_process = []
        self.cursor = None

    def __construct(self):
        self.exposure_in_queue = Queue()
        self.exposure_out_queue = Queue()
        self.sequence_in_queue = Queue()
        self.sequence_out_queue = Queue()

    # Need to call this after __setstate__ is called, e.g. after loading from pickle.
    def inject(self, sequence_finder: SequenceFinder, remote_api: IExposureApi, local_db: RealtimeStateCache,
               subprocess_tick_interval: float):
        self.sequence_finder = sequence_finder
        self.remote_api = remote_api
        self.local_db = local_db
        self.subprocess_tick_interval = subprocess_tick_interval

    def __getstate__(self):
        return {key: self.__dict__[key] for key in ('sequence_mapper',
                                                    'exposures_to_process',
                                                    'sequences_to_process',
                                                    'cursor')}

    def __setstate__(self, state):
        self.__construct()
        self.__dict__.update(state)
        for exposure in self.exposures_to_process:
            self.exposure_in_queue.put(exposure)
        for sequence in self.sequences_to_process:
            self.sequence_in_queue.put(sequence)

    def main(self, num_processes: int, init_operation: InitProcess, process_operation: ProcessFromQueues,
             fetch_interval: float, tick_interval: float,
             started_running: Event, finished_running: Value, stop_running: Event):

        stop_signal = Event()
        init_args: InitArgs = (self.subprocess_tick_interval, stop_signal,
                               self.exposure_in_queue, self.sequence_in_queue,
                               self.exposure_out_queue, self.sequence_out_queue)
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
            self.sequence_mapper.add_unmapped_exposures(new_exposures)
            unmapped_exposures = self.sequence_mapper.get_unmapped_exposures()
            completed_sequences = self.sequence_finder(unmapped_exposures)
            self.sequence_mapper.mark_sequences_complete(completed_sequences)
            for exposure in new_exposures:
                self.exposure_in_queue.put(exposure)
                self.exposures_to_process.append(exposure)
            self.local_db.save(self)

    def __queue_tick(self, finished_running):
        updated = 0
        while not self.sequence_out_queue.empty():
            try:
                sequence = self.sequence_out_queue.get(block=False)
                self.sequences_to_process.remove(sequence)
                updated += 1
            except queue.Empty:
                pass
        while not self.exposure_out_queue.empty():
            try:
                exposure = self.exposure_out_queue.get(block=False)
                self.exposures_to_process.remove(exposure)
                self.sequence_mapper.mark_exposure_processed(exposure)
                sequence = self.sequence_mapper.get_sequence_if_ready_to_process(exposure)
                if sequence:
                    self.sequence_in_queue.put(sequence)
                    self.sequences_to_process.append(sequence)
                    self.sequence_mapper.done_with_sequence(sequence)
                updated += 1
            except queue.Empty:
                pass
        if updated:
            self.local_db.save(self)
            with finished_running.get_lock():
                finished_running.value += updated
