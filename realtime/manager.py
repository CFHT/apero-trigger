from __future__ import annotations

import time
from abc import ABC, abstractmethod
from multiprocessing import Event, Pool, Queue, Value
from typing import Callable, Iterable, Sequence

from logger import log
from trigger.baseinterface.exposure import IExposure
from .localdb import DataCache
from .sequencestatetracker import SequenceStateTracker

SequenceFinder = Callable[[Iterable[IExposure]], Iterable[Sequence[IExposure]]]
ExposureQueue = 'Queue[IExposure]'
SequenceQueue = 'Queue[Sequence[IExposure]]'
ProcessFromQueues = Callable[[ExposureQueue, SequenceQueue, ExposureQueue, SequenceQueue, Value, Value, Event], None]


class IExposureApi(ABC):
    @abstractmethod
    def get_new_exposures(self, cursor) -> Iterable[IExposure]:
        pass


def start_realtime(find_sequences: SequenceFinder, remote_api: IExposureApi, realtime_cache: DataCache,
                   process_from_queues: Callable, num_processes: int, fetch_interval: float, tick_interval: float,
                   started_running: Event = None, finished_running: Event = None, stop_running: Event = None):
    if started_running is None:
        started_running = Event()
    if finished_running is None:
        finished_running = Event()
    if stop_running is None:
        stop_running = Event()
    realtime = Realtime(find_sequences, remote_api, realtime_cache)
    try:
        realtime = realtime_cache.load()
        realtime.inject(find_sequences, remote_api, realtime_cache)
    except (OSError, IOError):
        log.warning('No realtime state file found. This should only appear the first time realtime is run.')
    realtime.main(num_processes, process_from_queues, fetch_interval, tick_interval,
                  started_running, finished_running, stop_running)


class Realtime:
    def __init__(self, sequence_finder: SequenceFinder, remote_api: IExposureApi, local_db: DataCache):
        self.__construct()
        self.sequence_finder = sequence_finder
        self.remote_api = remote_api
        self.local_db = local_db
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
    def inject(self, sequence_finder: SequenceFinder, remote_api: IExposureApi, local_db: DataCache):
        self.sequence_finder = sequence_finder
        self.remote_api = remote_api
        self.local_db = local_db

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

    def main(self, num_processes: int, operation: ProcessFromQueues, fetch_interval: float, tick_interval: float,
             started_running: Event, finished_running: Event, stop_running: Event):
        started_processes = Value('i', 0)
        finished_processes = Value('i', 0)
        stop_signal = Event()
        with Pool(num_processes, operation, (self.exposure_in_queue, self.sequence_in_queue,
                                             self.exposure_out_queue, self.sequence_out_queue,
                                             started_processes, finished_processes, stop_signal)) as pool:
            # Wait for all child processes to start up before marking process as started (needed for tests)
            while started_processes.value < num_processes:
                time.sleep(tick_interval)
            started_running.set()
            pool.close()

            while not stop_running.is_set():
                self.__fetch_and_handle_new_exposures()
                fetch_time = time.time() + fetch_interval
                while time.time() < fetch_time:
                    self.__queue_tick()
                    time.sleep(tick_interval)

            # Wait for all child processes to wind down before returning (needed for tests)
            stop_signal.set()
            while finished_processes.value < num_processes:
                time.sleep(tick_interval)
        finished_running.set()

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

    def __queue_tick(self):
        updated = False
        while not self.sequence_out_queue.empty():
            sequence = self.sequence_out_queue.get(block=False)
            self.sequences_to_process.remove(sequence)
            updated = True
        while not self.exposure_out_queue.empty():
            exposure = self.exposure_out_queue.get(block=False)
            self.exposures_to_process.remove(exposure)
            self.sequence_mapper.mark_exposure_processed(exposure)
            sequence = self.sequence_mapper.get_sequence_if_ready_to_process(exposure)
            if sequence:
                self.sequence_in_queue.put(sequence)
                self.sequences_to_process.append(sequence)
                self.sequence_mapper.done_with_sequence(sequence)
            updated = True
        if updated:
            self.local_db.save(self)
