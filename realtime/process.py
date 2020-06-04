from __future__ import annotations

import queue
import time
from multiprocessing import Event, Queue, Value
from typing import Sequence

from logger import log
from trigger.baseinterface.drstrigger import IDrsTrigger
from trigger.baseinterface.exposure import IExposure
from .localdb import DataCache


class RealtimeProcessor:
    def __init__(self, trigger: IDrsTrigger, calibration_cache: DataCache, tick_interval: float):
        self.trigger = trigger
        self.calibration_cache = calibration_cache
        self.tick_interval = tick_interval

    def process_from_queues(self, exposure_queue: Queue[IExposure], sequence_queue: Queue[Sequence[IExposure]],
                            exposures_done: Queue[IExposure], sequences_done: Queue[Sequence[IExposure]],
                            started_processes: Value, finished_processes: Value, stop_signal: Event):
        with started_processes.get_lock():
            started_processes.value += 1
        while not stop_signal.is_set():
            self.process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)
            time.sleep(self.tick_interval)
        with finished_processes.get_lock():
            finished_processes.value += 1

    def process_next_from_queue(self, exposure_queue: Queue[IExposure], sequence_queue: Queue[Sequence[IExposure]],
                                exposures_done: Queue[IExposure], sequences_done: Queue[Sequence[IExposure]]):
        try:
            sequence = sequence_queue.get(block=False)
        except queue.Empty:
            try:
                exposure = exposure_queue.get(block=False)
            except queue.Empty:
                pass
            else:
                log.info('Processing %s', exposure)
                try:
                    self.__process_exposure(exposure)
                except:
                    log.error('An error occurred while processing %s', exposure, exc_info=True)
                exposures_done.put(exposure)
        else:
            log.info('Processing %s', sequence)
            try:
                self.__process_sequence(sequence)
            except:
                log.error('An error occurred while processing %s', sequence, exc_info=True)
            sequences_done.put(sequence)

    def __process_exposure(self, exposure: IExposure):
        if self.trigger.preprocess(exposure):
            self.trigger.process_file(exposure)

    def __process_sequence(self, sequence: Sequence[IExposure]):
        try:
            self.trigger.calibration_state = self.calibration_cache.load()
        except (OSError, IOError):
            log.warning('No calibration state file found. This should only appear the first time realtime is run.')
        result = self.trigger.process_sequence(sequence)
        # We only save the calibration state if the sequence was a calibration sequence.
        if result and 'calibrations_complete' in result:
            self.calibration_cache.save(self.trigger.calibration_state)
