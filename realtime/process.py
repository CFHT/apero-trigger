from __future__ import annotations

import queue
import time
from multiprocessing import Event, Queue, current_process
from typing import Optional, Sequence

from logger import log
from trigger.baseinterface.drstrigger import ICalibrationState, IDrsTrigger
from trigger.baseinterface.exposure import IExposure
from .localdb import DataCache
from .typing import BlockingParams, ExposureQueue, IRealtimeProcessor, InitProcess, ProcessFromQueues, SequenceQueue

CalibrationStateCache = DataCache[ICalibrationState]


def init_realtime_process(retry_interval: float, stop_signal: Event,
                          exposure_queue: ExposureQueue, sequence_queue: SequenceQueue,
                          exposures_done: ExposureQueue, sequences_done: SequenceQueue):
    global exposure_queue_global
    global sequence_queue_global
    global exposures_done_global
    global sequences_done_global
    global blocking_params_global
    exposure_queue_global = exposure_queue
    sequence_queue_global = sequence_queue
    exposures_done_global = exposures_done
    sequences_done_global = sequences_done
    blocking_params_global = BlockingParams(retry_interval, stop_signal)
    log.info('Started process %i', current_process().pid)


def process_from_queues(realtime_processor) -> bool:
    realtime_processor.process_id = current_process().pid
    return realtime_processor.process_next_from_queue(exposure_queue_global, sequence_queue_global,
                                                      exposures_done_global, sequences_done_global,
                                                      blocking_params_global)


# Just here for static typechecking, calling this is pointless
# Doesn't really seem to work in PyCharm... maybe try MyPy?
def __typechecking():
    init_process_check: InitProcess = init_realtime_process
    process_from_queues_check: ProcessFromQueues = process_from_queues
    assert init_process_check == init_realtime_process
    assert process_from_queues_check == process_from_queues


class RealtimeProcessor(IRealtimeProcessor):
    def __init__(self, trigger: IDrsTrigger, calibration_cache: CalibrationStateCache):
        super().__init__()
        self.trigger = trigger
        self.calibration_cache = calibration_cache

    def process_next_from_queue(self, exposure_queue: Queue[IExposure], sequence_queue: Queue[Sequence[IExposure]],
                                exposures_done: Queue[IExposure], sequences_done: Queue[Sequence[IExposure]],
                                block: Optional[BlockingParams] = None) -> bool:
        if block:
            while not block.stop_signal.is_set():
                result = self.__process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)
                if result:
                    return result
                time.sleep(block.retry_interval)
            return False
        else:
            return self.__process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)

    def __process_next_from_queue(self, exposure_queue: Queue[IExposure], sequence_queue: Queue[Sequence[IExposure]],
                                  exposures_done: Queue[IExposure], sequences_done: Queue[Sequence[IExposure]]) -> bool:
        try:
            sequence = sequence_queue.get(block=False)
        except queue.Empty:
            try:
                exposure = exposure_queue.get(block=False)
            except queue.Empty:
                pass
            else:
                log.info('Process %i processing %s', self.process_id, exposure)
                try:
                    self.__process_exposure(exposure)
                except:
                    log.error('An error occurred while processing %s', exposure, exc_info=True)
                exposures_done.put(exposure)
                return True
        else:
            log.info('Process %i processing %s', self.process_id, sequence)
            try:
                self.__process_sequence(sequence)
            except:
                log.error('An error occurred while processing %s', sequence, exc_info=True)
            sequences_done.put(sequence)
            return True
        return False

    def __process_exposure(self, exposure: IExposure):
        if self.trigger.preprocess(exposure):
            self.trigger.process_file(exposure)

    def __process_sequence(self, sequence: Sequence[IExposure]):
        try:
            self.trigger.calibration_state = self.calibration_cache.load()
        except (OSError, IOError):
            log.warning('Calibration state file %s not found. This should only appear the first time realtime is run.',
                        self.calibration_cache.cache_file)
        result = self.trigger.process_sequence(sequence)
        # We only save the calibration state if the sequence was a calibration sequence.
        if result and 'calibrations_complete' in result:
            if result.get('calibrations_complete'):
                self.trigger.reset_calibration_state()
            self.calibration_cache.save(self.trigger.calibration_state)
        else:
            self.calibration_cache.unlock()
