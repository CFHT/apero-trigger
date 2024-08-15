from __future__ import annotations

import queue
import time
from multiprocessing import Event, Queue, current_process
from typing import Optional

from logger import log
from trigger import CfhtTrigger
from trigger.common.pathhandler import Exposure
from .typing import BlockingParams, ExposureQueue, IRealtimeProcessor, InitProcess, ProcessFromQueues


def init_realtime_process(retry_interval: float, stop_signal: Event,
                          exposure_queue: ExposureQueue,
                          exposures_done: ExposureQueue):
    global exposure_queue_global
    global exposures_done_global
    global blocking_params_global
    exposure_queue_global = exposure_queue
    exposures_done_global = exposures_done
    blocking_params_global = BlockingParams(retry_interval, stop_signal)
    log.info('Started process %i', current_process().pid)


def process_from_queues(realtime_processor) -> bool:
    realtime_processor.process_id = current_process().pid
    return realtime_processor.process_next_from_queue(exposure_queue_global,
                                                      exposures_done_global,
                                                      blocking_params_global)


# Just here for static typechecking, calling this is pointless
# Doesn't really seem to work in PyCharm... maybe try MyPy?
def __typechecking():
    init_process_check: InitProcess = init_realtime_process
    process_from_queues_check: ProcessFromQueues = process_from_queues
    assert init_process_check == init_realtime_process
    assert process_from_queues_check == process_from_queues


class RealtimeProcessor(IRealtimeProcessor):
    def __init__(self, trigger: CfhtTrigger):
        super().__init__()
        self.trigger = trigger

    def process_next_from_queue(self, exposure_queue: Queue[Exposure],
                                exposures_done: Queue[Exposure],
                                block: Optional[BlockingParams] = None) -> bool:
        if block:
            while not block.stop_signal.is_set():
                result = self.__process_next_from_queue(exposure_queue, exposures_done)
                if result:
                    return result
                time.sleep(block.retry_interval)
            return False
        else:
            return self.__process_next_from_queue(exposure_queue, exposures_done)

    def __process_next_from_queue(self, exposure_queue: Queue[Exposure],
                                  exposures_done: Queue[Exposure]) -> bool:
        try:
            exposure = exposure_queue.get(block=False)
        except queue.Empty:
            pass
        else:
            log.info('Process %i processing %s', self.process_id, exposure)
            try:
                if self.trigger.preprocess(exposure):
                    self.trigger.process_file(exposure)
            except:
                log.error('An error occurred while processing %s', exposure, exc_info=True)
            exposures_done.put(exposure)
            return True
        return False
