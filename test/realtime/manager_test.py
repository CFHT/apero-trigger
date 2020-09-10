import queue
import time
from enum import Enum
from functools import partial
from threading import Timer
from typing import Optional

import pytest

from realtime.typing import BlockingParams, ExposureQueue, IRealtimeProcessor, SequenceQueue
from test.realtime.helpers import Log, LogActions, MockExposureMetadata, StartRealtimeParams, \
    consistency_check_general, mock_sequence_finder, start_realtime_blocking_until_n_finish


class ProcessorActionT(Enum):
    STARTED = 'started'
    FINISHED = 'finished'
    SEQUENCE = 'sequence'

    def __repr__(self):
        return self.value


class MockProcessor(IRealtimeProcessor):
    def __init__(self, log):
        super().__init__()
        self.log = log
        self.process_id = 0

    def process_next_from_queue(self, exposure_queue: ExposureQueue, sequence_queue: SequenceQueue,
                                exposures_done: ExposureQueue, sequences_done: SequenceQueue,
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

    def __process_next_from_queue(self, exposure_queue, sequence_queue, exposures_done, sequences_done):
        try:
            sequence = sequence_queue.get(block=False)
        except queue.Empty:
            try:
                exposure = exposure_queue.get(block=False)
            except queue.Empty:
                pass
            else:
                self.log.put((ProcessorActionT.STARTED, exposure))
                time.sleep(MockExposureMetadata(exposure).time)
                self.log.put((ProcessorActionT.FINISHED, exposure))
                exposures_done.put(exposure)
                return True
        else:
            self.log.put((ProcessorActionT.SEQUENCE, sequence))
            sequences_done.put(sequence)
            return True
        return False


@pytest.fixture
def check_data(test_data, mock_trigger):
    return [mock_trigger.exposure_from_path(f) for f in test_data]


@pytest.fixture
def mock_processor():
    return MockProcessor(Log())


@pytest.fixture
def realtime_params():
    return StartRealtimeParams(4, 0.5, 0.05, 0.01)


log_actions = LogActions(ProcessorActionT.STARTED, ProcessorActionT.FINISHED, ProcessorActionT.SEQUENCE)

consistency_check = partial(consistency_check_general, log_actions=log_actions)


def test_realtime_data_before(remote_api, realtime_cache, mock_processor, realtime_params, test_data, check_data):
    remote_api.add_new_exposures(test_data)
    start_realtime_blocking_until_n_finish(remote_api, realtime_cache, mock_processor, realtime_params, 19)
    consistency_check(mock_processor.log, check_data)


def test_realtime_data_after(remote_api, realtime_cache, mock_processor, realtime_params, test_data, check_data):
    Timer(1.5, partial(remote_api.add_new_exposures, test_data)).start()
    start_realtime_blocking_until_n_finish(remote_api, realtime_cache, mock_processor, realtime_params, 19)
    consistency_check(mock_processor.log, check_data)


def test_realtime_split_data(remote_api, realtime_cache, mock_processor, realtime_params, test_data, check_data):
    remote_api.add_new_exposures(test_data[:12])
    Timer(1.5, partial(remote_api.add_new_exposures, test_data[12:])).start()
    start_realtime_blocking_until_n_finish(remote_api, realtime_cache, mock_processor, realtime_params, 19)
    consistency_check(mock_processor.log, check_data)


@pytest.mark.parametrize('n', [0, 1, 8, 12, 18])
def test_realtime_save_and_load(remote_api, realtime_cache, mock_processor, realtime_params, test_data, check_data, n):
    remote_api.add_new_exposures(test_data)
    finished = start_realtime_blocking_until_n_finish(remote_api, realtime_cache, mock_processor, realtime_params, n)
    realtime_loaded = realtime_cache.load()
    realtime_loaded.inject(mock_sequence_finder, remote_api, realtime_cache, 0.01)
    start_realtime_blocking_until_n_finish(remote_api, realtime_cache, mock_processor, realtime_params, 19 - finished)
    consistency_check(mock_processor.log, check_data)
