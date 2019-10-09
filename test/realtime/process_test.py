from queue import Queue
from unittest import mock

import pytest

from realtime.process import RealtimeProcessor


@pytest.fixture
def realtime_processor(session_dir, mock_trigger):
    calib_cache = mock.MagicMock()
    return RealtimeProcessor(mock_trigger, session_dir, calib_cache, tick_interval=0)


@pytest.fixture
def test_data(session_dir):
    return [session_dir.joinpath(str(i)).with_suffix('.fits') for i in range(0, 10)]


def test_process_in_correct_order(realtime_processor, test_data):
    exposure_queue = Queue()
    sequence_queue = Queue()
    exposures_done = Queue()
    sequences_done = Queue()
    exposure_queue.put(test_data[0])
    exposure_queue.put(test_data[1])
    exposure_queue.put(test_data[2])
    exposure_queue.put(test_data[3])
    sequence_queue.put(test_data[4:8])

    realtime_processor.process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)
    assert exposures_done.empty()
    assert sequences_done.get() == test_data[4:8]
    realtime_processor.process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)
    assert exposures_done.get() == test_data[0]
    assert sequences_done.empty()
    realtime_processor.process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)
    assert exposures_done.get() == test_data[1]
    assert sequences_done.empty()

    sequence_queue.put(test_data[8:10])
    realtime_processor.process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)
    assert exposures_done.empty()
    assert sequences_done.get() == test_data[8:10]
    realtime_processor.process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)
    assert exposures_done.get() == test_data[2]
    assert sequences_done.empty()
    realtime_processor.process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)
    assert exposures_done.get() == test_data[3]
    assert sequences_done.empty()
