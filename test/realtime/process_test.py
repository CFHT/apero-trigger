import time
from multiprocessing import Event, Process, Queue, Value

import pytest

from realtime.process import RealtimeProcessor
from test.realtime.helpers import Log, MockCache, MockExposure


@pytest.fixture
def realtime_processor(session_dir, mock_trigger):
    calibration_cache = MockCache()
    return RealtimeProcessor(mock_trigger, calibration_cache, tick_interval=0.01)


@pytest.fixture
def test_data(link_dir, night):
    return [MockExposure(link_dir, night, str(i) + '.fits') for i in range(0, 10)]


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


def test_process_starts_and_stops(realtime_processor, test_data):
    exposure_queue = Queue()
    sequence_queue = Queue()
    exposures_done = Queue()
    sequences_done = Queue()
    started_processes = Value('i', 0)
    finished_processes = Value('i', 0)
    stop_signal = Event()
    exposure_log = Log(exposures_done)
    sequence_log = Log(sequences_done)
    exposure_queue.put(test_data[0])
    exposure_queue.put(test_data[1])
    exposure_queue.put(test_data[2])
    exposure_queue.put(test_data[3])
    sequence_queue.put(test_data[4:8])
    assert started_processes.value == 0
    p = Process(target=realtime_processor.process_from_queues,
                args=(exposure_queue, sequence_queue, exposures_done, sequences_done,
                      started_processes, finished_processes, stop_signal))
    p.start()
    while started_processes.value == 0:
        pass
    time.sleep(0.1)
    stop_signal.set()
    sequence_queue.put(test_data[8:10])
    while finished_processes.value > 0:
        pass
    exposure_log.flush()
    sequence_log.flush()
    assert(exposure_log.data == test_data[0:4])
    assert(sequence_log.data == [test_data[4:8]])
