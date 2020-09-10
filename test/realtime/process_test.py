import time
from functools import partial
from multiprocessing import Event, Pool, Process, Queue

import pytest

from realtime.process import BlockingParams, RealtimeProcessor, init_realtime_process, process_from_queues
from test.realtime.helpers import MockCache, MockExposure, instant_log


@pytest.fixture
def realtime_processor(session_dir, mock_trigger):
    calibration_cache = MockCache()
    return RealtimeProcessor(mock_trigger, calibration_cache)


@pytest.fixture
def test_data(link_dir, night):
    return [MockExposure(link_dir, night, str(i) + '.fits') for i in range(0, 10)]


def test_process_queues_in_correct_order(realtime_processor, test_data):
    exposure_queue = Queue()
    sequence_queue = Queue()
    exposures_done = Queue()
    sequences_done = Queue()
    exposure_queue.put(test_data[0])
    exposure_queue.put(test_data[1])
    exposure_queue.put(test_data[2])
    exposure_queue.put(test_data[3])
    sequence_queue.put(test_data[4:8])
    time.sleep(0.1)

    r = realtime_processor.process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)
    assert r is True
    assert instant_log(exposures_done) == []
    assert instant_log(sequences_done) == [test_data[4:8]]
    r = realtime_processor.process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)
    assert r is True
    assert instant_log(exposures_done) == [test_data[0]]
    assert instant_log(sequences_done) == []
    r = realtime_processor.process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)
    assert r is True
    assert instant_log(exposures_done) == [test_data[1]]
    assert instant_log(sequences_done) == []

    sequence_queue.put(test_data[8:10])
    r = realtime_processor.process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)
    assert r is True
    assert instant_log(exposures_done) == []
    assert instant_log(sequences_done) == [test_data[8:10]]
    r = realtime_processor.process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)
    assert r is True
    assert instant_log(exposures_done) == [test_data[2]]
    assert instant_log(sequences_done) == []
    r = realtime_processor.process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)
    assert r is True
    assert instant_log(exposures_done) == [test_data[3]]
    assert instant_log(sequences_done) == []
    r = realtime_processor.process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)
    assert r is False


def test_process_queues_blocks_and_interruptable(realtime_processor, test_data):
    exposure_queue = Queue()
    sequence_queue = Queue()
    exposures_done = Queue()
    sequences_done = Queue()
    stop_signal = Event()
    blocking_params = BlockingParams(0.1, stop_signal)
    exposure_queue.put(test_data[0])
    sequence_queue.put(test_data[4:8])
    p = Process(target=realtime_processor.process_next_from_queue,
                args=(exposure_queue, sequence_queue, exposures_done, sequences_done, blocking_params))
    p.start()
    p.join()
    assert p.is_alive() is False
    assert instant_log(exposures_done) == []
    assert instant_log(sequences_done) == [test_data[4:8]]

    p = Process(target=realtime_processor.process_next_from_queue,
                args=(exposure_queue, sequence_queue, exposures_done, sequences_done, blocking_params))
    p.start()
    p.join()
    assert p.is_alive() is False
    assert instant_log(exposures_done) == [test_data[0]]
    assert instant_log(sequences_done) == []

    p = Process(target=realtime_processor.process_next_from_queue,
                args=(exposure_queue, sequence_queue, exposures_done, sequences_done, blocking_params))
    p.start()
    time.sleep(1.0)
    assert p.is_alive() is True
    assert instant_log(exposures_done) == []
    assert instant_log(sequences_done) == []

    exposure_queue.put(test_data[1])
    p.join()
    assert p.is_alive() is False
    assert instant_log(exposures_done) == [test_data[1]]
    assert instant_log(sequences_done) == []

    p = Process(target=realtime_processor.process_next_from_queue,
                args=(exposure_queue, sequence_queue, exposures_done, sequences_done, blocking_params))
    p.start()
    time.sleep(0.3)
    assert p.is_alive() is True
    stop_signal.set()
    p.join()
    assert p.is_alive() is False
    assert instant_log(exposures_done) == []
    assert instant_log(sequences_done) == []


@pytest.mark.parametrize('tasks_before_new_process', [None, 1])
def test_can_start_process_from_pool(realtime_processor, test_data, tasks_before_new_process):
    exposure_queue = Queue()
    sequence_queue = Queue()
    exposures_done = Queue()
    sequences_done = Queue()
    stop_signal = Event()
    results = []
    exposure_queue.put(test_data[0])
    exposure_queue.put(test_data[1])
    sequence_queue.put(test_data[4:8])
    p = Pool(1, init_realtime_process, (0.1, stop_signal,
                                        exposure_queue, sequence_queue, exposures_done, sequences_done),
             maxtasksperchild=tasks_before_new_process)

    process_from_queues_partial = partial(process_from_queues, realtime_processor)
    results.append(p.apply_async(process_from_queues_partial))
    for result in results:
        result.wait()

    results.append(p.apply_async(process_from_queues_partial))
    results.append(p.apply_async(process_from_queues_partial))
    for result in results:
        result.wait()

    results.append(p.apply_async(process_from_queues_partial))
    time.sleep(0.3)
    assert not results[-1].ready()
    stop_signal.set()
    results[-1].wait()
    assert results[-1].ready()

    assert instant_log(exposures_done) == test_data[0:2]
    assert instant_log(sequences_done) == [test_data[4:8]]
