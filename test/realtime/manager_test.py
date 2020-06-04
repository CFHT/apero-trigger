import queue
import time
from enum import Enum

import pytest

from realtime.manager import start_realtime
from test.realtime.helpers import Log, ProcessRunner, last_index


class MockExposureMetadata:
    def __init__(self, exposure):
        stem = exposure.raw.stem
        parts = stem.split('-')
        self.exposure = exposure
        self.group = parts[0]
        self.index = int(parts[1]) - 1
        self.count = int(parts[2])
        self.time = float(parts[3]) * 0.1


class ProcessorActionT(Enum):
    STARTED = 'started'
    FINISHED = 'finished'
    SEQUENCE = 'sequence'

    def __repr__(self):
        return self.value


class MockProcessor:
    def __init__(self, log, tick_interval):
        self.log = log
        self.tick_interval = tick_interval

    def process_from_queues(self, exposure_queue, sequence_queue, exposures_done, sequences_done,
                            started_processes, finished_processes, stop_signal):
        with started_processes.get_lock():
            started_processes.value += 1
        while not stop_signal.is_set():
            self.process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)
            time.sleep(self.tick_interval)
        with finished_processes.get_lock():
            finished_processes.value += 1

    def process_next_from_queue(self, exposure_queue, sequence_queue, exposures_done, sequences_done):
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
        else:
            self.log.put((ProcessorActionT.SEQUENCE, sequence))
            sequences_done.put(sequence)


def mock_sequence_finder(exposures):
    sequences = []
    groups = {}
    for exposure in exposures:
        exposure_metadata = MockExposureMetadata(exposure)
        assert exposure_metadata.index < exposure_metadata.count
        if exposure_metadata.group in groups:
            sequence = groups[exposure_metadata.group]
            assert sequence[-1].count == exposure_metadata.count
            assert all(existing_exposure.index != exposure_metadata.index for existing_exposure in sequence)
            sequence.append(exposure_metadata)
        else:
            sequence = [exposure_metadata]
            groups[exposure_metadata.group] = sequence
        if len(sequence) == exposure_metadata.count:
            sequence.sort(key=lambda exp: exp.index)
            sequences.append(tuple(map(lambda exp: exp.exposure, sequence)))
    return sequences


@pytest.fixture
def test_data():
    return [
        'a-1-4-10',
        'a-2-4-9',
        'a-3-4-8',
        'a-4-4-8',
        'e-1-1-2',
        'b-4-4-1',
        'b-2-4-1',
        'b-1-4-1',
        'c-2-2-2',
        'b-3-4-1',
        'd-1-1-2',
        'c-1-2-2',
        'f-1-1-1',
    ]


@pytest.fixture
def check_data(test_data, mock_trigger):
    return [mock_trigger.Exposure('', f) for f in test_data]


def start_realtime_process(api, cache, processor):
    return ProcessRunner(target=start_realtime, args=(mock_sequence_finder, api, cache, processor.process_from_queues,
                                                      4, 0.5, 0.05)).start()


@pytest.fixture
def mock_processor():
    return MockProcessor(Log(), 0.01)


def consistency_check(log, exposures):
    sequences = mock_sequence_finder(exposures)
    log.flush()
    assert len(set(log.data)) == 2 * len(exposures) + len(sequences)
    for exposure in exposures:
        start_entry = (ProcessorActionT.STARTED, exposure)
        assert start_entry in log.data
        finish_entry = (ProcessorActionT.FINISHED, exposure)
        assert finish_entry in log.data
        assert log.data.index(start_entry) < log.data.index(finish_entry)
        assert last_index(log.data, start_entry) < last_index(log.data, finish_entry)
    for sequence in sequences[0:1]:
        sequence_entry = (ProcessorActionT.SEQUENCE, sequence)
        assert sequence_entry in log.data
        sequence_index = log.data.index(sequence_entry)
        for exposure in sequence:
            finish_index = last_index(log.data, (ProcessorActionT.FINISHED, exposure))
            assert finish_index < sequence_index


def test_realtime_data_before(remote_api, realtime_cache, mock_processor, test_data, check_data, end_safe):
    remote_api.add_new_exposures(test_data)
    p = start_realtime_process(remote_api, realtime_cache, mock_processor)
    time.sleep(3.0)
    p.stop(block=end_safe)
    consistency_check(mock_processor.log, check_data)


def test_realtime_data_after(remote_api, realtime_cache, mock_processor, test_data, check_data, end_safe):
    p = start_realtime_process(remote_api, realtime_cache, mock_processor)
    time.sleep(0.2)
    remote_api.add_new_exposures(test_data)
    time.sleep(2.8)
    p.stop(block=end_safe)
    consistency_check(mock_processor.log, check_data)


def test_realtime_split_data(remote_api, realtime_cache, mock_processor, test_data, check_data, end_safe):
    remote_api.add_new_exposures(test_data[:12])
    p = start_realtime_process(remote_api, realtime_cache, mock_processor)
    time.sleep(1.5)
    remote_api.add_new_exposures(test_data[12:])
    time.sleep(1.5)
    p.stop(block=end_safe)
    consistency_check(mock_processor.log, check_data)


@pytest.mark.parametrize('kill_time', [0.5, 0.8, 1.0, 1.2, 2.0])
def test_realtime_save_and_load(remote_api, realtime_cache, mock_processor, test_data, check_data, end_safe, kill_time):
    remote_api.add_new_exposures(test_data)
    p = start_realtime_process(remote_api, realtime_cache, mock_processor)
    time.sleep(kill_time)
    p.stop(block=end_safe)
    if not end_safe:
        mock_processor.log.flush()
    realtime_loaded = realtime_cache.load()
    realtime_loaded.inject(mock_sequence_finder, remote_api, realtime_cache)
    p = start_realtime_process(remote_api, realtime_cache, mock_processor)
    time.sleep(3.0 - kill_time)
    p.stop(block=end_safe)
    consistency_check(mock_processor.log, check_data)
