import queue
import time
from enum import Enum
from pathlib import Path

import pytest

from realtime.manager import Realtime
from test.realtime.helpers import Log, last_index, ProcessRunner


class MockExposure:
    def __init__(self, full_name):
        stem = Path(full_name).stem
        parts = stem.split('-')
        self.name = full_name
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
    def __init__(self, log):
        self.log = log

    def process_from_queues(self, exposure_queue, sequence_queue, exposures_done, sequences_done):
        while True:
            self.process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)

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
                time.sleep(MockExposure(exposure).time)
                self.log.put((ProcessorActionT.FINISHED, exposure))
                exposures_done.put(exposure)
        else:
            self.log.put((ProcessorActionT.SEQUENCE, sequence))
            sequences_done.put(sequence)


def mock_sequence_finder(exposures):
    sequences = []
    groups = {}
    for raw_exposure in exposures:
        exposure = MockExposure(raw_exposure)
        assert exposure.index < exposure.count
        if exposure.group in groups:
            sequence = groups[exposure.group]
            assert sequence[-1].count == exposure.count
            assert all(existing_exposure.index != exposure.index for existing_exposure in sequence)
            sequence.append(exposure)
        else:
            sequence = [exposure]
            groups[exposure.group] = sequence
        if len(sequence) == exposure.count:
            sequence.sort(key=lambda exp: exp.index)
            sequences.append(tuple(map(lambda exp: exp.name, sequence)))
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


def RealtimeProcess(realtime, processor):
    return ProcessRunner(target=realtime.main, args=(4, processor.process_from_queues, 0.5, 0.05)).start()


@pytest.fixture
def realtime_fresh(remote_api, realtime_cache):
    return Realtime(mock_sequence_finder, remote_api, realtime_cache)


@pytest.fixture
def mock_processor():
    return MockProcessor(Log())


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


def test_realtime_data_before(realtime_fresh, remote_api, mock_processor, test_data):
    remote_api.add_new_exposures(test_data)
    p = RealtimeProcess(realtime_fresh, mock_processor)
    time.sleep(3.0)
    p.stop()
    consistency_check(mock_processor.log, test_data)


def test_realtime_data_after(realtime_fresh, remote_api, mock_processor, test_data):
    p = RealtimeProcess(realtime_fresh, mock_processor)
    time.sleep(0.2)
    remote_api.add_new_exposures(test_data)
    time.sleep(2.8)
    p.stop()
    consistency_check(mock_processor.log, test_data)


def test_realtime_split_data(realtime_fresh, remote_api, mock_processor, test_data):
    remote_api.add_new_exposures(test_data[:12])
    p = RealtimeProcess(realtime_fresh, mock_processor)
    time.sleep(1.5)
    remote_api.add_new_exposures(test_data[12:])
    time.sleep(1.5)
    p.stop()
    consistency_check(mock_processor.log, test_data)


@pytest.mark.parametrize('death_time', [0.5, 0.8, 1.0, 1.2, 2.0])
def test_realtime_save_and_load(realtime_fresh, remote_api, mock_processor, test_data, death_time):
    remote_api.add_new_exposures(test_data)
    p = RealtimeProcess(realtime_fresh, mock_processor)
    time.sleep(death_time)
    p.stop()
    realtime_cache = realtime_fresh.local_db
    realtime_loaded = realtime_cache.load()
    realtime_loaded.inject(mock_sequence_finder, remote_api, realtime_cache)
    p = RealtimeProcess(realtime_loaded, mock_processor)
    time.sleep(3.0 - death_time)
    p.stop()
    consistency_check(mock_processor.log, test_data)
