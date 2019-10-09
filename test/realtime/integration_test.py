import time
from unittest import mock

import pytest

from realtime.process import RealtimeProcessor
from realtime.starter import start_realtime
from test.realtime.helpers import ProcessRunner, TriggerActionT, last_index
from test.realtime.manager_test import mock_sequence_finder


@pytest.fixture
def test_data(session_dir, night):
    stems = [
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
    return [session_dir.joinpath(night, stem).with_suffix('.fits') for stem in stems]


@pytest.fixture
def processor(mock_trigger, session_dir):
    calibration_cache = mock.MagicMock()
    return RealtimeProcessor(mock_trigger, session_dir, calibration_cache, 0.02)


@pytest.fixture
def trigger_log(mock_trigger):
    return mock_trigger.log


@pytest.fixture
def realtime_starter(remote_api, realtime_cache, processor):
    num_processes = 4
    return ProcessRunner(target=start_realtime, args=(mock_sequence_finder, remote_api, realtime_cache,
                                                      processor.process_from_queues, num_processes, 1, 0.1))


def consistency_check(log, night, exposures):
    sequences = mock_sequence_finder(exposures)
    log.flush()
    assert len(set(log.data)) == 2 * len(exposures) + len(sequences)
    for exposure in exposures:
        preprocess_entry = (TriggerActionT.PREPROCESS, night, exposure)
        assert preprocess_entry in log.data
        exposure_entry = (TriggerActionT.PROCESS_FILE, night, exposure)
        assert exposure_entry in log.data
        assert log.data.index(preprocess_entry) < log.data.index(exposure_entry)
        assert last_index(log.data, preprocess_entry) < last_index(log.data, exposure_entry)
    for sequence in sequences[0:1]:
        sequence_entry = (TriggerActionT.PROCESS_SEQUENCE, night, sequence)
        assert sequence_entry in log.data
        sequence_index = log.data.index(sequence_entry)
        for exposure in sequence:
            last_exposure_index = last_index(log.data, (TriggerActionT.PROCESS_FILE, night, exposure))
            assert last_exposure_index < sequence_index


def test_realtime(realtime_starter, remote_api, trigger_log, test_data, night):
    remote_api.add_new_exposures(test_data[:7])
    p = realtime_starter.start()
    time.sleep(1)
    remote_api.add_new_exposures(test_data[7:])
    time.sleep(0.1)
    p.stop()
    p = realtime_starter.start()
    time.sleep(1)
    p.stop()
    consistency_check(trigger_log, night, [path.name for path in test_data])
