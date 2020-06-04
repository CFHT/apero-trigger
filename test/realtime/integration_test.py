import time
from unittest import mock

import pytest

from realtime.apibridge import ApiBridge
from realtime.manager import start_realtime
from realtime.process import RealtimeProcessor
from test.realtime.helpers import ProcessRunner, TriggerActionT, files_as_exposures, last_index
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


class MockCache(mock.MagicMock):
    def __reduce__(self):
        return MockCache, ()


@pytest.fixture
def processor(mock_trigger, session_dir):
    calibration_cache = MockCache()
    return RealtimeProcessor(mock_trigger, calibration_cache, 0.02)


@pytest.fixture
def api_bridge(remote_api, session_dir, mock_trigger):
    return ApiBridge(remote_api.new_exposures, session_dir, mock_trigger)


@pytest.fixture
def realtime_starter(api_bridge, realtime_cache, processor):
    return ProcessRunner(target=start_realtime, args=(mock_sequence_finder, api_bridge, realtime_cache,
                                                      processor.process_from_queues, 4, 1.0, 0.1))


def consistency_check(log, exposures):
    sequences = mock_sequence_finder(exposures)
    log.flush()
    assert len(set(log.data)) == 2 * len(exposures) + len(sequences)
    for exposure in exposures:
        preprocess_entry = (TriggerActionT.PREPROCESS, exposure)
        assert preprocess_entry in log.data
        exposure_entry = (TriggerActionT.PROCESS_FILE, exposure)
        assert exposure_entry in log.data
        assert log.data.index(preprocess_entry) < log.data.index(exposure_entry)
        assert last_index(log.data, preprocess_entry) < last_index(log.data, exposure_entry)
    for sequence in sequences[0:1]:
        sequence_entry = (TriggerActionT.PROCESS_SEQUENCE, sequence)
        assert sequence_entry in log.data
        sequence_index = log.data.index(sequence_entry)
        for exposure in sequence:
            last_exposure_index = last_index(log.data, (TriggerActionT.PROCESS_FILE, exposure))
            assert last_exposure_index < sequence_index


def test_realtime(realtime_starter, remote_api, mock_trigger, test_data, night, link_dir, end_safe):
    remote_api.add_new_exposures(test_data[:7])
    p = realtime_starter.start()
    time.sleep(1)
    remote_api.add_new_exposures(test_data[7:])
    time.sleep(0.1)
    p.stop(block=end_safe)
    if not end_safe:
        mock_trigger.log.flush()
    p = realtime_starter.start()
    time.sleep(1)
    p.stop(block=end_safe)
    consistency_check(mock_trigger.log, files_as_exposures(link_dir, night, test_data))
