from functools import partial
from pathlib import Path
from threading import Timer

import pytest

from realtime.apibridge import ApiBridge
from realtime.process import RealtimeProcessor
from test.realtime.helpers import LogActions, StartRealtimeParams, TriggerActionT, consistency_check_general, \
    start_realtime_blocking_until_n_finish


@pytest.fixture
def processor(mock_trigger, calibration_cache):
    return RealtimeProcessor(mock_trigger, calibration_cache)


@pytest.fixture
def api_bridge(remote_api, mock_trigger):
    return ApiBridge(remote_api.new_exposures, mock_trigger)


@pytest.fixture
def check(test_data, night, mock_trigger):
    return [mock_trigger.exposure(night, Path(f).name) for f in test_data]


@pytest.fixture
def realtime_params():
    return StartRealtimeParams(4, 1.0, 0.1, 0.02)


log_actions = LogActions(TriggerActionT.PREPROCESS, TriggerActionT.PROCESS_FILE, TriggerActionT.PROCESS_SEQUENCE)

consistency_check = partial(consistency_check_general, log_actions=log_actions)


def test_realtime(remote_api, api_bridge, realtime_cache, processor, realtime_params, mock_trigger, test_data, check):
    remote_api.add_new_exposures(test_data[:7])

    Timer(1, partial(remote_api.add_new_exposures, test_data[7:])).start()
    finished = start_realtime_blocking_until_n_finish(api_bridge, realtime_cache, processor, realtime_params, 5)

    start_realtime_blocking_until_n_finish(api_bridge, realtime_cache, processor, realtime_params, 19 - finished)
    consistency_check(mock_trigger.log, check)
