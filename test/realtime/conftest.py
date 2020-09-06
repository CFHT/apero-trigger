import pytest

from realtime.localdb import DataCache
from test.realtime.helpers import MockApi, MockTrigger


@pytest.fixture(scope='session')
def session_dir(data_dir):
    return data_dir.joinpath('session')


@pytest.fixture(scope='session')
def link_dir(data_dir):
    return data_dir.joinpath('links')


@pytest.fixture(scope='session')
def realtime_cache(cache_dir):
    return DataCache(cache_dir.joinpath('.drstrigger.realtime.cache'))


@pytest.fixture(scope='session')
def calibration_cache(cache_dir):
    return DataCache(cache_dir.joinpath('.drstrigger.calib.cache'))


@pytest.fixture
def remote_api(mock_trigger):
    return MockApi(mock_trigger)


@pytest.fixture
def mock_trigger(link_dir, session_dir):
    return MockTrigger(link_dir, session_dir)


@pytest.fixture
def night():
    return 'test'


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
