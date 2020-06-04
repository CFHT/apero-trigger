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
    return DataCache(cache_dir.joinpath('.drstrigger.test.cache'))


@pytest.fixture
def remote_api(mock_trigger):
    return MockApi(mock_trigger)


@pytest.fixture
def mock_trigger(link_dir):
    return MockTrigger(link_dir)


@pytest.fixture
def night():
    return 'test'


@pytest.fixture
def end_safe():
    """
    For some unknown after the process completes, data that was previously in a queue can disappear.
    We set this to False to grab data from queues without waiting from spawned process to complete.
    This issue might be platform dependent?
    """
    return False
