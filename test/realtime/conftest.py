import pytest

from realtime.localdb import DataCache
from test.realtime.helpers import MockApi, MockTrigger


@pytest.fixture(scope='session')
def session_dir(data_dir):
    return data_dir.joinpath('session')


@pytest.fixture(scope='session')
def link_dir(data_dir):
    return data_dir.joinpath('links')


@pytest.fixture(scope='module')
def realtime_cache(cache_dir):
    return DataCache(cache_dir.joinpath('.drstrigger.test.cache'))


@pytest.fixture
def remote_api():
    return MockApi()


@pytest.fixture
def mock_trigger(link_dir):
    return MockTrigger(link_dir)


@pytest.fixture
def night():
    return 'test'
