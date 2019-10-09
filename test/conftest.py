import pytest

from logger import configure_logger

configure_logger()


@pytest.fixture(scope='session')
def cache_dir(tmp_path_factory):
    return tmp_path_factory.mktemp('cache')


@pytest.fixture(scope='session')
def data_dir(tmp_path_factory):
    return tmp_path_factory.mktemp('data')
