import pytest
from astropy.io import fits

from trigger.basedrstrigger import BaseDrsTrigger
from trigger.pathhandler import Exposure, RootDirectories


@pytest.fixture(autouse=True, scope='session')
def setup_data_dirs(data_dir, night):
    RootDirectories.input = data_dir.joinpath('raw')
    RootDirectories.temp = data_dir.joinpath('preprocessed')
    RootDirectories.reduced = data_dir.joinpath('reduced')
    RootDirectories.input.mkdir()
    RootDirectories.input.joinpath(night).mkdir()


def create_test_file(filename, index, count, dprtype=None):
    hdu = fits.PrimaryHDU()
    if index:
        hdu.header['CMPLTEXP'] = index
    if count:
        hdu.header['NEXP'] = count
    if dprtype:
        hdu.header['DPRTYPE'] = dprtype
    exposure = Exposure('test', filename)
    if dprtype:
        hdu.writeto(exposure.preprocessed)
    else:
        hdu.writeto(exposure.raw)


@pytest.fixture(scope='session')
def generate_test_data():
    create_test_file('1.fits', 1, 1)
    create_test_file('2.fits', 1, 3)
    create_test_file('3.fits', 2, 3)
    create_test_file('4.fits', 3, 3)
    create_test_file('5.fits', 1, 4)
    create_test_file('6.fits', 1, 4)
    create_test_file('7.fits', 2, 4)
    create_test_file('8.fits', 1, 4)
    create_test_file('9.fits', 3, 4)
    create_test_file('10.fits', 4, 4)
    create_test_file('11.fits', 1, 4)
    create_test_file('12.fits', 2, 4)
    create_test_file('13.fits', 3, 4)
    create_test_file('14.fits', 4, 4)
    create_test_file('15.fits', 1, 4)


@pytest.fixture(scope='session')
def night():
    return 'test'


@pytest.fixture(scope='session')
def files():
    return [str(i) + '.fits' for i in range(1, 16)]


@pytest.mark.usefixtures('generate_test_data')
def test_find_sequences_fallback(night, files):
    sequences = BaseDrsTrigger.find_sequences_fallback(night, files)
    assert sequences == [
        ['1.fits'],
        ['2.fits', '3.fits', '4.fits'],
        ['5.fits'],
        ['6.fits', '7.fits'],
        ['8.fits', '9.fits', '10.fits'],
        ['11.fits', '12.fits', '13.fits', '14.fits'],
        ['15.fits'],
    ]


@pytest.mark.usefixtures('generate_test_data')
def test_find_sequences_fallback_without_incomplete_last(night, files):
    sequences = BaseDrsTrigger.find_sequences_fallback(night, files, ignore_incomplete_last=True)
    assert sequences == [
        ['1.fits'],
        ['2.fits', '3.fits', '4.fits'],
        ['5.fits'],
        ['6.fits', '7.fits'],
        ['8.fits', '9.fits', '10.fits'],
        ['11.fits', '12.fits', '13.fits', '14.fits'],
    ]


@pytest.mark.usefixtures('generate_test_data')
def test_find_sequences_fallback_without_incomplete(night, files):
    sequences = BaseDrsTrigger.find_sequences_fallback(night, files, ignore_incomplete=True)
    assert sequences == [
        ['1.fits'],
        ['2.fits', '3.fits', '4.fits'],
        ['8.fits', '9.fits', '10.fits'],  # This is still returned even though an exposure was skipped... is that good?
        ['11.fits', '12.fits', '13.fits', '14.fits'],
    ]
