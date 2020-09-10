import pytest
from astropy.io import fits

from trigger.basedrstrigger import BaseDrsTrigger
from trigger.common.pathhandler import Exposure, RootDataDirectories


@pytest.fixture(autouse=True, scope='session')
def setup_data_dirs(data_dir, night):
    RootDataDirectories.input = data_dir.joinpath('raw')
    RootDataDirectories.temp = data_dir.joinpath('preprocessed')
    RootDataDirectories.reduced = data_dir.joinpath('reduced')
    RootDataDirectories.input.mkdir()
    RootDataDirectories.input.joinpath(night).mkdir()


def create_test_file(night, filename, index, count, dprtype=None):
    hdu = fits.PrimaryHDU()
    if index:
        hdu.header['CMPLTEXP'] = index
    if count:
        hdu.header['NEXP'] = count
    if dprtype:
        hdu.header['DPRTYPE'] = dprtype
    exposure = Exposure(night, filename)
    if dprtype:
        hdu.writeto(exposure.preprocessed)
    else:
        hdu.writeto(exposure.raw)


@pytest.fixture(scope='session')
def generate_test_data(night):
    create_test_file(night, '1.fits', 1, 1)
    create_test_file(night, '2.fits', 1, 3)
    create_test_file(night, '3.fits', 2, 3)
    create_test_file(night, '4.fits', 3, 3)
    create_test_file(night, '5.fits', 1, 4)
    create_test_file(night, '6.fits', 1, 4)
    create_test_file(night, '7.fits', 2, 4)
    create_test_file(night, '8.fits', 1, 4)
    create_test_file(night, '9.fits', 3, 4)
    create_test_file(night, '10.fits', 4, 4)
    create_test_file(night, '11.fits', 1, 4)
    create_test_file(night, '12.fits', 2, 4)
    create_test_file(night, '13.fits', 3, 4)
    create_test_file(night, '14.fits', 4, 4)
    create_test_file(night, '15.fits', 1, 4)


@pytest.fixture(scope='session')
def night():
    return 'test'


@pytest.fixture(scope='session')
def exposures(night):
    return [Exposure(night, str(i) + '.fits') for i in range(1, 16)]


def sequences_to_names(sequences):
    return list(map(lambda seq: list(map(lambda exp: exp.raw.name, seq)), sequences))


@pytest.mark.usefixtures('generate_test_data')
def test_find_sequences(exposures):
    sequences = BaseDrsTrigger.find_sequences(exposures)
    assert sequences_to_names(sequences) == [
        ['1.fits'],
        ['2.fits', '3.fits', '4.fits'],
        ['5.fits'],
        ['6.fits', '7.fits'],
        ['8.fits', '9.fits', '10.fits'],
        ['11.fits', '12.fits', '13.fits', '14.fits'],
        ['15.fits'],
    ]


@pytest.mark.usefixtures('generate_test_data')
def test_find_sequences_without_incomplete_last(exposures):
    sequences = BaseDrsTrigger.find_sequences(exposures, ignore_incomplete_last=True)
    assert sequences_to_names(sequences) == [
        ['1.fits'],
        ['2.fits', '3.fits', '4.fits'],
        ['5.fits'],
        ['6.fits', '7.fits'],
        ['8.fits', '9.fits', '10.fits'],
        ['11.fits', '12.fits', '13.fits', '14.fits'],
    ]


@pytest.mark.usefixtures('generate_test_data')
def test_find_sequences_without_incomplete(exposures):
    sequences = BaseDrsTrigger.find_sequences(exposures, ignore_incomplete=True)
    assert sequences_to_names(sequences) == [
        ['1.fits'],
        ['2.fits', '3.fits', '4.fits'],
        ['8.fits', '9.fits', '10.fits'],  # This is still returned even though an exposure was skipped... is that good?
        ['11.fits', '12.fits', '13.fits', '14.fits'],
    ]
