from .exposureConfigMapper import ExposureConfig
from astropy.io import fits


class ActualExposureConfig(ExposureConfig):
    @classmethod
    def from_file(cls, filename):
        header = cls.__get_fits_header(filename)
        keyword_map = cls.__get_keyword_map()
        return cls.__get_exposure_config(header, keyword_map)

    @staticmethod
    def __get_fits_header(filename):
        try:
            return fits.open(filename)[0].header
        except:
            raise OpeningFITSError

    @staticmethod
    def __get_keyword_map():
        return ExposureConfig(
            calibwh='SBCALI_P',
            rhomb1='SBRHB1_P',
            rhomb2='SBRHB2_P',
            refout='SBCREF_P',
            cassout='SBCCAS_P',
            nexp='NEXP',
            exptime='EXPTIME')

    @staticmethod
    def __get_exposure_config(header, keyword_map):
        missing_keys = []
        for keyword in keyword_map:
            if keyword not in header:
                missing_keys.append(keyword)

        if missing_keys:
            raise MissingKeysError(missing_keys)
        return ExposureConfig(*(header[keyword] for keyword in keyword_map))


class MissingKeysError(Exception):
    def __init__(self, keys):
        self.keys = keys


class OpeningFITSError(Exception):
    pass
