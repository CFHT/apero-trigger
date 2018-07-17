from astropy.io import fits


class ActualExposureConfig:
    @classmethod
    def from_file(cls, filename):
        header = cls.__get_fits_header(filename)
        key = 'DPRTYPE'
        if key not in header:
            raise MissingKeysError(filename, [key])
        return header[key]

    @staticmethod
    def __get_fits_header(filename):
        try:
            return fits.open(filename)[0].header
        except:
            raise OpeningFITSError(filename)

class MissingKeysError(Exception):
    def __init__(self, filename, keys):
        self.filename = filename
        self.keys = keys


class OpeningFITSError(Exception):
    def __init__(self, filename):
        self.filename = filename
