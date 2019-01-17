import os
from envloader import input_root_directory, reduced_root_directory, temp_root_directory


class Path:
    def __init__(self, directory, filename):
        self.directory = directory
        self.filename = filename

    @property
    def fullpath(self):
        return os.path.join(self.directory, self.filename)


class PathHandler(object):
    def __init__(self, night, raw_file):
        self.__night = night
        self.__raw_filename = os.path.basename(raw_file)

    @property
    def night(self):
        return self.__night

    @property
    def raw(self):
        return Path(self.input_directory, self.__raw_filename)

    @property
    def preprocessed(self):
        return Path(self.temp_directory, os.path.splitext(self.raw.filename)[0] + '_pp.fits')

    @property
    def saved_calib(self):
        return Path(self.reduced_directory, self.night + '_' + self.preprocessed.filename)

    def s1d(self, fiber):
        return self.extracted_product('s1d', fiber)

    def e2ds(self, fiber, telluric_corrected=False, flat_fielded=False):
        product_name = 'e2dsff' if flat_fielded else 'e2ds'
        suffix = 'tellu_corrected' if telluric_corrected else None
        return self.extracted_product(product_name, fiber, suffix)

    def ccf(self, fiber, mask, telluric_corrected=False):
        product_name = 'ccf_' + mask.replace('.mas', '')
        suffix = 'tellu_corrected' if telluric_corrected else None
        return self.extracted_product(product_name, fiber, suffix)

    def extracted_product(self, product, fiber, suffix=None):
        suffix = '_' + suffix if suffix else ''
        return self.reduced(product + '_' + fiber + suffix)

    def reduced(self, product):
        return Path(self.reduced_directory, self.preprocessed.filename.replace('.fits', '_' + product + '.fits'))

    def final_product(self, letter):
        return Path(self.reduced_directory, self.raw.filename.replace('o.fits', letter + '.fits'))

    @property
    def input_directory(self):
        return os.path.join(input_root_directory, self.night)

    @property
    def temp_directory(self):
        return os.path.join(temp_root_directory, self.night)

    @property
    def reduced_directory(self):
        return os.path.join(reduced_root_directory, self.night)
