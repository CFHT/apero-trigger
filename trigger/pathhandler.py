from pathlib import Path
from .drswrapper import ROOT_DATA_DIRECTORIES


class RootDirectories:
    input = Path(ROOT_DATA_DIRECTORIES.input)
    temp = Path(ROOT_DATA_DIRECTORIES.tmp)
    reduced = Path(ROOT_DATA_DIRECTORIES.reduced)


class Night:
    def __init__(self, night):
        self.night = Path(night).name

    @property
    def input_directory(self):
        return RootDirectories.input.joinpath(self.night)

    @property
    def temp_directory(self):
        return RootDirectories.temp.joinpath(self.night)

    @property
    def reduced_directory(self):
        return RootDirectories.reduced.joinpath(self.night)


class Exposure:
    def __init__(self, night, raw_file):
        self.__night = Night(night)
        self.__raw_filename = Path(raw_file).name

    @property
    def night(self):
        return self.__night.night

    @property
    def raw(self):
        return Path(self.input_directory, self.__raw_filename)

    @property
    def preprocessed(self):
        return Path(self.temp_directory, self.raw.name.replace('.fits', '_pp.fits'))

    def s1d(self, sample_space, fiber, telluric_corrected=False):
        product_name = 's1d_' + sample_space
        return self.extracted_product(product_name, fiber, telluric_corrected)

    def e2ds(self, fiber, telluric_corrected=False, telluric_reconstruction=False, flat_fielded=False):
        product_name = 'e2dsff' if flat_fielded else 'e2ds'
        assert not (telluric_corrected and telluric_reconstruction)
        if telluric_reconstruction:
            return self.extracted_product_general(product_name, fiber, 'tellu_recon')
        else:
            return self.extracted_product(product_name, fiber, telluric_corrected)

    def ccf(self, fiber, mask, fp=True, telluric_corrected=False):
        product_name = 'ccf_' + ('fp_' if fp else '') + mask.replace('.mas', '')
        return self.extracted_product(product_name, fiber, telluric_corrected)

    def extracted_product(self, product, fiber, telluric_corrected=False):
        suffix = 'tellu_corrected' if telluric_corrected else None
        return self.extracted_product_general(product, fiber, suffix)

    def extracted_product_general(self, product, fiber, suffix=None):
        suffix = '_' + suffix if suffix else ''
        return self.reduced(product + '_' + fiber + suffix)

    def reduced(self, product):
        return Path(self.reduced_directory, self.preprocessed.name.replace('.fits', '_' + product + '.fits'))

    def final_product(self, letter):
        return Path(self.reduced_directory, self.raw.name.replace('o.fits', letter + '.fits'))

    @property
    def input_directory(self):
        return self.__night.input_directory

    @property
    def temp_directory(self):
        return self.__night.temp_directory

    @property
    def reduced_directory(self):
        return self.__night.reduced_directory

    @property
    def obsid(self):
        return self.raw.stem

    @property
    def odometer(self):
        return int(self.obsid[:-1])
