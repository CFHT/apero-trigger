import os
from envloader import input_root_directory, reduced_root_directory, temp_root_directory


class PathHandler(object):
    def __init__(self, night, raw_file):
        self.__night = night
        self.__raw_filename = os.path.basename(raw_file)

    def night(self):
        return self.__night

    def raw_path(self):
        return self.input_file_path(self.raw_filename())

    def raw_filename(self):
        return self.__raw_filename

    def preprocessed_path(self):
        return self.temp_file_path(self.preprocessed_filename())

    def preprocessed_filename(self):
        return os.path.splitext(self.raw_filename())[0] + '_pp.fits'

    def saved_calib_filename(self):
        return self.night() + '_' + self.preprocessed_filename()

    def s1d_path(self, fiber):
        return self.reduced_file_path(self.s1d_filename(fiber))

    def s1d_filename(self, fiber):
        return self.extracted_product_filename('s1d', fiber)

    def e2ds_path(self, fiber, telluric_corrected=False):
        return self.reduced_file_path(self.e2ds_filename(fiber, telluric_corrected))

    def e2ds_filename(self, fiber, telluric_corrected=False):
        return self.extracted_product_filename('e2ds', fiber, telluric_corrected)

    def ccf_path(self, fiber, mask, telluric_corrected=False):
        return self.reduced_file_path(self.ccf_filename(fiber, mask, telluric_corrected))

    def ccf_filename(self, fiber, mask, telluric_corrected=False):
        product_name = 'ccf_' + mask.replace('.mas', '')
        return self.extracted_product_filename(product_name, fiber, telluric_corrected)

    def extracted_product_filename(self, product, fiber, telluric_corrected=False):
        telluric_suffix =  '_tellu_corrected' if telluric_corrected else ''
        return self.preprocessed_filename().replace('.fits', '_' + product + '_' + fiber + telluric_suffix + '.fits')

    def final_product_path(self, letter):
        return self.reduced_file_path(self.final_product_filename(letter))

    def final_product_filename(self, letter):
        return self.raw_filename().replace('o.fits', letter + '.fits')

    def input_directory(self):
        return os.path.join(input_root_directory, self.night())

    def input_file_path(self, filename):
        return os.path.join(input_root_directory, self.night(), filename)

    def temp_file_path(self, filename):
        return os.path.join(temp_root_directory, self.night(), filename)

    def reduced_file_path(self, filename):
        return os.path.join(reduced_root_directory, self.night(), filename)
