import glob
import os
import textwrap
from collections import defaultdict, OrderedDict

import numpy as np
from astropy.io import fits

from dbinterface import DatabaseHeaderConverter
from distribution import distribute_file
from drswrapper import FIBER_LIST
from logger import logger
from pathhandler import PathHandler, Path, Night


class BinTableConfig:
    def __init__(self, **kwargs):
        self.column_names = kwargs.get('column_names')
        self.column_units = kwargs.get('column_units')
        self.column_formats = kwargs.get('column_formats')
        self.transpose = bool(kwargs.get('transpose'))

    def convert_image_to_binary_table(self, image_hdu):
        header = image_hdu.header.copy()
        default_unit = header.get('BUNIT')
        if 'BUNIT' in header:
            header.remove('BUNIT')
        bitpix_format_map = {16: 'I', 32: 'J', 64: 'K', -32: 'E', -64: 'D'}
        default_format = bitpix_format_map.get(header.get('BITPIX'))
        data = image_hdu.data if not self.transpose else image_hdu.data.T
        names = self.column_names
        units = self.column_units if self.column_units else [default_unit] * len(names)
        formats = self.column_formats if self.column_formats else [default_format] * len(names)
        assert len(names) == len(data)
        columns = [fits.Column(name=name, format=format, unit=unit, array=column)
                   for name, format, unit, column in zip(names, formats, units, data)]
        return fits.BinTableHDU.from_columns(columns, header=header)


class HDUConfig:
    def __init__(self, ext_name, path, extension=0, **kwargs):
        self.ext_name = ext_name
        self.path = path
        self.extension = extension
        self.primary_header_only = kwargs.get('primary_header_only')
        self.header_operation = kwargs.get('header_operation')
        self.data_operation = kwargs.get('data_operation')
        self.hdu_operation = kwargs.get('hdu_operation')
        self.bin_table = kwargs.get('bin_table')

    def is_bin_table(self):
        return bool(self.bin_table)

    def set_bin_table_config(self, bin_table):
        self.bin_table = bin_table

    def open_hdu_and_update(self):
        hdu = self.open_hdu()
        if self.ext_name:
            if 'XTENSION' in hdu.header:
                hdu.header.insert('GCOUNT', ('EXTNAME', self.ext_name), after=True)
            else:
                hdu.header.insert(0, ('EXTNAME', self.ext_name))
        if self.primary_header_only:
            hdu = fits.PrimaryHDU(header=hdu.header)
        if self.hdu_operation:
            self.hdu_operation(hdu)
        if self.header_operation:
            self.header_operation(hdu.header)
        if self.data_operation:
            self.data_operation(hdu.data)
        if self.bin_table:
            hdu = self.bin_table.convert_image_to_binary_table(hdu)
        return hdu

    def open_hdu(self):
        input_file = self.path.fullpath
        try:
            hdu_list = fits.open(input_file)
        except:
            logger.error('Unable to open file %s', input_file)
            raise
        hdu = hdu_list[self.extension]
        return hdu


class MEFConfig:
    def __init__(self, extension_configs, hdulist_operation=None):
        self.hdu_configs = extension_configs
        self.hdulist_operation = hdulist_operation

    def create_hdu_list(self):
        hdu_list = fits.HDUList()
        for hdu_config in self.hdu_configs:
            source_hdu = hdu_config.open_hdu_and_update()
            if len(hdu_list) == 0:
                source_hdu.header['NEXTEND'] = len(self.hdu_configs) - 1
            else:
                source_hdu.header.remove('NEXTEND', ignore_missing=True)
            self.wcs_clean(source_hdu.header)
            hdu_list.append(source_hdu)
        if self.hdulist_operation:
            self.hdulist_operation(hdu_list)
        return hdu_list

    @staticmethod
    def wcs_clean(header):
        if header['NAXIS'] < 2 or header.get('XTENSION') == 'BINTABLE':
            remove_keys(header, ('CTYPE2', 'CUNIT2', 'CRPIX2', 'CRVAL2', 'CDELT2'))
        if header['NAXIS'] < 1:
            remove_keys(header, ('CTYPE1', 'CUNIT1', 'CRPIX1', 'CRVAL1', 'CDELT1'))


class ProductBundler:
    def __init__(self, trace, realtime, create, distribute):
        self.realtime = realtime
        self.create = (create or realtime) and not trace
        self.distribute = (distribute or realtime) and not trace
        self.exposures_status = None

    def set_exposure_status(self, exposure_status):
        if exposure_status:
            self.set_sequence_status([exposure_status])

    def set_sequence_status(self, exposures_status):
        self.exposures_status = exposures_status

    def create_spec_product(self, path):
        self.create_1d_spectra_product(path)
        self.create_2d_spectra_product(path)

    def produce(self, path, config_builder, use_default_columns=False, skip_header_magic=False):
        fullpath = path.fullpath
        if self.create:
            hdu_configs = config_builder()
            if use_default_columns:
                for hdu_config in hdu_configs:
                    if not hdu_config.is_bin_table():
                        hdu_config.set_bin_table_config(BinTableConfig(column_names=self.get_default_columns()))
            hdulist_operation = self.product_header_update if not skip_header_magic else None
            mef_config = MEFConfig(hdu_configs, hdulist_operation=hdulist_operation)
            self.create_mef(mef_config, fullpath)
        if self.distribute:
            self.distribute_file(fullpath)

    def create_1d_spectra_product(self, path):
        def update_hdu(hdu):
            flux = hdu.data
            errs = np.full(len(flux), np.nan)
            wavelengths = self.resample_wcs_to_data(hdu.header, flux)
            hdu.data = np.row_stack((flux, errs, wavelengths))

        def config_builder():
            bin_table_config = BinTableConfig(column_names=['Flux', 'FluxErr', 'Wave'],
                                              column_units=['Relative Flux', 'Relative Flux', 'nm'])
            extensions = [HDUConfig(fiber, path.s1d(fiber), hdu_operation=update_hdu,
                                    bin_table=bin_table_config) for fiber in FIBER_LIST]
            return [
                self.get_primary_header(path),
                *extensions
            ]

        product_1d = path.final_product('s')
        self.produce(product_1d, config_builder)

    def create_2d_spectra_product(self, path):
        def config_builder():
            flux_extensions = [HDUConfig('Flux' + fiber, path.e2ds(fiber, flat_fielded=True)) for fiber in FIBER_LIST]
            try:
                cal_extensions = self.get_cal_extensions(path)
            except:
                logger.error('Failed to find calibration files in header, cannot create product %s', product_2d)
            else:
                return [
                    self.get_primary_header(path),
                    *flux_extensions,
                    *cal_extensions
                ]

        product_2d = path.final_product('e')
        self.produce(product_2d, config_builder)

    def create_pol_product(self, path):
        def wipe_snr(header):
            for i in range(0, 49):
                key = 'SNR' + str(i)
                header[key] = 'Unknown'

        def config_builder():
            try:
                cal_extensions = self.get_cal_extensions(path, 'WaveAB', 'BlazeAB')
            except:
                logger.error('Failed to find calibration files in header, cannot create product %s', product_pol)
            else:
                return [
                    self.get_primary_header(path),
                    HDUConfig('Pol', path.reduced('e2ds_pol'), header_operation=wipe_snr),
                    HDUConfig('PolErr', path.reduced('e2ds_pol'), extension=1),
                    HDUConfig('StokesI', path.reduced('e2ds_AB_StokesI'), header_operation=wipe_snr),
                    HDUConfig('StokesIErr', path.reduced('e2ds_AB_StokesI'), extension=1),
                    HDUConfig('Null1', path.reduced('e2ds_null1_pol'), header_operation=wipe_snr),
                    HDUConfig('Null2', path.reduced('e2ds_null2_pol'), header_operation=wipe_snr),
                    *cal_extensions
                ]

        product_pol = path.final_product('p')
        self.produce(product_pol, config_builder)

    def create_tell_product(self, path):
        def config_builder():
            try:
                cal_extensions = self.get_cal_extensions(path, 'WaveAB', 'BlazeAB')
            except:
                logger.error('Failed to find calibration files in header, cannot create product %s', product_tell)
            else:
                return [
                    self.get_primary_header(path),
                    HDUConfig('FluxAB', path.e2ds('AB', telluric_corrected=True, flat_fielded=True)),
                    *cal_extensions,
                    HDUConfig('Recon', path.e2ds('AB', telluric_reconstruction=True, flat_fielded=True))
                ]

        product_tell = path.final_product('t')
        self.produce(product_tell, config_builder)

    def create_ccf_product(self, path, ccf_mask, telluric_corrected, fp):
        def fix_header(header):
            header.insert('CRVAL1', ('CRPIX1', 0))

        def update_hdu(hdu):
            existing = hdu.data
            velocities = self.resample_wcs_to_data(hdu.header, existing[0])
            hdu.data = np.row_stack((velocities, existing))
            fix_header(hdu.header)

        def config_builder():
            ccf_path = path.ccf('AB', ccf_mask, telluric_corrected=telluric_corrected, fp=fp)
            bin_table_config = BinTableConfig(column_names=['Velocity'] + self.get_default_columns() + ['Combined'])
            return [
                HDUConfig(None, ccf_path, header_operation=fix_header, primary_header_only=True),
                HDUConfig('CCF', ccf_path, hdu_operation=update_hdu, bin_table=bin_table_config)
            ]

        product_ccf = path.final_product('v')
        self.produce(product_ccf, config_builder, skip_header_magic=True)

    def get_primary_header(self, path):
        return HDUConfig(None, path.preprocessed, primary_header_only=True)

    def get_default_columns(self):
        return ['Order' + str(i) for i in range(0, 49)]

    def product_header_update(self, hdulist):
        if len(hdulist) <= 1:
            logger.error('Trying to create product primary HDU with no extensions')
            return
        primary_header = hdulist[0].header
        ext_header = hdulist[1].header
        primary_header.insert('PVERSION', get_card(ext_header, 'VERSION'), after=True)
        remove_keys(primary_header, ('BITPIX', 'NAXIS', 'NAXIS1', 'NAXIS2'))
        if ext_header.get('EXTNAME') == 'Pol':
            primary_header['EXPTIME'] = (ext_header['EXPTIME'], '[sec] total integration time of 4 exposures')
            primary_header['MJDATE'] = (ext_header['MJDATE'], 'Modified Julian Date at middle of sequence')
        for extension in hdulist[1:]:
            extname = extension.header.get('EXTNAME')
            if extname.startswith('Wave') or extname.startswith('Blaze') or extname.endswith('Err'):
                continue
            dupe_keys = verify_duplicate_cards(extension.header, primary_header.items())
            remove_keys(ext_header, dupe_keys)
        self.add_extension_description(hdulist)
        self.set_post_processing_headers(primary_header)

    @staticmethod
    def add_extension_description(hdulist):
        ext_names = [hdu.header.get('EXTNAME') for hdu in hdulist[1:]]
        description = 'This file contains the following extensions: ' + ', '.join(ext_names)
        for line in textwrap.wrap(description, 71):
            hdulist[0].header.insert('FILENAME', ('COMMENT', line))

    def get_cal_extensions(self, path, *args):
        def cleanup_wave(header):
            remove_keys(header, ('CRVAL2', 'CDELT2', 'CTYPE2'))

        def extension_operation(ext_name):
            return cleanup_wave if ext_name.startswith('Wave') else None

        cal_path_dict = self.get_cal_paths(path)
        ext_names = args if args else cal_path_dict.keys()
        return [HDUConfig(name, cal_path_dict[name], header_operation=extension_operation(name)) for name in ext_names]

    def get_cal_paths(self, path):
        headers_per_fiber = defaultdict(OrderedDict)
        for fiber in FIBER_LIST:
            source_file = path.e2ds(fiber, flat_fielded=True).fullpath
            source_header = fits.open(source_file)[0].header
            for keyword in ['WAVEFILE', 'BLAZFILE']:
                headers_per_fiber[keyword][fiber] = source_header[keyword]
        extensions = OrderedDict()
        reduced_directory = Night(path.night).reduced_directory
        for fiber in FIBER_LIST:
            filename = headers_per_fiber['WAVEFILE'][fiber]
            extensions['Wave' + fiber] = Path(reduced_directory, filename)
        for fiber in FIBER_LIST:
            flat_path = PathHandler.from_preprocessed('*', headers_per_fiber['BLAZFILE'][fiber])
            path_pattern = flat_path.saved_calibration('blaze', fiber).fullpath
            filename = next(file for file in glob.glob(path_pattern) if os.path.exists(file))
            extensions['Blaze' + fiber] = Path(reduced_directory, filename)
        return extensions

    @classmethod
    def resample_wcs_to_data(cls, header, data):
        c_start = header['CRVAL1']
        c_delta = header['CDELT1']
        num_bins = len(data)
        return cls.steps(c_start, c_delta, num_bins)

    @staticmethod
    def steps(start, step, n):
        end = start + step * n
        return np.linspace(start, end, num=n, endpoint=False)

    def create_mef(self, mef_config, output_file):
        if self.create:
            logger.info('Creating MEF %s', output_file)
            try:
                hdu_list = mef_config.create_hdu_list()
            except:
                logger.error('Unable to create product %s', output_file, exc_info=True)
            else:
                hdu_list.writeto(output_file, overwrite=True)

    def distribute_file(self, file):
        if self.distribute:
            subdir = 'quicklook' if self.realtime else 'reduced'
            try:
                new_file = distribute_file(file, subdir)
            except:
                logger.error('Unable to distribute product %s', file, exc_info=True)
            else:
                logger.info('Distributing %s', new_file)

    def set_post_processing_headers(self, header):
        exposure_statuses = self.exposures_status
        if exposure_statuses:
            if len(exposure_statuses) == 1:
                header_dict = DatabaseHeaderConverter.exp_status_db_to_header(exposure_statuses[0])
            else:
                header_dict =  DatabaseHeaderConverter.seq_status_db_to_header(exposure_statuses)
            header.update(header_dict)


def get_card(header, keyword):
    return (keyword, header[keyword], header.comments[keyword])


def remove_keys(header, keys):
    for key in keys:
        header.remove(key, ignore_missing=True)


def verify_duplicate_cards(header, cards):
    dupe_keys = []
    for card in cards:
        key, value = card[0], card[1]  # Split up so it still works if len(card) is 3
        if key in ('SIMPLE', 'EXTEND', 'NEXTEND'):
            continue
        if header.get(key) == value:
            dupe_keys.append(key)
        else:
            extname = header.get('EXTNAME')
            logger.warning('Header key %s expected to be duplicate in extension %s but was not', key, extname)
    return dupe_keys