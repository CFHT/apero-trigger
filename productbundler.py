import glob
import os
from collections import defaultdict, OrderedDict

from astropy.io import fits

from dbinterface import DatabaseHeaderConverter
from distribution import distribute_file
from drswrapper import FIBER_LIST
from logger import logger
from pathhandler import PathHandler, Path, Night


class HDUConfig:
    def __init__(self, ext_name, path, extension=0, header_operation=None, **kwargs):
        self.ext_name = ext_name
        self.path = path
        self.extension = extension
        self.header_operation = header_operation
        self.kwargs = kwargs

    def open_hdu_and_update(self):
        hdu = self.open_hdu()
        if self.ext_name:
            if 'XTENSION' in hdu.header:
                hdu.header.insert('GCOUNT', ('EXTNAME', self.ext_name), after=True)
            else:
                hdu.header.insert(0, ('EXTNAME', self.ext_name))
        column_names = self.kwargs.get('column_names')
        if column_names:
            hdu = self.to_binary_table(column_names)
        if self.header_operation:
            self.header_operation(hdu.header)
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

    def to_binary_table(self, column_names):
        image_hdu = self.open_hdu()
        return self.convert_image_to_binary_table(image_hdu, column_names)

    # Note: currently assumes all columns are double
    @staticmethod
    def convert_image_to_binary_table(image_hdu, column_names, transpose=False):
        data = image_hdu.data if not transpose else image_hdu.data.T
        if image_hdu.header['NAXIS'] == 1:
            name = column_names if isinstance(column_names, str) else column_names[0]
            columns = [fits.Column(name=name, format='D', array=data)]
        else:
            assert len(column_names) == len(image_hdu.data)
            columns = [fits.Column(name=name, format='D', array=column) for name, column in zip(column_names, data)]
        header = image_hdu.header.copy()
        if 'BUNIT' in header:
            header.insert('BUNIT', ('TUNIT', header['BUNIT']))
            header.remove('BUNIT')
        return fits.BinTableHDU.from_columns(columns, header=header)

class MEFConfig:
    def __init__(self, extension_configs):
        self.hdu_configs = extension_configs

    def create_hdu_list(self):
        hdu_list = fits.HDUList()
        for hdu_config in self.hdu_configs:
            source_hdu = hdu_config.open_hdu_and_update()
            if len(hdu_list) == 0:
                source_hdu.header['NEXTEND'] = len(self.hdu_configs) - 1
            else:
                source_hdu.header.remove('NEXTEND', ignore_missing=True)
            hdu_list.append(source_hdu)
        return hdu_list


class ProductBundler:
    def __init__(self, trace, realtime, create, distribute):
        self.trace = trace
        self.realtime = realtime
        self.create = create or realtime
        self.distribute = distribute or realtime
        self.exposures_status = None

    def set_exposure_status(self, exposure_status):
        if exposure_status:
            self.set_sequence_status([exposure_status])

    def set_sequence_status(self, exposures_status):
        self.exposures_status = exposures_status

    def create_spec_product(self, path):
        self.create_1d_spectra_product(path)
        self.create_2d_spectra_product(path)

    def create_1d_spectra_product(self, path):
        product_1d = path.final_product('s').fullpath
        extensions = [HDUConfig(fiber, path.s1d(fiber), column_names=['Flux']) for fiber in FIBER_LIST]
        mef_config = MEFConfig([
            self.get_primary_header(path),
            *extensions
        ])
        self.create_mef(mef_config, product_1d)
        self.distribute_file(product_1d)

    def create_2d_spectra_product(self, path):
        product_2d = path.final_product('e').fullpath
        flux_extensions = [HDUConfig('Flux' + fiber, path.e2ds(fiber, flat_fielded=True)) for fiber in FIBER_LIST]
        try:
            cal_extensions = self.get_cal_extensions(path)
        except:
            logger.error('Failed to find calibration files in header, cannot create product %s', product_2d)
            return
        mef_config = MEFConfig([
            self.get_primary_header(path),
            *flux_extensions,
            *cal_extensions
        ])
        self.create_mef(mef_config, product_2d)
        self.distribute_file(product_2d)

    def create_pol_product(self, path):
        product_pol = path.final_product('p').fullpath
        try:
            cal_extensions = self.get_cal_extensions(path, 'WaveAB', 'BlazeAB')
        except:
            logger.error('Failed to find calibration files in header, cannot create product %s', product_pol)
            return
        mef_config = MEFConfig([
            self.get_primary_header(path),
            HDUConfig('Pol', path.reduced('e2ds_pol')),
            HDUConfig('PolErr', path.reduced('e2ds_pol'), extension=1),
            HDUConfig('StokesI', path.reduced('e2ds_AB_StokesI')),
            HDUConfig('StokesIErr', path.reduced('e2ds_AB_StokesI'), extension=1),
            HDUConfig('Null1', path.reduced('e2ds_null1_pol')),
            HDUConfig('Null2', path.reduced('e2ds_null2_pol')),
            *cal_extensions
        ])
        self.create_mef(mef_config, product_pol)
        self.distribute_file(product_pol)

    def create_tell_product(self, path):
        product_tell = path.final_product('t').fullpath
        try:
            cal_extensions = self.get_cal_extensions(path, 'WaveAB', 'BlazeAB')
        except:
            logger.error('Failed to find calibration files in header, cannot create product %s', product_tell)
            return
        mef_config = MEFConfig([
            self.get_primary_header(path),
            HDUConfig('FluxAB', path.e2ds('AB', telluric_corrected=True, flat_fielded=True)),
            *cal_extensions
        ])
        self.create_mef(mef_config, product_tell)
        self.distribute_file(product_tell)

    def create_ccf_product(self, path, ccf_mask, telluric_corrected):
        product_ccf = path.final_product('v').fullpath
        ccf_path = path.ccf('AB', ccf_mask, telluric_corrected=telluric_corrected)
        column_names = ['Order' + str(i) for i in range(1, 50)] + ['Combined']
        wcs_cleanup = lambda header: [header.remove(key, ignore_missing=True) for key in
                                      ('CRVAL1', 'CRVAL2', 'CDELT1', 'CDELT2', 'CTYPE1', 'CTYPE2')]
        mef_config = MEFConfig([
            self.get_primary_header(path),
            HDUConfig('CCF', ccf_path, column_names=column_names, header_operation=wcs_cleanup)
        ])
        self.create_mef(mef_config, product_ccf)
        self.distribute_file(product_ccf)

    def get_primary_header(self, path):
        raw_header = HDUConfig(None, path.raw, header_operation=self.set_post_processing_headers)
        return raw_header

    def get_cal_extensions(self, path, *args):
        cal_path_dict = self.get_cal_paths(path)
        if args:
            return [HDUConfig(ext_name, cal_path_dict[ext_name]) for ext_name in args]
        else:
            return [HDUConfig(ext_name, ext_path) for ext_name, ext_path in cal_path_dict.items()]

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
            header = headers_per_fiber['WAVEFILE'][fiber]
            if fiber == 'A' or fiber == 'B':
                filename = header.replace('AB', fiber)
            else:
                filename = header
            extensions['Wave' + fiber] = Path(reduced_directory, filename)
        for fiber in FIBER_LIST:
            flat_path = PathHandler.from_preprocessed('*', headers_per_fiber['BLAZFILE'][fiber])
            path_pattern = flat_path.saved_calibration('blaze', fiber).fullpath
            filename = next(file for file in glob.glob(path_pattern) if os.path.exists(file))
            extensions['Blaze' + fiber] = Path(reduced_directory, filename)
        return extensions

    def create_mef(self, mef_config, output_file):
        if not self.create:
            return
        logger.info('Creating MEF %s', output_file)
        if self.trace:
            return
        try:
            hdu_list = mef_config.create_hdu_list()
        except:
            logger.error('Unable to create product %s', output_file, exc_info=True)
            return
        hdu_list.writeto(output_file, overwrite=True)

    def distribute_file(self, file):
        if self.trace or not self.distribute:
            return
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
