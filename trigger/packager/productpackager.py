import textwrap
from collections import defaultdict, OrderedDict

import numpy as np
from astropy.io import fits

from .fitsbuilder import BinTableBuilder, MEFBuilder, HDUBuilder
from .fitsoperations import get_card, remove_keys, verify_duplicate_cards
from ..common import FIBER_LIST, log


class ProductPackager:
    def __init__(self, trace, create):
        self.create = create and not trace

    def create_1d_spectra_product(self, exposure, is_telluric_corrected=False):
        product = exposure.final_product('s')
        log.info('Creating %s', product)
        try:
            primary_hdu = self.get_primary_header(exposure)
            extensions = []
            for ext, ext_name in {'w': 'UniformWavelength', 'v': 'UniformVelocity'}.items():
                input_files = {fiber: exposure.s1d(ext, fiber) for fiber in FIBER_LIST}
                if is_telluric_corrected:
                    input_files['ABTelluCorrected'] = exposure.s1d(ext, 'AB', telluric_corrected=True)
                header = None
                cols = None
                for fiber, input_file in input_files.items():
                    input_hdu_list = fits.open(input_file)
                    input_hdu = input_hdu_list[1]
                    if cols is None:
                        cols = [BinTableBuilder.column_from_bin_table(input_hdu, 0, name='Wave')]
                        header = input_hdu.header
                    cols.append(BinTableBuilder.column_from_bin_table(input_hdu, 1, name='Flux' + fiber,
                                                                      unit='Relative Flux'))
                    cols.append(BinTableBuilder.column_from_bin_table(input_hdu, 2, name='FluxErr' + fiber,
                                                                      unit='Relative Flux'))
                table = fits.BinTableHDU.from_columns(columns=cols, header=header)
                extensions.append(HDUBuilder.extension_from_hdu(ext_name, table))
            hdu_list = MEFBuilder.create_hdu_list([primary_hdu, *extensions])
            self.product_header_update(hdu_list)
            hdu_list.writeto(product, overwrite=True)
        except FileNotFoundError as err:
            log.error('Creation of %s failed: unable to open file %s', product, err.filename)
        except:
            log.error('Creation of %s failed', product, exc_info=True)

    def create_2d_spectra_product(self, exposure):
        product = exposure.final_product('e')
        log.info('Creating %s', product)
        try:
            primary_hdu = self.get_primary_header(exposure)
            cal_extensions = self.get_cal_extensions(exposure)
            flux_extensions = []
            for fiber in FIBER_LIST:
                e2ds = exposure.e2ds(fiber, flat_fielded=True)
                flux_extensions.append(HDUBuilder.extension_from_file('Flux' + fiber, e2ds))
            hdu_list = MEFBuilder.create_hdu_list([primary_hdu, *flux_extensions, *cal_extensions])
            self.product_header_update(hdu_list)
            hdu_list.writeto(product, overwrite=True)
        except CalExtensionError:
            log.error('Creation of %s failed: could not find calibrations from header', product.name, exc_info=True)
        except FileNotFoundError as err:
            log.error('Creation of %s failed: unable to open file %s', product, err.filename)
        except:
            log.error('Creation of %s failed', product, exc_info=True)

    def create_pol_product(self, exposure):
        def wipe_snr(header):
            for i in range(0, 49):
                key = 'SNR' + str(i)
                header[key] = 'Unknown'

        product = exposure.final_product('p')
        log.info('Creating %s', product)
        try:
            primary_hdu = self.get_primary_header(exposure)
            cal_extensions = self.get_cal_extensions(exposure, 'WaveAB', 'BlazeAB')
            pol_extensions = []
            pol = fits.open(exposure.reduced('e2ds_pol'))
            pol_extensions.extend([
                HDUBuilder.extension_from_hdu('Pol', pol[0], header_operation=wipe_snr),
                HDUBuilder.extension_from_hdu('PolErr', pol[1]),
            ])
            stokes_i = fits.open(exposure.reduced('e2ds_AB_StokesI'))
            pol_extensions.extend([
                HDUBuilder.extension_from_hdu('StokesI', stokes_i[0], header_operation=wipe_snr),
                HDUBuilder.extension_from_hdu('StokesIErr', stokes_i[1]),
            ])
            pol_extensions.extend([
                HDUBuilder.extension_from_file('Null1', exposure.reduced('e2ds_null1_pol'), header_operation=wipe_snr),
                HDUBuilder.extension_from_file('Null2', exposure.reduced('e2ds_null2_pol'), header_operation=wipe_snr),
            ])
            hdu_list = MEFBuilder.create_hdu_list([primary_hdu, *pol_extensions, *cal_extensions])
            self.product_header_update(hdu_list)

            # We copy input files to primary header after duplicate keys have been cleaned out
            primary_header = hdu_list[0].header
            pol_header = hdu_list[1].header
            in_file_cards = [card for card in pol_header.cards if card[0].startswith('FILENAM')]
            for card in in_file_cards:
                primary_header.insert('FILENAME', card)
            primary_header.remove('FILENAME', ignore_missing=True)

            hdu_list.writeto(product, overwrite=True)
        except CalExtensionError:
            log.error('Creation of %s failed: could not find calibrations from header', product.name, exc_info=True)
        except FileNotFoundError as err:
            log.error('Creation of %s failed: unable to open file %s', product, err.filename)
        except:
            log.error('Creation of %s failed', product, exc_info=True)

    def create_tell_product(self, exposure):
        product = exposure.final_product('t')
        log.info('Creating %s', product)
        try:
            cal_extensions = self.get_cal_extensions(exposure, 'WaveAB', 'BlazeAB')
        except:
            log.error('Could not find calibrations from header, cannot create %s', product.name, exc_info=True)
            return
        try:
            primary_hdu = self.get_primary_header(exposure)
            flux = HDUBuilder.extension_from_file('FluxAB', exposure.e2ds('AB', telluric_corrected=True,
                                                                          flat_fielded=True))
            recon = HDUBuilder.extension_from_file('Recon', exposure.e2ds('AB', telluric_reconstruction=True,
                                                                          flat_fielded=True))
            hdu_list = MEFBuilder.create_hdu_list([primary_hdu, flux, *cal_extensions, recon])
            self.product_header_update(hdu_list)
            hdu_list.writeto(product, overwrite=True)
        except FileNotFoundError as err:
            log.error('Creation of %s failed: unable to open file %s', product, err.filename)
        except:
            log.error('Creation of %s failed', product, exc_info=True)

    def create_ccf_product(self, exposure, ccf_mask, telluric_corrected, fp):
        def resample_wcs_to_data(header, data):
            c_start = float(header['CRVAL1'])
            c_delta = float(header['CDELT1'])
            num_bins = len(data)
            c_end = c_start + c_delta * num_bins
            return np.linspace(c_start, c_end, num=num_bins, endpoint=False)

        def fix_header(header):
            header.insert('CRVAL1', ('CRPIX1', 0))

        def update_hdu(hdu):
            existing = hdu.data
            velocities = resample_wcs_to_data(hdu.header, existing[0])
            hdu.data = np.row_stack((velocities, existing))

        product = exposure.final_product('v')
        log.info('Creating %s', product)
        try:
            ccf_path = exposure.ccf('AB', ccf_mask, telluric_corrected=telluric_corrected, fp=fp)
            ccf_input_hdu = fits.open(ccf_path)[0]
            fix_header(ccf_input_hdu.header)
            primary_hdu = fits.PrimaryHDU(header=ccf_input_hdu.header)
            column_configs = [BinTableBuilder.column_config(name='Velocity', unit='km/s'),
                             *(BinTableBuilder.column_config(name='Order'+ str(i)) for i in range(0, 49)),
                              BinTableBuilder.column_config(name='Combined')]
            update_hdu(ccf_input_hdu)
            ccf_out_hdu = BinTableBuilder.hdu_from_image('CCF', ccf_input_hdu, column_configs)
            hdu_list = MEFBuilder.create_hdu_list([primary_hdu, ccf_out_hdu])
            hdu_list.writeto(product, overwrite=True)
        except FileNotFoundError as err:
            log.error('Creation of %s failed: unable to open file %s', product, err.filename)
        except:
            log.error('Creation of %s failed', product, exc_info=True)

    def get_primary_header(self, exposure):
        hdu = fits.open(exposure.preprocessed)[0]
        remove_keys(hdu.header, ('DRSPID', 'INF1000', 'QCC', 'QCC000N',
                             'QCC001N', 'QCC001V', 'QCC001L', 'QCC001P',
                             'QCC002N', 'QCC002V', 'QCC002L', 'QCC002P'))
        return fits.PrimaryHDU(header=hdu.header)

    def product_header_update(self, hdu_list):
        if len(hdu_list) <= 1:
            log.error('Trying to create product primary HDU with no extensions')
            return
        primary_header = hdu_list[0].header
        ext_header = hdu_list[1].header
        primary_header.insert('PVERSION', get_card(ext_header, 'VERSION'), after=True)
        remove_keys(primary_header, ('BITPIX', 'NAXIS', 'NAXIS1', 'NAXIS2'))
        if ext_header.get('EXTNAME') == 'Pol':
            primary_header['EXPTIME'] = (ext_header['EXPTIME'], '[sec] total integration time of 4 exposures')
            primary_header['MJDATE'] = (ext_header['MJDATE'], 'Modified Julian Date at middle of sequence')
        for extension in hdu_list[1:]:
            extname = extension.header.get('EXTNAME')
            if extname.startswith('Wave') or extname.startswith('Blaze') or extname.endswith('Err'):
                continue
            dupe_keys = verify_duplicate_cards(extension.header, primary_header.items())
            remove_keys(extension.header, dupe_keys)
        # add extension descriptions
        ext_names = [hdu.header.get('EXTNAME') for hdu in hdu_list[1:]]
        description = 'This file contains the following extensions: ' + ', '.join(ext_names)
        for line in textwrap.wrap(description, 71):
            hdu_list[0].header.insert('FILENAME', ('COMMENT', line))

    def get_cal_extensions(self, exposure, *args):
        def keep_key(key):
            return key in ('EXTNAME', 'NAXIS', 'NAXIS1', 'NAXIS2') or key.startswith('INF') or key.startswith('CDB')

        def cleanup_keys(header):
            remove_keys(header, [key for key in header.keys() if not keep_key(key)])

        try:
            cal_path_dict = self.get_cal_paths(exposure)
            ext_names = args if args else cal_path_dict.keys()
            extensions = []
            for name in ext_names:
                extension = HDUBuilder.extension_from_file(name, cal_path_dict[name])
                if name.startswith('Wave') or name.startswith('Blaze'):
                    cleanup_keys(extension.header)
                extensions.append(extension)
            return extensions
        except:
            raise CalExtensionError()

    def get_cal_paths(self, exposure):
        headers_per_fiber = defaultdict(OrderedDict)
        for fiber in FIBER_LIST:
            source_file = exposure.e2ds(fiber, flat_fielded=True)
            source_header = fits.open(source_file)[0].header
            for keyword in ['CDBWAVE', 'CDBBLAZE']:
                headers_per_fiber[keyword][fiber] = source_header[keyword]
        extensions = OrderedDict()
        reduced_directory = exposure.reduced_directory
        for fiber in FIBER_LIST:
            filename = headers_per_fiber['CDBWAVE'][fiber]
            extensions['Wave' + fiber] = reduced_directory.joinpath(filename)
        for fiber in FIBER_LIST:
            filename = headers_per_fiber['CDBBLAZE'][fiber]
            extensions['Blaze' + fiber] = reduced_directory.joinpath(filename)
        return extensions


class CalExtensionError(Exception):
    pass
