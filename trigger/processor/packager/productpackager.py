import textwrap
from collections import OrderedDict, defaultdict
from typing import Iterable

from astropy.io import fits

from logger import log
from . import fitsoperations as fits_op
from ...common import Exposure, Fiber, SampleSpace, TelluSuffix


def create_1d_spectra_product(exposure: Exposure, is_telluric_corrected=False):
    """
    Create the s.fits product:
    FITS table containing the 1D extracted and rebinned spectra, for each channel (AB, A, B, C). The spectrum is
    corrected from the Blaze function, which provides a first-order continuum subtraction, and overlapping parts in the
    contiguous orders are merged. There are two versions of the 1D file: they both have wavelength units from 965 to
    2500 nm, but one has the flux rebinned in regular wavelength bins, while the other has a constant velocity bin. The
    table for each channel contains about 300,000 pixels (NAXIS2). There is also a telluric corrected version (only for
    the AB fiber).

    HDU #   Name                Type             Description
        1                       Primary Header
        2   UniformWavelength   Binary Table     Flux rebinned in regular wavelength bins
        3   UniformVelocity     Binary Table     Flux rebinned with a constant velocity bin

    Both UniformVelocity and UniformWavelength binary tables are in the following format:
    Col #   Name                      Units           Format   Description
        1   Wave                      nm              D        Wavelength vector
        2   FluxAB                    Relative Flux   D        Flux for fibers AB combined
        3   FluxErrAB                 Relative Flux   D        Error on FluxAB
        4   FluxA                     Relative Flux   D        Flux for fiber A
        5   FluxErrA                  Relative Flux   D        Error on FluxA
        6   FluxB                     Relative Flux   D        Flux for fiber B
        7   FluxErrB                  Relative Flux   D        Error on FluxB
        8   FluxC                     Relative Flux   D        Flux for fiber C
        9   FluxErrC                  Relative Flux   D        Error on FluxC
      *10   FluxABTelluCorrected      Relative Flux   D        Telluric corrected flux for fibers AB combined
      *11   FluxErrABTelluCorrected   Relative Flux   D        Error on FluxABTelluCorrected
    Format D = Double-precision floating-point
    *Columns 10 and 11 will only be present for telluric corrected exposures.

    :param exposure: Exposure to create product for
    :param is_telluric_corrected: Whether a telluric corrected s1d exists
    """
    product = exposure.final_product('s')
    log.info('Creating %s', product)
    try:
        primary_hdu = get_primary_header(exposure)
        extensions = []
        for ext, ext_name in {SampleSpace.WAVELENGTH: 'UniformWavelength',
                              SampleSpace.VELOCITY: 'UniformVelocity'}.items():
            input_files = {fiber.value: exposure.s1d(ext, fiber) for fiber in Fiber}
            if is_telluric_corrected:
                input_files['ABTelluCorrected'] = exposure.s1d(ext, Fiber.AB, TelluSuffix.TCORR)
            header = None
            cols = None
            for fiber, input_file in input_files.items():
                input_hdu_list = fits.open(input_file)
                input_hdu = input_hdu_list[1]
                if cols is None:
                    cols = [fits_op.column_from_bin_table(input_hdu, 0, name='Wave', unit='nm')]
                    header = input_hdu.header
                cols.append(fits_op.column_from_bin_table(input_hdu, 1, name='Flux' + fiber, unit='Relative Flux'))
                cols.append(fits_op.column_from_bin_table(input_hdu, 2, name='FluxErr' + fiber, unit='Relative Flux'))
            table = fits.BinTableHDU.from_columns(columns=cols, header=header)
            extensions.append(fits_op.extension_from_hdu(ext_name, table))
        hdu_list = fits_op.create_hdu_list([primary_hdu, *extensions])
        product_header_update(hdu_list)
        hdu_list.writeto(product, overwrite=True)
    except FileNotFoundError as err:
        log.error('Creation of %s failed: unable to open file %s', product, err.filename)
    except Exception:
        log.error('Creation of %s failed', product, exc_info=True)


def create_2d_spectra_product(exposure: Exposure):
    """
    Create the e.fits product:
    2D extracted spectra that use the instrument profile and order localization and performs optimal extraction. There
    is one extracted spectrum for channels A, B, combined AB, and C; these spectra are respectively saved into different
    extensions of the e files, each with its proper header. The Blaze function is not removed from these spectra, so
    that the flux distribution along each order is unchanged. Each extension contains 49 orders of 4088 pixels. These
    are the spectra used for radial-velocity analyses.

    HDU #   Name      Type             Description
        1             Primary Header
        2   FluxAB    Image            Flux for fibers AB combined
        3   FluxA     Image            Flux for fiber B
        4   FluxB     Image            Flux for fiber A
        5   FluxC     Image            Flux for fiber C
        6   WaveAB    Image            Wavelength vector for fibers AB combined
        7   WaveA     Image            Wavelength vector for fiber A
        8   WaveB     Image            Wavelength vector for fiber B
        9   WaveC     Image            Wavelength vector for fiber C
       10   BlazeAB   Image            Blaze function for fibers AB combined
       11   BlazeA    Image            Blaze function for fiber A
       12   BlazeB    Image            Blaze function for fiber B
       13   BlazeC    Image            Blaze function for fiber C

    :param exposure: Exposure to create product for
    """
    product = exposure.final_product('e')
    log.info('Creating %s', product)
    try:
        primary_hdu = get_primary_header(exposure)
        cal_extensions = get_cal_extensions(exposure)
        flux_extensions = []
        for fiber in Fiber:
            e2ds = exposure.e2ds(fiber)
            flux_extensions.append(fits_op.extension_from_file('Flux' + fiber.value, e2ds))
        hdu_list = fits_op.create_hdu_list([primary_hdu, *flux_extensions, *cal_extensions])
        product_header_update(hdu_list)
        hdu_list.writeto(product, overwrite=True)
    except CalExtensionError:
        log.error('Creation of %s failed: could not find calibrations from header', product.name, exc_info=True)
    except FileNotFoundError as err:
        log.error('Creation of %s failed: unable to open file %s', product, err.filename)
    except Exception:
        log.error('Creation of %s failed', product, exc_info=True)


def create_pol_product(exposure: Exposure):
    """
    Create the p.fits product:
    Polarimetric products only processed in polarimetric mode, from the combination of 4 consecutive exposures.

    HDU #    Name        Type             Description
        1                Primary Header
        2   Pol          Image            The polarized spectrum in the required Stokes configuration
        3   PolErr       Image            The error on the polarized spectrum
        4   StokesI      Image            The combined Stokes I (intensity) spectrum
        5   StokesIErr   Image            The error on the Stokes I spectrum
        6   Null1        Image            One null spectrum used to check the polarized signal (see Donati et al 1997)
        7   Null2        Image            The other null spectrum used to check the polarized signal
        8   WaveAB       Image            The wavelength vector for the AB science channel
        9   BlazeAB      Image            The Blaze function for AB (useful for Stokes I)

    :param exposure: Exposure to create product for
    """

    def wipe_snr(header):
        for key in header:
            if key.startswith('EXTSN'):
                header[key] = 'Unknown'

    product = exposure.final_product('p')
    log.info('Creating %s', product)
    try:
        primary_hdu = get_primary_header(exposure)
        cal_extensions = get_cal_extensions(exposure, 'WaveAB', 'BlazeAB')
        pol = fits.open(exposure.e2ds(Fiber.A, suffix='pol'))
        stokes_i = fits.open(exposure.e2ds(Fiber.A, suffix='StokesI'))
        pol_extensions = [
            fits_op.extension_from_hdu('Pol', pol[0]),
            fits_op.extension_from_hdu('PolErr', pol[1]),
            fits_op.extension_from_hdu('StokesI', stokes_i[0]),
            fits_op.extension_from_hdu('StokesIErr', stokes_i[1]),
            fits_op.extension_from_file('Null1', exposure.e2ds(Fiber.A, suffix='null1_pol')),
            fits_op.extension_from_file('Null2', exposure.e2ds(Fiber.A, suffix='null2_pol')),
        ]
        for ext in pol_extensions:
            wipe_snr(ext.header)

        hdu_list = fits_op.create_hdu_list([primary_hdu, *pol_extensions, *cal_extensions])
        product_header_update(hdu_list)

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
    except Exception:
        log.error('Creation of %s failed', product, exc_info=True)


def create_tell_product(exposure: Exposure):
    """
    Create the t.fits product:
    2D spectra in the same format as the e spectra, after the correction of telluric lines has been applied. This
    correction uses the library of empirical spectra of fast-rotating A stars used as telluric standards and the PCA
    method to optimally remove the contribution of the Earth atmosphere. Many parts of the spectrum are replaced by NaNs
    because the telluric absorption in this location is too intense and correction residuals will be too large. The
    correction is applied only on the combined AB channel, so there is only one Flux extension of 49 orders times 4088
    pixels.

    HDU #   Name      Type             Description
        1             Primary Header
        2   FluxAB    Image            The flux for the AB science channel
        3   WaveAB    Image            The wavelength vector for the AB science channel
        4   BlazeAB   Image            The Blaze function for the AB science channel
        5   Recon     Image            The spectrum of the Earth atmosphere which has been used in the correction

    :param exposure: Exposure to create product for
    """
    product = exposure.final_product('t')
    log.info('Creating %s', product)
    try:
        primary_hdu = get_primary_header(exposure)
        cal_extensions = get_cal_extensions(exposure, 'WaveAB', 'BlazeAB')
        flux = fits_op.extension_from_file('FluxAB', exposure.e2ds(Fiber.AB, TelluSuffix.TCORR))
        recon = fits_op.extension_from_file('Recon', exposure.e2ds(Fiber.AB, TelluSuffix.RECON))
        hdu_list = fits_op.create_hdu_list([primary_hdu, flux, *cal_extensions, recon])
        product_header_update(hdu_list)
        hdu_list.writeto(product, overwrite=True)
    except CalExtensionError:
        log.error('Creation of %s failed: could not find calibrations from header', product.name, exc_info=True)
    except FileNotFoundError as err:
        log.error('Creation of %s failed: unable to open file %s', product, err.filename)
    except Exception:
        log.error('Creation of %s failed', product, exc_info=True)


def create_ccf_product(exposure: Exposure, ccf_mask: str, fiber: Fiber, telluric_corrected=True):
    """
    Create the v.fits product:
    FITS table containing the radial velocity of the star extracted from the CCF. One cross-correlation mask is used per
    default, corresponding to an M3 spectral type, so it is not optimized for other types of stars. The velocity range
    of [-100,100] km/s is searched and a step of 1 km/s is used.

    HDU #   Name   Type
        1          Primary Header
        2   CCF    Binary Table

    CCF table is in the following format:
    Col #   Name       Units   Format   Description
        1   Velocity   km/s    D        Radial velocity step
        2   Order0             D        Cross-correlation calculated for the individual order
        3   Order1             D        Cross-correlation calculated for the individual order
      ...
       50   Order48            D        Cross-correlation calculated for the individual order
       51   Combined           D        Weighted mean of all orders, on which the velocity is modeled and measured
    Format D = Double-precision floating-point

    :param exposure: Exposure to create product for
    :param ccf_mask: Filename of the ccf mask that was used
    :param fiber: Fiber the ccf was done using
    :param telluric_corrected: Whether the ccf was done using a telluric corrected file
    :return:
    """
    product = exposure.final_product('v')
    log.info('Creating %s', product)
    try:
        ccf_path = exposure.ccf(ccf_mask, fiber, TelluSuffix.tcorr(telluric_corrected))
        ccf_input_hdu = fits.open(ccf_path)[1]
        primary_hdu = fits.PrimaryHDU(header=ccf_input_hdu.header)
        columns = [
            fits_op.column_from_bin_table(ccf_input_hdu, 0, name='Velocity', unit='km/s'),
            *(fits_op.column_from_bin_table(ccf_input_hdu, i, name='Order' + str(i - 1)) for i in range(1, 50)),
            fits_op.column_from_bin_table(ccf_input_hdu, 50, name='Combined')
        ]
        ccf_out_hdu = fits.BinTableHDU.from_columns(columns, header=ccf_input_hdu.header)
        ccf_extension = fits_op.extension_from_hdu('CCF', ccf_out_hdu)
        hdu_list = fits_op.create_hdu_list([primary_hdu, ccf_extension])
        hdu_list.writeto(product, overwrite=True)
    except FileNotFoundError as err:
        log.error('Creation of %s failed: unable to open file %s', product, err.filename)
    except Exception:
        log.error('Creation of %s failed', product, exc_info=True)


def get_primary_header(exposure: Exposure) -> fits.PrimaryHDU:
    """
    Creates a primary header to use with products for a given exposure.
    Contains the header values which should be the same across various reduced files.
    :param exposure: Exposure to get the primary header for
    :return: A fits primary HDU containing the created header
    """
    hdu = fits.open(exposure.preprocessed)[0]
    fits_op.remove_keys(hdu.header, ('DRSPDATE', 'DRSPID', 'INF1000',
                                     'QCC001N', 'QCC001V', 'QCC001L', 'QCC001P',
                                     'QCC002N', 'QCC002V', 'QCC002L', 'QCC002P',
                                     'QCC_ALL',))
    return fits.PrimaryHDU(header=hdu.header)


def product_header_update(hdu_list: fits.HDUList):
    """
    Puts the finishing touches on the product header, which currently consists of:
    1. Removing data dimension keys from primary header
    2. Copying VERSION key from first extension to primary header
    3. For p.fits copying EXPTIME/MJDATE from Pol extension to primary header
    4. For non-calibration non-Err extensions, removing cards which duplicates from the primary header.
    5. Adding a COMMENT to the primary header listing the EXTNAME of each extension.
    :param hdu_list: HDUList to update
    """
    if len(hdu_list) <= 1:
        log.error('Trying to create product primary HDU with no extensions')
        return
    primary_header = hdu_list[0].header
    fits_op.remove_keys(primary_header, ('BITPIX', 'NAXIS', 'NAXIS1', 'NAXIS2'))
    primary_header.insert('PVERSION', fits_op.get_card(hdu_list[1].header, 'VERSION'), after=True)
    ext_names = []
    for extension in hdu_list[1:]:
        ext_header = extension.header
        ext_name = ext_header['EXTNAME']
        ext_names.append(ext_name)
        if ext_name == 'Pol':
            primary_header['EXPTIME'] = (ext_header['EXPTIME'], '[sec] total integration time of 4 exposures')
            primary_header['MJDATE'] = (ext_header['MJDATE'], 'Modified Julian Date at middle of sequence')
        if ext_name.startswith('Wave') or ext_name.startswith('Blaze') or ext_name.endswith('Err'):
            continue
        dupe_keys = fits_op.verify_duplicate_cards(ext_header, primary_header.items())
        fits_op.remove_keys(ext_header, dupe_keys)
    description = 'This file contains the following extensions: ' + ', '.join(ext_names)
    for line in textwrap.wrap(description, 71):
        primary_header.insert('FILENAME', ('COMMENT', line))


def get_cal_extensions(exposure: Exposure, *args) -> Iterable[fits_op.ExtensionHDU]:
    """
    Creates fits extensions for the calibrations used to reduce an exposure. The extension are created from the files
    determined by the CDBWAVE and CDBBLAZE keys in the e2ds file. The extension names will be:
    WaveAB, WaveA, WaveB, WaveC, BlazeAB, BlazeA, BlazeB, BlazeC

    Note that the headers are stripped except for structural keys and keys indicating the source of the data.

    :param exposure: Exposure to get the calibration extensions for
    :param args: The subset of extensions to create (blank for all)
    :return: The created extensions
    """

    def keep_key(key: str) -> bool:
        return key in ('EXTNAME', 'NAXIS', 'NAXIS1', 'NAXIS2') or key.startswith('INF') or key.startswith('CDB')

    def cleanup_keys(header: fits.Header):
        fits_op.remove_keys(header, [key for key in header.keys() if not keep_key(key)])

    try:
        headers_per_fiber = defaultdict(OrderedDict)
        for fiber in Fiber:
            source_file = exposure.e2ds(fiber)
            source_header = fits.open(source_file)[0].header
            for keyword in ['CDBWAVE', 'CDBBLAZE']:
                headers_per_fiber[keyword][fiber] = source_header[keyword]
        cal_path_dict = OrderedDict()
        reduced_directory = exposure.reduced_directory
        for fiber in Fiber:
            filename = headers_per_fiber['CDBWAVE'][fiber]
            cal_path_dict['Wave' + fiber.value] = reduced_directory.joinpath(filename)
        for fiber in Fiber:
            filename = headers_per_fiber['CDBBLAZE'][fiber]
            cal_path_dict['Blaze' + fiber.value] = reduced_directory.joinpath(filename)

        ext_names = args if args else cal_path_dict.keys()
        extensions = []
        for name in ext_names:
            extension = fits_op.extension_from_file(name, cal_path_dict[name])
            if name.startswith('Wave') or name.startswith('Blaze'):
                cleanup_keys(extension.header)
            extensions.append(extension)
        return extensions
    except Exception:
        raise CalExtensionError()


class CalExtensionError(Exception):
    """
    Exception used internally to indicate something went wrong during get_cal_extensions.
    """
    pass
