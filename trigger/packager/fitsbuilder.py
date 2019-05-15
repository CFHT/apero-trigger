from astropy.io import fits

from .fitsoperations import remove_keys


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
        hdu_list = fits.open(self.path)
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
        if header['NAXIS'] < 1 or header.get('XTENSION') == 'BINTABLE':
            remove_keys(header, ('CTYPE1', 'CUNIT1', 'CRPIX1', 'CRVAL1', 'CDELT1'))
