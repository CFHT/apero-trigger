from astropy.io import fits

from .fitsoperations import remove_keys


class HDUBuilder:
    @staticmethod
    def extension_from_file(ext_name, input_file, input_extension=0, header_operation=None):
        hdu_list = fits.open(input_file)
        hdu = hdu_list[input_extension]
        return HDUBuilder.extension_from_hdu(ext_name, hdu, header_operation)

    @staticmethod
    def extension_from_hdu(ext_name, hdu, header_operation=None):
        if ext_name:
            if 'XTENSION' in hdu.header:
                if 'TFIELDS' in hdu.header:
                    hdu.header.insert('TFIELDS', ('EXTNAME', ext_name), after=True)
                else:
                    hdu.header.insert('GCOUNT', ('EXTNAME', ext_name), after=True)
            else:
                hdu.header.insert(0, ('EXTNAME', ext_name))
        if header_operation:
            header_operation(hdu.header)
        return hdu


class BinTableBuilder:
    @staticmethod
    def column_config(**kwargs):
        return kwargs

    @staticmethod
    def column_from_bin_table(input_hdu, col_num, **kwargs):
        column = input_hdu.columns[col_num]
        kwargs.setdefault('name', column.name)
        kwargs.setdefault('format', column.format)
        kwargs.setdefault('unit', column.unit)
        kwargs['array'] = input_hdu.data.field(col_num)
        return fits.Column(**kwargs)

    @staticmethod
    def column_from_image(input_hdu, col_num, **kwargs):
        bitpix_format_map = {16: 'I', 32: 'J', 64: 'K', -32: 'E', -64: 'D'}
        default_format = bitpix_format_map.get(input_hdu.header.get('BITPIX'))
        default_unit = input_hdu.header.get('BUNIT')
        kwargs.setdefault('format', default_format)
        kwargs.setdefault('unit', default_unit)
        kwargs['array'] = input_hdu.data[col_num]
        return fits.Column(**kwargs)

    @classmethod
    def hdu_from_image(cls, ext_name, input_hdu, column_configs):
        header = input_hdu.header.copy()
        if 'BUNIT' in header:
            header.remove('BUNIT')
        assert len(column_configs) == len(input_hdu.data)
        columns = [cls.column_from_image(input_hdu, num, **config) for num, config in enumerate(column_configs)]
        bin_table_hdu = fits.BinTableHDU.from_columns(columns, header=header)
        return HDUBuilder.extension_from_hdu(ext_name, bin_table_hdu)


class MEFBuilder:
    @classmethod
    def create_hdu_list(cls, hdus):
        hdu_list = fits.HDUList()
        for hdu in hdus:
            if len(hdu_list) == 0:
                hdu.header['NEXTEND'] = len(hdus) - 1
            else:
                hdu.header.remove('NEXTEND', ignore_missing=True)
            cls.wcs_clean(hdu.header)
            hdu_list.append(hdu)
        return hdu_list

    @staticmethod
    def wcs_clean(header):
        if header['NAXIS'] < 2 or header.get('XTENSION') == 'BINTABLE':
            remove_keys(header, ('CTYPE2', 'CUNIT2', 'CRPIX2', 'CRVAL2', 'CDELT2'))
        if header['NAXIS'] < 1 or header.get('XTENSION') == 'BINTABLE':
            remove_keys(header, ('CTYPE1', 'CUNIT1', 'CRPIX1', 'CRVAL1', 'CDELT1'))
