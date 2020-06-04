from pathlib import Path
from typing import Any, Collection, Iterable, Tuple, Union

from astropy.io import fits

from logger import log

ExtensionHDU = Union[fits.ImageHDU, fits.BinTableHDU]
HDU = Union[fits.PrimaryHDU, ExtensionHDU]


def get_card(header: fits.Header, keyword: str) -> Tuple[str, Any, str]:
    """
    Retrieve a fits card from a fits header
    :param header: The header
    :param keyword: The keyword of the card
    :return: The (keyword, value, comment) tuple of the card
    """
    return keyword, header[keyword], header.comments[keyword]


def remove_keys(header: fits.Header, keys: Iterable[str]):
    """
    Removes any of the specified keys from a fits header, if present.
    :param header: The fits header to update
    :param keys: The keys to remove
    """
    for key in keys:
        header.remove(key, ignore_missing=True)


def verify_duplicate_cards(header: fits.Header, cards: Iterable[Tuple[str, Any, str]]) -> Iterable[str]:
    """
    Searches through a fits header for expected header cards, and logs a warning for any which were not found.
    :param header: The fits header to examine
    :param cards: The cards expected to be found in the header
    :return: The expected cards which were actually found in the header
    """
    dupe_keys = []
    for card in cards:
        key, value = card[0], card[1]  # Split up so it still works if len(card) is 3
        if key in ('SIMPLE', 'EXTEND', 'NEXTEND'):
            continue
        if header.get(key) == value:
            dupe_keys.append(key)
        else:
            extname = header.get('EXTNAME')
            log.warning('Header key %s expected to be duplicate in extension %s but was not', key, extname)
    return dupe_keys


def create_hdu_list(hdus: Collection[HDU]) -> fits.HDUList:
    """
    Takes a collection of fits HDUs and converts into an HDUList ready to be saved to a file
    :param hdus: The collection of fits HDUs
    :return: A fits HDUList ready to be saved to a file
    """
    hdu_list = fits.HDUList()
    for hdu in hdus:
        if len(hdu_list) == 0:
            hdu.header['NEXTEND'] = len(hdus) - 1
        else:
            hdu.header.remove('NEXTEND', ignore_missing=True)
        hdu_list.append(hdu)
    return hdu_list


def column_from_bin_table(input_hdu: fits.BinTableHDU, col_num: int, **kwargs) -> fits.Column:
    """
    Create a fits column from a column in existing fits binary table.
    :param input_hdu: The HDU to grab the column from
    :param col_num: The index of the column to grab
    :param kwargs: Addition arguments which are passed along when creating the fits column
    :return: New fits column
    """
    column = input_hdu.columns[col_num]
    kwargs.setdefault('name', column.name)
    kwargs.setdefault('format', column.format)
    kwargs.setdefault('unit', column.unit)
    kwargs['array'] = input_hdu.data.field(col_num)
    return fits.Column(**kwargs)


def extension_from_hdu(ext_name: str, hdu: HDU) -> ExtensionHDU:
    """
    Takes a fits HDU and inserts the EXTNAME at the at the first available spot in the header.
    :param ext_name: The value for EXTNAME
    :param hdu: The HDU to update
    :return: The updated HDU
    """
    if ext_name:
        extname_card = ('EXTNAME', ext_name)
        if 'XTENSION' in hdu.header:
            if 'TFIELDS' in hdu.header:
                hdu.header.insert('TFIELDS', extname_card, after=True)
            else:
                hdu.header.insert('GCOUNT', extname_card, after=True)
        else:
            hdu.header.insert(0, extname_card)
    return hdu


def extension_from_file(ext_name: str, input_file: Path, input_extension=0) -> ExtensionHDU:
    """
    Takes a fits file and inserts the EXTNAME at the at the first available spot in the header of the specified HDU.
    :param ext_name: The value for EXTNAME
    :param input_file: The fits file
    :param input_extension: The index of the HDU to use
    :return: The updated HDU
    """
    hdu_list = fits.open(input_file)
    hdu = hdu_list[input_extension]
    return extension_from_hdu(ext_name, hdu)
