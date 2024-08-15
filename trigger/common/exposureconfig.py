from __future__ import annotations

from pathlib import Path
from typing import Union

from astropy.io import fits

from logger import log


class ExposureConfig:
    def __init__(self, obj: bool, is_aborted=False):
        self.object = obj
        self.is_aborted = is_aborted

    @classmethod
    def from_file(cls, file: Path) -> ExposureConfig:
        header_checker = HeaderChecker(file)
        return cls.from_header_checker(header_checker)

    @classmethod
    def from_header_checker(cls, header_checker: HeaderChecker) -> ExposureConfig:
        return cls(obj=header_checker.is_object(), is_aborted=header_checker.is_aborted())


class HeaderChecker:
    MIN_EXP_TIME_RATIO_THRESHOLD = 0.1
    __header: Union[fits.Header, None]

    def __init__(self, file: Path):
        self.file: Path = file
        self.__header = None

    @property
    def header(self) -> fits.Header:
        self.__lazy_loading()
        return self.__header

    def __lazy_loading(self):
        if not self.__header:
            hdulist = fits.open(self.file)
            self.__header = hdulist[0].header

    def get_dpr_type(self) -> str:
        if 'DPRTYPE' not in self.header or self.header['DPRTYPE'] == 'None':
            raise RuntimeError('File missing DPRTYPE keyword', self.file)
        return self.header['DPRTYPE']

    def is_object(self) -> bool:
        return self.header.get('OBSTYPE') == 'OBJECT'

    def is_aborted(self) -> bool:
        if 'EXPTIME' not in self.header or 'EXPREQ' not in self.header:
            log.warning('%s missing EXPTIME/EXPREQ in header, assuming not an aborted exposure', self.file)
            return False
        return self.header['EXPTIME'] / self.header['EXPREQ'] < self.MIN_EXP_TIME_RATIO_THRESHOLD
