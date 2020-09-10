from abc import ABC, abstractmethod
from pathlib import Path
from typing import Tuple, Union

from astropy.io import fits

# Ideally, we should use a python datetime, but since we are using MJD this works for Spirou cases.
DateTime = float


class HeaderChecker(ABC):
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

    @abstractmethod
    def is_object(self) -> bool:
        pass

    @abstractmethod
    def get_object_name(self) -> str:
        pass

    @abstractmethod
    def is_sky(self) -> bool:
        pass

    @abstractmethod
    def get_exposure_index_and_total(self) -> Tuple[int, int]:
        pass

    @abstractmethod
    def get_rhomb_positions(self) -> Tuple[str, str]:
        pass

    @abstractmethod
    def get_obs_date(self) -> DateTime:
        pass

    @abstractmethod
    def get_runid(self) -> str:
        pass

    @abstractmethod
    def is_aborted(self) -> bool:
        pass
