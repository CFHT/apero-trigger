from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Optional

from .drsconstants import CcfParams, Fiber, RootDataDirectories
from ..baseinterface.exposure import IExposure


class Night:
    """
    Class representing a single directory of input data and corresponding output directories.
    """

    def __init__(self, night: str):
        """
        :param night: Path of the night directory relative to the root input directory
        """
        self.night = night

    @property
    def input_directory(self) -> Path:
        return RootDataDirectories.input.joinpath(self.night)

    @property
    def temp_directory(self) -> Path:
        return RootDataDirectories.tmp.joinpath(self.night)

    @property
    def reduced_directory(self) -> Path:
        return RootDataDirectories.reduced.joinpath(self.night)


class SampleSpace(Enum):
    VELOCITY = 'v'
    WAVELENGTH = 'w'


class TelluSuffix(Enum):
    NONE = ''
    TCORR = '_tcorr'
    RECON = '_recon'

    @staticmethod
    def tcorr(tellu_corrected: bool) -> TelluSuffix:
        if tellu_corrected:
            return TelluSuffix.TCORR
        return TelluSuffix.NONE


class Exposure(IExposure):
    """
    Class representing a single input file and corresponding output files.
    """

    def __init__(self, night: str, raw_file: str):
        """
        :param night: Path of the night directory relative to the root input directory
        :param raw_file: Name or path of the input file, which must be located directly in night directory
        """
        self.__night = Night(night)
        self.__raw_filename = Path(raw_file).name

    @property
    def night(self) -> str:
        return self.__night.night

    @property
    def raw(self) -> Path:
        return Path(self.input_directory, self.__raw_filename)

    @property
    def preprocessed(self) -> Path:
        return Path(self.temp_directory, self.raw.name.replace('.fits', '_pp.fits'))

    def s1d(self, sample_space: SampleSpace, fiber: Fiber, tellu_suffix=TelluSuffix.NONE) -> Path:
        product_name = 's1d_' + sample_space.value
        return self.__extracted_product(product_name, fiber, tellu_suffix)

    def e2ds(self, fiber: Fiber, tellu_suffix=TelluSuffix.NONE, flat_fielded=True, suffix=None) -> Path:
        product_name = 'e2dsff' if flat_fielded else 'e2ds'
        return self.__extracted_product(product_name, fiber, tellu_suffix, suffix)

    def q2ds(self, fiber: Fiber, flat_fielded=True) -> Path:
        product_name = 'q2dsff' if flat_fielded else 'q2ds'
        return self.__extracted_product(product_name, fiber, TelluSuffix.NONE)

    def ccf(self, fiber=Fiber.AB, tellu_suffix=TelluSuffix.TCORR) -> Path:
        suffix = CcfParams.mask.replace('.mas', '') + '_' + fiber.value
        return self.e2ds(Fiber.AB, tellu_suffix, suffix='ccf_' + suffix)

    def __extracted_product(self, product: str, fiber: Fiber, tellu_suffix: TelluSuffix, suffix: str = None) -> Path:
        return self.__extracted_product_general(product + tellu_suffix.value, fiber, suffix)

    def __extracted_product_general(self, product: str, fiber: Fiber, suffix: str) -> Path:
        if suffix:
            return self.reduced(product + '_' + fiber.value + '_' + suffix)
        return self.reduced(product + '_' + fiber.value)

    def reduced(self, product: str) -> Path:
        return Path(self.reduced_directory, self.preprocessed.name.replace('.fits', '_' + product + '.fits'))

    def final_product(self, letter: str) -> Path:
        return Path(self.reduced_directory, self.raw.name.replace('o.fits', letter + '.fits'))

    @property
    def input_directory(self) -> Path:
        return self.__night.input_directory

    @property
    def temp_directory(self) -> Path:
        return self.__night.temp_directory

    @property
    def reduced_directory(self) -> Path:
        return self.__night.reduced_directory

    @property
    def obsid(self) -> str:
        return self.raw.stem

    @property
    def odometer(self) -> int:
        return int(self.obsid[:-1])

    @staticmethod
    def from_path(file_path: Path, custom_raw_root: Optional[Path] = None) -> Exposure:
        root_dir = RootDataDirectories.input if custom_raw_root is None else custom_raw_root
        try:
            relative_path = Path(file_path).relative_to(Path(root_dir))
        except ValueError:
            raise RuntimeError('Night directory should start with ' + str(root_dir))
        night = relative_path.parent.name
        filename = relative_path.name
        return Exposure(night, filename)
