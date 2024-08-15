from __future__ import annotations

from pathlib import Path
from typing import Optional

from .drsconstants import Fiber, RootDataDirectories


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


class Exposure:
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

    def __repr__(self):
        return str(self.raw)

    def __str__(self):
        return str(self.raw)

    def __eq__(self, other):
        return (self.night, self.raw.name) == (other.night, other.raw.name)

    def __hash__(self):
        return hash((self.night, self.raw.name))

    @property
    def night(self) -> str:
        return self.__night.night

    @property
    def raw(self) -> Path:
        return Path(self.input_directory, self.__raw_filename)

    @property
    def preprocessed(self) -> Path:
        return Path(self.temp_directory, self.raw.name.replace('.fits', '_pp.fits'))

    def q2ds(self, fiber: Fiber, flat_fielded=True) -> Path:
        product_name = 'q2dsff' if flat_fielded else 'q2ds'
        return self.reduced(product_name + '_' + fiber.value)

    def reduced(self, product: str) -> Path:
        return Path(self.reduced_directory, self.preprocessed.name.replace('.fits', '_' + product + '.fits'))

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
