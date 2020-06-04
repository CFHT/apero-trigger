from __future__ import annotations

from pathlib import Path

from .common.exposureconfig import ExposureConfig
from .headerchecker import SpirouHeaderChecker


class SpirouExposureConfig(ExposureConfig):
    @classmethod
    def from_file(cls, file: Path) -> ExposureConfig:
        header_checker = SpirouHeaderChecker(file)
        return cls.from_header_checker(header_checker)
