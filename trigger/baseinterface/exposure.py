from abc import ABC, abstractmethod
from pathlib import Path


class IExposure(ABC):
    """
    Class representing a single input file and corresponding output files.
    """
    def __repr__(self):
        return str(self.raw)

    def __str__(self):
        return str(self.raw)

    def __eq__(self, other):
        return (self.night, self.raw.name) == (other.night, other.raw.name)

    def __hash__(self):
        return hash((self.night, self.raw.name))

    @property
    @abstractmethod
    def night(self) -> str:
        pass

    @property
    @abstractmethod
    def raw(self) -> Path:
        pass

    @property
    @abstractmethod
    def preprocessed(self) -> Path:
        pass
