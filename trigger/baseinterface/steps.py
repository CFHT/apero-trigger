from abc import ABC, abstractmethod
from enum import Enum
from typing import Type, Set, Iterable

_DEFAULT_OFF_VALUE = 'DEFAULT_OFF_VALUE'


class Step(Enum):
    """
    Enum base class representing a processing step that can be enabled or disabled.
    """
    pass


def off_by_default():
    """
    Function to call in place of enum.auto() to indicate that a given step is off unless explicitly enabled.
    In other words, all() will not include the step if its value is set to off_by_default().
    """
    return _DEFAULT_OFF_VALUE


class StepsFactory:
    """
    Class used to convert string keys to sets of steps.
    """

    def __init__(self, enum: Type[Step], all_key: str = None):
        """
        :param enum: The Step enum subclass
        :param all_key: A key which is treated synonymously with calling all()
        """
        self.enum = enum
        self.all_key = all_key

    def all(self) -> Set[Step]:
        """
        :return: A set containing all entries in the enum, excluding those which are off_by_default
        """
        return set((const for const in self.enum if const.value != _DEFAULT_OFF_VALUE))

    def from_keys(self, keys: Iterable[str]) -> Set[Step]:
        """
        Converts input strings to enum entries where the enum keys match after converting to uppercase.
        :param keys: The strings to convert
        :return: :return: A set of entries in the enum subclass where the keys match
        """
        if self.all_key and self.all_key in keys:
            return self.all()
        steps = set()
        for key in keys:
            try:
                steps.add(self.enum[key.upper()])
            except KeyError:
                pass
        return steps


class Steps(ABC):
    """
    Base class used to combine multiple StepsFactory instances.
    """

    @classmethod
    @abstractmethod
    def _steps_factories(cls) -> Iterable[StepsFactory]:
        """
        Override this method to specify which StepsFactory instances will be combined.
        :return: All StepsFactory instances which will be used
        """
        pass

    @classmethod
    def all(cls) -> Set[Step]:
        """
        :return: A set containing all entries in all the enums, excluding those which are off_by_default
        """
        return set.union(*(factory.all() for factory in cls._steps_factories()))

    @classmethod
    def from_keys(cls, keys: Iterable[str]) -> Set[Step]:
        """
        Converts input strings to enum entries where the enum keys match after converting to uppercase.
        :param keys: The strings to convert
        :return: :return: A set of entries in the enum subclass where the keys match
        """
        return set.union(*(factory.from_keys(keys) for factory in cls._steps_factories()))
