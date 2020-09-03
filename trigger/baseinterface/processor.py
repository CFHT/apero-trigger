from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional


class RecipeFailure(Exception):
    """
    Exception representing any failure for a DRS recipe.
    """

    def __init__(self, reason: str, command_string: Optional[str] = None, traceback_string: Optional[str] = None):
        """
        :param reason: A short string explaining what kind of recipe failure occurred
        :param command_string: A string representation of the recipe call that failed
        """
        self.reason = reason
        self.command_string = command_string
        self.traceback_string = traceback_string

    def __str__(self):
        if self.command_string:
            return 'DRS command failed (' + self.reason + '): ' + self.command_string
        return 'DRS command failed (' + self.reason + ')'

    def full_string(self):
        if self.traceback_string:
            return str(self) + '\n' + self.traceback_string
        return str(self)

    def from_command(self, command_string: str) -> RecipeFailure:
        return RecipeFailure(self.reason, command_string, self.traceback_string)

    def with_traceback_string(self, traceback_string: str) -> RecipeFailure:
        return RecipeFailure(self.reason, self.command_string, traceback_string)


class IErrorHandler(ABC):
    """
    A base class for handling recipe failures.
    """

    @abstractmethod
    def handle_recipe_failure(self, error: RecipeFailure):
        pass
