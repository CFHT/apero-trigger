from abc import ABC, abstractmethod


class RecipeFailure(Exception):
    """
    Exception representing any failure for a DRS recipe.
    """

    def __init__(self, reason: str, command_string: str):
        """
        :param reason: A short string explaining what kind of recipe failure occurred
        :param command_string: A string representation of the recipe call that failed
        """
        self.reason = reason
        self.command_string = command_string

    def __str__(self):
        return 'DRS command failed (' + self.reason + '): ' + self.command_string


class IErrorHandler(ABC):
    """
    A base class for handling recipe failures.
    """

    @abstractmethod
    def handle_recipe_failure(self, error: RecipeFailure):
        pass
