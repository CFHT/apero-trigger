from __future__ import annotations

from abc import ABC, abstractmethod

from .recipefailure import RecipeFailure


class IErrorHandler(ABC):
    """
    A base class for handling recipe failures.
    """

    @abstractmethod
    def handle_recipe_failure(self, error: RecipeFailure):
        pass
