import collections
import sys
import typing

from logger import log
from ...baseinterface.processor import RecipeFailure, IErrorHandler


def flatten(items: typing.Iterable) -> typing.Iterable:
    """
    Takes a nested iterable structure and recursively flattens it to a 1d iterable.
    """
    for x in items:
        if isinstance(x, collections.Iterable) and not isinstance(x, str):
            yield from flatten(x)
        else:
            yield x


class QCFailure(Exception):
    """
    Exception used internally used to represent a quality control failure for a DRS recipe.
    """

    def __init__(self, errors):
        super().__init__('QC failure: ' + ', '.join(errors))
        self.errors = errors


class RecipeRunner:
    def __init__(self, trace: bool = False, log_command: bool = True, error_handler: IErrorHandler = None):
        self.trace = trace
        self.log_command = log_command
        self.error_handler = error_handler

    def run(self, module, *args, **kwargs) -> bool:
        # Get a string representation of the command, ideally matching what the command line call would be
        arg_strings = map(str, flatten(args))
        kwarg_strings = tuple('--{}={}'.format(k, v) for k, v in kwargs.items())
        command_string = ' '.join((module.__NAME__, *arg_strings, *kwarg_strings))
        if self.log_command:
            log.info(command_string)
        try:
            return self.__run(module, *args, **kwargs)
        except SystemExit:
            failure = RecipeFailure('system exit', command_string)
            log.error(failure)
            self.__handle_error(failure)
        except QCFailure:
            failure = RecipeFailure('QC failure', command_string)
            log.error(failure)
            self.__handle_error(failure)
        except Exception:
            failure = RecipeFailure('uncaught exception', command_string)
            log.error(failure, exc_info=True)
            self.__handle_error(failure)
        return False

    def __run(self, module, *args, **kwargs) -> bool:
        if self.trace:
            return True
        else:
            sys.argv = [sys.argv[0]]  # Wipe out argv so DRS doesn't rely on CLI arguments instead of what is passed in
            returned_locals = module.main(*args, **kwargs)
            qc_passed = returned_locals.get('passed')
            qc_failures = returned_locals.get('fail_msg')
            if qc_failures and not qc_passed:
                raise QCFailure(qc_failures)
            return True

    def __handle_error(self, error: RecipeFailure):
        if self.error_handler:
            self.error_handler.handle_recipe_failure(error)
