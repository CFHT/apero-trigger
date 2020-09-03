import collections
import sys
import typing
from multiprocessing import Pool

from logger import log
from ...baseinterface.processor import IErrorHandler, RecipeFailure


def flatten(items: typing.Iterable) -> typing.Iterable:
    """
    Takes a nested iterable structure and recursively flattens it to a 1d iterable.
    """
    for x in items:
        if isinstance(x, collections.Iterable) and not isinstance(x, str):
            yield from flatten(x)
        else:
            yield x


class RecipeRunner:
    def __init__(self, trace: bool = False, log_command: bool = True, error_handler: IErrorHandler = None):
        self.trace = trace
        self.log_command = log_command
        self.error_handler = error_handler
        # Could set this true for offline processing to avoid memory leaks?
        self.forking = False

    def run(self, module, *args, **kwargs) -> bool:
        # Get a string representation of the command, ideally matching what the command line call would be
        arg_strings = map(str, flatten(args))
        kwarg_strings = tuple('--{}={}'.format(k, v) for k, v in kwargs.items())
        command_string = ' '.join((module.__NAME__, *arg_strings, *kwarg_strings))
        if self.log_command:
            log.info(command_string)
        try:
            return self.__run(module, *args, **kwargs)
        except RecipeFailure as e:
            failure = e.from_command(command_string)
            log.error(failure.full_string())
            self.__handle_error(failure)
        except SystemExit:
            failure = RecipeFailure('system exit', command_string)
            log.error(failure)
            self.__handle_error(failure)
        except Exception as e:
            failure = RecipeFailure('uncaught exception', command_string)
            log.error(failure, exc_info=e)
            self.__handle_error(failure)
        return False

    def __run(self, module, *args, **kwargs) -> bool:
        if self.trace:
            return True
        else:
            if self.forking:
                result = fork_recipe(module.main, *args, **kwargs)
            else:
                result = call_recipe(module.main, *args, **kwargs)
            if not result.get('success'):
                traceback = result.get('traceback')
                if traceback:
                    raise RecipeFailure('exception').with_traceback_string(traceback)
                else:
                    raise RecipeFailure('exception')
            if not result.get('passed'):
                raise RecipeFailure('QC failure')
            pid = result.get('pid')
            return True

    def __handle_error(self, error: RecipeFailure):
        if self.error_handler:
            self.error_handler.handle_recipe_failure(error)


def call_recipe(function, *args, **kwargs):
    sys.argv = [sys.argv[0]]  # Wipe out argv so DRS doesn't rely on CLI arguments instead of what is passed in
    returned_locals = function(*args, **kwargs)
    result = {
        'pid': returned_locals.get('params', {}).get('PID'),
        'success': returned_locals.get('success'),
        'passed': returned_locals.get('passed'),
        'traceback': returned_locals.get('trace'),
    }
    return result


def fork_recipe(function, *args, **kwargs):
    with Pool(1, maxtasksperchild=1) as pool:
        result = pool.apply(call_recipe, (function, *args), **kwargs)
    return result
