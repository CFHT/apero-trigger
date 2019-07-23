import sys

from .utilmisc import flatten
from ..common import log, RecipeFailure


# Exception type internally used to represent a quality control failure for a DRS recipe
class QCFailure(Exception):
    def __init__(self, errors):
        super().__init__('QC failure: ' + ', '.join(errors))
        self.errors = errors


class RecipeRunner:
    def __init__(self, trace=False, log_command=True, error_handler=None):
        self.trace = trace
        self.log_command = log_command
        self.error_handler = error_handler

    def run(self, module, night, *args):
        # Get a string representation of the command, ideally matching what the command line call would be
        command_string = ' '.join((module.__NAME__, night, *map(str, flatten(args))))
        if self.log_command:
            log.info(command_string)
        try:
            return self.__run(module, night, *args)
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

    def __run(self, module, night, *args):
        if self.trace:
            return True
        else:
            sys.argv = [sys.argv[0]]  # Wipe out argv so DRS doesn't rely on CLI arguments instead of what is passed in
            locals = module.main(night, *args)
            qc_passed = locals.get('passed')
            qc_failures = locals.get('fail_msg')
            if qc_failures and not qc_passed:
                raise QCFailure(qc_failures)
            return True

    def __handle_error(self, error):
        if self.error_handler:
            self.error_handler.handle_recipe_failure(error)
