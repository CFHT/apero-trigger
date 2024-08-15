from apero.recipes.spirou import apero_extract_spirou, apero_preprocess_spirou
from apero.tools.recipes.bin import apero_processing

from trigger.drswrapper.errorhandler import IErrorHandler
from trigger.common.pathhandler import Exposure
from .reciperunner import RecipeRunner


class DRS:
    def __init__(self, trace=False, log_command=True, error_handler: IErrorHandler = None):
        self.runner = RecipeRunner(trace=trace, log_command=log_command, error_handler=error_handler)

    @property
    def trace(self):
        return self.runner.trace

    def preprocess(self, exposure: Exposure) -> bool:
        """
        :param exposure: Any exposure
        :return: Whether the recipe completed successfully
        """
        return self.runner.run(apero_preprocess_spirou, exposure.night, exposure.raw.name)

    def extract(self, exposure: Exposure, **kwargs) -> bool:
        """
        :param exposure: Any exposure that has been preprocessed
        :return: Whether the recipe completed successfully
        """
        return self.runner.run(apero_extract_spirou, exposure.night, exposure.preprocessed.name, **kwargs)

    def apero_processing(self, runfile: str, **kwargs) -> bool:
        return self.runner.run(apero_processing, runfile, trigger=True, **kwargs)
