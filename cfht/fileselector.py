from typing import Collection

from trigger.baseinterface.steps import Step
from trigger.common import Exposure, ExposureConfig
from trigger.fileselector import FileSelector, SingleFileSelector
from .steps import CfhtStep


class CfhtFileSelector(FileSelector):
    def single_file_selector(self, exposure: Exposure, steps: Collection[Step]) -> SingleFileSelector:
        return CfhtSingleFileSelector(exposure, steps)


class CfhtSingleFileSelector(SingleFileSelector):
    @staticmethod
    def is_exposure_config_used_for_step(exposure_config: ExposureConfig, step: Step):
        if exposure_config.object and isinstance(step, CfhtStep):
            return True
        return SingleFileSelector.is_exposure_config_used_for_step(exposure_config, step)

    @staticmethod
    def is_object_step(step: Step) -> bool:
        return SingleFileSelector.is_object_step(step) or isinstance(step, CfhtStep)
