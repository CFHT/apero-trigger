from __future__ import annotations

from multiprocessing import Queue
from pathlib import Path
from typing import Iterable

from logger import log
from trigger.baseinterface.drstrigger import IDrsTrigger
from trigger.baseinterface.exposure import IExposure
from .manager import IExposureApi


class ApiBridge(IExposureApi):
    def __init__(self, file_queue: Queue[Path], trigger: IDrsTrigger):
        self.queue = file_queue
        self.trigger = trigger

    def get_new_exposures(self, cursor) -> Iterable[IExposure]:
        exposures = []
        while not self.queue.empty():
            file = self.queue.get(block=False)
            try:
                exposure = self.trigger.exposure_from_path(file)
                exposures.append(exposure)
            except RuntimeError as err:
                log.error('Failed to create link to %s: %s', str(file), str(err))
        return exposures
