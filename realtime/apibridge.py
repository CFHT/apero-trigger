from __future__ import annotations

from multiprocessing import Queue
from pathlib import Path
from typing import Iterable

from logger import log
from trigger.baseinterface.drstrigger import IDrsTrigger
from trigger.baseinterface.exposure import IExposure
from .manager import IExposureApi


class ApiBridge(IExposureApi):
    def __init__(self, file_queue: Queue[Path], session_root: str, trigger: IDrsTrigger):
        self.queue = file_queue
        self.session_root = session_root
        self.trigger = trigger

    def get_new_exposures(self, cursor) -> Iterable[IExposure]:
        exposures = []
        while not self.queue.empty():
            file = self.queue.get(block=False)
            try:
                exposure = self.__setup_symlink(file)
                exposures.append(exposure)
            except RuntimeError as err:
                log.error('Failed to create link to %s: %s', str(file), str(err))
        return exposures

    def __setup_symlink(self, session_path: Path) -> IExposure:
        exposure = self.__exposure_from_session_path(session_path)
        link_path = exposure.raw
        try:
            link_path.parent.mkdir(parents=True, exist_ok=True)
            link_path.symlink_to(session_path)
        except FileExistsError:
            pass
        except OSError as err:
            log.error('Failed to create night directory %s due to %s', str(link_path.parent), str(err))
        return exposure

    def __exposure_from_session_path(self, session_path: Path) -> IExposure:
        try:
            relative_path = Path(session_path).relative_to(Path(self.session_root))
        except ValueError:
            raise RuntimeError('Night directory should start with ' + str(self.session_root))
        night = relative_path.parent.name
        filename = relative_path.name
        return self.trigger.Exposure(night, filename)
