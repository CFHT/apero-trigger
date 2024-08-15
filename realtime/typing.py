from __future__ import annotations

from abc import ABC, abstractmethod
from multiprocessing import Event
from typing import Callable, NamedTuple, Optional, Tuple

# For some reason using the usual typing method here blows up when we run tests...
ExposureQueue = 'Queue[IExposure]'
SequenceQueue = 'Queue[Sequence[IExposure]]'


class BlockingParams(NamedTuple):
    retry_interval: float
    stop_signal: Event


class IRealtimeProcessor(ABC):
    def __init__(self):
        self.process_id = 0

    @abstractmethod
    def process_next_from_queue(self, exposure_queue: ExposureQueue,
                                exposures_done: ExposureQueue,
                                block: Optional[BlockingParams] = None) -> bool:
        pass


InitArgs = Tuple[float, Event, ExposureQueue, ExposureQueue]
InitProcess = Callable[[float, Event, ExposureQueue, ExposureQueue], None]
ProcessFromQueues = Callable[[], bool]
