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
    def process_next_from_queue(self, exposure_queue: ExposureQueue, sequence_queue: SequenceQueue,
                                exposures_done: ExposureQueue, sequences_done: SequenceQueue,
                                block: Optional[BlockingParams] = None) -> bool:
        pass


InitArgs = Tuple[float, Event, ExposureQueue, SequenceQueue, ExposureQueue, SequenceQueue]
InitProcess = Callable[[float, Event, ExposureQueue, SequenceQueue, ExposureQueue, SequenceQueue], None]
ProcessFromQueues = Callable[[], bool]
