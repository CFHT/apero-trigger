import queue
import time
from enum import Enum
from functools import partial
from multiprocessing import Event, Manager, Process, Queue, Value
from pathlib import Path
from typing import Any, NamedTuple
from unittest import mock

from realtime.manager import IExposureApi, start_realtime
from realtime.process import init_realtime_process, process_from_queues
from trigger.baseinterface.drstrigger import IDrsTrigger
from trigger.baseinterface.exposure import IExposure


class MockApi(IExposureApi):
    def __init__(self, trigger):
        self.new_exposures = Queue()
        self.trigger = trigger

    def add_new_exposures(self, exposures):
        for exposure in exposures:
            self.new_exposures.put(exposure)

    def get_new_exposures(self, cursor):
        found = []
        while not self.new_exposures.empty():
            try:
                exposure_path = self.new_exposures.get(block=False)
                exposure = self.trigger.exposure_from_path(Path(exposure_path))
                found.append(exposure)
            except queue.Empty:
                pass
        return found


class Log:
    def __init__(self, managed_list=None):
        self.data = managed_list
        if managed_list is None:
            manager = Manager()
            self.data = manager.list()

    def put(self, item):
        self.data.append(item)

    def __str__(self):
        return '\n'.join(str(item) for item in self.data)


def instant_log(log_queue):
    log = Log()
    while not log_queue.empty():
        try:
            log.put(log_queue.get(block=False))
        except queue.Empty:
            pass
    return list(log.data)


def last_index(iterable, value):
    return len(iterable) - 1 - iterable[::-1].index(value)


class MockExposure(IExposure):
    def __init__(self, root_dir, night, raw_file):
        self.root_dir = root_dir
        self.__night = night
        self.__file = raw_file

    @property
    def night(self) -> str:
        return self.__night

    @property
    def raw(self) -> Path:
        return self.root_dir.joinpath('raw', self.__night, self.__file)

    @property
    def preprocessed(self) -> Path:
        return self.root_dir.joinpath('tmp', self.__night, self.__file)

    @staticmethod
    def from_path(file_path: Path, root_dir: Path):
        try:
            relative_path = Path(file_path).relative_to(Path(root_dir))
        except ValueError:
            raise RuntimeError('Night directory should start with ' + str(root_dir))
        night = relative_path.parent.name
        filename = relative_path.name
        return MockExposure(root_dir, night, filename)


class MockExposureMetadata:
    def __init__(self, exposure):
        stem = exposure.raw.stem
        parts = stem.split('-')
        self.exposure = exposure
        self.group = parts[0]
        self.index = int(parts[1]) - 1
        self.count = int(parts[2])
        self.time = float(parts[3]) * 0.1


def mock_sequence_finder(exposures):
    sequences = []
    groups = {}
    for exposure in exposures:
        exposure_metadata = MockExposureMetadata(exposure)
        assert exposure_metadata.index < exposure_metadata.count
        if exposure_metadata.group in groups:
            sequence = groups[exposure_metadata.group]
            assert sequence[-1].count == exposure_metadata.count
            assert all(existing_exposure.index != exposure_metadata.index for existing_exposure in sequence)
            sequence.append(exposure_metadata)
        else:
            sequence = [exposure_metadata]
            groups[exposure_metadata.group] = sequence
        if len(sequence) == exposure_metadata.count:
            sequence.sort(key=lambda exp: exp.index)
            sequences.append(tuple(map(lambda exp: exp.exposure, sequence)))
    return sequences


class TriggerActionT(Enum):
    PREPROCESS = 'preprocess'
    PROCESS_FILE = 'file'
    PROCESS_SEQUENCE = 'sequence'

    def __repr__(self):
        return self.value


class MockCache(mock.MagicMock):
    def __reduce__(self):
        return mock.MagicMock, ()


class MockProcessor(mock.MagicMock):
    def __getstate__(self):
        return {
        }

    def __setstate__(self, state):
        pass


class MockTrigger(IDrsTrigger):
    def __init__(self, root_dir, session_dir, managed_list=None):
        self.root_dir = root_dir
        self.session_dir = session_dir
        self.processor = MockProcessor()
        self.log = Log(managed_list)

    def __reduce__(self):
        return self.__class__, (self.root_dir, self.session_dir, self.log.data)

    def preprocess(self, exposure):
        self.log.put((TriggerActionT.PREPROCESS, exposure))
        return True

    def process_file(self, exposure):
        self.log.put((TriggerActionT.PROCESS_FILE, exposure))
        return True

    def process_sequence(self, exposures):
        self.log.put((TriggerActionT.PROCESS_SEQUENCE, exposures))
        return {}

    def exposure(self, night, filename):
        return MockExposure(Path(self.root_dir), night, filename)

    def exposure_from_path(self, path: Path) -> IExposure:
        return MockExposure.from_path(path, self.session_dir)

    def reduce(self, exposures_in_order):
        pass

    @staticmethod
    def find_sequences(exposures, **kwargs):
        pass

    @property
    def calibration_state(self):
        return None

    @calibration_state.setter
    def calibration_state(self, state):
        pass

    def reset_calibration_state(self):
        pass


class LogActions(NamedTuple):
    exposure_pre: Any
    exposure_post: Any
    sequence: Any


def consistency_check_general(log, exposures, log_actions):
    sequences = mock_sequence_finder(exposures)
    try:
        assert len(set(log.data)) == 2 * len(exposures) + len(sequences)
    except AssertionError:
        print(log)
        raise
    for exposure in exposures:
        exposure_pre_entry = (log_actions.exposure_pre, exposure)
        assert exposure_pre_entry in log.data
        exposure_post_entry = (log_actions.exposure_post, exposure)
        assert exposure_post_entry in log.data
        assert log.data.index(exposure_pre_entry) < log.data.index(exposure_post_entry)
        assert last_index(log.data, exposure_pre_entry) < last_index(log.data, exposure_post_entry)
    for sequence in sequences[0:1]:
        sequence_entry = (log_actions.sequence, sequence)
        assert sequence_entry in log.data
        sequence_index = log.data.index(sequence_entry)
        for exposure in sequence:
            last_exposure_post_index = last_index(log.data, (log_actions.exposure_post, exposure))
            assert last_exposure_post_index < sequence_index


class StartRealtimeParams(NamedTuple):
    num_processes: int
    fetch_interval: float
    tick_interval: float
    subprocess_tick_interval: float


def start_realtime_blocking(api, cache, processor, params, started_running, finished_running, stop_running):
    process_from_queues_partial = partial(process_from_queues, processor)
    start_realtime(mock_sequence_finder, api, cache,
                   init_realtime_process, process_from_queues_partial,
                   params.num_processes, params.fetch_interval, params.tick_interval,
                   params.subprocess_tick_interval,
                   started_running, finished_running, stop_running)


def stop_after_n_finish(finished_running: Value, target_n: int, stop_running: Event, tick_interval: float):
    last_count = 0
    while last_count < target_n:
        with finished_running.get_lock():
            last_count = finished_running.value
        time.sleep(tick_interval)
    stop_running.set()


def start_realtime_blocking_until_n_finish(api, cache, processor, params, n):
    started_running = Event()
    finished_running = Value('i', 0)
    stop_running = Event()
    p = Process(target=stop_after_n_finish, args=(finished_running, n, stop_running, 0.1))
    p.start()
    start_realtime_blocking(api, cache, processor, params, started_running, finished_running, stop_running)
    p.join()
    return finished_running.value
