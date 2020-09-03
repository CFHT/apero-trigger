import time
from enum import Enum
from multiprocessing import Event, Process, Queue
from pathlib import Path
from unittest import mock

from realtime.manager import IExposureApi
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
            exposure_path = self.new_exposures.get()
            exposure = self.trigger.exposure_from_path(Path(exposure_path))
            found.append(exposure)
        return found


class Log:
    def __init__(self, log_queue=None):
        self.log_queue = log_queue
        if log_queue is None:
            self.log_queue = Queue()
        self.data = []

    def put(self, item):
        self.log_queue.put(item)

    def flush(self):
        while not self.log_queue.empty():
            self.data.append(self.log_queue.get())

    def __str__(self):
        return '\n'.join(str(item) for item in self.data)


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


def files_as_exposures(root, night, files):
    return [MockExposure(root, night, Path(f).name) for f in files]


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
    def __init__(self, root_dir, session_dir):
        self.root_dir = root_dir
        self.session_dir = session_dir
        self.processor = MockProcessor()
        self.log = Log()

    def __reduce__(self):
        return self.__class__, (self.root_dir, self.log.log_queue)

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
        return MockExposure.from_path(path, Path(self.session_dir))

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


class ProcessRunner:
    def __init__(self, target, args):
        self.target = target
        self.args = args

    def start(self, block=True):
        return RunningProcess(self.target, self.args, block)

    def run_for(self, interval):
        rp = self.start()
        time.sleep(interval)
        rp.stop()


class RunningProcess:
    def __init__(self, target, args, block=True):
        started_running = Event()
        self.finished_running = Event()
        self.stop_running = Event()
        self.process = Process(target=target, args=(*args, started_running, self.finished_running, self.stop_running))
        self.process.start()
        if block:
            started_running.wait()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def stop(self, block=True):
        self.stop_running.set()
        if block:
            self.finished_running.wait()
