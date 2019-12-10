import time
from enum import Enum
from multiprocessing import Queue, Process, Event
from pathlib import Path
from unittest import mock


class MockApi:
    def __init__(self):
        self.new_exposures = Queue()

    def add_new_exposures(self, exposures):
        for exposure in exposures:
            self.new_exposures.put(exposure)

    def get_new_exposures(self, cursor):
        found = []
        while not self.new_exposures.empty():
            found.append(self.new_exposures.get())
        return found


class Log:
    def __init__(self):
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


class MockExposurePathFactory:
    def __init__(self, root_dir):
        self.root_dir = Path(root_dir)

    def exposure(self, night, filename):
        return MockExposurePath(self.root_dir, night, Path(filename).name)


class MockExposurePath:
    def __init__(self, root_dir, night, raw_file):
        self.root_dir = root_dir
        self.night = night
        self.file = raw_file

    @property
    def raw(self):
        return self.root_dir.joinpath('raw', self.night, self.file)


class TriggerActionT(Enum):
    PREPROCESS = 'preprocess'
    PROCESS_FILE = 'file'
    PROCESS_SEQUENCE = 'sequence'

    def __repr__(self):
        return self.value


class MockTrigger:
    def __init__(self, root_dir):
        self.exposure_path_factory = MockExposurePathFactory(root_dir)
        self.processor = mock.MagicMock()
        self.log = Log()

    def preprocess(self, night, file):
        self.log.put((TriggerActionT.PREPROCESS, night, file))
        return True

    def process_file(self, night, file):
        self.log.put((TriggerActionT.PROCESS_FILE, night, file))
        return True

    def process_sequence(self, night, files):
        self.log.put((TriggerActionT.PROCESS_SEQUENCE, night, files))
        return {}

    def Exposure(self, night, filename):
        return self.exposure_path_factory.exposure(night, filename)


class ProcessRunner:
    def __init__(self, target, args):
        self.target = target
        self.args = args

    def start(self):
        return RunningProcess(self.target, self.args)

    def run_for(self, interval):
        rp = self.start()
        time.sleep(interval)
        rp.stop()


class RunningProcess:
    def __init__(self, target, args):
        self.exit_event = Event()
        p = Process(target=target, args=(*args, self.exit_event))
        p.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def stop(self):
        self.exit_event.set()
