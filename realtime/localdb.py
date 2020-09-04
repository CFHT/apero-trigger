import pickle
import time
from pathlib import Path
from typing import Generic, Optional, TypeVar

from filelock import BaseFileLock, SoftFileLock

from logger import log

T = TypeVar('T')


class DataCache(Generic[T]):
    def __init__(self, file: Path, locking: bool = False):
        self.cache_file = file
        self.lock = None
        if locking:
            lockfile = file.with_suffix(file.suffix + '.lock')
            retry = True
            while retry:
                try:
                    lockfile.unlink()
                    retry = False
                except FileNotFoundError:
                    retry = False
                except OSError:
                    pass
                    time.sleep(3.0)
            self.lock = SoftFileLock(lockfile)

    def load(self) -> T:
        return self.load_cache(self.cache_file, self.lock)

    def save(self, data: T):
        self.save_cache(data, self.cache_file, self.lock)

    def unlock(self):
        if self.lock is not None:
            self.lock.release()

    @staticmethod
    def save_cache(cache: T, file: Path, lock: Optional[BaseFileLock] = None):
        try:
            pickle.dump(cache, open(file, 'wb'))
        except (OSError, IOError) as e:
            log.error('Failed to save to %s', file, exc_info=e)
        if lock is not None:
            lock.release()

    @staticmethod
    def load_cache(file: Path, lock: Optional[BaseFileLock] = None) -> T:
        if lock is not None:
            lock.acquire()
        realtime_state = pickle.load(open(file, 'rb'))
        return realtime_state
