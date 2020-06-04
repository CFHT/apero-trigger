import pickle

from logger import log


class DataCache:
    def __init__(self, file):
        self.cache_file = file

    def load(self):
        return self.load_cache(self.cache_file)

    def save(self, data):
        self.save_cache(data, self.cache_file)

    @staticmethod
    def save_cache(cache, file):
        try:
            pickle.dump(cache, open(file, 'wb'))
        except (OSError, IOError) as e:
            log.error('Failed to save to %s', file, exc_info=True)

    @staticmethod
    def load_cache(file):
        realtime_state = pickle.load(open(file, 'rb'))
        return realtime_state


class RealtimeStateCache(DataCache):
    def __init__(self):
        super().__init__('.drstrigger-realtime.cache')


class CalibrationStateCache(DataCache):
    def __init__(self):
        super().__init__('.drstrigger-calib.cache')
