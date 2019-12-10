from logger import log
from .manager import Realtime


def start_realtime(find_sequences, remote_api, realtime_cache, process_from_queues, num_processes,
                   fetch_interval, tick_interval, exit_event):
    realtime = Realtime(find_sequences, remote_api, realtime_cache)
    try:
        realtime = realtime_cache.load()
        realtime.inject(find_sequences, remote_api, realtime_cache)
    except (OSError, IOError):
        log.warning('No realtime state file found. This should only appear the first time realtime is run.')
    realtime.main(num_processes, process_from_queues, fetch_interval, tick_interval, exit_event)
