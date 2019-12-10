import time
from multiprocessing import Queue
from multiprocessing.pool import Pool

from .sequencestatetracker import SequenceStateTracker


class Realtime:
    def __init__(self, sequence_finder, remote_api, local_db):
        self.__construct()
        self.inject(sequence_finder, remote_api, local_db)
        self.sequence_mapper = SequenceStateTracker()
        self.exposures_to_process = []
        self.sequences_to_process = []
        self.cursor = None

    def __construct(self):
        self.exposure_in_queue = Queue()
        self.exposure_out_queue = Queue()
        self.sequence_in_queue = Queue()
        self.sequence_out_queue = Queue()

    # Need to call this again after __setstate__ is called, e.g. after loading from pickle.
    def inject(self, sequence_finder, remote_api, local_db):
        self.sequence_finder = sequence_finder
        self.remote_api = remote_api
        self.local_db = local_db

    def __getstate__(self):
        return {key: self.__dict__[key] for key in ('sequence_mapper',
                                                    'exposures_to_process',
                                                    'sequences_to_process',
                                                    'cursor')}

    def __setstate__(self, state):
        self.__construct()
        self.__dict__.update(state)
        for exposure in self.exposures_to_process:
            self.exposure_in_queue.put(exposure)
        for sequence in self.sequences_to_process:
            self.sequence_in_queue.put(sequence)

    def main(self, num_processes, operation, fetch_interval, tick_interval, exit_event):
        Pool(num_processes, operation, (self.exposure_in_queue, self.sequence_in_queue,
                                        self.exposure_out_queue, self.sequence_out_queue))
        while not exit_event.is_set():
            self.__fetch_and_handle_new_exposures()
            fetch_time = time.time() + fetch_interval
            while time.time() < fetch_time:
                self.__queue_tick()
                time.sleep(tick_interval)

    def __fetch_and_handle_new_exposures(self):
        new_exposures = self.remote_api.get_new_exposures(self.cursor)
        if new_exposures:
            # self.cursor = new_exposures[-1].get_timestamp()  # THIS IS NOT A REAL METHOD
            self.sequence_mapper.add_unmapped_exposures(new_exposures)
            unmapped_exposures = self.sequence_mapper.get_unmapped_exposures()
            completed_sequences = self.sequence_finder(None, unmapped_exposures)
            self.sequence_mapper.mark_sequences_complete(completed_sequences)
            for exposure in new_exposures:
                self.exposure_in_queue.put(exposure)
                self.exposures_to_process.append(exposure)
            self.local_db.save(self)

    def __queue_tick(self):
        updated = False
        while not self.sequence_out_queue.empty():
            sequence = self.sequence_out_queue.get(block=False)
            self.sequences_to_process.remove(sequence)
            updated = True
        while not self.exposure_out_queue.empty():
            exposure = self.exposure_out_queue.get(block=False)
            self.exposures_to_process.remove(exposure)
            self.sequence_mapper.mark_exposure_processed(exposure)
            sequence = self.sequence_mapper.get_sequence_if_ready_to_process(exposure)
            if sequence:
                self.sequence_in_queue.put(sequence)
                self.sequences_to_process.append(sequence)
                self.sequence_mapper.done_with_sequence(sequence)
            updated = True
        if updated:
            self.local_db.save(self)
