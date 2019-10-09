import queue
import time
from pathlib import Path

from logger import log


class RealtimeProcessor:
    def __init__(self, trigger, session_root, calibration_cache, tick_interval):
        self.trigger = trigger
        self.session_root = session_root
        self.calibration_cache = calibration_cache
        self.tick_interval = tick_interval

    def process_from_queues(self, exposure_queue, sequence_queue, exposures_done, sequences_done):
        while True:
            self.process_next_from_queue(exposure_queue, sequence_queue, exposures_done, sequences_done)
            time.sleep(self.tick_interval)

    def process_next_from_queue(self, exposure_queue, sequence_queue, exposures_done, sequences_done):
        try:
            sequence = sequence_queue.get(block=False)
        except queue.Empty:
            try:
                exposure = exposure_queue.get(block=False)
            except queue.Empty:
                pass
            else:
                log.info('Processing %s', exposure)
                try:
                    self.__process_exposure(exposure)
                except:
                    log.error('An error occurred while processing %s', exposure, exc_info=True)
                exposures_done.put(exposure)
        else:
            log.info('Processing %s', sequence)
            try:
                self.__process_sequence(sequence)
            except:
                log.error('An error occurred while processing %s', sequence, exc_info=True)
            sequences_done.put(sequence)

    def __process_exposure(self, exposure):
        night, file = self.__setup_symlink(exposure)
        if not self.trigger.preprocess(night, file):
            return
        self.trigger.process_file(night, file)

    def __process_sequence(self, sequence):
        nights_and_files = [self.__night_and_file_from_session_path(exposure) for exposure in sequence]
        nights, files = zip(*nights_and_files)
        if len(set(nights)) > 1:
            log.error('Exposure sequence split across multiple nights: %s', sequence)
        else:
            try:
                self.trigger.processor.calibration_processor.state = self.calibration_cache.load()
            except (OSError, IOError):
                log.warning('No calibration state file found. This should only appear the first time realtime is run.')
            result = self.trigger.process_sequence(nights[0], files)
            # We only save the calibration state if the sequence was a calibration sequence.
            if 'calibrations_complete' in result:
                self.calibration_cache.save(self.trigger.processor.calibration_processor.state)

    def __setup_symlink(self, session_path):
        night, filename = self.__night_and_file_from_session_path(session_path)
        link_path = self.trigger.Exposure(night, filename).raw
        link_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            link_path.symlink_to(session_path)
        except FileExistsError as e:
            pass
        return night, filename

    def __night_and_file_from_session_path(self, session_path):
        try:
            relative_path = Path(session_path).relative_to(self.session_root)
        except ValueError:
            raise RuntimeError('Night directory should start with ' + self.session_root)
        night = relative_path.parent.name
        filename = relative_path.name
        return night, filename
