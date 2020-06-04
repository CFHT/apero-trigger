from typing import Iterable, Sequence

from trigger.baseinterface.exposure import IExposure


class SequenceStateTracker:
    def __init__(self):
        self.unmapped_exposures = []
        self.processed_exposures = set()
        self.mapped_sequences = set()  # Only tracked for saving/loading state
        self.reverse_map = {}

    def __getstate__(self):
        return {
            'unmapped_exposures': self.unmapped_exposures,
            'processed_exposures': self.processed_exposures,
            'mapped_sequences': self.mapped_sequences,
        }

    def __setstate__(self, state):
        self.unmapped_exposures = state['unmapped_exposures']
        self.processed_exposures = state['processed_exposures']
        self.mapped_sequences = set()
        self.reverse_map = {}
        self.__map_sequences(state['mapped_sequences'])

    def add_unmapped_exposures(self, exposures: Iterable[IExposure]):
        self.unmapped_exposures.extend(exposures)

    def get_unmapped_exposures(self) -> Iterable[IExposure]:
        return self.unmapped_exposures

    def mark_sequences_complete(self, sequences: Iterable[Sequence[IExposure]]):
        for sequence in sequences:
            for exposure in sequence:
                self.unmapped_exposures.remove(exposure)
        self.__map_sequences(sequences)

    def __map_sequences(self, sequences: Iterable[Sequence[IExposure]]):
        for sequence in sequences:
            for exposure in sequence:
                self.reverse_map[exposure] = tuple(sequence)
            self.mapped_sequences.add(tuple(sequence))

    def mark_exposure_processed(self, exposure: IExposure):
        self.processed_exposures.add(exposure)

    def get_sequence_if_ready_to_process(self, exposure: IExposure) -> Sequence[IExposure]:
        sequence = self.reverse_map.get(exposure)
        if sequence and all(exposure in self.processed_exposures for exposure in sequence):
            return sequence

    def done_with_sequence(self, sequence: Sequence[IExposure]):
        for exposure in sequence:
            del self.reverse_map[exposure]
            self.processed_exposures.remove(exposure)
        self.mapped_sequences.remove(tuple(sequence))
