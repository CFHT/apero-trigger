from realtime.sequencestatetracker import SequenceStateTracker


def test_sequence_state_tracker():
    sequence_state_tracker = SequenceStateTracker()
    sequence_state_tracker.add_unmapped_exposures(('a', 'b', 'c', 'd', 'e', 'f'))
    assert sequence_state_tracker.get_unmapped_exposures() == ['a', 'b', 'c', 'd', 'e', 'f']
    sequence_state_tracker.mark_exposure_processed('a')
    assert sequence_state_tracker.get_unmapped_exposures() == ['a', 'b', 'c', 'd', 'e', 'f']
    assert sequence_state_tracker.get_sequence_if_ready_to_process('a') is None
    sequence_state_tracker.mark_sequences_complete((['a', 'b', 'c'], ('e')))
    assert sequence_state_tracker.get_unmapped_exposures() == ['d', 'f']
    assert sequence_state_tracker.get_sequence_if_ready_to_process('a') is None
    assert sequence_state_tracker.get_sequence_if_ready_to_process('e') is None
    sequence_state_tracker.mark_exposure_processed('c')
    assert sequence_state_tracker.get_sequence_if_ready_to_process('a') is None
    assert sequence_state_tracker.get_sequence_if_ready_to_process('b') is None
    assert sequence_state_tracker.get_sequence_if_ready_to_process('c') is None
    sequence_state_tracker.mark_exposure_processed('b')
    assert sequence_state_tracker.get_sequence_if_ready_to_process('a') == ('a', 'b', 'c')
    assert sequence_state_tracker.get_sequence_if_ready_to_process('b') == ('a', 'b', 'c')
    assert sequence_state_tracker.get_sequence_if_ready_to_process('c') == ('a', 'b', 'c')
    sequence_state_tracker.done_with_sequence(('a', 'b', 'c'))
    assert sequence_state_tracker.get_sequence_if_ready_to_process('a') is None
    assert sequence_state_tracker.get_sequence_if_ready_to_process('b') is None
    assert sequence_state_tracker.get_sequence_if_ready_to_process('c') is None


def test_sequence_state_tracker_save_and_load(realtime_cache):
    sequence_state_tracker = SequenceStateTracker()
    sequence_state_tracker.add_unmapped_exposures(('a', 'b', 'c', 'd', 'e', 'f'))
    sequence_state_tracker.mark_exposure_processed('a')
    sequence_state_tracker.mark_sequences_complete((['a', 'b', 'c'], ('e')))
    sequence_state_tracker.mark_exposure_processed('c')
    realtime_cache.save(sequence_state_tracker)
    sequence_state_tracker = realtime_cache.load()
    assert sequence_state_tracker.get_unmapped_exposures() == ['d', 'f']
    assert sequence_state_tracker.get_sequence_if_ready_to_process('a') is None
    assert sequence_state_tracker.get_sequence_if_ready_to_process('b') is None
    assert sequence_state_tracker.get_sequence_if_ready_to_process('c') is None
    assert sequence_state_tracker.get_sequence_if_ready_to_process('e') is None
    sequence_state_tracker.mark_exposure_processed('b')
    assert sequence_state_tracker.get_sequence_if_ready_to_process('a') == ('a', 'b', 'c')
    assert sequence_state_tracker.get_sequence_if_ready_to_process('b') == ('a', 'b', 'c')
    assert sequence_state_tracker.get_sequence_if_ready_to_process('c') == ('a', 'b', 'c')
    sequence_state_tracker.done_with_sequence(('a', 'b', 'c'))
    assert sequence_state_tracker.get_sequence_if_ready_to_process('a') is None
    assert sequence_state_tracker.get_sequence_if_ready_to_process('b') is None
    assert sequence_state_tracker.get_sequence_if_ready_to_process('c') is None
