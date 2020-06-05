from collections import defaultdict, deque

from ..common import FIBER_LIST
from ..exposureconfig import CalibrationType
from ..steps import CalibrationStep


class WaitingForCalibration(Exception):
    def __init__(self, step):
        super().__init__('Missing pre-requisite calibration step: ' + step.name)


class CalibrationState():
    def __init__(self):
        self.completed_calibrations = set()
        self.calibration_sequences = defaultdict(list)
        self.remaining_queue = defaultdict(deque)


class CalibrationProcessor():
    def __init__(self, calibration_steps, drs):
        self.steps = calibration_steps
        self.drs = drs
        self.state = CalibrationState()

    def reset_state(self):
        self.state = CalibrationState()

    def add_sequence_to_queue(self, exposures, calibration_type):
        self.state.calibration_sequences[calibration_type].append(exposures)
        self.state.remaining_queue[calibration_type].append(exposures)

    def attempt_processing_queue(self):
        try:
            self.processed_sequences = []
            self.process_queue()
        except WaitingForCalibration:
            calibrations_complete = False
        else:
            calibrations_complete = True
            self.reset_state()
        return {
            'calibrations_complete': calibrations_complete,
            'processed_sequences': self.processed_sequences,
        }

    def process_queue(self):
        def badpix_logic():
            if CalibrationStep.BADPIX not in self.state.completed_calibrations:
                last_dark = self.get_last_exposure_of(CalibrationType.DARK_DARK)
                last_flat = self.get_last_exposure_of(CalibrationType.FLAT_FLAT)
                if last_dark and last_flat:
                    self.drs.cal_BADPIX(last_flat, last_dark)
                    return True

        def shape_logic():
            self.process_remaining(CalibrationType.FP_FP, lambda seq: None)
            last_fp_sequence = self.get_last_sequence_of(CalibrationType.FP_FP)
            if last_fp_sequence:
                process_shape = lambda hc_sequence: self.drs.cal_SHAPE(hc_sequence[-1], last_fp_sequence)
                return self.process_remaining(CalibrationType.HCONE_HCONE, process_shape)

        def wave_logic():
            last_fp = self.get_last_exposure_of(CalibrationType.FP_FP)
            last_hc = self.get_last_exposure_of(CalibrationType.HCONE_HCONE)
            self.drs.cal_extract_RAW(last_fp)
            self.drs.cal_extract_RAW(last_hc)
            for fiber in FIBER_LIST:
                self.drs.cal_HC_E2DS(last_hc, fiber)
            for fiber in FIBER_LIST:
                self.drs.cal_WAVE_E2DS(last_fp, last_hc, fiber)

        self.process_step_simple(CalibrationStep.DARK, CalibrationType.DARK_DARK, self.drs.cal_DARK)
        self.process_step_generalized(CalibrationStep.BADPIX, badpix_logic)
        self.process_step_simple(CalibrationStep.LOC, CalibrationType.DARK_FLAT, self.drs.cal_loc_RAW)
        self.process_step_simple(CalibrationStep.LOC, CalibrationType.FLAT_DARK, self.drs.cal_loc_RAW)
        self.process_step_generalized(CalibrationStep.SHAPE, shape_logic)
        self.process_step_simple(CalibrationStep.FF, CalibrationType.FLAT_FLAT, self.drs.cal_FF_RAW)
        if CalibrationStep.WAVE in self.steps:
            wave_logic()  # Able to assume we'll only hit this once since calibrations are complete after this step

    def process_step_simple(self, step, calibration_type, recipe):
        return self.process_step_generalized(step, lambda: self.process_remaining(calibration_type, recipe))

    def process_step_generalized(self, step, function):
        if step in self.steps:
            result = function()
            if result:
                self.state.completed_calibrations.add(step)
                return result
            if step not in self.state.completed_calibrations:
                raise WaitingForCalibration(step)

    def process_remaining(self, calibration_type, recipe):
        queue = self.state.remaining_queue[calibration_type]
        processed = []
        while queue:
            sequence = queue.popleft()
            recipe(sequence)
            processed.append(sequence)
        self.processed_sequences.extend(processed)
        return processed

    def get_last_sequence_of(self, calibration_type):
        all_sequences = self.state.calibration_sequences[calibration_type]
        if all_sequences:
            return all_sequences[-1]

    def get_last_exposure_of(self, calibration_type):
        last_sequence = self.get_last_sequence_of(calibration_type)
        if last_sequence:
            return last_sequence[-1]
