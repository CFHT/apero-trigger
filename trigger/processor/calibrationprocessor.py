from collections import defaultdict, deque
from typing import Any, Callable, Collection, Deque, Dict, Mapping, MutableSequence, Sequence, Set, Tuple, Union

from .drswrapper import DRS
from ..baseinterface.drstrigger import ICalibrationState
from ..baseinterface.steps import Step
from ..common import CalibrationStep, CalibrationType, Exposure

ExposureSeq = Sequence[Exposure]
SimpleRecipe = Callable[[ExposureSeq], bool]
CalibrationStepCompleteKey = Union[CalibrationStep, Tuple[CalibrationStep, CalibrationType]]


class WaitingForCalibration(Exception):
    """
    Exception used internally to indicate more calibrations must be added to the queue to reach the next step.
    """

    def __init__(self, step: CalibrationStepCompleteKey):
        if isinstance(step, tuple):
            step_str = str(tuple(i.name for i in step))
        else:
            step_str = step.name
        super().__init__('Missing pre-requisite calibration step: ' + step_str)


class CalibrationState(ICalibrationState):
    def __init__(self):
        self.completed_calibrations: Set[CalibrationStepCompleteKey] = set()
        self.calibration_sequences: Mapping[CalibrationType, MutableSequence[ExposureSeq]] = defaultdict(list)
        self.remaining_queue: Mapping[CalibrationType, Deque[ExposureSeq]] = defaultdict(deque)


class CalibrationProcessor:
    def __init__(self, steps: Collection[Step], drs: DRS):
        self.steps = steps
        self.drs = drs
        self.state = CalibrationState()
        self.processed_sequences = None

    def reset_state(self, partial: bool):
        self.state = CalibrationState()
        if partial:
            self.state.completed_calibrations.add(CalibrationStep.BADPIX)
            self.state.completed_calibrations.add((CalibrationStep.THERMAL, CalibrationType.DARK_DARK_INT))
            self.state.completed_calibrations.add((CalibrationStep.THERMAL, CalibrationType.DARK_DARK_TEL))

    def add_sequence_to_queue(self, exposures: ExposureSeq, calibration_type: CalibrationType):
        self.state.calibration_sequences[calibration_type].append(exposures)
        self.state.remaining_queue[calibration_type].append(exposures)

    def attempt_processing_queue(self) -> Dict:
        try:
            self.processed_sequences = []
            self.__process_queue()
        except WaitingForCalibration:
            calibrations_complete = False
        else:
            calibrations_complete = True
        return {
            'calibrations_complete': calibrations_complete,
            'processed_sequences': self.processed_sequences,
        }

    @staticmethod
    def is_calibration_type_used_for_step(calibration_type: CalibrationType, step: CalibrationStep) -> bool:
        if step == CalibrationStep.BADPIX:
            return calibration_type in (CalibrationType.DARK_DARK_TEL, CalibrationType.FLAT_FLAT)
        elif step == CalibrationStep.LOC:
            return calibration_type in (CalibrationType.DARK_FLAT, CalibrationType.FLAT_DARK)
        elif step == CalibrationStep.SHAPE:
            return calibration_type == CalibrationType.FP_FP
        elif step == CalibrationStep.FLAT:
            return calibration_type == CalibrationType.FLAT_FLAT
        elif step == CalibrationStep.THERMAL:
            return calibration_type in (CalibrationType.DARK_DARK_INT, CalibrationType.DARK_DARK_TEL)
        elif step == CalibrationStep.WAVE:
            return calibration_type in (CalibrationType.HCONE_HCONE, CalibrationType.FP_FP)
        else:
            raise TypeError('invalid calibration step ' + str(step))

    def __process_queue(self):
        def badpix_logic():
            # dark seq not included in AM calibrations
            if CalibrationStep.BADPIX not in self.state.completed_calibrations:
                # ignoring DARK_DARK_INT to mirror behavior of apero_processing
                last_dark = self.__get_last_sequence_of(CalibrationType.DARK_DARK_TEL)
                last_flat = self.__get_last_sequence_of(CalibrationType.FLAT_FLAT)
                if last_dark and last_flat:
                    self.drs.cal_badpix(last_flat, last_dark)
                    return True

        def wave_logic(hc_exposures: ExposureSeq) -> bool:
            last_fp_seq = self.__get_last_sequence_of(CalibrationType.FP_FP)
            return self.drs.cal_wave(hc_exposures, last_fp_seq)

        self.__process_step_generalized(CalibrationStep.BADPIX, badpix_logic)
        self.__process_step_simple(CalibrationStep.LOC, CalibrationType.DARK_FLAT, self.drs.cal_loc)
        self.__process_step_simple(CalibrationStep.LOC, CalibrationType.FLAT_DARK, self.drs.cal_loc)
        self.__process_step_simple(CalibrationStep.SHAPE, CalibrationType.FP_FP, self.drs.cal_shape)
        self.__process_step_simple(CalibrationStep.FLAT, CalibrationType.FLAT_FLAT, self.drs.cal_flat)
        self.__process_step_simple(CalibrationStep.THERMAL, CalibrationType.DARK_DARK_INT, self.drs.cal_thermal)
        self.__process_step_simple(CalibrationStep.THERMAL, CalibrationType.DARK_DARK_TEL, self.drs.cal_thermal)
        self.__process_step_simple(CalibrationStep.WAVE, CalibrationType.HCONE_HCONE, wave_logic)

    def __process_step_simple(self, step: CalibrationStep, calibration_type: CalibrationType, recipe: SimpleRecipe):
        """
        Grabs any remaining sequences of a given calibration type from the processing queue and runs a recipe on each.
        If there was at least one sequence of that type in the queue, a specified calibration step is marked complete.
        Otherwise, raises WaitingForCalibration if the step has not previously been marked complete.
        :param step: The step to mark/check
        :param calibration_type: The calibration type
        :param recipe: The recipe that is run on each sequence
        """
        complete_key = (step, calibration_type)
        self.__process_step_generalized(step, lambda: self.__process_remaining(calibration_type, recipe), complete_key)

    def __process_step_generalized(self, step: CalibrationStep, function: Callable[[], Any], complete_key=None):
        """
        Calls a function then marks a specified calibration step as complete if the function returned a truthy value.
        Otherwise, raises WaitingForCalibration if the step has not previously been marked complete.
        :param step: The step to mark/check
        :param function: The function that is called
        :param complete_key: Override the key that is used to check/mark completed steps.
        """
        if complete_key is None:
            complete_key = step
        if step in self.steps:
            result = function()
            if result:
                self.state.completed_calibrations.add(complete_key)
            elif complete_key not in self.state.completed_calibrations:
                raise WaitingForCalibration(complete_key)

    def __process_remaining(self, calibration_type: CalibrationType, recipe: SimpleRecipe) -> Sequence[ExposureSeq]:
        queue = self.state.remaining_queue[calibration_type]
        processed = []
        while queue:
            sequence = queue.popleft()
            recipe(sequence)
            processed.append(sequence)
        self.processed_sequences.extend(processed)
        return processed

    def __get_last_sequence_of(self, calibration_type: CalibrationType) -> ExposureSeq:
        all_sequences = self.state.calibration_sequences[calibration_type]
        if all_sequences:
            return all_sequences[-1]

    def __get_last_exposure_of(self, calibration_type: CalibrationType) -> Exposure:
        last_sequence = self.__get_last_sequence_of(calibration_type)
        if last_sequence:
            return last_sequence[-1]
