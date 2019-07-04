from .calibrationprocessor import CalibrationProcessor
from .objectprocessor import ObjectProcessor
from ..drswrapper import DRS
from ..packager import ProductPackager
from ..steps import ObjectStep, PreprocessStep


class Processor:
    def __init__(self, steps, ccf_params, trace):
        self.steps = steps
        self.drs = DRS(trace)
        create_products = ObjectStep.PRODUCTS in steps.objects
        packager = ProductPackager(trace, create_products)
        self.calibration_processor = CalibrationProcessor(steps.calibrations, self.drs)
        self.object_processor = ObjectProcessor(steps.objects, self.drs, packager, ccf_params)

    def preprocess_exposure(self, config, exposure):
        pp_steps = self.steps.preprocess
        if pp_steps:
            if (config.object and PreprocessStep.PPOBJ in pp_steps
                    or config.calibration and PreprocessStep.PPCAL in pp_steps) and not config.is_aborted:
                return self.drs.cal_preprocess(exposure)
        else:
            return exposure.preprocessed.exists()

    def process_exposure(self, config, exposure):
        if config.object:
            return self.object_processor.process_object_exposure(config, exposure)

    def process_sequence(self, config, exposures):
        if config.calibration:
            if not self.steps.calibrations:
                return # Hack to speed things up a little
            calibration = config.calibration
            self.calibration_processor.add_sequence_to_queue(exposures, calibration)
            return self.calibration_processor.attempt_processing_queue()
        elif config.object.instrument_mode.is_polarimetry():
            return self.object_processor.process_polar_seqeunce(exposures)
