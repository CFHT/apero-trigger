from pathlib import Path
from typing import Collection, Dict, Sequence, Tuple

from . import packager
from .drswrapper import DRS
from ..baseinterface.steps import Step
from ..common import CcfParams, Exposure, Fiber, ObjectConfig, ObjectStep, ObjectType, TargetType, TelluSuffix


class ObjectProcessor:
    def __init__(self, steps: Collection[Step], drs: DRS, ccf_params: CcfParams):
        self.steps = steps
        self.drs = drs
        self.ccf_params = ccf_params

    def process_object_exposure(self, object_config: ObjectConfig, exposure: Exposure) -> Dict:
        extracted_path = self.__extract_object(exposure)
        if object_config.object_type == ObjectType.OBJ_FP:
            if ObjectStep.LEAK in self.steps:
                self.drs.cal_leak(exposure)
        if object_config.target == TargetType.STAR:
            is_telluric_corrected = self.__telluric_correction(exposure)
            is_ccf_calculated, ccf_path = self.__ccf(exposure, is_telluric_corrected)
            if ObjectStep.PRODUCTS in self.steps and not self.drs.trace:
                packager.create_1d_spectra_product(exposure, is_telluric_corrected)
            return {
                'extracted_path': extracted_path,
                'ccf_path': ccf_path if is_ccf_calculated else None,
                'is_ccf_calculated': is_ccf_calculated,
                'is_telluric_corrected': is_telluric_corrected,
            }
        if ObjectStep.PRODUCTS in self.steps and not self.drs.trace:
            packager.create_1d_spectra_product(exposure)
        return {'extracted_path': extracted_path}

    def process_object_sequence(self, object_config: ObjectConfig, exposures: Sequence[Exposure]) -> Dict:
        if object_config.instrument_mode.is_polarimetry():
            return self.__process_polar_sequence(exposures)

    @staticmethod
    def is_object_config_used_for_step(object_config: ObjectConfig, step: ObjectStep) -> bool:
        if step in (ObjectStep.EXTRACT, ObjectStep.PRODUCTS, ObjectStep.SNRONLY):
            return True
        elif step == ObjectStep.LEAK:
            return object_config.object_type == ObjectType.OBJ_FP
        elif step in (ObjectStep.FITTELLU, ObjectStep.CCF):
            return object_config.target == TargetType.STAR
        elif step == ObjectStep.POL:
            return object_config.instrument_mode.is_polarimetry()
        else:
            raise TypeError('invalid object step ' + str(step))

    def __extract_object(self, exposure: Exposure) -> Path:
        if ObjectStep.SNRONLY in self.steps:
            self.drs.cal_extract(exposure, fiber=Fiber.AB.value)
        if ObjectStep.EXTRACT in self.steps:
            self.drs.cal_extract(exposure)
        if ObjectStep.PRODUCTS in self.steps and not self.drs.trace:
            packager.create_2d_spectra_product(exposure)
        return exposure.e2ds(Fiber.AB)

    def __telluric_correction(self, exposure: Exposure) -> bool:
        if ObjectStep.FITTELLU in self.steps:
            telluric_corrected = self.drs.obj_fit_tellu(exposure)
        else:
            telluric_corrected = exposure.e2ds(Fiber.AB, TelluSuffix.TCORR).exists()
        if ObjectStep.PRODUCTS in self.steps and not self.drs.trace:
            packager.create_tell_product(exposure)
        return telluric_corrected

    def __ccf(self, exposure: Exposure, telluric_corrected: bool) -> Tuple[bool, Path]:
        # We are not passing in the ccf params, but we are using them to generate the ccf filename
        mask = self.ccf_params.mask
        ccf_path = exposure.ccf(mask, tellu_suffix=TelluSuffix.tcorr(telluric_corrected))
        if ObjectStep.CCF in self.steps:
            ccf_calculated = self.drs.cal_ccf(exposure, telluric_corrected)
        else:
            ccf_calculated = ccf_path.exists()
        if ObjectStep.PRODUCTS in self.steps and not self.drs.trace:
            packager.create_ccf_product(exposure, mask, Fiber.AB, telluric_corrected=telluric_corrected)
        return ccf_calculated, ccf_path

    def __process_polar_sequence(self, exposures: Sequence[Exposure]) -> Dict:
        if len(exposures) < 2:
            return {'is_polar_done': False}
        if ObjectStep.POL in self.steps:
            # Note: if the telluric correction previously succeeded but failed this time, this will use old files
            telluric_corrected = all(exposure.e2ds(Fiber.AB, TelluSuffix.TCORR).exists() for exposure in exposures)
            self.drs.pol(exposures, telluric_corrected)
        if ObjectStep.PRODUCTS in self.steps and not self.drs.trace:
            packager.create_pol_product(exposures[0])
        return {'is_polar_done': True}
